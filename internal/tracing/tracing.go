// Package tracing is the OpenTelemetry core shared by the gateway and the mock worker: provider setup, the
// sampler, the W3C propagator, a bounded non-blocking export pipeline, and the attribute allow-list.
// OpenTelemetry is imported only under internal/tracing (ADR-018); internal/gateway never sees it.
//
// Nothing here uses OpenTelemetry's global state except where documented: the tracer provider and the
// propagator are passed explicitly, the standard OTEL_* environment variables are not honoured, and span
// limits are set explicitly so a hostile environment cannot widen them.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Sampler selects who decides whether a trace is recorded.
type Sampler int

const (
	// SamplerRatio samples a fraction of new traces and ignores any incoming sampled flag (gateway default).
	SamplerRatio Sampler = iota
	// SamplerParentRatio follows a remote parent's decision and samples a fraction of root traces (gateway
	// with tracing.incoming=trust).
	SamplerParentRatio
	// SamplerParentOnly records only when a remote parent was sampled and never starts a recorded trace of
	// its own (mock worker): an untraced or unsampled request costs almost nothing, and a caller reaching
	// the worker directly cannot make it record.
	SamplerParentOnly
)

// Config is what Setup needs. The gateway and the mock worker both build one.
type Config struct {
	Endpoint       string
	Sampler        Sampler
	SampleRatio    float64
	QueueSize      int
	MaxExportBatch int
	BatchTimeout   time.Duration
	ExportTimeout  time.Duration
	// HTTPClient, if set, carries the export requests (tests). Otherwise a client without proxy is built.
	HTTPClient *http.Client
	// Exporter replaces the OTLP exporter (tests of the pipeline).
	Exporter sdktrace.SpanExporter
}

// Service identifies the process in the resource.
type Service struct {
	Name, Version, InstanceID string
}

// Provider owns the tracer provider and the export pipeline.
type Provider struct {
	tp     *sdktrace.TracerProvider
	tracer trace.Tracer
	prop   propagation.TextMapPropagator
	bat    *batcher
}

const instrumentationName = "serverflow/internal/tracing"

// Setup builds a provider that exports over OTLP/HTTP. It does not connect: a collector that is down at
// start is not an error, and a dead collector can never slow or fail a request (ADR-018).
func Setup(cfg Config, svc Service, log *slog.Logger) (*Provider, error) {
	exp := cfg.Exporter
	if exp == nil {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, errors.New("tracing endpoint must be an http:// or https:// URL")
		}
		client := cfg.HTTPClient
		if client == nil {
			tr := http.DefaultTransport.(*http.Transport).Clone()
			tr.Proxy = nil // the collector is an operator-chosen endpoint; do not honour proxy variables
			client = &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		}
		endpoint := cfg.Endpoint
		if u.Path == "" || u.Path == "/" {
			endpoint = strings.TrimRight(endpoint, "/") + "/v1/traces" // a bare base URL gets the standard path
		}
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpointURL(endpoint),
			otlptracehttp.WithHTTPClient(client),
			// Explicit values for everything the SDK would otherwise read from OTEL_EXPORTER_OTLP_*: headers
			// (credentials) and compression, timeout and TLS settings cannot be injected by the environment.
			otlptracehttp.WithHeaders(map[string]string{}),
			otlptracehttp.WithCompression(otlptracehttp.NoCompression),
			otlptracehttp.WithTimeout(cfg.ExportTimeout),
			otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
				Enabled: true, InitialInterval: 200 * time.Millisecond, MaxInterval: time.Second, MaxElapsedTime: cfg.ExportTimeout,
			}),
		}
		exp, err = otlptracehttp.New(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("create the trace exporter: %w", err)
		}
	}

	res := resource.NewSchemaless(
		KeyServiceName.String(svc.Name),
		KeyServiceVersion.String(svc.Version),
		KeyServiceInstance.String(svc.InstanceID),
	)
	bat := newBatcher(exp, res, cfg.QueueSize, cfg.MaxExportBatch, cfg.BatchTimeout, cfg.ExportTimeout, log)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(samplerFor(cfg)),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bat),
		sdktrace.WithRawSpanLimits(spanLimits()),
	)
	return newProvider(tp, bat), nil
}

// NewProvider wraps an SDK tracer provider built by the caller. It is the seam tests use to record spans in
// memory; production code uses Setup.
func NewProvider(tp *sdktrace.TracerProvider) *Provider { return newProvider(tp, nil) }

func newProvider(tp *sdktrace.TracerProvider, bat *batcher) *Provider {
	return &Provider{tp: tp, tracer: tp.Tracer(instrumentationName), prop: propagation.TraceContext{}, bat: bat}
}

// spanLimits bounds what one span can hold, whatever the environment says.
func spanLimits() sdktrace.SpanLimits {
	return sdktrace.SpanLimits{
		AttributeValueLengthLimit:   MaxAttrValueBytes,
		AttributeCountLimit:         32,
		EventCountLimit:             8,
		LinkCountLimit:              4,
		AttributePerEventCountLimit: 8,
		AttributePerLinkCountLimit:  4,
	}
}

func samplerFor(cfg Config) sdktrace.Sampler {
	ratio := sdktrace.TraceIDRatioBased(cfg.SampleRatio)
	switch cfg.Sampler {
	case SamplerParentRatio:
		return sdktrace.ParentBased(ratio)
	case SamplerParentOnly:
		return sdktrace.ParentBased(sdktrace.NeverSample())
	}
	return ratio
}

// Tracer returns the provider's tracer.
func (p *Provider) Tracer() trace.Tracer { return p.tracer }

// Stats reports the export pipeline's counters; zero for a provider that has none (tests).
func (p *Provider) Stats() Stats {
	if p.bat == nil {
		return Stats{}
	}
	return p.bat.stats()
}

// ForceFlush exports what is queued now.
func (p *Provider) ForceFlush(ctx context.Context) error { return p.tp.ForceFlush(ctx) }

// Shutdown flushes and stops the pipeline. Call it once after the server stopped; ctx bounds the wait, and
// with a dead collector it is what bounds the process exit.
func (p *Provider) Shutdown(ctx context.Context) error { return p.tp.Shutdown(ctx) }

// maxTracestateBytes bounds the tracestate accepted from a caller.
const maxTracestateBytes = 512

// ExtractRemote returns ctx carrying the remote span context found in h. Only traceparent (and, if
// withState, a tracestate of at most 512 bytes) is read; a malformed or duplicated header is ignored.
func (p *Provider) ExtractRemote(ctx context.Context, h http.Header, withState bool) context.Context {
	sc := RemoteSpanContext(h, withState)
	if !sc.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

// RemoteSpanContext parses h's trace context into a remote span context (invalid if absent or malformed).
func RemoteSpanContext(h http.Header, withState bool) trace.SpanContext {
	tp := h["Traceparent"]
	if len(tp) != 1 || len(tp[0]) > 55 {
		return trace.SpanContext{}
	}
	carrier := propagation.MapCarrier{"traceparent": tp[0]}
	if ts := h["Tracestate"]; withState && len(ts) == 1 && len(ts[0]) <= maxTracestateBytes {
		carrier["tracestate"] = ts[0]
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return trace.SpanContext{}
	}
	return sc
}

// Inject writes the span context of ctx into h as traceparent (and tracestate if it has one).
func (p *Provider) Inject(ctx context.Context, h http.Header) {
	c := propagation.MapCarrier{}
	p.prop.Inject(ctx, c)
	for _, k := range []string{"traceparent", "tracestate"} {
		if v := c.Get(k); v != "" {
			h.Set(k, v)
		}
	}
}

// TraceIDFrom returns the trace ID of the span in ctx as hex, or "" if there is none.
func TraceIDFrom(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// StringAttr is a bounded string attribute: the value is cut to MaxAttrValueBytes.
func StringAttr(k attribute.Key, v string) attribute.KeyValue {
	if len(v) > MaxAttrValueBytes {
		v = v[:MaxAttrValueBytes]
	}
	return k.String(v)
}
