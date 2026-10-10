package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Incoming trace context policies (tracing.incoming). See docs/operations/tracing.md and ADR-018.
const (
	// TraceIncomingLink starts a new trace per request and links the client's span context (the default).
	TraceIncomingLink = "link"
	// TraceIncomingIgnore starts a new trace and ignores any client trace context.
	TraceIncomingIgnore = "ignore"
	// TraceIncomingTrust continues the client's trace. Only for a gateway behind a trusted proxy: any
	// client can otherwise force sampling and graft spans into a trace of its choosing.
	TraceIncomingTrust = "trust"
)

// TracingConfig configures OpenTelemetry tracing (Phase 11). Tracing is off by default; when Enabled, spans
// are exported over OTLP/HTTP to Endpoint. The section holds no secret today, but it redacts its endpoint
// like the other sections so a credential added later cannot leak through a log line.
type TracingConfig struct {
	Enabled bool `yaml:"enabled"`
	// Endpoint is the OTLP/HTTP base URL, for example http://127.0.0.1:4318.
	Endpoint string `yaml:"endpoint"`
	// SampleRatio is the head sampling probability in 0-1.
	SampleRatio float64 `yaml:"sample_ratio"`
	// Incoming is link, ignore or trust (see the Tracing constants).
	Incoming string `yaml:"incoming"`
	// IncludeTenantID adds the opaque tenant ID to spans.
	IncludeTenantID bool `yaml:"include_tenant_id"`
	// QueueSize, MaxExportBatch, BatchTimeout and ExportTimeout shape the non-blocking span pipeline.
	QueueSize      int           `yaml:"queue_size"`
	MaxExportBatch int           `yaml:"max_export_batch"`
	BatchTimeout   time.Duration `yaml:"batch_timeout"`
	ExportTimeout  time.Duration `yaml:"export_timeout"`
	// AllowInsecureTransport permits plaintext http:// to a non-loopback collector.
	AllowInsecureTransport bool `yaml:"allow_insecure_transport"`
}

// Bounds for tracing.
const (
	maxTraceQueue        = 1 << 20
	maxTraceBatchTimeout = time.Minute
	maxTraceExportTO     = time.Minute
	maxTraceEndpointLen  = 2048
)

// String formats the config with the endpoint redacted. GoString and LogValue do the same.
func (t TracingConfig) String() string {
	ep := t.Endpoint
	if ep != "" {
		ep = "<redacted>"
	}
	return fmt.Sprintf("{enabled:%t endpoint:%s sample_ratio:%v incoming:%s include_tenant_id:%t queue_size:%d max_export_batch:%d batch_timeout:%v export_timeout:%v allow_insecure_transport:%t}",
		t.Enabled, ep, t.SampleRatio, t.Incoming, t.IncludeTenantID, t.QueueSize, t.MaxExportBatch, t.BatchTimeout, t.ExportTimeout, t.AllowInsecureTransport)
}

// GoString implements fmt.GoStringer.
func (t TracingConfig) GoString() string { return "config.TracingConfig" + t.String() }

// LogValue implements slog.LogValuer.
func (t TracingConfig) LogValue() slog.Value { return slog.StringValue(t.String()) }

// MarshalJSON redacts the endpoint, so logging a whole Config as JSON cannot leak it.
func (t TracingConfig) MarshalJSON() ([]byte, error) {
	type plain TracingConfig // no methods, so no recursion
	p := plain(t)
	if p.Endpoint != "" {
		p.Endpoint = "<redacted>"
	}
	return json.Marshal(p)
}

func defaultTracing() TracingConfig {
	return TracingConfig{
		Endpoint:        "http://127.0.0.1:4318",
		SampleRatio:     1.0,
		Incoming:        TraceIncomingLink,
		IncludeTenantID: true,
		QueueSize:       2048,
		MaxExportBatch:  512,
		BatchTimeout:    5 * time.Second,
		ExportTimeout:   5 * time.Second,
	}
}

// validate checks the tracing settings. With tracing disabled nothing else is checked, so a malformed
// value cannot stop a gateway that never traces. It never echoes the endpoint.
func (t *TracingConfig) validate() error {
	if !t.Enabled {
		return nil
	}
	if err := checkTraceEndpoint(t.Endpoint, t.AllowInsecureTransport); err != nil {
		return err
	}
	if !(t.SampleRatio >= 0 && t.SampleRatio <= 1) { // also rejects NaN
		return fmt.Errorf("tracing.sample_ratio must be between 0 and 1")
	}
	switch t.Incoming {
	case TraceIncomingLink, TraceIncomingIgnore, TraceIncomingTrust:
	default:
		return fmt.Errorf("tracing.incoming must be %q, %q or %q", TraceIncomingLink, TraceIncomingIgnore, TraceIncomingTrust)
	}
	if t.QueueSize < 1 || t.QueueSize > maxTraceQueue {
		return fmt.Errorf("tracing.queue_size must be in 1-%d, got %d", maxTraceQueue, t.QueueSize)
	}
	if t.MaxExportBatch < 1 || t.MaxExportBatch > t.QueueSize {
		return fmt.Errorf("tracing.max_export_batch must be in 1 to tracing.queue_size, got %d", t.MaxExportBatch)
	}
	if t.BatchTimeout <= 0 || t.BatchTimeout > maxTraceBatchTimeout {
		return fmt.Errorf("tracing.batch_timeout must be > 0 and at most %v", maxTraceBatchTimeout)
	}
	if t.ExportTimeout <= 0 || t.ExportTimeout > maxTraceExportTO {
		return fmt.Errorf("tracing.export_timeout must be > 0 and at most %v", maxTraceExportTO)
	}
	return nil
}

// checkTraceEndpoint validates an OTLP/HTTP endpoint URL without echoing it. Exported through
// CheckTraceEndpoint for the mock worker's flag.
func checkTraceEndpoint(endpoint string, allowInsecure bool) error {
	if endpoint == "" || len(endpoint) > maxTraceEndpointLen {
		return fmt.Errorf("tracing.endpoint must be an http:// or https:// URL")
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("tracing.endpoint must be an http:// or https:// URL")
	}
	if u.User != nil {
		return fmt.Errorf("tracing.endpoint must not contain credentials")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("tracing.endpoint must not contain a query or fragment")
	}
	if u.Scheme == "http" && !isLoopbackHost(u.Hostname()) && !allowInsecure {
		return fmt.Errorf("tracing.endpoint uses plaintext http:// to a non-loopback host; use https:// or set tracing.allow_insecure_transport")
	}
	return nil
}

// CheckTraceEndpoint applies the endpoint rules of tracing.endpoint to another component's setting.
func CheckTraceEndpoint(endpoint string, allowInsecure bool) error {
	return checkTraceEndpoint(endpoint, allowInsecure)
}

// applyTracingEnv overlays the SERVERFLOW_TRACING_* variables. Invalid values are ignored (validation
// still sees the previous value), like the other sections.
func applyTracingEnv(cfg *Config) {
	env := func(key string) (string, bool) { return os.LookupEnv("SERVERFLOW_TRACING_" + key) }
	t := &cfg.Tracing
	if v, ok := env("ENABLED"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			t.Enabled = b
		}
	}
	if v, ok := env("INCLUDE_TENANT_ID"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			t.IncludeTenantID = b
		}
	}
	if v, ok := env("ENDPOINT"); ok {
		t.Endpoint = strings.TrimSpace(v)
	}
	if v, ok := env("SAMPLE_RATIO"); ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			t.SampleRatio = f
		}
	}
	if v, ok := env("INCOMING"); ok {
		t.Incoming = strings.ToLower(strings.TrimSpace(v))
	}
}
