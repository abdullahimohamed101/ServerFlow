package mockworker

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"serverflow/internal/tracing"
)

const (
	tpSampled   = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	tpUnsampled = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
)

type traceRig struct {
	exp  *tracetest.InMemoryExporter
	rec  *tracetest.SpanRecorder
	srv  *httptest.Server
	logs *bytes.Buffer
}

func newTraceRig(t *testing.T, mutate func(*Config)) *traceRig {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Model, cfg.TTFT, cfg.TokensPerSecond, cfg.OutputTokens, cfg.Seed = "qwen-7b", 5*time.Millisecond, 1000, 4, 1
	if mutate != nil {
		mutate(&cfg)
	}
	r := &traceRig{exp: tracetest.NewInMemoryExporter(), rec: tracetest.NewSpanRecorder(), logs: &bytes.Buffer{}}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.NewSampler(tracing.SamplerParentOnly, 0)),
		sdktrace.WithResource(tracing.NewResource(tracing.Service{Name: "serverflow-mock-worker", Version: "test", InstanceID: "test"})),
		sdktrace.WithSyncer(r.exp), sdktrace.WithSpanProcessor(r.rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	r.srv = httptest.NewServer(New(cfg, slog.New(slog.NewJSONHandler(r.logs, nil)), WithTracing(tracing.NewProvider(tp))).Handler())
	t.Cleanup(r.srv.Close)
	return r
}

// settle waits until every started span has ended: the handler's deferred span end runs just after the body, so
// a fixed sleep is a guess that fails on a loaded runner. A request that started no span returns at once.
func (r *traceRig) settle() {
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline) && len(r.rec.Started()) != len(r.rec.Ended()); {
		time.Sleep(2 * time.Millisecond)
	}
}

func (r *traceRig) chat(t *testing.T, body string, hdr ...string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, r.srv.URL+"/v1/chat/completions", strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	r.settle()
	return resp.StatusCode
}

const stream = `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`

func TestWorkerSpansJoinTheGatewaysTrace(t *testing.T) {
	r := newTraceRig(t, nil)
	if got := r.chat(t, stream, "Traceparent", tpSampled, "X-Request-ID", "req_0123456789abcdef", "X-Attempt-ID", "att_fedcba9876543210"); got != 200 {
		t.Fatalf("status %d", got)
	}
	spans := r.exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("want inference and queue_wait, got %d spans", len(spans))
	}
	var inf, q tracetest.SpanStub
	for _, s := range spans {
		switch s.Name {
		case "inference":
			inf = s
		case "queue_wait":
			q = s
		}
	}
	if inf.SpanContext.TraceID().String() != "0af7651916cd43dd8448eb211c80319c" || inf.Parent.SpanID().String() != "b7ad6b7169203331" || !inf.Parent.IsRemote() {
		t.Errorf("inference must be a child of the caller's span: %+v", inf.Parent)
	}
	if q.Parent.SpanID() != inf.SpanContext.SpanID() {
		t.Error("queue_wait must be a child of inference")
	}
	got := map[string]string{}
	for _, kv := range inf.Attributes {
		got[string(kv.Key)] = kv.Value.String()
	}
	if got["serverflow.request_id"] != "req_0123456789abcdef" || got["serverflow.attempt_id"] != "att_fedcba9876543210" ||
		got["serverflow.model"] != "qwen-7b" || got["serverflow.stream"] != "true" || got["serverflow.worker_id"] == "" {
		t.Errorf("attributes: %v", got)
	}
	if len(inf.Events) != 1 || inf.Events[0].Name != "first_token" {
		t.Errorf("events: %+v", inf.Events)
	}
	if !strings.Contains(r.logs.String(), `"trace_id":"0af7651916cd43dd8448eb211c80319c"`) {
		t.Errorf("the chat log must carry the trace id: %s", r.logs.String())
	}
}

func TestWorkerRecordsNothingUnlessTheGatewaySampled(t *testing.T) {
	r := newTraceRig(t, nil)
	r.chat(t, stream)                                                                       // no traceparent: a direct caller
	r.chat(t, stream, "Traceparent", tpUnsampled)                                           // the gateway decided not to record
	r.chat(t, stream, "Traceparent", "garbage")                                             // malformed
	r.chat(t, stream, "Traceparent", tpSampled, "Tracestate", strings.Repeat("a=b,", 1000)) // tracestate ignored
	if got := len(r.exp.GetSpans()); got != 2 {
		t.Fatalf("only the sampled request may record (2 spans), got %d", got)
	}
	if n := strings.Count(r.logs.String(), "trace_id"); n != 2 {
		t.Errorf("valid traceparents (sampled or not) are logged, others are not: %d\n%s", n, r.logs.String())
	}
}

func TestWorkerValidatesTheIDsItPutsOnSpans(t *testing.T) {
	r := newTraceRig(t, nil)
	r.chat(t, stream, "Traceparent", tpSampled, "X-Request-ID", "req_ canary secret ", "X-Attempt-ID", strings.Repeat("a", 5000))
	for _, s := range r.exp.GetSpans() {
		for _, kv := range s.Attributes {
			if strings.Contains(kv.Value.String(), "canary") || len(kv.Value.String()) > 128 {
				t.Errorf("unvalidated value on %s: %s", kv.Key, kv.Value.String())
			}
			if kv.Key == tracing.KeyRequestID || kv.Key == tracing.KeyAttemptID {
				t.Errorf("an invalid ID must be omitted, got %s=%s", kv.Key, kv.Value.String())
			}
		}
	}
}

func TestWorkerFailureAndQueueSpans(t *testing.T) {
	r := newTraceRig(t, func(c *Config) { c.FailureRate, c.FailureMode = 1, ModeUnavailable })
	if got := r.chat(t, stream, "Traceparent", tpSampled); got != 503 {
		t.Fatalf("status %d", got)
	}
	spans := r.exp.GetSpans()
	if len(spans) != 1 || spans[0].Name != "inference" || spans[0].Status.Code != codes.Error {
		t.Fatalf("an injected failure ends one error inference span (no queue_wait): %+v", spans)
	}
	var mode string
	for _, kv := range spans[0].Attributes {
		if kv.Key == tracing.KeyInjectedFailure {
			mode = kv.Value.AsString()
		}
	}
	if mode != "unavailable" {
		t.Errorf("injected failure attribute %q", mode)
	}
	if len(r.rec.Started()) != len(r.rec.Ended()) {
		t.Error("span leak")
	}
}

func TestWorkerDropAndMidstreamEndTheirSpans(t *testing.T) {
	for _, mode := range []FailureMode{ModeDrop, ModeMidstream} {
		r := newTraceRig(t, func(c *Config) { c.FailureRate, c.FailureMode, c.OutputTokens = 1, mode, 6 })
		r.chat(t, stream, "Traceparent", tpSampled)
		r.settle()
		if s, e := len(r.rec.Started()), len(r.rec.Ended()); s != e || s == 0 {
			t.Errorf("%s: started %d ended %d", mode, s, e)
		}
	}
}

func TestOTLPEndpointFlagValidation(t *testing.T) {
	if _, _, err := ParseFlags([]string{"--otlp-endpoint=http://collector.example:4318"}, io.Discard); err == nil || strings.Contains(err.Error(), "collector.example") {
		t.Errorf("plaintext non-loopback must be refused without echoing the endpoint: %v", err)
	}
	for _, args := range [][]string{
		{"--otlp-endpoint=http://127.0.0.1:4318"},
		{"--otlp-endpoint=https://collector.example:4318"},
		{"--otlp-endpoint=http://collector.example:4318", "--trace-insecure-ok"},
		{},
	} {
		if _, _, err := ParseFlags(args, io.Discard); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}
	if _, _, err := ParseFlags([]string{"--otlp-endpoint=ftp://x"}, io.Discard); err == nil {
		t.Error("expected an error for a bad scheme")
	}
}
