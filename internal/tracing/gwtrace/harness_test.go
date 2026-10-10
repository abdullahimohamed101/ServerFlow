package gwtrace_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
)

const (
	model      = "qwen-7b"
	plainBody  = `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`
	streamBody = `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hi"}]}`
)

type syncBuf struct {
	mu sync.Mutex
	sb strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sb.Write(p)
}
func (s *syncBuf) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.sb.String() }

// seen is what the fake worker received.
type seen struct {
	mu      sync.Mutex
	headers []http.Header
}

func (s *seen) add(h http.Header) {
	s.mu.Lock()
	s.headers = append(s.headers, h.Clone())
	s.mu.Unlock()
}
func (s *seen) all() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]http.Header(nil), s.headers...)
}

type envOpts struct {
	ratio     float64 // default 1
	unsampled bool    // ratio 0
	sampler   tracing.Sampler
	incoming  string
	noTenant  bool
	upstream  func(seen *seen) http.Handler
	gwOpts    []gateway.Option
	models    []string
}

type env struct {
	t      *testing.T
	exp    *tracetest.InMemoryExporter
	rec    *tracetest.SpanRecorder
	prov   *tracing.Provider
	url    string
	client *http.Client
	logs   *syncBuf
	up     *seen
}

func sseWorker(s *seen) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.add(r.Header)
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			for i := 0; i < 3; i++ {
				_, _ = io.WriteString(w, "data: {\"x\":1}\n\n")
				fl.Flush()
				time.Sleep(3 * time.Millisecond)
			}
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
}

func newEnv(t *testing.T, o envOpts) *env {
	t.Helper()
	if o.upstream == nil {
		o.upstream = sseWorker
	}
	ratio := o.ratio
	if ratio == 0 && !o.unsampled {
		ratio = 1
	}
	e := &env{t: t, exp: tracetest.NewInMemoryExporter(), rec: tracetest.NewSpanRecorder(), logs: &syncBuf{}, up: &seen{}}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(tracing.NewSampler(o.sampler, ratio)),
		sdktrace.WithResource(tracing.NewResource(tracing.Service{Name: "serverflow-gateway", Version: "test", InstanceID: "test"})),
		sdktrace.WithSyncer(e.exp), sdktrace.WithSpanProcessor(e.rec),
		sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{AttributeValueLengthLimit: 128, AttributeCountLimit: 32, EventCountLimit: 8,
			LinkCountLimit: 4, AttributePerEventCountLimit: 8, AttributePerLinkCountLimit: 4}),
	)
	e.prov = tracing.NewProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	fake := httptest.NewServer(o.upstream(e.up))
	t.Cleanup(fake.Close)
	cfg := config.Default().Gateway
	cfg.UpstreamURL, cfg.Models = fake.URL, []string{model}
	if o.models != nil {
		cfg.Models = o.models
	}
	cfg.UpstreamHeaderTimeout, cfg.UpstreamIdleTimeout = 5*time.Second, 5*time.Second
	incoming := o.incoming
	opts := append([]gateway.Option{gateway.WithObserver(gwtrace.New(e.prov, gwtrace.Options{Incoming: incoming, IncludeTenantID: !o.noTenant}))}, o.gwOpts...)
	gw := gateway.New(cfg, slog.New(slog.NewJSONHandler(e.logs, nil)), opts...)
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	e.url, e.client = srv.URL, &http.Client{Transport: tr, Timeout: 10 * time.Second}
	return e
}

func (e *env) post(body string, hdr ...string) (*http.Response, string) {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// spans waits for every started span to end and returns what was exported.
func (e *env) spans() tracetest.SpanStubs {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(e.rec.Started()) > 0 && len(e.rec.Started()) == len(e.rec.Ended()) {
			time.Sleep(20 * time.Millisecond)
			if len(e.rec.Started()) == len(e.rec.Ended()) {
				return e.exp.GetSpans()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.t.Fatalf("spans did not all end: started %d ended %d", len(e.rec.Started()), len(e.rec.Ended()))
	return nil
}

// noLeak fails if a span was started and never ended.
func (e *env) noLeak() {
	e.t.Helper()
	time.Sleep(50 * time.Millisecond)
	if s, en := len(e.rec.Started()), len(e.rec.Ended()); s != en {
		e.t.Errorf("span leak: started %d, ended %d", s, en)
	}
}

func byName(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func one(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()
	got := byName(spans, name)
	if len(got) != 1 {
		t.Fatalf("want exactly one %q span, got %d (all: %v)", name, len(got), names(spans))
	}
	return got[0]
}

func names(spans tracetest.SpanStubs) []string {
	var out []string
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}
