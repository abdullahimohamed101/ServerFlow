package gateway_test

import (
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

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
)

// panicUpstream makes the inference handler panic after the attempt began.
type panicUpstream struct{}

func (panicUpstream) Do(context.Context, string, []byte, string) (*http.Response, error) {
	panic("upstream blew up")
}
func (panicUpstream) Probe(context.Context) error { return nil }

func TestAHandlerPanicEndsEverySpanOnce(t *testing.T) {
	exp, rec := tracetest.NewInMemoryExporter(), tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	cfg := config.Default().Gateway
	cfg.Models = []string{"qwen-7b"}
	gw := gateway.NewWithUpstreamForTest(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), panicUpstream{},
		gateway.WithObserver(gwtrace.New(tracing.NewProvider(tp), gwtrace.Options{IncludeTenantID: true})))
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("a panic is a 500, got %d", resp.StatusCode)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(rec.Ended()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if s, e := len(rec.Started()), len(rec.Ended()); s != 2 || e != 2 {
		t.Fatalf("started %d, ended %d: every span ends exactly once", s, e)
	}
	for _, s := range exp.GetSpans() {
		if s.Status.Code != codes.Error {
			t.Errorf("%s status %v", s.Name, s.Status)
		}
		if strings.Contains(s.Status.Description, "blew up") {
			t.Errorf("the panic text leaked into %s: %q", s.Name, s.Status.Description)
		}
	}
}
