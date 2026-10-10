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

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
)

type cannedUpstream struct{ stream bool }

func (c cannedUpstream) Do(context.Context, string, []byte, string) (*http.Response, error) {
	h := http.Header{}
	body := `{"id":"x","object":"chat.completion","choices":[]}`
	if c.stream {
		h.Set("Content-Type", "text/event-stream")
		body = "data: {\"n\":1}\n\ndata: [DONE]\n\n"
	} else {
		h.Set("Content-Type", "application/json")
	}
	return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(body))}, nil
}
func (cannedUpstream) Probe(context.Context) error { return nil }

// discardExporter accepts every batch, so the benchmark includes the real queue and batcher but no network.
type discardExporter struct{}

func (discardExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (discardExporter) Shutdown(context.Context) error                             { return nil }

// BenchmarkTracingObserver is the per-request cost of tracing on the gateway's in-process request path:
// tracing off (no observer), on but unsampled, and on at 100% sampling with the real export queue. Run:
//
//	go test -run '^$' -bench BenchmarkTracingObserver -benchmem -count=5 ./internal/gateway
func BenchmarkTracingObserver(b *testing.B) {
	bodies := map[bool]string{
		false: `{"model":"qwen-7b","messages":[{"role":"user","content":"hello"}]}`,
		true:  `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hello"}]}`,
	}
	for _, mode := range []string{"off", "unsampled", "sampled"} {
		for _, stream := range []bool{false, true} {
			name := mode + "_nonstream"
			if stream {
				name = mode + "_stream"
			}
			b.Run(name, func(b *testing.B) {
				cfg := config.Default().Gateway
				cfg.Models = []string{"qwen-7b"}
				var opts []gateway.Option
				if mode != "off" {
					ratio := 1.0
					if mode == "unsampled" {
						ratio = 0
					}
					prov, err := tracing.Setup(tracing.Config{Sampler: tracing.SamplerRatio, SampleRatio: ratio, QueueSize: 2048, MaxExportBatch: 512,
						BatchTimeout: 50 * time.Millisecond, ExportTimeout: time.Second, Exporter: discardExporter{}}, tracing.Service{Name: "bench"}, nil)
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = prov.Shutdown(context.Background()) }()
					opts = append(opts, gateway.WithObserver(gwtrace.New(prov, gwtrace.Options{IncludeTenantID: true})))
				}
				h := gateway.NewWithUpstreamForTest(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), cannedUpstream{stream: stream}, opts...).Handler()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(bodies[stream]))
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					if rec.Code != 200 {
						b.Fatalf("status %d", rec.Code)
					}
				}
			})
		}
	}
}
