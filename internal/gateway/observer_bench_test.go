package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"serverflow/internal/config"
)

// cannedUpstream answers every request from memory, so a benchmark measures the gateway's own request path
// (middleware, parsing, relay, metrics and observers) and not a network.
type cannedUpstream struct {
	stream bool
}

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

func benchServer(stream bool, opts ...Option) *Server {
	cfg := config.Default().Gateway
	cfg.Models = []string{"qwen-7b"}
	s := newWithUpstream(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), cannedUpstream{stream: stream})
	for _, o := range opts {
		o(s)
	}
	return s
}

// BenchmarkObserverRequest is the per-request cost of the gateway's instrumentation path: a whole in-process
// request through Handler(), upstream stubbed. Run it with -benchmem; the prep plan records allocations per
// request before and after the observer seam.
//
//	go test -run '^$' -bench BenchmarkObserverRequest -benchmem ./internal/gateway
func BenchmarkObserverRequest(b *testing.B) {
	for _, tc := range []struct {
		name   string
		stream bool
		body   string
		opts   []Option
	}{
		{"static_nonstream", false, `{"model":"qwen-7b","messages":[{"role":"user","content":"hello"}]}`, nil},
		{"static_stream", true, `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hello"}]}`, nil},
		// Two more (no-op) observers beyond metrics: the marginal cost of the fan-out itself.
		{"static_nonstream_two_nop_observers", false, `{"model":"qwen-7b","messages":[{"role":"user","content":"hello"}]}`,
			[]Option{WithObserver(NopObserver{}), WithObserver(NopObserver{})}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			h := benchServer(tc.stream, tc.opts...).Handler()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodPost, chatCompletionsPath, strings.NewReader(tc.body))
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != 200 {
					b.Fatalf("status %d", rec.Code)
				}
			}
		})
	}
}

// BenchmarkMetricsObserverEvents is the cost of the metrics observer alone: the event sequence of one
// successful registry-mode request (started, admitted, attempt started, first token, attempt ended,
// completed) fed straight to it, with no HTTP in between. Phase 10 budgets this at 20 microseconds p95 and
// no allocations in the steady state (docs/benchmarks/phase-10-observability.md).
//
//	go test -run '^$' -bench BenchmarkMetricsObserverEvents -benchmem -count=10 ./internal/gateway
func BenchmarkMetricsObserverEvents(b *testing.B) {
	m := newMetrics()
	ctx := context.Background()
	run := func(workers []string) func(*testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					w := workers[i%len(workers)]
					i++
					m.RequestStarted(ctx, RequestStart{ID: "r"})
					m.RequestAdmitted(ctx, Admission{RequestID: "r", Model: "qwen-7b", RateLimitChecked: true, RateLimitDuration: 200_000})
					m.AttemptStarted(ctx, AttemptStart{RequestID: "r", Number: 1, WorkerID: w, Model: "qwen-7b", Strategy: "least-active", SelectDuration: 20_000, SinceRequestStart: 100_000, WorkerState: "READY"})
					m.FirstToken(ctx, FirstToken{RequestID: "r", TTFT: 5_000_000})
					m.AttemptEnded(ctx, AttemptEnd{RequestID: "r", Number: 1, WorkerID: w, Model: "qwen-7b", Outcome: AttemptOK})
					m.RequestCompleted(ctx, Completion{RequestID: "r", Model: "qwen-7b", Status: 200, Duration: 50_000_000, Attempts: 1, TTFT: 5_000_000, Handled: true})
				}
			})
		}
	}
	b.Run("one_worker", run([]string{"w1"}))
	b.Run("sixteen_workers", run([]string{"w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10", "w11", "w12", "w13", "w14", "w15", "w16"}))
}
