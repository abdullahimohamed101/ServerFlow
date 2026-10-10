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
