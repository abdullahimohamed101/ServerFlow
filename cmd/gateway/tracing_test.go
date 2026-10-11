package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
)

type logSink struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logSink) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logSink) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// collector counts the spans it is sent.
func collector(t *testing.T) (url string, spans *atomic.Int64) {
	t.Helper()
	spans = &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req collectortrace.ExportTraceServiceRequest
		if proto.Unmarshal(body, &req) == nil {
			for _, rs := range req.ResourceSpans {
				for _, ss := range rs.ScopeSpans {
					spans.Add(int64(len(ss.Spans)))
				}
			}
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		resp, _ := proto.Marshal(&collectortrace.ExportTraceServiceResponse{})
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, spans
}

func tracingConfig(endpoint string) config.Config {
	cfg := config.Default()
	cfg.Tracing.Enabled, cfg.Tracing.Endpoint = true, endpoint
	// A batch interval far beyond the test: only the shutdown flush can deliver the spans.
	cfg.Tracing.BatchTimeout = time.Hour
	return cfg
}

func TestTracingIsOffByDefaultAndSetupDoesNothing(t *testing.T) {
	p, opts, err := setupTracing(config.Default(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil || p != nil || len(opts) != 0 {
		t.Fatalf("tracing off must set nothing up: %v %v %v", p, opts, err)
	}
	shutdownTracing(nil, slog.New(slog.NewJSONHandler(io.Discard, nil))) // must not panic
}

func TestGatewayShutdownFlushesTheQueuedSpans(t *testing.T) {
	url, got := collector(t)
	logs := &logSink{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	cfg := tracingConfig(url)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer up.Close()
	cfg.Gateway.UpstreamURL, cfg.Gateway.Models = up.URL, []string{"qwen-7b"}

	tracer, topts, err := setupTracing(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	srv := gateway.New(cfg.Gateway, logger, topts...)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveAndFlush(ctx, srv, ln, tracer, logger) }()

	for i := 0; i < 5; i++ {
		resp, err := http.Post("http://"+ln.Addr().String()+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	time.Sleep(100 * time.Millisecond)
	if n := got.Load(); n != 0 {
		t.Fatalf("%d spans were exported before shutdown although the batch interval is an hour", n)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	// 5 requests, each at least gateway.receive and worker.forward.
	if n := got.Load(); n < 10 {
		t.Errorf("the collector received %d spans after shutdown, want at least 10: the queue was not flushed", n)
	}
	if !strings.Contains(logs.String(), `"msg":"tracing stopped"`) || !strings.Contains(logs.String(), `"spans_exported":`) {
		t.Errorf("shutdown must log the final span counts:\n%s", logs.String())
	}
}

func TestStartupNeverLogsTheTracingEndpoint(t *testing.T) {
	logs := &logSink{}
	cfg := tracingConfig("https://canary-collector-host.example:4318/canary-path")
	cfg.Tracing.Incoming = config.TraceIncomingTrust // also exercises the warning line
	p, _, err := setupTracing(cfg, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
	if out := logs.String(); !strings.Contains(out, "tracing enabled") || strings.Contains(out, "canary-collector-host") || strings.Contains(out, "canary-path") {
		t.Errorf("startup logs must announce tracing without the endpoint:\n%s", out)
	}
}
