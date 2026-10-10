package tracing

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func testConfig(endpoint string) Config {
	return Config{Endpoint: endpoint, Sampler: SamplerRatio, SampleRatio: 1, QueueSize: 64, MaxExportBatch: 16,
		BatchTimeout: 50 * time.Millisecond, ExportTimeout: 300 * time.Millisecond}
}

func emit(p *Provider, n int) {
	for i := 0; i < n; i++ {
		_, s := p.Tracer().Start(context.Background(), "gateway.receive", trace.WithSpanKind(trace.SpanKindServer))
		s.End()
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestExportReachesTheReceiver(t *testing.T) {
	rcv := newFakeReceiver(t)
	p, err := Setup(testConfig(rcv.srv.URL), Service{Name: "serverflow-gateway", Version: "test", InstanceID: "inst-1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	emit(p, 40)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(rcv.spanNames()); got != 40 {
		t.Fatalf("receiver got %d spans, want 40", got)
	}
	st := p.Stats()
	if st.Exported != 40 || st.Dropped != 0 || st.Failed != 0 || st.Ended != 40 {
		t.Errorf("stats %+v", st)
	}
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	if rcv.paths[0] != "/v1/traces" {
		t.Errorf("path %q", rcv.paths[0])
	}
	if len(rcv.requests) < 3 { // 40 spans in batches of at most 16
		t.Errorf("expected batching, got %d requests", len(rcv.requests))
	}
	res := rcv.requests[0].ResourceSpans[0].Resource
	got := map[string]string{}
	for _, kv := range res.Attributes {
		got[kv.Key] = kv.Value.GetStringValue()
	}
	if len(got) != 3 || got["service.name"] != "serverflow-gateway" || got["service.version"] != "test" || got["service.instance.id"] != "inst-1" {
		t.Errorf("resource attributes: %v", got)
	}
}

func TestEnvironmentCannotInjectHeadersOrSettings(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer canary-env-token,X-Canary=canary-env-header")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "X-Canary2=canary-env-header2")
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "env.canary=canary-env-resource")
	t.Setenv("OTEL_SERVICE_NAME", "canary-env-service")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	t.Setenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "100000")
	rcv := newFakeReceiver(t)
	p, err := Setup(testConfig(rcv.srv.URL), Service{Name: "serverflow-gateway", InstanceID: "i"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	emit(p, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rcv.spanNames()) != 3 {
		t.Fatalf("the environment changed the sampler: %v", rcv.spanNames())
	}
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	for _, h := range rcv.headers {
		for k, v := range h {
			joined := k + "=" + strings.Join(v, ",")
			if strings.Contains(joined, "canary") {
				t.Errorf("environment value was sent: %s", joined)
			}
		}
		if h.Get("Content-Encoding") != "" {
			t.Errorf("environment enabled compression")
		}
	}
	for _, kv := range rcv.requests[0].ResourceSpans[0].Resource.Attributes {
		if strings.Contains(kv.Value.GetStringValue(), "canary") {
			t.Errorf("environment value reached the resource: %v", kv)
		}
	}
}

func TestFailingReceiverNeverBlocksAndCountsFailures(t *testing.T) {
	for _, mode := range []string{"error", "hang"} {
		t.Run(mode, func(t *testing.T) {
			rcv := newFakeReceiver(t)
			rcv.setMode(mode)
			buf := &syncBuf{}
			p, err := Setup(testConfig(rcv.srv.URL), Service{Name: "svc"}, slog.New(slog.NewTextHandler(buf, nil)))
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			emit(p, 2000) // far above the queue size of 64
			if d := time.Since(start); d > time.Second {
				t.Fatalf("ending 2000 spans took %v: the request path blocked on the exporter", d)
			}
			st := p.Stats()
			if st.Queued > 64 {
				t.Errorf("queue grew past its bound: %d", st.Queued)
			}
			if st.Dropped == 0 {
				t.Errorf("expected drops with a tiny queue and a dead collector: %+v", st)
			}
			waitFor(t, "a failure to be counted", func() bool { return p.Stats().Failures > 0 })
			start = time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = p.Shutdown(ctx)
			if d := time.Since(start); d > 2500*time.Millisecond {
				t.Errorf("shutdown took %v with a dead collector", d)
			}
			st = p.Stats()
			if st.Exported != 0 || st.Ended != 2000 {
				t.Errorf("stats %+v", st)
			}
			if st.Exported+st.Failed+st.Dropped+st.Queued != st.Ended {
				t.Errorf("accounting does not add up: %+v", st)
			}
			if got := strings.Count(buf.String(), "trace export"); got > 2 {
				t.Errorf("failure logging is not rate limited: %d lines\n%s", got, buf.String())
			}
			if strings.Contains(buf.String(), rcv.srv.URL) || strings.Contains(buf.String(), "127.0.0.1") {
				t.Errorf("log leaks the collector address: %s", buf.String())
			}
		})
	}
}

func TestClosedPortAndRecovery(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: connection refused
	buf := &syncBuf{}
	p, err := Setup(testConfig("http://"+addr), Service{Name: "svc"}, slog.New(slog.NewTextHandler(buf, nil)))
	if err != nil {
		t.Fatalf("a collector that is down must not fail setup: %v", err)
	}
	emit(p, 5)
	waitFor(t, "the failed export", func() bool { return p.Stats().Failed >= 5 })
	if !strings.Contains(buf.String(), "reason=") {
		t.Errorf("expected a logged reason: %s", buf.String())
	}

	// A receiver appears on the same port: later spans arrive and a recovery line is logged.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not reuse the port: %v", err)
	}
	rcv := newFakeReceiver(t)
	_ = rcv.srv.Listener.Close()
	srv2 := httptest.NewUnstartedServer(rcv.srv.Config.Handler)
	_ = srv2.Listener.Close()
	srv2.Listener = ln2
	srv2.Start()
	t.Cleanup(srv2.Close)
	emit(p, 5)
	waitFor(t, "recovery", func() bool { return p.Stats().Exported >= 5 })
	if !strings.Contains(buf.String(), "recovered") {
		t.Errorf("expected a recovery line: %s", buf.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
}

func TestFailureLogIsRateLimited(t *testing.T) {
	buf := &syncBuf{}
	f := newFailureLog(slog.New(slog.NewTextHandler(buf, nil)))
	now := time.Unix(1000, 0)
	f.now = func() time.Time { return now }
	for i := 0; i < 100; i++ {
		f.failure(context.DeadlineExceeded)
	}
	if n := strings.Count(buf.String(), "failing"); n != 1 {
		t.Fatalf("want 1 line for 100 failures in the same instant, got %d", n)
	}
	now = now.Add(61 * time.Second)
	f.failure(context.DeadlineExceeded)
	if n := strings.Count(buf.String(), "trace export"); n != 2 {
		t.Fatalf("want a second line after a minute, got %d\n%s", n, buf.String())
	}
	f.success()
	f.success()
	if n := strings.Count(buf.String(), "recovered"); n != 1 {
		t.Fatalf("want exactly one recovery line, got %d", n)
	}
}

func TestEndingASpanAfterShutdownIsHarmless(t *testing.T) {
	rcv := newFakeReceiver(t)
	p, _ := Setup(testConfig(rcv.srv.URL), Service{Name: "svc"}, nil)
	_, s := p.Tracer().Start(context.Background(), "late")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
	s.End()
	// The SDK stops calling processors after shutdown; the span must simply vanish.
	if st := p.Stats(); st.Ended != 0 || st.Exported != 0 {
		t.Errorf("stats %+v", st)
	}
}

func TestUnsampledSpansAreNotCounted(t *testing.T) {
	rcv := newFakeReceiver(t)
	cfg := testConfig(rcv.srv.URL)
	cfg.SampleRatio = 0
	p, _ := Setup(cfg, Service{Name: "svc"}, nil)
	emit(p, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
	if st := p.Stats(); st.Ended != 0 || rcv.hits.Load() != 0 {
		t.Errorf("stats %+v hits %d", st, rcv.hits.Load())
	}
}

func TestSetupRejectsBadEndpoint(t *testing.T) {
	if _, err := Setup(testConfig("ftp://x"), Service{}, nil); err == nil {
		t.Error("expected an error")
	}
}

func TestShutdownLeavesNoGoroutinesBehind(t *testing.T) {
	rcv := newFakeReceiver(t)
	before := runtimeGoroutines()
	for i := 0; i < 5; i++ {
		p, _ := Setup(testConfig(rcv.srv.URL), Service{Name: "svc"}, nil)
		emit(p, 50)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = p.Shutdown(ctx)
		cancel()
	}
	rcv.srv.CloseClientConnections()
	waitFor(t, "goroutines to return to the baseline", func() bool { return runtimeGoroutines() <= before+1 })
}

func runtimeGoroutines() int { return runtime.NumGoroutine() }

func TestShutdownBoundWithAHungCollector(t *testing.T) {
	rcv := newFakeReceiver(t)
	rcv.setMode("hang")
	p, _ := Setup(testConfig(rcv.srv.URL), Service{Name: "svc"}, nil)
	emit(p, 500)
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("shutdown took %v against a hung collector with a 1s budget", d)
	}
}
