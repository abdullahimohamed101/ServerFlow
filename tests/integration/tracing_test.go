package integration

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
	"serverflow/internal/tracing/tracingtest"
	"serverflow/internal/worker"
)

// The Phase 11 acceptance runs: a real gateway in registry mode, real agents and mock workers, the gateway
// and every worker with its own tracer provider (so the traceparent really crosses an HTTP hop), spans
// recorded in memory. No collector and no Docker.

type tracedProc struct {
	exp  *tracetest.InMemoryExporter
	rec  *tracetest.SpanRecorder
	prov *tracing.Provider
}

func newTracedProc(t *testing.T, service string, mode tracing.Sampler) *tracedProc {
	t.Helper()
	p := &tracedProc{exp: tracetest.NewInMemoryExporter(), rec: tracetest.NewSpanRecorder()}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(tracing.NewSampler(mode, 1)),
		sdktrace.WithResource(tracing.NewResource(tracing.Service{Name: service, Version: "test", InstanceID: service})),
		sdktrace.WithSyncer(p.exp), sdktrace.WithSpanProcessor(p.rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	p.prov = tracing.NewProvider(tp)
	return p
}

// startTracedNode is startModelNode with the mock worker recording spans into proc.
func (c *controlPlane) startTracedNode(t *testing.T, id string, proc *tracedProc, mutate func(*mockworker.Config)) *countedNode {
	t.Helper()
	mcfg := mockworker.DefaultConfig()
	mcfg.Model, mcfg.WorkerID, mcfg.TTFT, mcfg.TokensPerSecond, mcfg.OutputTokens, mcfg.Seed = model, id, 5*time.Millisecond, 1000, 4, 1
	if mutate != nil {
		mutate(&mcfg)
	}
	n := &countedNode{id: id}
	n.mock = httptest.NewServer(mockworker.New(mcfg, quiet(), mockworker.WithTracing(proc.prov)).Handler())
	t.Cleanup(n.mock.Close)
	agent := worker.New(worker.Config{WorkerID: id, Model: model, AdvertiseURL: n.mock.URL, Interval: hbInterval}, worker.NewMockBackend(n.mock.URL), c.cp, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	n.stop = cancel
	go func() { _ = agent.Run(ctx) }()
	t.Cleanup(cancel)
	return n
}

func startTracedGateway(t *testing.T, c *controlPlane, proc *tracedProc, mutate ...func(*config.Config)) string {
	t.Helper()
	cfg := config.Default()
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Gateway.ControlPlaneURL = c.url
	cfg.Gateway.RegistryRefresh = 25 * time.Millisecond
	cfg.Gateway.RegistryMaxStaleness = 2 * time.Second
	cfg.Gateway.UpstreamHeaderTimeout = 5 * time.Second
	cfg.Worker.SuspectTimeout = c.suspect
	cfg.ControlPlane.Token = testToken
	cfg.Scheduler.Strategy = "round-robin"
	for _, m := range mutate {
		m(&cfg)
	}
	gw, err := gateway.NewRegistry(cfg, quiet(), gateway.WithObserver(gwtrace.New(proc.prov, gwtrace.Options{Incoming: cfg.Tracing.Incoming, IncludeTenantID: true})))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	return "http://" + ln.Addr().String()
}

func tracedPost(t *testing.T, gw string, stream bool, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, gw+"/v1/chat/completions", strings.NewReader(chat(stream, "hello canary-prompt-4d2f")))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func settle(t *testing.T, procs ...*tracedProc) {
	t.Helper()
	waitFor(t, 5*time.Second, "all spans to end", func() bool {
		time.Sleep(30 * time.Millisecond)
		for _, p := range procs {
			if len(p.rec.Started()) != len(p.rec.Ended()) {
				return false
			}
		}
		return true
	})
}

func all(procs ...*tracedProc) tracetest.SpanStubs {
	var out tracetest.SpanStubs
	for _, p := range procs {
		out = append(out, p.exp.GetSpans()...)
	}
	return out
}

func named(spans tracetest.SpanStubs, name string) tracetest.SpanStubs {
	var out tracetest.SpanStubs
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func TestOneTraceAcrossGatewayAndWorker(t *testing.T) {
	c := startControlPlane(t)
	gwp, wp := newTracedProc(t, "serverflow-gateway", tracing.SamplerRatio), newTracedProc(t, "serverflow-mock-worker", tracing.SamplerParentOnly)
	c.startTracedNode(t, "w1", wp, nil)
	gw := startTracedGateway(t, c, gwp)
	waitFor(t, 5*time.Second, "worker eligible", func() bool { return c.eligible("w1") })
	waitReady(t, gw)

	resp, _ := tracedPost(t, gw, true)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	settle(t, gwp, wp)
	spans := all(gwp, wp)

	root := named(spans, "gateway.receive")[0]
	fwd := named(spans, "worker.forward")
	sel := named(spans, "scheduler.select")
	inf := named(spans, "inference")
	qw := named(spans, "queue_wait")
	if len(fwd) != 1 || len(sel) != 1 || len(inf) != 1 || len(qw) != 1 || len(named(spans, "first_token")) != 1 || len(named(spans, "completion")) != 1 {
		t.Fatalf("unexpected spans: %s", tracingtest.Dump(spans))
	}
	for _, s := range spans {
		if s.SpanContext.TraceID() != root.SpanContext.TraceID() {
			t.Errorf("%s is in another trace", s.Name)
		}
	}
	if inf[0].Parent.SpanID() != fwd[0].SpanContext.SpanID() {
		t.Errorf("the worker's inference span must be a child of the gateway's worker.forward span")
	}
	if qw[0].Parent.SpanID() != inf[0].SpanContext.SpanID() {
		t.Error("queue_wait must be a child of inference")
	}
	if sel[0].Parent.SpanID() != root.SpanContext.SpanID() || fwd[0].Parent.SpanID() != root.SpanContext.SpanID() {
		t.Error("scheduler.select and worker.forward are children of the root")
	}
	if !inf[0].Parent.IsRemote() {
		t.Error("the worker's parent crossed a process boundary")
	}
	// Both processes agree on the IDs.
	if got := attrString(inf[0], tracing.KeyAttemptID); got != attrString(fwd[0], tracing.KeyAttemptID) || got == "" {
		t.Errorf("attempt id on the worker span %q, on the gateway span %q", got, attrString(fwd[0], tracing.KeyAttemptID))
	}
	if attrString(inf[0], tracing.KeyRequestID) != resp.Header.Get("X-Request-ID") || attrString(inf[0], tracing.KeyWorkerID) != "w1" || attrString(fwd[0], tracing.KeyWorkerID) != "w1" {
		t.Errorf("ids: %s", tracingtest.Dump(spans))
	}
	tracingtest.RequireAllowListed(t, spans)
	tracingtest.RequireNoCanary(t, spans, "canary-prompt-4d2f", "127.0.0.1")
}

func attrString(s tracetest.SpanStub, k attribute.Key) string {
	for _, kv := range s.Attributes {
		if kv.Key == k {
			return kv.Value.String()
		}
	}
	return ""
}

func TestARetriedRequestShowsTwoWorkerAttemptsInOneTrace(t *testing.T) {
	c := startControlPlane(t)
	gwp := newTracedProc(t, "serverflow-gateway", tracing.SamplerRatio)
	badp, goodp := newTracedProc(t, "serverflow-mock-worker", tracing.SamplerParentOnly), newTracedProc(t, "serverflow-mock-worker", tracing.SamplerParentOnly)
	c.startTracedNode(t, "bad", badp, func(m *mockworker.Config) { m.FailureRate, m.FailureMode = 1, mockworker.ModeUnavailable })
	c.startTracedNode(t, "good", goodp, nil)
	gw := startTracedGateway(t, c, gwp)
	waitFor(t, 5*time.Second, "both eligible", func() bool { return c.eligible("bad") && c.eligible("good") })
	waitReady(t, gw)

	var retried string
	for i := 0; i < 8 && retried == ""; i++ {
		resp, _ := tracedPost(t, gw, i%2 == 0, "Tracestate", "v=canary-state-0b1c")
		if resp.StatusCode != 200 {
			t.Fatalf("a retried request must still succeed, got %d", resp.StatusCode)
		}
		if resp.Header.Get("X-ServerFlow-Attempts") == "2" {
			retried = resp.Header.Get("X-Request-ID")
		}
	}
	if retried == "" {
		t.Fatal("no request needed a retry with round-robin over a failing and a healthy worker")
	}
	settle(t, gwp, badp, goodp)
	spans := all(gwp, badp, goodp)

	// Find the retried request's root by its request ID.
	var root tracetest.SpanStub
	for _, s := range named(spans, "gateway.receive") {
		if attrString(s, tracing.KeyRequestID) == retried {
			root = s
		}
	}
	if !root.SpanContext.IsValid() {
		t.Fatal("no root span for the retried request")
	}
	var mine tracetest.SpanStubs
	for _, s := range spans {
		if s.SpanContext.TraceID() == root.SpanContext.TraceID() {
			mine = append(mine, s)
		}
	}
	fwd, sel, inf := named(mine, "worker.forward"), named(mine, "scheduler.select"), named(mine, "inference")
	if len(fwd) != 2 || len(sel) != 2 || len(inf) != 2 {
		t.Fatalf("want two attempts, two selections and two worker spans:\n%s", tracingtest.Dump(mine))
	}
	if attrString(root, tracing.KeyAttempts) != "2" {
		t.Errorf("attempts attribute %q", attrString(root, tracing.KeyAttempts))
	}
	first, second := fwd[0], fwd[1]
	if attrString(first, tracing.KeyAttempt) != "1" {
		first, second = second, first
	}
	if first.Parent.SpanID() != root.SpanContext.SpanID() || second.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Error("both attempts are siblings under the root")
	}
	if len(second.Links) != 1 || second.Links[0].SpanContext.SpanID() != first.SpanContext.SpanID() || len(first.Links) != 0 {
		t.Errorf("the second attempt must link to the first: first %v second %v", first.Links, second.Links)
	}
	if len(first.Events) != 1 || first.Events[0].Name != "retry" {
		t.Errorf("the failed attempt carries a retry event: %+v", first.Events)
	}
	if attrString(first, tracing.KeyWorkerID) != "bad" || attrString(second, tracing.KeyWorkerID) != "good" || attrString(first, tracing.KeyAttemptClass) != "status_503" {
		t.Errorf("attempt attributes:\n%s", tracingtest.Dump(mine))
	}
	if attrString(first, tracing.KeyAttemptID) == attrString(second, tracing.KeyAttemptID) {
		t.Error("each attempt has its own ID")
	}
	for _, w := range inf {
		parent := first
		if attrString(w, tracing.KeyWorkerID) == "good" {
			parent = second
		}
		if w.Parent.SpanID() != parent.SpanContext.SpanID() || attrString(w, tracing.KeyAttemptID) != attrString(parent, tracing.KeyAttemptID) {
			t.Errorf("worker span %s must be parented to its own attempt", attrString(w, tracing.KeyWorkerID))
		}
	}
	tracingtest.RequireAllowListed(t, spans)
	tracingtest.RequireNoCanary(t, spans, "canary-prompt-4d2f", "canary-state-0b1c", "127.0.0.1")
}

func TestUnsampledRequestsLeaveNoWorkerSpans(t *testing.T) {
	c := startControlPlane(t)
	gwp := &tracedProc{exp: tracetest.NewInMemoryExporter(), rec: tracetest.NewSpanRecorder()}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.NewSampler(tracing.SamplerRatio, 0)), sdktrace.WithSyncer(gwp.exp), sdktrace.WithSpanProcessor(gwp.rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	gwp.prov = tracing.NewProvider(tp)
	wp := newTracedProc(t, "serverflow-mock-worker", tracing.SamplerParentOnly)
	c.startTracedNode(t, "w1", wp, nil)
	gw := startTracedGateway(t, c, gwp)
	waitFor(t, 5*time.Second, "worker eligible", func() bool { return c.eligible("w1") })
	waitReady(t, gw)
	for i := 0; i < 3; i++ {
		if resp, _ := tracedPost(t, gw, true, "Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"); resp.StatusCode != 200 {
			t.Fatal(resp.StatusCode)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(all(gwp, wp)); n != 0 {
		t.Fatalf("ratio 0 must record nothing in either process, got %d spans", n)
	}
}
