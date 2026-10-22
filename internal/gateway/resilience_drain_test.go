package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/pkg/protocol"
)

// Gateway drain (Phase 15 PR F, D16): on the first shutdown signal /readyz fails at once and responses say
// Connection: close; for the readiness grace the listener stays open and requests are served; then the listener
// closes and in-flight requests finish within what is left of the shutdown timeout.

// drainCfg sets the shutdown timeout and the explicit readiness grace of a test gateway.
func drainCfg(shutdown, grace time.Duration) func(*config.Config) {
	return func(c *config.Config) {
		c.Gateway.ShutdownTimeout = shutdown
		c.Resilience.Drain.ReadinessGrace = &grace
	}
}

// serving starts Serve on a loopback listener and returns what a test needs to signal and observe it.
type serving struct {
	url    string
	addr   string
	cancel context.CancelFunc
	done   chan error
	client *http.Client
}

func (e *resEnv) serve(t *testing.T) *serving {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &serving{url: "http://" + ln.Addr().String(), addr: ln.Addr().String(), cancel: cancel, done: make(chan error, 1),
		client: &http.Client{Transport: &http.Transport{DisableKeepAlives: false}, Timeout: 20 * time.Second}}
	go func() { s.done <- e.gw.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); s.client.CloseIdleConnections() })
	return s
}

func (s *serving) get(path string) (*http.Response, string, error) {
	resp, err := s.client.Get(s.url + path)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b), nil
}

func (s *serving) post(content string) <-chan chatResult {
	out := make(chan chatResult, 1)
	go func() {
		body := `{"model":"qwen-7b","messages":[{"role":"user","content":"` + content + `"}]}`
		resp, err := s.client.Post(s.url+chatCompletionsPath, "application/json", strings.NewReader(body))
		if err != nil {
			out <- chatResult{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		out <- chatResult{resp: resp, body: string(b)}
	}()
	return out
}

func (s *serving) waitDone(t *testing.T, d time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(d):
		t.Fatalf("Serve did not return within %v", d)
	}
	return time.Since(start)
}

func metricValue(t *testing.T, e *resEnv, name string) float64 {
	t.Helper()
	fams, err := e.gw.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			for _, m := range f.GetMetric() {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no series %s", name)
	return 0
}

// The first signal fails /readyz at once, even though the probe result was cached a moment earlier (the 2 s cache is
// bypassed), and the gauge says so; before the signal /readyz is 200 and there is no Connection: close.
func TestDrainFailsReadyzAtOnceBypassingTheCache(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(10*time.Second, 2*time.Second))
	s := e.serve(t)
	resp, body, err := s.get("/readyz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("before the signal: %v %v %s", err, resp, body)
	}
	if resp.Close {
		t.Fatal("a gateway that is not draining must not close connections")
	}
	if metricValue(t, e, "gateway_draining") != 0 {
		t.Fatal("gateway_draining must be 0 before the signal")
	}
	s.cancel()
	var last *http.Response
	var lastBody string
	eventually(t, 200*time.Millisecond, func() bool {
		r, b, err := s.get("/readyz")
		last, lastBody = r, b
		return err == nil && r.StatusCode == 503
	}, "/readyz fails within 200 ms of the signal")
	if errorCode(t, lastBody) != "NO_CAPACITY" || !strings.Contains(lastBody, "shutting down") || last.Header.Get("Retry-After") != "1" {
		t.Fatalf("draining readiness: %d %s retry-after %q", last.StatusCode, lastBody, last.Header.Get("Retry-After"))
	}
	if metricValue(t, e, "gateway_draining") != 1 {
		t.Fatal("gateway_draining must be 1 after the signal")
	}
	if resp, _, err := s.get("/healthz"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("liveness must stay 200 while draining: %v %v", err, resp)
	}
	s.waitDone(t, 5*time.Second)
}

// During the grace the listener is open and requests are served, with Connection: close; at the end of it the
// listener closes and Serve returns.
func TestDrainServesDuringTheGraceThenClosesTheListener(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(10*time.Second, 700*time.Millisecond))
	s := e.serve(t)
	began := time.Now()
	s.cancel()
	eventually(t, 300*time.Millisecond, func() bool { return e.rs.draining.Load() }, "draining set")

	c := s.post("during-grace")
	g.awaitArrivals(t, 1)
	g.release(t, 1)
	r := awaitResult(t, c)
	if r.resp.StatusCode != 200 || !r.resp.Close { // the client folds the Connection: close header into resp.Close
		t.Fatalf("a request in the grace: %d close=%v", r.resp.StatusCode, r.resp.Close)
	}
	if time.Since(began) >= 700*time.Millisecond {
		t.Skip("the machine was too slow to test inside the grace")
	}
	c2, err := net.DialTimeout("tcp", s.addr, time.Second)
	if err != nil {
		t.Fatalf("the listener must be open during the grace: %v", err)
	}
	_ = c2.Close()
	took := s.waitDone(t, 5*time.Second)
	if total := time.Since(began); total < 650*time.Millisecond || total > 3*time.Second {
		t.Fatalf("Serve ended after %v (waited %v), want about the 700 ms grace", total, took)
	}
	if c, err := net.DialTimeout("tcp", s.addr, 200*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatal("the listener is still open after the drain")
	}
}

// A request that is in flight when the signal comes, and outlives the grace, completes: the drain waits for it
// (up to the shutdown timeout) instead of cutting it.
func TestDrainLetsAnInFlightRequestFinishAfterTheGrace(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(10*time.Second, 300*time.Millisecond))
	s := e.serve(t)
	inflight := s.post("long")
	g.awaitArrivals(t, 1)
	s.cancel()
	time.Sleep(900 * time.Millisecond) // past the grace: the listener is closed, Serve is waiting for the request
	select {
	case <-s.done:
		t.Fatal("Serve returned with a request still in flight")
	default:
	}
	g.release(t, 1)
	r := awaitResult(t, inflight)
	if r.resp.StatusCode != 200 {
		t.Fatalf("the in-flight request got %d %s", r.resp.StatusCode, r.body)
	}
	s.waitDone(t, 5*time.Second)
}

// The shutdown timeout covers the grace: a request that never finishes ends the drain at the timeout, not at
// grace plus timeout.
func TestDrainShutdownTimeoutCoversTheGrace(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(1200*time.Millisecond, 500*time.Millisecond))
	s := e.serve(t)
	stuck := s.post("stuck")
	g.awaitArrivals(t, 1)
	began := time.Now()
	s.cancel()
	select {
	case err := <-s.done:
		if err == nil {
			t.Fatal("a drain cut short by the timeout must report it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
	if took := time.Since(began); took < 1000*time.Millisecond || took > 1500*time.Millisecond {
		// A shutdown that restarted the full timeout after the grace would end at about 1.7 s.
		t.Fatalf("Serve ended after %v, want about the 1.2 s shutdown timeout (grace included)", took)
	}
	<-stuck // cut by the timeout; its handler at the worker has gone with the connection
}

// With no grace the drain starts at once: the listener closes immediately, as before, and /readyz is failing.
func TestDrainWithZeroGraceStartsAtOnce(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(5*time.Second, 0))
	s := e.serve(t)
	began := time.Now()
	s.cancel()
	took := s.waitDone(t, 3*time.Second)
	if time.Since(began) > time.Second {
		t.Fatalf("an idle gateway with no grace took %v to stop", took)
	}
	if !e.rs.draining.Load() {
		t.Fatal("draining must be set")
	}
}

// A waiter that is in the queue when the signal comes keeps its place through the grace and is served when a slot
// frees; a request that arrives during the grace may still queue (the queue closes at the end of the grace).
func TestDrainWaitersAndArrivalsKeepQueueingDuringTheGrace(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(8, 20*time.Second), drainCfg(10*time.Second, 1500*time.Millisecond))
	s := e.serve(t)
	held := s.post("held")
	g.awaitArrivals(t, 1)
	waiter := s.post("early")
	e.awaitDepth(t, 1)
	s.cancel()
	eventually(t, time.Second, func() bool { return e.rs.draining.Load() }, "draining")
	late := s.post("late")
	e.awaitDepth(t, 2) // arrived during the grace and queued: shedding it would be a failure the grace exists to avoid
	g.release(t, 1)
	if r := awaitResult(t, held); r.resp.StatusCode != 200 {
		t.Fatalf("held: %d", r.resp.StatusCode)
	}
	g.awaitArrivals(t, 2)
	g.release(t, 1)
	if r := awaitResult(t, waiter); r.resp.StatusCode != 200 {
		t.Fatalf("early waiter: %d %s", r.resp.StatusCode, r.body)
	}
	g.awaitArrivals(t, 3)
	g.release(t, 1)
	if r := awaitResult(t, late); r.resp.StatusCode != 200 {
		t.Fatalf("late waiter: %d %s", r.resp.StatusCode, r.body)
	}
	s.waitDone(t, 5*time.Second)
}

// A waiter that cannot finish is answered before the shutdown budget ends (counted from the signal, grace included),
// not cut by it: the flush happens 250 ms before the end of the budget.
func TestDrainFlushesWaitersBeforeTheBudgetEnds(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(8, 30*time.Second), drainCfg(1600*time.Millisecond, 400*time.Millisecond))
	s := e.serve(t)
	held := s.post("held")
	g.awaitArrivals(t, 1)
	waiter := s.post("waiter")
	e.awaitDepth(t, 1)
	began := time.Now()
	s.cancel()
	r := awaitResult(t, waiter)
	took := time.Since(began)
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || !strings.Contains(r.body, "shutting down") {
		t.Fatalf("the waiter got %d %s after %v, want the draining shed", r.resp.StatusCode, r.body, took)
	}
	if took < 1000*time.Millisecond || took > 1550*time.Millisecond {
		t.Fatalf("the waiter was flushed after %v, want about 1350 ms (1600 ms budget minus the 250 ms margin)", took)
	}
	select {
	case err := <-s.done: // the held request outlives the budget: Serve reports the timeout
		if err == nil {
			t.Fatal("a drain cut short by the budget must say so")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
	<-held
}

// A listener that fails during the grace ends Serve with that error at once (it does not wait out the grace).
func TestDrainListenerFailureDuringTheGraceEndsServe(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(30*time.Second, 20*time.Second))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.gw.Serve(ctx, ln) }()
	cancel()
	eventually(t, time.Second, func() bool { return e.rs.draining.Load() }, "draining")
	_ = ln.Close() // the accept loop fails
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a listener failure must be reported")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve waited out the grace after its listener failed")
	}
}

// What the background work keeps alive (the registry refresh) runs through the grace and the drain and stops only
// when Serve returns, so routing does not go stale while a drain is under way.
func TestDrainKeepsBackgroundWorkRunningUntilServeReturns(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(10*time.Second, 500*time.Millisecond))
	var cancelledAt, returnedAt atomic.Int64
	e.gw.background = func(ctx context.Context) {
		<-ctx.Done()
		cancelledAt.Store(time.Now().UnixNano())
	}
	s := e.serve(t)
	inflight := s.post("x")
	g.awaitArrivals(t, 1)
	s.cancel()
	time.Sleep(900 * time.Millisecond)
	if cancelledAt.Load() != 0 {
		t.Fatal("the background work was stopped by the signal; it must run through the grace and the drain")
	}
	g.release(t, 1)
	awaitResult(t, inflight)
	s.waitDone(t, 5*time.Second)
	returnedAt.Store(time.Now().UnixNano())
	eventually(t, time.Second, func() bool { return cancelledAt.Load() != 0 }, "the background work stops when Serve returns")
}

// With the feature off nothing changes: no drain state, the background work stops at the signal as it always did,
// and the listener closes at once (the readiness grace does not exist).
func TestDrainDoesNothingWithTheFeatureOff(t *testing.T) {
	g := newGateWorker(t, "w1")
	re := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{g.snap(1)}, func(c *config.GatewayConfig) { c.ShutdownTimeout = 3 * time.Second })
	if re.gw.res != nil {
		t.Fatal("the feature must be off")
	}
	var cancelledAt atomic.Int64
	re.gw.background = func(ctx context.Context) { <-ctx.Done(); cancelledAt.Store(time.Now().UnixNano()) }
	e := &resEnv{regEnv: re}
	s := e.serve(t)
	if resp, _, err := s.get("/readyz"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("readyz %v %v", err, resp)
	}
	inflight := s.post("x")
	g.awaitArrivals(t, 1)
	s.cancel()
	eventually(t, time.Second, func() bool { return cancelledAt.Load() != 0 }, "with the feature off the background work stops at the signal, as before")
	if _, err := net.DialTimeout("tcp", s.addr, 300*time.Millisecond); err == nil {
		t.Fatal("with the feature off the listener closes at the signal (no grace)")
	}
	g.release(t, 1)
	if r := awaitResult(t, inflight); r.resp.StatusCode != 200 {
		t.Fatalf("in-flight request: %d", r.resp.StatusCode)
	}
	s.waitDone(t, 5*time.Second)
}

// With no explicit readiness grace the gateway's wiring derives min(5s, shutdown_timeout/2) from its own shutdown
// timeout, and Serve honours it.
func TestDrainDefaultGraceFollowsTheGatewaysShutdownTimeout(t *testing.T) {
	for _, tc := range []struct{ shutdown, want time.Duration }{
		{time.Second, 500 * time.Millisecond}, {4 * time.Second, 2 * time.Second}, {30 * time.Second, 5 * time.Second},
	} {
		g := newGateWorker(t, "w1")
		e := queueEnv(t, g, func(c *config.Config) { c.Gateway.ShutdownTimeout = tc.shutdown })
		if e.rs.grace != tc.want {
			t.Fatalf("shutdown timeout %v: default grace %v, want %v", tc.shutdown, e.rs.grace, tc.want)
		}
	}
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, func(c *config.Config) { c.Gateway.ShutdownTimeout = time.Second })
	s := e.serve(t)
	began := time.Now()
	s.cancel()
	took := s.waitDone(t, 3*time.Second)
	if total := time.Since(began); total < 400*time.Millisecond || total > 900*time.Millisecond {
		t.Fatalf("Serve ended after %v (%v waiting), want about the 500 ms default grace", total, took)
	}
}

// The draining /readyz is a 503 and its access-log line says why: error_code NO_CAPACITY at WARN.
func TestDrainReadyzLogLineCarriesTheErrorCode(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, drainCfg(10*time.Second, 5*time.Second))
	s := e.serve(t)
	s.cancel()
	eventually(t, time.Second, func() bool { return e.rs.draining.Load() }, "draining")
	if resp, _, err := s.get("/readyz"); err != nil || resp.StatusCode != 503 {
		t.Fatalf("readyz: %v %v", err, resp)
	}
	var found bool
	for _, l := range strings.Split(e.logs.String(), "\n") {
		if strings.Contains(l, `"path":"/readyz"`) && strings.Contains(l, `"status":503`) {
			found = true
			if !strings.Contains(l, `"error_code":"NO_CAPACITY"`) || !strings.Contains(l, `"level":"WARN"`) {
				t.Fatalf("the draining /readyz log line lacks error_code NO_CAPACITY at WARN: %s", l)
			}
		}
	}
	if !found {
		t.Fatalf("no access-log line for the draining /readyz:\n%s", e.logs.String())
	}
}
