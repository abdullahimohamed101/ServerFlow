package worker

import (
	"context"
	"errors"
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

	"serverflow/internal/registry/client"
	"serverflow/pkg/protocol"
)

// fakeClock is the agent's time source in these tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

const readyTimeout = 10 * time.Second

func newStatusAgent(t *testing.T) (*Agent, *fakeBackend, *fakeCP, *fakeClock) {
	t.Helper()
	be := &fakeBackend{status: BackendStatus{State: protocol.StateReady, MaxConcurrency: 4, QueueSize: 32}}
	cp := &fakeCP{}
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	a := New(Config{WorkerID: "secret-worker-id", Model: "qwen-7b", AdvertiseURL: "http://10.9.8.7:9001", Interval: interval, ReadyTimeout: readyTimeout},
		be, cp, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	a.now = clk.Now
	a.st.aliveUntil = clk.Now().Add(3 * interval) // New ran with the real clock
	return a, be, cp, clk
}

func wantStatus(t *testing.T, a *Agent, live, ready bool, readyReason string) {
	t.Helper()
	s := a.Status()
	if s.Live != live || s.Ready != ready || s.ReadyReason != readyReason {
		t.Fatalf("status = %+v, want live=%v ready=%v reason=%q", s, live, ready, readyReason)
	}
}

func TestStatusIsNotReadyBeforeTheFirstRegistration(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	wantStatus(t, a, true, false, reasonUnregistered)
}

func TestStatusIsReadyOnceRegisteredAndHeartbeatAccepted(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	a.Step(context.Background())
	wantStatus(t, a, true, true, "")
}

func TestStatusStaysLiveButNotReadyWhileTheControlPlaneIsDown(t *testing.T) {
	a, _, cp, clk := newStatusAgent(t)
	cp.registerErr = errors.New("connection refused")
	// A whole hour of failing registrations, each step as late as the backoff allows: liveness never fails,
	// because a control plane outage must not make an orchestrator restart every agent.
	for i := 0; i < 400; i++ {
		d := a.Step(context.Background())
		wantStatus(t, a, true, false, reasonUnregistered)
		clk.Advance(d)
		wantStatus(t, a, true, false, reasonUnregistered) // at the moment the next step is due
	}
}

func TestStatusStaysLiveWhileTheControlPlaneRefusesHeartbeats(t *testing.T) {
	a, _, cp, clk := newStatusAgent(t)
	a.Step(context.Background())
	cp.beatErr = errors.New("503")
	for i := 0; i < 50; i++ {
		clk.Advance(a.Step(context.Background()))
		if !a.Status().Live {
			t.Fatalf("step %d: a failing control plane must not fail liveness", i)
		}
	}
	wantStatus(t, a, true, false, reasonStaleBeat)
}

func TestReadinessLapsesOnlyAfterTheReadyTimeoutWithoutAnAcceptedHeartbeat(t *testing.T) {
	a, _, cp, clk := newStatusAgent(t)
	a.Step(context.Background())
	cp.beatErr = errors.New("timeout")
	for elapsed := time.Second; elapsed <= readyTimeout; elapsed += time.Second {
		clk.Advance(time.Second)
		a.Step(context.Background()) // the loop keeps running; the control plane keeps failing
		wantStatus(t, a, true, true, "")
	}
	// Exactly readyTimeout since the last accepted heartbeat: still inside it.
	clk.Advance(time.Millisecond)
	a.Step(context.Background())
	wantStatus(t, a, true, false, reasonStaleBeat)
	cp.beatErr = nil
	a.Step(context.Background())
	wantStatus(t, a, true, true, "") // the control plane is back
}

func TestStatusIsNotReadyWhileTheBackendIsDown(t *testing.T) {
	a, be, _, _ := newStatusAgent(t)
	a.Step(context.Background())
	be.set("", errors.New("connection refused"))
	a.Step(context.Background())
	wantStatus(t, a, true, false, reasonBackendDown) // a FAILED heartbeat is accepted, the pod is still not ready
	be.set(protocol.StateReady, nil)
	a.Step(context.Background())
	wantStatus(t, a, true, true, "")
}

func TestStatusIsNotReadyWhenTheBackendReportsGarbage(t *testing.T) {
	a, be, _, _ := newStatusAgent(t)
	a.Step(context.Background())
	be.set("", errUnusableReport)
	a.Step(context.Background())
	wantStatus(t, a, true, false, reasonBackendDown)
}

func TestStatusIsNotReadyAfterTheRegistrationIsLost(t *testing.T) {
	a, _, cp, _ := newStatusAgent(t)
	a.Step(context.Background())
	cp.beatErr = &client.Error{Status: http.StatusNotFound, Code: "unknown_worker"}
	a.Step(context.Background())
	if a.Registered() {
		t.Fatal("test setup: the registration should have been dropped")
	}
	wantStatus(t, a, true, false, reasonUnregistered)
}

func TestStatusIsNotReadyOnceSuperseded(t *testing.T) {
	a, _, cp, _ := newStatusAgent(t)
	a.Step(context.Background())
	cp.beatErr = &client.Error{Status: http.StatusConflict, Code: "stale_registration"}
	a.Step(context.Background())
	if !a.Superseded() {
		t.Fatal("test setup: the agent should be superseded")
	}
	wantStatus(t, a, true, false, reasonSuperseded)
}

func TestLivenessFailsWhenTheHeartbeatLoopStalls(t *testing.T) {
	a, _, _, clk := newStatusAgent(t)
	a.Step(context.Background())
	clk.Advance(3 * interval)
	wantStatus(t, a, true, true, "") // on the edge: still alive (readiness is within its timeout too)
	clk.Advance(time.Millisecond)
	s := a.Status()
	if s.Live || s.LiveReason != reasonStalled || s.Ready || s.ReadyReason != reasonStalled {
		t.Fatalf("a loop that stopped waking up must fail both probes: %+v", s)
	}
	a.Step(context.Background()) // the loop recovers
	wantStatus(t, a, true, true, "")
}

func TestLivenessBeforeTheFirstStepTolerates3Intervals(t *testing.T) {
	a, _, _, clk := newStatusAgent(t)
	clk.Advance(3 * interval)
	if !a.Status().Live {
		t.Fatal("startup is allowed 3 intervals to finish the first step")
	}
	clk.Advance(time.Millisecond)
	if a.Status().Live {
		t.Fatal("a first step that never finishes is a stall")
	}
}

func TestLivenessAllowsTheBackoffTheLoopAsksFor(t *testing.T) {
	a, be, _, clk := newStatusAgent(t)
	be.set("", errors.New("down"))
	var d time.Duration
	for i := 0; i < 8; i++ {
		d = a.Step(context.Background())
	}
	if d != maxBackoffIntervals*interval {
		t.Fatalf("test setup: expected the maximum backoff, got %v", d)
	}
	clk.Advance(d + 2*interval) // the step is late by two intervals: still alive
	if !a.Status().Live {
		t.Fatal("a loop that is within 2 intervals of the schedule it announced is alive")
	}
	clk.Advance(time.Millisecond)
	if a.Status().Live {
		t.Fatal("more than 2 intervals late is a stall even in backoff")
	}
}

func TestStatusIsNotReadyAsSoonAsRunStopsAndLivenessSurvivesTheShutdown(t *testing.T) {
	a, _, cp, clk := newStatusAgent(t)
	a.Step(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Run has returned; Shutdown (which talks to the control plane) has not started yet.
	wantStatus(t, a, true, false, reasonDraining)
	cp.reset()
	a.Shutdown(context.Background())
	if got := cp.kinds(); got != "heartbeat:DRAINING deregister" {
		t.Fatalf("the shutdown contract is unchanged, got %q", got)
	}
	clk.Advance(time.Hour) // a long shutdown is not a stalled loop
	wantStatus(t, a, true, false, reasonDraining)
}

func TestShutdownOfAnUnregisteredAgentStillMarksItDraining(t *testing.T) {
	a, _, cp, _ := newStatusAgent(t)
	cp.registerErr = errors.New("down")
	a.Step(context.Background())
	a.Shutdown(context.Background())
	wantStatus(t, a, true, false, reasonDraining)
}

func TestStatusIsSafeToReadWhileTheLoopRuns(t *testing.T) {
	a, be, cp, _ := newStatusAgent(t)
	a.now = time.Now // real time for this one
	a.cfg.Interval, a.interval = 5*time.Millisecond, 5*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = a.Run(ctx) }()
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = a.Status()
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		cp.mu.Lock()
		if i%2 == 0 {
			cp.beatErr = errors.New("x")
		} else {
			cp.beatErr = nil
		}
		cp.mu.Unlock()
		be.set(protocol.StateReady, nil)
		time.Sleep(3 * time.Millisecond)
	}
	close(stop)
	readers.Wait()
	cancel()
	wg.Wait()
	a.Shutdown(context.Background())
}

// --- HTTP ------------------------------------------------------------------------------------

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestHandlerMapsStatusToHTTP(t *testing.T) {
	a, _, _, clk := newStatusAgent(t)
	h := StatusHandler(a)
	if r := get(t, h, "GET", "/healthz"); r.Code != 200 {
		t.Fatalf("healthz before registration: %d", r.Code)
	}
	if r := get(t, h, "GET", "/readyz"); r.Code != 503 || strings.TrimSpace(r.Body.String()) != reasonUnregistered {
		t.Fatalf("readyz before registration must be 503: %d %q", r.Code, r.Body.String())
	}
	a.Step(context.Background())
	for _, p := range []string{"/healthz", "/readyz"} {
		r := get(t, h, "GET", p)
		if r.Code != 200 || strings.TrimSpace(r.Body.String()) != "ok" || r.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s after registration: %d %q %v", p, r.Code, r.Body.String(), r.Header())
		}
	}
	if r := get(t, h, "HEAD", "/readyz"); r.Code != 200 {
		t.Fatalf("HEAD: %d", r.Code)
	}
	clk.Advance(time.Minute) // the loop stalls
	for _, p := range []string{"/healthz", "/readyz"} {
		if r := get(t, h, "GET", p); r.Code != 503 {
			t.Fatalf("%s with a stalled loop: %d", p, r.Code)
		}
	}
}

func TestHandlerOnlyServesTheTwoPathsWithGetOrHead(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	a.Step(context.Background())
	h := StatusHandler(a)
	for _, m := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		for _, p := range []string{"/healthz", "/readyz"} {
			if r := get(t, h, m, p); r.Code != http.StatusMethodNotAllowed || r.Header().Get("Allow") == "" {
				t.Errorf("%s %s: %d", m, p, r.Code)
			}
		}
	}
	for _, p := range []string{"/", "/metrics", "/debug/pprof/", "/stats", "/readyz/", "/healthz/x", "/v1/workers"} {
		if r := get(t, h, "GET", p); r.Code != http.StatusNotFound && r.Code != http.StatusMovedPermanently {
			t.Errorf("GET %s: %d", p, r.Code)
		}
	}
}

func TestHandlerNeverRevealsIdentityAddressesOrIDs(t *testing.T) {
	a, be, cp, clk := newStatusAgent(t)
	h := StatusHandler(a)
	check := func() {
		for _, p := range []string{"/healthz", "/readyz"} {
			r := get(t, h, "GET", p)
			body := r.Body.String() + r.Header().Get("Content-Type")
			for k, vs := range r.Header() {
				body += k + strings.Join(vs, ",")
			}
			for _, secret := range []string{"secret-worker-id", "qwen", "10.9.8.7", "reg_"} {
				if strings.Contains(body, secret) {
					t.Fatalf("%s leaks %q: %s", p, secret, body)
				}
			}
			if len(r.Body.String()) > 64 {
				t.Fatalf("%s body should be one short line: %q", p, r.Body.String())
			}
		}
	}
	check()
	a.Step(context.Background())
	check()
	be.set("", errors.New("dial tcp 10.9.8.7:1: refused secret-worker-id"))
	a.Step(context.Background())
	check()
	cp.beatErr = errors.New("Bearer topsecrettoken")
	be.set(protocol.StateReady, nil)
	clk.Advance(time.Minute)
	a.Step(context.Background())
	check()
}

// --- the server --------------------------------------------------------------------------------

func fetch(t *testing.T, url string) (int, string) {
	t.Helper()
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestStartStatusServerServesOnALoopbackListener(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	s, err := StartStatusServer("127.0.0.1:0", a, quiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	host, _, _ := net.SplitHostPort(s.Addr())
	if !net.ParseIP(host).IsLoopback() {
		t.Fatalf("not loopback: %s", s.Addr())
	}
	if code, _ := fetch(t, "http://"+s.Addr()+"/healthz"); code != 200 {
		t.Fatalf("healthz: %d", code)
	}
	if code, body := fetch(t, "http://"+s.Addr()+"/readyz"); code != 503 || body != reasonUnregistered {
		t.Fatalf("readyz: %d %q", code, body)
	}
	a.Step(context.Background())
	if code, _ := fetch(t, "http://"+s.Addr()+"/readyz"); code != 200 {
		t.Fatalf("readyz after registration: %d", code)
	}
}

func TestStartStatusServerRefusesAnAddressThatIsNotLoopback(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	for _, addr := range []string{"0.0.0.0:0", ":0", "[::]:0"} {
		s, err := StartStatusServer(addr, a, quiet())
		if err == nil {
			s.Shutdown(context.Background())
			t.Errorf("%q must be refused", addr)
		} else if !errors.Is(err, ErrNotLoopback) {
			t.Errorf("%q: unexpected error %v", addr, err)
		}
	}
}

func TestStartStatusServerReportsAPortThatIsTaken(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	a, _, _, _ := newStatusAgent(t)
	if s, err := StartStatusServer(ln.Addr().String(), a, quiet()); err == nil {
		s.Shutdown(context.Background())
		t.Fatal("a taken port must be an error, not a silent no-op")
	}
}

func TestStatusServerShutdownIsBoundedByItsContext(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	s, err := StartStatusServer("127.0.0.1:0", a, quiet())
	if err != nil {
		t.Fatal(err)
	}
	// A client that sends half a request and stalls keeps its connection active.
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write([]byte("GET /readyz HTTP/1.1\r\nHost: x\r\n"))
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.Shutdown(ctx)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("shutdown took %v with a stalled client; it must give up at its context", took)
	}
	if _, err := net.DialTimeout("tcp", s.Addr(), 200*time.Millisecond); err == nil {
		t.Fatal("the listener must be closed")
	}
}

// blockingCP makes Deregister wait, to observe the endpoint during the agent's shutdown.
type blockingCP struct {
	*fakeCP
	entered chan struct{}
	release chan struct{}
}

func (b *blockingCP) Deregister(ctx context.Context, id, reg string) error {
	close(b.entered)
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return b.fakeCP.Deregister(ctx, id, reg)
}

// The endpoint keeps answering (503 "draining") for the whole of the agent's graceful shutdown, and is closed last,
// without lengthening the shutdown.
func TestStatusEndpointAnswersDrainingThroughoutShutdownAndClosesLast(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	bcp := &blockingCP{fakeCP: &fakeCP{}, entered: make(chan struct{}), release: make(chan struct{})}
	a.cp = bcp
	s, err := StartStatusServer("127.0.0.1:0", a, quiet())
	if err != nil {
		t.Fatal(err)
	}
	a.Step(context.Background())
	if code, _ := fetch(t, "http://"+s.Addr()+"/readyz"); code != 200 {
		t.Fatalf("setup: ready expected, got %d", code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = a.Run(ctx); close(runDone) }()
	cancel() // SIGTERM
	<-runDone

	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	var shutdownDone atomic.Bool
	go func() { a.Shutdown(sctx); shutdownDone.Store(true) }()
	<-bcp.entered // the agent is inside Deregister
	if shutdownDone.Load() {
		t.Fatal("test setup: shutdown should be blocked")
	}
	if code, body := fetch(t, "http://"+s.Addr()+"/readyz"); code != 503 || body != reasonDraining {
		t.Fatalf("during the shutdown readyz must say draining: %d %q", code, body)
	}
	if code, _ := fetch(t, "http://"+s.Addr()+"/healthz"); code != 200 {
		t.Fatalf("during the shutdown healthz stays 200: %d", code)
	}
	close(bcp.release)
	deadline := time.Now().Add(2 * time.Second)
	for !shutdownDone.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !shutdownDone.Load() {
		t.Fatal("agent shutdown did not finish")
	}
	start := time.Now()
	s.Shutdown(sctx)
	if time.Since(start) > time.Second {
		t.Fatalf("closing the idle endpoint took %v", time.Since(start))
	}
	if _, err := net.DialTimeout("tcp", s.Addr(), 200*time.Millisecond); err == nil {
		t.Fatal("the endpoint must be closed at the end")
	}
}

func TestStatusServerRefusesOversizedHeaders(t *testing.T) {
	a, _, _, _ := newStatusAgent(t)
	s, err := StartStatusServer("127.0.0.1:0", a, quiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	req, _ := http.NewRequest("GET", "http://"+s.Addr()+"/healthz", nil)
	req.Header.Set("X-Padding", strings.Repeat("a", 64<<10))
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return // the server may also just drop the connection
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("a 64 KiB header must be refused, got %d", resp.StatusCode)
	}
}
