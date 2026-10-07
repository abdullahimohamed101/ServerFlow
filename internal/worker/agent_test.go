package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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

type fakeBackend struct {
	mu     sync.Mutex
	status BackendStatus
	err    error
}

func (b *fakeBackend) Status(context.Context) (BackendStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.status, b.err
}

func (b *fakeBackend) set(s protocol.WorkerState, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status.State, b.err = s, err
}

type call struct {
	kind  string // register, heartbeat, deregister
	state protocol.WorkerState
	reg   string
	info  protocol.WorkerInfo
	hb    protocol.Heartbeat
}

type fakeCP struct {
	mu          sync.Mutex
	calls       []call
	regCount    int
	registerErr error
	beatErr     error
	deregErr    error
	intervalSec float64
}

func (c *fakeCP) Register(_ context.Context, info protocol.WorkerInfo) (protocol.RegisterResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{kind: "register", info: info})
	if c.registerErr != nil {
		return protocol.RegisterResponse{}, c.registerErr
	}
	c.regCount++
	return protocol.RegisterResponse{RegistrationID: fmt.Sprintf("reg_%d", c.regCount), HeartbeatIntervalSeconds: c.intervalSec}, nil
}

func (c *fakeCP) Heartbeat(_ context.Context, id string, hb protocol.Heartbeat) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{kind: "heartbeat", state: hb.State, reg: hb.RegistrationID, hb: hb})
	return c.beatErr
}

func (c *fakeCP) Deregister(_ context.Context, id, reg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{kind: "deregister", reg: reg})
	return c.deregErr
}

func (c *fakeCP) kinds() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, k := range c.calls {
		s := k.kind
		if k.kind == "heartbeat" {
			s += ":" + string(k.state)
		}
		out = append(out, s)
	}
	return strings.Join(out, " ")
}

func (c *fakeCP) reset() {
	c.mu.Lock()
	c.calls = nil
	c.mu.Unlock()
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

const interval = 2 * time.Second

func newAgent(t *testing.T) (*Agent, *fakeBackend, *fakeCP, *lockedBuf) {
	t.Helper()
	be := &fakeBackend{status: BackendStatus{State: protocol.StateReady, MaxConcurrency: 4, QueueSize: 32,
		Metrics: protocol.Metrics{ActiveRequests: 1, QueueDepth: 2, QueuedInputTokens: 30, RecentTokensPerSecond: 88}}}
	cp := &fakeCP{}
	logs := &lockedBuf{}
	a := New(Config{WorkerID: "w1", Model: "qwen-7b", AdvertiseURL: "http://127.0.0.1:9001", Interval: interval}, be, cp, slog.New(slog.NewJSONHandler(logs, nil)))
	return a, be, cp, logs
}

// --- registration and heartbeats ---------------------------------------------------------

func TestFirstStepRegistersThenHeartbeatsImmediately(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	if d := a.Step(context.Background()); d != interval {
		t.Fatalf("the next step is one interval away, got %v", d)
	}
	if got := cp.kinds(); got != "register heartbeat:READY" {
		t.Fatalf("calls: %q", got)
	}
	reg := cp.calls[0].info
	if reg.WorkerID != "w1" || reg.Model != "qwen-7b" || reg.Address != "http://127.0.0.1:9001" || reg.MaxConcurrency != 4 || reg.QueueSize != 32 {
		t.Fatalf("registration must carry the worker's identity, advertised address and the backend's capacity: %+v", reg)
	}
	hb := cp.calls[1].hb
	if hb.RegistrationID != "reg_1" || hb.Metrics.QueueDepth != 2 || hb.Metrics.RecentTokensPerSecond != 88 || hb.Metrics.ActiveRequests != 1 {
		t.Fatalf("the heartbeat must carry the registration and the backend's load: %+v", hb)
	}
	a.Step(context.Background())
	if got := cp.kinds(); got != "register heartbeat:READY heartbeat:READY" {
		t.Fatalf("a registered agent only heartbeats: %q", got)
	}
}

func TestStatesAreForwardedFromTheBackend(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	for _, st := range []protocol.WorkerState{protocol.StateLoadingModel, protocol.StateWarming, protocol.StateReady, protocol.StateDraining} {
		be.set(st, nil)
		a.Step(context.Background())
		if last := cp.calls[len(cp.calls)-1]; last.kind != "heartbeat" || last.state != st {
			t.Fatalf("backend %s was reported as %+v", st, last)
		}
	}
}

func TestCapacityIsClampedToWhatARegistrationAccepts(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	be.status.MaxConcurrency, be.status.QueueSize = 0, -5 // a backend that does not report capacity
	a.Step(context.Background())
	if got := cp.calls[0].info; got.MaxConcurrency != 1 || got.QueueSize != 0 {
		t.Fatalf("got %+v", got)
	}
	if err := cp.calls[0].info.Validate(); err != nil {
		t.Fatalf("the registration must be valid: %v", err)
	}
}

func TestTheControlPlanesHeartbeatIntervalTakesOver(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	cp.intervalSec = 0.5
	if d := a.Step(context.Background()); d != 500*time.Millisecond {
		t.Fatalf("the agent must follow the control plane's interval, got %v", d)
	}
}

// --- waiting for the backend and backing off -----------------------------------------------

func TestNothingIsRegisteredWhileTheBackendIsDown(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	be.set("", errors.New("connection refused"))
	var delays []time.Duration
	for i := 0; i < 6; i++ {
		delays = append(delays, a.Step(context.Background()))
	}
	if cp.kinds() != "" {
		t.Fatalf("an unreachable backend must not be registered: %q", cp.kinds())
	}
	want := []time.Duration{interval, 2 * interval, 4 * interval, 8 * interval, 10 * interval, 10 * interval}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("retry delays %v, want %v (doubling, capped at 10 intervals)", delays, want)
		}
	}
	be.set(protocol.StateReady, nil)
	a.Step(context.Background())
	if got := cp.kinds(); got != "register heartbeat:READY" {
		t.Fatalf("once the backend answers the worker registers: %q", got)
	}
	if d := a.Step(context.Background()); d != interval {
		t.Fatalf("backoff must reset after success, got %v", d)
	}
}

func TestRegistrationFailuresBackOffAndRecover(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	cp.registerErr = errors.New("connection refused")
	if d1, d2 := a.Step(context.Background()), a.Step(context.Background()); d1 != interval || d2 != 2*interval {
		t.Fatalf("got %v then %v", d1, d2)
	}
	if a.Registered() {
		t.Fatal("not registered")
	}
	cp.registerErr = nil
	a.Step(context.Background())
	if !a.Registered() || !strings.Contains(cp.kinds(), "heartbeat:READY") {
		t.Fatalf("it must register as soon as the control plane is back: %q", cp.kinds())
	}
	if strings.Count(logs.String(), "registration failed") != 1 {
		t.Fatalf("repeated failures must not flood the log: %s", logs.String())
	}
}

func TestARejectedTokenIsReportedClearly(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	cp.registerErr = &client.Error{Status: 401, Code: "unauthorized", Message: "a valid bearer token is required"}
	a.Step(context.Background())
	if a.Registered() || !strings.Contains(logs.String(), "control_plane.token") {
		t.Fatalf("a token problem must be called out, got: %s", logs.String())
	}
}

// --- the backend failing and recovering --------------------------------------------------------

func TestADeadBackendIsReportedFailedAtOnceAndRecoveryFollows(t *testing.T) {
	a, be, cp, logs := newAgent(t)
	a.Step(context.Background())
	cp.reset()

	be.set("", errors.New("connection refused"))
	a.Step(context.Background())
	a.Step(context.Background())
	if got := cp.kinds(); got != "heartbeat:FAILED heartbeat:FAILED" {
		t.Fatalf("a dead backend must be reported FAILED at once, not left to time out: %q", got)
	}
	if hb := cp.calls[0].hb; hb.Reason != "backend unreachable" || hb.Metrics != (protocol.Metrics{}) {
		t.Fatalf("a failed heartbeat carries a reason and no stale load: %+v", hb)
	}
	if strings.Count(logs.String(), "backend unreachable") != 1 {
		t.Fatalf("the outage must be logged once, not every tick: %s", logs.String())
	}

	be.set(protocol.StateLoadingModel, nil) // the backend restarted
	a.Step(context.Background())
	be.set(protocol.StateReady, nil)
	a.Step(context.Background())
	if got := cp.kinds(); got != "heartbeat:FAILED heartbeat:FAILED heartbeat:LOADING_MODEL heartbeat:READY" {
		t.Fatalf("recovery: %q", got)
	}
	if !strings.Contains(logs.String(), "backend reachable again") {
		t.Fatal("the recovery must be logged")
	}
}

func TestABackendThatDrainedAndExitedIsDeregisteredNotFailed(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	a.Step(context.Background())
	be.set(protocol.StateDraining, nil)
	a.Step(context.Background())
	cp.reset()

	be.set("", errors.New("connection refused")) // the drained backend exited
	a.Step(context.Background())
	if got := cp.kinds(); got != "deregister" || a.Registered() {
		t.Fatalf("a graceful end is a deregistration, not FAILED: %q registered=%v", got, a.Registered())
	}
	if cp.calls[0].reg != "reg_1" {
		t.Fatalf("it must present its registration: %+v", cp.calls[0])
	}
	cp.reset()
	a.Step(context.Background())
	if cp.kinds() != "" {
		t.Fatalf("it waits for the backend to return: %q", cp.kinds())
	}
	be.set(protocol.StateReady, nil)
	a.Step(context.Background())
	if got := cp.kinds(); got != "register heartbeat:READY" || cp.calls[1].reg != "reg_2" {
		t.Fatalf("a restarted backend registers as a new incarnation: %q", got)
	}
}

// --- the control plane misbehaving or restarting --------------------------------------------------

func TestUnknownWorkerMeansReRegisterImmediately(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.Step(context.Background())
	cp.reset()

	cp.beatErr = &client.Error{Status: 404, Code: "unknown_worker"} // the control plane restarted
	if d := a.Step(context.Background()); d != 0 || a.Registered() {
		t.Fatalf("the first lost registration is retried at once, got delay %v registered=%v", d, a.Registered())
	}
	cp.beatErr = nil
	a.Step(context.Background())
	if got := cp.kinds(); got != "heartbeat:READY register heartbeat:READY" || cp.calls[2].reg != "reg_2" {
		t.Fatalf("it must re-register and heartbeat in the same step: %q", got)
	}
}

func TestAnIllegalTransitionMeansReRegister(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.Step(context.Background())
	cp.beatErr = &client.Error{Status: 409, Code: "illegal_transition"} // e.g. the backend restarted in place
	a.Step(context.Background())
	if a.Registered() {
		t.Fatal("an illegal transition must drop the registration so the worker starts a fresh incarnation")
	}
}

func TestRepeatedLostRegistrationsDoNotSpin(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.Step(context.Background())
	cp.beatErr = &client.Error{Status: 404, Code: "unknown_worker"} // a control plane that always forgets us
	delays := []time.Duration{}
	for i := 0; i < 4; i++ {
		delays = append(delays, a.Step(context.Background()))
	}
	if delays[0] != 0 {
		t.Fatalf("the first retry is immediate: %v", delays)
	}
	for _, d := range delays[1:] {
		if d != interval {
			t.Fatalf("after the first, a flapping registration must wait a full interval: %v", delays)
		}
	}
}

func TestNetworkErrorsOnHeartbeatKeepTheRegistration(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	a.Step(context.Background())
	cp.reset()
	cp.beatErr = errors.New("connection refused")
	for i := 0; i < 3; i++ {
		if d := a.Step(context.Background()); d != interval {
			t.Fatalf("got %v", d)
		}
	}
	if !a.Registered() || strings.Contains(cp.kinds(), "register ") && !strings.HasPrefix(cp.kinds(), "heartbeat") {
		t.Fatalf("a network blip must not drop the registration: %q", cp.kinds())
	}
	if strings.Contains(cp.kinds(), "register") {
		t.Fatalf("it must not re-register on a network error: %q", cp.kinds())
	}
	if strings.Count(logs.String(), "heartbeat failed") != 1 {
		t.Fatalf("a long outage must not flood the log: %s", logs.String())
	}
	cp.beatErr = nil
	a.Step(context.Background())
	if !strings.HasSuffix(cp.kinds(), "heartbeat:READY") {
		t.Fatalf("it keeps heartbeating when the control plane returns: %q", cp.kinds())
	}
}

// --- shutdown and crash ---------------------------------------------------------------------------------

func TestShutdownReportsDrainingThenDeregisters(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.Step(context.Background())
	cp.reset()
	a.Shutdown(context.Background())
	if got := cp.kinds(); got != "heartbeat:DRAINING deregister" {
		t.Fatalf("calls: %q", got)
	}
	if cp.calls[0].reg != "reg_1" || cp.calls[1].reg != "reg_1" || a.Registered() {
		t.Fatalf("%+v registered=%v", cp.calls, a.Registered())
	}
}

func TestShutdownWhenNotRegisteredDoesNothingAndToleratesFailures(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.Shutdown(context.Background())
	if cp.kinds() != "" {
		t.Fatalf("%q", cp.kinds())
	}
	a.Step(context.Background())
	cp.reset()
	cp.beatErr, cp.deregErr = errors.New("down"), errors.New("down")
	a.Shutdown(context.Background()) // must not panic or hang
	if got := cp.kinds(); got != "heartbeat:DRAINING deregister" {
		t.Fatalf("it still tries to deregister after a failed drain report: %q", got)
	}
}

func TestRunStepsUntilCancelledAndLeavesTheRegistrationToExpire(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.interval = 5 * time.Millisecond
	cp.intervalSec = 0.005
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cp.mu.Lock()
		n := len(cp.calls)
		cp.mu.Unlock()
		if n >= 6 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	if strings.Contains(cp.kinds(), "deregister") {
		t.Fatalf("a cancelled Run simulates a crash and must not deregister: %q", cp.kinds())
	}
	if !strings.HasPrefix(cp.kinds(), "register heartbeat:READY heartbeat:READY") {
		t.Fatalf("Run should have registered and kept heartbeating: %q", cp.kinds())
	}
}

// --- the mock backend --------------------------------------------------------------------------------------

func statsServer(t *testing.T, status int, body string) *MockBackend {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stats" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)
	return NewMockBackend(ts.URL)
}

func TestMockBackendMapsStatusAndLoad(t *testing.T) {
	for status, want := range map[string]protocol.WorkerState{
		"starting": protocol.StateLoadingModel, "ready": protocol.StateReady, "draining": protocol.StateDraining,
	} {
		b := statsServer(t, 200, `{"status":"`+status+`","active_requests":3,"queue_depth":4,"queued_input_tokens":50,"recent_tokens_per_second":61.5,"max_concurrency":8,"queue_size":16,"extra":"ignored"}`)
		got, err := b.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got.State != want || got.MaxConcurrency != 8 || got.QueueSize != 16 ||
			got.Metrics != (protocol.Metrics{ActiveRequests: 3, QueueDepth: 4, QueuedInputTokens: 50, RecentTokensPerSecond: 61.5}) {
			t.Fatalf("%s -> %+v", status, got)
		}
	}
}

func TestMockBackendErrors(t *testing.T) {
	for name, b := range map[string]*MockBackend{
		"server error":   statsServer(t, 500, `{}`),
		"not found":      statsServer(t, 404, `nope`),
		"invalid json":   statsServer(t, 200, `{nope`),
		"unknown status": statsServer(t, 200, `{"status":"exploding"}`),
		"missing status": statsServer(t, 200, `{}`),
		"huge response":  statsServer(t, 200, `{"status":"ready","pad":"`+strings.Repeat("x", 2<<20)+`"}`),
	} {
		_, err := b.Status(context.Background())
		if err == nil {
			t.Errorf("%s: expected an error", name)
		}
		if bad := name == "unknown status" || name == "missing status"; bad != errors.Is(err, errUnusableReport) {
			t.Errorf("%s: unusable-report classification is wrong: %v", name, err)
		}
	}
	down := httptest.NewServer(http.NotFoundHandler())
	url := down.URL
	down.Close()
	if _, err := NewMockBackend(url).Status(context.Background()); err == nil {
		t.Error("an unreachable backend must be an error")
	}
}

func TestMockBackendDoesNotFollowRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"status":"ready"}`)
	}))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/stats", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)
	if _, err := NewMockBackend(redirector.URL).Status(context.Background()); err == nil {
		t.Fatal("a redirecting backend must not be believed")
	}
}

func TestMockBackendHonorsTheContext(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(slow.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := NewMockBackend(slow.URL).Status(ctx); err == nil || time.Since(start) > time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

// --- independent review and verification findings ------------------------------------------------

func TestASupersededAgentStopsInsteadOfStealingTheIdentityBack(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	a.Step(context.Background())
	cp.reset()

	// Another process registered under our worker ID: our heartbeat is stale.
	cp.beatErr = &client.Error{Status: 409, Code: "stale_registration"}
	if d := a.Step(context.Background()); d != 0 {
		t.Fatalf("got %v", d)
	}
	if !a.Superseded() || a.Registered() {
		t.Fatal("the agent must recognise it was superseded and drop its registration")
	}
	if got := cp.kinds(); got != "heartbeat:READY" {
		t.Fatalf("a superseded agent must not register again (that steals the identity and starts a fight): %q", got)
	}
	if !strings.Contains(logs.String(), "superseded") || !strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Fatalf("the operator must be told, loudly: %s", logs.String())
	}
	// Even if asked to keep stepping, it stays quiet.
	cp.reset()
	for i := 0; i < 3; i++ {
		a.Step(context.Background())
	}
	if cp.kinds() != "" {
		t.Fatalf("a superseded agent must make no further control plane calls: %q", cp.kinds())
	}
	// And a graceful shutdown must not deregister someone else's registration.
	a.Shutdown(context.Background())
	if cp.kinds() != "" {
		t.Fatalf("shutdown after being superseded must not touch the registry: %q", cp.kinds())
	}
}

func TestRunReturnsErrSupersededAndDoesNotLoop(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.interval = 5 * time.Millisecond
	cp.intervalSec = 0.005
	cp.beatErr = &client.Error{Status: 409, Code: "stale_registration"}
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrSuperseded) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop for a superseded agent")
	}
	cp.mu.Lock()
	registers := cp.regCount
	cp.mu.Unlock()
	if registers != 1 {
		t.Fatalf("a superseded agent must register exactly once, registered %d times", registers)
	}
}

func TestOutOfRangeHeartbeatIntervalsAreClamped(t *testing.T) {
	// cfg interval is 2s: the accepted range is [200ms, 20s].
	for name, tc := range map[string]struct {
		sec  float64
		want time.Duration
	}{
		"1ns (a hot loop)": {1e-9, 200 * time.Millisecond}, "1us": {1e-6, 200 * time.Millisecond}, "10ms": {0.01, 200 * time.Millisecond},
		"just under the floor": {0.199, 200 * time.Millisecond}, "the floor": {0.2, 200 * time.Millisecond},
		"in range": {1.5, 1500 * time.Millisecond}, "the ceiling": {20, 20 * time.Second},
		"just over the ceiling": {20.5, 20 * time.Second}, "huge (stops heartbeating)": {1e300, 20 * time.Second}, "an hour": {3600, 20 * time.Second},
	} {
		a, _, cp, _ := newAgent(t)
		cp.intervalSec = tc.sec
		if d := a.Step(context.Background()); d != tc.want {
			t.Errorf("%s: control plane said %vs, the agent used %v, want %v", name, tc.sec, d, tc.want)
		}
	}
	for name, sec := range map[string]float64{"zero": 0, "negative": -5, "NaN": math.NaN(), "+Inf": math.Inf(1), "-Inf": math.Inf(-1)} {
		a, _, cp, _ := newAgent(t)
		cp.intervalSec = sec
		if d := a.Step(context.Background()); d != interval && !(sec > 1e9) {
			t.Errorf("%s: an unusable interval must be ignored (keep %v), got %v", name, interval, d)
		}
	}
}

func TestUnusableBackendDataIsReportedFailedNotSilentlyRetried(t *testing.T) {
	for name, mutate := range map[string]func(*BackendStatus){
		"negative queue depth": func(s *BackendStatus) { s.Metrics.QueueDepth = -3 },
		"absurd counter":       func(s *BackendStatus) { s.Metrics.QueuedInputTokens = 1 << 50 },
		"NaN throughput":       func(s *BackendStatus) { s.Metrics.RecentTokensPerSecond = math.NaN() },
		"infinite throughput":  func(s *BackendStatus) { s.Metrics.RecentTokensPerSecond = math.Inf(1) },
		"huge concurrency":     func(s *BackendStatus) { s.MaxConcurrency = 1 << 30 },
		"huge queue size":      func(s *BackendStatus) { s.QueueSize = 1 << 30 },
	} {
		t.Run(name, func(t *testing.T) {
			a, be, cp, logs := newAgent(t)
			a.Step(context.Background()) // registered and healthy
			cp.reset()
			mutate(&be.status)
			a.Step(context.Background())
			if got := cp.kinds(); got != "heartbeat:FAILED" {
				t.Fatalf("a backend that reports nonsense must be reported FAILED (not retried until the worker is declared LOST): %q", got)
			}
			if hb := cp.calls[0].hb; hb.Reason != "backend reported unusable data" {
				t.Fatalf("the reason must say why: %+v", hb)
			}
			if !strings.Contains(logs.String(), "unusable") {
				t.Fatalf("it must be logged: %s", logs.String())
			}
		})
	}
	// Before registration the worker is simply not registered.
	a, be, cp, _ := newAgent(t)
	be.status.Metrics.QueueDepth = -1
	a.Step(context.Background())
	if cp.kinds() != "" {
		t.Fatalf("a backend with unusable data must not be registered: %q", cp.kinds())
	}
}

func TestARejectedHeartbeatIsLoggedAsAnError(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	a.Step(context.Background())
	cp.beatErr = &client.Error{Status: 400, Code: "invalid_request", Message: "queue_depth is out of range"}
	for i := 0; i < 3; i++ {
		a.Step(context.Background())
	}
	if !a.Registered() {
		t.Fatal("a 400 is not a lost registration")
	}
	if !strings.Contains(logs.String(), `"level":"ERROR"`) || !strings.Contains(logs.String(), "rejected") {
		t.Fatalf("a heartbeat the control plane calls invalid is a bug, not a blip: %s", logs.String())
	}
}

// slowBackend answers after a delay, like a backend that is busy.
type slowBackend struct {
	fakeBackend
	delay time.Duration
}

func (b *slowBackend) Status(ctx context.Context) (BackendStatus, error) {
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
		return BackendStatus{}, ctx.Err()
	}
	return b.fakeBackend.Status(ctx)
}

func TestHeartbeatCadenceDoesNotDriftWithProbeLatency(t *testing.T) {
	be := &slowBackend{fakeBackend: fakeBackend{status: BackendStatus{State: protocol.StateReady, MaxConcurrency: 1}}, delay: 40 * time.Millisecond}
	cp := &fakeCP{intervalSec: 0.1}
	a := New(Config{WorkerID: "w1", Model: "m", AdvertiseURL: "http://127.0.0.1:1", Interval: 100 * time.Millisecond}, be, cp, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 1050*time.Millisecond)
	defer cancel()
	_ = a.Run(ctx)
	cp.mu.Lock()
	beats := 0
	for _, c := range cp.calls {
		if c.kind == "heartbeat" {
			beats++
		}
	}
	cp.mu.Unlock()
	// A period of interval+probe (140ms) would give about 7; a steady 100ms gives about 10.
	if beats < 9 {
		t.Fatalf("heartbeats drifted: %d in ~1s with a 100ms interval and a 40ms probe", beats)
	}
}

func TestAStalledBackendCannotStallHeartbeatsBeyondTheInterval(t *testing.T) {
	be := &slowBackend{fakeBackend: fakeBackend{status: BackendStatus{State: protocol.StateReady, MaxConcurrency: 1}}, delay: time.Hour}
	cp := &fakeCP{}
	a := New(Config{WorkerID: "w1", Model: "m", AdvertiseURL: "http://127.0.0.1:1", Interval: 100 * time.Millisecond}, be, cp, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	start := time.Now()
	done := make(chan struct{})
	go func() { a.Step(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a hung backend probe must be cut off by a timeout; the step is still stuck after 3s")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a hung backend probe must be cut off by a timeout, took %v", took)
	}
}

// --- state flags the mutation testing showed were unpinned -----------------------------------------------

func TestTheSpinGuardResetsAfterASuccessfulHeartbeat(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	a.Step(context.Background())
	for i := 0; i < 3; i++ {
		cp.beatErr = &client.Error{Status: 404, Code: "unknown_worker"}
		if d := a.Step(context.Background()); d != 0 {
			t.Fatalf("round %d: the first loss after a healthy period is retried at once, got %v", i, d)
		}
		cp.beatErr = nil
		a.Step(context.Background()) // re-registers and heartbeats successfully
	}
}

func TestBackoffRestartsFromOneIntervalAfterASuccess(t *testing.T) {
	a, _, cp, _ := newAgent(t)
	cp.registerErr = errors.New("down")
	a.Step(context.Background())
	a.Step(context.Background())
	if d := a.Step(context.Background()); d != 4*interval {
		t.Fatalf("setup: %v", d)
	}
	cp.registerErr = nil
	a.Step(context.Background()) // registers
	cp.beatErr = &client.Error{Status: 404, Code: "unknown_worker"}
	a.Step(context.Background()) // loses it
	cp.beatErr = nil
	cp.registerErr = errors.New("down again")
	if d := a.Step(context.Background()); d != interval {
		t.Fatalf("the backoff must start over after a successful registration, got %v", d)
	}
}

func TestFailureLoggingRestartsAfterASuccess(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	a.Step(context.Background())
	cp.beatErr = errors.New("down")
	a.Step(context.Background())
	a.Step(context.Background())
	cp.beatErr = nil
	a.Step(context.Background())
	cp.beatErr = errors.New("down again")
	a.Step(context.Background())
	if n := strings.Count(logs.String(), "heartbeat failed"); n != 2 {
		t.Fatalf("each new outage is logged once (first failure), got %d lines", n)
	}
}

func TestDeregistrationToleratesAnAlreadyGoneWorkerQuietly(t *testing.T) {
	for _, e := range []error{&client.Error{Status: 404, Code: "unknown_worker"}, &client.Error{Status: 409, Code: "stale_registration"}} {
		a, _, cp, logs := newAgent(t)
		a.Step(context.Background())
		cp.deregErr = e
		a.Shutdown(context.Background())
		if strings.Contains(logs.String(), "deregistration failed") {
			t.Fatalf("%v: a worker that is already gone is not a failure to report", e)
		}
		if a.Registered() {
			t.Fatal("it must consider itself deregistered")
		}
	}
	a, _, cp, logs := newAgent(t)
	a.Step(context.Background())
	cp.deregErr = errors.New("connection refused")
	a.Shutdown(context.Background())
	if !strings.Contains(logs.String(), "deregistration failed") {
		t.Fatal("a real failure to deregister must be reported")
	}
}

func TestAReRegisteredAgentForgetsItsOldDrainingState(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	a.Step(context.Background())
	be.set(protocol.StateDraining, nil)
	a.Step(context.Background()) // reported DRAINING
	be.set(protocol.StateReady, nil)
	a.Step(context.Background()) // the control plane refuses DRAINING -> READY
	cp.beatErr = &client.Error{Status: 409, Code: "illegal_transition"}
	a.Step(context.Background())
	cp.beatErr = nil
	a.Step(context.Background()) // fresh incarnation, healthy
	cp.reset()

	be.set("", errors.New("connection refused")) // now the backend dies WITHOUT having drained
	a.Step(context.Background())
	if got := cp.kinds(); got != "heartbeat:FAILED" {
		t.Fatalf("a new incarnation that never drained must report FAILED, not treat the exit as graceful: %q", got)
	}
}

func TestMockBackendLimitsAndTimeout(t *testing.T) {
	if b := NewMockBackend("http://x/"); b.hc.Timeout != 2*time.Second || b.base != "http://x" {
		t.Fatalf("timeout %v base %q", b.hc.Timeout, b.base)
	}
	// A VALID document just over 1 MiB is still refused.
	big := statsServer(t, 200, `{"status":"ready","pad":"`+strings.Repeat("x", maxBackendResponse)+`"}`)
	if _, err := big.Status(context.Background()); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("got %v", err)
	}
	for _, code := range []int{201, 204, 301, 400, 401, 403, 404, 429} {
		if _, err := statsServer(t, code, `{"status":"ready"}`).Status(context.Background()); err == nil {
			t.Errorf("status %d must not be treated as a healthy backend", code)
		}
	}
}

// --- the last mutation survivors ---------------------------------------------------------------------

func TestRegistrationFailuresDoNotSuppressTheNextHeartbeatFailureLog(t *testing.T) {
	a, _, cp, logs := newAgent(t)
	cp.registerErr = errors.New("down")
	for i := 0; i < 3; i++ {
		a.Step(context.Background()) // three failed registrations
	}
	cp.registerErr = nil
	cp.beatErr = errors.New("heartbeat refused by the network")
	a.Step(context.Background()) // registers, and its very first heartbeat fails
	if !strings.Contains(logs.String(), "heartbeat failed") {
		t.Fatalf("the first heartbeat failure after a successful registration must be logged (the failure count starts over): %s", logs.String())
	}
}

func TestAStaleDrainingMarkerDoesNotSurviveReRegistration(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	a.Step(context.Background())
	be.set(protocol.StateDraining, nil)
	a.Step(context.Background()) // reported DRAINING
	be.set(protocol.StateReady, nil)
	cp.beatErr = &client.Error{Status: 409, Code: "illegal_transition"} // the backend restarted in place
	a.Step(context.Background())                                        // registration dropped
	cp.beatErr = errors.New("network blip")                             // the new incarnation's first heartbeat does not get through
	a.Step(context.Background())                                        // re-registers; lastReported must now be REGISTERING, not the old DRAINING
	cp.reset()
	cp.beatErr = nil

	be.set("", errors.New("connection refused")) // the backend dies without ever draining in this incarnation
	a.Step(context.Background())
	if got := cp.kinds(); got != "heartbeat:FAILED" {
		t.Fatalf("a fresh incarnation must not inherit the old incarnation's draining marker (it would deregister instead of reporting FAILED): %q", got)
	}
}

func TestTheBackendReadIsBoundedBySizeNotJustChecked(t *testing.T) {
	var sent atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ready","pad":"`)
		chunk := []byte(strings.Repeat("x", 64<<10))
		for i := 0; i < 2000; i++ { // up to ~125 MiB, if the client keeps reading
			n, err := w.Write(chunk)
			sent.Add(int64(n))
			if err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	t.Cleanup(ts.Close)
	if _, err := NewMockBackend(ts.URL).Status(context.Background()); err == nil {
		t.Fatal("an endless body must be an error")
	}
	time.Sleep(200 * time.Millisecond)
	if got := sent.Load(); got > 20<<20 {
		t.Fatalf("the agent kept reading a hostile body: the server managed to send %d MiB (the limit is %d MiB)", got>>20, maxBackendResponse>>20)
	}
}

// --- second verification: boundaries and guards ----------------------------------------------------

func TestCallTimeoutIsHalfTheIntervalWithAFloor(t *testing.T) {
	for in, want := range map[time.Duration]time.Duration{
		2 * time.Second:        time.Second,
		time.Second:            500 * time.Millisecond,
		600 * time.Millisecond: 300 * time.Millisecond,
		500 * time.Millisecond: 250 * time.Millisecond,
		400 * time.Millisecond: 250 * time.Millisecond, // the floor
		10 * time.Millisecond:  250 * time.Millisecond,
	} {
		a := &Agent{interval: in}
		if got := a.callTimeout(); got != want {
			t.Errorf("interval %v: call timeout %v, want %v", in, got, want)
		}
	}
}

func TestReportedCapacityIsBelievedUpToTheCapAndNoFurther(t *testing.T) {
	ok := BackendStatus{State: protocol.StateReady, MaxConcurrency: maxReportedConcurrency, QueueSize: maxReportedQueueSize}
	if err := validateStatus(ok); err != nil {
		t.Fatalf("capacity exactly at the cap is valid: %v", err)
	}
	over := ok
	over.MaxConcurrency++
	if validateStatus(over) == nil {
		t.Error("concurrency over the cap must be rejected")
	}
	over = ok
	over.QueueSize++
	if validateStatus(over) == nil {
		t.Error("queue size over the cap must be rejected")
	}
	// The same capacity must register successfully.
	a, be, cp, _ := newAgent(t)
	be.status.MaxConcurrency, be.status.QueueSize = maxReportedConcurrency, maxReportedQueueSize
	a.Step(context.Background())
	if err := cp.calls[0].info.Validate(); err != nil || cp.kinds() != "register heartbeat:READY" {
		t.Fatalf("a backend at the cap must register: %v %q", err, cp.kinds())
	}
}

func TestADrainingBackendThatReturnsGarbageIsFailedNotTreatedAsFinished(t *testing.T) {
	a, be, cp, _ := newAgent(t)
	be.set(protocol.StateDraining, nil)
	a.Step(context.Background())
	cp.reset()
	be.status.Metrics.ActiveRequests = -1 // reachable, but unbelievable
	a.Step(context.Background())
	if got := cp.kinds(); got != "heartbeat:FAILED" {
		t.Fatalf("garbage from a draining backend is a failure, not a graceful end: %q", got)
	}
	if !a.Registered() {
		t.Fatal("the worker must stay registered (and visibly FAILED), not vanish")
	}
}

// slowDownBackend takes a while to answer and then reports an error, so the agent backs off.
type slowDownBackend struct {
	mu     sync.Mutex
	starts []time.Time
	delay  time.Duration
}

func (b *slowDownBackend) Status(context.Context) (BackendStatus, error) {
	b.mu.Lock()
	b.starts = append(b.starts, time.Now())
	b.mu.Unlock()
	time.Sleep(b.delay)
	return BackendStatus{}, errors.New("connection refused")
}

func TestOnlyTheSteadyCadenceIsCompensatedForWork(t *testing.T) {
	// Probes take 80ms. Delay 1 is one interval (compensated); delay 2 is a backoff of two
	// intervals, which must be waited in full, not shortened by the work already done.
	be := &slowDownBackend{delay: 80 * time.Millisecond}
	a := New(Config{WorkerID: "w1", Model: "m", AdvertiseURL: "http://127.0.0.1:1", Interval: 100 * time.Millisecond}, be, &fakeCP{}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		be.mu.Lock()
		n := len(be.starts)
		be.mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	be.mu.Lock()
	defer be.mu.Unlock()
	if len(be.starts) < 3 {
		t.Fatalf("only %d probes", len(be.starts))
	}
	if gap := be.starts[2].Sub(be.starts[1]); gap < 250*time.Millisecond {
		t.Fatalf("a backoff of two intervals must be waited in full after the probe; start-to-start gap was %v", gap)
	}
}
