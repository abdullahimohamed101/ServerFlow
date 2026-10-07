package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func testConfig() Config {
	return Config{
		Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second,
		Retention: 5 * time.Minute, MaxWorkers: 100, HeartbeatInterval: 2 * time.Second,
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func newTest(t *testing.T, mutate ...func(*Config)) (*Registry, *fakeClock) {
	t.Helper()
	cfg := testConfig()
	for _, m := range mutate {
		m(&cfg)
	}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r, err := NewWithClock(cfg, quiet(), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	return r, clock
}

func info(id, model string) protocol.WorkerInfo {
	return protocol.WorkerInfo{WorkerID: id, Model: model, Address: "http://127.0.0.1:9000", MaxConcurrency: 4, QueueSize: 8}
}

func hb(reg string, state protocol.WorkerState) protocol.Heartbeat {
	return protocol.Heartbeat{RegistrationID: reg, State: state}
}

func mustRegister(t *testing.T, r *Registry, id, model string) string {
	t.Helper()
	reg, err := r.Register(info(id, model))
	if err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
	return reg
}

func mustBeat(t *testing.T, r *Registry, id, reg string, state protocol.WorkerState) {
	t.Helper()
	if err := r.Heartbeat(id, reg, hb(reg, state)); err != nil {
		t.Fatalf("Heartbeat(%s, %s): %v", id, state, err)
	}
}

func get(t *testing.T, r *Registry, id string) protocol.WorkerSnapshot {
	t.Helper()
	s, ok := r.Get(id)
	if !ok {
		t.Fatalf("worker %s not found", id)
	}
	return s
}

// --- config ---------------------------------------------------------------------------

func TestConfigValidate(t *testing.T) {
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"zero suspect":         func(c *Config) { c.Suspect = 0 },
		"zero retention":       func(c *Config) { c.Retention = 0 },
		"zero heartbeat":       func(c *Config) { c.HeartbeatInterval = 0 },
		"suspect >= unhealthy": func(c *Config) { c.Suspect = c.Unhealthy },
		"unhealthy >= lost":    func(c *Config) { c.Unhealthy = c.Lost },
		"no capacity":          func(c *Config) { c.MaxWorkers = 0 },
	} {
		c := testConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		if _, err := New(c, quiet()); err == nil {
			t.Errorf("%s: New must reject it", name)
		}
	}
}

// --- health from heartbeat age, with exact boundaries (spec section 12) ----------------

func TestHealthBoundaries(t *testing.T) {
	r, clock := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)

	steps := []struct {
		advance  time.Duration // from the previous step
		health   protocol.Health
		state    protocol.WorkerState
		eligible bool
	}{
		{0, protocol.HealthHealthy, protocol.StateReady, true},
		{5*time.Second - time.Nanosecond, protocol.HealthHealthy, protocol.StateReady, true},  // just under 5s
		{time.Nanosecond, protocol.HealthSuspect, protocol.StateReady, false},                 // exactly 5s
		{5*time.Second - time.Nanosecond, protocol.HealthSuspect, protocol.StateReady, false}, // just under 10s
		{time.Nanosecond, protocol.HealthSuspect, protocol.StateReady, false},                 // exactly 10s: still suspect (>10s is unhealthy)
		{time.Nanosecond, protocol.HealthUnhealthy, protocol.StateUnhealthy, false},           // just over 10s
		{20*time.Second - time.Nanosecond - time.Nanosecond, protocol.HealthUnhealthy, protocol.StateUnhealthy, false},
		{time.Nanosecond, protocol.HealthUnhealthy, protocol.StateUnhealthy, false}, // exactly 30s: not yet lost
		{time.Nanosecond, protocol.HealthLost, protocol.StateLost, false},           // just over 30s
		{time.Hour, protocol.HealthLost, protocol.StateLost, false},
	}
	for i, s := range steps {
		clock.Advance(s.advance)
		got := get(t, r, "w1")
		if got.Health != s.health || got.State != s.state || got.Eligible != s.eligible {
			t.Fatalf("step %d (age %v): got health=%s state=%s eligible=%v, want %s %s %v",
				i, time.Duration(got.HeartbeatAgeSeconds*float64(time.Second)), got.Health, got.State, got.Eligible, s.health, s.state, s.eligible)
		}
	}
}

func TestAHeartbeatResetsHealth(t *testing.T) {
	r, clock := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	clock.Advance(20 * time.Second)
	if s := get(t, r, "w1"); s.Health != protocol.HealthUnhealthy || s.Eligible {
		t.Fatalf("setup: %+v", s)
	}
	mustBeat(t, r, "w1", reg, protocol.StateReady) // a healed partition resumes without re-registering
	if s := get(t, r, "w1"); s.Health != protocol.HealthHealthy || s.State != protocol.StateReady || !s.Eligible {
		t.Fatalf("a worker that reports in again must recover: %+v", s)
	}
	clock.Advance(40 * time.Second) // lost
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	if s := get(t, r, "w1"); s.Health != protocol.HealthHealthy || !s.Eligible {
		t.Fatalf("even a lost worker resumes when its incarnation reports in: %+v", s)
	}
}

func TestSuspectEligibilityIsConfigurable(t *testing.T) {
	r, clock := newTest(t, func(c *Config) { c.SuspectEligible = true })
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	clock.Advance(6 * time.Second)
	if s := get(t, r, "w1"); s.Health != protocol.HealthSuspect || !s.Eligible {
		t.Fatalf("a suspect worker is eligible when configured: %+v", s)
	}
	clock.Advance(10 * time.Second)
	if s := get(t, r, "w1"); s.Eligible {
		t.Fatal("an unhealthy worker is never eligible")
	}
}

func TestWorkerClocksAreIgnored(t *testing.T) {
	// Heartbeat has no timestamp field at all; age is measured at receipt.
	if _, ok := any(protocol.Heartbeat{}).(interface{ Timestamp() time.Time }); ok {
		t.Fatal("heartbeats must not carry worker time")
	}
	raw, _ := json.Marshal(protocol.Heartbeat{})
	if strings.Contains(string(raw), "time") || strings.Contains(string(raw), "timestamp") {
		t.Fatalf("heartbeat JSON carries a timestamp: %s", raw)
	}
}

// --- state machine ----------------------------------------------------------------------

func TestRegistrationStartsInRegistering(t *testing.T) {
	r, _ := newTest(t)
	mustRegister(t, r, "w1", "qwen")
	s := get(t, r, "w1")
	if s.State != protocol.StateRegistering || s.Eligible || s.Health != protocol.HealthHealthy {
		t.Fatalf("a new worker is REGISTERING and not eligible: %+v", s)
	}
}

func TestLegalLifecycleToReadyAndDraining(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	for _, st := range []protocol.WorkerState{protocol.StateLoadingModel, protocol.StateWarming, protocol.StateReady, protocol.StateDraining} {
		mustBeat(t, r, "w1", reg, st)
		s := get(t, r, "w1")
		if s.State != st || s.Eligible != (st == protocol.StateReady) {
			t.Fatalf("after %s: %+v", st, s)
		}
	}
}

func TestIllegalTransitionsAreRejectedAndDoNotCountAsProofOfLife(t *testing.T) {
	r, clock := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateDraining)
	clock.Advance(4 * time.Second)

	err := r.Heartbeat("w1", reg, hb(reg, protocol.StateReady))
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("DRAINING -> READY must be refused, got %v", err)
	}
	s := get(t, r, "w1")
	if s.State != protocol.StateDraining {
		t.Fatalf("a refused heartbeat must not change the state: %+v", s)
	}
	if s.HeartbeatAgeSeconds < 3.9 {
		t.Fatalf("a refused heartbeat must not refresh the heartbeat age, got %v", s.HeartbeatAgeSeconds)
	}
}

func TestFailedIsReachableFromAnywhereAndRecoverable(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	mustBeat(t, r, "w1", reg, protocol.StateFailed)
	if s := get(t, r, "w1"); s.State != protocol.StateFailed || s.Eligible {
		t.Fatalf("%+v", s)
	}
	mustBeat(t, r, "w1", reg, protocol.StateReady) // the backend came back
	if s := get(t, r, "w1"); s.State != protocol.StateReady || !s.Eligible {
		t.Fatalf("%+v", s)
	}
}

func TestHeartbeatCarriesMetricsAndReason(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateFailed, Reason: "backend unreachable",
		Metrics: protocol.Metrics{ActiveRequests: 2, QueueDepth: 3, QueuedInputTokens: 40, RecentTokensPerSecond: 77.5}})
	if err != nil {
		t.Fatal(err)
	}
	s := get(t, r, "w1")
	if s.Metrics.ActiveRequests != 2 || s.Metrics.QueueDepth != 3 || s.Metrics.QueuedInputTokens != 40 || s.Metrics.RecentTokensPerSecond != 77.5 || s.Reason != "backend unreachable" {
		t.Fatalf("%+v", s)
	}
}

func TestInvalidInputIsRejected(t *testing.T) {
	r, _ := newTest(t)
	bad := info("w1", "qwen")
	bad.Address = "http://user:secret@h:1"
	if _, err := r.Register(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
	if r.Len() != 0 {
		t.Fatal("an invalid registration must not be stored")
	}
	reg := mustRegister(t, r, "w1", "qwen")
	if err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: "NOPE"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
	if err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateReady, Metrics: protocol.Metrics{QueueDepth: -1}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

// --- incarnations -------------------------------------------------------------------------

func TestUnknownAndStaleRegistrations(t *testing.T) {
	r, _ := newTest(t)
	if err := r.Heartbeat("nobody", "reg_x", hb("reg_x", protocol.StateReady)); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("got %v", err)
	}
	reg := mustRegister(t, r, "w1", "qwen")
	if err := r.Heartbeat("w1", "reg_wrong", hb("reg_wrong", protocol.StateReady)); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("got %v", err)
	}
	if err := r.Deregister("w1", "reg_wrong"); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("got %v", err)
	}
	if err := r.Deregister("nobody", reg); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("got %v", err)
	}
	if _, ok := r.Get("w1"); !ok {
		t.Fatal("a rejected deregistration must not remove the worker")
	}
}

func TestARestartedWorkerSupersedesItsOldIncarnation(t *testing.T) {
	r, clock := newTest(t)
	old := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", old, protocol.StateReady)
	clock.Advance(3 * time.Second)

	fresh := mustRegister(t, r, "w1", "qwen") // the process restarted under the same ID
	if fresh == old {
		t.Fatal("a new incarnation needs a new registration ID")
	}
	if r.Len() != 1 {
		t.Fatalf("the restart must replace, not duplicate: %d workers", r.Len())
	}
	if s := get(t, r, "w1"); s.State != protocol.StateRegistering || s.Eligible {
		t.Fatalf("a fresh incarnation starts from REGISTERING: %+v", s)
	}
	// The old process is still sending heartbeats: they must not be believed.
	if err := r.Heartbeat("w1", old, hb(old, protocol.StateReady)); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("a superseded incarnation's heartbeat must be refused, got %v", err)
	}
	if err := r.Deregister("w1", old); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("a superseded incarnation cannot deregister the new one, got %v", err)
	}
	mustBeat(t, r, "w1", fresh, protocol.StateReady)
	if s := get(t, r, "w1"); !s.Eligible {
		t.Fatalf("%+v", s)
	}
}

func TestRegistrationIDsAreUniqueAndNotExposed(t *testing.T) {
	r, _ := newTest(t)
	seen := map[string]bool{}
	var last string
	for i := 0; i < 500; i++ {
		last = mustRegister(t, r, "w1", "qwen")
		if seen[last] || !strings.HasPrefix(last, "reg_") {
			t.Fatalf("bad or repeated registration ID %q", last)
		}
		seen[last] = true
	}
	raw, _ := json.Marshal(get(t, r, "w1"))
	if strings.Contains(string(raw), last) || strings.Contains(string(raw), "registration") {
		t.Fatalf("a snapshot must never expose the registration ID: %s", raw)
	}
}

func TestDeregister(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	if err := r.Deregister("w1", reg); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("w1"); ok || len(r.Eligible("qwen")) != 0 {
		t.Fatal("a deregistered worker must be gone and not eligible")
	}
	if err := r.Heartbeat("w1", reg, hb(reg, protocol.StateReady)); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("heartbeating after deregistration is an unknown worker, got %v", err)
	}
}

// --- lookup -----------------------------------------------------------------------------------

func TestLookupByModelStateAndEligibility(t *testing.T) {
	r, clock := newTest(t)
	regs := map[string]string{}
	for id, model := range map[string]string{"a": "qwen", "b": "qwen", "c": "qwen", "d": "qwen", "e": "llama"} {
		regs[id] = mustRegister(t, r, id, model)
	}
	mustBeat(t, r, "a", regs["a"], protocol.StateReady)
	mustBeat(t, r, "b", regs["b"], protocol.StateReady)
	mustBeat(t, r, "c", regs["c"], protocol.StateDraining)
	mustBeat(t, r, "d", regs["d"], protocol.StateFailed)
	mustBeat(t, r, "e", regs["e"], protocol.StateReady)

	ids := func(s []protocol.WorkerSnapshot) string {
		var out []string
		for _, w := range s {
			out = append(out, w.WorkerID)
		}
		return strings.Join(out, ",")
	}
	if got := ids(r.Eligible("qwen")); got != "a,b" {
		t.Fatalf("eligible qwen workers: %q, want a,b (draining and failed are never offered)", got)
	}
	if got := ids(r.Eligible("llama")); got != "e" {
		t.Fatalf("got %q", got)
	}
	if got := ids(r.Eligible("nope")); got != "" {
		t.Fatalf("an unknown model has no workers, got %q", got)
	}
	if got := ids(r.List(ListFilter{})); got != "a,b,c,d,e" {
		t.Fatalf("List must be sorted by ID, got %q", got)
	}
	if got := ids(r.List(ListFilter{Model: "qwen", State: protocol.StateDraining})); got != "c" {
		t.Fatalf("got %q", got)
	}

	clock.Advance(7 * time.Second) // a, b, e now suspect
	if got := ids(r.Eligible("qwen")); got != "" {
		t.Fatalf("suspect workers are not offered by default, got %q", got)
	}
	mustBeat(t, r, "a", regs["a"], protocol.StateReady)
	if got := ids(r.Eligible("qwen")); got != "a" {
		t.Fatalf("got %q", got)
	}
	models := r.Models()
	if len(models) != 2 || models[0] != (protocol.ModelInfo{Model: "llama", Workers: 1, Eligible: 0}) || models[1] != (protocol.ModelInfo{Model: "qwen", Workers: 4, Eligible: 1}) {
		t.Fatalf("model inventory wrong: %+v", models)
	}
}

func TestSnapshotsAreCopies(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	s := r.List(ListFilter{})
	s[0].Model, s[0].State = "tampered", protocol.StateLost
	if got := get(t, r, "w1"); got.Model != "qwen" || got.State != protocol.StateReady {
		t.Fatalf("mutating a snapshot changed the registry: %+v", got)
	}
}

// --- sweeping, eviction, caps -------------------------------------------------------------------

func TestSweepEvictsOnlyAfterLostPlusRetention(t *testing.T) {
	r, clock := newTest(t, func(c *Config) { c.Retention = time.Minute })
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)

	clock.Advance(30*time.Second + time.Minute) // exactly lost + retention
	if n := r.Sweep(); n != 0 || r.Len() != 1 {
		t.Fatalf("a worker is kept until strictly past lost+retention (evicted %d)", n)
	}
	clock.Advance(time.Nanosecond)
	if n := r.Sweep(); n != 1 || r.Len() != 0 {
		t.Fatalf("expected one eviction, got %d (len %d)", n, r.Len())
	}
	if _, ok := r.Get("w1"); ok {
		t.Fatal("an evicted worker must be gone")
	}
}

func TestReadsDoNotDependOnSweep(t *testing.T) {
	r, clock := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	clock.Advance(15 * time.Second) // no sweep has run
	if s := get(t, r, "w1"); s.State != protocol.StateUnhealthy || s.Eligible || len(r.Eligible("qwen")) != 0 {
		t.Fatalf("a dead worker must read as unhealthy even if the sweeper is not running: %+v", s)
	}
}

func TestWorkerCapIsEnforcedAndDeadWorkersMakeRoom(t *testing.T) {
	r, clock := newTest(t, func(c *Config) { c.MaxWorkers = 3; c.Retention = time.Minute })
	for _, id := range []string{"a", "b", "c"} {
		mustRegister(t, r, id, "qwen")
	}
	if _, err := r.Register(info("d", "qwen")); !errors.Is(err, ErrFull) {
		t.Fatalf("a full registry must refuse new workers, got %v", err)
	}
	if _, err := r.Register(info("a", "qwen")); err != nil {
		t.Fatalf("re-registering an existing worker needs no extra room: %v", err)
	}
	clock.Advance(30*time.Second + time.Minute + time.Second) // all three are long gone
	if _, err := r.Register(info("d", "qwen")); err != nil {
		t.Fatalf("workers lost past retention must make room: %v", err)
	}
	if r.Len() != 1 {
		t.Fatalf("the dead workers should have been evicted, len %d", r.Len())
	}
}

func TestTransitionsAreLoggedOnceEach(t *testing.T) {
	var buf lockedBuf
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r, _ := NewWithClock(testConfig(), slog.New(slog.NewJSONHandler(&buf, nil)), clock.Now)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	clock.Advance(12 * time.Second)
	r.Sweep()
	r.Sweep() // a second sweep in the same state must not log again
	clock.Advance(30 * time.Second)
	r.Sweep()
	out := buf.String()
	for _, want := range []string{`"to":"READY"`, `"to":"UNHEALTHY"`, `"to":"LOST"`} {
		if strings.Count(out, want) != 1 {
			t.Fatalf("want exactly one log line with %s, got:\n%s", want, out)
		}
	}
	if !strings.Contains(out, `"component":"registry"`) {
		t.Fatalf("log lines must carry the component: %s", out)
	}
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

// --- concurrency ---------------------------------------------------------------------------------

func TestConcurrentUseIsRaceFree(t *testing.T) {
	r, clock := newTest(t, func(c *Config) { c.MaxWorkers = 1000 })
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("w%d", w)
			for i := 0; i < 200; i++ {
				reg, err := r.Register(info(id, "qwen"))
				if err != nil {
					t.Errorf("register: %v", err)
					return
				}
				_ = r.Heartbeat(id, reg, hb(reg, protocol.StateReady))
				_ = r.Heartbeat(id, "reg_stale", hb("reg_stale", protocol.StateReady))
				if i%50 == 0 {
					_ = r.Deregister(id, reg)
				}
			}
		}()
	}
	for rd := 0; rd < 4; rd++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_ = r.List(ListFilter{})
				_ = r.Eligible("qwen")
				_ = r.Models()
				r.Sweep()
				clock.Advance(time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

// --- randomized comparison with a brute-force model ---------------------------------------------------

type oracleWorker struct {
	reg      string
	state    protocol.WorkerState
	lastBeat time.Time
	drained  bool
}

func TestRandomOperationsMatchABruteForceModel(t *testing.T) {
	states := []protocol.WorkerState{protocol.StateRegistering, protocol.StateLoadingModel, protocol.StateWarming, protocol.StateReady, protocol.StateDraining, protocol.StateFailed}
	ids := []string{"a", "b", "c", "d"}
	models := map[string]string{"a": "m1", "b": "m1", "c": "m2", "d": "m2"}

	for seed := int64(0); seed < 150; seed++ {
		rng := rand.New(rand.NewSource(seed))
		r, clock := newTest(t, func(c *Config) { c.MaxWorkers = 100; c.Retention = 20 * time.Second })
		oracle := map[string]*oracleWorker{}
		regOf := map[string]string{}

		check := func(step int, op string) {
			now := clock.Now()
			// Evictions happen only on Sweep or when full; mirror lazily by
			// comparing only what both sides still hold.
			want := map[string]protocol.WorkerSnapshot{}
			for id, w := range oracle {
				age := now.Sub(w.lastBeat)
				var health protocol.Health
				switch {
				case age > 30*time.Second:
					health = protocol.HealthLost
				case age > 10*time.Second:
					health = protocol.HealthUnhealthy
				case age >= 5*time.Second:
					health = protocol.HealthSuspect
				default:
					health = protocol.HealthHealthy
				}
				state := w.state
				switch health {
				case protocol.HealthLost:
					state = protocol.StateLost
				case protocol.HealthUnhealthy:
					state = protocol.StateUnhealthy
				}
				want[id] = protocol.WorkerSnapshot{WorkerID: id, Model: models[id], State: state, Health: health,
					Eligible: state == protocol.StateReady && health == protocol.HealthHealthy}
			}
			for _, got := range r.List(ListFilter{}) {
				w, ok := want[got.WorkerID]
				if !ok {
					t.Fatalf("seed %d step %d after %s: registry has %s, the model does not", seed, step, op, got.WorkerID)
				}
				if got.State != w.State || got.Health != w.Health || got.Eligible != w.Eligible || got.Model != w.Model {
					t.Fatalf("seed %d step %d after %s: %s got state=%s health=%s eligible=%v, model says %s %s %v",
						seed, step, op, got.WorkerID, got.State, got.Health, got.Eligible, w.State, w.Health, w.Eligible)
				}
				delete(want, got.WorkerID)
			}
			for id := range want {
				// Only a worker evicted for being lost past retention may be missing.
				if oracle[id] != nil && clock.Now().Sub(oracle[id].lastBeat) <= 50*time.Second {
					t.Fatalf("seed %d step %d after %s: the registry lost worker %s too early", seed, step, op, id)
				}
				delete(oracle, id)
			}
		}

		for step := 0; step < 120; step++ {
			id := ids[rng.Intn(len(ids))]
			switch op := rng.Intn(6); op {
			case 0: // register
				reg, err := r.Register(info(id, models[id]))
				if err != nil {
					t.Fatalf("seed %d: register: %v", seed, err)
				}
				oracle[id] = &oracleWorker{reg: reg, state: protocol.StateRegistering, lastBeat: clock.Now()}
				regOf[id] = reg
				check(step, "register "+id)
			case 1, 2: // heartbeat with the current registration
				w, known := oracle[id]
				st := states[rng.Intn(len(states))]
				reg := regOf[id]
				if reg == "" {
					reg = "reg_never_issued"
				}
				err := r.Heartbeat(id, reg, hb(reg, st))
				switch {
				case !known:
					if !errors.Is(err, ErrUnknownWorker) && !errors.Is(err, ErrStaleRegistration) {
						t.Fatalf("seed %d: heartbeat for an unknown worker: %v", seed, err)
					}
				case !protocol.CanTransition(w.state, st) || (w.drained && st != protocol.StateDraining && st != protocol.StateFailed):
					if !errors.Is(err, ErrIllegalTransition) {
						t.Fatalf("seed %d: expected an illegal transition %s->%s, got %v", seed, w.state, st, err)
					}
				default:
					if err != nil {
						t.Fatalf("seed %d: heartbeat %s->%s: %v", seed, w.state, st, err)
					}
					w.state, w.lastBeat = st, clock.Now()
					if st == protocol.StateDraining {
						w.drained = true
					}
				}
				check(step, "heartbeat "+id)
			case 3: // a stale heartbeat must never change anything
				if _, known := oracle[id]; known {
					if err := r.Heartbeat(id, "reg_stale", hb("reg_stale", protocol.StateReady)); !errors.Is(err, ErrStaleRegistration) {
						t.Fatalf("seed %d: got %v", seed, err)
					}
				}
				check(step, "stale heartbeat "+id)
			case 4: // time passes
				clock.Advance(time.Duration(rng.Intn(14000)) * time.Millisecond)
				check(step, "advance")
			case 5: // deregister
				if w, known := oracle[id]; known {
					if err := r.Deregister(id, w.reg); err != nil {
						t.Fatalf("seed %d: deregister: %v", seed, err)
					}
					delete(oracle, id)
				}
				check(step, "deregister "+id)
			}
		}
	}
}

// --- independent review and verification findings -----------------------------------------------

func TestADrainIsOneWayEvenThroughFailed(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	mustBeat(t, r, "w1", reg, protocol.StateDraining)
	mustBeat(t, r, "w1", reg, protocol.StateFailed) // the backend wobbled while draining

	for _, st := range []protocol.WorkerState{protocol.StateReady, protocol.StateLoadingModel, protocol.StateWarming, protocol.StateRegistering} {
		if err := r.Heartbeat("w1", reg, hb(reg, st)); !errors.Is(err, ErrIllegalTransition) {
			t.Fatalf("DRAINING -> FAILED -> %s must be refused (a drain is one-way), got %v", st, err)
		}
	}
	if s := get(t, r, "w1"); s.State != protocol.StateFailed || s.Eligible {
		t.Fatalf("%+v", s)
	}
	mustBeat(t, r, "w1", reg, protocol.StateDraining) // it may still report that it is draining
	mustBeat(t, r, "w1", reg, protocol.StateFailed)

	// A worker that failed WITHOUT ever draining can still recover (the backend restarted).
	reg2 := mustRegister(t, r, "w2", "qwen")
	mustBeat(t, r, "w2", reg2, protocol.StateReady)
	mustBeat(t, r, "w2", reg2, protocol.StateFailed)
	mustBeat(t, r, "w2", reg2, protocol.StateReady)

	// Registering again is the only way back, and it is a fresh incarnation.
	reg3 := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg3, protocol.StateReady)
	if s := get(t, r, "w1"); !s.Eligible {
		t.Fatalf("a re-registered worker starts clean: %+v", s)
	}
}

func TestModelMatchingIsExact(t *testing.T) {
	r, _ := newTest(t)
	for id, model := range map[string]string{"a": "qwen", "b": "qwen-7b", "c": "Qwen", "d": "qwen7b"} {
		reg := mustRegister(t, r, id, model)
		mustBeat(t, r, id, reg, protocol.StateReady)
	}
	ids := func(s []protocol.WorkerSnapshot) string {
		var out []string
		for _, w := range s {
			out = append(out, w.WorkerID)
		}
		return strings.Join(out, ",")
	}
	for model, want := range map[string]string{"qwen": "a", "qwen-7b": "b", "Qwen": "c", "qwen-7": "", "qwe": "", "QWEN": "", "qwen7b": "d"} {
		if got := ids(r.List(ListFilter{Model: model})); got != want {
			t.Errorf("List(model=%q) = %q, want %q (matching must be exact: never a prefix, never case-folded)", model, got, want)
		}
		if got := ids(r.Eligible(model)); got != want {
			t.Errorf("Eligible(%q) = %q, want %q", model, got, want)
		}
	}
}

func TestSnapshotFieldsAreAccurate(t *testing.T) {
	r, clock := newTest(t)
	registeredAt := clock.Now()
	reg := mustRegister(t, r, "w1", "qwen")
	clock.Advance(3 * time.Second)
	beatAt := clock.Now()
	if err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateReady, Reason: "all good",
		Metrics: protocol.Metrics{ActiveRequests: 2, QueueDepth: 1, QueuedInputTokens: 9, RecentTokensPerSecond: 42}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(7500 * time.Millisecond)
	s := get(t, r, "w1")
	if !s.RegisteredAt.Equal(registeredAt) || !s.LastHeartbeat.Equal(beatAt) {
		t.Fatalf("timestamps wrong: registered %v want %v, last heartbeat %v want %v", s.RegisteredAt, registeredAt, s.LastHeartbeat, beatAt)
	}
	if s.HeartbeatAgeSeconds != 7.5 {
		t.Fatalf("the age is reported in seconds: got %v, want 7.5", s.HeartbeatAgeSeconds)
	}
	if s.Reason != "all good" || s.Metrics.QueuedInputTokens != 9 || s.Address != "http://127.0.0.1:9000" || s.MaxConcurrency != 4 || s.QueueSize != 8 {
		t.Fatalf("snapshot fields: %+v", s)
	}
}

func TestARejectedHeartbeatDoesNotOverwriteStoredData(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	if err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateDraining, Reason: "original",
		Metrics: protocol.Metrics{QueueDepth: 3}}); err != nil {
		t.Fatal(err)
	}
	err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateReady, Reason: "overwritten",
		Metrics: protocol.Metrics{QueueDepth: 99}})
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("got %v", err)
	}
	if s := get(t, r, "w1"); s.Metrics.QueueDepth != 3 || s.Reason != "original" || s.State != protocol.StateDraining {
		t.Fatalf("a refused heartbeat must change nothing: %+v", s)
	}
	err = r.Heartbeat("w1", "reg_wrong", protocol.Heartbeat{RegistrationID: "reg_wrong", State: protocol.StateDraining, Reason: "forged",
		Metrics: protocol.Metrics{QueueDepth: 77}})
	if !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("got %v", err)
	}
	if s := get(t, r, "w1"); s.Metrics.QueueDepth != 3 || s.Reason != "original" {
		t.Fatalf("a forged heartbeat must change nothing: %+v", s)
	}
}

func TestAClockThatStepsBackwardsNeverMakesAWorkerLookDead(t *testing.T) {
	r, clock := newTest(t)
	reg := mustRegister(t, r, "w1", "qwen")
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	clock.Advance(-time.Hour)
	s := get(t, r, "w1")
	if s.HeartbeatAgeSeconds != 0 || s.Health != protocol.HealthHealthy || !s.Eligible {
		t.Fatalf("a negative age must read as zero: %+v", s)
	}
}

// slow logging must not hold the registry's lock: a handler that calls back
// into the registry would deadlock if Sweep logged while holding it.
type reentrantHandler struct {
	inner  slog.Handler
	reg    **Registry
	active *atomic.Bool // only re-enter while the sweep under test is running
}

func (h reentrantHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}
func (h reentrantHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return reentrantHandler{inner: h.inner.WithAttrs(a), reg: h.reg, active: h.active}
}
func (h reentrantHandler) WithGroup(n string) slog.Handler {
	return reentrantHandler{inner: h.inner.WithGroup(n), reg: h.reg, active: h.active}
}
func (h reentrantHandler) Handle(ctx context.Context, rec slog.Record) error {
	if r := *h.reg; r != nil && h.active.Load() {
		_ = r.Len() // needs the registry lock: deadlocks if the caller holds it
	}
	return h.inner.Handle(ctx, rec)
}

func TestSweepLogsWithoutHoldingTheLock(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var reg *Registry
	var active atomic.Bool
	h := reentrantHandler{inner: slog.NewJSONHandler(io.Discard, nil), reg: &reg, active: &active}
	r, err := NewWithClock(testConfig(), slog.New(h), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	reg = r
	for _, id := range []string{"a", "b", "c"} {
		wid := mustRegister(t, r, id, "qwen")
		mustBeat(t, r, id, wid, protocol.StateReady)
	}
	clock.Advance(40 * time.Second) // all lost: Sweep has transitions to log
	active.Store(true)
	done := make(chan struct{})
	go func() { r.Sweep(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Sweep deadlocked: it must not log while holding the registry lock")
	}
}

// --- log content (second verification) ------------------------------------------------------------

func logRecords(t *testing.T, buf *lockedBuf) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func findLog(recs []map[string]any, msgPart string) map[string]any {
	for _, m := range recs {
		if s, _ := m["msg"].(string); strings.Contains(s, msgPart) {
			return m
		}
	}
	return nil
}

func TestLifecycleLogsCarryTheirDetails(t *testing.T) {
	var buf lockedBuf
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cfg := testConfig()
	cfg.MaxWorkers = 1
	r, _ := NewWithClock(cfg, slog.New(slog.NewJSONHandler(&buf, nil)), clock.Now)

	reg := mustRegister(t, r, "w1", "qwen")
	if err := r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateFailed, Reason: "gpu fell off the bus"}); err != nil {
		t.Fatal(err)
	}
	failed := findLog(logRecords(t, &buf), "state changed")
	if failed == nil {
		t.Fatalf("no state-change log: %s", buf.String())
	}
	if failed["level"] != "WARN" || failed["to"] != "FAILED" || failed["reason"] != "gpu fell off the bus" {
		t.Errorf("a FAILED transition must be a warning carrying the reason: %v", failed)
	}

	// Re-registering supersedes, and says what it superseded.
	reg2 := mustRegister(t, r, "w1", "qwen")
	sup := findLog(logRecords(t, &buf), "superseding")
	if sup == nil || sup["previous_state"] != "FAILED" || sup["worker_id"] != "w1" {
		t.Errorf("the supersede log must name the previous state: %v", sup)
	}
	if n := strings.Count(buf.String(), "worker registered"); n != 1 {
		t.Errorf("a re-registration must not also log a fresh registration (got %d)", n)
	}

	// Deregistration logs the last state.
	mustBeat(t, r, "w1", reg2, protocol.StateReady)
	if err := r.Deregister("w1", reg2); err != nil {
		t.Fatal(err)
	}
	dereg := findLog(logRecords(t, &buf), "deregistered")
	if dereg == nil || dereg["last_state"] != "READY" || dereg["worker_id"] != "w1" {
		t.Errorf("the deregistration log must carry the last state: %v", dereg)
	}

	// A sweep evicts a long-lost worker and says so.
	mustRegister(t, r, "w2", "qwen")
	clock.Advance(cfg.Lost + cfg.Retention + time.Second)
	if n := r.Sweep(); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	if ev := findLog(logRecords(t, &buf), "evicted"); ev == nil || ev["worker_id"] != "w2" || ev["level"] != "WARN" {
		t.Errorf("the sweep eviction must be logged as a warning: %v", ev)
	}

	// Making room on a full registry evicts, and logs it, too.
	buf = lockedBuf{}
	mustRegister(t, r, "w3", "qwen")
	clock.Advance(cfg.Lost + cfg.Retention + time.Second)
	mustRegister(t, r, "w4", "qwen") // the registry is full (MaxWorkers 1); w3 is long gone
	if ev := findLog(logRecords(t, &buf), "evicted"); ev == nil || ev["worker_id"] != "w3" {
		t.Errorf("evicting to make room must be logged: %s", buf.String())
	}
}
