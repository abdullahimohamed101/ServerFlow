package chaos

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/report"
	"serverflow/internal/bench/runner"
	"serverflow/internal/mockworker"
)

func specs(n, conc int) []embedded.WorkerSpec {
	var out []embedded.WorkerSpec
	for i := range n {
		out = append(out, embedded.WorkerSpec{ID: "worker-0" + string(rune('1'+i)), MaxConcurrency: conc})
	}
	return out
}

// --- scenario catalogue ---------------------------------------------------------------------------

func TestCatalogueIsTheRevisedSet(t *testing.T) {
	want := []string{"worker-crash", "worker-freeze", "slow-worker-below-timeout", "slow-worker-above-timeout", "error-burst-unavailable",
		"error-burst-unavailable-partial", "error-burst-500", "midstream", "conn-reset", "drain-roll", "overload"}
	got := map[string]bool{}
	for _, s := range Scenarios() {
		got[s.Name] = true
		if s.Description == "" || s.CannotShow == "" {
			t.Errorf("%s must say what it shows and what it cannot", s.Name)
		}
		if strings.Contains(s.Name, "flap") {
			t.Errorf("%s: flapping health was dropped from the plan", s.Name)
		}
	}
	for _, n := range want {
		if !got[n] {
			t.Errorf("scenario %s is missing", n)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d scenarios, want %d: %v", len(got), len(want), Names())
	}
	if _, err := Lookup("nope"); err == nil {
		t.Error("an unknown scenario was found")
	}
}

func TestSafeSlotFractionMatchesPhase7(t *testing.T) {
	if SafeSlotFraction != runner.SafeSlotFraction {
		t.Fatalf("chaos %v, runner %v", SafeSlotFraction, runner.SafeSlotFraction)
	}
}

func TestParamsScaleAndKeepTheShape(t *testing.T) {
	for _, scale := range []float64{0.05, 0.1, 0.5, 1, 2} {
		p := newParams(scale)
		if p.Bucket < MinBucket {
			t.Errorf("scale %v: bucket %v", scale, p.Bucket)
		}
		if p.Warmup/p.Bucket != warmupBuckets || p.Baseline/p.Bucket != baselineBuckets || p.Fault/p.Bucket != faultBuckets || p.Recovery/p.Bucket != recoveryBuckets {
			t.Errorf("scale %v: windows %v %v %v %v are not whole buckets of the same shape", scale, p.Warmup, p.Baseline, p.Fault, p.Recovery)
		}
	}
	if p := newParams(1); p.Bucket != time.Second || p.Total() != 53*time.Second {
		t.Fatalf("scale 1: %+v", p)
	}
}

func TestScenarioParams(t *testing.T) {
	freeze, _ := Lookup("worker-freeze")
	if p := freeze.Params(4, 1); p.HeaderTimeout != 2*time.Second {
		t.Fatalf("freeze lowers the header timeout to 2s at scale 1, got %v", p.HeaderTimeout)
	}
	below, _ := Lookup("slow-worker-below-timeout")
	above, _ := Lookup("slow-worker-above-timeout")
	pb, pa := below.Params(4, 1), above.Params(4, 1)
	if pb.Delay != 5*time.Second || pa.Delay != 5*time.Second || pb.HeaderTimeout >= pb.Delay || pa.HeaderTimeout <= pa.Delay {
		t.Fatalf("slow worker: delay %v, timeouts below %v and above %v must straddle it", pb.Delay, pb.HeaderTimeout, pa.HeaderTimeout)
	}
	for _, scale := range []float64{0.05, 0.2} {
		pb, pa := below.Params(4, scale), above.Params(4, scale)
		if pb.HeaderTimeout >= pb.Delay || pa.HeaderTimeout <= pa.Delay {
			t.Errorf("scale %v: the timeouts no longer straddle the delay: %v %v %v", scale, pb.HeaderTimeout, pb.Delay, pa.HeaderTimeout)
		}
	}
	roll, _ := Lookup("drain-roll")
	p := roll.Params(4, 0.05)
	if p.DrainAnnounce <= embedded.HeartbeatInterval+embedded.RegistryRefresh {
		t.Fatalf("the announce period %v must exceed the heartbeat plus the registry refresh", p.DrainAnnounce)
	}
	if p.Fault < 16*time.Second {
		t.Fatalf("a roll of 4 workers needs a fault window of at least 16s whatever the scale, got %v", p.Fault)
	}
	if mutate := freeze.GatewayMutate(freeze.Params(4, 1)); mutate == nil {
		t.Fatal("freeze must edit the gateway configuration")
	}
	crash, _ := Lookup("worker-crash")
	if crash.GatewayMutate(crash.Params(4, 1)) != nil {
		t.Fatal("a crash leaves the gateway configuration alone")
	}
}

func TestClientPlanUsesSurvivingSlots(t *testing.T) {
	four := specs(4, 8) // 32 slots
	for _, name := range []string{"worker-crash", "worker-freeze", "error-burst-500", "drain-roll", "midstream"} {
		sc, _ := Lookup(name)
		p := Clients(sc, four, 0)
		if p.Slots != 32 || p.Surviving != 24 || p.Base != 14 || p.Extra != 0 {
			t.Errorf("%s: %+v (a worker's 8 slots do not count, and 60%% of 24 is 14)", name, p)
		}
	}
	over, _ := Lookup("overload")
	p := Clients(over, four, 0)
	if p.Surviving != 32 || p.Base != 19 || p.Base+p.Extra != 48 {
		t.Errorf("overload: %+v (all slots survive; the fault window offers 1.5 times the slots)", p)
	}
	if p := Clients(over, four, 10); p.Base != 10 || p.Base+p.Extra != 48 {
		t.Errorf("explicit clients: %+v", p)
	}
	crash, _ := Lookup("worker-crash")
	if p := Clients(crash, four, 20); p.Base != 20 {
		t.Errorf("an explicit client count is honored so the gate can reject it: %+v", p)
	}
}

// --- options and flags -------------------------------------------------------------------------

func TestParseFlags(t *testing.T) {
	o, list, err := ParseFlags([]string{"--scenario", "worker-freeze", "--scale", "0.5", "--repeat", "3", "--resilience", "off"})
	if err != nil || list || o.Scenario != "worker-freeze" || o.Scale != 0.5 || o.Repeat != 3 || o.Resilience != "off" || o.Workers != 4 {
		t.Fatalf("%+v %v %v", o, list, err)
	}
	if _, list, err := ParseFlags([]string{"-list"}); err != nil || !list {
		t.Fatalf("-list: %v %v", list, err)
	}
	for name, args := range map[string][]string{
		"no scenario":    {},
		"unknown":        {"--scenario", "nope"},
		"bad resilience": {"--scenario", "all", "--resilience", "maybe"},
		"tiny scale":     {"--scenario", "all", "--scale", "0.001"},
		"one worker":     {"--scenario", "all", "--workers", "1"},
		"repeat":         {"--scenario", "all", "--repeat", "99"},
		"burst":          {"--scenario", "all", "--workload", "burst"},
		"extra argument": {"--scenario", "all", "x"},
		"error rate":     {"--scenario", "all", "--max-error-rate", "2"},
	} {
		if _, _, err := ParseFlags(args); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestListNamesEverythingAndTheCaveats(t *testing.T) {
	var b strings.Builder
	WriteList(&b)
	for _, n := range Names() {
		if !strings.Contains(b.String(), n) {
			t.Errorf("-list lacks %s", n)
		}
	}
	if !strings.Contains(b.String(), "Cannot show") {
		t.Error("-list must say what a scenario cannot show")
	}
}

// --- clock -----------------------------------------------------------------------------------

func TestRealClockWallOffsetIsQuietWhileTimePasses(t *testing.T) {
	c := RealClock()
	if err := c.SleepUntil(context.Background(), c.Now().Add(30*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if d := c.WallOffset(); d > 50*time.Millisecond || d < -50*time.Millisecond {
		t.Fatalf("the wall clock and the monotonic clock disagree by %v with nothing wrong", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.SleepUntil(ctx, c.Now().Add(time.Hour)); err == nil {
		t.Fatal("a cancelled sleep must return its error")
	}
}

func TestFakeClockJumpsAndNeverGoesBack(t *testing.T) {
	c := NewFakeClock(time.Unix(100, 0))
	_ = c.SleepUntil(context.Background(), time.Unix(110, 0))
	_ = c.SleepUntil(context.Background(), time.Unix(105, 0))
	if !c.Now().Equal(time.Unix(110, 0)) {
		t.Fatalf("%v", c.Now())
	}
}

// --- conductor ----------------------------------------------------------------------------------

func runConductor(t *testing.T, name string, workers int, scale float64, tweak func(*fakeCluster), after func(*FakeClock)) (Timeline, *fakeCluster, Params) {
	t.Helper()
	sc, _ := Lookup(name)
	p := sc.Params(workers, scale)
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	c := newFakeCluster(clock, workers)
	c.t0 = clock.Now()
	if tweak != nil {
		tweak(c)
	}
	cond := &conductor{clock: clock, t0: clock.Now(), p: p, sc: sc, env: &Env{C: c, P: p, Extra: noExtra{}},
		snap:  func() Counters { return Counters{} },
		state: func(context.Context) string { return "FAILED" }}
	clock.OnSleep = func(now time.Time) {
		if sc.Rolling && now.Sub(cond.t0) >= p.Warmup+p.Baseline+p.Fault {
			select { // a roll in the fake takes no time, but its goroutine must get to run
			case <-c.rollDone:
			case <-time.After(2 * time.Second):
			}
			time.Sleep(2 * time.Millisecond) // and to return
		}
		if after != nil {
			after(clock)
		}
	}
	return cond.run(context.Background()), c, p
}

type noExtra struct{}

func (noExtra) Start(int) {}
func (noExtra) Stop()     {}

func TestConductorAppliesAfterTheBaselineAndClearsAfterTheFault(t *testing.T) {
	tl, c, p := runConductor(t, "worker-freeze", 4, 0.2, nil, nil)
	if !tl.Applied || !tl.Cleared || tl.ApplyErr != nil || tl.ClearErr != nil {
		t.Fatalf("%+v", tl)
	}
	if tl.AppliedAt != p.Warmup+p.Baseline || tl.ClearedAt != p.Warmup+p.Baseline+p.Fault {
		t.Fatalf("applied at %v cleared at %v, want %v and %v", tl.AppliedAt, tl.ClearedAt, p.Warmup+p.Baseline, p.Warmup+p.Baseline+p.Fault)
	}
	if want := int(p.Total()/p.Bucket) + 1; len(tl.Ticks) != want {
		t.Fatalf("%d ticks, want %d", len(tl.Ticks), want)
	}
	for i, tk := range tl.Ticks {
		if tk.At != time.Duration(i)*p.Bucket {
			t.Fatalf("tick %d at %v", i, tk.At)
		}
	}
	if got := c.faults["worker-01"].Calls(); strings.Join(got, ",") != "blackhole true,blackhole false" {
		t.Fatalf("the freeze must blackhole the first worker and release it: %v", got)
	}
	if tl.ControlPlane != "FAILED" {
		t.Fatalf("the control plane state is read just before the fault is cleared: %q", tl.ControlPlane)
	}
	for _, other := range []string{"worker-02", "worker-03", "worker-04"} {
		if len(c.faults[other].Calls()) != 0 {
			t.Fatalf("%s was touched", other)
		}
	}
}

func TestConductorEveryScenarioTouchesOnlyItsFault(t *testing.T) {
	want := map[string]string{
		"worker-crash":                    "crash,restart",
		"worker-freeze":                   "blackhole true,blackhole false",
		"slow-worker-below-timeout":       "delay 1.5s,delay 0s",
		"slow-worker-above-timeout":       "delay 1.5s,delay 0s",
		"error-burst-unavailable":         "errorrate 1 unavailable,errorrate 0 unavailable",
		"error-burst-unavailable-partial": "errorrate 0.5 unavailable,errorrate 0 unavailable",
		"error-burst-500":                 "errorrate 1 error,errorrate 0 error",
		"midstream":                       "errorrate 0.5 midstream,errorrate 0 midstream",
		"conn-reset":                      "errorrate 1 drop,errorrate 0 drop",
	}
	for name, calls := range want {
		t.Run(name, func(t *testing.T) {
			tl, c, _ := runConductor(t, name, 4, 0.05, nil, nil)
			if !tl.Cleared {
				t.Fatalf("%+v", tl)
			}
			if got := strings.Join(c.faults["worker-01"].Calls(), ","); got != calls {
				t.Fatalf("got %q, want %q", got, calls)
			}
		})
	}
}

func TestConductorRollDrainsAndReplacesEveryWorkerInTurn(t *testing.T) {
	tl, c, _ := runConductor(t, "drain-roll", 4, 0.05, nil, nil)
	if !tl.Cleared {
		t.Fatalf("applied %v err %v cleared %v err %v ticks %d clearTick %d drained %v", tl.Applied, tl.ApplyErr, tl.Cleared, tl.ClearErr, len(tl.Ticks), tl.ClearTick, c.drained)
	}
	if strings.Join(c.drained, ",") != "worker-01,worker-02,worker-03,worker-04" || strings.Join(c.replace, ",") != "worker-01,worker-02,worker-03,worker-04" {
		t.Fatalf("drained %v replaced %v", c.drained, c.replace)
	}
}

func TestConductorAbandonsARollThatNeverFinishesAndDoesNotClear(t *testing.T) {
	sc, _ := Lookup("drain-roll")
	p := sc.Params(2, 0.05)
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	c := newFakeCluster(clock, 2)
	c.drainFn = func(ctx context.Context, _ string) error { <-ctx.Done(); return ctx.Err() } // the roll is stuck
	cond := &conductor{clock: clock, t0: clock.Now(), p: p, sc: sc, env: &Env{C: c, P: p, Extra: noExtra{}}, snap: func() Counters { return Counters{} }}
	finished := make(chan Timeline, 1)
	go func() { finished <- cond.run(context.Background()) }()
	select {
	case tl := <-finished:
		if tl.Cleared || !tl.Applied {
			t.Fatalf("a roll that never finished must not count as cleared: %+v", tl)
		}
		if !errors.Is(tl.ApplyErr, context.Canceled) {
			t.Fatalf("the abandoned roll's error is expected: %v", tl.ApplyErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the conductor waited for a stuck roll")
	}
}

func TestConductorStopsWhenTheFaultCannotBeApplied(t *testing.T) {
	tl, _, p := runConductor(t, "worker-crash", 4, 0.05, func(c *fakeCluster) { delete(c.faults, "worker-01") }, nil)
	if tl.ApplyErr == nil || tl.Cleared {
		t.Fatalf("%+v", tl)
	}
	if len(tl.Ticks) > int((p.Warmup+p.Baseline)/p.Bucket)+2 {
		t.Fatalf("kept ticking after a failed apply: %d ticks", len(tl.Ticks))
	}
}

func TestConductorSeesAClockStep(t *testing.T) {
	tl, _, p := runConductor(t, "worker-freeze", 4, 0.05, nil, func(c *FakeClock) {
		if c.Now().Sub(time.Unix(1_700_000_000, 0)) > p0Mid {
			c.SetWallOffset(40 * time.Minute) // the laptop slept
		}
	})
	_ = p
	if tl.ClockStep < 40*time.Minute {
		t.Fatalf("clock step %v", tl.ClockStep)
	}
}

const p0Mid = 2 * time.Second

func TestConductorStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sc, _ := Lookup("worker-freeze")
	p := sc.Params(4, 0.05)
	clock := NewFakeClock(time.Unix(1, 0))
	c := newFakeCluster(clock, 4)
	cond := &conductor{clock: clock, t0: clock.Now(), p: p, sc: sc, env: &Env{C: c, P: p, Extra: noExtra{}}, snap: func() Counters { return Counters{} }}
	if tl := cond.run(ctx); tl.Applied || len(tl.Ticks) != 0 {
		t.Fatalf("%+v", tl)
	}
}

// --- analysis ---------------------------------------------------------------------------------

func ticksOf(p Params, n int, f func(i int) Counters) []Snapshot {
	var out []Snapshot
	for i := 0; i <= n; i++ {
		out = append(out, Snapshot{At: time.Duration(i) * p.Bucket, Counters: f(i)})
	}
	return out
}

func TestTimeToIsolate(t *testing.T) {
	p := newParams(1)
	// fault applied at tick 10, cleared at tick 30; the faulted worker is selected 5 times per tick until
	// tick 14, then never.
	sel := func(i int) Counters {
		n := float64(min(max(i-10, 0), 4) * 5)
		return Counters{FaultySelections: 100 + n}
	}
	tl := Timeline{Ticks: ticksOf(p, 50, sel), ApplyTick: 10, ClearTick: 30, Applied: true, Cleared: true, AppliedAt: 10 * time.Second, ClearedAt: 30 * time.Second}
	got, note := timeToIsolate(p, tl, true)
	if got == nil || *got != 4 {
		t.Fatalf("isolated after 4s expected, got %v (%s)", got, note)
	}
	// Never isolated: still selected in the last bucket.
	tl.Ticks = ticksOf(p, 50, func(i int) Counters { return Counters{FaultySelections: float64(i * 5)} })
	if got, note := timeToIsolate(p, tl, true); got != nil || note == "" {
		t.Fatalf("a worker selected until the end of the fault is not isolated: %v %q", got, note)
	}
	// Probes do not count against isolation: one selection per tick that is a probe.
	tl.Ticks = ticksOf(p, 50, func(i int) Counters { return Counters{FaultySelections: float64(i), Probes: float64(i)} })
	if got, _ := timeToIsolate(p, tl, true); got == nil || *got != 0 {
		t.Fatalf("probes must be ignored: %v", got)
	}
	if got, note := timeToIsolate(p, tl, false); got != nil || note == "" {
		t.Fatal("no single faulted worker: not defined")
	}
}

func TestTimeToRecover(t *testing.T) {
	p := newParams(1)
	mk := func(okFrom int) []report.ChaosBucket {
		var s []report.ChaosBucket
		for i := 0; i < 53-3; i++ {
			b := report.ChaosBucket{StartSeconds: float64(3 + i), Succeeded: 20}
			if i+3 < okFrom {
				b.Failed = 4
			}
			s = append(s, b)
		}
		return s
	}
	tl := Timeline{Cleared: true, ClearedAt: 33 * time.Second}
	got, _ := timeToRecover(p, tl, mk(38), 1)
	if got == nil || *got != 5 {
		t.Fatalf("recovered from the bucket at 38s, 5s after the fault ended: %v", got)
	}
	// A baseline ratio below 1 tolerates the same few failures.
	if got, _ := timeToRecover(p, tl, mk(60), 20.0/24); got == nil || *got != 0 {
		t.Fatalf("%v", got)
	}
	// Buckets with fewer than 5 requests never count.
	thin := mk(0)
	for i := range thin {
		thin[i].Succeeded = 3
	}
	if got, note := timeToRecover(p, tl, thin, 1); got != nil || note == "" {
		t.Fatalf("%v %q", got, note)
	}
	if got, _ := timeToRecover(p, Timeline{}, mk(0), 1); got != nil {
		t.Fatal("a fault that was never cleared has no recovery")
	}
}

func TestSummarizeWindowSeparatesSuccessesFromFailuresAndCountsAttempts(t *testing.T) {
	w := collect.Window{Start: 10 * time.Second, End: 20 * time.Second}
	var recs []collect.Record
	add := func(at time.Duration, status int, dur time.Duration, attempts int) {
		recs = append(recs, collect.Record{Intended: at, Started: at, Done: at + dur, Status: status, Attempts: attempts})
	}
	add(5*time.Second, 200, 10*time.Millisecond, 0) // before the window
	add(11*time.Second, 200, 100*time.Millisecond, 0)
	add(12*time.Second, 200, 200*time.Millisecond, 2) // retried
	add(13*time.Second, 504, 2*time.Second, 0)
	add(25*time.Second, 200, 10*time.Millisecond, 0) // after the window
	cw := summarizeWindow("fault", w, recs, Counters{Attempts: 9, Requests: 6, NoCapacity: 1})
	if cw.Sent != 3 || cw.Succeeded != 2 || cw.Failed != 1 || cw.Retried != 1 || cw.SuccessRate < 0.66 || cw.SuccessRate > 0.67 {
		t.Fatalf("%+v", cw)
	}
	if cw.AttemptsPerRequest == nil || *cw.AttemptsPerRequest != 1.5 {
		t.Fatalf("attempts per request %v", cw.AttemptsPerRequest)
	}
	if cw.FailureP50 == nil || *cw.FailureP50 != 2000 || cw.LatencyP95 == nil || *cw.LatencyP95 != 200 {
		t.Fatalf("%v %v", cw.FailureP50, cw.LatencyP95)
	}
	if empty := summarizeWindow("x", w, nil, Counters{}); empty.AttemptsPerRequest != nil || empty.SuccessRate != 0 {
		t.Fatalf("%+v", empty)
	}
}

func TestWindowsFollowWhatTheConductorDid(t *testing.T) {
	p := newParams(1)
	late := Timeline{Applied: true, Cleared: true, AppliedAt: 13*time.Second + 200*time.Millisecond, ClearedAt: 33*time.Second + 800*time.Millisecond}
	w := windowsOf(p, late)
	if w.Baseline.End != late.AppliedAt || w.Fault.Start != late.AppliedAt || w.Fault.End != late.ClearedAt || w.Recovery.Start != late.ClearedAt {
		t.Fatalf("%+v", w)
	}
	if w.Baseline.Start != p.Warmup || w.Recovery.End != p.Total() {
		t.Fatalf("%+v", w)
	}
	planned := windowsOf(p, Timeline{})
	if planned.Fault.Start != 13*time.Second || planned.Fault.End != 33*time.Second {
		t.Fatalf("without a timeline the plan is used: %+v", planned)
	}
}

func TestSeriesBucketsByCompletionTime(t *testing.T) {
	p := newParams(1)
	tl := Timeline{Ticks: ticksOf(p, 53, func(i int) Counters { return Counters{FaultySelections: float64(i * 2), Probes: float64(i)} })}
	recs := []collect.Record{
		{Done: 2 * time.Second, Status: 200},                      // warm-up: left out
		{Done: 3*time.Second + time.Millisecond, Status: 200},     // first bucket
		{Done: 3*time.Second + 500*time.Millisecond, Status: 503}, // first bucket
		{Done: 52*time.Second + 999*time.Millisecond, Status: 200},
		{Done: 60 * time.Second, Status: 200}, // after the run: left out
	}
	s := series(p, tl, recs)
	if len(s) != 50 || s[0].StartSeconds != 3 || s[0].Succeeded != 1 || s[0].Failed != 1 || s[49].Succeeded != 1 {
		t.Fatalf("%d buckets, first %+v last %+v", len(s), s[0], s[len(s)-1])
	}
	if s[0].FaultySelections != 2 || s[0].Probes != 1 {
		t.Fatalf("%+v", s[0])
	}
}

// --- validity gate --------------------------------------------------------------------------------

func goodGate() GateInput {
	sc, _ := Lookup("worker-freeze")
	good := report.ChaosWindow{Sent: 500, Succeeded: 500, SuccessRate: 1}
	return GateInput{
		Scenario: sc, Options: Options{MaxErrorRate: 0.05, MinRequests: 100}, Plan: Clients(sc, specs(4, 8), 0), Params: newParams(1),
		Timeline: Timeline{Applied: true, Cleared: true}, Windows: []report.ChaosWindow{good, {Name: "fault", Sent: 400, Succeeded: 300}, good},
		Evidence: Evidence{Stats: mockworker.FaultStats{Blackholed: 12}}, Settled: 600 * time.Millisecond, GitTree: "clean", Commit: "0123456789abcdef", DrivenOK: true,
	}
}

func failed(in GateInput) string {
	return strings.Join(FailedChecks(Gate(in)), " | ")
}

func TestGateAcceptsAGoodRun(t *testing.T) {
	if f := failed(goodGate()); f != "" {
		t.Fatal(f)
	}
}

func TestGateRejectsBadRuns(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*GateInput)
		want   string
	}{
		"baseline errors":     {func(in *GateInput) { in.Windows[0].Succeeded, in.Windows[0].SuccessRate = 400, 0.8 }, "baseline error rate"},
		"empty baseline":      {func(in *GateInput) { in.Windows[0] = report.ChaosWindow{} }, "baseline error rate"},
		"fault not evidenced": {func(in *GateInput) { in.Evidence = Evidence{} }, "fault evidenced"},
		"too many clients":    {func(in *GateInput) { in.Plan.Base = 20 }, "clients within the surviving slots"},
		"too few requests":    {func(in *GateInput) { in.Windows[2].Sent = 10 }, "minimum requests per window"},
		"not settled":         {func(in *GateInput) { in.Settled = 100 * time.Millisecond }, "cluster settled"},
		"dirty tree":          {func(in *GateInput) { in.GitTree = "dirty" }, "clean working tree"},
		"unknown tree":        {func(in *GateInput) { in.GitTree = "unknown" }, "clean working tree"},
		"no commit":           {func(in *GateInput) { in.Commit = "unknown" }, "clean working tree"},
		"clock step":          {func(in *GateInput) { in.Timeline.ClockStep = 300 * time.Millisecond }, "no sleep or clock step"},
		"not cleared":         {func(in *GateInput) { in.Timeline.Cleared = false }, "fault applied and cleared"},
		"apply failed":        {func(in *GateInput) { in.Timeline.ApplyErr = errors.New("boom") }, "fault applied and cleared"},
		"driver failed":       {func(in *GateInput) { in.DrivenOK = false }, "load driver"},
	} {
		t.Run(name, func(t *testing.T) {
			in := goodGate()
			tc.mutate(&in)
			if f := failed(in); !strings.Contains(f, tc.want) {
				t.Fatalf("expected %q to fail the gate, got %q", tc.want, f)
			}
		})
	}
}

func TestGateClockStepLimitIsExact(t *testing.T) {
	in := goodGate()
	in.Timeline.ClockStep = MaxClockStep
	if f := failed(in); f != "" {
		t.Fatalf("exactly %v is allowed: %s", MaxClockStep, f)
	}
	in.Timeline.ClockStep = MaxClockStep + time.Millisecond
	if f := failed(in); f == "" {
		t.Fatal("one millisecond over must fail")
	}
}

func TestGateAllowDirtyWaivesOnlyTheTree(t *testing.T) {
	in := goodGate()
	in.GitTree, in.Options.AllowDirty = "dirty", true
	if f := failed(in); f != "" {
		t.Fatal(f)
	}
	for _, c := range Gate(in) {
		if c.Name == "clean working tree" && !strings.Contains(c.Detail, "WAIVED") {
			t.Fatalf("a waived check must say so: %s", c.Detail)
		}
	}
	in.Evidence = Evidence{}
	if f := failed(in); !strings.Contains(f, "fault evidenced") {
		t.Fatal("--allow-dirty must not waive anything else")
	}
}

func TestGateOverloadInvertsTheClientCheck(t *testing.T) {
	sc, _ := Lookup("overload")
	in := goodGate()
	in.Scenario = sc
	in.Plan = Clients(sc, specs(4, 8), 0)
	in.Evidence = Evidence{WindowFailures: 40}
	if f := failed(in); f != "" {
		t.Fatalf("1.5 times the slots is what an overload needs: %s", f)
	}
	in.Plan = ClientPlan{Base: 12, Slots: 32, Surviving: 32}
	if f := failed(in); !strings.Contains(f, "clients exceed slots") {
		t.Fatalf("an overload with fewer clients than slots is not an overload: %q", f)
	}
	in.Plan = Clients(sc, specs(4, 8), 0)
	in.Evidence = Evidence{}
	if f := failed(in); !strings.Contains(f, "fault evidenced") {
		t.Fatalf("an overload that refused nothing is not evidenced: %q", f)
	}
}

func TestEvidencePredicates(t *testing.T) {
	cases := map[string]Evidence{
		"worker-crash":              {Stats: mockworker.FaultStats{Crashes: 1}},
		"worker-freeze":             {Stats: mockworker.FaultStats{Blackholed: 1}},
		"slow-worker-below-timeout": {Stats: mockworker.FaultStats{Delayed: 1}},
		"slow-worker-above-timeout": {Stats: mockworker.FaultStats{Delayed: 1}},
		"error-burst-500":           {Stats: mockworker.FaultStats{Errored: 1}},
		"midstream":                 {Stats: mockworker.FaultStats{Errored: 1}},
		"conn-reset":                {Stats: mockworker.FaultStats{Errored: 1}},
		"drain-roll":                {Drains: 4},
		"overload":                  {GatewayNoCapacity: 3},
	}
	for name, ev := range cases {
		sc, _ := Lookup(name)
		if ok, _ := sc.evidenced(ev); !ok {
			t.Errorf("%s: evidence %+v not accepted", name, ev)
		}
		if ok, _ := sc.evidenced(Evidence{}); ok {
			t.Errorf("%s: no evidence was accepted", name)
		}
	}
}

// --- pipeline with fakes -----------------------------------------------------------------------

func fakeRun(t *testing.T, name string, o Options, d Deps, c *fakeCluster, loader *fakeLoader) report.Result {
	t.Helper()
	sc, _ := Lookup(name)
	p := sc.Params(o.Workers, o.Scale)
	c.faultFrom, c.faultTo = (p.Warmup + p.Baseline).Seconds(), (p.Warmup + p.Baseline + p.Fault).Seconds()
	loader.failFrom, loader.failTo = p.Warmup+p.Baseline, p.Warmup+p.Baseline+p.Fault
	d.Load = loader
	res, recs, err := RunOnce(context.Background(), o, sc, d, "run_001", report.Repeat{Index: 1, Of: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("no records")
	}
	return res
}

func TestPipelineProducesAValidChaosResult(t *testing.T) {
	o := testOptions(t, "worker-freeze")
	d, c, _ := fakeDeps(t, 4, func(c *fakeCluster) { c.isolateAt = 5 })
	res := fakeRun(t, "worker-freeze", o, d, c, &fakeLoader{failEvery: 4})
	if !res.Valid || len(res.InvalidReasons) != 0 {
		t.Fatalf("invalid: %v", res.InvalidReasons)
	}
	ch := res.Chaos
	if ch == nil || len(ch.Windows) != 3 || ch.Scenario != "worker-freeze" || ch.FaultWorker != "worker-01" {
		t.Fatalf("%+v", ch)
	}
	base, fault, rec := ch.Windows[0], ch.Windows[1], ch.Windows[2]
	if base.Failed != 0 || rec.Failed != 0 || fault.Failed == 0 {
		t.Fatalf("failures must fall in the fault window only: baseline %d fault %d recovery %d", base.Failed, fault.Failed, rec.Failed)
	}
	if ch.Measures.FaultSuccessRate != fault.SuccessRate || ch.Measures.FaultSuccessRate < 0.7 || ch.Measures.FaultSuccessRate > 0.8 {
		t.Fatalf("primary success rate %v (fault window %v)", ch.Measures.FaultSuccessRate, fault.SuccessRate)
	}
	if a := ch.Measures.FaultAttemptsPerRequest; a == nil || *a != 1.1 {
		t.Fatalf("attempts per request in the fault window %v, want 1.1 (100 requests/s, 10 extra attempts/s)", a)
	}
	if b := base.AttemptsPerRequest; b == nil || *b != 1 {
		t.Fatalf("baseline attempts per request %v", b)
	}
	if ch.Measures.TimeToIsolate == nil || ch.Measures.TimeToRecover == nil {
		t.Fatalf("%+v", ch.Measures)
	}
	if ch.Evidence.Blackholed == 0 || !ch.Evidence.Applied || ch.Evidence.SurvivingSlots != 24 || ch.Evidence.Clients != 14 {
		t.Fatalf("%+v", ch.Evidence)
	}
	if res.Metadata.Resilience != "absent" || res.Metadata.Target != "embedded" || res.Metadata.Concurrency != 14 {
		t.Fatalf("metadata %+v", res.Metadata)
	}
	if res.Summary.Sent == 0 || len(ch.Series) == 0 || !c.closed.Load() {
		t.Fatal("the whole-run summary and series are expected, and the cluster must be closed")
	}
	if got := strings.Join(c.faults["worker-01"].Calls(), ","); got != "blackhole true,blackhole false" {
		t.Fatalf("%s", got)
	}
}

func TestPipelineMarksTheRunInvalidWhenTheGateFails(t *testing.T) {
	cases := map[string]struct {
		tweak func(*fakeCluster)
		deps  func(*Deps)
		opts  func(*Options)
		load  *fakeLoader
		want  string
	}{
		"dirty tree": {deps: func(d *Deps) {
			d.Git = func(context.Context) report.GitState { return report.GitState{Commit: "abc", Tree: "dirty"} }
		}, load: &fakeLoader{failEvery: 4}, want: "clean working tree"},
		"slept laptop": {tweak: func(c *fakeCluster) {
			c.clock.OnSleep = func(now time.Time) {
				if now.Sub(c.t0) > 3*time.Second {
					c.clock.SetWallOffset(10 * time.Minute)
				}
			}
		}, load: &fakeLoader{failEvery: 4}, want: "no sleep or clock step"},
		"baseline already failing": {load: &fakeLoader{failEvery: 2, failFrom: 0}, want: "baseline error rate"},
		"too many clients":         {opts: func(o *Options) { o.Concurrency = 30 }, load: &fakeLoader{failEvery: 4}, want: "clients within the surviving slots"},
		"load driver error":        {load: &fakeLoader{err: errors.New("boom")}, want: "load driver"},
		"unsettled":                {tweak: func(c *fakeCluster) { c.settled = 0 }, load: &fakeLoader{failEvery: 4}, want: "cluster settled"},
		"too short windows":        {opts: func(o *Options) { o.MinRequests = 100000 }, load: &fakeLoader{failEvery: 4}, want: "minimum requests per window"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			o := testOptions(t, "worker-freeze")
			if tc.opts != nil {
				tc.opts(&o)
			}
			d, c, _ := fakeDeps(t, 4, tc.tweak)
			if tc.deps != nil {
				tc.deps(&d)
			}
			sc, _ := Lookup("worker-freeze")
			p := sc.Params(o.Workers, o.Scale)
			c.faultFrom, c.faultTo = (p.Warmup + p.Baseline).Seconds(), (p.Warmup + p.Baseline + p.Fault).Seconds()
			if tc.load.failFrom == 0 && tc.load.failTo == 0 && name != "baseline already failing" {
				tc.load.failFrom, tc.load.failTo = p.Warmup+p.Baseline, p.Warmup+p.Baseline+p.Fault
			}
			if name == "baseline already failing" {
				tc.load.failTo = p.Total() + p.Warmup
			}
			d.Load = tc.load
			res, _, err := RunOnce(context.Background(), o, sc, d, "run_001", report.Repeat{Index: 1, Of: 1})
			if err != nil {
				t.Fatal(err)
			}
			if res.Valid {
				t.Fatalf("a run with %q must be invalid", name)
			}
			if !strings.Contains(strings.Join(res.InvalidReasons, " | "), tc.want) {
				t.Fatalf("reasons %v lack %q", res.InvalidReasons, tc.want)
			}
		})
	}
}

// ignoringFaults accepts every call and does nothing, like a mock that ignored the fault.
type ignoringFaults struct{ fakeFaults }

func (f *ignoringFaults) SetBlackhole(bool) {}

func TestPipelineRejectsAFaultTheWorkerIgnored(t *testing.T) {
	o := testOptions(t, "worker-freeze")
	d, c, _ := fakeDeps(t, 4, nil)
	ign := &ignoringFaults{}
	sc, _ := Lookup("worker-freeze")
	p := sc.Params(o.Workers, o.Scale)
	l := &fakeLoader{failEvery: 4, failFrom: p.Warmup + p.Baseline, failTo: p.Warmup + p.Baseline + p.Fault}
	d.Load = l
	d2 := d
	d2.Boot = func(ctx context.Context, cfg embedded.Config) (Cluster, error) {
		cl, err := d.Boot(ctx, cfg)
		return &swapFaults{Cluster: cl, h: ign}, err
	}
	_ = c
	res, _, err := RunOnce(context.Background(), o, sc, d2, "run_001", report.Repeat{Index: 1, Of: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid || !strings.Contains(strings.Join(res.InvalidReasons, "|"), "fault evidenced") {
		t.Fatalf("a fault no counter shows must invalidate the run: %v", res.InvalidReasons)
	}
}

type swapFaults struct {
	Cluster
	h FaultHandle
}

func (s *swapFaults) Faults(string) FaultHandle { return s.h }

func TestPipelineRecordsTheResilienceValueTheClusterReports(t *testing.T) {
	for _, v := range []string{"absent", "on", "off"} {
		o := testOptions(t, "worker-crash")
		d, c, _ := fakeDeps(t, 4, func(c *fakeCluster) { c.resilience = v })
		res := fakeRun(t, "worker-crash", o, d, c, &fakeLoader{failEvery: 4})
		if res.Metadata.Resilience != v {
			t.Fatalf("recorded %q, want %q", res.Metadata.Resilience, v)
		}
	}
}

func TestPipelineSwappedWindowsWouldBeSeen(t *testing.T) {
	// The synthetic failures sit in the fault window; if the windows were taken in the wrong order the baseline
	// would show them. This pins which window holds which failures.
	o := testOptions(t, "worker-freeze")
	d, c, _ := fakeDeps(t, 4, nil)
	res := fakeRun(t, "worker-freeze", o, d, c, &fakeLoader{failEvery: 4})
	w := res.Chaos.Windows
	if w[0].Name != "baseline" || w[1].Name != "fault" || w[2].Name != "recovery" {
		t.Fatalf("%v %v %v", w[0].Name, w[1].Name, w[2].Name)
	}
	if !(w[0].EndSeconds <= w[1].StartSeconds+1e-9 && w[1].EndSeconds <= w[2].StartSeconds+1e-9) || w[1].StartSeconds != res.Chaos.FaultAppliedSeconds {
		t.Fatalf("windows %+v must follow the fault's real start and end", w)
	}
	if w[1].Failed == 0 || w[0].Failed+w[2].Failed != 0 {
		t.Fatalf("fault window failures %d, others %d", w[1].Failed, w[0].Failed+w[2].Failed)
	}
}

func TestPipelineOverloadAddsClientsOnlyForTheFaultWindow(t *testing.T) {
	o := testOptions(t, "overload")
	d, c, _ := fakeDeps(t, 4, nil)
	loader := &fakeLoader{}
	res := fakeRun(t, "overload", o, d, c, loader)
	if loader.calls.Load() != 2 {
		t.Fatalf("overload drives the base load and an extra load: %d drives", loader.calls.Load())
	}
	if res.Chaos.Evidence.Clients != 48 {
		t.Fatalf("clients in the fault window %d, want 1.5 x 32 slots", res.Chaos.Evidence.Clients)
	}
}

func TestPipelineBootFailureIsAnError(t *testing.T) {
	o := testOptions(t, "worker-freeze")
	d, _, _ := fakeDeps(t, 4, nil)
	d.Boot = func(context.Context, embedded.Config) (Cluster, error) { return nil, errors.New("no ports") }
	sc, _ := Lookup("worker-freeze")
	if _, _, err := RunOnce(context.Background(), o, sc, d, "run_001", report.Repeat{Index: 1, Of: 1}); err == nil {
		t.Fatal("want an error")
	}
}

func TestRunWritesResultsAndReportsInvalidRuns(t *testing.T) {
	o := testOptions(t, "worker-freeze")
	o.Repeat = 2
	d, c, _ := fakeDeps(t, 4, nil)
	sc, _ := Lookup("worker-freeze")
	p := sc.Params(o.Workers, o.Scale)
	c.faultFrom, c.faultTo = (p.Warmup + p.Baseline).Seconds(), (p.Warmup + p.Baseline + p.Fault).Seconds()
	d.Load = &fakeLoader{failEvery: 4, failFrom: p.Warmup + p.Baseline, failTo: p.Warmup + p.Baseline + p.Fault}
	// A second Boot registers the same fake counters again, so each repeat gets a fresh cluster.
	boot := d.Boot
	d.Boot = func(ctx context.Context, cfg embedded.Config) (Cluster, error) { return boot(ctx, cfg) }
	var out strings.Builder
	results, err := Run(context.Background(), o, d, &out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if len(results) != 2 || results[0].Metadata.Repeat.Group != results[1].Metadata.Repeat.Group {
		t.Fatalf("%d results", len(results))
	}
	for _, id := range []string{"run_001", "run_002"} {
		r, err := report.Load(o.OutDir + "/" + id)
		if err != nil || r.Chaos == nil || !r.Valid {
			t.Fatalf("%s: %v %+v", id, err, r.Chaos)
		}
	}
	for _, want := range []string{"fault-window success rate", "(primary)", "Only 2 valid run(s)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}

	// The same run with a dirty tree is recorded as invalid and Run says so.
	o2 := testOptions(t, "worker-freeze")
	d.Git = func(context.Context) report.GitState { return report.GitState{Commit: "x", Tree: "dirty"} }
	out.Reset()
	results, err = Run(context.Background(), o2, d, &out)
	if err == nil || !strings.Contains(err.Error(), "invalid") || len(results) != 1 || results[0].Valid {
		t.Fatalf("%v %v", err, results)
	}
}

func TestSpreadSummaryListsInvalidRunsAndLeavesThemOut(t *testing.T) {
	sc, _ := Lookup("worker-freeze")
	mk := func(id string, valid bool, rate float64) report.Result {
		att := 1.1
		r := report.Result{RunID: id, Valid: valid, Chaos: &report.Chaos{Measures: report.ChaosMeasures{FaultSuccessRate: rate, FaultAttemptsPerRequest: &att}}}
		r.Metadata.Resilience = "absent"
		if !valid {
			r.InvalidReasons = []string{"validity gate: clean working tree: dirty"}
		}
		return r
	}
	s := SpreadSummary(sc, []report.Result{mk("run_001", true, 0.8), mk("run_002", true, 0.9), mk("run_003", true, 0.7), mk("run_004", false, 0.1)})
	for _, want := range []string{"4 run(s), 3 valid", "run_004 is INVALID and left out", "70.00 / 80.00 / 90.00", "not reached in any run"} {
		if !strings.Contains(s, want) {
			t.Errorf("lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "fewer than 3") {
		t.Error("three valid runs are enough")
	}
	if s := SpreadSummary(sc, []report.Result{mk("run_001", true, 0.8)}); !strings.Contains(s, "mostly luck") {
		t.Error("one run must be flagged as no basis for a comparison")
	}
	if s := SpreadSummary(sc, []report.Result{mk("run_001", false, 0.8)}); strings.Contains(s, "Min / median") {
		t.Error("no valid run, no numbers")
	}
}

func TestReadCountersSumsTheGatewaySeriesAndToleratesAbsentOnes(t *testing.T) {
	d, c, _ := fakeDeps(t, 4, nil)
	_, _ = d.Boot(context.Background(), embedded.Config{})
	c.clock.SleepUntil(context.Background(), c.t0.Add(2*time.Second)) //nolint:errcheck
	got, err := readCounters(c.Metrics(), "worker-01")
	if err != nil {
		t.Fatal(err)
	}
	if got.Requests != 200 || got.Attempts != 200 || got.FaultySelections != 50 || got.Probes != 0 {
		t.Fatalf("%+v", got)
	}
	if none, _ := readCounters(c.Metrics(), ""); none.FaultySelections != 0 {
		t.Fatalf("no faulted worker, no selections: %+v", none)
	}
}
