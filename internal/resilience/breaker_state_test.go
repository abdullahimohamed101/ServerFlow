package resilience

import (
	"testing"
	"time"
)

func tripConsecutive(r *rig, id string) {
	r.t.Helper()
	r.sendN(id, Fault, 5)
	r.want(id, Open)
}

func TestOpenUntilTheCoolDownElapsed(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")
	if r.bs.Admissible(key("w1")) {
		t.Fatal("an open breaker must not be admissible")
	}
	if _, ok := r.bs.Acquire(key("w1")); ok {
		t.Fatal("an open breaker must not hand out tickets")
	}
	r.clk.Advance(10*time.Second - time.Nanosecond)
	if r.bs.Admissible(key("w1")) {
		t.Fatal("still open one nanosecond before the cool-down")
	}
	r.clk.Advance(time.Nanosecond)
	if !r.bs.Admissible(key("w1")) {
		t.Fatal("half-open with a free probe slot is admissible")
	}
	r.want("w1", HalfOpen)
	if len(r.changes) != 2 || r.changes[0].To != Open || r.changes[0].Reason != ReasonConsecutive || r.changes[0].Failures != 5 ||
		r.changes[1].From != Open || r.changes[1].To != HalfOpen || r.changes[1].Reason != ReasonCoolDown {
		t.Fatalf("changes = %+v", r.changes)
	}
}

func TestRatioTripReportsItsReason(t *testing.T) {
	p := testParams()
	p.ConsecutiveFailures = 100
	r := newRig(t, p, "w1")
	r.sendN("w1", RatioFault, 10)
	r.want("w1", Open)
	if r.changes[0].Reason != ReasonRatio || r.changes[0].Failures != 10 {
		t.Fatalf("change = %+v", r.changes[0])
	}
}

func TestCoolDownIsJittered(t *testing.T) {
	for _, tc := range []struct {
		rand float64
		want time.Duration
	}{{0, 8 * time.Second}, {0.5, 10 * time.Second}, {0.75, 11 * time.Second}} {
		r := newRig(t, testParams(), "w1")
		r.rnd.push(tc.rand)
		tripConsecutive(r, "w1")
		if got := r.openFor("w1"); got < tc.want-time.Microsecond || got > tc.want+time.Microsecond {
			t.Errorf("rand %v: open for %v, want %v", tc.rand, got, tc.want)
		}
	}
}

func TestJitterNeverExceedsTheCap(t *testing.T) {
	p := testParams()
	p.CoolDown, p.MaxCoolDown = time.Minute, time.Minute
	r := newRig(t, p, "w1")
	r.rnd.push(0.999)
	tripConsecutive(r, "w1")
	if got := r.openFor("w1"); got > time.Minute {
		t.Fatalf("open for %v, above the cap", got)
	}
}

func TestHalfOpenAdmitsExactlyProbeConcurrency(t *testing.T) {
	p := testParams()
	p.ProbeConcurrency = 2
	r := newRig(t, p, "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	t1, ok1 := r.bs.Acquire(key("w1"))
	t2, ok2 := r.bs.Acquire(key("w1"))
	if !ok1 || !ok2 || !t1.Probe() || !t2.Probe() {
		t.Fatalf("two probes expected: %v %v", ok1, ok2)
	}
	if _, ok := r.bs.Acquire(key("w1")); ok {
		t.Fatal("a third probe exceeds probe_concurrency")
	}
	if r.bs.Admissible(key("w1")) {
		t.Fatal("not admissible while all probe slots are taken")
	}
	r.bs.Release(t1)
	r.bs.Release(t1) // idempotent
	if _, ok := r.bs.Acquire(key("w1")); !ok {
		t.Fatal("a released ticket frees its slot")
	}
	if _, ok := r.bs.Acquire(key("w1")); ok {
		t.Fatal("exactly one slot was freed")
	}
	_ = t2
}

func TestProbeSuccessesCloseAndReset(t *testing.T) {
	p := testParams()
	p.ProbeSuccesses = 2
	r := newRig(t, p, "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	r.send("w1", OK)
	r.want("w1", HalfOpen)
	r.send("w1", OK)
	r.want("w1", Closed)
	if last := r.changes[len(r.changes)-1]; last.To != Closed || last.Reason != ReasonProbesPassed {
		t.Fatalf("last change = %+v", last)
	}
	// The window and the streak start over: four faults are not five.
	r.sendN("w1", Fault, 4)
	r.want("w1", Closed)
}

func TestFailedProbeReopensWithDoublingCoolDownUpToTheCap(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")
	want := []time.Duration{20 * time.Second, 40 * time.Second, 80 * time.Second, 2 * time.Minute, 2 * time.Minute}
	r.clk.Advance(10 * time.Second)
	for i, d := range want {
		verdict := []Verdict{Fault, RatioFault}[i%2] // an unlisted 5xx also fails a probe
		tk, ok := r.bs.Acquire(key("w1"))
		if !ok || !tk.Probe() {
			t.Fatalf("round %d: probe refused", i)
		}
		r.bs.Record(tk, verdict)
		r.want("w1", Open)
		if got := r.openFor("w1"); got != d {
			t.Fatalf("round %d: open for %v, want %v", i, got, d)
		}
		r.clk.Advance(d)
	}
}

func TestNeutralProbeReturnsTheTicket(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	r.send("w1", Neutral)
	r.want("w1", HalfOpen)
	if _, ok := r.bs.Acquire(key("w1")); !ok {
		t.Fatal("a neutral verdict must return the ticket")
	}
}

func TestProbeTicketIsReturnedExactlyOnce(t *testing.T) {
	p := testParams()
	p.ProbeSuccesses = 2
	r := newRig(t, p, "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	tk, _ := r.bs.Acquire(key("w1"))
	r.bs.Record(tk, OK)
	r.bs.Record(tk, OK) // a second return must not count as a second success
	r.want("w1", HalfOpen)
	r.bs.Release(tk) // nor free anything
	t2, ok := r.bs.Acquire(key("w1"))
	if !ok {
		t.Fatal("slot should be free after the first return")
	}
	if _, ok := r.bs.Acquire(key("w1")); ok {
		t.Fatal("double return freed an extra slot")
	}
	r.bs.Record(t2, OK)
	r.want("w1", Closed)
}

func TestReleasedTicketCannotRecordLater(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	tk, _ := r.bs.Acquire(key("w1"))
	r.bs.Release(tk)
	r.bs.Record(tk, OK)
	r.want("w1", HalfOpen)
}

func TestTicketFromAnEarlierOpenPeriodIsStale(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	old, _ := r.bs.Acquire(key("w1"))
	probe2, _ := r.bs.Acquire(key("w1")) // refused (concurrency 1): keeps zero value
	_ = probe2
	r.bs.Record(old, Fault) // reopen, 20 s
	r.clk.Advance(20 * time.Second)
	r.want("w1", HalfOpen)
	r.bs.Record(old, OK) // late echo of the old period
	r.want("w1", HalfOpen)
}

func TestLostTicketBackstop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header time.Duration
		after  time.Duration // backstop = max(2 x cool_down, header + 5 s)
	}{
		{"two cool-downs", 10 * time.Second, 20 * time.Second},
		{"header timeout plus five seconds", 60 * time.Second, 65 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testParams()
			p.HeaderTimeout = tc.header
			r := newRig(t, p, "w1")
			tripConsecutive(r, "w1")
			r.clk.Advance(10 * time.Second)
			if _, ok := r.bs.Acquire(key("w1")); !ok {
				t.Fatal("probe refused")
			}
			r.clk.Advance(tc.after) // exactly the backstop: still held
			if r.bs.Admissible(key("w1")) {
				t.Fatal("ticket released too early")
			}
			r.clk.Advance(time.Nanosecond)
			if !r.bs.Admissible(key("w1")) {
				t.Fatal("a ticket outstanding past the backstop must be released")
			}
		})
	}
}

func TestLateFaultCountsOnlyWhileClosed(t *testing.T) {
	p := testParams()
	p.FailureRatio = 0
	r := newRig(t, p, "w1")
	var tickets []Ticket
	for i := 0; i < 5; i++ {
		tk, ok := r.bs.Acquire(key("w1"))
		if !ok {
			t.Fatal("acquire")
		}
		r.bs.Record(tk, OK) // the first byte arrived: verdict OK
		tickets = append(tickets, tk)
	}
	for _, tk := range tickets { // then each stream aborted
		r.bs.RecordLate(tk, Fault)
	}
	r.want("w1", Open)
	n := len(r.changes)
	r.bs.RecordLate(tickets[0], Fault) // open: ignored
	if len(r.changes) != n {
		t.Fatal("a late fault must not change an open breaker")
	}
	r.clk.Advance(10 * time.Second)
	r.bs.RecordLate(tickets[0], Fault) // a request from before the breaker opened: ignored when half-open
	r.want("w1", HalfOpen)
	if _, ok := r.bs.Acquire(key("w1")); !ok {
		t.Fatal("probe slot must still be free")
	}
}

// A probe stream that aborts after its first byte must not count as a success (probe_successes > 1) and must
// reopen the breaker with a doubled cool-down.
func TestLateFaultOfAProbeReopensTheBreaker(t *testing.T) {
	p := testParams()
	p.ProbeSuccesses = 2
	r := newRig(t, p, "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	probe, _ := r.bs.Acquire(key("w1"))
	r.bs.Record(probe, OK) // first byte: one success of two
	r.want("w1", HalfOpen)
	r.bs.RecordLate(probe, Fault) // the stream aborted
	r.want("w1", Open)
	if got := r.openFor("w1"); got != 20*time.Second {
		t.Fatalf("reopened for %v, want a doubled 20s", got)
	}
	r.bs.RecordLate(probe, Fault) // the same echo again: stale, nothing happens
	if got := r.openFor("w1"); got != 20*time.Second {
		t.Fatalf("a stale late fault changed the cool-down: %v", got)
	}
	// With the success not counted, two fresh good probes are needed to close.
	r.clk.Advance(20 * time.Second)
	r.send("w1", OK)
	r.want("w1", HalfOpen)
	r.send("w1", OK)
	r.want("w1", Closed)
}

// When one good probe already closed the breaker, an abort after that is an ordinary fault of a closed breaker.
func TestLateFaultOfAClosingProbeCountsAsAFault(t *testing.T) {
	p := testParams()
	p.FailureRatio = 0
	r := newRig(t, p, "w1")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	probe, _ := r.bs.Acquire(key("w1"))
	r.bs.Record(probe, OK)
	r.want("w1", Closed)
	r.bs.RecordLate(probe, Fault)
	r.sendN("w1", Fault, 3)
	r.want("w1", Closed) // 4 faults
	r.send("w1", Fault)
	r.want("w1", Open) // the late one was the first of five
}

func TestNewRegistrationStartsClosed(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")
	old := key("w1")
	oldTicket, _ := r.bs.Acquire(old)
	fresh := KeyOf("w1", registered.Add(time.Hour))
	r.bs.Sync([]Member{{Key: fresh, Eligible: true}})
	if !r.bs.Admissible(fresh) || r.bs.State(fresh) != Closed {
		t.Fatal("a re-registered worker starts closed")
	}
	r.bs.RecordLate(oldTicket, Fault) // verdicts of the old registration are ignored
	r.sendN2(fresh, Fault, 4)
	if r.bs.State(fresh) != Closed {
		t.Fatal("the old registration's history must not carry over")
	}
}

func (r *rig) sendN2(k Key, v Verdict, n int) {
	for i := 0; i < n; i++ {
		tk, ok := r.bs.Acquire(k)
		if !ok {
			r.t.Fatal("Acquire refused")
		}
		r.bs.Record(tk, v)
	}
}

func TestPrunedWorkersVanish(t *testing.T) {
	r := newRig(t, testParams(), "w1", "w2")
	tk0, _ := r.bs.Acquire(key("w1")) // a ticket taken while closed
	tripConsecutive(r, "w1")
	r.bs.Sync([]Member{{Key: key("w2"), Eligible: true}})
	if !r.bs.Admissible(key("w1")) || r.bs.State(key("w1")) != Closed {
		t.Fatal("a pruned worker has no breaker")
	}
	for _, i := range r.bs.Snapshot() {
		if i.Key.ID == "w1" {
			t.Fatal("pruned worker still listed")
		}
	}
	r.bs.RecordLate(tk0, Fault) // must not resurrect it
	if tk, ok := r.bs.Acquire(key("w1")); !ok || tk.Probe() {
		t.Fatal("unknown workers are plain closed")
	}
	// Prune keeps only the listed keys.
	tripConsecutive(r, "w2")
	r.bs.Prune(nil)
	r.want("w2", Closed)
}

func TestCoolDownLevelPersistsAcrossPassThenRetrip(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	tripConsecutive(r, "w1")        // level 0: 10 s
	r.clk.Advance(10 * time.Second) // half-open
	r.send("w1", Fault)             // failed probe: level 1, 20 s
	r.clk.Advance(20 * time.Second) // half-open
	r.send("w1", OK)                // passed: closed, level stays 1
	r.want("w1", Closed)
	r.clk.Advance(30 * time.Second) // closed for a while, but less than max_cool_down
	tripConsecutive(r, "w1")
	if got := r.openFor("w1"); got != 20*time.Second {
		t.Fatalf("pass-then-retrip must keep the grown cool-down: %v", got)
	}
	r.clk.Advance(20 * time.Second)
	r.send("w1", Fault) // failed probe: level 2, 40 s
	if got := r.openFor("w1"); got != 40*time.Second {
		t.Fatalf("open for %v, want 40s", got)
	}
	r.clk.Advance(40 * time.Second)
	r.send("w1", OK) // closed at this instant
	r.clk.Advance(2*time.Minute - time.Nanosecond)
	tripConsecutive(r, "w1")
	if got := r.openFor("w1"); got != 40*time.Second {
		t.Fatalf("closed for just under max_cool_down keeps the level: %v", got)
	}
	r.clk.Advance(40 * time.Second)
	r.send("w1", OK)
	r.clk.Advance(2 * time.Minute) // closed for exactly max_cool_down: the level resets
	tripConsecutive(r, "w1")
	if got := r.openFor("w1"); got != 10*time.Second {
		t.Fatalf("a close that lasted max_cool_down resets the level: %v", got)
	}
}

func TestChangeHookRunsOutsideTheLock(t *testing.T) {
	r := newRig(t, testParams(), "w1")
	done := make(chan struct{})
	var bs *Breakers
	bs = NewBreakers(testParams(), Options{Now: r.clk.Now, Rand: r.rnd.Float64, OnChange: func(Change) {
		bs.State(key("w1")) // would deadlock if the hook ran under the lock
		close(done)
	}})
	bs.Sync([]Member{{Key: key("w1"), Eligible: true}})
	go func() {
		for i := 0; i < 5; i++ {
			tk, _ := bs.Acquire(key("w1"))
			bs.Record(tk, Fault)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the change hook deadlocked")
	}
}

func TestSnapshotListsNonClosedOnly(t *testing.T) {
	r := newRig(t, testParams(), "w1", "w2", "w3")
	tripConsecutive(r, "w1")
	tripConsecutive(r, "w2")
	r.clk.Advance(10 * time.Second) // both half-open
	r.send("w2", OK)                // w2 closes
	snap := r.bs.Snapshot()
	if len(snap) != 1 || snap[0].Key.ID != "w1" || snap[0].State != HalfOpen {
		t.Fatalf("snapshot = %+v", snap)
	}
}
