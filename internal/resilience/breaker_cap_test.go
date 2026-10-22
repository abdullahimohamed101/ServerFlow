package resilience

import (
	"fmt"
	"testing"
	"time"
)

func capParams() BreakerParams {
	p := testParams()
	p.MaxOpenFraction = 0.5
	return p
}

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("w%d", i+1)
	}
	return out
}

func TestMaxOpenTable(t *testing.T) {
	for _, tc := range []struct {
		fraction float64
		eligible int
		want     int
	}{
		{0.5, 0, 0}, {0.5, 1, 0}, {0.5, 2, 1}, {0.5, 3, 1}, {0.5, 4, 2}, {0.5, 5, 2}, {0.5, 6, 3},
		{0, 10, 0}, {-1, 10, 0}, {1, 4, 4}, {2, 4, 4}, {0.29, 100, 29}, {0.1, 30, 3}, {0.7, 10, 7},
	} {
		if got := MaxOpen(tc.fraction, tc.eligible); got != tc.want {
			t.Errorf("MaxOpen(%v, %d) = %d, want %d", tc.fraction, tc.eligible, got, tc.want)
		}
	}
}

// With fraction 0.5: 1 eligible worker -> 0 open, 2 -> 1, 3 -> 1, 4 -> 2 (D9, acceptance 10).
func TestEjectionCapTable(t *testing.T) {
	for _, tc := range []struct {
		workers, wantOpen int
	}{{1, 0}, {2, 1}, {3, 1}, {4, 2}} {
		t.Run(fmt.Sprintf("%d workers", tc.workers), func(t *testing.T) {
			r := newRig(t, capParams(), ids(tc.workers)...)
			for _, id := range ids(tc.workers) {
				r.sendN(id, Fault, 5) // every worker fails hard
			}
			eligible, open, half := r.bs.Counts()
			if eligible != tc.workers || open+half != tc.wantOpen {
				t.Fatalf("eligible %d open %d half-open %d, want %d open", eligible, open, half, tc.wantOpen)
			}
			if got := r.bs.Suppressed(); got != uint64(tc.workers-tc.wantOpen) {
				t.Errorf("suppressed = %d, want %d", got, tc.workers-tc.wantOpen)
			}
		})
	}
}

func TestSingleWorkerNeverOpens(t *testing.T) {
	r := newRig(t, capParams(), "w1")
	r.sendN("w1", Fault, 100)
	r.want("w1", Closed)
	if !r.bs.Admissible(key("w1")) {
		t.Fatal("the sole worker must stay in service")
	}
}

func setEligible(r *rig, eligible map[string]bool) {
	for i := range r.members {
		r.members[i].Eligible = eligible[r.members[i].Key.ID]
	}
	r.bs.Sync(r.members)
}

// Four workers with two UNHEALTHY: only the two eligible ones count, so at most one may open.
func TestIneligibleWorkersAreOutsideBothCounts(t *testing.T) {
	r := newRig(t, capParams(), ids(4)...)
	setEligible(r, map[string]bool{"w1": true, "w2": true})
	r.sendN("w1", Fault, 5)
	r.sendN("w2", Fault, 5)
	_, open, half := r.bs.Counts()
	if open+half != 1 {
		t.Fatalf("open+half-open = %d among 2 eligible workers, want 1", open+half)
	}
	r.want("w1", Open)
	r.want("w2", Closed) // suppressed
}

// A LOST worker's OPEN breaker must not use up the room: the real failure must still be ejected.
func TestLostWorkersOpenBreakerDoesNotBlockEjectingARealFailure(t *testing.T) {
	r := newRig(t, capParams(), ids(3)...) // cap = floor(0.5 x 3) = 1
	r.sendN("w3", Fault, 5)
	r.want("w3", Open)
	r.sendN("w1", Fault, 5)
	r.want("w1", Closed)                                    // suppressed: w3 holds the only slot
	setEligible(r, map[string]bool{"w1": true, "w2": true}) // w3 is now LOST
	r.send("w1", Fault)                                     // next outcome: cap is floor(0.5 x 2) = 1 and nobody eligible is open
	r.want("w1", Open)
}

// A worker that tripped while it was ineligible takes no room. When it turns eligible again the cap is enforced:
// the most recently ejected breaker is closed, so open + half-open <= cap again (the verifier's sequence: 2 workers,
// cap 1; A opens; B is ineligible, takes an in-flight fault and opens; B turns eligible again).
func TestReEligibleWorkerDoesNotExceedTheCap(t *testing.T) {
	r := newRig(t, capParams(), "A", "B", "C") // cap = floor(0.5 x 3) = 1
	r.sendN("A", Fault, 5)
	r.want("A", Open)
	setEligible(r, map[string]bool{"A": true, "C": true}) // B is unhealthy: cap = floor(0.5 x 2) = 1, A still fits
	r.want("A", Open)
	r.clk.Advance(time.Second)
	r.sendN("B", Fault, 5) // B's in-flight failures finish; an ineligible worker takes no room
	r.want("B", Open)
	setEligible(r, map[string]bool{"A": true, "B": true, "C": true})
	_, open, half := r.bs.Counts()
	if open+half != 1 {
		t.Fatalf("open %d half-open %d after B became eligible again, want 1 (cap)", open, half)
	}
	r.want("A", Open)   // the older ejection is kept
	r.want("B", Closed) // the most recent one is relaxed
	if !r.bs.Admissible(key("B")) {
		t.Fatal("the model must keep an admissible worker")
	}
	last := r.changes[len(r.changes)-1]
	if last.Key.ID != "B" || last.To != Closed || last.Reason != ReasonCapRelaxed {
		t.Fatalf("change = %+v", last)
	}
}

// The cap also shrinks when healthy workers turn unhealthy: with 4 eligible (cap 2) and 2 open, losing both healthy
// ones leaves 2 eligible (cap 1), so one open breaker is relaxed.
func TestCapShrinksWhenOtherWorkersLeave(t *testing.T) {
	r := newRig(t, capParams(), ids(4)...)
	r.sendN("w1", Fault, 5)
	r.clk.Advance(time.Second)
	r.sendN("w2", Fault, 5)
	_, open, _ := r.bs.Counts()
	if open != 2 {
		t.Fatalf("open = %d", open)
	}
	setEligible(r, map[string]bool{"w1": true, "w2": true})
	_, open, half := r.bs.Counts()
	if open+half != 1 {
		t.Fatalf("open+half-open = %d, want 1 of 2 eligible", open+half)
	}
	r.want("w1", Open)
	r.want("w2", Closed)
}

// An open breaker of a worker that is eligible again counts at once: no new trip until there is room.
func TestOpenBreakerCountsAgainWhenTheWorkerIsEligibleAgain(t *testing.T) {
	r := newRig(t, capParams(), ids(3)...)
	r.sendN("w3", Fault, 5)
	setEligible(r, map[string]bool{"w1": true, "w2": true})
	r.clk.Advance(time.Second)
	r.sendN("w1", Fault, 5)
	r.want("w1", Open)
	setEligible(r, map[string]bool{"w1": true, "w2": true, "w3": true})
	r.want("w3", Open) // the older ejection stays; w1 was relaxed to respect the cap of 1
	r.want("w1", Closed)
	r.sendN("w2", Fault, 5)
	r.want("w2", Closed) // suppressed: w3 holds the only place
}

func TestHalfOpenCountsAgainstTheCap(t *testing.T) {
	r := newRig(t, capParams(), ids(3)...)
	r.sendN("w1", Fault, 5)
	r.clk.Advance(10 * time.Second)
	r.want("w1", HalfOpen)
	r.sendN("w2", Fault, 5)
	r.want("w2", Closed)
	_, open, half := r.bs.Counts()
	if open != 0 || half != 1 {
		t.Fatalf("open %d half-open %d", open, half)
	}
}

func TestSuppressedBreakerKeepsItsStreakAndTripsWhenThereIsRoom(t *testing.T) {
	r := newRig(t, capParams(), ids(2)...)
	r.sendN("w1", Fault, 5) // takes the only slot
	r.sendN("w2", Fault, 8) // suppressed, but still counting
	r.want("w2", Closed)
	if got := r.bs.Suppressed(); got != 1 {
		t.Fatalf("suppressed = %d, a continuing suppression is reported once", got)
	}
	suppressedChanges := 0
	for _, c := range r.changes {
		if c.Suppressed {
			suppressedChanges++
			if c.Key.ID != "w2" || c.From != Closed || c.To != Closed || c.Reason != ReasonSuppressed {
				t.Fatalf("suppressed change = %+v", c)
			}
		}
	}
	if suppressedChanges != 1 {
		t.Fatalf("suppressed notifications = %d", suppressedChanges)
	}
	r.clk.Advance(10 * time.Second)
	r.send("w1", OK) // w1's probe passes and closes it: room again
	r.want("w1", Closed)
	r.send("w2", Fault) // one more outcome is enough: the streak was kept
	r.want("w2", Open)
}

func TestSuppressedBreakerKeepsItsRatioWindow(t *testing.T) {
	p := capParams()
	p.ConsecutiveFailures = 100
	p.Window = time.Minute
	r := newRig(t, p, ids(2)...)
	r.sendN("w1", RatioFault, 10)
	r.want("w1", Open)
	r.sendN("w2", RatioFault, 10)
	r.want("w2", Closed)
	r.clk.Advance(10 * time.Second)
	r.send("w1", OK)
	r.want("w1", Closed)
	r.send("w2", RatioFault) // 11 outcomes inside the window, all faults
	r.want("w2", Open)
}

func TestFractionZeroNeverEjects(t *testing.T) {
	p := capParams()
	p.MaxOpenFraction = 0
	r := newRig(t, p, ids(4)...)
	r.sendN("w1", Fault, 20)
	r.want("w1", Closed)
}

func TestFractionOneAllOpenGivesRetryAfterToTheEarliestProbe(t *testing.T) {
	p := testParams() // fraction 1
	p.CoolDown, p.MaxCoolDown = time.Hour, time.Hour
	r := newRig(t, p, ids(2)...)
	if got := r.bs.RetryAfter(); got != 1 {
		t.Errorf("nothing open: %d", got)
	}
	tripConsecutive(r, "w1")
	tripConsecutive(r, "w2")
	_, open, _ := r.bs.Counts()
	if open != 2 {
		t.Fatalf("open = %d", open)
	}
	if got := r.bs.RetryAfter(); got != 30 {
		t.Errorf("an hour away clamps to 30, got %d", got)
	}

	q := testParams()
	r = newRig(t, q, ids(2)...)
	tripConsecutive(r, "w1") // 10 s
	r.clk.Advance(3500 * time.Millisecond)
	tripConsecutive(r, "w2") // 10 s from now
	if got := r.bs.RetryAfter(); got != 7 {
		t.Errorf("earliest probe in 6.5 s rounds up to 7, got %d", got)
	}
	r.clk.Advance(6500 * time.Millisecond) // w1 is half-open
	if got := r.bs.RetryAfter(); got != 1 {
		t.Errorf("a half-open breaker is a probe away: %d", got)
	}
}

// A suppressed consecutive trip happens on the next counted fault of either kind once there is room (plan: "its next
// outcome"). An unlisted 5xx does not break the streak; an OK resets it.
func TestSuppressedStreakTripsOnTheNextUnlistedFault(t *testing.T) {
	for _, tc := range []struct {
		name  string
		next  Verdict
		opens bool
	}{{"unlisted 5xx", RatioFault, true}, {"fault", Fault, true}, {"ok resets the streak", OK, false}} {
		t.Run(tc.name, func(t *testing.T) {
			p := capParams()
			p.FailureRatio = 0
			r := newRig(t, p, ids(2)...)
			r.sendN("w1", Fault, 5)
			r.sendN("w2", Fault, 5) // suppressed
			r.want("w2", Closed)
			r.clk.Advance(10 * time.Second)
			r.send("w1", OK) // room again
			r.send("w2", tc.next)
			if got := r.bs.State(key("w2")) == Open; got != tc.opens {
				t.Fatalf("open = %v, want %v", got, tc.opens)
			}
		})
	}
}

func TestCapRelaxationTieBreaksByLargerWorkerID(t *testing.T) {
	r := newRig(t, capParams(), ids(4)...) // cap 2
	r.sendN("w1", Fault, 5)
	r.sendN("w2", Fault, 5) // same instant: equal ejection time
	r.want("w1", Open)
	r.want("w2", Open)
	setEligible(r, map[string]bool{"w1": true, "w2": true}) // cap 1
	r.want("w1", Open)
	r.want("w2", Closed)
}

func TestPruneEnforcesTheCap(t *testing.T) {
	r := newRig(t, capParams(), ids(4)...)
	r.sendN("w1", Fault, 5)
	r.clk.Advance(time.Second)
	r.sendN("w2", Fault, 5)
	r.bs.Prune([]Key{key("w1"), key("w2")}) // w3 and w4 are gone: 2 eligible, cap 1
	_, open, half := r.bs.Counts()
	if open+half != 1 {
		t.Fatalf("open+half-open = %d after Prune, want 1", open+half)
	}
	r.want("w1", Open)
	r.want("w2", Closed)
}

func TestCapRelaxationClosesAsManyAsNeeded(t *testing.T) {
	r := newRig(t, capParams(), ids(6)...) // cap 3
	for _, id := range []string{"w1", "w2", "w3"} {
		r.sendN(id, Fault, 5)
		r.clk.Advance(time.Second)
	}
	setEligible(r, map[string]bool{"w1": true, "w2": true, "w3": true}) // cap 1
	_, open, half := r.bs.Counts()
	if open+half != 1 {
		t.Fatalf("open+half-open = %d, want 1: two breakers had to be closed", open+half)
	}
	r.want("w1", Open) // the longest-standing ejection survives
}

// A failed probe reopens a breaker but does not make it a "recent" ejection: the cap keeps the older evidence.
func TestFailedProbeDoesNotRefreshTheEjectionTime(t *testing.T) {
	r := newRig(t, capParams(), ids(4)...) // cap 2
	r.sendN("w1", Fault, 5)                // ejected at t0
	r.clk.Advance(time.Second)
	r.sendN("w2", Fault, 5) // ejected at t0+1s
	r.clk.Advance(10 * time.Second)
	probe, ok := r.bs.Acquire(key("w1")) // both are half-open now
	if !ok || !probe.Probe() {
		t.Fatal("probe")
	}
	r.bs.Record(probe, Fault) // w1 reopens, 20s
	setEligible(r, map[string]bool{"w1": true, "w2": true})
	r.want("w1", Open)   // still the older ejection
	r.want("w2", Closed) // the newer one is relaxed
}

// RecordLate is for faults only: an OK or a neutral verdict must change nothing.
func TestRecordLateIgnoresOKAndNeutral(t *testing.T) {
	p := testParams()
	p.FailureRatio = 0
	r := newRig(t, p, "w1")
	r.sendN("w1", Fault, 4)
	tk, _ := r.bs.Acquire(key("w1"))
	r.bs.RecordLate(tk, OK)      // would reset the streak if it counted
	r.bs.RecordLate(tk, Neutral) // would be counted as an outcome
	r.send("w1", Fault)
	r.want("w1", Open)
	// ...and on a half-open probe it does not reopen the breaker.
	r.clk.Advance(10 * time.Second)
	probe, _ := r.bs.Acquire(key("w1"))
	r.bs.RecordLate(probe, OK)
	r.want("w1", HalfOpen)
}

// The cap is per model (plan D9: "a single-worker model never opens"): a gateway that serves a model with one worker
// and another with three must not eject the sole worker just because the fleet as a whole has room.
func TestEjectionCapIsPerModel(t *testing.T) {
	r := newRig(t, capParams())
	r.members = []Member{
		{Key: key("solo"), Model: "single", Eligible: true},
		{Key: key("a1"), Model: "triple", Eligible: true},
		{Key: key("a2"), Model: "triple", Eligible: true},
		{Key: key("a3"), Model: "triple", Eligible: true},
	}
	r.bs.Sync(r.members)
	for _, id := range []string{"solo", "a1", "a2", "a3"} {
		r.sendN(id, Fault, 5)
	}
	r.want("solo", Closed) // the sole worker of its model stays in service
	if !r.bs.Admissible(key("solo")) {
		t.Fatal("the sole worker of a model must stay admissible")
	}
	open := 0
	for _, id := range []string{"a1", "a2", "a3"} {
		if r.bs.State(key(id)) != Closed {
			open++
		}
	}
	if open != 1 { // floor(0.5 x 3)
		t.Fatalf("%d of the three workers of one model are open, want 1", open)
	}
	if got := r.bs.Suppressed(); got != 3 { // solo and two of the triple were held back
		t.Errorf("suppressed = %d, want 3", got)
	}
}

// Another model's workers becoming eligible or open does not change this model's cap on a view change.
func TestEjectionCapIsPerModelWhenTheViewChanges(t *testing.T) {
	r := newRig(t, capParams())
	r.members = []Member{
		{Key: key("a1"), Model: "a", Eligible: true}, {Key: key("a2"), Model: "a", Eligible: true},
		{Key: key("b1"), Model: "b", Eligible: false}, {Key: key("b2"), Model: "b", Eligible: false},
	}
	r.bs.Sync(r.members)
	r.sendN("b1", Fault, 5) // ineligible: outside the cap, its trip takes no room
	r.sendN("b2", Fault, 5)
	r.want("b1", Open)
	r.want("b2", Open)
	for i := range r.members { // both b workers turn eligible again: only one may stay open
		r.members[i].Eligible = true
	}
	r.bs.Sync(r.members)
	open := 0
	for _, id := range []string{"b1", "b2"} {
		if r.bs.State(key(id)) != Closed {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("%d of model b's two workers stay open, want 1", open)
	}
	r.sendN("a1", Fault, 5)
	r.sendN("a2", Fault, 5)
	if a1, a2 := r.bs.State(key("a1")), r.bs.State(key("a2")); (a1 == Closed) == (a2 == Closed) {
		t.Fatalf("model a: exactly one of two workers may be ejected, got a1=%v a2=%v", a1, a2)
	}
}

// Prune counts the workers that are left: pruning two of four eligible workers halves the cap, and an ejection beyond the
// new cap is relaxed.
func TestPruneRecountsTheEligibleWorkersAndRelaxesTheCap(t *testing.T) {
	r := newRig(t, capParams(), ids(4)...)
	r.sendN("w1", Fault, 5)
	r.sendN("w2", Fault, 5)
	r.want("w1", Open)
	r.want("w2", Open) // 2 of 4 eligible
	r.bs.Prune([]Key{key("w1"), key("w2")})
	open := 0
	for _, id := range []string{"w1", "w2"} {
		if r.bs.State(key(id)) != Closed {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("%d of the two remaining workers are open, want 1 (floor(0.5 x 2))", open)
	}
}
