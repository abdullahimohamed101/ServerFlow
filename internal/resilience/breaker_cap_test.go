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
