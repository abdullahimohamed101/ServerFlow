package resilience

import (
	"testing"
	"time"
)

func testParams() BreakerParams {
	return BreakerParams{
		ConsecutiveFailures: 5, FailureRatio: 0.5, MinVolume: 10, Window: 10 * time.Second,
		CoolDown: 10 * time.Second, MaxCoolDown: 2 * time.Minute, ProbeConcurrency: 1, ProbeSuccesses: 1,
		MaxOpenFraction: 1, HeaderTimeout: 10 * time.Second,
	}
}

type rig struct {
	t       *testing.T
	clk     *fakeClock
	rnd     *scriptedRand
	bs      *Breakers
	changes []Change
	members []Member
}

var registered = time.Unix(1_700_000_000, 0)

func key(id string) Key { return KeyOf(id, registered) }

func newRig(t *testing.T, p BreakerParams, ids ...string) *rig {
	t.Helper()
	r := &rig{t: t, clk: newFakeClock(), rnd: &scriptedRand{def: 0.5}}
	r.bs = NewBreakers(p, Options{Now: r.clk.Now, Rand: r.rnd.Float64, OnChange: func(c Change) { r.changes = append(r.changes, c) }})
	for _, id := range ids {
		r.members = append(r.members, Member{Key: key(id), Eligible: true})
	}
	r.bs.Sync(r.members)
	return r
}

// send acquires a ticket for the worker and records the verdict, as one finished attempt.
func (r *rig) send(id string, v Verdict) Ticket {
	r.t.Helper()
	tk, ok := r.bs.Acquire(key(id))
	if !ok {
		r.t.Fatalf("Acquire(%s) refused in state %v", id, r.bs.State(key(id)))
	}
	r.bs.Record(tk, v)
	return tk
}

func (r *rig) sendN(id string, v Verdict, n int) {
	r.t.Helper()
	for i := 0; i < n; i++ {
		r.send(id, v)
	}
}

func (r *rig) want(id string, s State) {
	r.t.Helper()
	if got := r.bs.State(key(id)); got != s {
		r.t.Fatalf("state of %s = %v, want %v", id, got, s)
	}
}

func (r *rig) openFor(id string) time.Duration {
	r.t.Helper()
	for _, i := range r.bs.Snapshot() {
		if i.Key.ID == id && i.State == Open {
			return i.OpenUntil.Sub(r.clk.Now())
		}
	}
	r.t.Fatalf("%s is not open", id)
	return 0
}

func TestConsecutiveRuleBoundary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		steps []Verdict
		trips bool
	}{
		{"four faults", []Verdict{Fault, Fault, Fault, Fault}, false},
		{"five faults", []Verdict{Fault, Fault, Fault, Fault, Fault}, true},
		{"an ok resets the streak", []Verdict{Fault, Fault, Fault, Fault, OK, Fault}, false},
		{"an ok then five faults", []Verdict{Fault, OK, Fault, Fault, Fault, Fault, Fault}, true},
		{"neutral neither counts nor resets", []Verdict{Fault, Fault, Neutral, Fault, Fault, Neutral, Fault}, true},
		{"unlisted 5xx never extends the streak", []Verdict{Fault, Fault, RatioFault, RatioFault, RatioFault, Fault}, false},
		{"unlisted 5xx does not break the streak", []Verdict{Fault, Fault, RatioFault, Fault, Fault, Fault}, true},
		{"only unlisted 5xx below the volume", []Verdict{RatioFault, RatioFault, RatioFault, RatioFault, RatioFault, RatioFault}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testParams()
			p.FailureRatio = 0 // isolate the consecutive rule
			r := newRig(t, p, "w1")
			for _, v := range tc.steps {
				r.send("w1", v)
			}
			if got := r.bs.State(key("w1")) == Open; got != tc.trips {
				t.Fatalf("open = %v, want %v", got, tc.trips)
			}
		})
	}
}

func TestConsecutiveStreakMustFitTheWindow(t *testing.T) {
	// Four faults, a pause, then the fifth: the first of the five is older than the window unless the pause is
	// short enough. The boundary is inclusive: exactly Window old still counts.
	for _, tc := range []struct {
		name  string
		gap   time.Duration
		trips bool
	}{
		{"within the window", 9 * time.Second, true},
		{"exactly the window", 10 * time.Second, true},
		{"just past the window", 10*time.Second + time.Nanosecond, false},
		{"long past", time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testParams()
			p.FailureRatio = 0
			r := newRig(t, p, "w1")
			r.sendN("w1", Fault, 4)
			r.clk.Advance(tc.gap)
			r.send("w1", Fault)
			if got := r.bs.State(key("w1")) == Open; got != tc.trips {
				t.Fatalf("open = %v, want %v", got, tc.trips)
			}
		})
	}
	// A stale streak does not poison later ones: four more faults make five recent ones and trip.
	p := testParams()
	p.FailureRatio = 0
	r := newRig(t, p, "w1")
	r.sendN("w1", Fault, 4)
	r.clk.Advance(time.Minute)
	r.sendN("w1", Fault, 5)
	r.want("w1", Open)
}

func TestRatioRule(t *testing.T) {
	type step struct {
		v Verdict
		n int
	}
	for _, tc := range []struct {
		name  string
		steps []step
		trips bool
	}{
		{"below min volume", []step{{Fault, 3}, {OK, 6}}, false},                                         // 9 outcomes
		{"min volume reached at the ratio", []step{{OK, 5}, {Fault, 4}, {OK, 0}, {RatioFault, 1}}, true}, // 10, 5 faults
		{"just below the ratio", []step{{OK, 6}, {Fault, 2}, {RatioFault, 2}}, false},                    // 4/10
		{"at the ratio exactly", []step{{OK, 5}, {Fault, 2}, {RatioFault, 3}}, true},                     // 5/10
		{"unlisted 5xx alone trips the ratio", []step{{RatioFault, 10}}, true},
		{"neutral outcomes do not count towards volume", []step{{Fault, 5}, {Neutral, 5}}, false}, // 5 counted, not 10
		{"neutral outcomes are not faults", []step{{OK, 5}, {Fault, 4}, {Neutral, 1}}, false},    // 9 counted
		{"neutral outcomes do not dilute the ratio", []step{{Fault, 5}, {Neutral, 50}, {OK, 4}, {RatioFault, 1}}, true}, // 6/10
		{"an ok can complete the volume", []step{{Fault, 4}, {RatioFault, 1}, {OK, 4}, {OK, 1}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testParams()
			p.ConsecutiveFailures = 100 // isolate the ratio rule
			r := newRig(t, p, "w1")
			for _, s := range tc.steps {
				r.sendN("w1", s.v, s.n)
			}
			if got := r.bs.State(key("w1")) == Open; got != tc.trips {
				t.Fatalf("open = %v, want %v", got, tc.trips)
			}
		})
	}
}

func TestRatioRuleCanBeSwitchedOff(t *testing.T) {
	p := testParams()
	p.ConsecutiveFailures = 100
	p.FailureRatio = 0
	r := newRig(t, p, "w1")
	r.sendN("w1", RatioFault, 50)
	r.want("w1", Closed)
}

func TestWindowSlidesBucketByBucket(t *testing.T) {
	p := testParams()
	p.ConsecutiveFailures = 100
	p.MinVolume = 4
	p.FailureRatio = 1 // only an all-fault window trips
	r := newRig(t, p, "w1")
	// Faults at t0, t0+1, t0+2 (3 outcomes), an OK at t0+3 would make 3/4. Instead fill the window with faults
	// and watch the oldest fall out: at t0+10 the fault from t0 is gone.
	r.send("w1", Fault)        // t0
	r.clk.Advance(time.Second) // t0+1
	r.send("w1", Fault)
	r.clk.Advance(time.Second) // t0+2
	r.send("w1", Fault)
	r.clk.Advance(7*time.Second + 500*time.Millisecond) // t0+9.5: all three still inside (buckets t0..t0+9)
	r.send("w1", OK)                                    // 3 faults + 1 ok = 3/4 < 1
	r.want("w1", Closed)
	r.clk.Advance(500 * time.Millisecond) // t0+10: the t0 fault leaves; faults t0+1,t0+2 remain
	r.send("w1", Fault)
	r.send("w1", Fault) // window: faults t0+1,t0+2,t0+10 x2 = 4 faults + 1 ok => 4/5, not 1
	r.want("w1", Closed)
	r.clk.Advance(2 * time.Second) // t0+12: window t0+3..t0+12 holds the ok (t0+9) and the faults of t0+10
	r.send("w1", Fault)            // 3 faults, 1 ok => 3/4
	r.want("w1", Closed)
	r.clk.Advance(8 * time.Second) // t0+20: window t0+11..t0+20 holds only the fault of t0+12
	r.sendN("w1", Fault, 2)        // 3 outcomes < min volume
	r.want("w1", Closed)
	r.send("w1", Fault) // 4/4
	r.want("w1", Open)
}

func TestOldFailuresAgeOutOfTheRatio(t *testing.T) {
	for _, tc := range []struct {
		name  string
		wait  time.Duration
		trips bool
	}{
		{"still inside the window", 5 * time.Second, true},
		{"aged out", 11 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testParams()
			p.ConsecutiveFailures = 100
			r := newRig(t, p, "w1")
			r.sendN("w1", Fault, 5)
			r.sendN("w1", OK, 4) // 9 outcomes: below the volume
			r.want("w1", Closed)
			r.clk.Advance(tc.wait)
			r.send("w1", OK) // 10 outcomes (5 faults) inside the window, or just 1
			if got := r.bs.State(key("w1")) == Open; got != tc.trips {
				t.Fatalf("open = %v, want %v", got, tc.trips)
			}
		})
	}
}
