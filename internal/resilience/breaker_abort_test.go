package resilience

import (
	"testing"
	"time"
)

// RecordAbort turns an ok recorded at the first byte into a fault when the stream then breaks, so a request counts
// once with its final outcome.

func ratioOnly() BreakerParams {
	p := testParams()
	p.ConsecutiveFailures = 1000
	p.MinVolume = 4
	return p
}

func TestAnAbortTurnsItsOkIntoAFaultInTheRatioWindow(t *testing.T) {
	r := newRig(t, ratioOnly(), "w1", "w2")
	// Three oks and a request that is ok at the first byte and then aborts: 4 outcomes, 1 fault: 25% < 50%.
	r.sendN("w1", OK, 3)
	tk, _ := r.bs.Acquire(key("w1"))
	at := r.clk.Now()
	r.bs.Record(tk, OK)
	r.bs.RecordAbort(tk, at)
	r.want("w1", Closed)
	// Two more aborts: now 6 outcomes of which 3 are faults = 50%: it trips. Had the oks stayed counted (an ok
	// followed by a fault), 9 outcomes with 3 faults would stay below.
	for i := 0; i < 2; i++ {
		tk, _ := r.bs.Acquire(key("w1"))
		at := r.clk.Now()
		r.bs.Record(tk, OK)
		r.bs.RecordAbort(tk, at)
	}
	r.want("w1", Open)
	if c := r.changes[len(r.changes)-1]; c.Reason != ReasonRatio || c.Failures != 3 {
		t.Fatalf("change %+v", c)
	}
}

func TestAnAbortWhoseOkLeftTheWindowOnlyAddsTheFault(t *testing.T) {
	p := ratioOnly()
	p.MinVolume = 2
	r := newRig(t, p, "w1", "w2")
	tk, _ := r.bs.Acquire(key("w1"))
	at := r.clk.Now()
	r.bs.Record(tk, OK)
	r.clk.Advance(11 * time.Second) // the ok's bucket is out of the 10 s window
	r.bs.Record(mustTicket(t, r, "w1"), OK)
	r.bs.RecordAbort(tk, at) // nothing to take out: one fault joins one ok
	// 1 ok + 1 fault in the window: 50% with volume 2 -> trips
	r.want("w1", Open)
}

func mustTicket(t *testing.T, r *rig, id string) Ticket {
	t.Helper()
	tk, ok := r.bs.Acquire(key(id))
	if !ok {
		t.Fatalf("Acquire(%s)", id)
	}
	return tk
}

func TestAnAbortFromBeforeTheBreakerOpenedIsIgnored(t *testing.T) {
	r := newRig(t, ratioOnly(), "w1", "w2")
	stale := mustTicket(t, r, "w1")
	at := r.clk.Now()
	r.bs.Record(stale, OK)
	faultsUntilOpen(t, r, "w1")
	r.want("w1", Open)
	r.clk.Advance(10 * time.Second)
	probe := mustTicket(t, r, "w1")
	r.bs.Record(probe, OK) // closes
	r.want("w1", Closed)
	r.bs.RecordAbort(stale, at) // epoch differs: the breaker already acted on that evidence
	r.sendN("w1", OK, 3)
	r.want("w1", Closed)
}

func TestAnAbortIsIgnoredWhileNotClosedAndForUnknownOrInvalidTickets(t *testing.T) {
	r := newRig(t, ratioOnly(), "w1", "w2")
	tk := mustTicket(t, r, "w1")
	at := r.clk.Now()
	r.bs.Record(tk, OK)
	faultsUntilOpen(t, r, "w1")
	changes := len(r.changes)
	r.bs.RecordAbort(tk, at)
	r.bs.RecordAbort(Ticket{}, at)
	r.bs.RecordAbort(Ticket{key: key("ghost"), valid: true}, at)
	if len(r.changes) != changes {
		t.Fatal("an ignored abort changed the breaker")
	}
}

func TestAnAbortOfAProbeReopensLikeALateFault(t *testing.T) {
	p := testParams()
	p.ProbeSuccesses = 2
	r := newRig(t, p, "w1", "w2")
	tripConsecutive(r, "w1")
	r.clk.Advance(10 * time.Second)
	tk := mustTicket(t, r, "w1")
	if !tk.Probe() {
		t.Fatal("setup")
	}
	at := r.clk.Now()
	r.bs.Record(tk, OK) // 1 of 2 probes passed: still half-open
	r.want("w1", HalfOpen)
	r.bs.RecordAbort(tk, at)
	r.want("w1", Open)
	if c := r.changes[len(r.changes)-1]; c.Reason != ReasonProbeFailed {
		t.Fatalf("%+v", c)
	}
}

// The streak: an abort is a fault now, after the reset of its ok; three aborts that follow each other directly
// (no ok between the abort and the next first byte) still build a streak.
func TestAbortsCountInTheStreakAsFaultsWhenNoOkFollows(t *testing.T) {
	p := testParams()
	p.ConsecutiveFailures = 3
	r := newRig(t, p, "w1", "w2")
	var tks []Ticket
	var ats []time.Time
	for i := 0; i < 3; i++ { // three streams reach their first byte...
		tk := mustTicket(t, r, "w1")
		tks = append(tks, tk)
		ats = append(ats, r.clk.Now())
		r.bs.Record(tk, OK)
	}
	for i := range tks { // ...and then all break
		r.bs.RecordAbort(tks[i], ats[i])
	}
	r.want("w1", Open)
}

func faultsUntilOpen(t *testing.T, r *rig, id string) {
	t.Helper()
	for i := 0; i < 20 && r.bs.State(key(id)) != Open; i++ {
		r.send(id, Fault)
	}
	r.want(id, Open)
}
