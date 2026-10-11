package resilience

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func newTestQueue(depth int, share float64, maxModels int) (*Queue, *fakeClock) {
	clk := newFakeClock()
	return NewQueue(QueueParams{MaxModelDepth: depth, MaxWait: 3 * time.Second, MaxTenantShare: share, MaxModels: maxModels}, clk.Now), clk
}

func mustEnqueue(t *testing.T, q *Queue, model, tenant string) *Waiter {
	t.Helper()
	w, err := q.Enqueue(model, tenant)
	if err != nil {
		t.Fatalf("Enqueue(%s,%s): %v", model, tenant, err)
	}
	return w
}

func recv(t *testing.T, w *Waiter) Grant {
	t.Helper()
	select {
	case g := <-w.C():
		return g
	default:
		t.Fatal("expected a grant, none pending")
		return Grant{}
	}
}

func noGrant(t *testing.T, w *Waiter) {
	t.Helper()
	select {
	case g := <-w.C():
		t.Fatalf("unexpected grant %+v", g)
	default:
	}
}

func TestQueueHandoffIsStrictFIFO(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	var ws []*Waiter
	for i := 0; i < 5; i++ {
		ws = append(ws, mustEnqueue(t, q, "m", ""))
	}
	for i := range ws {
		if !q.Handoff("m", i) {
			t.Fatalf("handoff %d found no waiter", i)
		}
		g := recv(t, ws[i])
		if g.Kind != Handoff || g.Value != i {
			t.Fatalf("waiter %d got %+v", i, g)
		}
		for _, later := range ws[i+1:] {
			noGrant(t, later) // nobody is served out of order
		}
	}
	if q.Handoff("m", 99) {
		t.Fatal("an empty queue hands off nothing: the slot is released normally")
	}
	if q.Depth("m") != 0 || len(q.Depths()) != 0 {
		t.Fatal("the drained queue must leave no state behind")
	}
}

func TestQueueArrivalsMustEnqueueWhileBusy(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	if q.Busy("m") {
		t.Fatal("empty queue is not busy")
	}
	first := mustEnqueue(t, q, "m", "")
	if !q.Busy("m") {
		t.Fatal("a queue with a waiter is busy: arrivals must enqueue, never bypass")
	}
	if q.Busy("other") {
		t.Fatal("models are independent")
	}
	// Arrivals queue behind, and are served in arrival order after the earlier waiter.
	late1 := mustEnqueue(t, q, "m", "")
	late2 := mustEnqueue(t, q, "m", "")
	for i, w := range []*Waiter{first, late1, late2} {
		q.Handoff("m", i)
		if g := recv(t, w); g.Value != i {
			t.Fatalf("order broken at %d: %+v", i, g)
		}
	}
	if q.Busy("m") {
		t.Fatal("drained")
	}
}

func TestQueueDepthBound(t *testing.T) {
	q, _ := newTestQueue(3, 0, 0)
	accepted, full := 0, 0
	for i := 0; i < 30; i++ { // 10x arrivals
		if _, err := q.Enqueue("m", ""); err == nil {
			accepted++
		} else if errors.Is(err, ErrQueueFull) {
			full++
			if ShedReason(err) != ReasonQueueFull {
				t.Fatal("full queue sheds as queue_full")
			}
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 3 || full != 27 || q.Depth("m") != 3 {
		t.Fatalf("accepted %d full %d depth %d", accepted, full, q.Depth("m"))
	}
	if !q.Full("m", "") {
		t.Fatal("Full must agree with Enqueue")
	}
}

func TestQueueTenantShareCap(t *testing.T) {
	q, _ := newTestQueue(10, 0.5, 0) // 5 per tenant
	var a []*Waiter
	for i := 0; i < 5; i++ {
		a = append(a, mustEnqueue(t, q, "m", "A"))
	}
	if _, err := q.Enqueue("m", "A"); !errors.Is(err, ErrTenantShare) || ShedReason(err) != ReasonQueueFull {
		t.Fatalf("sixth waiter of one tenant: %v", err)
	}
	if !q.Full("m", "A") || q.Full("m", "B") {
		t.Fatal("Full must apply the tenant share")
	}
	for i := 0; i < 5; i++ { // other tenants and unauthenticated requests are not capped by A
		mustEnqueue(t, q, "m", "B")
	}
	if _, err := q.Enqueue("m", ""); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("depth 10 reached: %v", err)
	}
	q.Leave(a[0]) // a place frees up for A (and one in the depth)
	mustEnqueue(t, q, "m", "A")
	if _, err := q.Enqueue("m", "A"); err == nil {
		t.Fatal("A is at its share again")
	}
	// Unauthenticated requests share no cap: tenant "" can fill the queue.
	q2, _ := newTestQueue(4, 0.25, 0)
	for i := 0; i < 4; i++ {
		mustEnqueue(t, q2, "m", "")
	}
	// Share 0 switches the guard off.
	q3, _ := newTestQueue(4, 0, 0)
	for i := 0; i < 4; i++ {
		mustEnqueue(t, q3, "m", "A")
	}
	// A tiny share never rounds down to "nobody may queue".
	q4, _ := newTestQueue(2, 0.1, 0)
	mustEnqueue(t, q4, "m", "A")
	if _, err := q4.Enqueue("m", "A"); !errors.Is(err, ErrTenantShare) {
		t.Fatalf("share floor of one: %v", err)
	}
}

func TestQueueNoQueueForOverflowModelsOrDepthZero(t *testing.T) {
	q, _ := newTestQueue(5, 0, 2)
	x := mustEnqueue(t, q, "x", "")
	mustEnqueue(t, q, "y", "")
	if _, err := q.Enqueue("z", ""); !errors.Is(err, ErrNoQueue) {
		t.Fatalf("a third model must get no queue: %v", err)
	}
	if !q.Full("z", "") {
		t.Fatal("Full(z)")
	}
	mustEnqueue(t, q, "x", "") // existing queues keep working
	q.Leave(x)
	q.Handoff("x", nil) // x drains completely
	if _, err := q.Enqueue("z", ""); err != nil {
		t.Fatalf("a freed model slot lets z queue: %v", err)
	}
	q0, _ := newTestQueue(0, 0, 0)
	if _, err := q0.Enqueue("m", ""); !errors.Is(err, ErrNoQueue) {
		t.Fatalf("depth 0 = no queue: %v", err)
	}
}

func TestQueueWakeKeepsPlaceAndParkReportsMissedHandoffs(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w1, w2, w3 := mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", "")
	if n := q.Wake("m", 2); n != 2 {
		t.Fatalf("woke %d", n)
	}
	if g := recv(t, w1); g.Kind != Reroute {
		t.Fatalf("w1: %+v", g)
	}
	if g := recv(t, w2); g.Kind != Reroute {
		t.Fatalf("w2: %+v", g)
	}
	noGrant(t, w3)
	if q.Depth("m") != 3 || !q.Busy("m") {
		t.Fatal("woken waiters keep their place (arrivals still queue behind them)")
	}
	if n := q.Wake("m", 5); n != 1 { // only w3 is not already routing
		t.Fatalf("second wake reached %d waiters, want just the idle one", n)
	}
	recv(t, w3)
	// A slot released while the head is routing must not jump to another waiter; the head is told to look again.
	if q.Handoff("m", "slot") {
		t.Fatal("the head is routing: the slot is released normally")
	}
	if !q.Park(w1) { // still full when it looked, but a release happened meanwhile
		t.Fatal("Park must report the missed hand-off")
	}
	if q.Park(w1) {
		t.Fatal("the miss is reported once")
	}
	// Parked again, w1 is the head and gets the next hand-off.
	if !q.Handoff("m", "slot2") {
		t.Fatal("parked head must receive hand-offs")
	}
	if g := recv(t, w1); g.Kind != Handoff || g.Value != "slot2" {
		t.Fatalf("w1: %+v", g)
	}
	q.Done(w2) // served by its own routing
	if q.Depth("m") != 1 {
		t.Fatalf("depth %d", q.Depth("m"))
	}
}

func TestQueueLeaveIsImmediateAndNeverLosesAHandoff(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w1, w2 := mustEnqueue(t, q, "m", "A"), mustEnqueue(t, q, "m", "A")
	if _, granted := q.Leave(w1); granted {
		t.Fatal("nothing was handed to w1")
	}
	if q.Depth("m") != 1 {
		t.Fatal("a cancelled waiter leaves at once")
	}
	if _, granted := q.Leave(w1); granted {
		t.Fatal("idempotent")
	}
	// The head after the cancel is w2.
	q.Handoff("m", "token")
	// w2 gave up at the same moment: Leave returns the slot so the caller can give it back.
	g, granted := q.Leave(w2)
	if !granted || g.Kind != Handoff || g.Value != "token" {
		t.Fatalf("Leave after a hand-off = %+v %v", g, granted)
	}
	if _, granted := q.Leave(w2); granted {
		t.Fatal("the slot is returned once")
	}
	if q.Depth("m") != 0 {
		t.Fatal("leftover waiters")
	}
}

func TestQueueWaitOutcomes(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	never := make(<-chan time.Time)

	w := mustEnqueue(t, q, "m", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, out := q.Wait(ctx, w, never); out != ClientGone || q.Depth("m") != 0 {
		t.Fatalf("cancelled: %v depth %d", out, q.Depth("m"))
	}

	w = mustEnqueue(t, q, "m", "")
	fired := make(chan time.Time, 1)
	fired <- time.Now()
	if _, out := q.Wait(context.Background(), w, fired); out != TimedOut || q.Depth("m") != 0 {
		t.Fatalf("timeout: %v depth %d", out, q.Depth("m"))
	}

	w = mustEnqueue(t, q, "m", "")
	q.Handoff("m", 7)
	if g, out := q.Wait(context.Background(), w, never); out != Granted || g.Value != 7 {
		t.Fatalf("handoff: %+v %v", g, out)
	}

	// A hand-off that races a timeout and a cancel is never dropped, whichever branch the select takes.
	for i := 0; i < 50; i++ {
		w = mustEnqueue(t, q, "m", "")
		q.Handoff("m", i)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fired := make(chan time.Time, 1)
		fired <- time.Now()
		if g, out := q.Wait(ctx, w, fired); out != Granted || g.Value != i {
			t.Fatalf("lost a hand-off: %+v %v", g, out)
		}
	}
}

func TestQueueFlushAndClose(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	a1, a2 := mustEnqueue(t, q, "a", ""), mustEnqueue(t, q, "a", "")
	b1 := mustEnqueue(t, q, "b", "")
	q.Wake("a", 1) // a1 has a pending Reroute that it has not read
	if n := q.Flush("a"); n != 2 {
		t.Fatalf("flushed %d", n)
	}
	for _, w := range []*Waiter{a1, a2} {
		if g := recv(t, w); g.Kind != Flushed {
			t.Fatalf("grant %+v", g)
		}
	}
	noGrant(t, b1)
	if q.Depth("a") != 0 || q.Depth("b") != 1 {
		t.Fatal("flush is per model")
	}
	q.Close()
	if _, err := q.Enqueue("c", ""); !errors.Is(err, ErrDraining) || ShedReason(err) != ReasonDraining {
		t.Fatalf("closed queue: %v", err)
	}
	if !q.Handoff("b", nil) { // existing waiters finish
		t.Fatal("close must not strand waiters")
	}
	b2 := mustEnqueue2(q, "b")
	_ = b2
}

func mustEnqueue2(q *Queue, model string) *Waiter { w, _ := q.Enqueue(model, ""); return w }

func TestQueueFlushAll(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	var ws []*Waiter
	for i := 0; i < 6; i++ {
		ws = append(ws, mustEnqueue(t, q, fmt.Sprint("m", i%3), ""))
	}
	if n := q.Flush(""); n != 6 {
		t.Fatalf("flushed %d", n)
	}
	for _, w := range ws {
		if g := recv(t, w); g.Kind != Flushed {
			t.Fatal("not flushed")
		}
	}
	if len(q.Models()) != 0 {
		t.Fatal("state left behind")
	}
}

func TestQueueRetryAfter(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	if got := q.RetryAfter(); got != 1 {
		t.Fatalf("no data: %d", got)
	}
	q.ObserveWait(300 * time.Millisecond)
	if got := q.RetryAfter(); got != 1 {
		t.Fatalf("0.3 s: %d", got)
	}
	q2, _ := newTestQueue(10, 0, 0)
	q2.ObserveWait(2400 * time.Millisecond)
	if got := q2.RetryAfter(); got != 3 {
		t.Fatalf("2.4 s rounds up: %d", got)
	}
	q3, _ := newTestQueue(10, 0, 0)
	q3.ObserveWait(25 * time.Second)
	if got := q3.RetryAfter(); got != 10 {
		t.Fatalf("clamped to 10: %d", got)
	}
	// The EWMA moves: 20% of the new sample.
	q4, _ := newTestQueue(10, 0, 0)
	q4.ObserveWait(time.Second)
	for i := 0; i < 20; i++ {
		q4.ObserveWait(8 * time.Second)
	}
	if got := q4.RetryAfter(); got != 8 {
		t.Fatalf("converges to 8: %d", got)
	}
	for reason, want := range map[string]int{ReasonGlobalLimit: 1, ReasonDraining: 1, ReasonQueueFull: 8, ReasonQueueTimeout: 8} {
		if got := q4.RetryAfterSeconds(reason); got != want {
			t.Errorf("RetryAfterSeconds(%s) = %d, want %d", reason, got, want)
		}
	}
}

func TestShedReasonOfOtherErrors(t *testing.T) {
	if ShedReason(errors.New("boom")) != "" || ShedReason(nil) != "" {
		t.Fatal("only queue errors are sheds")
	}
}

// settled reports the queue invariant: credit is outstanding only while a waiter is routing (that waiter's Park, Done
// or Leave will spend it), and never while the first idle waiter has nobody routing ahead of it.
func (q *Queue) settled(model string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	mq := q.models[model]
	if mq == nil || mq.credit == 0 {
		return true
	}
	for e := mq.waiters.Front(); e != nil; e = e.Next() {
		if e.Value.(*Waiter).woken {
			return true
		}
	}
	return false
}

func (q *Queue) creditOf(model string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if mq := q.models[model]; mq != nil {
		return mq.credit
	}
	return 0
}

// The verifier's three scenarios for a slot freed while waiters are routing. A declined hand-off becomes credit
// that is spent on the first waiter not routing, and otherwise waits for the first routing waiter to stop.

// (a) Wake(2) wakes both; a slot is released while they route; w1 ends with Done; w2 parks and must be told to look again.
func TestQueueCreditScenarioA_BothRoutingFirstDone(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w1, w2 := mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", "")
	q.Wake("m", 2)
	recv(t, w1)
	recv(t, w2)
	if q.Handoff("m", "freed") {
		t.Fatal("the head is routing: the slot is released normally")
	}
	if q.creditOf("m") != 1 {
		t.Fatalf("credit %d", q.creditOf("m"))
	}
	q.Done(w1) // took some other slot
	noGrant(t, w2)
	if !q.Park(w2) {
		t.Fatal("w2 must be told to route again: the freed slot is still unused")
	}
	if q.creditOf("m") != 0 || !q.settled("m") {
		t.Fatal("credit spent once")
	}
	if q.Park(w2) {
		t.Fatal("no credit left: w2 waits")
	}
}

// (a') the same with Leave (timeout or cancel) instead of Done, and with a routing waiter that leaves.
func TestQueueCreditScenarioA_LeaveGivesTheWakeUpBack(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w1, w2 := mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", "")
	q.Wake("m", 1)
	recv(t, w1)
	q.Leave(w1) // woken for a slot, gave up before using it: someone else may use the slot
	if g := recv(t, w2); g.Kind != Reroute {
		t.Fatalf("w2: %+v", g)
	}
	// Done does not give the wake-up back: the slot was used.
	q2, _ := newTestQueue(10, 0, 0)
	a, b := mustEnqueue(t, q2, "m", ""), mustEnqueue(t, q2, "m", "")
	q2.Wake("m", 1)
	recv(t, a)
	q2.Done(a)
	noGrant(t, b)
}

// (b) three slots released during one routing: each is offered to a different waiter, none is dropped.
func TestQueueCreditScenarioB_ThreeReleasesDuringOneRouting(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	ws := []*Waiter{mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", "")}
	q.Wake("m", 1)
	recv(t, ws[0]) // w1 is routing
	for i := 1; i <= 3; i++ {
		if q.Handoff("m", i) {
			t.Fatal("the head is routing")
		}
		if g := recv(t, ws[i]); g.Kind != Reroute {
			t.Fatalf("release %d must wake w%d: %+v", i, i+1, g)
		}
	}
	// A fourth release finds everybody routing: it waits as credit for the first one that stops.
	if q.Handoff("m", 4) || q.creditOf("m") != 1 {
		t.Fatalf("credit %d", q.creditOf("m"))
	}
	q.Done(ws[0])
	if !q.Park(ws[3]) {
		t.Fatal("the last unused slot must reach a waiter")
	}
	if !q.settled("m") {
		t.Fatal("invariant")
	}
}

// A late arrival must not take credit ahead of an earlier waiter that is still routing: when that waiter stops, the
// credit goes to it (the first idle waiter in queue order), and only later to the newcomer.
func TestQueueLateArrivalDoesNotOvertakeAnEarlierRoutingWaiter(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w1 := mustEnqueue(t, q, "m", "")
	q.Wake("m", 1)
	recv(t, w1) // w1 is routing
	if q.Handoff("m", "slot") {
		t.Fatal("the head is routing")
	}
	w2 := mustEnqueue(t, q, "m", "") // arrives while credit is banked
	noGrant(t, w2)
	if !q.settled("m") {
		t.Fatal("invariant")
	}
	if !q.Park(w1) { // w1 found everything full, but a slot was released meanwhile
		t.Fatal("the banked credit belongs to w1, who was first")
	}
	noGrant(t, w2)
	if q.Park(w1) { // routed again, still full: idle now
		t.Fatal("credit was spent")
	}
	// The next release goes to w1 (the head) by hand-off, w2 stays behind it.
	if !q.Handoff("m", "next") {
		t.Fatal("w1 is idle again and is the head")
	}
	if g := recv(t, w1); g.Kind != Handoff {
		t.Fatalf("w1: %+v", g)
	}
	noGrant(t, w2)
	// And when the earlier routing waiter is served instead, the credit reaches the newcomer.
	q2, _ := newTestQueue(10, 0, 0)
	a := mustEnqueue(t, q2, "m", "")
	q2.Wake("m", 1)
	recv(t, a)
	q2.Handoff("m", "slot")
	b := mustEnqueue(t, q2, "m", "")
	q2.Done(a)
	if g := recv(t, b); g.Kind != Reroute {
		t.Fatalf("credit must reach the newcomer once nobody is ahead: %+v", g)
	}
}

// Busy must stay true while a model has waiters whatever the credit says, or an arrival could bypass the queue.
func TestQueueBusyWithCreditBanked(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w := mustEnqueue(t, q, "m", "")
	q.Wake("m", 1)
	recv(t, w)
	q.Handoff("m", "slot") // credit 1; the only waiter is routing
	if !q.Busy("m") {
		t.Fatal("a queue with a routing waiter and banked credit is busy: arrivals must enqueue")
	}
	q.Done(w)
	if q.Busy("m") {
		t.Fatal("drained")
	}
}

// Every declined release is one unit of credit, not a flag: two releases during one routing are two units.
func TestQueueCreditCountsEveryDeclinedRelease(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w := mustEnqueue(t, q, "m", "")
	q.Wake("m", 1)
	recv(t, w)
	q.Handoff("m", 1)
	q.Handoff("m", 2)
	q.Handoff("m", 3)
	if got := q.creditOf("m"); got != 3 {
		t.Fatalf("credit %d, want 3", got)
	}
	if !q.Park(w) || q.creditOf("m") != 2 {
		t.Fatalf("Park spends one unit: credit %d", q.creditOf("m"))
	}
}

func TestQueueFlushDoesNotSettle(t *testing.T) {
	q, _ := newTestQueue(10, 0, 0)
	w1, w2 := mustEnqueue(t, q, "m", ""), mustEnqueue(t, q, "m", "")
	q.Wake("m", 1)
	recv(t, w1)
	q.Handoff("m", "x")
	q.Flush("m")
	if g := recv(t, w1); g.Kind != Flushed {
		t.Fatal(g)
	}
	if g := recv(t, w2); g.Kind != Flushed {
		t.Fatalf("a flushed waiter is flushed, not woken: %+v", g)
	}
}
