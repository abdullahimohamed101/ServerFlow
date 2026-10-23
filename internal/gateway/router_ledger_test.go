package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/internal/scheduler"
	"serverflow/internal/scheduler/signals"
	"serverflow/pkg/protocol"
)

// Phase 14 PR B: the in-flight ledger and the opt-in "ledger" load overlay at the router (ADR-022, D4, D14, D17).

func ledgerCfg() signals.Config { return signals.Config{Window: 12 * time.Second} }

// withLedger switches e's router to the ledger overlay and lets it see the current snapshot.
func withLedger(t *testing.T, e *regEnv) *signals.Tracker {
	t.Helper()
	e.gw.router.enableLedger(ledgerCfg())
	e.refresh(t)
	return e.gw.router.sig
}

func ledgerReq(in, out int) *protocol.InferenceRequest {
	return &protocol.InferenceRequest{Model: "qwen-7b", EstInputTokens: in, EstOutputTokens: out}
}

func wantTotals(t *testing.T, tr *signals.Tracker, n int, work int64, why string) {
	t.Helper()
	gn, gw := tr.Totals()
	if gn != n || gw != work {
		t.Fatalf("%s: ledger holds %d requests / %d tokens, want %d / %d", why, gn, gw, n, work)
	}
}

func ledgerOf(r *router, w *protocol.WorkerSnapshot) (int, int64) {
	return r.sig.Ledger(signals.KeyOf(w))
}

// Reserving adds the request's cost to the chosen worker's ledger and releasing removes exactly it, once: a second
// release is harmless.
func TestLedgerAddsAtReserveAndRemovesOnceAtRelease(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(4)}, noBreaker)
	tr := withLedger(t, e.regEnv)
	r := e.gw.router
	a, rerr := r.Route(context.Background(), ledgerReq(100, 400))
	if rerr != nil {
		t.Fatal(rerr)
	}
	b, rerr := r.Route(context.Background(), ledgerReq(10, 40))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if n, w := ledgerOf(r, &a.worker); n != 2 || w != 550 {
		t.Fatalf("ledger %d / %d, want 2 / 550", n, w)
	}
	a.release()
	a.release() // idempotent
	if n, w := ledgerOf(r, &b.worker); n != 1 || w != 50 {
		t.Fatalf("after releasing a: %d / %d, want 1 / 50", n, w)
	}
	b.release()
	wantTotals(t, tr, 0, 0, "after both releases")
	if r.InFlight("w1") != 0 {
		t.Fatal("slot leaked")
	}
}

// Nothing of the ledger exists, and no snapshot field is filled, unless the overlay is configured.
func TestNoLedgerByDefault(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(4)}, noBreaker)
	if e.gw.router.sig != nil || e.gw.router.cache.onSnapshot != nil {
		t.Fatal("the default configuration created a ledger")
	}
	spy := &observedSpy{inner: e.gw.router.sched}
	e.gw.router.sched = spy
	rt, rerr := e.gw.router.Route(context.Background(), ledgerReq(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt.release()
	if spy.last == nil || spy.last[0].Observed != (protocol.ObservedLoad{}) {
		t.Fatalf("Observed was filled without the ledger: %+v", spy.last)
	}
	if rt.lease.Valid() || rt.cost.Load() != 0 {
		t.Fatal("a ledger lease exists without the ledger")
	}
}

type observedSpy struct {
	inner interface {
		SelectWorker(context.Context, *protocol.InferenceRequest, []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error)
	}
	mu   sync.Mutex
	last []protocol.WorkerSnapshot
}

func (s *observedSpy) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	s.mu.Lock()
	s.last = append([]protocol.WorkerSnapshot(nil), ws...)
	s.mu.Unlock()
	return s.inner.SelectWorker(ctx, req, ws)
}

// With the ledger on, the strategies see their gateway-local signals on the snapshot copy.
func TestObservedIsFilledOnTheCopyWhenTheLedgerIsOn(t *testing.T) {
	g1, g2 := newGateWorker(t, "w1"), newGateWorker(t, "w2")
	e := newResEnv(t, []protocol.WorkerSnapshot{g1.snap(4), g2.snap(4)}, noBreaker)
	withLedger(t, e.regEnv)
	spy := &observedSpy{inner: e.gw.router.sched}
	e.gw.router.sched = spy
	r := e.gw.router
	a, _ := r.Route(context.Background(), ledgerReq(100, 400))
	defer a.release()
	b, rerr := r.Route(context.Background(), ledgerReq(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer b.release()
	spy.mu.Lock()
	defer spy.mu.Unlock()
	var withA protocol.WorkerSnapshot
	for _, w := range spy.last {
		if w.WorkerID == a.worker.WorkerID {
			withA = w
		}
	}
	if withA.Observed.InflightRequests != 1 || withA.Observed.WorkAheadTokens != 500 || withA.Observed.EffectiveActive != 1 {
		t.Fatalf("Observed on the loaded worker: %+v", withA.Observed)
	}
	for _, w := range spy.last {
		if w.WorkerID != a.worker.WorkerID && w.Observed.WorkAheadTokens != 0 {
			t.Fatalf("Observed on the idle worker: %+v", w.Observed)
		}
	}
	// the shared view the cache keeps is a copy: nothing leaked into it
	again, _ := r.cache.View("qwen-7b")
	for _, w := range again {
		if w.Observed != (protocol.ObservedLoad{}) {
			t.Fatal("the cached snapshot was modified")
		}
	}
}

// A panic in the strategy leaves nothing in the ledger (the deferred unlock still runs).
func TestLedgerStaysZeroWhenTheStrategyPanics(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(4)}, noBreaker)
	tr := withLedger(t, e.regEnv)
	ps := &panicSched{inner: e.gw.router.sched, armed: true}
	e.gw.router.sched = ps
	func() {
		defer func() { _ = recover() }()
		_, _ = e.gw.router.Route(context.Background(), ledgerReq(5, 5))
		t.Error("the strategy did not panic")
	}()
	wantTotals(t, tr, 0, 0, "after a panicking strategy")
	ps.mu.Lock()
	ps.armed = false
	ps.mu.Unlock()
	rt, rerr := e.gw.router.Route(context.Background(), ledgerReq(5, 5)) // the router still works: the lock was released
	if rerr != nil {
		t.Fatal(rerr)
	}
	rt.release()
	wantTotals(t, tr, 0, 0, "after a later request")
}

// A refused breaker ticket drops the chosen worker and selects again: only the final worker is in the ledger.
func TestLedgerHoldsOnlyTheFinalChoiceAfterATicketRefusal(t *testing.T) {
	g1, g2 := newGateWorker(t, "w-1"), newGateWorker(t, "w-2")
	e := newResEnv(t, []protocol.WorkerSnapshot{g1.snap(4), g2.snap(4)})
	tr := withLedger(t, e.regEnv)
	r := e.gw.router
	trip := &tripSched{inner: r.sched, trip: func() {
		for i := 0; i < 3; i++ {
			tk, ok := e.rs.breakers.Acquire(e.key("w-1"))
			if !ok {
				t.Error("acquire")
				return
			}
			e.rs.breakers.Record(tk, resilience.Fault)
		}
	}}
	r.sched = trip
	rt, rerr := r.Route(context.Background(), ledgerReq(20, 30))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if rt.worker.WorkerID != "w-2" {
		t.Fatalf("routed to %s: the ejected worker was chosen", rt.worker.WorkerID)
	}
	if trip.calls < 2 {
		t.Fatalf("the scheduler ran %d times, want a re-selection", trip.calls)
	}
	if n, w := r.sig.Ledger(signals.Key{WorkerID: "w-1", Registered: g1.snap(4).RegisteredAt.UnixNano()}); n != 0 || w != 0 {
		t.Fatalf("the refused worker holds %d / %d", n, w)
	}
	if n, w := ledgerOf(r, &rt.worker); n != 1 || w != 50 {
		t.Fatalf("the final worker holds %d / %d", n, w)
	}
	rt.release()
	wantTotals(t, tr, 0, 0, "after release")
}

// tripSched trips a breaker during the first selection, so the choice it makes is refused at the ticket.
type tripSched struct {
	inner interface {
		SelectWorker(context.Context, *protocol.InferenceRequest, []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error)
	}
	trip  func()
	calls int
}

func (s *tripSched) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	s.calls++
	w, err := s.inner.SelectWorker(ctx, req, ws)
	if s.calls == 1 {
		s.trip()
	}
	return w, err
}

// A hand-off moves the slot, not the cost (D4, D14): the finished request's cost leaves the ledger by its own
// release, the handed-off routed starts at cost 0, and the waiter adds its own on receipt. The undercount window is
// one request. A hand-off nobody consumes subtracts nothing.
func TestLedgerAcrossAHandOff(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(1)}, noBreaker, queueCfg(4, time.Minute))
	tr := withLedger(t, e.regEnv)
	r := e.gw.router
	holder, rerr := r.Route(context.Background(), ledgerReq(100, 900)) // 1000 tokens, fills the one slot
	if rerr != nil {
		t.Fatal(rerr)
	}
	// a second request finds everything full and queues
	waiterReq := ledgerReq(30, 70)
	rt2, w, rerr := r.RouteOrWait(context.Background(), waiterReq, "")
	if rerr != nil || rt2 != nil || w == nil {
		t.Fatalf("expected to queue: %v %v %v", rt2, w, rerr)
	}
	wantTotals(t, tr, 1, 1000, "while the waiter waits")

	holder.release() // hands the slot to the waiter
	// the window: the old cost is gone, the new one is not there yet, the slot count is right
	wantTotals(t, tr, 1, 0, "after the hand-off, before the waiter charges")
	if r.InFlight("w1") != 1 {
		t.Fatalf("inflight %d: the slot must stay taken across a hand-off", r.InFlight("w1"))
	}
	g2, out := e.rs.waitq.Wait(context.Background(), w, nil)
	if out != resilience.Granted || g2.Kind != resilience.Handoff {
		t.Fatalf("wait: %v %v", out, g2.Kind)
	}
	got := g2.Value.(*routed)
	if got.cost.Load() != 0 {
		t.Fatalf("a handed-off routed starts at cost %d, want 0", got.cost.Load())
	}
	r.charge(got, waiterReq)
	r.charge(got, waiterReq) // charging twice must not double the entry
	wantTotals(t, tr, 1, 100, "after the waiter charged its cost")
	got.release()
	wantTotals(t, tr, 0, 0, "after the waiter's release")
	if r.InFlight("w1") != 0 {
		t.Fatal("slot leaked")
	}
}

// A handed-off slot the waiter never consumes (it leaves) comes back through release with cost 0: nothing is
// subtracted that was never added, so another request's cost on the same worker is intact.
func TestAnUnconsumedHandOffSubtractsNothing(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(2)}, noBreaker, queueCfg(4, time.Minute))
	tr := withLedger(t, e.regEnv)
	r := e.gw.router
	a, _ := r.Route(context.Background(), ledgerReq(100, 100)) // 200
	b, _ := r.Route(context.Background(), ledgerReq(300, 300)) // 600: both slots taken
	_, w, rerr := r.RouteOrWait(context.Background(), ledgerReq(1, 1), "")
	if rerr != nil || w == nil {
		t.Fatalf("expected to queue: %v", rerr)
	}
	a.release() // hand-off to the waiter
	wantTotals(t, tr, 2, 600, "b plus the unconsumed hand-off")
	g2, ok := e.rs.waitq.Leave(w) // the client left before taking the slot
	if !ok {
		t.Fatal("the hand-off was not delivered")
	}
	g2.Value.(*routed).release()
	wantTotals(t, tr, 1, 600, "b's cost must be untouched")
	b.release()
	wantTotals(t, tr, 0, 0, "end")
	if r.InFlight("w1") != 0 {
		t.Fatal("slot leaked")
	}
}

// A worker that registers again is a new incarnation: its ledger starts empty and the old incarnation's requests
// cannot subtract from it; a worker that disappears is pruned.
func TestLedgerResetsOnReRegistrationAndPrunesGoneWorkers(t *testing.T) {
	g := newGateWorker(t, "w1")
	old := g.snap(4)
	old.RegisteredAt = time.Unix(1_700_000_000, 0)
	e := newResEnv(t, []protocol.WorkerSnapshot{old}, noBreaker)
	tr := withLedger(t, e.regEnv)
	r := e.gw.router
	a, _ := r.Route(context.Background(), ledgerReq(10, 10))
	renewed := old
	renewed.RegisteredAt = old.RegisteredAt.Add(time.Minute)
	e.lister.set([]protocol.WorkerSnapshot{renewed}, nil)
	e.clock.Advance(time.Second)
	e.refresh(t)
	b, _ := r.Route(context.Background(), ledgerReq(1, 1))
	if n, w := ledgerOf(r, &b.worker); n != 1 || w != 2 {
		t.Fatalf("the new incarnation holds %d / %d, want 1 / 2", n, w)
	}
	a.release() // the old incarnation's request ends
	if n, w := ledgerOf(r, &b.worker); n != 1 || w != 2 {
		t.Fatalf("a request of the old registration changed the new one: %d / %d", n, w)
	}
	b.release()
	wantTotals(t, tr, 0, 0, "end")
	e.lister.set(nil, nil)
	e.clock.Advance(time.Second)
	e.refresh(t)
	if s := tr.Stats(); s.Tracked != 0 {
		t.Fatalf("a worker that left the snapshot is still tracked: %d", s.Tracked)
	}
}

// Every exit path of an attempt over HTTP returns the ledger to zero: success, a worker error with a retry, a client
// that goes away, a panic in the upstream call. (The strategy panic and the ticket refusal are above.)
func TestLedgerReturnsToZeroOnEveryAttemptExit(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		a := newScriptWorker(t, "a", okJSON("a"))
		e := newRegEnv(t, "least-active", []protocol.WorkerSnapshot{a.snapshot("qwen-7b")})
		tr := withLedger(t, e)
		resp, _ := e.post(t, false)
		if resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		allIdle(t, e, "a")
		wantTotals(t, tr, 0, 0, "success")
	})
	t.Run("retry after a worker error", func(t *testing.T) {
		bad := newScriptWorker(t, "a-bad", status(503, `{"error":{"code":"x"}}`))
		good := newScriptWorker(t, "b-good", okJSON("b-good"))
		e := newRegEnv(t, "least-active", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
		tr := withLedger(t, e)
		resp, _ := e.post(t, false)
		if resp.StatusCode != 200 || bad.hits.Load() != 1 || good.hits.Load() != 1 {
			t.Fatalf("status %d, hits %d/%d: the retry did not happen", resp.StatusCode, bad.hits.Load(), good.hits.Load())
		}
		allIdle(t, e, "a-bad", "b-good")
		wantTotals(t, tr, 0, 0, "retry")
	})
	t.Run("all attempts fail", func(t *testing.T) {
		bad := newScriptWorker(t, "a-bad", status(503, `{"error":{"code":"x"}}`))
		bad2 := newScriptWorker(t, "b-bad", status(503, `{"error":{"code":"x"}}`))
		e := newRegEnv(t, "least-active", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), bad2.snapshot("qwen-7b")})
		tr := withLedger(t, e)
		resp, _ := e.post(t, false)
		if resp.StatusCode == 200 {
			t.Fatal("expected a failure")
		}
		allIdle(t, e, "a-bad", "b-bad")
		wantTotals(t, tr, 0, 0, "failure")
	})
	t.Run("client goes away mid request", func(t *testing.T) {
		g := newGateWorker(t, "w1")
		e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(2)}, noBreaker)
		tr := withLedger(t, e.regEnv)
		ctx, cancel := context.WithCancel(context.Background())
		res := e.chatAsync(ctx, "r0")
		g.awaitArrivals(t, 1)
		if n, w := tr.Totals(); n != 1 || w <= 0 {
			t.Fatalf("while held the ledger holds %d / %d", n, w)
		}
		cancel()
		<-res
		eventually(t, 5*time.Second, func() bool { n, w := tr.Totals(); return n == 0 && w == 0 }, "ledger back to zero after the client left")
		e.settle(t, "w1")
	})
	t.Run("panic in the upstream call", func(t *testing.T) {
		a := newScriptWorker(t, "a", okJSON("a"))
		e, _ := panicEnv(t, a.snapshot("qwen-7b"))
		tr := withLedger(t, e)
		e.injectTransport(&faultTransport{panicSend: func(*http.Request) bool { return true }})
		resp, _ := e.post(t, false)
		if resp.StatusCode != 500 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		allIdle(t, e, "a")
		wantTotals(t, tr, 0, 0, "panic")
	})
	t.Run("a queued request is abandoned", func(t *testing.T) {
		g := newGateWorker(t, "w1")
		e := queueEnv(t, g)
		tr := withLedger(t, e.regEnv)
		held := e.chatAsync(context.Background(), "r0")
		g.awaitArrivals(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		queued := e.chatAsync(ctx, "r1")
		e.awaitDepth(t, 1)
		cancel()
		<-queued
		e.awaitDepth(t, 0)
		g.release(t, 1)
		awaitResult(t, held)
		eventually(t, 5*time.Second, func() bool { n, w := tr.Totals(); return n == 0 && w == 0 }, "ledger back to zero")
		e.settle(t, "w1")
	})
	t.Run("a queued request is served by a hand-off", func(t *testing.T) {
		g := newGateWorker(t, "w1")
		e := queueEnv(t, g)
		tr := withLedger(t, e.regEnv)
		held := e.chatAsync(context.Background(), "r0")
		g.awaitArrivals(t, 1)
		queued := e.chatAsync(context.Background(), "r1")
		e.awaitDepth(t, 1)
		g.release(t, 1)
		awaitResult(t, held)
		g.awaitArrivals(t, 2)
		if n, w := tr.Totals(); n != 1 || w <= 0 {
			t.Fatalf("the served waiter must be on the ledger with its cost: %d / %d", n, w)
		}
		g.release(t, 1)
		awaitResult(t, queued)
		eventually(t, 5*time.Second, func() bool { n, w := tr.Totals(); return n == 0 && w == 0 }, "ledger back to zero")
		e.settle(t, "w1")
	})
}

// The stale-high report that master's max rule keeps in force: this gateway sent two requests, the worker reported
// them, they finished, and the worker still reports 2 until its next heartbeat. max keeps the worker unpickable;
// the ledger rule sees the finishes.
func TestStaleHighReportNoLongerHidesAnEmptiedWorker(t *testing.T) {
	for _, mode := range []string{config.LoadOverlayMax, config.LoadOverlayLedger} {
		t.Run(mode, func(t *testing.T) {
			g := newGateWorker(t, "w1")
			e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(2)}, noBreaker)
			if mode == config.LoadOverlayLedger {
				withLedger(t, e.regEnv)
			}
			r := e.gw.router
			e.clock.Advance(time.Second)
			a, _ := r.Route(context.Background(), ledgerReq(1, 1))
			b, _ := r.Route(context.Background(), ledgerReq(1, 1))
			e.clock.Advance(time.Second)
			s := g.snap(2)
			s.Metrics.ActiveRequests = 2 // the worker's report includes both
			s.HeartbeatAgeSeconds = 0.5  // made half a second before the snapshot was fetched
			e.lister.set([]protocol.WorkerSnapshot{s}, nil)
			e.refresh(t)
			e.clock.Advance(500 * time.Millisecond)
			a.release()
			b.release()
			e.clock.Advance(100 * time.Millisecond)
			free := r.freeSlots("qwen-7b")
			rt, rerr := r.Route(context.Background(), ledgerReq(1, 1))
			switch mode {
			case config.LoadOverlayMax:
				if free != 0 || rerr == nil {
					t.Fatalf("max: free %d, err %v: the stale report should still hide the worker (that is today's behaviour)", free, rerr)
				}
			default:
				if free != 2 || rerr != nil {
					t.Fatalf("ledger: free %d, err %v: the worker has been emptied by this gateway", free, rerr)
				}
				rt.release()
			}
		})
	}
}

// A stale-low report is still covered by the local count, in both modes.
func TestStaleLowReportIsCoveredByTheLocalCount(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(2)}, noBreaker)
	withLedger(t, e.regEnv)
	r := e.gw.router
	e.clock.Advance(time.Second)
	a, _ := r.Route(context.Background(), ledgerReq(1, 1))
	b, _ := r.Route(context.Background(), ledgerReq(1, 1))
	defer a.release()
	defer b.release()
	if _, rerr := r.Route(context.Background(), ledgerReq(1, 1)); rerr == nil {
		t.Fatal("a third request fitted on a worker with two slots, both taken by this gateway")
	}
	if n := r.freeSlots("qwen-7b"); n != 0 {
		t.Fatalf("free slots %d", n)
	}
}

// reserve (through the scheduler's AtCapacity check) and freeSlots must agree on whether a worker has room, for every
// mix of local load, reported load, report age and history: a model check over random states.
func TestReserveAndFreeSlotsAgreeUnderTheLedgerOverlay(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		rng := rand.New(rand.NewSource(seed))
		g1, g2 := newGateWorker(t, "w1"), newGateWorker(t, "w2")
		e := newResEnv(t, []protocol.WorkerSnapshot{g1.snap(3), g2.snap(2)}, noBreaker)
		withLedger(t, e.regEnv)
		r := e.gw.router
		var held []*routed
		for step := 0; step < 60; step++ {
			switch rng.Intn(4) {
			case 0: // take
				if rt, rerr := r.Route(context.Background(), ledgerReq(rng.Intn(50)+1, rng.Intn(50)+1)); rerr == nil {
					held = append(held, rt)
				}
			case 1: // finish one
				if len(held) > 0 {
					i := rng.Intn(len(held))
					held[i].release()
					held = append(held[:i], held[i+1:]...)
				}
			case 2: // a new registry report, of random age and content
				s1, s2 := g1.snap(3), g2.snap(2)
				s1.Metrics.ActiveRequests, s2.Metrics.ActiveRequests = rng.Intn(5), rng.Intn(4)
				s1.HeartbeatAgeSeconds, s2.HeartbeatAgeSeconds = rng.Float64()*2, rng.Float64()*2
				e.lister.set([]protocol.WorkerSnapshot{s1, s2}, nil)
				e.refresh(t)
			default:
				e.clock.Advance(time.Duration(rng.Intn(700)) * time.Millisecond)
			}
			free := r.freeSlots("qwen-7b")
			rt, rerr := r.Route(context.Background(), ledgerReq(1, 1))
			if (free > 0) != (rerr == nil) {
				t.Fatalf("seed %d step %d: freeSlots=%d but Route err=%v: the two disagree about capacity", seed, step, free, rerr)
			}
			if rerr == nil {
				rt.release()
			}
		}
		for _, h := range held {
			h.release()
		}
		wantTotals(t, r.sig, 0, 0, fmt.Sprintf("seed %d end", seed))
	}
}

// Concurrent routing, hand-offs, cancellations and releases never leave the ledger or a slot behind (run with -race).
func TestLedgerSurvivesConcurrentRoutingAndHandOffs(t *testing.T) {
	g1, g2 := newGateWorker(t, "w1"), newGateWorker(t, "w2")
	e := newResEnv(t, []protocol.WorkerSnapshot{g1.snap(2), g2.snap(2)}, noBreaker, queueCfg(64, time.Minute))
	tr := withLedger(t, e.regEnv)
	r := e.gw.router
	// Time must pass: with a frozen clock every finish stays inside the report's boundary buckets, where the
	// overlay deliberately does not subtract it (an over-count that only a later bucket, or a new report, clears).
	// The clock runs fast and the registry is re-read, as a real gateway does every refresh interval.
	stop := make(chan struct{})
	var clockDone sync.WaitGroup
	clockDone.Add(1)
	go func() {
		defer clockDone.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			e.clock.Advance(15 * time.Millisecond)
			if i%5 == 0 {
				_ = e.gw.router.cache.Refresh(context.Background())
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i)))
			for j := 0; j < 200; j++ {
				req := ledgerReq(rng.Intn(100)+1, rng.Intn(100)+1)
				ctx, cancel := context.WithCancel(context.Background())
				rt, w, rerr := r.RouteOrWait(ctx, req, "")
				if rerr != nil {
					cancel()
					continue
				}
				if w != nil {
					if rng.Intn(3) == 0 {
						cancel() // abandon while queued
					}
					rt = awaitLikeTheServer(t, e.rs.waitq, r, ctx, w, req)
					if rt == nil {
						cancel()
						continue
					}
				}
				if rt != nil {
					if rng.Intn(4) == 0 {
						time.Sleep(time.Duration(rng.Intn(200)) * time.Microsecond)
					}
					rt.release()
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	clockDone.Wait()
	eventually(t, 5*time.Second, func() bool {
		n, w := tr.Totals()
		return n == 0 && w == 0 && r.InFlight("w1") == 0 && r.InFlight("w2") == 0
	}, "ledger and slots back to zero")
}

// awaitLikeTheServer is Server.awaitSlot without the observer calls: wait, take a hand-off (and charge it), or route
// again when woken, parking in between. nil means the waiter left (the client went away) or timed out.
func awaitLikeTheServer(t *testing.T, q *resilience.Queue, r *router, ctx context.Context, w *resilience.Waiter, req *protocol.InferenceRequest) *routed {
	t.Helper()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		g, out := q.Wait(ctx, w, timer.C)
		switch out {
		case resilience.TimedOut:
			t.Error("a waiter was never served: the model of the queue and the router disagree")
			return nil
		case resilience.ClientGone:
			return nil
		}
		if g.Kind == resilience.Handoff {
			rt := g.Value.(*routed)
			r.charge(rt, req)
			return rt
		}
		for {
			rt, rerr := r.Route(ctx, req)
			if rerr == nil {
				q.Done(w)
				return rt
			}
			if !q.Park(w) {
				break
			}
		}
	}
}

// The history buckets and tracked workers stay bounded with worker-ID churn at the gateway level.
func TestSignalsTrackerFollowsTheSnapshot(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(2)}, noBreaker)
	tr := withLedger(t, e.regEnv)
	if tr.Stats().Tracked != 1 {
		t.Fatalf("tracked %d", tr.Stats().Tracked)
	}
	var ws []protocol.WorkerSnapshot
	for i := 0; i < 50; i++ {
		s := g.snap(2)
		s.WorkerID = fmt.Sprintf("churn-%d", i)
		ws = append(ws, s)
	}
	e.lister.set(ws, nil)
	e.clock.Advance(time.Second)
	e.refresh(t)
	if s := tr.Stats(); s.Tracked != 50 || s.Pruned != 1 {
		t.Fatalf("stats %+v", s)
	}
}

// A scheduler built for each existing strategy sees the overlaid load: the strategies need no change.
func TestLedgerOverlayWorksWithEveryExistingStrategy(t *testing.T) {
	for _, name := range scheduler.Strategies() {
		t.Run(name, func(t *testing.T) {
			g1, g2 := newGateWorker(t, "w1"), newGateWorker(t, "w2")
			e := newResEnv(t, []protocol.WorkerSnapshot{g1.snap(3), g2.snap(3)}, noBreaker)
			tr := withLedger(t, e.regEnv)
			sched, err := scheduler.New(name)
			if err != nil {
				t.Fatal(err)
			}
			e.gw.router.sched = sched
			var held []*routed
			for i := 0; i < 6; i++ {
				rt, rerr := e.gw.router.Route(context.Background(), ledgerReq(10, 10))
				if rerr != nil {
					t.Fatalf("request %d: %v", i, rerr)
				}
				held = append(held, rt)
			}
			if _, rerr := e.gw.router.Route(context.Background(), ledgerReq(10, 10)); rerr == nil {
				t.Fatal("a seventh request fitted in six slots")
			}
			for _, h := range held {
				h.release()
			}
			wantTotals(t, tr, 0, 0, name)
		})
	}
}

func TestLoadOverlayConfigIsWiredByNewRegistry(t *testing.T) {
	for _, tc := range []struct {
		overlay string
		want    bool
	}{{config.LoadOverlayMax, false}, {config.LoadOverlayLedger, true}} {
		cfg := config.Default()
		cfg.Gateway = testConfig("http://127.0.0.1:1")
		cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
		cfg.Scheduler.LoadOverlay = tc.overlay
		gw, err := NewRegistry(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		if got := gw.router.sig != nil; got != tc.want {
			t.Fatalf("load_overlay %q: ledger enabled = %v, want %v", tc.overlay, got, tc.want)
		}
	}
}

// BenchmarkRouteLedger is BenchmarkRoute with the ledger overlay on (plan D18 budget for decisions: 25 us at 50
// workers, 400 us at 1,000). The workers carry a report so the overlay does its history lookups, and every
// iteration is a reserve plus a release (a start and a finish in the history).
func BenchmarkRouteLedger(b *testing.B) {
	for _, n := range []int{3, 50, 1000} {
		b.Run("w"+strconv.Itoa(n), func(b *testing.B) {
			var ws []protocol.WorkerSnapshot
			for i := 0; i < n; i++ {
				s := snapWorker("w"+strconv.Itoa(i), "m", 0.3)
				s.Address, s.MaxConcurrency = "http://127.0.0.1:1", 1<<30
				s.Metrics.ActiveRequests = i % 4
				ws = append(ws, s)
			}
			lister := &fakeLister{workers: ws}
			gw, err := newRegistryServer(testConfig("http://127.0.0.1:1"), "least-active", 5*time.Second, lister, slog.New(slog.NewJSONHandler(io.Discard, nil)))
			if err != nil {
				b.Fatal(err)
			}
			gw.router.enableLedger(ledgerCfg())
			if err := gw.router.cache.Refresh(context.Background()); err != nil {
				b.Fatal(err)
			}
			req := &protocol.InferenceRequest{Model: "m", EstInputTokens: 100, EstOutputTokens: 400}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					rt, apiErr := gw.router.Route(context.Background(), req)
					if apiErr != nil {
						b.Fatal(apiErr)
					}
					rt.release()
				}
			})
		})
	}
}
