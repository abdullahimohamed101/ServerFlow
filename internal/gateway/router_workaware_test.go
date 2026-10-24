package gateway

import (
	"context"
	"io"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/cost"
	"serverflow/internal/scheduler"
	"serverflow/internal/scheduler/signals"
	"serverflow/pkg/protocol"
)

// Phase 14 PR C1: the router fills Observed with the work-ahead and capacity terms for least-tokens and least-work
// (ADR-022, D5, D6), and the strategies choose from them.

var t0 = time.Unix(1_700_000_000, 0)

// waRouter is a registry router on a fake clock with the ledger and the work-aware terms on.
func waRouter(t testing.TB, strategy string, w cost.Weights, ws []protocol.WorkerSnapshot) (*router, *fakeLister, *testClock) {
	t.Helper()
	lister := &fakeLister{workers: ws}
	gw, err := newRegistryServer(testConfig("http://127.0.0.1:1"), strategy, 5*time.Second, lister,
		slog.New(slog.NewJSONHandler(io.Discard, nil)), scheduler.WithCostWeights(w))
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{t: t0}
	r := gw.router
	r.cache.now = clock.Now
	r.enableLedger(signals.Config{Window: 12 * time.Second, CapacityHalflife: 60 * time.Second, CapacityFloor: 0.25})
	r.enableWorkAware(w)
	if err := r.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r, lister, clock
}

func reporting(id string, active int, queued int, tps float64, hb time.Time) protocol.WorkerSnapshot {
	s := snapWorker(id, "m", 0)
	s.MaxConcurrency, s.LastHeartbeat = 16, hb
	s.Metrics = protocol.Metrics{ActiveRequests: active, QueuedInputTokens: queued, RecentTokensPerSecond: tps}
	return s
}

func req(in, out int) *protocol.InferenceRequest {
	return &protocol.InferenceRequest{Model: "m", EstInputTokens: in, EstOutputTokens: out}
}

// capture is a scheduler that records the view it was given and chooses a worker by ID.
type capture struct {
	mu   sync.Mutex
	view []protocol.WorkerSnapshot
	pick string
}

func (c *capture) SelectWorker(_ context.Context, _ *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	c.mu.Lock()
	c.view = append([]protocol.WorkerSnapshot(nil), ws...)
	c.mu.Unlock()
	for i := range ws {
		if ws[i].WorkerID == c.pick {
			w := ws[i]
			return &w, nil
		}
	}
	w := ws[0]
	return &w, nil
}

func (c *capture) of(id string) protocol.ObservedLoad {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.view {
		if w.WorkerID == id {
			return w.Observed
		}
	}
	return protocol.ObservedLoad{}
}

// Hand-computed. Weights: input 0.5, output 1, so the request (100 in, 400 out) costs 50 + 400 = 450.
//
//	w1 reports 2 active, 600 queued input tokens, 200 tokens/s; w2 is idle and has never reported a rate.
//	w1: ledger 0 + foreign 2 x mean 450 (the request's own cost until a mean exists) + queued 600 x 0.5 = 1200
//	w2: 0, and a capacity that is the fleet prior (the median of the sampled workers: 200), flagged as such.
func TestRouterFillsTheWorkAheadAndCapacityTerms(t *testing.T) {
	w := cost.Weights{InputWeight: 0.5, OutputFraction: 1}
	r, lister, clock := waRouter(t, scheduler.LeastWork, w, nil)
	lister.set([]protocol.WorkerSnapshot{reporting("w1", 2, 600, 200, t0), reporting("w2", 0, 0, 0, t0)}, nil)
	clock.Advance(time.Second)
	if err := r.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	cap := &capture{pick: "w2"}
	r.sched = cap
	first, rerr := r.Route(context.Background(), req(100, 400))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer first.release()
	o1, o2 := cap.of("w1"), cap.of("w2")
	if o1.WorkAheadTokens != 1200 || o1.CapacityTPS != 200 || o1.Flags&protocol.ObservedPriorCapacity != 0 || o1.Flags&protocol.ObservedLedger == 0 {
		t.Fatalf("w1: %+v", o1)
	}
	if o2.WorkAheadTokens != 0 || o2.CapacityTPS != 200 || o2.Flags&protocol.ObservedPriorCapacity == 0 {
		t.Fatalf("w2: want the prior 200 flagged as a prior: %+v", o2)
	}
	if o1.EffectiveActive != 2 {
		t.Fatalf("effective active %d", o1.EffectiveActive)
	}
	// the chosen request is now in w2's ledger at its weighted cost
	if n, work := ledgerOf(r, &first.worker); n != 1 || work != 450 {
		t.Fatalf("ledger %d / %d, want 1 / 450", n, work)
	}
	// the second decision sees it: w2 has 450 ahead (its own ledger), w1 is unchanged
	cap.pick = "w1"
	second, rerr := r.Route(context.Background(), req(100, 400))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer second.release()
	if got := cap.of("w2").WorkAheadTokens; got != 450 {
		t.Fatalf("w2 work ahead %v, want 450", got)
	}
	if got := cap.of("w1").WorkAheadTokens; got != 1200 {
		t.Fatalf("w1 work ahead %v, want 1200", got)
	}
}

// Foreign load is valued at the model's mean request cost, an EWMA of what this gateway routed.
func TestForeignLoadIsPricedAtTheMeanRequestCost(t *testing.T) {
	r, lister, clock := waRouter(t, scheduler.LeastTokens, cost.DefaultWeights(), nil)
	lister.set([]protocol.WorkerSnapshot{reporting("busy", 3, 0, 100, t0), reporting("idle", 0, 0, 100, t0)}, nil)
	clock.Advance(time.Second)
	if err := r.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	cap := &capture{pick: "idle"}
	r.sched = cap
	for _, c := range [][2]int{{100, 400}, {500, 500}} { // costs 500 then 1000
		rt, rerr := r.Route(context.Background(), req(c[0], c[1]))
		if rerr != nil {
			t.Fatal(rerr)
		}
		rt.release()
	}
	if m, ok := r.sig.MeanCost("m"); !ok || math.Abs(m-550) > 1e-9 { // 500 + 0.1 x (1000 - 500)
		t.Fatalf("mean cost %v %v, want 550", m, ok)
	}
	rt, rerr := r.Route(context.Background(), req(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt.release()
	if got := cap.of("busy").WorkAheadTokens; math.Abs(got-3*550) > 1e-9 {
		t.Fatalf("busy: %v, want 3 foreign requests x 550 = 1650", got)
	}
}

// Only workers the registry offers are sampled for the capacity estimate: a draining or unhealthy worker's report
// must not move the fleet prior that cold workers use.
func TestOnlyWorkersTheRegistryOffersAreSampledForCapacity(t *testing.T) {
	r, lister, clock := waRouter(t, scheduler.LeastWork, cost.DefaultWeights(), nil)
	draining := reporting("drain", 2, 0, 5000, t0)
	draining.State, draining.Eligible = protocol.StateDraining, false
	sick := reporting("sick", 2, 0, 7000, t0)
	sick.Health = protocol.HealthUnhealthy
	cold := reporting("cold", 0, 0, 0, t0)
	lister.set([]protocol.WorkerSnapshot{reporting("ok", 1, 0, 100, t0), draining, sick, cold}, nil)
	clock.Advance(time.Second)
	if err := r.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, w := range []*protocol.WorkerSnapshot{&draining, &sick} {
		if tps, prior := r.sig.Capacity(signals.KeyOf(w)); !prior || tps != 100 {
			t.Errorf("%s was sampled: capacity %v prior=%v", w.WorkerID, tps, prior)
		}
	}
	if tps, prior := r.sig.Capacity(signals.KeyOf(&cold)); !prior || tps != 100 {
		t.Errorf("the prior was moved by workers the registry does not offer: %v %v", tps, prior)
	}
}

func TestLeastWorkChoosesTheFasterWorkerAndLeastTokensTakesTurns(t *testing.T) {
	ws := []protocol.WorkerSnapshot{reporting("fast", 1, 0, 400, t0), reporting("slow", 1, 0, 100, t0)}
	r, lister, clock := waRouter(t, scheduler.LeastWork, cost.DefaultWeights(), nil)
	lister.set(ws, nil)
	clock.Advance(time.Second)
	if err := r.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt, rerr := r.Route(context.Background(), req(100, 100))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt.release()
	if rt.worker.WorkerID != "fast" {
		t.Fatalf("least-work picked %s: equal work ahead, the faster worker predicts the shorter wait", rt.worker.WorkerID)
	}

	r2, lister2, clock2 := waRouter(t, scheduler.LeastTokens, cost.DefaultWeights(), nil)
	lister2.set(ws, nil)
	clock2.Advance(time.Second)
	if err := r2.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// equal work ahead (the same foreign load on both): least-tokens ignores speed and the two take turns
	a, _ := r2.Route(context.Background(), req(100, 100))
	defer a.release()
	b, _ := r2.Route(context.Background(), req(100, 100))
	defer b.release()
	if a.worker.WorkerID == b.worker.WorkerID {
		t.Fatalf("least-tokens sent both requests to %s: the second sees work ahead on it", a.worker.WorkerID)
	}
}

// A request's weighted cost is exactly what the ledger holds, and exactly what release removes.
func TestTheLedgerHoldsTheWeightedCostAndReturnsToZero(t *testing.T) {
	w := cost.Weights{InputWeight: 0.5, OutputFraction: 0.3}
	r, _, _ := waRouter(t, scheduler.LeastTokens, w, []protocol.WorkerSnapshot{reporting("w1", 0, 0, 0, t0)})
	rt, rerr := r.Route(context.Background(), req(101, 1000))
	if rerr != nil {
		t.Fatal(rerr)
	}
	want := int64(math.Ceil(0.5*101)) + int64(math.Ceil(0.3*1000)) // 51 + 300
	if _, work := ledgerOf(r, &rt.worker); work != want || rt.cost.Load() != want {
		t.Fatalf("ledger %d, routed %d, want %d", work, rt.cost.Load(), want)
	}
	rt.release()
	wantTotals(t, r.sig, 0, 0, "after release")
}

func TestAPanickingStrategyLeavesNoLedgerEntryAndNoMeanCost(t *testing.T) {
	r, _, _ := waRouter(t, scheduler.LeastWork, cost.DefaultWeights(), []protocol.WorkerSnapshot{reporting("w1", 0, 0, 0, t0)})
	r.sched = panicScheduler{}
	func() {
		defer func() { _ = recover() }()
		_, _ = r.Route(context.Background(), req(10, 10))
	}()
	wantTotals(t, r.sig, 0, 0, "after the panic")
	if _, ok := r.sig.MeanCost("m"); ok {
		t.Fatal("a request that was never routed moved the mean cost")
	}
	if r.InFlight("w1") != 0 {
		t.Fatal("slot leaked")
	}
}

// A control plane that reports absurd numbers cannot make the arithmetic overflow, turn a score non-finite, or make
// a worker free: the terms are clamped, and the strategy still returns an eligible worker.
func TestHostileReportsStayFiniteAndBounded(t *testing.T) {
	hostile := func(id string, queued int, tps float64) protocol.WorkerSnapshot {
		return reporting(id, 1, queued, tps, t0)
	}
	ws := []protocol.WorkerSnapshot{
		hostile("a", math.MaxInt32, math.Inf(1)), hostile("b", -5, math.NaN()), hostile("c", math.MaxInt, -7), hostile("d", 0, 1e300),
		hostile("e", 100, 100),
	}
	for _, st := range []string{scheduler.LeastTokens, scheduler.LeastWork} {
		r, lister, clock := waRouter(t, st, cost.DefaultWeights(), nil)
		lister.set(ws, nil)
		clock.Advance(time.Second)
		if err := r.cache.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		cap := &capture{}
		r.sched = cap
		rt, rerr := r.Route(context.Background(), req(1<<20, 1<<20))
		if rerr != nil {
			t.Fatalf("%s: %v", st, rerr)
		}
		rt.release()
		for _, w := range cap.view {
			o := w.Observed
			if math.IsNaN(o.WorkAheadTokens) || math.IsInf(o.WorkAheadTokens, 0) || o.WorkAheadTokens < 0 || o.WorkAheadTokens > 1e15 {
				t.Errorf("%s/%s: work ahead %v", st, w.WorkerID, o.WorkAheadTokens)
			}
			if math.IsNaN(o.CapacityTPS) || math.IsInf(o.CapacityTPS, 0) || o.CapacityTPS < 0 || o.CapacityTPS > 1e7 {
				t.Errorf("%s/%s: capacity %v", st, w.WorkerID, o.CapacityTPS)
			}
		}
		// the real strategy on the same views
		r2, lister2, clock2 := waRouter(t, st, cost.DefaultWeights(), nil)
		lister2.set(ws, nil)
		clock2.Advance(time.Second)
		if err := r2.cache.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			rt, rerr := r2.Route(context.Background(), req(1000, 1000))
			if rerr != nil {
				t.Fatalf("%s: %v", st, rerr)
			}
			rt.release()
		}
	}
}

// Without the ledger the router holds no tracker and the strategy degrades to least-active.
func TestWorkAwareStrategyWithoutTheLedgerDegradesToLeastActive(t *testing.T) {
	lister := &fakeLister{workers: []protocol.WorkerSnapshot{reporting("a", 3, 0, 0, t0), reporting("b", 1, 0, 0, t0)}}
	gw, err := newRegistryServer(testConfig("http://127.0.0.1:1"), scheduler.LeastWork, 5*time.Second, lister, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := gw.router.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt, rerr := gw.router.Route(context.Background(), req(10, 10))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt.release()
	if rt.worker.WorkerID != "b" || rt.worker.Observed.Flags&protocol.ObservedDegraded == 0 {
		t.Fatalf("picked %s flags %b, want b and degraded", rt.worker.WorkerID, rt.worker.Observed.Flags)
	}
}

func TestNewRegistryWiresTheWorkAwareSettings(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mk := func(strategy, overlay string, mut func(*config.Config)) (*Server, error) {
		cfg := config.Default()
		cfg.Gateway = testConfig("http://127.0.0.1:1")
		cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
		cfg.Scheduler.Strategy, cfg.Scheduler.LoadOverlay = strategy, overlay
		if mut != nil {
			mut(&cfg)
		}
		return NewRegistry(cfg, logger)
	}
	for _, st := range []string{scheduler.LeastTokens, scheduler.LeastWork} {
		gw, err := mk(st, config.LoadOverlayLedger, func(c *config.Config) {
			c.Scheduler.Cost = config.CostConfig{OutputFraction: 0.4, InputWeight: 0.7}
			c.Scheduler.Signals = config.SignalsConfig{CapacityHalflife: 5 * time.Second, CapacityFloor: 0.5}
		})
		if err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		r := gw.router
		if r.sig == nil || !r.workAware || r.weights != (cost.Weights{InputWeight: 0.7, OutputFraction: 0.4}) {
			t.Fatalf("%s: sig=%v workAware=%v weights=%+v", st, r.sig != nil, r.workAware, r.weights)
		}
		if c := r.sig.Config(); c.CapacityHalflife != 5*time.Second || c.CapacityFloor != 0.5 {
			t.Fatalf("%s: the tracker runs with half-life %v and floor %v, want 5s and 0.5", st, c.CapacityHalflife, c.CapacityFloor)
		}
		if _, err := mk(st, config.LoadOverlayMax, nil); err == nil {
			t.Fatalf("%s with the max overlay must be refused", st)
		}
	}
	// the other strategies keep the weights and the work-aware fill off, even with the ledger on
	gw, err := mk(scheduler.LeastActive, config.LoadOverlayLedger, nil)
	if err != nil || gw.router.workAware || gw.router.sig == nil {
		t.Fatalf("least-active + ledger: err=%v workAware=%v", err, gw != nil && gw.router.workAware)
	}
	gw, err = mk(scheduler.LeastActive, config.LoadOverlayMax, nil)
	if err != nil || gw.router.workAware || gw.router.sig != nil {
		t.Fatalf("defaults: err=%v", err)
	}
}

// route() reports the wait least-work predicted for the decision it made, priced with the configured weights.
// Hand-computed. Weights: input 0.5, output 1, so the request (100 in, 400 out) costs 450 (not the 500 estimated).
//
//	w1 reports 2 active, 600 queued input tokens, 200 tokens/s, and is the only worker:
//	work ahead = foreign 2 x mean 450 (the request's own cost until a mean exists) + 600 x 0.5 = 1200
//	predicted wait = (1200 + 450) / 200 = 8.25 s
func TestRoutePredictsTheWaitWithTheWeightedCost(t *testing.T) {
	w := cost.Weights{InputWeight: 0.5, OutputFraction: 1}
	r, lister, clock := waRouter(t, scheduler.LeastWork, w, nil)
	lister.set([]protocol.WorkerSnapshot{reporting("w1", 2, 600, 200, t0)}, nil)
	clock.Advance(time.Second)
	if err := r.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt, rerr := r.Route(context.Background(), req(100, 400))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt.release()
	if !rt.hasPrediction || math.Abs(rt.predicted-8.25) > 1e-9 {
		t.Fatalf("predicted %v (has=%v), want 8.25", rt.predicted, rt.hasPrediction)
	}
}

// The scheduler NewRegistry builds must score with the configured cost weights (scheduler.WithCostWeights), not
// the defaults: least-work's choice depends on the request's own cost c. Hand-computed, for a request of 1000
// estimated input tokens and no output budget:
//
//	a: 100 tokens ahead at 100 tokens/s     b: nothing ahead at 10 tokens/s
//	default weights (c = 1000):      a (100+1000)/100 = 11     b 1000/10 = 100      -> a
//	input_weight 0.001 (c = 1):      a 101/100 = 1.01          b 1/10 = 0.1         -> b
func TestNewRegistrySchedulerScoresWithTheConfiguredCostWeights(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pick := func(weights config.CostConfig) string {
		cfg := config.Default()
		cfg.Gateway = testConfig("http://127.0.0.1:1")
		cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
		cfg.Scheduler.Strategy, cfg.Scheduler.LoadOverlay = scheduler.LeastWork, config.LoadOverlayLedger
		cfg.Scheduler.Cost = weights
		gw, err := NewRegistry(cfg, logger)
		if err != nil {
			t.Fatal(err)
		}
		mk := func(id string, ahead, rate float64) protocol.WorkerSnapshot {
			w := snapWorker(id, "m", 0)
			w.Observed = protocol.ObservedLoad{WorkAheadTokens: ahead, CapacityTPS: rate, Flags: protocol.ObservedLedger}
			return w
		}
		chosen, err := gw.router.sched.SelectWorker(context.Background(), req(1000, 0), []protocol.WorkerSnapshot{mk("a", 100, 100), mk("b", 0, 10)})
		if err != nil {
			t.Fatal(err)
		}
		return chosen.WorkerID
	}
	if got := pick(config.CostConfig{OutputFraction: 1, InputWeight: 1}); got != "a" {
		t.Fatalf("default weights: picked %s, want a", got)
	}
	if got := pick(config.CostConfig{OutputFraction: 1, InputWeight: 0.001}); got != "b" {
		t.Fatalf("input_weight 0.001: picked %s, want b (the scheduler ignores the configured weights)", got)
	}
}

// A hand-off's waiter charges the weighted cost, the same as reserve.
func TestAWaiterChargesTheWeightedCost(t *testing.T) {
	w := cost.Weights{InputWeight: 0.5, OutputFraction: 0.5}
	snap := reporting("w1", 0, 0, 0, t0)
	r, _, _ := waRouter(t, scheduler.LeastTokens, w, []protocol.WorkerSnapshot{snap})
	rt := &routed{lease: r.sig.Begin(r.cache.now(), signals.KeyOf(&snap))} // a hand-off: a token, no cost yet
	r.charge(rt, req(100, 100))
	if got := rt.cost.Load(); got != 100 { // 50 + 50
		t.Fatalf("charged %d, want 100", got)
	}
	r.charge(rt, req(5000, 5000)) // a second charge is ignored
	if got := rt.cost.Load(); got != 100 {
		t.Fatalf("a second charge changed the cost to %d", got)
	}
}

// BenchmarkRouteWorkAware is BenchmarkRoute with the ledger and the work-aware terms on, per strategy (plan D18:
// at most 25 us at 50 workers and 400 us at 1,000). Every iteration reserves and releases.
func BenchmarkRouteWorkAware(b *testing.B) {
	for _, st := range []string{scheduler.LeastActive, scheduler.LeastTokens, scheduler.LeastWork} {
		for _, n := range []int{3, 50, 1000} {
			b.Run(st+"/w"+strconv.Itoa(n), func(b *testing.B) {
				ws := make([]protocol.WorkerSnapshot, n)
				for i := range ws {
					s := reporting("w"+strconv.Itoa(i), i%4, (i%5)*100, float64(100+i%7*20), t0)
					s.HeartbeatAgeSeconds, s.MaxConcurrency = 0.3, 1<<30
					ws[i] = s
				}
				var r *router
				if st == scheduler.LeastActive {
					lister := &fakeLister{workers: ws}
					gw, err := newRegistryServer(testConfig("http://127.0.0.1:1"), st, 5*time.Second, lister, slog.New(slog.NewJSONHandler(io.Discard, nil)))
					if err != nil {
						b.Fatal(err)
					}
					r = gw.router
					r.enableLedger(signals.Config{Window: 12 * time.Second})
					if err := r.cache.Refresh(context.Background()); err != nil {
						b.Fatal(err)
					}
				} else {
					r, _, _ = waRouter(b, st, cost.DefaultWeights(), ws)
				}
				rq := req(100, 400)
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						rt, apiErr := r.Route(context.Background(), rq)
						if apiErr != nil {
							b.Fatal(apiErr)
						}
						rt.release()
					}
				})
			})
		}
	}
}
