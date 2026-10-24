package scheduler

import (
	"context"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"sort"
	"sync"
	"testing"

	"serverflow/internal/cost"
	"serverflow/pkg/protocol"
)

// The work-aware strategies (ADR-022, D5): least-tokens minimises the work ahead of the request, least-work the
// predicted wait (work ahead + the request's own cost) / capacity.

// ow is a worker as the router presents it with the ledger overlay on: active is the effective active count, ahead
// the work ahead in tokens and rate the capacity estimate in tokens per second (0 = unknown).
func ow(id string, active int, ahead, rate float64) protocol.WorkerSnapshot {
	w := worker(id, active, 0)
	w.Observed = protocol.ObservedLoad{EffectiveActive: active, WorkAheadTokens: ahead, CapacityTPS: rate, Flags: protocol.ObservedLedger}
	return w
}

func wreq(in, out int) *protocol.InferenceRequest {
	return &protocol.InferenceRequest{RequestID: "r", Model: model, EstInputTokens: in, EstOutputTokens: out}
}

func selectFrom(t *testing.T, s Scheduler, r *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) *protocol.WorkerSnapshot {
	t.Helper()
	w, err := s.SelectWorker(context.Background(), r, ws)
	if err != nil {
		t.Fatalf("SelectWorker: %v", err)
	}
	return w
}

func TestWorkAwareStrategyNamesAndKind(t *testing.T) {
	if !WorkAware(LeastTokens) || !WorkAware(LeastWork) {
		t.Fatal("the two strategies read the ledger")
	}
	for _, s := range []string{Random, RoundRobin, LeastActive, LeastQueue, "", "latency-aware"} {
		if WorkAware(s) {
			t.Errorf("%q does not read the ledger", s)
		}
	}
}

func TestLeastTokensPicksTheSmallestWorkAheadWhateverTheCounts(t *testing.T) {
	s := mustNew(t, LeastTokens)
	ws := []protocol.WorkerSnapshot{ow("a", 0, 900, 50), ow("b", 5, 100, 50), ow("c", 1, 500, 50)}
	if got := pick(t, s, ws); got != "b" {
		t.Fatalf("picked %s; b has the least work ahead (100) although it has the most active requests", got)
	}
	// least-active would choose a: the strategies differ exactly where one request is much bigger than another
	if got := pick(t, mustNew(t, LeastActive), ws); got != "a" {
		t.Fatalf("control: least-active picked %s", got)
	}
	// the request's own size does not change the order for least-tokens
	for _, r := range []*protocol.InferenceRequest{wreq(0, 0), wreq(10, 10), wreq(1<<20, 1<<20)} {
		if got := selectFrom(t, s, r, ws).WorkerID; got != "b" {
			t.Fatalf("request %+v: picked %s", r, got)
		}
	}
}

func TestLeastTokensIgnoresCapacity(t *testing.T) {
	s := mustNew(t, LeastTokens)
	ws := []protocol.WorkerSnapshot{ow("slow", 0, 100, 1), ow("fast", 0, 200, 1000)}
	if got := pick(t, s, ws); got != "slow" {
		t.Fatalf("picked %s: least-tokens counts tokens, not time", got)
	}
}

func TestTiesTakeTurnsInIDOrderForTheWorkAwareStrategies(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{ow("c", 0, 100, 50), ow("a", 0, 100, 50), ow("b", 0, 100, 50), ow("d", 0, 9000, 50)}
		var got []string
		for i := 0; i < 7; i++ {
			got = append(got, pick(t, s, ws))
		}
		if want := []string{"a", "b", "c", "a", "b", "c", "a"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got %v, want %v", st, got, want)
		}
	}
}

func TestTieToleranceIsRelativeOneInABillion(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		// 5e-10 apart: a tie
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{ow("a", 0, 1e6, 100), ow("b", 0, 1e6*(1+5e-10), 100)}
		seen := map[string]bool{}
		for i := 0; i < 4; i++ {
			seen[pick(t, s, ws)] = true
		}
		if len(seen) != 2 {
			t.Errorf("%s: scores 5e-10 apart must tie and take turns, saw %v", st, seen)
		}
		// 1e-8 apart: not a tie, the smaller always wins
		s = mustNew(t, st)
		ws = []protocol.WorkerSnapshot{ow("a", 0, 1e6*(1+1e-8), 100), ow("b", 0, 1e6, 100)}
		for i := 0; i < 4; i++ {
			if got := pick(t, s, ws); got != "b" {
				t.Errorf("%s: scores 1e-8 apart are not a tie; picked %s", st, got)
			}
		}
	}
}

// Hand-computed: the request costs 100+50 = 150 tokens.
//
//	a: (1000 + 150) / 100 = 11.5 s   b: (300 + 150) / 20 = 22.5 s   c: (0 + 150) / 10 = 15 s
func TestLeastWorkPredictsTheWaitFromWorkAheadAndCapacity(t *testing.T) {
	s := mustNew(t, LeastWork)
	ws := []protocol.WorkerSnapshot{ow("a", 3, 1000, 100), ow("b", 1, 300, 20), ow("c", 0, 0, 10)}
	r := wreq(100, 50)
	if got := selectFrom(t, s, r, ws).WorkerID; got != "a" {
		t.Fatalf("picked %s, want a (11.5 s against 22.5 s and 15 s)", got)
	}
	if got := pick(t, mustNew(t, LeastTokens), ws); got != "c" {
		t.Fatalf("control: least-tokens picks the empty worker, got %s", got)
	}
}

// The request's own cost is part of the wait: it is what makes speed differences matter on an empty fleet.
func TestLeastWorkCountsTheRequestsOwnCost(t *testing.T) {
	ws := []protocol.WorkerSnapshot{ow("a", 0, 0, 10), ow("b", 0, 100, 100)}
	// cost 50: a waits 5 s, b waits (100+50)/100 = 1.5 s -> b, although b has work ahead and a has none
	if got := selectFrom(t, mustNew(t, LeastWork), wreq(20, 30), ws).WorkerID; got != "b" {
		t.Fatalf("picked %s, want b", got)
	}
	// cost 1: a waits 0.1 s, b waits 1.01 s -> a
	if got := selectFrom(t, mustNew(t, LeastWork), wreq(0, 1), ws).WorkerID; got != "a" {
		t.Fatalf("picked %s, want a", got)
	}
}

func TestLeastWorkUsesTheCostWeights(t *testing.T) {
	ws := []protocol.WorkerSnapshot{ow("a", 0, 0, 10), ow("b", 0, 100, 100)}
	r := wreq(20, 10) // 30 tokens by default: a 3 s, b 1.3 s -> b
	if got := selectFrom(t, mustNew(t, LeastWork), r, ws).WorkerID; got != "b" {
		t.Fatalf("default weights: picked %s, want b", got)
	}
	// input weight 0 and output fraction 0.1: the request costs ceil(0.1*10) = 1 token -> a 0.1 s, b 1.01 s
	w := cost.Weights{InputWeight: 0, OutputFraction: 0.1}
	if got := selectFrom(t, mustNew(t, LeastWork, WithCostWeights(w)), r, ws).WorkerID; got != "a" {
		t.Fatalf("light request: picked %s, want a", got)
	}
}

func TestLeastWorkUsesTheMedianRateForAWorkerWithoutOne(t *testing.T) {
	// rates known: b 10, c 30 -> the median 20 stands in for a. cost 100.
	// a: (100+100)/20 = 10 s   b: (60+100)/10 = 16 s   c: (400+100)/30 = 16.7 s
	ws := []protocol.WorkerSnapshot{ow("a", 0, 100, 0), ow("b", 0, 60, 10), ow("c", 0, 400, 30)}
	if got := selectFrom(t, mustNew(t, LeastWork), wreq(0, 100), ws).WorkerID; got != "a" {
		t.Fatalf("picked %s, want a", got)
	}
	// never a free ride: with a's work ahead at 2000, the stand-in rate makes it lose (2100/20 = 105 s)
	ws[0].Observed.WorkAheadTokens = 2000
	if got := selectFrom(t, mustNew(t, LeastWork), wreq(0, 100), ws).WorkerID; got != "b" {
		t.Fatalf("picked %s, want b", got)
	}
}

func TestLeastWorkWithNoRateAnywhereScoresLikeLeastTokensAndSaysSo(t *testing.T) {
	ws := []protocol.WorkerSnapshot{ow("a", 0, 500, 0), ow("b", 0, 100, 0), ow("c", 0, 300, 0)}
	got := selectFrom(t, mustNew(t, LeastWork), wreq(10, 10), ws)
	if got.WorkerID != "b" {
		t.Fatalf("picked %s, want b (least work ahead)", got.WorkerID)
	}
	if got.Observed.Flags&protocol.ObservedDegraded == 0 {
		t.Fatalf("the fallback must be flagged: %+v", got.Observed)
	}
	// least-tokens needs no rate and does not degrade
	if g := selectFrom(t, mustNew(t, LeastTokens), wreq(10, 10), ws); g.Observed.Flags&protocol.ObservedDegraded != 0 {
		t.Fatalf("least-tokens flagged: %+v", g.Observed)
	}
	// a rate that is NaN, infinite, negative or zero is no rate
	for _, r := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -3, 0} {
		ws := []protocol.WorkerSnapshot{ow("a", 0, 500, r), ow("b", 0, 100, r)}
		g := selectFrom(t, mustNew(t, LeastWork), wreq(10, 10), ws)
		if g.WorkerID != "b" || g.Observed.Flags&protocol.ObservedDegraded == 0 {
			t.Errorf("rate %v: picked %s flags %b", r, g.WorkerID, g.Observed.Flags)
		}
	}
}

func TestWithoutALedgerObservationTheChoiceIsLeastActiveAndFlagged(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{worker("a", 3, 0), worker("b", 1, 0), worker("c", 2, 0)} // Observed absent
		g := selectFrom(t, s, wreq(10, 10), ws)
		if g.WorkerID != "b" || g.Observed.Flags&protocol.ObservedDegraded == 0 {
			t.Errorf("%s: picked %s flags %b, want b flagged degraded", st, g.WorkerID, g.Observed.Flags)
		}
		// ties among the least-active take turns
		ws = []protocol.WorkerSnapshot{worker("a", 1, 0), worker("b", 1, 0)}
		first, second := pick(t, s, ws), pick(t, s, ws)
		if first == second {
			t.Errorf("%s: a tie must take turns, got %s twice", st, first)
		}
	}
}

func TestAWorkerWithoutAnObservationScoresNeutrallyAmongObservedOnes(t *testing.T) {
	s := mustNew(t, LeastTokens)
	// valid scores 100, 300, 500 -> the unobserved worker scores the median 300: it never beats 100
	ws := []protocol.WorkerSnapshot{ow("a", 0, 100, 1), ow("b", 0, 300, 1), ow("c", 0, 500, 1), worker("u", 0, 0)}
	for i := 0; i < 5; i++ {
		if got := pick(t, s, ws); got != "a" {
			t.Fatalf("picked %s, want a", got)
		}
	}
	// but it is not exiled either: when every observed worker is worse than the median, it can win
	ws = []protocol.WorkerSnapshot{ow("a", 0, 100, 1), worker("u", 0, 0)} // the median of [100] is 100: a tie
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		seen[pick(t, s, ws)] = true
	}
	if !seen["u"] || !seen["a"] {
		t.Fatalf("the unobserved worker and the observed one tie: saw %v", seen)
	}
}

func TestInvalidWorkAheadScoresNeutrallyAndTheChoiceIsFlagged(t *testing.T) {
	bad := []float64{math.NaN(), math.Inf(1), math.Inf(-1), -5, -1e300}
	for _, st := range []string{LeastTokens, LeastWork} {
		for _, v := range bad {
			s := mustNew(t, st)
			// valid: a 100, c 900 (rate 10 everywhere); the garbage worker b scores the median (500)
			ws := []protocol.WorkerSnapshot{ow("a", 0, 100, 10), ow("b", 0, v, 10), ow("c", 0, 900, 10)}
			for i := 0; i < 3; i++ {
				if got := selectFrom(t, s, wreq(1, 1), ws); got.WorkerID != "a" {
					t.Fatalf("%s %v: picked %s, want a: garbage must not look best", st, v, got.WorkerID)
				}
			}
			// garbage never exiles: with the others both worse than the median it wins the tie-or-better
			ws = []protocol.WorkerSnapshot{ow("a", 0, 1000, 10), ow("b", 0, v, 10), ow("c", 0, 2000, 10)}
			got := selectFrom(t, s, wreq(1, 1), ws)
			if got.WorkerID != "a" { // median of the valid {1000, 2000} is 1500: a (1000) still wins
				t.Fatalf("%s %v: picked %s, want a", st, v, got.WorkerID)
			}
		}
		// all garbage: every eligible worker ties and they take turns
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{ow("a", 0, math.NaN(), 10), ow("b", 0, math.Inf(1), 10), ow("c", 0, -1, 10)}
		var got []string
		for i := 0; i < 3; i++ {
			g := selectFrom(t, s, wreq(1, 1), ws)
			got = append(got, g.WorkerID)
			if g.Observed.Flags&protocol.ObservedInvalid == 0 {
				t.Errorf("%s: the chosen garbage must be flagged invalid", st)
			}
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
			t.Errorf("%s: all-garbage views take turns, got %v", st, got)
		}
	}
}

func TestOverflowingScoresAreInvalidNotInfinite(t *testing.T) {
	// 1e308 + 1e308 overflows to +Inf inside the division: the worker scores the median, not "infinitely slow".
	s := mustNew(t, LeastWork)
	ws := []protocol.WorkerSnapshot{ow("a", 0, 1e308, 1e-300), ow("b", 0, 10, 10), ow("c", 0, 20, 10)}
	if got := selectFrom(t, s, wreq(1, 1), ws); got.WorkerID != "b" {
		t.Fatalf("picked %s, want b", got.WorkerID)
	}
	// huge but finite scores still order correctly
	ws = []protocol.WorkerSnapshot{ow("a", 0, 1e300, 1), ow("b", 0, 1e299, 1)}
	if got := pick(t, mustNew(t, LeastWork), ws); got != "b" {
		t.Fatalf("picked %s, want b", got)
	}
}

func TestWorkAwareNeverPicksAWorkerThatMayNotServe(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		best := func(mut func(*protocol.WorkerSnapshot)) []protocol.WorkerSnapshot {
			b := ow("best", 0, 0, 1000) // by far the best score
			mut(&b)
			return []protocol.WorkerSnapshot{b, ow("ok", 0, 1e9, 1)}
		}
		cases := map[string]func(*protocol.WorkerSnapshot){
			"not ready":         func(w *protocol.WorkerSnapshot) { w.State = protocol.StateDraining },
			"not eligible":      func(w *protocol.WorkerSnapshot) { w.Eligible = false },
			"unhealthy":         func(w *protocol.WorkerSnapshot) { w.Health = protocol.HealthUnhealthy },
			"lost":              func(w *protocol.WorkerSnapshot) { w.Health = protocol.HealthLost },
			"other model":       func(w *protocol.WorkerSnapshot) { w.Model = "llama" },
			"full":              func(w *protocol.WorkerSnapshot) { w.Metrics.ActiveRequests = w.MaxConcurrency },
			"capacity zero":     func(w *protocol.WorkerSnapshot) { w.MaxConcurrency = 0 },
			"effective over":    func(w *protocol.WorkerSnapshot) { w.Metrics.ActiveRequests = w.MaxConcurrency + 3 },
			"negative capacity": func(w *protocol.WorkerSnapshot) { w.MaxConcurrency = -1 },
		}
		for name, mut := range cases {
			for i := 0; i < 3; i++ {
				if got := pick(t, s, best(mut)); got != "ok" {
					t.Errorf("%s/%s: picked %s", st, name, got)
				}
			}
		}
		// nothing eligible: the same NoWorkerError as the other strategies
		_, err := s.SelectWorker(context.Background(), wreq(1, 1), []protocol.WorkerSnapshot{ow("x", 0, 0, 1)}[:0])
		if _, ok := err.(*NoWorkerError); !ok {
			t.Errorf("%s: want *NoWorkerError, got %v", st, err)
		}
	}
}

func TestWorkAwareReturnsACopyAndLeavesTheInputAlone(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{ow("a", 0, 5, 10), ow("b", 0, 1, 10)}
		before := append([]protocol.WorkerSnapshot(nil), ws...)
		got := selectFrom(t, s, wreq(1, 1), ws)
		got.Observed.Flags |= protocol.ObservedDegraded
		got.Metrics.ActiveRequests = 99
		if !reflect.DeepEqual(ws, before) {
			t.Fatalf("%s: the input changed: %+v", st, ws)
		}
	}
}

func TestWorkAwareHonoursACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, st := range []string{LeastTokens, LeastWork} {
		if _, err := mustNew(t, st).SelectWorker(ctx, wreq(1, 1), []protocol.WorkerSnapshot{ow("a", 0, 0, 1)}); err == nil {
			t.Errorf("%s: a cancelled context must fail", st)
		}
	}
}

// I2: the same view gives the same choice from a fresh scheduler.
func TestWorkAwareIsDeterministic(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	for _, st := range []string{LeastTokens, LeastWork} {
		for i := 0; i < 500; i++ {
			ws := randomWorkers(rng)
			r := wreq(rng.IntN(5000), rng.IntN(5000))
			a, errA := mustNew(t, st).SelectWorker(context.Background(), r, ws)
			b, errB := mustNew(t, st).SelectWorker(context.Background(), r, ws)
			if (errA == nil) != (errB == nil) || (a != nil && !sameViews(*a, *b)) {
				t.Fatalf("%s: two fresh schedulers disagree on %+v: %v / %v", st, ws, a, b)
			}
		}
	}
}

// --- a naive reference implementation of the plan's formulas ---------------------------------------------------

// refScores computes, with exact rational arithmetic, the plan's score of every worker that may serve (D5), for
// views whose signals are all valid. The strategy is right if its choice is among the minima within the tie
// tolerance.
func refScores(kind string, w cost.Weights, r *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) map[string]*big.Rat {
	c := new(big.Rat).SetInt64(int64(math.Ceil(w.InputWeight*float64(r.EstInputTokens))) + int64(math.Ceil(w.OutputFraction*float64(r.EstOutputTokens))))
	var known []float64
	for _, x := range ws {
		if mayServe(x) && x.Observed.CapacityTPS > 0 {
			known = append(known, x.Observed.CapacityTPS)
		}
	}
	sort.Float64s(known)
	var prior *big.Rat
	switch n := len(known); {
	case n == 0:
	case n%2 == 1:
		prior = new(big.Rat).SetFloat64(known[n/2])
	default:
		prior = new(big.Rat).Quo(new(big.Rat).Add(new(big.Rat).SetFloat64(known[n/2-1]), new(big.Rat).SetFloat64(known[n/2])), big.NewRat(2, 1))
	}
	out := map[string]*big.Rat{}
	for _, x := range ws {
		if !mayServe(x) {
			continue
		}
		ahead := new(big.Rat).SetFloat64(x.Observed.WorkAheadTokens)
		if kind == LeastTokens || prior == nil {
			out[x.WorkerID] = ahead
			continue
		}
		rate := prior
		if x.Observed.CapacityTPS > 0 {
			rate = new(big.Rat).SetFloat64(x.Observed.CapacityTPS)
		}
		out[x.WorkerID] = new(big.Rat).Quo(new(big.Rat).Add(ahead, c), rate)
	}
	return out
}

func withinTie(v, min *big.Rat) bool {
	// v <= min * (1 + 1e-9)
	limit := new(big.Rat).Mul(min, new(big.Rat).Add(big.NewRat(1, 1), big.NewRat(1, 1_000_000_000)))
	return v.Cmp(limit) <= 0
}

func randomObserved(rng *rand.Rand, ws []protocol.WorkerSnapshot, valid bool) {
	for i := range ws {
		ahead := float64(rng.IntN(20000))
		if rng.IntN(5) == 0 {
			ahead = float64(rng.IntN(4)) * 500 // many exact ties
		}
		rate := float64(1 + rng.IntN(400))
		if rng.IntN(4) == 0 {
			rate = 0 // unknown
		}
		ws[i].Observed = protocol.ObservedLoad{EffectiveActive: ws[i].Metrics.ActiveRequests, WorkAheadTokens: ahead, CapacityTPS: rate, Flags: protocol.ObservedLedger}
		if !valid && rng.IntN(6) == 0 {
			ws[i].Observed.WorkAheadTokens = []float64{math.NaN(), math.Inf(1), -1, math.Inf(-1)}[rng.IntN(4)]
		}
		if !valid && rng.IntN(8) == 0 {
			ws[i].Observed.CapacityTPS = []float64{math.NaN(), math.Inf(1), -4}[rng.IntN(3)]
		}
		if !valid && rng.IntN(10) == 0 {
			ws[i].Observed.Flags = 0
		}
	}
}

func TestWorkAwareMatchesANaiveReferenceOnRandomViews(t *testing.T) {
	rng := rand.New(rand.NewPCG(21, 22))
	for _, st := range []string{LeastTokens, LeastWork} {
		for _, weights := range []cost.Weights{cost.DefaultWeights(), {InputWeight: 0.25, OutputFraction: 0.5}, {InputWeight: 0, OutputFraction: 1}} {
			s := mustNew(t, st, WithCostWeights(weights))
			for i := 0; i < 3000; i++ {
				ws := randomWorkers(rng)
				randomObserved(rng, ws, true)
				r := wreq(rng.IntN(8000), rng.IntN(4096))
				got, err := s.SelectWorker(context.Background(), r, ws)
				scores := refScores(st, weights, r, ws)
				if len(scores) == 0 {
					if err == nil {
						t.Fatalf("%s: nothing may serve, got %+v", st, got)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s: %v", st, err)
				}
				var min *big.Rat
				for _, v := range scores {
					if min == nil || v.Cmp(min) < 0 {
						min = v
					}
				}
				if !withinTie(scores[got.WorkerID], min) {
					t.Fatalf("%s %+v: picked %s with score %s, the minimum is %s\nview: %+v", st, weights, got.WorkerID, scores[got.WorkerID].FloatString(6), min.FloatString(6), ws)
				}
			}
		}
	}
}

// With garbage in the view nothing may panic or block, and the choice is still a worker that may serve (I1, I5).
func TestWorkAwareSurvivesGarbageSignals(t *testing.T) {
	rng := rand.New(rand.NewPCG(31, 32))
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		for i := 0; i < 5000; i++ {
			ws := randomWorkers(rng)
			randomObserved(rng, ws, false)
			got, err := s.SelectWorker(context.Background(), wreq(rng.IntN(1<<20), rng.IntN(1<<20)), ws)
			qualified := false
			for _, w := range ws {
				if mayServe(w) {
					qualified = true
				}
			}
			switch {
			case !qualified && err == nil:
				t.Fatalf("%s: nothing may serve but got %+v", st, got)
			case qualified && err != nil:
				t.Fatalf("%s: %v on %+v", st, err, ws)
			case err == nil && !mayServe(*got):
				t.Fatalf("%s: picked a worker that may not serve: %+v", st, *got)
			}
		}
	}
}

// I3 (monotonicity): raising one worker's work ahead never makes it more likely to be chosen. With a fresh
// scheduler per trial, "chosen after the raise" implies "chosen before it".
func TestRaisingWorkAheadNeverHelpsAWorker(t *testing.T) {
	rng := rand.New(rand.NewPCG(41, 42))
	for _, st := range []string{LeastTokens, LeastWork} {
		for i := 0; i < 3000; i++ {
			ws := randomWorkers(rng)
			randomObserved(rng, ws, true)
			if len(ws) == 0 {
				continue
			}
			k := rng.IntN(len(ws))
			r := wreq(rng.IntN(1000), rng.IntN(1000))
			after := append([]protocol.WorkerSnapshot(nil), ws...)
			after[k].Observed.WorkAheadTokens += float64(1 + rng.IntN(100000))
			// the same rotor state: both runs start fresh
			a, errA := mustNew(t, st).SelectWorker(context.Background(), r, ws)
			b, errB := mustNew(t, st).SelectWorker(context.Background(), r, after)
			if errA != nil || errB != nil {
				continue
			}
			if b.WorkerID == ws[k].WorkerID && a.WorkerID != ws[k].WorkerID {
				// Allowed only if the first run had k among its ties and the rotor picked another: not possible,
				// because raising k's score cannot add a tie for it.
				t.Fatalf("%s: raising %s's work ahead made it chosen (was %s)\nbefore: %+v", st, ws[k].WorkerID, a.WorkerID, ws)
			}
		}
	}
}

// I4: with equal scores the order is the rotor's: workers in ID order, wrapping, over any number of laps.
func TestEqualScoresFollowTheRotorOrder(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{ow("w3", 0, 70, 7), ow("w1", 2, 70, 7), ow("w2", 5, 70, 7), ow("w0", 0, 7000, 7)}
		want := []string{"w1", "w2", "w3"}
		for lap := 0; lap < 5; lap++ {
			for _, id := range want {
				if got := pick(t, s, ws); got != id {
					t.Fatalf("%s lap %d: got %s, want %s", st, lap, got, id)
				}
			}
		}
	}
}

// The rotor is per model, and the tie set may shrink or grow between calls without skipping a worker for good.
func TestTieRotationSurvivesMembershipChanges(t *testing.T) {
	s := mustNew(t, LeastTokens)
	ab := []protocol.WorkerSnapshot{ow("a", 0, 1, 1), ow("b", 0, 1, 1)}
	abc := []protocol.WorkerSnapshot{ow("a", 0, 1, 1), ow("b", 0, 1, 1), ow("c", 0, 1, 1)}
	var got []string
	got = append(got, pick(t, s, ab), pick(t, s, abc), pick(t, s, abc), pick(t, s, abc))
	if want := []string{"a", "b", "c", "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// Phase 6 style: 1000 synthetic requests through a loop that applies each choice to the view (work ahead grows by
// the request's cost) and completes requests after a fixed number of arrivals.
func TestDistributionOver1000Requests(t *testing.T) {
	type fleet struct {
		name  string
		rates []float64
	}
	for _, fl := range []fleet{{"identical", []float64{100, 100, 100, 100}}, {"heterogeneous", []float64{100, 60, 20, 100}}} {
		for _, st := range []string{LeastTokens, LeastWork} {
			s := mustNew(t, st)
			rng := rand.New(rand.NewPCG(51, 52))
			n := len(fl.rates)
			ahead := make([]float64, n)
			counts := make([]int, n)
			tokens := make([]float64, n)
			type job struct {
				w    int
				cost float64
			}
			var running []job
			for i := 0; i < 1000; i++ {
				ws := make([]protocol.WorkerSnapshot, n)
				for k := range ws {
					ws[k] = ow(string(rune('a'+k)), 0, ahead[k], fl.rates[k])
				}
				in, out := 50+rng.IntN(400), 50+rng.IntN(800)
				c := float64(in + out)
				w := selectFrom(t, s, wreq(in, out), ws)
				k := int(w.WorkerID[0] - 'a')
				counts[k]++
				tokens[k] += c
				ahead[k] += c
				running = append(running, job{k, c})
				if len(running) > 6 { // the oldest request finishes
					j := running[0]
					running = running[1:]
					ahead[j.w] -= j.cost
				}
			}
			total := 0
			for _, c := range counts {
				total += c
			}
			if total != 1000 {
				t.Fatalf("%s/%s: %v", fl.name, st, counts)
			}
			t.Logf("%s %s: requests %v tokens %.0f", fl.name, st, counts, tokens)
			switch {
			case fl.name == "identical":
				for k, c := range counts {
					if c < 200 || c > 300 {
						t.Errorf("%s/%s: worker %d got %d of 1000, want 200-300 (%v)", fl.name, st, k, c, counts)
					}
				}
			case st == LeastWork:
				// the 20 tokens/s worker must carry clearly less than a 100 tokens/s one
				if tokens[2] > 0.6*tokens[0] || tokens[2] > 0.6*tokens[3] {
					t.Errorf("%s/%s: the slow worker carried %.0f tokens against %.0f and %.0f", fl.name, st, tokens[2], tokens[0], tokens[3])
				}
			}
		}
	}
}

func TestWorkAwareIsSafeForConcurrentUse(t *testing.T) {
	ws := []protocol.WorkerSnapshot{ow("a", 0, 10, 5), ow("b", 1, 20, 5), ow("c", 0, 10, 5)}
	for _, st := range []string{LeastTokens, LeastWork} {
		s := mustNew(t, st)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if _, err := s.SelectWorker(context.Background(), wreq(10, 10), ws); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
	}
}

// I7: a selection allocates the candidate index, the tie rotation closure and the one copy of the chosen worker,
// not a copy per candidate and nothing that grows with the number of workers.
func TestWorkAwareAllocationsDoNotGrowWithTheFleet(t *testing.T) {
	for _, st := range []string{LeastTokens, LeastWork} {
		var perN []float64
		for _, n := range []int{8, 512} {
			ws := make([]protocol.WorkerSnapshot, n)
			for i := range ws {
				ws[i] = ow(string(rune(0x100+i)), 0, float64(i%7)*100, float64(10+i%5))
			}
			s := mustNew(t, st)
			r := wreq(100, 100)
			perN = append(perN, testing.AllocsPerRun(50, func() { _, _ = s.SelectWorker(context.Background(), r, ws) }))
		}
		t.Logf("%s: allocations per selection for 8 and 512 workers: %v", st, perN)
		if perN[1] > perN[0] || perN[1] > 6 {
			t.Errorf("%s: allocations per selection %v for 8 and 512 workers", st, perN)
		}
	}
}

func BenchmarkSelect(b *testing.B) {
	for _, st := range []string{LeastActive, LeastTokens, LeastWork} {
		for _, n := range []int{3, 50, 1000} {
			ws := make([]protocol.WorkerSnapshot, n)
			for i := range ws {
				ws[i] = ow(string(rune(0x100+i)), i%4, float64((i*37)%1000)*10, float64(50+i%9))
				ws[i].MaxConcurrency = 64
			}
			s, _ := New(st)
			r := wreq(300, 700)
			b.Run(st+"/"+itoa(n), func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := s.SelectWorker(context.Background(), r, ws); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

func itoa(n int) string { return big.NewInt(int64(n)).String() }

func TestPredictedWaitIsWorkAheadPlusCostOverCapacity(t *testing.T) {
	obs := func(ahead, rate float64, flags protocol.ObservedFlags) protocol.ObservedLoad {
		return protocol.ObservedLoad{WorkAheadTokens: ahead, CapacityTPS: rate, Flags: flags}
	}
	led := protocol.ObservedLedger
	for _, tc := range []struct {
		name string
		o    protocol.ObservedLoad
		cost float64
		want float64
		ok   bool
	}{
		{"hand-computed: (500 + 100) / 200", obs(500, 200, led), 100, 3, true},
		{"an idle worker still waits for the request itself", obs(0, 50, led), 100, 2, true},
		{"free request", obs(300, 100, led), 0, 3, true},
		{"no ledger observation: no prediction", obs(500, 200, 0), 100, 0, false},
		{"unknown capacity", obs(500, 0, led), 100, 0, false},
		{"NaN capacity", obs(500, math.NaN(), led), 100, 0, false},
		{"infinite capacity", obs(500, math.Inf(1), led), 100, 0, false},
		{"negative work ahead", obs(-1, 100, led), 10, 0, false},
		{"NaN work ahead", obs(math.NaN(), 100, led), 10, 0, false},
		{"infinite work ahead", obs(math.Inf(1), 100, led), 10, 0, false},
		{"negative cost", obs(1, 100, led), -10, 0, false},
		{"overflow", obs(1e308, 1e-300, led), 1, 0, false},
	} {
		got, ok := PredictedWait(tc.o, tc.cost)
		if ok != tc.ok || (ok && math.Abs(got-tc.want) > 1e-12) {
			t.Errorf("%s: got %v %v, want %v %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}
