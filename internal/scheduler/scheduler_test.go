package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"

	"serverflow/pkg/protocol"
)

const model = "qwen-7b"

func worker(id string, active, queue int) protocol.WorkerSnapshot {
	return protocol.WorkerSnapshot{
		WorkerID: id, Model: model, Address: "http://127.0.0.1:1", MaxConcurrency: 8, QueueSize: 16,
		State: protocol.StateReady, Health: protocol.HealthHealthy, Eligible: true,
		Metrics: protocol.Metrics{ActiveRequests: active, QueueDepth: queue},
	}
}

func req(m string) *protocol.InferenceRequest {
	return &protocol.InferenceRequest{RequestID: "r", Model: m}
}

func mustNew(t *testing.T, s string, o ...Option) Scheduler {
	t.Helper()
	sc, err := New(s, o...)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func pick(t *testing.T, s Scheduler, ws []protocol.WorkerSnapshot) string {
	t.Helper()
	w, err := s.SelectWorker(context.Background(), req(model), ws)
	if err != nil {
		t.Fatalf("SelectWorker: %v", err)
	}
	return w.WorkerID
}

var all = []string{Random, RoundRobin, LeastActive, LeastQueue}

func TestNewRejectsUnknownAndUnimplementedStrategies(t *testing.T) {
	for _, s := range []string{"", "magic", "least-work", "Random", "round_robin"} {
		if _, err := New(s); err == nil {
			t.Errorf("%q must be rejected", s)
		}
	}
	for _, s := range all {
		if _, err := New(s); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
}

func TestRoundRobinCyclesInIDOrderAndSurvivesMembershipChanges(t *testing.T) {
	s := mustNew(t, RoundRobin)
	ws := []protocol.WorkerSnapshot{worker("c", 0, 0), worker("a", 0, 0), worker("b", 0, 0)} // unsorted on purpose
	var got []string
	for i := 0; i < 7; i++ {
		got = append(got, pick(t, s, ws))
	}
	if want := []string{"a", "b", "c", "a", "b", "c", "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// "b" leaves while "a" was last: the next one is "c", then it wraps to "a".
	without := []protocol.WorkerSnapshot{worker("a", 0, 0), worker("c", 0, 0)}
	if a, b := pick(t, s, without), pick(t, s, without); a != "c" || b != "a" {
		t.Fatalf("after b left: %s %s, want c a", a, b)
	}
	// "b" returns while "a" was last: it is next in ID order, not skipped.
	if got := pick(t, s, ws); got != "b" {
		t.Fatalf("after b returned: %s, want b", got)
	}
}

func TestRoundRobinKeepsAnIndependentPositionPerModel(t *testing.T) {
	s := mustNew(t, RoundRobin)
	other := func(id string) protocol.WorkerSnapshot { w := worker(id, 0, 0); w.Model = "llama"; return w }
	ws := []protocol.WorkerSnapshot{worker("a", 0, 0), worker("b", 0, 0), other("x"), other("y")}
	if pick(t, s, ws) != "a" {
		t.Fatal("first pick for the model is the lowest ID")
	}
	w, _ := s.SelectWorker(context.Background(), req("llama"), ws)
	if w.WorkerID != "x" {
		t.Fatalf("llama starts at its own first worker, got %s", w.WorkerID)
	}
	if pick(t, s, ws) != "b" {
		t.Fatal("the other model must not move this model's position")
	}
}

func TestLeastActiveAndLeastQueuePickTheMinimum(t *testing.T) {
	ws := []protocol.WorkerSnapshot{worker("a", 5, 1), worker("b", 2, 9), worker("c", 4, 0)}
	if got := pick(t, mustNew(t, LeastActive), ws); got != "b" {
		t.Errorf("least-active picked %s, want b", got)
	}
	if got := pick(t, mustNew(t, LeastQueue), ws); got != "c" {
		t.Errorf("least-queue picked %s, want c", got)
	}
}

func TestTiesTakeTurnsAmongTheTiedOnly(t *testing.T) {
	for _, st := range []string{LeastActive, LeastQueue} {
		s := mustNew(t, st)
		ws := []protocol.WorkerSnapshot{worker("a", 1, 1), worker("b", 1, 1), worker("c", 7, 7), worker("d", 1, 1)}
		var got []string
		for i := 0; i < 6; i++ {
			got = append(got, pick(t, s, ws))
		}
		if want := []string{"a", "b", "d", "a", "b", "d"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v (c is busier and never chosen)", st, got, want)
		}
	}
}

func TestRandomIsReproducibleWithASeedAndCoversAllWorkers(t *testing.T) {
	ws := []protocol.WorkerSnapshot{worker("a", 0, 0), worker("b", 0, 0), worker("c", 0, 0)}
	seq := func(seed uint64) []string {
		s := mustNew(t, Random, WithSeed(seed))
		var out []string
		for i := 0; i < 50; i++ {
			out = append(out, pick(t, s, ws))
		}
		return out
	}
	if !reflect.DeepEqual(seq(42), seq(42)) {
		t.Fatal("the same seed must give the same sequence")
	}
	if reflect.DeepEqual(seq(42), seq(43)) {
		t.Fatal("different seeds should differ")
	}
	seen := map[string]bool{}
	for _, id := range seq(7) {
		seen[id] = true
	}
	if len(seen) != 3 {
		t.Fatalf("random never chose some worker in 50 picks: %v", seen)
	}
	// Unseeded schedulers work too.
	if _, err := mustNew(t, Random).SelectWorker(context.Background(), req(model), ws); err != nil {
		t.Fatal(err)
	}
}

func TestWhyNothingWasSelected(t *testing.T) {
	full := worker("full", 8, 0) // at MaxConcurrency
	draining := worker("drain", 0, 0)
	draining.State, draining.Eligible = protocol.StateDraining, false
	other := worker("other", 0, 0)
	other.Model = "llama"
	cases := []struct {
		name string
		ws   []protocol.WorkerSnapshot
		want NoWorkerError
		unk  bool
	}{
		{"no workers at all", nil, NoWorkerError{Model: model}, true},
		{"only another model", []protocol.WorkerSnapshot{other}, NoWorkerError{Model: model}, true},
		{"all draining", []protocol.WorkerSnapshot{draining}, NoWorkerError{Model: model, Serving: 1}, false},
		{"all full", []protocol.WorkerSnapshot{full, full}, NoWorkerError{Model: model, Serving: 2, Eligible: 2, AtCapacity: 2}, false},
		{"mixed", []protocol.WorkerSnapshot{full, draining, other}, NoWorkerError{Model: model, Serving: 2, Eligible: 1, AtCapacity: 1}, false},
	}
	for _, st := range all {
		s := mustNew(t, st)
		for _, c := range cases {
			w, err := s.SelectWorker(context.Background(), req(model), c.ws)
			var nw *NoWorkerError
			if w != nil || !errors.As(err, &nw) {
				t.Fatalf("%s/%s: got %v %v", st, c.name, w, err)
			}
			if *nw != c.want || nw.UnknownModel() != c.unk {
				t.Errorf("%s/%s: got %+v, want %+v", st, c.name, *nw, c.want)
			}
			if nw.Error() == "" {
				t.Error("an error must explain itself")
			}
		}
	}
}

func TestConcurrencyLimitBoundary(t *testing.T) {
	s := mustNew(t, LeastActive)
	if got := pick(t, s, []protocol.WorkerSnapshot{worker("a", 7, 0)}); got != "a" {
		t.Fatalf("one slot left is still selectable, got %s", got)
	}
	if _, err := s.SelectWorker(context.Background(), req(model), []protocol.WorkerSnapshot{worker("a", 8, 0)}); err == nil {
		t.Fatal("a worker exactly at MaxConcurrency must not be selected")
	}
	if _, err := s.SelectWorker(context.Background(), req(model), []protocol.WorkerSnapshot{worker("a", 9, 0)}); err == nil {
		t.Fatal("a worker over MaxConcurrency must not be selected")
	}
}

func TestACancelledContextIsHonoured(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, st := range all {
		w, err := mustNew(t, st).SelectWorker(ctx, req(model), []protocol.WorkerSnapshot{worker("a", 0, 0)})
		if w != nil || !errors.Is(err, context.Canceled) {
			t.Errorf("%s: got %v %v", st, w, err)
		}
	}
}

// --- invariants (spec section 15), checked against an independent oracle -------------------

func randomWorkers(rng *rand.Rand) []protocol.WorkerSnapshot {
	var ws []protocol.WorkerSnapshot
	states := []protocol.WorkerState{protocol.StateReady, protocol.StateReady, protocol.StateLoadingModel, protocol.StateWarming, protocol.StateDraining, protocol.StateFailed, protocol.StateUnhealthy, protocol.StateLost}
	for i := 0; i < rng.IntN(9); i++ {
		w := worker(fmt.Sprintf("w%02d", i), rng.IntN(10), rng.IntN(20))
		w.State = states[rng.IntN(len(states))]
		w.Eligible = rng.IntN(5) != 0
		w.Health = []protocol.Health{protocol.HealthHealthy, protocol.HealthHealthy, protocol.HealthSuspect, protocol.HealthUnhealthy, protocol.HealthLost}[rng.IntN(5)]
		if rng.IntN(4) == 0 {
			w.Model = "llama"
		}
		w.MaxConcurrency = 1 + rng.IntN(9)
		ws = append(ws, w)
	}
	rng.Shuffle(len(ws), func(i, j int) { ws[i], ws[j] = ws[j], ws[i] })
	return ws
}

func mayServe(w protocol.WorkerSnapshot) bool {
	healthy := w.Health == protocol.HealthHealthy || w.Health == protocol.HealthSuspect
	return w.Model == model && w.State == protocol.StateReady && w.Eligible && healthy && w.Metrics.ActiveRequests < w.MaxConcurrency
}

func TestSelectionInvariantsHoldForEveryStrategy(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, st := range all {
		s := mustNew(t, st, WithSeed(9))
		for i := 0; i < 2000; i++ {
			ws := randomWorkers(rng)
			before := append([]protocol.WorkerSnapshot(nil), ws...)
			qualified := map[string]bool{}
			for _, w := range ws {
				if mayServe(w) {
					qualified[w.WorkerID] = true
				}
			}
			got, err := s.SelectWorker(context.Background(), req(model), ws)
			if !reflect.DeepEqual(ws, before) {
				t.Fatalf("%s: the input was modified", st)
			}
			switch {
			case len(qualified) == 0 && (got != nil || err == nil):
				t.Fatalf("%s: nothing qualifies but got %v %v for %+v", st, got, err, ws)
			case len(qualified) > 0 && err != nil:
				t.Fatalf("%s: workers qualify but got error %v", st, err)
			case got != nil && !qualified[got.WorkerID]:
				t.Fatalf("%s: selected %+v, which may not serve", st, *got)
			}
		}
	}
}

func TestLeastStrategiesMatchABruteForceMinimum(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for st, load := range map[string]func(protocol.WorkerSnapshot) int{
		LeastActive: func(w protocol.WorkerSnapshot) int { return w.Metrics.ActiveRequests },
		LeastQueue:  func(w protocol.WorkerSnapshot) int { return w.Metrics.QueueDepth },
	} {
		s := mustNew(t, st)
		for i := 0; i < 2000; i++ {
			ws := randomWorkers(rng)
			min := -1
			for _, w := range ws {
				if mayServe(w) && (min < 0 || load(w) < min) {
					min = load(w)
				}
			}
			got, err := s.SelectWorker(context.Background(), req(model), ws)
			if err != nil {
				continue
			}
			if load(*got) != min {
				t.Fatalf("%s picked load %d, minimum is %d: %+v", st, load(*got), min, ws)
			}
		}
	}
}

func TestSchedulersAreSafeForConcurrentUse(t *testing.T) {
	ws := []protocol.WorkerSnapshot{worker("a", 0, 0), worker("b", 1, 1), worker("c", 0, 0)}
	for _, st := range all {
		s := mustNew(t, st)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if _, err := s.SelectWorker(context.Background(), req(model), ws); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
	}
}

func TestRoundRobinSpreadsEvenlyUnderConcurrency(t *testing.T) {
	s := mustNew(t, RoundRobin)
	ws := []protocol.WorkerSnapshot{worker("a", 0, 0), worker("b", 0, 0), worker("c", 0, 0)}
	var mu sync.Mutex
	counts := map[string]int{}
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				w, _ := s.SelectWorker(context.Background(), req(model), ws)
				mu.Lock()
				counts[w.WorkerID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	for id, n := range counts {
		if n != 600 {
			t.Errorf("%s got %d of 1800, want exactly 600", id, n)
		}
	}
}

func TestStrategiesListsExactlyWhatNewAccepts(t *testing.T) {
	if !reflect.DeepEqual(Strategies(), all) {
		t.Fatalf("got %v", Strategies())
	}
	for _, s := range Strategies() {
		if _, err := New(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
}

func TestTheChosenWorkerIsACopyNotAPointerIntoTheInput(t *testing.T) {
	for _, st := range all {
		ws := []protocol.WorkerSnapshot{worker("a", 0, 0), worker("b", 0, 0)}
		got, err := mustNew(t, st).SelectWorker(context.Background(), req(model), ws)
		if err != nil {
			t.Fatal(err)
		}
		got.Metrics.ActiveRequests = 99
		got.Address = "changed"
		for _, w := range ws {
			if w.Metrics.ActiveRequests == 99 || w.Address == "changed" {
				t.Fatalf("%s returned a pointer into the caller's slice", st)
			}
		}
	}
}

func TestAnUnhealthyWorkerIsNeverChosenEvenIfFlaggedEligible(t *testing.T) {
	for _, h := range []protocol.Health{protocol.HealthUnhealthy, protocol.HealthLost, ""} {
		w := worker("a", 0, 0)
		w.Health = h
		for _, st := range all {
			if _, err := mustNew(t, st).SelectWorker(context.Background(), req(model), []protocol.WorkerSnapshot{w}); err == nil {
				t.Errorf("%s chose a worker with health %q", st, h)
			}
		}
	}
	w := worker("s", 0, 0)
	w.Health = protocol.HealthSuspect // the registry may offer suspect workers when configured to
	if got := pick(t, mustNew(t, RoundRobin), []protocol.WorkerSnapshot{w}); got != "s" {
		t.Fatal("a suspect worker the registry marks eligible stays selectable")
	}
}
