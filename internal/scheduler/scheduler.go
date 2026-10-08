// Package scheduler chooses which worker serves a request (spec sections
// 13-15). Strategies do no I/O and read no clock, and their only state is a
// per-model round-robin position and a (seedable) random source, so they are
// deterministic under test. Every strategy sees only workers that passed the
// shared filter (Candidates is its copying form), which enforces the section
// 15 invariants in one place.
package scheduler

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"

	"serverflow/pkg/protocol"
)

// Strategy names, as used in scheduler.strategy.
const (
	Random      = "random"
	RoundRobin  = "round-robin"
	LeastActive = "least-active"
	LeastQueue  = "least-queue"
)

// Scheduler picks a worker for a request (spec section 13).
type Scheduler interface {
	// SelectWorker returns a copy of the chosen worker. It returns a
	// *NoWorkerError when no worker may serve the request, and never blocks.
	SelectWorker(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error)
}

// NoWorkerError explains why nothing was selected (spec section 15).
type NoWorkerError struct {
	Model string
	// Serving is how many of the given workers serve the model at all.
	Serving int
	// Eligible is how many of those the registry offers (READY and healthy),
	// before the concurrency check, so "all full" and "all dead" read differently.
	Eligible int
	// AtCapacity is how many of the eligible ones were full.
	AtCapacity int
}

func (e *NoWorkerError) Error() string {
	return fmt.Sprintf("no worker can serve the model: %d serving, %d eligible, %d at capacity", e.Serving, e.Eligible, e.AtCapacity)
}

// UnknownModel reports that no given worker serves the model, eligible or not.
func (e *NoWorkerError) UnknownModel() bool { return e.Serving == 0 }

// Candidates keeps the workers that may serve req: same model, READY, marked
// eligible by the registry, and below their concurrency limit. The result is
// a copy sorted by worker ID. When it is empty the *NoWorkerError says why.
func Candidates(req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) ([]protocol.WorkerSnapshot, *NoWorkerError) {
	idx, why := eligible(req, workers)
	if why != nil {
		return nil, why
	}
	out := make([]protocol.WorkerSnapshot, len(idx))
	for i, j := range idx {
		out[i] = workers[j]
	}
	return out, nil
}

// eligible is Candidates without the copying: it returns indices into workers,
// ordered by worker ID, so the strategies on the request path copy only the one
// worker they choose.
func eligible(req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) ([]int, *NoWorkerError) {
	why := &NoWorkerError{Model: req.Model}
	idx := make([]int, 0, len(workers))
	for i := range workers {
		w := &workers[i]
		if w.Model != req.Model {
			continue
		}
		why.Serving++
		if w.State != protocol.StateReady || !w.Eligible || (w.Health != protocol.HealthHealthy && w.Health != protocol.HealthSuspect) {
			continue
		}
		why.Eligible++
		if w.Metrics.ActiveRequests >= w.MaxConcurrency {
			why.AtCapacity++
			continue
		}
		idx = append(idx, i)
	}
	if len(idx) == 0 {
		return nil, why
	}
	// The gateway's snapshot is already in ID order, so this is normally one pass.
	if !sort.SliceIsSorted(idx, func(a, b int) bool { return workers[idx[a]].WorkerID < workers[idx[b]].WorkerID }) {
		sort.Slice(idx, func(a, b int) bool { return workers[idx[a]].WorkerID < workers[idx[b]].WorkerID })
	}
	return idx, nil
}

// Strategies lists the implemented strategy names.
func Strategies() []string { return []string{Random, RoundRobin, LeastActive, LeastQueue} }

// Option configures New.
type Option func(*options)

type options struct{ seed *[2]uint64 }

// WithSeed makes the random strategy reproducible.
func WithSeed(seed uint64) Option {
	return func(o *options) { o.seed = &[2]uint64{seed, seed ^ 0x9e3779b97f4a7c15} }
}

// New returns the scheduler for a strategy name.
func New(strategy string, opts ...Option) (Scheduler, error) {
	var o options
	for _, f := range opts {
		f(&o)
	}
	switch strategy {
	case Random:
		var rng *rand.Rand
		if o.seed != nil {
			rng = rand.New(rand.NewPCG(o.seed[0], o.seed[1]))
		} else {
			rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
		}
		return &randomScheduler{rng: rng}, nil
	case RoundRobin:
		return &roundRobin{rot: newRotor()}, nil
	case LeastActive:
		return &leastLoaded{rot: newRotor(), load: func(w protocol.WorkerSnapshot) int { return w.Metrics.ActiveRequests }}, nil
	case LeastQueue:
		return &leastLoaded{rot: newRotor(), load: func(w protocol.WorkerSnapshot) int { return w.Metrics.QueueDepth }}, nil
	default:
		return nil, fmt.Errorf("scheduler strategy %q is not implemented", strategy)
	}
}

// rotor remembers, per model, the last worker it handed out, so "next" is
// defined by worker ID and stays stable when workers come and go.
type rotor struct {
	mu   sync.Mutex
	last map[string]string
}

func newRotor() *rotor { return &rotor{last: map[string]string{}} }

// next returns the position, among n workers sorted by ID (id(i) is the ID of
// the i-th), of the first one after the one handed out last for the model,
// wrapping around, and records the choice.
func (r *rotor) next(model string, n int, id func(i int) string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := r.last[model]
	i := sort.Search(n, func(i int) bool { return id(i) > last })
	if i == n {
		i = 0
	}
	r.last[model] = id(i)
	return i
}

type randomScheduler struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func (s *randomScheduler) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, why := eligible(req, workers)
	if why != nil {
		return nil, why
	}
	s.mu.Lock()
	i := s.rng.IntN(len(idx))
	s.mu.Unlock()
	chosen := workers[idx[i]]
	return &chosen, nil
}

type roundRobin struct{ rot *rotor }

func (s *roundRobin) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, why := eligible(req, workers)
	if why != nil {
		return nil, why
	}
	chosen := workers[idx[s.rot.next(req.Model, len(idx), func(i int) string { return workers[idx[i]].WorkerID })]]
	return &chosen, nil
}

// leastLoaded picks the minimum of load; workers tied for the minimum take
// turns, so ties neither favor the lowest ID nor starve any worker.
type leastLoaded struct {
	rot  *rotor
	load func(protocol.WorkerSnapshot) int
}

func (s *leastLoaded) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, why := eligible(req, workers)
	if why != nil {
		return nil, why
	}
	min := s.load(workers[idx[0]])
	for _, j := range idx[1:] {
		if l := s.load(workers[j]); l < min {
			min = l
		}
	}
	tied := idx[:0:0]
	for _, j := range idx {
		if s.load(workers[j]) == min {
			tied = append(tied, j)
		}
	}
	chosen := workers[tied[s.rot.next(req.Model, len(tied), func(i int) string { return workers[tied[i]].WorkerID })]]
	return &chosen, nil
}
