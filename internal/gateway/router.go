package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"

	"serverflow/internal/api"
	"serverflow/internal/resilience"
	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

// router picks a worker for each request: it reads the cached registry view,
// overlays what this gateway already has in flight, asks the scheduler, and
// reserves a slot on the chosen worker until the request finishes.
//
// Worker-reported metrics are at most one heartbeat old, so a burst would pile
// onto whichever worker looked idle at the last heartbeat. The overlay counters
// close that gap for this gateway. The effective load is the larger of the
// worker's own count and ours, not the sum: the worker's report already includes
// requests we sent earlier, and adding would count them twice. Other gateways
// are invisible to this one (Phase 8).
type router struct {
	cache    *snapshotCache
	policy   *AddressPolicy
	sched    scheduler.Scheduler
	strategy string
	client   *http.Client
	log      *slog.Logger

	// res is set when resilience is effective (Phase 15): the router then keeps workers whose circuit breaker is
	// open out of the selection and takes a breaker ticket for the worker it chooses.
	res *resilienceState

	mu       sync.Mutex
	inflight map[string]int // worker ID -> requests sent and not yet finished
}

func newRouter(cache *snapshotCache, policy *AddressPolicy, sched scheduler.Scheduler, strategy string, client *http.Client, log *slog.Logger) *router {
	return &router{cache: cache, policy: policy, sched: sched, strategy: strategy, client: client, log: log, inflight: map[string]int{}}
}

// routed is the outcome of a successful Route. The caller must call release
// exactly when it is done with the worker (more than once is harmless).
type routed struct {
	worker  protocol.WorkerSnapshot
	up      Upstream
	release func()
	// ticket is the circuit breaker's right to send this attempt (zero without resilience). A probe ticket must
	// come back through the breakers exactly once: with a verdict at the retry-decision point, or released by
	// the attempt's cleanup.
	ticket resilience.Ticket
}

// probe reports whether the attempt is a circuit breaker's trial request (AttemptStart.Probe).
func (t *routed) probe() bool { return t.ticket.Probe() }

// routeError is why Route found no worker: the API error to send, and what the scheduler counted when it
// refused (zero when the refusal did not come from the scheduler). The counts let callers tell "every eligible
// worker is full" (Eligible > 0 and Eligible == AtCapacity) from "none is eligible" without parsing messages.
// The embedded error keeps the callers that only need the answer simple.
type routeError struct {
	*api.Error
	Serving    int // workers in the view that serve the model
	Eligible   int // of those, the ones the registry offers
	AtCapacity int // of the eligible, the ones with no free slot
}

// Route chooses a worker for req, or returns why it could not. Workers named in exclude (the
// ones already tried for this request) are never chosen; if that leaves nothing, the answer is
// NO_CAPACITY, never a "model not found" for a model that is served.
func (r *router) Route(ctx context.Context, req *protocol.InferenceRequest, exclude ...string) (*routed, *routeError) {
	workers, ok := r.cache.View(req.Model)
	if !ok {
		// No snapshot, or one too old to trust: fail closed.
		return nil, &routeError{Error: api.ErrWorkerUnavailable()}
	}
	if len(exclude) > 0 {
		before := len(workers)
		kept := workers[:0] // View returned a private copy
		for _, w := range workers {
			if !slices.Contains(exclude, w.WorkerID) {
				kept = append(kept, w)
			}
		}
		if workers = kept; len(workers) == 0 && before > 0 {
			return nil, &routeError{Error: api.ErrNoCapacity(req.Model, 0), Serving: before}
		}
	}

	broke := 0 // registry-eligible workers the breakers took out of this selection
	if r.res != nil && r.res.nonClosed.Load() > 0 {
		for i := range workers {
			if schedulable(&workers[i]) && !r.res.breakers.Admissible(keyOf(&workers[i])) {
				workers[i].Eligible = false // a copy: the shared view is untouched, and eligible_workers stays truthful
				broke++
			}
		}
	}

	chosen, ticket, rerr := r.reserve(ctx, req, workers, broke)
	if rerr != nil {
		return nil, rerr
	}

	id := chosen.WorkerID
	var once sync.Once
	return &routed{
		ticket: ticket,
		worker: *chosen,
		up:     &httpUpstream{baseURL: trimSlash(chosen.Address), client: r.client},
		release: func() {
			once.Do(func() {
				r.mu.Lock()
				if r.inflight[id]--; r.inflight[id] <= 0 {
					delete(r.inflight, id)
				}
				r.mu.Unlock()
			})
		},
	}, nil
}

// reserve overlays local load, selects, takes the breaker ticket and takes the slot in one critical
// section, so two requests cannot both take a worker's last slot. The deferred
// unlock keeps a panicking scheduler from wedging every request. A worker whose breaker turns out to be
// unavailable after the scheduler chose it (another request took the last probe slot, or the breaker opened since
// the filter) is dropped from the selection and the scheduler chooses again; each candidate is tried at most once
// and no observer has been told anything yet. broke is how many registry-eligible workers the breakers already
// removed.
func (r *router) reserve(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot, broke int) (*protocol.WorkerSnapshot, resilience.Ticket, *routeError) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range workers {
		if local := r.inflight[workers[i].WorkerID]; local > workers[i].Metrics.ActiveRequests {
			workers[i].Metrics.ActiveRequests = local
		}
	}
	var chosen *protocol.WorkerSnapshot
	var ticket resilience.Ticket
	for {
		var err error
		chosen, err = r.sched.SelectWorker(ctx, req, workers)
		if err != nil {
			var none *scheduler.NoWorkerError
			if !errors.As(err, &none) {
				return nil, ticket, &routeError{Error: api.ErrInternal()} // the client went away, or the scheduler failed
			}
			counts := routeError{Serving: none.Serving, Eligible: none.Eligible, AtCapacity: none.AtCapacity}
			switch {
			case none.UnknownModel():
				counts.Error = api.ErrModelNotFound(req.Model)
			case r.cache.Degraded():
				// The registry cannot be reached and the cached view is aging out: that is
				// the cause, not a shortage of capacity, and a 1s retry hint would mislead.
				counts.Error = api.ErrWorkerUnavailable()
			default:
				counts.Error = api.ErrNoCapacity(req.Model, none.Eligible)
				if r.res != nil && broke > 0 && none.Eligible == 0 {
					// Every worker the registry offers is ejected: tell the client when the first one is tried again.
					counts.Error.RetryAfter = r.res.breakers.RetryAfter()
				}
			}
			return nil, ticket, &counts
		}
		if r.res == nil {
			break
		}
		t, ok := r.res.breakers.Acquire(keyOf(chosen))
		if ok {
			ticket = t
			break
		}
		for i := range workers {
			if workers[i].WorkerID == chosen.WorkerID {
				workers[i].Eligible = false
				break
			}
		}
		broke++
	}
	r.inflight[chosen.WorkerID]++
	return chosen, ticket, nil
}

// InFlight returns how many requests this gateway currently has on a worker.
func (r *router) InFlight(workerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inflight[workerID]
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

// Probe makes the router a readiness prober: ready means a snapshot within the
// staleness bound lists at least one model with an eligible worker.
func (r *router) Probe(context.Context) error {
	models, ok := r.cache.Models()
	if !ok {
		return errors.New("the worker registry snapshot is missing or too old")
	}
	if len(models) == 0 {
		return errors.New("no worker is eligible")
	}
	return nil
}
