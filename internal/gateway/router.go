package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"serverflow/internal/api"
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
}

// Route chooses a worker for req, or returns the API error to send.
func (r *router) Route(ctx context.Context, req *protocol.InferenceRequest) (*routed, *api.Error) {
	workers, ok := r.cache.View(req.Model)
	if !ok {
		// No snapshot, or one too old to trust: fail closed.
		return nil, api.ErrWorkerUnavailable()
	}

	chosen, apiErr := r.reserve(ctx, req, workers)
	if apiErr != nil {
		return nil, apiErr
	}

	id := chosen.WorkerID
	var once sync.Once
	return &routed{
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

// reserve overlays local load, selects, and takes the slot in one critical
// section, so two requests cannot both take a worker's last slot. The deferred
// unlock keeps a panicking scheduler from wedging every request.
func (r *router) reserve(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, *api.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range workers {
		if local := r.inflight[workers[i].WorkerID]; local > workers[i].Metrics.ActiveRequests {
			workers[i].Metrics.ActiveRequests = local
		}
	}
	chosen, err := r.sched.SelectWorker(ctx, req, workers)
	if err != nil {
		var none *scheduler.NoWorkerError
		switch {
		case errors.As(err, &none) && none.UnknownModel():
			return nil, api.ErrModelNotFound(req.Model)
		case errors.As(err, &none) && r.cache.Degraded():
			// The registry cannot be reached and the cached view is aging out: that is
			// the cause, not a shortage of capacity, and a 1s retry hint would mislead.
			return nil, api.ErrWorkerUnavailable()
		case errors.As(err, &none):
			return nil, api.ErrNoCapacity(req.Model, none.Eligible)
		default:
			return nil, api.ErrInternal() // the client went away, or the scheduler failed
		}
	}
	r.inflight[chosen.WorkerID]++
	return chosen, nil
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
