package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/cost"
	"serverflow/internal/resilience"
	"serverflow/internal/scheduler"
	"serverflow/internal/scheduler/signals"
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

	// sig, when set, is the in-flight ledger and start/finish history of the opt-in "ledger" load overlay
	// (scheduler.load_overlay, ADR-022). nil is the default: the original max(reported, local) rule, and nothing
	// else of this file's ledger code runs. It is set once, before the router serves (enableLedger).
	sig       *signals.Tracker
	capWarned atomic.Bool // the tracked-worker cap was reported once

	// weights are the cost weights of the ledger (scheduler.cost.*). workAware is set when the strategy reads the
	// gateway's observed load (least-tokens, least-work): the router then fills Observed with the work-ahead and
	// capacity terms and feeds the tracker the capacity samples and the per-model mean cost. Both are set once,
	// before the router serves (enableWorkAware).
	weights   cost.Weights
	workAware bool

	mu       sync.Mutex
	inflight map[string]int // worker ID -> requests sent and not yet finished
}

func newRouter(cache *snapshotCache, policy *AddressPolicy, sched scheduler.Scheduler, strategy string, client *http.Client, log *slog.Logger) *router {
	return &router{cache: cache, policy: policy, sched: sched, strategy: strategy, client: client, log: log, inflight: map[string]int{}, weights: cost.DefaultWeights()}
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

	// lease is this attempt's entry in the signals ledger (zero without the ledger overlay). cost is the estimated
	// token cost the ledger holds for it: set at reserve, or by the waiter that receives a handed-off slot
	// (charge), because a hand-off is made under r.mu, which knows the model but not the request (D4, D14). release
	// subtracts exactly what was added.
	lease signals.Token
	cost  atomic.Int64

	// predicted is what the work-aware strategy predicted for this attempt when it chose the worker (seconds of
	// wait, valid when hasPrediction). Zero for hand-offs: the scheduler did not choose those.
	predicted     float64
	hasPrediction bool
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
	// shed, when set, says the request was refused by admission control rather than by the scheduler: one of
	// the resilience.Reason* values. The error is then the overload NO_CAPACITY (api.ErrOverloaded).
	shed string
}

// full reports that the model is served and every worker the registry offers is at its concurrency limit, the one
// refusal a request may wait out (D11).
func (e *routeError) full() bool {
	return e.shed == "" && e.Code == api.CodeNoCapacity && e.Eligible > 0 && e.Eligible == e.AtCapacity
}

// Route chooses a worker for req, or returns why it could not. Workers named in exclude (the
// ones already tried for this request) are never chosen; if that leaves nothing, the answer is
// NO_CAPACITY, never a "model not found" for a model that is served. Route never waits: a retry's routing
// and a woken waiter's use it.
func (r *router) Route(ctx context.Context, req *protocol.InferenceRequest, exclude ...string) (*routed, *routeError) {
	rt, _, rerr := r.route(ctx, req, nil, exclude)
	return rt, rerr
}

// RouteOrWait is Route for the first routing of a request when the admission queue is on. A request that finds
// every eligible worker full, or finds other requests already waiting for the model, is queued instead of refused
// and returned as a waiter; the queue refusing it (full, tenant share, draining) is a routeError with shed set.
// The tenant is the authenticated tenant ID ("" when authentication is off).
func (r *router) RouteOrWait(ctx context.Context, req *protocol.InferenceRequest, tenant string) (*routed, *resilience.Waiter, *routeError) {
	return r.route(ctx, req, &waitOpts{tenant: tenant}, nil)
}

// waitOpts says a routing may end in the admission queue.
type waitOpts struct{ tenant string }

func (r *router) route(ctx context.Context, req *protocol.InferenceRequest, wait *waitOpts, exclude []string) (*routed, *resilience.Waiter, *routeError) {
	workers, viewAt, ok := r.cache.viewAt(req.Model)
	if !ok {
		// No snapshot, or one too old to trust: fail closed.
		return nil, nil, &routeError{Error: api.ErrWorkerUnavailable()}
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
			return nil, nil, &routeError{Error: api.ErrNoCapacity(req.Model, 0), Serving: before}
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

	chosen, ticket, lease, waiter, rerr := r.reserve(ctx, req, workers, viewAt, broke, wait)
	if rerr != nil || waiter != nil {
		return nil, waiter, rerr
	}
	rt := r.newRouted(chosen, ticket, lease)
	rt.cost.Store(lease.cost)
	if r.workAware {
		rt.predicted, rt.hasPrediction = scheduler.PredictedWait(chosen.Observed, float64(r.requestCost(req)))
	}
	return rt, nil, nil
}

// newRouted wraps a worker whose slot (inflight count) and breaker ticket the caller already holds.
func (r *router) newRouted(w *protocol.WorkerSnapshot, ticket resilience.Ticket, lease ledgerLease) *routed {
	snap := *w
	var once sync.Once
	rt := &routed{
		ticket: ticket,
		worker: snap,
		lease:  lease.tok,
		up:     &httpUpstream{baseURL: trimSlash(snap.Address), client: r.client},
	}
	rt.release = func() {
		once.Do(func() { r.releaseSlot(&snap, rt) })
	}
	return rt
}

// ledgerLease is what reserve (or a hand-off) took from the ledger for one attempt.
type ledgerLease struct {
	tok  signals.Token
	cost int64
}

// requestCost is the estimated token work of a request for the ledger: the input and output estimates the handler
// computed once (D3), weighted by scheduler.cost.* (1 and 1 by default, which is their plain sum).
func (r *router) requestCost(req *protocol.InferenceRequest) int64 {
	return cost.Work(req.EstInputTokens, req.EstOutputTokens, r.weights)
}

// charge adds the cost of req to the ledger entry of a routed that was handed a slot by a release (cost 0 until
// now). The waiter that received it calls this once, outside r.mu; until then the ledger undercounts by one
// request, which cannot over-admit because admission counts slots, not tokens (D4, D14).
func (r *router) charge(rt *routed, req *protocol.InferenceRequest) {
	if r.sig == nil || rt == nil || rt.cost.Load() != 0 {
		return
	}
	if c := r.requestCost(req); r.sig.AddCost(rt.lease, c) {
		rt.cost.Store(c)
	}
}

// enableLedger switches the router to the ledger load overlay. It must be called before the router serves.
func (r *router) enableLedger(cfg signals.Config) {
	r.sig = signals.New(cfg)
	r.cache.onSnapshot = func(now time.Time, ws []protocol.WorkerSnapshot) {
		obs := make([]signals.Observation, len(ws))
		for i := range ws {
			obs[i] = signals.Observation{Key: signals.KeyOf(&ws[i]), LastHeartbeat: ws[i].LastHeartbeat}
			if r.workAware {
				w := &ws[i]
				obs[i].Model, obs[i].ReportedActive, obs[i].ReportedTPS = w.Model, w.Metrics.ActiveRequests, w.Metrics.RecentTokensPerSecond
				obs[i].Fresh = schedulable(w)
			}
		}
		r.sig.Sync(now, obs)
		if st := r.sig.Stats(); st.Untracked > 0 && !r.capWarned.Swap(true) {
			r.log.Warn("the load ledger tracks a limited number of workers; the others use the original max(reported, local) rule",
				"component", "gateway", "tracked", st.Tracked)
		}
	}
}

// enableWorkAware makes the router serve a strategy that reads the gateway's observed load: it takes the cost
// weights of the ledger and turns on the work-ahead and capacity terms of Observed. It requires the ledger
// (enableLedger first) and must be called before the router serves.
func (r *router) enableWorkAware(w cost.Weights) {
	r.weights = w
	r.workAware = true
}

// overlay is the effective load of a worker: the one place that combines what the worker reported with what this
// gateway has in flight, used by reserve (the scheduler's view) and by freeSlots (the queue's capacity count) so
// the two cannot disagree. now is the instant of the view w came from. Without the ledger it is the original
// max(reported, local).
func (r *router) overlay(w *protocol.WorkerSnapshot, now time.Time) (active int, res signals.Result, ledger bool) {
	local := r.inflight[w.WorkerID]
	if r.sig == nil {
		return max(w.Metrics.ActiveRequests, local), signals.Result{}, false
	}
	res = r.sig.Overlay(now, signals.Input{
		Key: signals.KeyOf(w), Local: local, Reported: w.Metrics.ActiveRequests,
		At: signals.ReportTime(now, w.HeartbeatAgeSeconds),
	})
	return res.Active, res, true
}

// releaseSlot gives a worker's slot back. If requests are waiting for the model, the slot goes straight to the
// head waiter instead (D11): the worker's inflight count stays as it is, the waiter receives a routed for the same
// worker without running the scheduler, so there is no window in which an arrival or a stale count can take it.
func (r *router) releaseSlot(w *protocol.WorkerSnapshot, rt *routed) {
	r.mu.Lock()
	if r.sig != nil {
		// This request's own entry leaves the ledger first, whatever happens to its slot next.
		r.sig.End(r.cache.now(), rt.lease, rt.cost.Load())
	}
	handed, wake := r.handoffLocked(w)
	if !handed {
		if r.inflight[w.WorkerID]--; r.inflight[w.WorkerID] <= 0 {
			delete(r.inflight, w.WorkerID)
		}
	}
	r.mu.Unlock()
	if wake {
		r.res.waitq.Wake(w.Model, 1) // the slot is free but could not be handed to the head: let a waiter route
	}
}

// handoffLocked tries to give the slot of w to the head waiter of w's model. It holds r.mu (the caller), so the slot
// is never visible as free in between. handed means the slot now belongs to the waiter; wake means the slot was
// freed normally although waiters exist (the worker is no longer eligible or its breaker refuses), so one waiter
// must be asked to route again after the lock is dropped.
func (r *router) handoffLocked(w *protocol.WorkerSnapshot) (handed, wake bool) {
	if r.res == nil || r.res.waitq == nil {
		return false, false
	}
	q := r.res.waitq
	if !q.Busy(w.Model) {
		return false, false
	}
	cur, ok := r.cache.worker(w.Model, w.WorkerID)
	if !ok || cur.RegisteredAt != w.RegisteredAt || !schedulable(&cur) {
		return false, true
	}
	ticket, ok := r.res.breakers.Acquire(keyOf(&cur))
	if !ok {
		return false, true
	}
	var lease ledgerLease
	if r.sig != nil {
		// The slot is the new holder's from here; its token is taken now, its cost by the waiter (charge). The
		// token is wasted if the library refuses the hand-off below, so it is ended at once with nothing to
		// subtract.
		lease.tok = r.sig.Begin(r.cache.now(), signals.KeyOf(&cur))
	}
	if !q.Handoff(w.Model, r.newRouted(&cur, ticket, lease)) {
		r.res.breakers.Release(ticket) // the head is routing after a wake-up; the library banked the slot as credit
		if r.sig != nil {
			r.sig.End(r.cache.now(), lease.tok, 0)
		}
		return false, false
	}
	return true, false
}

// wakeWaiters asks the waiters that the registry's corrected counts may now serve to route again. The gateway
// calls it after every successful refresh: up to as many head waiters as there are free slots in the new view,
// which is when a stale worker-reported count stops hiding capacity.
func (r *router) wakeWaiters() {
	if r.res == nil || r.res.waitq == nil {
		return
	}
	for _, model := range r.res.waitq.Models() {
		if n := r.freeSlots(model); n > 0 {
			r.res.waitq.Wake(model, n)
		}
	}
}

// freeSlots counts the slots a routing could take for the model right now: the workers the registry offers and the
// breakers admit, with the larger of the worker-reported and local load, as the router overlays it.
func (r *router) freeSlots(model string) int {
	workers, now, ok := r.cache.viewAt(model)
	if !ok {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	free := 0
	for i := range workers {
		w := &workers[i]
		if !schedulable(w) || (r.res != nil && !r.res.breakers.Admissible(keyOf(w))) {
			continue
		}
		active, _, _ := r.overlay(w, now)
		free += max(0, w.MaxConcurrency-active)
	}
	return free
}

// reserve overlays local load, selects, takes the breaker ticket and takes the slot in one critical
// section, so two requests cannot both take a worker's last slot. The deferred
// unlock keeps a panicking scheduler from wedging every request. A worker whose breaker turns out to be
// unavailable after the scheduler chose it (another request took the last probe slot, or the breaker opened since
// the filter) is dropped from the selection and the scheduler chooses again; each candidate is tried at most once
// and no observer has been told anything yet. broke is how many registry-eligible workers the breakers already
// removed.
//
// With wait set the same critical section decides to queue: a request that finds the model's queue busy joins it
// without routing (strict FIFO), and one that finds every eligible worker full joins it too. Both happen under
// r.mu, which is also what a release holds while it hands a slot to the head waiter, so a slot cannot be freed in
// the gap between "everything is full" and "I am waiting".
func (r *router) reserve(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot, viewAt time.Time, broke int, wait *waitOpts) (*protocol.WorkerSnapshot, resilience.Ticket, ledgerLease, *resilience.Waiter, *routeError) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if wait != nil && anySchedulable(workers) && r.res.waitq.Busy(req.Model) {
		// Others are waiting for this model and a worker could serve it: join the queue, behind them. (With nobody
		// eligible there is nothing to wait for and the scheduler answers as it always did.)
		w, rerr := r.enqueueLocked(req, wait, routeError{})
		return nil, resilience.Ticket{}, ledgerLease{}, w, rerr
	}
	if r.workAware && r.sig != nil {
		c := r.requestCost(req)
		for i := range workers {
			w := &workers[i]
			o := r.sig.Load(viewAt, signals.LoadInput{
				Input: signals.Input{
					Key: signals.KeyOf(w), Local: r.inflight[w.WorkerID], Reported: w.Metrics.ActiveRequests,
					At: signals.ReportTime(viewAt, w.HeartbeatAgeSeconds),
				},
				Model: w.Model, QueuedTokens: int64(w.Metrics.QueuedInputTokens), Cost: c, InputWeight: r.weights.InputWeight,
			})
			w.Metrics.ActiveRequests, w.Observed = o.EffectiveActive, o
		}
	} else {
		for i := range workers {
			active, res, ledger := r.overlay(&workers[i], viewAt)
			workers[i].Metrics.ActiveRequests = active
			if ledger {
				workers[i].Observed = res.Observed()
			}
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
				return nil, ticket, ledgerLease{}, nil, &routeError{Error: api.ErrInternal()} // the client went away, or the scheduler failed
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
					counts.RetryAfter = r.res.breakers.RetryAfter()
				}
				if wait != nil && counts.full() {
					w, rerr := r.enqueueLocked(req, wait, counts)
					return nil, ticket, ledgerLease{}, w, rerr
				}
			}
			return nil, ticket, ledgerLease{}, nil, &counts
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
	var lease ledgerLease
	if r.sig != nil {
		now := r.cache.now()
		lease.tok = r.sig.Begin(now, signals.KeyOf(chosen))
		if lease.cost = r.requestCost(req); !r.sig.AddCost(lease.tok, lease.cost) {
			lease.cost = 0
		}
		if r.workAware {
			r.sig.ObserveCost(chosen.Model, r.requestCost(req))
		}
	}
	return chosen, ticket, lease, nil, nil
}

// anySchedulable reports whether the registry (and the breakers, which mark workers ineligible in the copy) offer
// at least one worker, full or not.
func anySchedulable(workers []protocol.WorkerSnapshot) bool {
	for i := range workers {
		if schedulable(&workers[i]) {
			return true
		}
	}
	return false
}

// enqueueLocked puts the request in the model's queue (the caller holds r.mu). counts is what the scheduler found
// when it decided to wait (zero when the queue was already busy). A queue that cannot take the request sheds it
// (full, tenant share, draining); a model that has no queue (depth 0, or beyond the model cap) keeps the plain
// NO_CAPACITY refusal it always had.
func (r *router) enqueueLocked(req *protocol.InferenceRequest, wait *waitOpts, counts routeError) (*resilience.Waiter, *routeError) {
	q := r.res.waitq
	w, err := q.Enqueue(req.Model, wait.tenant)
	if err == nil {
		return w, nil
	}
	if errors.Is(err, resilience.ErrNoQueue) {
		counts.Error = api.ErrNoCapacity(req.Model, counts.Eligible)
		return nil, &counts
	}
	reason := resilience.ShedReason(err)
	counts.shed = reason
	eligible := counts.Eligible
	if eligible == 0 && counts.AtCapacity == 0 {
		eligible = -1 // the queue was already busy: the scheduler was not asked, so the count is unknown
	}
	counts.Error = api.ErrOverloaded(reason, req.Model, eligible, q.RetryAfterSeconds(reason))
	return nil, &counts
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
