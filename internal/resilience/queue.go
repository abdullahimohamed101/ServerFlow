package resilience

import (
	"container/list"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"serverflow/internal/config"
)

// Shedding reasons (rejection reasons of kind overload, D11/D13).
const (
	ReasonGlobalLimit  = "global_limit"
	ReasonQueueFull    = "queue_full"
	ReasonQueueTimeout = "queue_timeout"
	ReasonDraining     = "draining"
)

// Why an Enqueue was refused.
var (
	// ErrNoQueue: the model has no queue (depth 0, or more distinct queued models than MaxModels, so client text
	// can never create a queue). The request is refused at once.
	ErrNoQueue = errors.New("resilience: no queue for this model")
	// ErrQueueFull: the model's queue is at its maximum depth. Reason queue_full.
	ErrQueueFull = errors.New("resilience: queue full")
	// ErrTenantShare: the tenant already holds its share of the model's queue. Reason queue_full.
	ErrTenantShare = errors.New("resilience: tenant share of the queue exhausted")
	// ErrDraining: the queue stopped accepting waiters. Reason draining.
	ErrDraining = errors.New("resilience: queue closed for draining")
)

// ShedReason maps an Enqueue error to the rejection reason ("" for an error that is not a shed).
func ShedReason(err error) string {
	switch {
	case errors.Is(err, ErrQueueFull), errors.Is(err, ErrTenantShare), errors.Is(err, ErrNoQueue):
		return ReasonQueueFull
	case errors.Is(err, ErrDraining):
		return ReasonDraining
	}
	return ""
}

// QueueParams configure the admission queue.
type QueueParams struct {
	MaxModelDepth  int
	MaxWait        time.Duration
	MaxTenantShare float64
	// MaxModels bounds the models that may have waiters at once; 0 means unbounded.
	MaxModels int
}

// QueueParamsFrom takes the queue settings from the configuration (the model cap is metrics.max_models).
func QueueParamsFrom(c config.Config) QueueParams {
	q := c.Resilience.Queue
	deadline := c.Resilience.Retry.EffectiveFirstByteDeadline(c.Gateway.UpstreamHeaderTimeout, c.Gateway.UpstreamIdleTimeout)
	return QueueParams{MaxModelDepth: q.MaxModelDepth, MaxWait: q.EffectiveMaxWait(deadline), MaxTenantShare: q.MaxTenantShare, MaxModels: c.Metrics.MaxModels}
}

// tenantCap is the most waiters one tenant may hold in a queue of the given depth; 0 means no guard.
func (p QueueParams) tenantCap() int {
	if p.MaxTenantShare <= 0 || p.MaxTenantShare >= 1 {
		return 0
	}
	return max(1, int(math.Floor(p.MaxTenantShare*float64(p.MaxModelDepth)+1e-9)))
}

// GrantKind says why a waiter was signalled.
type GrantKind uint8

const (
	// Handoff delivers a slot: Grant.Value is what the releaser passed to Queue.Handoff. The waiter owns it and
	// has left the queue.
	Handoff GrantKind = iota + 1
	// Reroute asks the waiter to run the normal routing again (a registry refresh may have freed capacity). The
	// waiter stays in the queue, in place, until it calls Done (served) or Park (still full).
	Reroute
	// Flushed ends the wait without a slot (the gateway is draining). The waiter has left the queue.
	Flushed
)

// Grant is what a waiter receives.
type Grant struct {
	Kind  GrantKind
	Value any
}

// Waiter is one queued request. It is created by Queue.Enqueue and used by one goroutine.
type Waiter struct {
	model, tenant string
	enqueued      time.Time
	ch            chan Grant

	// Guarded by Queue.mu.
	elem  *list.Element
	woken bool // a Reroute is out: the waiter is routing and may not receive a hand-off
	left  bool // no longer in the queue (granted, flushed, removed)
}

// C delivers the waiter's grants. The channel is never closed.
func (w *Waiter) C() <-chan Grant { return w.ch }

// EnqueuedAt is the time Enqueue was called.
func (w *Waiter) EnqueuedAt() time.Time { return w.enqueued }

// Model returns the queue key.
func (w *Waiter) Model() string { return w.model }

type modelQueue struct {
	waiters *list.List
	tenants map[string]int
	// credit counts slots that were released while the head waiter was routing and so could not be handed over
	// (the caller freed them normally). Invariant: credit > 0 only while some waiter is routing and every waiter
	// ahead of the first idle one is routing; credit is spent on the first idle waiter in queue order the moment
	// one exists to take it (settle), never on a waiter that arrived later while an earlier one is still routing.
	credit int
}

// Queue is the set of per-model FIFO queues. Waiting requests do not poll: a freed slot is handed directly to
// the head waiter (Handoff), so the stale-count race of "wake and re-route" cannot lose it, and an arrival that
// finds waiters must enqueue (Busy), so nobody jumps the queue. Lock order for a caller that holds its own lock
// around Handoff (the router's mutex): the caller's lock first, then the queue's; the queue never calls out.
type Queue struct {
	p   QueueParams
	now Clock

	mu      sync.Mutex
	models  map[string]*modelQueue
	closed  bool
	ewma    float64 // seconds
	ewmaSet bool
}

// NewQueue creates an empty queue set.
func NewQueue(p QueueParams, now Clock) *Queue {
	return &Queue{p: p, now: now.orNow(), models: map[string]*modelQueue{}}
}

// Busy reports whether the model has waiters. An arrival for a busy model must Enqueue instead of routing:
// that is what makes the queue strictly FIFO.
func (q *Queue) Busy(model string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	mq := q.models[model]
	return mq != nil && mq.waiters.Len() > 0
}

// Depth returns the number of waiters of the model.
func (q *Queue) Depth(model string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if mq := q.models[model]; mq != nil {
		return mq.waiters.Len()
	}
	return 0
}

// Depths returns the depth of every model that has waiters (for the depth gauge).
func (q *Queue) Depths() map[string]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]int, len(q.models))
	for m, mq := range q.models {
		out[m] = mq.waiters.Len()
	}
	return out
}

// Full reports whether an Enqueue for the model and tenant would be refused for depth or tenant share (the cheap
// pre-limiter check). Models that could not get a queue are also "full". tenant "" is never share-capped.
func (q *Queue) Full(model, tenant string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.refuseLocked(model, tenant) != nil
}

func (q *Queue) refuseLocked(model, tenant string) error {
	if q.closed {
		return ErrDraining
	}
	if q.p.MaxModelDepth <= 0 {
		return ErrNoQueue
	}
	mq := q.models[model]
	if mq == nil {
		if q.p.MaxModels > 0 && len(q.models) >= q.p.MaxModels {
			return ErrNoQueue
		}
		return nil
	}
	if mq.waiters.Len() >= q.p.MaxModelDepth {
		return ErrQueueFull
	}
	if c := q.p.tenantCap(); c > 0 && tenant != "" && mq.tenants[tenant] >= c {
		return ErrTenantShare
	}
	return nil
}

// Enqueue puts a request at the tail of the model's queue, or refuses it (ErrNoQueue, ErrQueueFull,
// ErrTenantShare, ErrDraining). The caller must have used a registry-confirmed model name, never client text.
func (q *Queue) Enqueue(model, tenant string) (*Waiter, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.refuseLocked(model, tenant); err != nil {
		return nil, err
	}
	mq := q.models[model]
	if mq == nil {
		mq = &modelQueue{waiters: list.New(), tenants: map[string]int{}}
		q.models[model] = mq
	}
	w := &Waiter{model: model, tenant: tenant, enqueued: q.now(), ch: make(chan Grant, 1)}
	w.elem = mq.waiters.PushBack(w)
	if tenant != "" {
		mq.tenants[tenant]++
	}
	return w, nil
}

// leaveLocked removes a waiter from its queue's bookkeeping. With settleAfter, credit for slots that were released
// while waiters were routing is spent on the waiters that remain (see settle), so a freed slot is not left unused
// while waiters exist.
func (q *Queue) leaveLocked(w *Waiter, settleAfter, giveBack bool) {
	mq := q.models[w.model]
	wasRouting := w.woken
	w.left = true
	w.woken = false
	if mq == nil || w.elem == nil {
		return
	}
	mq.waiters.Remove(w.elem)
	w.elem = nil
	if w.tenant != "" {
		if mq.tenants[w.tenant]--; mq.tenants[w.tenant] <= 0 {
			delete(mq.tenants, w.tenant)
		}
	}
	if mq.waiters.Len() == 0 {
		delete(q.models, w.model)
		return
	}
	if settleAfter {
		if giveBack && wasRouting {
			mq.credit++ // it was woken for a slot and never used it (timeout, cancel): somebody else may
		}
		settle(mq, nil)
	}
}

// settle spends credit: for each unit, the first waiter (in queue order) that is not routing is told to route
// (Reroute), or, if it is self, reported through the return value instead of through its channel. It stops when
// credit is gone or every waiter is routing. A unit may find nothing to take (the waiter that was routing took the
// freed slot itself); the cost is one routing attempt that fails, and each unit is spent once, so nothing loops.
func settle(mq *modelQueue, self *Waiter) (selfRoutes bool) {
	for mq.credit > 0 {
		var next *Waiter
		for e := mq.waiters.Front(); e != nil; e = e.Next() {
			if w := e.Value.(*Waiter); !w.woken {
				next = w
				break
			}
		}
		if next == nil {
			return selfRoutes
		}
		mq.credit--
		next.woken = true
		if next == self {
			selfRoutes = true
		} else {
			next.send(Grant{Kind: Reroute})
		}
	}
	return selfRoutes
}

// send delivers a grant on the buffered channel. At most one grant is ever pending: a pending Reroute that the
// waiter has not read yet is replaced.
func (w *Waiter) send(g Grant) {
	select {
	case <-w.ch:
	default:
	}
	w.ch <- g
}

// Handoff gives value to the head waiter of the model, which leaves the queue and receives
// Grant{Handoff, value}. It returns false, and the caller must release the slot normally, when the model has no
// waiter, or when the head is routing after a Reroute (it will take the free slot itself; it is told to route
// once more in case it already looked). Call it under the caller's own lock if the slot must not be visible to
// anybody else in between.
func (q *Queue) Handoff(model string, value any) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	mq := q.models[model]
	if mq == nil || mq.waiters.Len() == 0 {
		return false
	}
	head := mq.waiters.Front().Value.(*Waiter)
	if head.woken {
		mq.credit++
		settle(mq, nil)
		return false
	}
	q.leaveLocked(head, false, false)
	head.send(Grant{Kind: Handoff, Value: value})
	return true
}

// Wake asks up to n waiters at the head of the model's queue that are not already routing to route again
// (Reroute), and returns how many it asked. The gateway calls it on a registry refresh with the number of free
// slots, which is when stale counts correct themselves.
func (q *Queue) Wake(model string, n int) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	mq := q.models[model]
	if mq == nil || n <= 0 {
		return 0
	}
	woken := 0
	for e := mq.waiters.Front(); e != nil && woken < n; e = e.Next() {
		w := e.Value.(*Waiter)
		if w.woken {
			continue
		}
		w.woken = true
		w.send(Grant{Kind: Reroute})
		woken++
	}
	return woken
}

// Models lists the models that have waiters (to wake them on a refresh).
func (q *Queue) Models() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.models))
	for m := range q.models {
		out = append(out, m)
	}
	return out
}

// Park is called by a waiter that was woken, routed, and found every worker still full. It keeps its place and
// goes back to waiting. It returns true when a slot was released while waiters were routing and nobody else was free
// to take it (a missed hand-off): the waiter must then route again before it waits. Otherwise credit goes to the
// first waiter that is not routing.
func (q *Queue) Park(w *Waiter) (routeAgain bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if w.left {
		return false
	}
	w.woken = false
	mq := q.models[w.model]
	if mq == nil {
		return false
	}
	return settle(mq, w)
}

// Done removes a woken waiter that was served by its own routing. Unlike Leave it does not give its wake-up back:
// it used the slot it was woken for.
func (q *Queue) Done(w *Waiter) { q.leave(w, false) }

// Leave removes the waiter (cancelled, timed out, or served). If a hand-off had already been delivered to it, the
// grant is returned with true and the caller owns the slot in it (it must use or release it); the waiter is
// never lost and a slot never dropped. It is idempotent.
func (q *Queue) Leave(w *Waiter) (Grant, bool) { return q.leave(w, true) }

func (q *Queue) leave(w *Waiter, giveBack bool) (Grant, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !w.left {
		q.leaveLocked(w, true, giveBack)
		select { // a pending Reroute is moot
		case g := <-w.ch:
			if g.Kind != Reroute {
				return g, true
			}
		default:
		}
		return Grant{}, false
	}
	select {
	case g := <-w.ch:
		if g.Kind == Handoff {
			return g, true
		}
	default:
	}
	return Grant{}, false
}

// Outcome is how a Wait ended.
type Outcome uint8

const (
	// Granted: Grant is valid (a hand-off, a Reroute to act on, or a flush).
	Granted Outcome = iota
	// TimedOut: MaxWait passed. The waiter has left the queue.
	TimedOut
	// ClientGone: the context ended. The waiter has left the queue.
	ClientGone
)

// Wait blocks until the waiter is granted something, the context ends, or timeout fires. On timeout or
// cancellation it removes the waiter; if a hand-off raced in, it is returned as Granted so that no slot is lost.
// timeout is typically time.After(MaxWait) or a fake clock's channel.
func (q *Queue) Wait(ctx context.Context, w *Waiter, timeout <-chan time.Time) (Grant, Outcome) {
	select {
	case g := <-w.ch:
		return g, Granted
	case <-ctx.Done():
		if g, ok := q.Leave(w); ok {
			return g, Granted
		}
		return Grant{}, ClientGone
	case <-timeout:
		if g, ok := q.Leave(w); ok {
			return g, Granted
		}
		return Grant{}, TimedOut
	}
}

// Flush removes every waiter of the model (all models if model is "") and tells each one Flushed. It returns the
// number flushed. Used when the gateway cannot finish its waiters before the shutdown budget.
func (q *Queue) Flush(model string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for m, mq := range q.models {
		if model != "" && m != model {
			continue
		}
		for mq.waiters.Len() > 0 {
			w := mq.waiters.Front().Value.(*Waiter)
			q.leaveLocked(w, false, false)
			w.send(Grant{Kind: Flushed})
			n++
		}
	}
	return n
}

// Close makes Enqueue fail with ErrDraining. Existing waiters are untouched (they finish or time out; Flush
// removes them).
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
}

// ObserveWait feeds the EWMA behind Retry-After with the wait of a request that was served or timed out.
func (q *Queue) ObserveWait(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	const alpha = 0.2
	s := d.Seconds()
	if !q.ewmaSet {
		q.ewma, q.ewmaSet = s, true
		return
	}
	q.ewma = alpha*s + (1-alpha)*q.ewma
}

// RetryAfter returns the Retry-After seconds for a queue_full or queue_timeout shed: the EWMA of recent waits
// rounded up and clamped to 1-10, or 1 with no data.
func (q *Queue) RetryAfter() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.ewmaSet {
		return 1
	}
	return min(max(int(math.Ceil(q.ewma-1e-9)), 1), 10)
}

// RetryAfterSeconds returns the Retry-After for a shed reason: the queue's EWMA for the queue reasons, 1 s for
// global_limit and draining.
func (q *Queue) RetryAfterSeconds(reason string) int {
	switch reason {
	case ReasonQueueFull, ReasonQueueTimeout:
		return q.RetryAfter()
	}
	return 1
}
