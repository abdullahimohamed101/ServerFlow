package gateway

import (
	"net/http"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// The admission half of Phase 15 resilience (ADR-020, D11): the global request cap, the per-model FIFO queue that a
// request waits in when every eligible worker is full, and the shed responses. Every function returns before doing
// anything when Server.res is nil, so with the feature off nothing here is reached.

// retryAfterFor is the Retry-After of a shed: the queue's EWMA of recent waits for the queue reasons, one second for
// the global cap and for draining.
func (rs *resilienceState) retryAfterFor(reason string) int {
	return rs.waitq.RetryAfterSeconds(reason)
}

// acquireGlobal takes a place under admission.max_global_requests for the request, or answers 503 NO_CAPACITY
// (global_limit) and reports false. It runs before the body is read: a request over the cap is refused without
// spending anything on it (the connection is closed, as the unread body requires). The caller must Release the
// slot when the request ends, whichever way. The model is unknown here, so the shed is not attributed to one.
func (s *Server) acquireGlobal(w http.ResponseWriter, r *http.Request, info *reqInfo) (*resilience.Slot, bool) {
	slot, ok := s.res.admission.Acquire()
	if ok {
		return slot, true
	}
	w.Header().Set("Connection", "close")
	s.shed(w, r, info, api.OverloadGlobalLimit, api.ErrOverloaded(api.OverloadGlobalLimit, "", -1, s.res.retryAfterFor(api.OverloadGlobalLimit)))
	return nil, false
}

// shedBeforeLimiter is the cheap queue check that runs before the rate limiter (D11): a request that would have to
// join a queue that is full for it (depth reached, or its tenant's share used) is refused here, so the refusal
// spends none of the tenant's quota. It looks only at a queue that already has waiters: a request that finds
// capacity or an empty queue is not touched. It reports whether it answered.
func (s *Server) shedBeforeLimiter(w http.ResponseWriter, r *http.Request, info *reqInfo, ireq *protocol.InferenceRequest) bool {
	serves, ok := s.router.cache.Serves(ireq.Model)
	if !ok || !serves {
		return false // unknown or unconfirmed models never get a queue
	}
	q := s.res.waitq
	if !q.Busy(ireq.Model) {
		return false
	}
	reason := resilience.ShedReason(q.Refusal(ireq.Model, info.tenantID))
	if reason == "" {
		return false
	}
	info.model = ireq.Model       // confirmed: the registry serves it
	s.admitted(r.Context(), info) // no rate limit decision was made: the admission says so (as for an unknown model)
	s.shed(w, r, info, reason, api.ErrOverloaded(reason, ireq.Model, -1, s.res.retryAfterFor(reason)))
	return true
}

// shed answers a request refused by admission control: 503 NO_CAPACITY with Retry-After, the observers told
// (RejectOverload, which is admission and never a scheduling decision), one rate-limited log line. A client that
// has left is recorded as closed instead.
func (s *Server) shed(w http.ResponseWriter, r *http.Request, info *reqInfo, reason string, e *api.Error) {
	if r.Context().Err() != nil {
		info.clientClosed = true
		return
	}
	s.rejected(r.Context(), info, RejectOverload, reason, e.HTTPStatus)
	s.logShed(r, info, reason, e.RetryAfter)
	s.fail(w, info, e)
}

// reserveFirstAttempt is reserveAttempt for the first routing of a request. With the admission queue on, a request
// that finds every eligible worker full (or other requests already waiting) waits for a slot instead of being
// refused; everything else is exactly reserveAttempt.
func (s *Server) reserveFirstAttempt(r *http.Request, ireq *protocol.InferenceRequest, info *reqInfo) (*attemptRun, *routeError) {
	if s.res == nil {
		return s.reserveAttempt(r, ireq, info, nil)
	}
	selecting := time.Now()
	target, waiter, rerr := s.router.RouteOrWait(r.Context(), ireq, info.tenantID)
	info.selectStart, info.selectDuration = selecting, time.Since(selecting)
	s.res.flush()
	if waiter != nil {
		target, rerr = s.awaitSlot(r, ireq, info, waiter)
		selecting = time.Now() // the scheduling decision is the one after the wait; the wait has its own series
	}
	if rerr != nil {
		return nil, rerr
	}
	return &attemptRun{target: target, selecting: selecting}, nil
}

// awaitSlot waits in the model's queue until the request is handed a slot, routes again when woken, or gives up:
// max_wait passes (shed queue_timeout), the client leaves, or the gateway drains (shed draining). It fires
// QueueWaited exactly once, before anything that follows from the outcome. The waiter leaves the queue on every
// way out; a slot handed to it that the request no longer wants goes back through its release.
func (s *Server) awaitSlot(r *http.Request, ireq *protocol.InferenceRequest, info *reqInfo, w *resilience.Waiter) (*routed, *routeError) {
	rs := s.res
	q := rs.waitq
	info.model = ireq.Model // queued, so the registry confirmed it
	began := time.Now()
	timer := time.NewTimer(rs.maxWait)
	defer timer.Stop()

	finished := false
	defer func() {
		if finished {
			return
		}
		// A panic (or any exit not handled below): leave the queue, and give back a slot that arrived meanwhile.
		if g, ok := q.Leave(w); ok {
			if rt, _ := g.Value.(*routed); rt != nil {
				rt.release()
			}
		}
	}()
	finish := func(result string) {
		finished = true
		waited := time.Since(began)
		if result == QueueServed || result == QueueTimeout {
			q.ObserveWait(waited)
		}
		if result == QueueServed {
			info.queueWait = waited
		}
		s.obs.QueueWaited(r.Context(), QueueWait{RequestID: info.id, Model: ireq.Model, Waited: waited, Result: result})
	}
	shed := func(result, reason string) (*routed, *routeError) {
		finish(result)
		return nil, &routeError{shed: reason, Error: api.ErrOverloaded(reason, ireq.Model, -1, rs.retryAfterFor(reason))}
	}

	for {
		g, out := q.Wait(r.Context(), w, timer.C)
		switch out {
		case resilience.TimedOut:
			return shed(QueueTimeout, api.OverloadQueueTimeout)
		case resilience.ClientGone:
			finish(QueueClientGone)
			return nil, &routeError{Error: api.ErrInternal()} // the caller sees the cancelled context and records the client as gone
		}
		switch g.Kind {
		case resilience.Handoff:
			finish(QueueServed)
			return g.Value.(*routed), nil
		case resilience.Flushed:
			return shed(QueueDrained, api.OverloadDraining)
		}
		// Reroute: a registry refresh (or a slot released while the head was routing) says capacity may exist.
		for {
			rt, rerr := s.router.Route(r.Context(), ireq)
			if rerr == nil {
				q.Done(w)
				finish(QueueServed)
				return rt, nil
			}
			if !q.Park(w) {
				break // still nothing: keep waiting, in place
			}
		}
	}
}

// drainFlushMargin is how long before the shutdown timeout the queue is flushed, so that a request that cannot
// finish inside the budget is answered (503 draining) before its connection is cut.
const drainFlushMargin = 250 * time.Millisecond

// beginDrain starts the queue's part of a graceful shutdown (D16): arrivals stop being queued at once (they are
// served if a worker has room and shed as draining if they would have to wait), requests already waiting finish
// or time out as usual (max_wait bounds them), and whoever is still waiting when the shutdown budget is nearly
// spent is flushed with a draining shed. It returns the function that cancels the flush, which Serve defers.
func (rs *resilienceState) beginDrain(shutdownTimeout time.Duration) (stop func()) {
	rs.waitq.Close()
	t := time.AfterFunc(max(shutdownTimeout-drainFlushMargin, 0), func() { rs.waitq.Flush("") })
	return func() { t.Stop() }
}
