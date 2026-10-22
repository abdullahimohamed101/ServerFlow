package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Phase 15 fault tolerance in the gateway (ADR-020). This file holds the state the request path consults when
// resilience.mode is "on" in registry mode: the per-worker circuit breakers, the retry budget, the first-byte
// deadline and the delivery of breaker transitions to the observers. With the feature off (or static workers)
// Server.res is nil and every call site returns before it touches anything here.

// Attempt classes added by the resilience features (see the class constants in retry.go).
const (
	// classDeadline marks an attempt cut because the first-byte deadline (retry.first_byte_deadline) elapsed
	// before any byte reached the client. It is not a retry reason and is never a breaker fault.
	classDeadline = "deadline"
)

// Breaker verdicts as the observers see them (AttemptEnd.Verdict).
const (
	VerdictOK      = "ok"
	VerdictFault   = "fault"
	VerdictNeutral = "neutral"
)

// peekLimit and defaultPeekWait bound the body peek of a held retryable 503 (D4): at most 1 KiB, at most 250 ms
// (and never longer than the time left for the request's first byte).
const (
	peekLimit       = 1 << 10
	defaultPeekWait = 250 * time.Millisecond
)

// maxPendingChanges bounds the breaker transitions waiting for delivery. A breaker changes state at most once per
// cool-down per worker, so the bound is far above anything real; beyond it the newest is dropped and counted.
const maxPendingChanges = 4096

// resilienceDeps lets tests inject the clock and the jitter source of the breakers and the budget, and shorten the
// peek. The zero value means the real clock, the global random source and 250 ms.
type resilienceDeps struct {
	now      resilience.Clock
	rand     resilience.Rand
	peekWait time.Duration
}

// resilienceState is the feature's runtime state. It exists only when the feature is effective.
type resilienceState struct {
	cfg      config.ResilienceConfig
	breakers *resilience.Breakers
	budget   *resilience.Budget
	// waitq holds the requests waiting for a worker slot (per model, strict FIFO with direct hand-off), admission
	// the global request cap and maxWait the longest a request may wait (Phase 15 PR E, D11).
	waitq     *resilience.Queue
	admission *resilience.Admission
	maxWait   time.Duration
	shedLog   shedLimiter
	now       func() time.Time
	log       *slog.Logger

	// firstByte is the deadline of the retryable phase, minRemaining the least time a retry may start with,
	// probeHeader the header timeout of a probe attempt (0 = the transport's own) and peekWait the peek bound.
	firstByte    time.Duration
	minRemaining time.Duration
	probeHeader  time.Duration
	peekWait     time.Duration

	// models maps a worker ID to its model for the transition events (Change carries only the key). It is
	// replaced whole on every sync.
	models atomic.Pointer[map[string]string]
	// nonClosed approximates the number of breakers that are not closed. While it is zero the router skips the
	// per-candidate Admissible pass; Acquire stays authoritative, so a stale zero costs one lost-ticket re-select.
	nonClosed atomic.Int64

	deliver func(BreakerChange) // set by the server: observers and log

	// grace is how long the gateway keeps serving after /readyz starts failing (D16) and draining is set at the
	// first shutdown signal: /readyz fails, responses carry Connection: close.
	grace    time.Duration
	draining atomic.Bool

	peeks atomic.Int64 // 503 bodies peeked so far (tests and diagnostics)

	qmu      sync.Mutex
	queue    []resilience.Change
	dropped  atomic.Uint64
	flushing atomic.Bool
}

// newResilienceState builds the state from the whole configuration (registry mode).
func newResilienceState(cfg config.Config, log *slog.Logger, d resilienceDeps) *resilienceState {
	g, r := cfg.Gateway, cfg.Resilience
	rs := &resilienceState{cfg: r, log: log, now: time.Now}
	rs.grace = r.Drain.EffectiveReadinessGrace(g.ShutdownTimeout)
	if d.now != nil {
		rs.now = d.now
	}
	rs.firstByte = r.Retry.EffectiveFirstByteDeadline(g.UpstreamHeaderTimeout, g.UpstreamIdleTimeout)
	if r.Retry.FirstByteDeadline == 0 {
		// The derived deadline must never cut an attempt the gateway serves today. The header timeout runs
		// after the request is written, so connecting (bounded by the dial timeout) comes on top of it.
		rs.firstByte += workerDialTimeout
	}
	rs.minRemaining = r.Retry.MinRemaining
	if ph := r.Breaker.EffectiveProbeHeaderTimeout(g.UpstreamHeaderTimeout); ph < g.UpstreamHeaderTimeout {
		rs.probeHeader = ph
	}
	rs.peekWait = defaultPeekWait
	if d.peekWait > 0 {
		rs.peekWait = d.peekWait
	}
	opts := resilience.Options{Now: d.now, Rand: d.rand, OnChange: rs.push}
	rs.breakers = resilience.NewBreakers(resilience.BreakerParamsFrom(cfg), opts)
	rs.budget = resilience.NewBudget(resilience.BudgetParamsFrom(cfg), d.now)
	qp := resilience.QueueParamsFrom(cfg)
	rs.maxWait = qp.MaxWait
	rs.waitq = resilience.NewQueue(qp, nil) // waits are measured in real time: a timer decides them
	rs.admission = resilience.AdmissionFrom(cfg)
	return rs
}

// push is the breakers' change hook. It runs after the breakers' lock is released but possibly while the router's
// lock is held (Acquire is called inside the routing critical section), so it only appends to a bounded queue:
// observers are told later by flush, outside every lock of the request path.
func (rs *resilienceState) push(c resilience.Change) {
	switch {
	case c.Suppressed:
	case c.From == resilience.Closed && c.To != resilience.Closed:
		rs.nonClosed.Add(1)
	case c.From != resilience.Closed && c.To == resilience.Closed:
		rs.nonClosed.Add(-1)
	}
	rs.qmu.Lock()
	if len(rs.queue) >= maxPendingChanges {
		rs.qmu.Unlock()
		rs.dropped.Add(1)
		return
	}
	rs.queue = append(rs.queue, c)
	rs.qmu.Unlock()
}

// flush delivers the queued breaker transitions to the observers, in order. Only one goroutine delivers at a time
// (the others return at once and the running one picks their transitions up), so events of a worker never
// overtake each other. Call it from request goroutines after a routing or a verdict, after a registry refresh and
// never with a router or breaker lock held.
func (rs *resilienceState) flush() {
	if rs == nil {
		return
	}
	for {
		if !rs.flushing.CompareAndSwap(false, true) {
			return
		}
		for {
			rs.qmu.Lock()
			batch := rs.queue
			rs.queue = nil
			rs.qmu.Unlock()
			if len(batch) == 0 {
				break
			}
			for _, c := range batch {
				rs.dispatch(c)
			}
		}
		rs.flushing.Store(false)
		if n := rs.dropped.Swap(0); n > 0 {
			rs.log.Warn("circuit breaker transitions were dropped because too many were waiting for delivery", "component", "gateway", "dropped", n)
		}
		rs.qmu.Lock()
		more := len(rs.queue) > 0
		rs.qmu.Unlock()
		if !more {
			return
		}
	}
}

func (rs *resilienceState) dispatch(c resilience.Change) {
	defer func() { _ = recover() }() // an observer is contained by the multi-observer; this guards the log call
	ev := BreakerChange{
		WorkerID: c.Key.ID, Model: rs.modelOf(c.Key.ID), To: c.To.String(), Reason: c.Reason, Failures: c.Failures, CoolDown: c.CoolDown,
	}
	if rs.deliver != nil {
		rs.deliver(ev)
	}
}

func (rs *resilienceState) modelOf(workerID string) string {
	if m := rs.models.Load(); m != nil {
		return (*m)[workerID]
	}
	return ""
}

// sync gives the breakers the registry view (every worker with its registration time and whether the registry
// would route to it now) and prunes what the registry no longer lists. It runs on every refresh and whenever
// aging turns an eligible worker suspect.
func (rs *resilienceState) sync(members []resilience.Member, models map[string]string) {
	rs.models.Store(&models)
	rs.breakers.Sync(members)
	rs.nonClosed.Store(int64(len(rs.breakers.Snapshot())))
	served := make(map[string]struct{}, len(models))
	for _, m := range models {
		served[m] = struct{}{}
	}
	rs.budget.Prune(func(model string) bool { _, ok := served[model]; return ok })
	rs.flush()
}

// keyOf is the breaker key of a snapshot worker.
func keyOf(w *protocol.WorkerSnapshot) resilience.Key {
	return resilience.KeyOf(w.WorkerID, w.RegisteredAt)
}

// schedulable mirrors the scheduler's own eligibility test, without the capacity check.
func schedulable(w *protocol.WorkerSnapshot) bool {
	return w.State == protocol.StateReady && w.Eligible && (w.Health == protocol.HealthHealthy || w.Health == protocol.HealthSuspect)
}

// firstByteDeadline is the instant after which the retryable phase of a request that began at start is over.
func (rs *resilienceState) firstByteDeadline(start time.Time) time.Time {
	return start.Add(rs.firstByte)
}

// enableResilience turns the feature on for a registry server. NewRegistry calls it when resilience.mode is "on";
// tests call it with injected dependencies. It must run before the server serves.
func (s *Server) enableResilience(cfg config.Config, d resilienceDeps) {
	rs := newResilienceState(cfg, s.log, d)
	rs.deliver = s.deliverBreakerChange
	s.res = rs
	s.router.res = rs
	s.router.cache.onSync = rs.sync
	s.router.cache.onRefreshed = s.router.wakeWaiters
	s.metrics.enableResilience(rs)
}

func (s *Server) deliverBreakerChange(ev BreakerChange) {
	s.obs.BreakerTransition(context.Background(), ev)
	if ev.Reason == resilience.ReasonSuppressed {
		s.log.Info("circuit breaker trip held back by the ejection cap", "component", "gateway", "worker_id", ev.WorkerID, "model", ev.Model, "failures", ev.Failures)
		return
	}
	level := slog.LevelInfo
	if ev.To == resilience.Open.String() {
		level = slog.LevelWarn
	}
	attrs := []any{"component", "gateway", "worker_id", ev.WorkerID, "model", ev.Model, "state", ev.To, "reason", ev.Reason}
	if ev.Failures > 0 {
		attrs = append(attrs, "failures", ev.Failures)
	}
	if ev.CoolDown > 0 {
		attrs = append(attrs, "cool_down_ms", ev.CoolDown.Milliseconds())
	}
	s.log.Log(context.Background(), level, "circuit breaker changed state", attrs...)
}

// shedLimiter rate-limits the log line of sheds to one per second per reason, counting what it held back (D14). A
// shed storm must not become a log storm; the series count every one.
type shedLimiter struct {
	mu         sync.Mutex
	last       map[string]time.Time
	suppressed map[string]int
}

// allow reports whether a shed of the reason may be logged now, and how many were held back since the last line.
func (l *shedLimiter) allow(reason string, now time.Time) (ok bool, suppressed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last == nil {
		l.last, l.suppressed = map[string]time.Time{}, map[string]int{}
	}
	if t, seen := l.last[reason]; seen && now.Sub(t) < time.Second && now.Sub(t) >= 0 {
		l.suppressed[reason]++
		return false, 0
	}
	l.last[reason] = now
	n := l.suppressed[reason]
	l.suppressed[reason] = 0
	return true, n
}

// logShed writes the shed line (INFO, at most one per second per reason). It names no body, prompt, key or worker text.
func (s *Server) logShed(r *http.Request, info *reqInfo, reason string, retryAfter int) {
	ok, held := s.res.shedLog.allow(reason, s.res.now())
	if !ok {
		return
	}
	attrs := []any{"component", "gateway", "request_id", info.id, "reason", reason, "retry_after_s", retryAfter}
	if info.model != "" {
		attrs = append(attrs, "model", info.model)
	}
	if held > 0 {
		attrs = append(attrs, "suppressed", held)
	}
	attrs = append(attrs, tenantAttrs(info)...)
	s.log.Log(r.Context(), slog.LevelInfo, "request shed", attrs...)
}
