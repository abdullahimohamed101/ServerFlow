// Package registry tracks inference workers: who they are, what model they
// serve, what state they report, and whether they are still alive. Health is
// derived from heartbeat age measured at receipt (never from worker clocks)
// and computed on read, so it needs no timers. The registry is in memory; a
// restarted control plane starts empty and workers re-register on their next
// heartbeat (ADR-010).
package registry

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/pkg/protocol"
)

// Errors returned by Registry. Validation failures wrap ErrInvalid.
var (
	ErrInvalid           = errors.New("invalid request")
	ErrUnknownWorker     = errors.New("unknown worker")
	ErrStaleRegistration = errors.New("stale registration")
	ErrIllegalTransition = errors.New("illegal state transition")
	ErrFull              = errors.New("worker limit reached")
)

// Config sets the thresholds that turn heartbeat age into health (spec
// section 12). A worker is healthy while its last heartbeat is younger than
// Suspect, suspect from Suspect up to and including Unhealthy, unhealthy
// beyond Unhealthy, and lost beyond Lost. A lost worker is evicted once
// Retention more has passed. MaxWorkers bounds memory.
type Config struct {
	Suspect    time.Duration
	Unhealthy  time.Duration
	Lost       time.Duration
	Retention  time.Duration
	MaxWorkers int
	// HeartbeatInterval is what workers are told to use.
	HeartbeatInterval time.Duration
	// SuspectEligible makes suspect workers eligible for routing. The default
	// is strict: only healthy READY workers are offered (spec section 63, rule 10).
	SuspectEligible bool
}

// Validate checks the thresholds are ordered and positive.
func (c Config) Validate() error {
	if c.Suspect <= 0 || c.Unhealthy <= 0 || c.Lost <= 0 || c.Retention <= 0 || c.HeartbeatInterval <= 0 {
		return errors.New("registry: all durations must be > 0")
	}
	if c.Suspect >= c.Unhealthy || c.Unhealthy >= c.Lost {
		return errors.New("registry: thresholds must satisfy suspect < unhealthy < lost")
	}
	if c.MaxWorkers < 1 {
		return errors.New("registry: max workers must be >= 1")
	}
	return nil
}

func (c Config) healthAt(age time.Duration) protocol.Health {
	switch {
	case age > c.Lost:
		return protocol.HealthLost
	case age > c.Unhealthy:
		return protocol.HealthUnhealthy
	case age >= c.Suspect:
		return protocol.HealthSuspect
	}
	return protocol.HealthHealthy
}

// Registry is safe for concurrent use.
type Registry struct {
	cfg Config
	log *slog.Logger
	now func() time.Time

	mu      sync.RWMutex
	workers map[string]*entry

	// Counters for the Prometheus collector (metrics.go); written outside the lock.
	registrations atomic.Int64
	heartbeats    [numHeartbeatResults]atomic.Int64
}

type entry struct {
	info         protocol.WorkerInfo
	regID        string
	state        protocol.WorkerState // as last reported by the worker
	metrics      protocol.Metrics
	reason       string
	registeredAt time.Time
	lastBeat     time.Time
	observed     protocol.WorkerState // effective state last logged, to log each change once
	drained      bool                 // has reported DRAINING: it can never serve again in this incarnation
}

// New returns a registry using the real clock.
func New(cfg Config, log *slog.Logger) (*Registry, error) {
	return NewWithClock(cfg, log, time.Now)
}

// NewWithClock is New with an injected clock, for tests and simulations.
func NewWithClock(cfg Config, log *slog.Logger, now func() time.Time) (*Registry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Registry{cfg: cfg, log: log.With("component", "registry"), now: now, workers: make(map[string]*entry)}, nil
}

// HeartbeatInterval is the interval workers are told to use.
func (r *Registry) HeartbeatInterval() time.Duration { return r.cfg.HeartbeatInterval }

func newRegistrationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("registry: crypto/rand unavailable: " + err.Error())
	}
	return "reg_" + hex.EncodeToString(b[:])
}

// Register adds a worker, or replaces an existing worker with the same ID: a
// restarted worker supersedes its previous incarnation, whose heartbeats are
// then rejected. It returns the registration ID the worker must present.
func (r *Registry) Register(info protocol.WorkerInfo) (string, error) {
	if err := info.Validate(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	regID, logs, err := r.register(info)
	r.flush(logs) // log after the lock is released
	if err == nil {
		r.registrations.Add(1)
	}
	return regID, err
}

func (r *Registry) register(info protocol.WorkerInfo) (string, []func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	var logs []func()

	old, exists := r.workers[info.WorkerID]
	if !exists && len(r.workers) >= r.cfg.MaxWorkers {
		logs = append(logs, r.evictLocked(now)...) // make room by dropping workers that are long gone
		if len(r.workers) >= r.cfg.MaxWorkers {
			return "", logs, ErrFull
		}
	}
	e := &entry{
		info: info, regID: newRegistrationID(), state: protocol.StateRegistering,
		registeredAt: now, lastBeat: now, observed: protocol.StateRegistering,
	}
	r.workers[info.WorkerID] = e
	if exists {
		prev := r.effective(old, now)
		logs = append(logs, func() {
			r.log.Info("worker re-registered, superseding the previous incarnation",
				"worker_id", info.WorkerID, "model", info.Model, "previous_state", prev)
		})
	} else {
		logs = append(logs, func() {
			r.log.Info("worker registered", "worker_id", info.WorkerID, "model", info.Model, "max_concurrency", info.MaxConcurrency)
		})
	}
	return e.regID, logs, nil
}

// Heartbeat records a worker's report. The state change must be legal and
// the registration ID current; a rejected heartbeat does not count as proof
// of life and changes nothing. A worker that had gone suspect, unhealthy, or
// lost and then reports in (a healed partition) resumes without re-registering.
// Once a worker has reported DRAINING it can only keep draining or fail: a
// drain is one-way, even through FAILED.
func (r *Registry) Heartbeat(id, registrationID string, hb protocol.Heartbeat) error {
	if err := hb.Validate(); err != nil {
		r.heartbeats[heartbeatInvalid].Add(1)
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	logs, err := r.heartbeat(id, registrationID, hb)
	r.flush(logs)
	r.heartbeats[heartbeatResult(err)].Add(1)
	return err
}

func (r *Registry) heartbeat(id, registrationID string, hb protocol.Heartbeat) ([]func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()

	e, ok := r.workers[id]
	if !ok {
		return nil, ErrUnknownWorker
	}
	if subtle.ConstantTimeCompare([]byte(e.regID), []byte(registrationID)) != 1 {
		return nil, ErrStaleRegistration
	}
	if !protocol.CanTransition(e.state, hb.State) ||
		(e.drained && hb.State != protocol.StateDraining && hb.State != protocol.StateFailed) {
		return nil, fmt.Errorf("%w: %s to %s", ErrIllegalTransition, e.state, hb.State)
	}
	e.state, e.metrics, e.reason, e.lastBeat = hb.State, hb.Metrics, hb.Reason, now
	if hb.State == protocol.StateDraining {
		e.drained = true
	}
	var logs []func()
	if tr, changed := r.observeLocked(id, e, now); changed {
		logs = append(logs, func() { r.logTransition(tr) })
	}
	return logs, nil
}

// Deregister removes a worker that is shutting down gracefully.
func (r *Registry) Deregister(id, registrationID string) error {
	r.mu.Lock()
	e, ok := r.workers[id]
	if !ok {
		r.mu.Unlock()
		return ErrUnknownWorker
	}
	if subtle.ConstantTimeCompare([]byte(e.regID), []byte(registrationID)) != 1 {
		r.mu.Unlock()
		return ErrStaleRegistration
	}
	delete(r.workers, id)
	r.mu.Unlock()
	r.log.Info("worker deregistered", "worker_id", id, "model", e.info.Model, "last_state", e.state)
	return nil
}

// flush runs deferred log calls; callers invoke it after releasing the lock so
// slow or re-entrant logging can never stall or deadlock the registry.
func (r *Registry) flush(logs []func()) {
	for _, f := range logs {
		f()
	}
}

// effective is the state clients see: what the worker reported, overridden by
// UNHEALTHY or LOST once its heartbeats stop.
func (r *Registry) effective(e *entry, now time.Time) protocol.WorkerState {
	switch r.cfg.healthAt(age(e, now)) {
	case protocol.HealthLost:
		return protocol.StateLost
	case protocol.HealthUnhealthy:
		return protocol.StateUnhealthy
	}
	return e.state
}

func age(e *entry, now time.Time) time.Duration {
	if a := now.Sub(e.lastBeat); a > 0 {
		return a
	}
	return 0
}

func (r *Registry) snapshot(e *entry, now time.Time) protocol.WorkerSnapshot {
	a := age(e, now)
	health := r.cfg.healthAt(a)
	state := r.effective(e, now)
	eligible := state == protocol.StateReady && (health == protocol.HealthHealthy || (health == protocol.HealthSuspect && r.cfg.SuspectEligible))
	return protocol.WorkerSnapshot{
		WorkerID: e.info.WorkerID, Model: e.info.Model, Address: e.info.Address,
		MaxConcurrency: e.info.MaxConcurrency, QueueSize: e.info.QueueSize,
		State: state, Health: health, Eligible: eligible, Metrics: e.metrics, Reason: e.reason,
		RegisteredAt: e.registeredAt, LastHeartbeat: e.lastBeat, HeartbeatAgeSeconds: a.Seconds(),
	}
}

// ListFilter selects workers. Zero fields match everything.
type ListFilter struct {
	Model        string
	State        protocol.WorkerState // the effective state
	EligibleOnly bool
}

// List returns consistent snapshots of the matching workers, sorted by ID.
func (r *Registry) List(f ListFilter) []protocol.WorkerSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	out := make([]protocol.WorkerSnapshot, 0, len(r.workers))
	for _, e := range r.workers {
		s := r.snapshot(e, now)
		if (f.Model != "" && s.Model != f.Model) || (f.State != "" && s.State != f.State) || (f.EligibleOnly && !s.Eligible) {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out
}

// Get returns one worker.
func (r *Registry) Get(id string) (protocol.WorkerSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.workers[id]
	if !ok {
		return protocol.WorkerSnapshot{}, false
	}
	return r.snapshot(e, r.now()), true
}

// Eligible returns the workers that may be routed to for model: READY and
// healthy (and suspect only if configured), never draining, failed,
// unhealthy, or lost (spec section 15).
func (r *Registry) Eligible(model string) []protocol.WorkerSnapshot {
	return r.List(ListFilter{Model: model, EligibleOnly: true})
}

// Models returns the model inventory, sorted by name.
func (r *Registry) Models() []protocol.ModelInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	counts := map[string]*protocol.ModelInfo{}
	for _, e := range r.workers {
		m := counts[e.info.Model]
		if m == nil {
			m = &protocol.ModelInfo{Model: e.info.Model}
			counts[e.info.Model] = m
		}
		m.Workers++
		if r.snapshot(e, now).Eligible {
			m.Eligible++
		}
	}
	out := make([]protocol.ModelInfo, 0, len(counts))
	for _, m := range counts {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// Len returns the number of registered workers.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.workers)
}

type transition struct {
	id, model string
	from, to  protocol.WorkerState
	age       time.Duration
	reason    string
}

// observeLocked records a change of a worker's effective state, once, and
// returns it so the caller can log it after releasing the lock.
func (r *Registry) observeLocked(id string, e *entry, now time.Time) (transition, bool) {
	cur := r.effective(e, now)
	if cur == e.observed {
		return transition{}, false
	}
	tr := transition{id: id, model: e.info.Model, from: e.observed, to: cur, age: age(e, now), reason: e.reason}
	e.observed = cur
	return tr, true
}

func (r *Registry) logTransition(t transition) {
	level := slog.LevelInfo
	if t.to == protocol.StateUnhealthy || t.to == protocol.StateLost || t.to == protocol.StateFailed {
		level = slog.LevelWarn
	}
	r.log.Log(context.Background(), level, "worker state changed", "worker_id", t.id, "model", t.model,
		"from", t.from, "to", t.to, "heartbeat_age_seconds", t.age.Seconds(), "reason", t.reason)
}

// Sweep notices workers whose heartbeats have stopped (logging each change of
// state once) and evicts workers lost for longer than the retention period.
// It returns how many were evicted. Reads never depend on Sweep: state is
// always computed from heartbeat age. Logging happens after the lock is
// released, so a large sweep never blocks heartbeats on log I/O.
func (r *Registry) Sweep() int {
	r.mu.Lock()
	now := r.now()
	var logs []func()
	for id, e := range r.workers {
		if tr, changed := r.observeLocked(id, e, now); changed {
			logs = append(logs, func() { r.logTransition(tr) })
		}
	}
	evictLogs := r.evictLocked(now)
	r.mu.Unlock()
	r.flush(logs)
	r.flush(evictLogs)
	return len(evictLogs)
}

// evictLocked removes workers lost for longer than the retention period and
// returns the log calls to make once the lock is released.
func (r *Registry) evictLocked(now time.Time) []func() {
	var logs []func()
	for id, e := range r.workers {
		if age(e, now) > r.cfg.Lost+r.cfg.Retention {
			delete(r.workers, id)
			id, model := id, e.info.Model
			logs = append(logs, func() { r.log.Warn("worker evicted after being lost", "worker_id", id, "model", model) })
		}
	}
	return logs
}
