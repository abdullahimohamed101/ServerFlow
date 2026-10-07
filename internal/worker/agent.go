package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"serverflow/internal/registry/client"
	"serverflow/pkg/protocol"
)

// ControlPlane is what the agent needs from the registry; *client.Client
// satisfies it.
type ControlPlane interface {
	Register(ctx context.Context, info protocol.WorkerInfo) (protocol.RegisterResponse, error)
	Heartbeat(ctx context.Context, workerID string, hb protocol.Heartbeat) error
	Deregister(ctx context.Context, workerID, registrationID string) error
}

// Config identifies the worker and sets the agent's cadence.
type Config struct {
	WorkerID string
	Model    string
	// AdvertiseURL is the address the gateway should use to reach the backend.
	AdvertiseURL string
	// Interval is the default heartbeat interval; the control plane's value,
	// returned at registration, takes over within the bounds below.
	Interval time.Duration
}

const (
	// maxBackoffIntervals caps how far registration retries back off, in
	// heartbeat intervals.
	maxBackoffIntervals = 10
	// The control plane may suggest a heartbeat interval, but a buggy or hostile
	// one must not turn the agent into a hot loop or silence it. Accepted
	// intervals are clamped to [max(minInterval, configured/10), 10 x configured].
	minInterval        = 10 * time.Millisecond
	maxIntervalFactor  = 10
	minIntervalDivisor = 10
	// minCallTimeout is the floor for the per-call timeout, which is half the
	// heartbeat interval, so one slow call cannot stretch a heartbeat past it.
	minCallTimeout = 250 * time.Millisecond
	// Limits a backend's report must respect to be believable.
	maxReportedConcurrency = protocol.MaxConcurrencyCap
	maxReportedQueueSize   = protocol.MaxQueueSizeCap
)

// ErrSuperseded is returned by Run when another process registered under this
// worker ID. Registering again would steal the identity back and start a fight
// that flaps the registry, so the agent stops and says so.
var ErrSuperseded = errors.New("worker superseded by another process using the same worker ID")

// Agent keeps one worker registered and heartbeating. It is driven by Step so
// its behavior can be tested without real time; Run just calls Step in a loop.
type Agent struct {
	cfg     Config
	backend Backend
	cp      ControlPlane
	log     *slog.Logger

	interval     time.Duration
	backoff      time.Duration
	registered   bool
	superseded   bool
	regID        string
	lastReported protocol.WorkerState
	backendDown  bool // to log reachability changes once, not on every tick
	cpFailures   int
	reRegistered bool // the previous step ended by dropping the registration
}

// New returns an agent. It is not safe for concurrent use.
func New(cfg Config, backend Backend, cp ControlPlane, log *slog.Logger) *Agent {
	return &Agent{cfg: cfg, backend: backend, cp: cp, interval: cfg.Interval, log: log.With("component", "worker-agent", "worker_id", cfg.WorkerID)}
}

// Registered reports whether the agent currently holds a registration.
func (a *Agent) Registered() bool { return a.registered }

// Superseded reports whether another process has taken over this worker ID.
func (a *Agent) Superseded() bool { return a.superseded }

// callTimeout bounds each backend probe and control plane call to half a
// heartbeat interval (with a floor), so a step cannot outlast its interval.
func (a *Agent) callTimeout() time.Duration { return max(a.interval/2, minCallTimeout) }

// Step runs one cycle: probe the backend, register if needed, send a heartbeat.
// It returns how long to wait before the next cycle.
func (a *Agent) Step(ctx context.Context) time.Duration {
	if a.superseded {
		return a.interval
	}
	pctx, cancel := context.WithTimeout(ctx, a.callTimeout())
	status, berr := a.backend.Status(pctx)
	cancel()
	reason, unusable := "backend unreachable", false
	if errors.Is(berr, errUnusableReport) {
		reason, unusable = "backend reported unusable data", true
	}
	if berr == nil {
		if verr := validateStatus(status); verr != nil {
			berr, reason, unusable = fmt.Errorf("backend reported unusable data: %w", verr), "backend reported unusable data", true
		}
	}
	a.noteBackend(berr)

	if !a.registered {
		if berr != nil {
			return a.retryDelay() // nothing to register until the backend answers sensibly
		}
		if !a.register(ctx, status) {
			return a.retryDelay()
		}
	}

	hb := protocol.Heartbeat{RegistrationID: a.regID, State: status.State, Metrics: status.Metrics}
	if berr != nil {
		if a.lastReported == protocol.StateDraining && !unusable {
			// The backend drained and exited: that is a graceful end, not a failure.
			a.deregister(ctx, "backend finished draining")
			return a.interval
		}
		hb.State, hb.Metrics, hb.Reason = protocol.StateFailed, protocol.Metrics{}, reason
	}

	hctx, hcancel := context.WithTimeout(ctx, a.callTimeout())
	err := a.cp.Heartbeat(hctx, a.cfg.WorkerID, hb)
	hcancel()
	switch {
	case err == nil:
		a.lastReported = hb.State
		a.cpFailures = 0
		a.reRegistered = false
	case client.IsStale(err):
		// Another process now owns this worker ID. Registering again would take
		// it back and make the two fight forever, so stop.
		a.log.Error("this worker ID is now registered by another process (superseded); stopping instead of taking it back", "error", err)
		a.registered, a.superseded = false, true
		return 0
	case client.IsUnknownWorker(err) || client.IsConflict(err):
		// The control plane lost us (it restarted) or refused a state change (the
		// backend restarted in place): start over with a fresh incarnation.
		a.log.Warn("registration no longer valid, registering again", "reason", errReason(err))
		a.registered = false
		if a.reRegistered {
			return a.interval // it happened twice in a row: do not spin
		}
		a.reRegistered = true
		return 0
	case client.IsInvalid(err):
		a.cpFailures++
		if a.cpFailures == 1 || a.cpFailures%10 == 0 {
			a.log.Error("the control plane rejected our heartbeat as invalid; this is a bug in the agent or the backend's report", "error", err, "consecutive_failures", a.cpFailures)
		}
	default:
		a.cpFailures++
		if a.cpFailures == 1 || a.cpFailures%10 == 0 {
			a.log.Warn("heartbeat failed", "error", err, "consecutive_failures", a.cpFailures)
		}
	}
	return a.interval
}

// validateStatus rejects a backend report the control plane could not accept,
// so the agent reports FAILED with a reason instead of retrying a heartbeat
// that can never succeed while the worker decays to LOST.
var errUnusableReport = errors.New("backend reported unusable data")

func validateStatus(s BackendStatus) error {
	if err := s.Metrics.Validate(); err != nil {
		return err
	}
	if s.MaxConcurrency > maxReportedConcurrency || s.QueueSize > maxReportedQueueSize {
		return errors.New("capacity is out of range")
	}
	return nil
}

func (a *Agent) register(ctx context.Context, status BackendStatus) bool {
	info := protocol.WorkerInfo{
		WorkerID: a.cfg.WorkerID, Model: a.cfg.Model, Address: a.cfg.AdvertiseURL,
		MaxConcurrency: max(status.MaxConcurrency, 1), QueueSize: max(status.QueueSize, 0),
	}
	rctx, cancel := context.WithTimeout(ctx, a.callTimeout())
	resp, err := a.cp.Register(rctx, info)
	cancel()
	if err != nil {
		a.cpFailures++
		switch {
		case client.IsUnauthorized(err):
			a.log.Error("the control plane rejected the token; fix control_plane.token", "error", err)
		case a.cpFailures == 1 || a.cpFailures%10 == 0:
			a.log.Warn("registration failed", "error", err, "consecutive_failures", a.cpFailures)
		}
		return false
	}
	a.registered, a.regID, a.lastReported, a.cpFailures = true, resp.RegistrationID, protocol.StateRegistering, 0
	a.backoff = 0
	a.adoptInterval(resp.HeartbeatIntervalSeconds)
	a.log.Info("registered with the control plane", "model", a.cfg.Model, "heartbeat_interval", a.interval.String())
	return true
}

// adoptInterval follows the control plane's suggested heartbeat interval,
// within bounds. An unusable value is ignored; an extreme one is clamped.
func (a *Agent) adoptInterval(secs float64) {
	lo := max(minInterval, a.cfg.Interval/minIntervalDivisor)
	hi := a.cfg.Interval * maxIntervalFactor
	switch {
	case math.IsNaN(secs) || secs <= 0:
		return
	case secs >= hi.Seconds():
		a.interval = hi
	case secs <= lo.Seconds():
		a.interval = lo
	default:
		a.interval = time.Duration(secs * float64(time.Second))
	}
	if a.interval != time.Duration(secs*float64(time.Second)) {
		a.log.Warn("the control plane's heartbeat interval is out of bounds; clamped", "requested_seconds", secs, "using", a.interval.String())
	}
}

func (a *Agent) deregister(ctx context.Context, why string) {
	dctx, cancel := context.WithTimeout(ctx, a.callTimeout())
	err := a.cp.Deregister(dctx, a.cfg.WorkerID, a.regID)
	cancel()
	if err != nil && !client.IsUnknownWorker(err) && !client.IsConflict(err) {
		a.log.Warn("deregistration failed", "error", err)
	} else {
		a.log.Info("deregistered", "why", why)
	}
	a.registered, a.regID = false, ""
}

// retryDelay grows from one interval up to maxBackoffIntervals intervals, so a
// down control plane or backend is not hammered.
func (a *Agent) retryDelay() time.Duration {
	if a.backoff < a.interval {
		a.backoff = a.interval
	} else {
		a.backoff = min(a.backoff*2, maxBackoffIntervals*a.interval)
	}
	return a.backoff
}

func (a *Agent) noteBackend(err error) {
	switch {
	case err != nil && !a.backendDown:
		a.backendDown = true
		a.log.Warn("backend unreachable or unusable", "error", err)
	case err == nil && a.backendDown:
		a.backendDown = false
		a.log.Info("backend reachable again")
	}
}

func errReason(err error) string {
	var e *client.Error
	if errors.As(err, &e) && e.Code != "" {
		return e.Code
	}
	return "conflict"
}

// Run steps until ctx is cancelled and returns nil, or until the agent is
// superseded and returns ErrSuperseded. Heartbeats are scheduled from the
// start of each step, so probe and network latency do not stretch the period
// beyond one interval. Run does not deregister: cancelling it simulates a
// crash; call Shutdown for a graceful exit.
func (a *Agent) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		started := time.Now()
		d := a.Step(ctx)
		if a.superseded {
			return ErrSuperseded
		}
		if d == a.interval {
			d -= time.Since(started) // steady cadence: the period is the interval, not interval plus work
		}
		if d <= 0 {
			continue
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
	return nil
}

// Shutdown ends the worker's membership gracefully: it reports DRAINING so the
// registry stops offering it, then deregisters. It is best effort and bounded
// by ctx. A superseded agent does nothing: the registration is no longer its own.
func (a *Agent) Shutdown(ctx context.Context) {
	if !a.registered {
		return
	}
	hb := protocol.Heartbeat{RegistrationID: a.regID, State: protocol.StateDraining, Reason: "agent shutting down"}
	if protocol.CanTransition(a.lastReported, protocol.StateDraining) {
		if err := a.cp.Heartbeat(ctx, a.cfg.WorkerID, hb); err != nil {
			a.log.Warn("could not report draining", "error", err)
		}
	}
	a.deregister(ctx, "agent shutting down")
}
