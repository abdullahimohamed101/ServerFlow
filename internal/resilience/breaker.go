package resilience

import (
	"time"

	"serverflow/internal/config"
)

// State is a breaker's state.
type State uint8

// Breaker states. The numbers are the values of the planned circuit_breaker_state gauge (a closed breaker has no
// series).
const (
	Closed   State = 0
	HalfOpen State = 1
	Open     State = 2
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half_open"
	case Open:
		return "open"
	}
	return "unknown"
}

// Verdict is how one attempt counts for its worker's breaker, taken at the retry-decision point (D7).
type Verdict uint8

const (
	// Neutral says nothing about the worker: load or lifecycle (queue_full, draining, not_ready), a client that
	// left, an expired request deadline, a panic, a relayed 4xx. A probe ticket is returned.
	Neutral Verdict = iota
	// OK is a first byte (or a 2xx complete).
	OK
	// Fault feeds both trip rules: connect error, reset before headers, an empty first chunk, 502, 503 with an
	// unknown code, header timeout, stall, 504, a stream aborted after the first byte.
	Fault
	// RatioFault feeds the ratio rule only: an unlisted 5xx (for example 500). It never extends or breaks a
	// consecutive streak.
	RatioFault
)

// Reasons carried by Change.Reason.
const (
	ReasonConsecutive  = "consecutive_failures"
	ReasonRatio        = "failure_ratio"
	ReasonCoolDown     = "cool_down_elapsed"
	ReasonProbeFailed  = "probe_failed"
	ReasonProbesPassed = "probes_passed"
	ReasonSuppressed   = "suppressed"
	// ReasonCapRelaxed: the ejection cap shrank (a worker became ineligible or eligible again) and this breaker,
	// one of the most recently ejected, was closed so that open + half-open <= the cap again.
	ReasonCapRelaxed = "ejection_cap"
)

// BreakerParams are the breaker settings. See config.ResilienceBreakerConfig for the meaning of each.
type BreakerParams struct {
	ConsecutiveFailures int
	FailureRatio        float64 // 0 switches the ratio rule off
	MinVolume           int
	Window              time.Duration
	CoolDown            time.Duration
	MaxCoolDown         time.Duration
	ProbeConcurrency    int
	ProbeSuccesses      int
	MaxOpenFraction     float64
	// HeaderTimeout is the gateway's upstream header timeout; with the cool-down it sets the lost-ticket backstop.
	HeaderTimeout time.Duration
}

// BreakerParamsFrom takes the breaker settings from the configuration.
func BreakerParamsFrom(c config.Config) BreakerParams {
	b := c.Resilience.Breaker
	return BreakerParams{
		ConsecutiveFailures: b.ConsecutiveFailures, FailureRatio: b.FailureRatio, MinVolume: b.MinVolume,
		Window: b.Window, CoolDown: b.CoolDown, MaxCoolDown: b.MaxCoolDown,
		ProbeConcurrency: b.ProbeConcurrency, ProbeSuccesses: b.ProbeSuccesses, MaxOpenFraction: b.MaxOpenFraction,
		HeaderTimeout: c.Gateway.UpstreamHeaderTimeout,
	}
}

// ticketBackstop is how long a probe ticket may stay outstanding before it is considered lost and released:
// max(2 x cool_down, header timeout + 5 s).
func (p BreakerParams) ticketBackstop() time.Duration {
	return max(2*p.CoolDown, p.HeaderTimeout+5*time.Second)
}

// jitterSpan is the cool-down jitter: the cool-down is multiplied by a factor in [0.8, 1.2].
const jitterSpan = 0.2

// breaker is one worker's state machine. All methods run under Breakers.mu.
type breaker struct {
	key      Key
	model    string // the ejection-cap group (Member.Model)
	eligible bool
	state    State

	// Closed: the sliding window (counter 0 = all counted outcomes, 1 = faults) and the streak of the most
	// recent consecutive Faults, kept as the times of the last ConsecutiveFailures of them (a ring).
	win       slidingWindow
	streak    []time.Time
	streakPos int
	streakLen int
	// suppressed is set while a trip is wanted but the ejection cap forbids it (reported once).
	suppressed bool

	// Cool-down memory. level indexes the doubling; it survives a pass-then-retrip and resets only after a close
	// that has lasted MaxCoolDown.
	level      int
	everOpened bool
	closedAt   time.Time

	// ejectedAt is when the breaker last tripped from closed (a failed probe does not change it). When the
	// ejection cap shrinks, the most recently ejected breakers are relaxed first.
	ejectedAt time.Time

	// Open.
	openUntil time.Time
	coolDown  time.Duration // the jittered duration of the current open period

	// Half-open. epoch changes on every open so a ticket from an earlier period is recognised as stale.
	epoch     uint64
	nextID    uint64
	tickets   map[uint64]time.Time
	successes int
}

func newBreaker(k Key, p *BreakerParams) *breaker {
	return &breaker{
		key:    k,
		win:    newSlidingWindow(p.Window),
		streak: make([]time.Time, p.ConsecutiveFailures),
	}
}

// advance applies the lazy transitions for time now: open to half-open once the cool-down has elapsed, and the
// release of lost probe tickets. It returns the change if the state moved.
func (b *breaker) advance(now time.Time, p *BreakerParams) (Change, bool) {
	var ch Change
	moved := false
	if b.state == Open && !now.Before(b.openUntil) {
		b.state = HalfOpen
		b.successes = 0
		b.tickets = nil
		ch = Change{Key: b.key, From: Open, To: HalfOpen, Reason: ReasonCoolDown, CoolDown: b.coolDown}
		moved = true
	}
	if b.state == HalfOpen && len(b.tickets) > 0 {
		backstop := p.ticketBackstop()
		for id, at := range b.tickets {
			if now.Sub(at) > backstop {
				delete(b.tickets, id)
			}
		}
	}
	return ch, moved
}

// probeFree reports whether a probe ticket can be issued now.
func (b *breaker) probeFree(p *BreakerParams) bool {
	return b.state == HalfOpen && len(b.tickets) < p.ProbeConcurrency
}

// recordClosed counts an outcome of a closed breaker and returns the reason to trip, or "".
func (b *breaker) recordClosed(now time.Time, v Verdict, p *BreakerParams) (reason string, failures int) {
	switch v {
	case OK:
		b.win.add(now, now, 0, 1)
		b.streakLen, b.streakPos = 0, 0
	case Fault:
		b.win.add(now, now, 0, 1)
		b.win.add(now, now, 1, 1)
		b.streak[b.streakPos] = now
		b.streakPos = (b.streakPos + 1) % len(b.streak)
		if b.streakLen < len(b.streak) {
			b.streakLen++
		}
	case RatioFault:
		b.win.add(now, now, 0, 1)
		b.win.add(now, now, 1, 1)
	default:
		return "", 0
	}
	// A suppressed trip is retried on the next counted fault of either kind: the streak (which an unlisted 5xx
	// neither extends nor breaks) is still there. An OK has just reset it.
	if v != OK && b.streakLen == len(b.streak) {
		oldest := b.streak[b.streakPos] // the next slot to be overwritten holds the oldest of the last N
		if now.Sub(oldest) <= p.Window {
			return ReasonConsecutive, b.streakLen
		}
	}
	if p.FailureRatio > 0 {
		total, faults := b.win.sum(now)
		if total >= p.MinVolume && total > 0 && float64(faults) >= p.FailureRatio*float64(total) {
			return ReasonRatio, faults
		}
	}
	return "", 0
}

// baseCoolDown is the un-jittered cool-down at a doubling level, clamped to MaxCoolDown.
func (p BreakerParams) baseCoolDown(level int) time.Duration {
	d := p.CoolDown
	for i := 0; i < level; i++ {
		if d >= p.MaxCoolDown/2+p.MaxCoolDown%2 { // the next doubling reaches the cap
			return p.MaxCoolDown
		}
		d *= 2
	}
	return min(d, p.MaxCoolDown)
}

// maxLevel is the first level whose cool-down is the cap; levels do not grow beyond it.
func (p BreakerParams) maxLevel() int {
	l := 0
	for p.baseCoolDown(l) < p.MaxCoolDown {
		l++
	}
	return l
}

// open moves the breaker to Open. fromProbe says that a half-open probe failed (the cool-down level grows);
// a trip from closed keeps the level it had unless the breaker has been closed for MaxCoolDown.
func (b *breaker) open(now time.Time, fromProbe bool, p *BreakerParams, rnd Rand) {
	switch {
	case fromProbe:
		if b.level < p.maxLevel() {
			b.level++
		}
	case !b.everOpened || now.Sub(b.closedAt) >= p.MaxCoolDown:
		b.level = 0
	}
	b.everOpened = true
	if !fromProbe {
		b.ejectedAt = now
	}
	d := time.Duration(float64(p.baseCoolDown(b.level)) * (1 + jitterSpan*(2*rnd()-1)))
	d = min(max(d, 1), p.MaxCoolDown)
	b.state = Open
	b.coolDown = d
	b.openUntil = now.Add(d)
	b.epoch++
	b.tickets = nil
	b.successes = 0
	b.suppressed = false
}

// close moves the breaker to Closed with a fresh window and streak.
func (b *breaker) close(now time.Time) {
	b.state = Closed
	b.closedAt = now
	b.tickets = nil
	b.successes = 0
	b.win.reset()
	b.streakLen, b.streakPos = 0, 0
	b.suppressed = false
}
