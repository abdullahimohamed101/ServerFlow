package resilience

import (
	"math"
	"sync"
	"time"
)

// Member is one worker of the current registry view, as the gateway reports it to Sync.
type Member struct {
	Key Key
	// Eligible is true when the registry would route to the worker right now (healthy, not draining, fresh
	// heartbeat in the aged view). Only eligible workers count towards the ejection cap (D9).
	Eligible bool
}

// Ticket is the right to send one attempt to a worker. A probe ticket (half-open) must come back through Record
// or Release exactly once; a ticket of a closed breaker is only a handle for the worker's key.
type Ticket struct {
	key   Key
	id    uint64
	epoch uint64
	probe bool
	valid bool
}

// Probe reports whether the attempt is a half-open probe (AttemptStart.Probe).
func (t Ticket) Probe() bool { return t.probe }

// Key returns the worker the ticket was taken for.
func (t Ticket) Key() Key { return t.key }

// Change describes a state transition, or a trip that the ejection cap suppressed (Suppressed, From == To ==
// Closed). It is delivered to Options.OnChange after the internal lock is released.
type Change struct {
	Key        Key
	From, To   State
	Reason     string
	Failures   int           // faults counted by the rule that tripped (consecutive streak or window faults)
	CoolDown   time.Duration // the open period for transitions into Open or HalfOpen
	Suppressed bool
}

// Info describes one non-closed breaker, for the state gauge.
type Info struct {
	Key       Key
	State     State
	Eligible  bool
	OpenUntil time.Time // when it becomes half-open (Open only)
	Level     int
	Probes    int // outstanding probe tickets
}

// Options configure Breakers.
type Options struct {
	Now      Clock
	Rand     Rand
	OnChange func(Change)
}

// Breakers holds one circuit breaker per worker registration, local to this gateway. The gateway reports the
// registry view with Sync (on every refresh and whenever eligibility changes by aging); a worker not in the last
// Sync has no breaker: it is admissible, takes a non-probe ticket and its verdicts are ignored. Everything is
// lazy: the clock is read on each call and nothing runs in the background.
type Breakers struct {
	p   BreakerParams
	now Clock
	rnd Rand
	cb  func(Change)

	mu         sync.Mutex
	byID       map[string]*breaker
	eligible   int
	suppressed uint64
	pending    []Change
}

// NewBreakers creates an empty set.
func NewBreakers(p BreakerParams, o Options) *Breakers {
	p = p.normalize()
	return &Breakers{p: p, now: o.Now.orNow(), rnd: o.Rand.orDefault(), cb: o.OnChange, byID: map[string]*breaker{}}
}

// normalize replaces values that would break the arithmetic (config validation keeps real ones in range).
func (p BreakerParams) normalize() BreakerParams {
	if p.ConsecutiveFailures < 1 {
		p.ConsecutiveFailures = 1
	}
	if p.MinVolume < 1 {
		p.MinVolume = 1
	}
	if p.Window < bucketWidth {
		p.Window = bucketWidth
	}
	if p.CoolDown <= 0 {
		p.CoolDown = time.Millisecond
	}
	if p.MaxCoolDown < p.CoolDown {
		p.MaxCoolDown = p.CoolDown
	}
	if p.ProbeConcurrency < 1 {
		p.ProbeConcurrency = 1
	}
	if p.ProbeSuccesses < 1 {
		p.ProbeSuccesses = 1
	}
	return p
}

func (bs *Breakers) lookup(k Key) *breaker {
	if b := bs.byID[k.ID]; b != nil && b.key == k {
		return b
	}
	return nil
}

func (bs *Breakers) emit(c Change) { bs.pending = append(bs.pending, c) }

// unlock releases the lock and delivers pending changes to the hook, outside the lock.
func (bs *Breakers) unlock() {
	var out []Change
	if len(bs.pending) > 0 {
		out = bs.pending
		bs.pending = nil
	}
	bs.mu.Unlock()
	if bs.cb != nil {
		for _, c := range out {
			bs.cb(c)
		}
	}
}

// Sync replaces the known workers by the current view: breakers of workers that are gone are pruned, a worker
// with a new registration starts closed, and eligibility is updated. It is cheap (one pass) and idempotent.
func (bs *Breakers) Sync(members []Member) {
	bs.mu.Lock()
	defer bs.unlock()
	keep := make(map[string]struct{}, len(members))
	bs.eligible = 0
	for _, m := range members {
		keep[m.Key.ID] = struct{}{}
		b := bs.lookup(m.Key)
		if b == nil {
			b = newBreaker(m.Key, &bs.p)
			bs.byID[m.Key.ID] = b
		}
		b.eligible = m.Eligible
		if m.Eligible {
			bs.eligible++
		}
	}
	for id := range bs.byID {
		if _, ok := keep[id]; !ok {
			delete(bs.byID, id)
		}
	}
	bs.enforceCap()
}

// Prune drops every breaker whose worker is not in keep, without touching the others.
func (bs *Breakers) Prune(keep []Key) {
	bs.mu.Lock()
	defer bs.unlock()
	set := make(map[Key]struct{}, len(keep))
	for _, k := range keep {
		set[k] = struct{}{}
	}
	for id, b := range bs.byID {
		if _, ok := set[b.key]; !ok {
			delete(bs.byID, id)
		}
	}
	bs.recountEligible()
	bs.enforceCap()
}

// enforceCap closes breakers until open + half-open among eligible workers is within the cap again. The cap can
// shrink or the numerator can grow without any new trip: a worker that tripped while it was ineligible turns
// eligible again, or healthy workers turn unhealthy. Leaving it would eject more than max_open_fraction and could
// leave a model with no admissible worker for a whole cool-down. The most recently ejected breakers (ties: the
// larger worker ID) are closed first, so the longest-standing evidence is kept; a closed breaker starts with a
// fresh window and keeps its cool-down level.
func (bs *Breakers) enforceCap() {
	limit := MaxOpen(bs.p.MaxOpenFraction, bs.eligible)
	now := bs.now()
	for {
		var victim *breaker
		open := 0
		for _, b := range bs.byID {
			if !b.eligible || b.state == Closed {
				continue
			}
			open++
			if victim == nil || b.ejectedAt.After(victim.ejectedAt) || (b.ejectedAt.Equal(victim.ejectedAt) && b.key.ID > victim.key.ID) {
				victim = b
			}
		}
		if open <= limit || victim == nil {
			return
		}
		from := victim.state
		victim.close(now)
		bs.emit(Change{Key: victim.key, From: from, To: Closed, Reason: ReasonCapRelaxed})
	}
}

func (bs *Breakers) recountEligible() {
	bs.eligible = 0
	for _, b := range bs.byID {
		if b.eligible {
			bs.eligible++
		}
	}
}

// advanceLocked applies lazy transitions of one breaker.
func (bs *Breakers) advanceLocked(b *breaker, now time.Time) {
	if ch, moved := b.advance(now, &bs.p); moved {
		bs.emit(ch)
	}
}

// Admissible reports whether the worker may be selected: its breaker is closed, or half-open with a free probe
// slot. A worker without a breaker is admissible. It does not take a ticket.
func (bs *Breakers) Admissible(k Key) bool {
	bs.mu.Lock()
	defer bs.unlock()
	b := bs.lookup(k)
	if b == nil {
		return true
	}
	bs.advanceLocked(b, bs.now())
	return b.state == Closed || b.probeFree(&bs.p)
}

// Acquire takes the right to send to the worker after it was selected. It fails when a concurrent request took
// the last probe slot or the breaker opened since Admissible; the caller then selects again. The ticket must be
// passed to Record (or Release) once the attempt's verdict is known.
func (bs *Breakers) Acquire(k Key) (Ticket, bool) {
	bs.mu.Lock()
	defer bs.unlock()
	b := bs.lookup(k)
	if b == nil {
		return Ticket{key: k, valid: true}, true
	}
	now := bs.now()
	bs.advanceLocked(b, now)
	switch b.state {
	case Closed:
		return Ticket{key: k, epoch: b.epoch, valid: true}, true
	case HalfOpen:
		if !b.probeFree(&bs.p) {
			return Ticket{}, false
		}
		b.nextID++
		if b.tickets == nil {
			b.tickets = map[uint64]time.Time{}
		}
		b.tickets[b.nextID] = now
		return Ticket{key: k, id: b.nextID, epoch: b.epoch, probe: true, valid: true}, true
	}
	return Ticket{}, false
}

// Release returns a probe ticket without a verdict (the attempt never ran, or was cancelled). It is idempotent.
func (bs *Breakers) Release(t Ticket) {
	if !t.probe {
		return
	}
	bs.mu.Lock()
	defer bs.unlock()
	if b := bs.lookup(t.key); b != nil && b.state == HalfOpen && b.epoch == t.epoch {
		delete(b.tickets, t.id)
	}
}

// Record takes the verdict of an attempt at its retry-decision point and returns the ticket. For a probe ticket
// OK counts towards closing, Fault and RatioFault reopen with a doubled cool-down, and Neutral just returns the
// ticket; a ticket that is stale (released, lost, or from an earlier open period) is ignored. For other tickets
// the verdict feeds the closed breaker's window; it is ignored while the breaker is not closed.
func (bs *Breakers) Record(t Ticket, v Verdict) {
	if !t.valid {
		return
	}
	bs.mu.Lock()
	defer bs.unlock()
	bs.recordLocked(t, v)
}

// RecordLate records a fault that is only known after the attempt's verdict was taken: a stream that aborted after
// its first byte. Pass the attempt's own ticket (the one Record already consumed). While the breaker is closed the
// fault counts like any other. If the ticket was a probe and its half-open period is still current, the probe did
// not really succeed: the breaker reopens with a doubled cool-down (so with probe_successes > 1 an aborted stream
// cannot count as a success). A fault from a request that started before the breaker last opened (an older ticket epoch), in
// whatever state the breaker is in now, is ignored: it already acted on that evidence, and counting it in the new
// window could trip a recovered worker on stale information.
func (bs *Breakers) RecordLate(t Ticket, v Verdict) {
	if !t.valid || (v != Fault && v != RatioFault) {
		return
	}
	bs.mu.Lock()
	defer bs.unlock()
	b := bs.lookup(t.key)
	if b == nil {
		return
	}
	now := bs.now()
	bs.advanceLocked(b, now)
	if t.probe {
		if b.epoch != t.epoch {
			return
		}
		if b.state == HalfOpen {
			b.open(now, true, &bs.p, bs.rnd)
			bs.emit(Change{Key: b.key, From: HalfOpen, To: Open, Reason: ReasonProbeFailed, CoolDown: b.coolDown})
			return
		}
	}
	if b.epoch != t.epoch {
		return // started before the breaker last opened, however it got back to closed
	}
	bs.countClosed(b, now, v)
}

func (bs *Breakers) recordLocked(t Ticket, v Verdict) {
	b := bs.lookup(t.key)
	if b == nil {
		return
	}
	now := bs.now()
	bs.advanceLocked(b, now)
	if t.probe {
		if b.state != HalfOpen || b.epoch != t.epoch {
			return
		}
		if _, ok := b.tickets[t.id]; !ok {
			return
		}
		delete(b.tickets, t.id)
		switch v {
		case OK:
			b.successes++
			if b.successes >= bs.p.ProbeSuccesses {
				b.close(now)
				bs.emit(Change{Key: b.key, From: HalfOpen, To: Closed, Reason: ReasonProbesPassed})
			}
		case Fault, RatioFault:
			b.open(now, true, &bs.p, bs.rnd)
			bs.emit(Change{Key: b.key, From: HalfOpen, To: Open, Reason: ReasonProbeFailed, CoolDown: b.coolDown})
		}
		return
	}
	if b.epoch != t.epoch {
		return // the request started before the breaker last opened: that evidence is old
	}
	bs.countClosed(b, now, v)
}

// countClosed counts an outcome of a closed breaker and trips it if a rule says so and the ejection cap has room.
func (bs *Breakers) countClosed(b *breaker, now time.Time, v Verdict) {
	if b.state != Closed || v == Neutral {
		return
	}
	reason, failures := b.recordClosed(now, v, &bs.p)
	if reason == "" {
		b.suppressed = false
		return
	}
	if !bs.roomToOpen(b) {
		if !b.suppressed {
			b.suppressed = true
			bs.suppressed++
			bs.emit(Change{Key: b.key, From: Closed, To: Closed, Reason: ReasonSuppressed, Failures: failures, Suppressed: true})
		}
		return
	}
	b.open(now, false, &bs.p, bs.rnd)
	bs.emit(Change{Key: b.key, From: Closed, To: Open, Reason: reason, Failures: failures, CoolDown: b.coolDown})
}

// MaxOpen is the most breakers that may be open or half-open among n registry-eligible workers:
// floor(fraction x n). With 0.5 it is 0, 1, 1, 2 for 1, 2, 3, 4 workers (D9).
func MaxOpen(fraction float64, eligible int) int {
	if fraction <= 0 || eligible <= 0 {
		return 0
	}
	if fraction >= 1 {
		return eligible
	}
	return int(math.Floor(fraction*float64(eligible) + 1e-9))
}

// roomToOpen applies the ejection cap. Both the denominator and the numerator count only registry-eligible
// workers; a worker that is not eligible cannot be selected anyway, so its own trip takes no room.
func (bs *Breakers) roomToOpen(b *breaker) bool {
	if !b.eligible {
		return true
	}
	open := 0
	for _, o := range bs.byID {
		if o.eligible && o.state != Closed {
			open++
		}
	}
	return open < MaxOpen(bs.p.MaxOpenFraction, bs.eligible)
}

// Snapshot lists the breakers that are not closed (the state gauge), after applying the lazy transitions.
func (bs *Breakers) Snapshot() []Info {
	bs.mu.Lock()
	defer bs.unlock()
	now := bs.now()
	var out []Info
	for _, b := range bs.byID {
		bs.advanceLocked(b, now)
		if b.state != Closed {
			out = append(out, Info{Key: b.key, State: b.state, Eligible: b.eligible, OpenUntil: b.openUntil, Level: b.level, Probes: len(b.tickets)})
		}
	}
	return out
}

// State returns one worker's state (Closed for an unknown worker) after applying the lazy transitions.
func (bs *Breakers) State(k Key) State {
	bs.mu.Lock()
	defer bs.unlock()
	b := bs.lookup(k)
	if b == nil {
		return Closed
	}
	bs.advanceLocked(b, bs.now())
	return b.state
}

// Counts returns how many registry-eligible workers there are and how many of them are open and half-open.
func (bs *Breakers) Counts() (eligible, open, halfOpen int) {
	bs.mu.Lock()
	defer bs.unlock()
	now := bs.now()
	for _, b := range bs.byID {
		bs.advanceLocked(b, now)
		if !b.eligible {
			continue
		}
		switch b.state {
		case Open:
			open++
		case HalfOpen:
			halfOpen++
		}
	}
	return bs.eligible, open, halfOpen
}

// Suppressed is the number of trips the ejection cap has held back so far.
func (bs *Breakers) Suppressed() uint64 {
	bs.mu.Lock()
	defer bs.unlock()
	return bs.suppressed
}

// RetryAfter is the whole seconds until the earliest open breaker becomes half-open, clamped to 1-30. It is the
// Retry-After for a refusal when every eligible worker is ejected. With a half-open breaker (or none open) it is 1.
func (bs *Breakers) RetryAfter() int {
	bs.mu.Lock()
	defer bs.unlock()
	now := bs.now()
	earliest := time.Duration(-1)
	for _, b := range bs.byID {
		bs.advanceLocked(b, now)
		if !b.eligible {
			continue
		}
		switch b.state {
		case HalfOpen:
			earliest = 0
		case Open:
			if d := b.openUntil.Sub(now); earliest < 0 || d < earliest {
				earliest = d
			}
		}
		if earliest == 0 {
			break
		}
	}
	secs := int(math.Ceil(earliest.Seconds()))
	return min(max(secs, 1), 30)
}
