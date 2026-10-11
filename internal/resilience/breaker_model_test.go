package resilience

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// The reference below restates the breaker rules in the most naive way: it keeps every outcome in a list and
// rescans it, with no ring buffers, buckets or cached counters. The test drives the real Breakers and the
// reference with the same random operations (same seeded jitter) and requires the same answers at every step.

type refEvent struct {
	t time.Time
	v Verdict
}

type refBreaker struct {
	reg        int64
	eligible   bool
	state      State
	events     []refEvent
	level      int
	everOpened bool
	closedAt   time.Time
	openUntil  time.Time
	epoch      int
	nextID     int
	tickets    map[int]time.Time
	successes  int
	suppressed bool
	changes    []string
}

type refModel struct {
	p  BreakerParams
	br map[string]*refBreaker
	rn *rand.Rand
}

func (m *refModel) eligibleCount() int {
	n := 0
	for _, b := range m.br {
		if b.eligible {
			n++
		}
	}
	return n
}

func (m *refModel) advance(b *refBreaker, now time.Time, id string) {
	if b.state == Open && !now.Before(b.openUntil) {
		b.state = HalfOpen
		b.successes = 0
		b.tickets = map[int]time.Time{}
		b.changes = append(b.changes, "open>half_open")
	}
	if b.state == HalfOpen {
		for id, at := range b.tickets {
			if now.Sub(at) > m.p.ticketBackstopRef() {
				delete(b.tickets, id)
			}
		}
	}
}

func (p BreakerParams) ticketBackstopRef() time.Duration {
	a := 2 * p.CoolDown
	b := p.HeaderTimeout + 5*time.Second
	if a > b {
		return a
	}
	return b
}

func (m *refModel) coolDownFor(level int) time.Duration {
	d := m.p.CoolDown
	for i := 0; i < level; i++ {
		d *= 2
		if d >= m.p.MaxCoolDown {
			return m.p.MaxCoolDown
		}
	}
	if d > m.p.MaxCoolDown {
		d = m.p.MaxCoolDown
	}
	return d
}

func (m *refModel) topLevel() int {
	l := 0
	for m.coolDownFor(l) < m.p.MaxCoolDown {
		l++
	}
	return l
}

func (m *refModel) open(b *refBreaker, now time.Time, probeFailed bool, why string) {
	if probeFailed {
		if b.level < m.topLevel() {
			b.level++
		}
	} else if !b.everOpened || now.Sub(b.closedAt) >= m.p.MaxCoolDown {
		b.level = 0
	}
	b.everOpened = true
	d := time.Duration(float64(m.coolDownFor(b.level)) * (1 + 0.2*(2*m.rn.Float64()-1)))
	if d < 1 {
		d = 1
	}
	if d > m.p.MaxCoolDown {
		d = m.p.MaxCoolDown
	}
	from := b.state
	b.state = Open
	b.openUntil = now.Add(d)
	b.epoch++
	b.tickets = nil
	b.successes = 0
	b.suppressed = false
	b.changes = append(b.changes, fmt.Sprintf("%v>open:%s", from, why))
}

func (m *refModel) acquire(id string, now time.Time) (probe, ok bool, tid, epoch int) {
	b := m.br[id]
	if b == nil {
		return false, true, 0, 0
	}
	m.advance(b, now, id)
	switch b.state {
	case Closed:
		return false, true, 0, 0
	case HalfOpen:
		if len(b.tickets) >= m.p.ProbeConcurrency {
			return false, false, 0, 0
		}
		b.nextID++
		b.tickets[b.nextID] = now
		return true, true, b.nextID, b.epoch
	}
	return false, false, 0, 0
}

func (m *refModel) admissible(id string, now time.Time) bool {
	b := m.br[id]
	if b == nil {
		return true
	}
	m.advance(b, now, id)
	return b.state == Closed || (b.state == HalfOpen && len(b.tickets) < m.p.ProbeConcurrency)
}

func (m *refModel) release(id string, probe bool, tid, epoch int) {
	b := m.br[id]
	if !probe || b == nil || b.state != HalfOpen || b.epoch != epoch {
		return
	}
	delete(b.tickets, tid)
}

func (m *refModel) record(id string, reg int64, probe bool, tid, epoch int, v Verdict, now time.Time) {
	b := m.br[id]
	if b == nil || b.reg != reg {
		return
	}
	m.advance(b, now, id)
	if probe {
		if b.state != HalfOpen || b.epoch != epoch {
			return
		}
		if _, ok := b.tickets[tid]; !ok {
			return
		}
		delete(b.tickets, tid)
		switch v {
		case OK:
			b.successes++
			if b.successes >= m.p.ProbeSuccesses {
				b.state = Closed
				b.closedAt = now
				b.events = nil
				b.tickets = nil
				b.successes = 0
				b.suppressed = false
				b.changes = append(b.changes, "half_open>closed")
			}
		case Fault, RatioFault:
			m.open(b, now, true, ReasonProbeFailed)
		}
		return
	}
	if b.state != Closed || v == Neutral {
		return
	}
	if v == OK {
		// an ok ends the streak but stays in the window
	}
	b.events = append(b.events, refEvent{now, v})
	why := m.tripReason(b, now, v)
	if why == "" {
		b.suppressed = false
		return
	}
	if b.eligible {
		open := 0
		for _, o := range m.br {
			if o.eligible && o.state != Closed {
				open++
			}
		}
		if open >= int(math.Floor(m.p.MaxOpenFraction*float64(m.eligibleCount())+1e-9)) {
			if !b.suppressed {
				b.suppressed = true
				b.changes = append(b.changes, "suppressed")
			}
			return
		}
	}
	m.open(b, now, false, why)
}

func (m *refModel) tripReason(b *refBreaker, now time.Time, v Verdict) string {
	if v == Fault {
		var times []time.Time // newest first, back to the last OK
		for i := len(b.events) - 1; i >= 0; i-- {
			switch b.events[i].v {
			case OK:
				i = -1
			case Fault:
				times = append(times, b.events[i].t)
			}
		}
		if len(times) >= m.p.ConsecutiveFailures && now.Sub(times[m.p.ConsecutiveFailures-1]) <= m.p.Window {
			return ReasonConsecutive
		}
	}
	if m.p.FailureRatio > 0 {
		nb := int64((m.p.Window + time.Second - 1) / time.Second)
		cur := now.UnixNano() / int64(time.Second)
		total, faults := 0, 0
		for _, e := range b.events {
			if e.t.UnixNano()/int64(time.Second) > cur-nb {
				total++
				if e.v == Fault || e.v == RatioFault {
					faults++
				}
			}
		}
		if total >= m.p.MinVolume && float64(faults) >= m.p.FailureRatio*float64(total) {
			return ReasonRatio
		}
	}
	return ""
}

type pendingTicket struct {
	real  Ticket
	id    string
	reg   int64
	probe bool
	tid   int
	epoch int
}

func TestBreakerMatchesTheNaiveReference(t *testing.T) {
	paramSets := []BreakerParams{
		testParams(),
		{ConsecutiveFailures: 3, FailureRatio: 0.4, MinVolume: 6, Window: 5 * time.Second, CoolDown: 2 * time.Second,
			MaxCoolDown: 9 * time.Second, ProbeConcurrency: 2, ProbeSuccesses: 2, MaxOpenFraction: 0.5, HeaderTimeout: time.Second},
		{ConsecutiveFailures: 2, FailureRatio: 0, MinVolume: 1, Window: time.Second, CoolDown: time.Second,
			MaxCoolDown: time.Second, ProbeConcurrency: 1, ProbeSuccesses: 1, MaxOpenFraction: 1, HeaderTimeout: time.Second},
		{ConsecutiveFailures: 50, FailureRatio: 0.7, MinVolume: 4, Window: 3 * time.Second, CoolDown: 3 * time.Second,
			MaxCoolDown: 30 * time.Second, ProbeConcurrency: 3, ProbeSuccesses: 1, MaxOpenFraction: 0.34, HeaderTimeout: 20 * time.Second},
	}
	steps := []time.Duration{0, 0, time.Millisecond, 400 * time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second,
		5 * time.Second, 9 * time.Second, 10 * time.Second, 11 * time.Second, 25 * time.Second, 70 * time.Second, 130 * time.Second}
	workers := []string{"a", "b", "c", "d"}
	verdicts := []Verdict{Neutral, OK, OK, Fault, Fault, Fault, RatioFault}

	seen := map[string]int{}
	for pi, p := range paramSets {
		for seed := uint64(1); seed <= 60; seed++ {
			t.Run(fmt.Sprintf("params%d/seed%d", pi, seed), func(t *testing.T) {
				p = p.normalize()
				clk := newFakeClock()
				realRand := rand.New(rand.NewPCG(seed, 99))
				var changes []Change
				bs := NewBreakers(p, Options{Now: clk.Now, Rand: realRand.Float64, OnChange: func(c Change) { changes = append(changes, c) }})
				ref := &refModel{p: p, br: map[string]*refBreaker{}, rn: rand.New(rand.NewPCG(seed, 99))}
				ops := rand.New(rand.NewPCG(seed, uint64(pi)+7))

				reg := map[string]int64{}
				present := map[string]bool{}
				eligible := map[string]bool{}
				for _, w := range workers {
					reg[w], present[w], eligible[w] = 1, true, true
				}
				keyOf := func(w string) Key { return KeyOf(w, time.Unix(1_700_000_000+reg[w], 0)) }
				incarnation := map[string]int{} // index into changes where the current breaker of a worker began
				sync := func() {
					var ms []Member
					for _, w := range workers {
						if present[w] {
							ms = append(ms, Member{Key: keyOf(w), Eligible: eligible[w]})
						}
					}
					bs.Sync(ms)
					for _, w := range workers {
						b := ref.br[w]
						switch {
						case !present[w]:
							delete(ref.br, w)
						case b == nil || b.reg != reg[w]:
							ref.br[w] = &refBreaker{reg: reg[w], eligible: eligible[w]}
							incarnation[w] = len(changes)
						default:
							b.eligible = eligible[w]
						}
					}
				}
				sync()
				var pending []pendingTicket
				_ = pending

				for step := 0; step < 400; step++ {
					now := clk.Now()
					w := workers[ops.IntN(len(workers))]
					switch op := ops.IntN(100); {
					case op < 38: // an attempt: acquire, then record now or later
						tk, ok := bs.Acquire(keyOf(w))
						rProbe, rOK, tid, ep := ref.acquire(w, now)
						if ok != rOK || (ok && tk.Probe() != rProbe) {
							t.Fatalf("step %d acquire %s: real (%v,%v) ref (%v,%v)", step, w, ok, tk.Probe(), rOK, rProbe)
						}
						if !ok {
							break
						}
						pt := pendingTicket{real: tk, id: w, reg: reg[w], probe: rProbe, tid: tid, epoch: ep}
						if ops.IntN(3) == 0 {
							pending = append(pending, pt)
						} else {
							v := verdicts[ops.IntN(len(verdicts))]
							bs.Record(tk, v)
							ref.record(pt.id, pt.reg, pt.probe, pt.tid, pt.epoch, v, now)
						}
					case op < 55 && len(pending) > 0: // finish a pending attempt
						i := ops.IntN(len(pending))
						pt := pending[i]
						pending = append(pending[:i], pending[i+1:]...)
						if ops.IntN(5) == 0 {
							bs.Release(pt.real)
							ref.release(pt.id, pt.probe, pt.tid, pt.epoch)
						} else {
							v := verdicts[ops.IntN(len(verdicts))]
							bs.Record(pt.real, v)
							ref.record(pt.id, pt.reg, pt.probe, pt.tid, pt.epoch, v, now)
						}
					case op < 63: // a stream abort after the first byte
						v := []Verdict{Fault, Fault, OK}[ops.IntN(3)]
						bs.RecordLate(keyOf(w), v)
						ref.record(w, reg[w], false, 0, 0, v, now)
					case op < 85: // time passes
						clk.Advance(steps[ops.IntN(len(steps))])
					default: // the registry view changes
						switch ops.IntN(4) {
						case 0:
							eligible[w] = !eligible[w]
						case 1:
							present[w] = !present[w]
						case 2:
							reg[w]++
						case 3:
							for _, o := range workers {
								eligible[o] = true
							}
						}
						sync()
					}
					now = clk.Now()
					for _, o := range workers {
						if !present[o] {
							continue
						}
						ref.advance(ref.br[o], now, o)
						if got, want := bs.State(keyOf(o)), ref.br[o].state; got != want {
							t.Fatalf("step %d: state of %s = %v, reference %v", step, o, got, want)
						}
						if got, want := bs.Admissible(keyOf(o)), ref.admissible(o, now); got != want {
							t.Fatalf("step %d: admissible(%s) = %v, reference %v", step, o, got, want)
						}
					}
				}
				// The same transitions in the same per-worker order.
				for _, o := range workers {
					if !present[o] {
						continue
					}
					var got []string
					for _, c := range changes[incarnation[o]:] {
						if c.Key == keyOf(o) {
							switch {
							case c.Suppressed:
								got = append(got, "suppressed")
							case c.To == Open && c.From == Closed:
								got = append(got, "closed>open:"+c.Reason)
							case c.To == Open:
								got = append(got, "half_open>open:"+c.Reason)
							default:
								got = append(got, fmt.Sprintf("%v>%v", c.From, c.To))
							}
						}
					}
					want := ref.br[o].changes
					for _, c := range want {
						seen[c]++
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("changes of %s differ\n real: %v\n ref:  %v", o, got, want)
					}
				}
			})
		}
	}
	// The random walk must actually reach every kind of transition, or agreement proves little.
	for _, k := range []string{"closed>open:consecutive_failures", "closed>open:failure_ratio", "open>half_open",
		"half_open>open:probe_failed", "half_open>closed", "suppressed"} {
		if seen[k] < 20 {
			t.Errorf("transition %q seen only %d times across the random runs", k, seen[k])
		}
	}
}
