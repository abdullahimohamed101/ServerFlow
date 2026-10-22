// Package resilience holds the pure building blocks of Phase 15 fault tolerance (ADR-020): per-worker circuit
// breakers (Breakers), the retry budget (Budget), the per-model admission queue with direct slot hand-off (Queue),
// and the global admission counter (Admission).
//
// Rules of the package:
//
//   - Standard library plus internal/config only. It must not import internal/gateway (a test enforces it); the
//     gateway adapts these types, not the other way round.
//   - No goroutines, no timers. Time comes from an injectable clock and randomness from an injectable source, and
//     every state change is evaluated lazily when a method is called. Tests drive a fake clock and never sleep.
//   - Every type is safe for concurrent use. Lock order, when a caller holds its own lock around these calls: the
//     caller's lock first, then the package's. Callbacks (Breakers' change hook) run after the package lock is
//     released.
//
// Windows (the breaker's outcome window, the budget) count whole seconds of the wall-clock reading of the injected
// clock (UnixNano), while the streak check, cool-downs and ticket ages use time differences, which are monotonic for
// the real clock. A system clock step therefore only moves events between window buckets: a step back keeps
// events in the future until they are ignored, a step forward ages them out early. Both are bounded by the window
// length and neither can wedge a breaker.
//
// The gateway wires this package in (Phase 15, ADR-020) when resilience.mode is on (the default) and workers come from the registry.
package resilience

import (
	"math/rand/v2"
	"time"
)

// Clock returns the current time. The zero value of an option means time.Now.
type Clock func() time.Time

// Rand returns a uniform float in [0, 1). The zero value of an option means a goroutine-safe global source.
type Rand func() float64

func (c Clock) orNow() Clock {
	if c == nil {
		return time.Now
	}
	return c
}

func (r Rand) orDefault() Rand {
	if r == nil {
		return rand.Float64
	}
	return r
}

// Key identifies a worker registration: its ID and the time it registered, as UnixNano. A worker that registers
// again has a different key, so its breaker starts closed.
type Key struct {
	ID         string
	Registered int64
}

// KeyOf builds a Key from a worker ID and its registration time.
func KeyOf(id string, registeredAt time.Time) Key {
	return Key{ID: id, Registered: registeredAt.UnixNano()}
}

// bucketWidth is the resolution of every sliding window in this package.
const bucketWidth = time.Second

// slidingWindow counts events in whole-second buckets over the last n seconds (the current bucket included).
// It allocates its ring once and never grows. Not safe for concurrent use; owners lock.
type slidingWindow struct {
	n     int64
	stamp []int64
	vals  [][2]int
}

func newSlidingWindow(window time.Duration) slidingWindow {
	n := int64((window + bucketWidth - 1) / bucketWidth)
	if n < 1 {
		n = 1
	}
	return slidingWindow{n: n, stamp: make([]int64, n), vals: make([][2]int, n)}
}

func bucketOf(t time.Time) int64 { return t.UnixNano() / int64(bucketWidth) } // floor for t >= epoch

func (w *slidingWindow) slot(idx int64) int { return int(((idx % w.n) + w.n) % w.n) }

// add adds delta to counter which (0 or 1) of the bucket at time t. A bucket outside the window (an event
// older than the window, or a refund for an expired bucket) is ignored.
func (w *slidingWindow) add(t time.Time, now time.Time, which, delta int) {
	idx, cur := bucketOf(t), bucketOf(now)
	if idx <= cur-w.n || idx > cur {
		return
	}
	s := w.slot(idx)
	if w.stamp[s] != idx {
		w.stamp[s] = idx
		w.vals[s] = [2]int{}
	}
	w.vals[s][which] += delta
	if w.vals[s][which] < 0 {
		w.vals[s][which] = 0
	}
}

// sum returns both counters over the buckets cur-n+1 .. cur.
func (w *slidingWindow) sum(now time.Time) (a, b int) {
	cur := bucketOf(now)
	for s := range w.stamp {
		if idx := w.stamp[s]; idx > cur-w.n && idx <= cur {
			a += w.vals[s][0]
			b += w.vals[s][1]
		}
	}
	return a, b
}

func (w *slidingWindow) reset() {
	for i := range w.stamp {
		w.stamp[i] = 0
		w.vals[i] = [2]int{}
	}
}
