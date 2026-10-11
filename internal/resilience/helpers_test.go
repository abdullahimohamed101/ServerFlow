package resilience

import (
	"sync"
	"time"
)

// fakeClock is a manual clock. Tests never sleep.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// scriptedRand returns the queued values, then a default. 0.5 gives a jitter factor of exactly 1.
type scriptedRand struct {
	mu   sync.Mutex
	vals []float64
	def  float64
}

func (r *scriptedRand) Float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.vals) > 0 {
		v := r.vals[0]
		r.vals = r.vals[1:]
		return v
	}
	return r.def
}

func (r *scriptedRand) push(v ...float64) {
	r.mu.Lock()
	r.vals = append(r.vals, v...)
	r.mu.Unlock()
}
