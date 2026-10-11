// Package chaos is the Phase 15 fault-injection harness. It runs a load against an embedded cluster in
// three windows (baseline, fault, recovery), applies a scenario's fault between the first and the second,
// removes it between the second and the third, and reports what the clients saw, with a validity gate
// that marks a run unusable when the run itself, not the system, explains the numbers.
//
// It reuses the Phase 7 packages (driver, collect, report, embedded) and compiles against a gateway with
// or without the resilience feature: it never names the feature's types. See docs/benchmarks/phase-15-chaos-harness.md.
package chaos

import (
	"context"
	"sync"
	"time"
)

// Clock is the harness's view of time, so a scenario's timeline can be tested without waiting for it.
// The load driver and the cluster use real time; the timeline (when the fault is applied and cleared,
// when the series is sampled) goes through a Clock.
type Clock interface {
	// Now is the current time. It carries a monotonic reading when the clock is real.
	Now() time.Time
	// SleepUntil returns when Now reaches t, or when ctx ends (with its error).
	SleepUntil(ctx context.Context, t time.Time) error
	// WallOffset is how far the wall clock has moved ahead of the monotonic clock since the clock was
	// made. It changes when the machine sleeps or the wall clock is stepped; the harness samples it each
	// tick and marks the run invalid when it moves by more than MaxClockStep.
	WallOffset() time.Duration
}

// MaxClockStep is the most the wall clock may move against the monotonic clock during a run before the
// run is invalid. Phase 8 lost a run to a laptop that slept: the load stalled and nothing said so.
const MaxClockStep = 250 * time.Millisecond

// RealClock is the system clock.
func RealClock() Clock { return &realClock{origin: time.Now()} }

type realClock struct{ origin time.Time }

func (c *realClock) Now() time.Time { return time.Now() }

func (c *realClock) SleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WallOffset compares the wall-clock and monotonic readings of the same interval: Round(0) strips the
// monotonic reading, so the first difference is wall time and the second monotonic time.
func (c *realClock) WallOffset() time.Duration {
	n := time.Now()
	return n.Round(0).Sub(c.origin.Round(0)) - n.Sub(c.origin)
}

// FakeClock is a clock that moves only when it is slept on, for tests: SleepUntil jumps to the
// deadline at once. Its wall offset is set by the test.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	offset time.Duration
	// OnSleep, if set, runs (without the lock) after each jump with the new time; a test uses it to
	// move the wall offset at a chosen moment.
	OnSleep func(now time.Time)
}

// NewFakeClock returns a fake clock starting at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{now: t} }

// Now returns the fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// SleepUntil advances the clock to t (never backwards) and returns.
func (c *FakeClock) SleepUntil(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if t.After(c.now) {
		c.now = t
	}
	now, hook := c.now, c.OnSleep
	c.mu.Unlock()
	if hook != nil {
		hook(now)
	}
	return nil
}

// WallOffset returns the offset set with SetWallOffset.
func (c *FakeClock) WallOffset() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

// SetWallOffset simulates the wall clock moving against the monotonic clock (a sleep or a step).
func (c *FakeClock) SetWallOffset(d time.Duration) {
	c.mu.Lock()
	c.offset = d
	c.mu.Unlock()
}
