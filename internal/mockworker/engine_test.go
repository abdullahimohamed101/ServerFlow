package mockworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustAcquire(t *testing.T, e *Engine, tokens int) func() {
	t.Helper()
	rel, err := e.Acquire(context.Background(), tokens)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	return rel
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestEngineRunsUpToMaxActive(t *testing.T) {
	e := NewEngine(2, 0)
	r1 := mustAcquire(t, e, 1)
	r2 := mustAcquire(t, e, 1)
	if _, err := e.Acquire(context.Background(), 1); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("with no queue a third request must be rejected, got %v", err)
	}
	if s := e.Stats(); s.Active != 2 || s.Rejected != 1 || s.QueueDepth != 0 {
		t.Fatalf("unexpected stats %+v", s)
	}
	r1()
	r2()
	if s := e.Stats(); s.Active != 0 {
		t.Fatalf("slots not freed: %+v", s)
	}
}

func TestEngineServesTheQueueInArrivalOrder(t *testing.T) {
	e := NewEngine(1, 3)
	first := mustAcquire(t, e, 10)

	var mu sync.Mutex
	var order []int
	releases := make(chan func(), 3)
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := e.Acquire(context.Background(), i*10)
			if err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			releases <- rel
		}()
		// Start the next waiter only once this one is queued, fixing arrival order.
		n := i
		waitFor(t, func() bool { return e.Stats().QueueDepth == n }, "waiter to queue")
	}
	if s := e.Stats(); s.QueuedInputTokens != 10+20+30 {
		t.Fatalf("queued tokens %d, want 60", s.QueuedInputTokens)
	}

	first()
	for i := 0; i < 3; i++ {
		select {
		case rel := <-releases:
			rel()
		case <-time.After(2 * time.Second):
			t.Fatal("a queued request was never granted a slot")
		}
	}
	wg.Wait()
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("queue was not served in arrival order: %v", order)
	}
	if s := e.Stats(); s.Active != 0 || s.QueueDepth != 0 || s.QueuedInputTokens != 0 {
		t.Fatalf("engine not idle after the queue drained: %+v", s)
	}
}

func TestEngineRejectsWhenQueueIsFull(t *testing.T) {
	e := NewEngine(1, 1)
	mustAcquire(t, e, 1)
	go func() { _, _ = e.Acquire(context.Background(), 5) }()
	waitFor(t, func() bool { return e.Stats().QueueDepth == 1 }, "one waiter queued")
	if _, err := e.Acquire(context.Background(), 1); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	if e.Stats().QueueDepth != 1 {
		t.Fatal("a rejected request must not enter the queue")
	}
}

func TestEngineCancelledWaiterLeavesTheQueue(t *testing.T) {
	e := NewEngine(1, 3)
	first := mustAcquire(t, e, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := e.Acquire(ctx, 7); errc <- err }()
	waitFor(t, func() bool { return e.Stats().QueueDepth == 1 }, "waiter queued")

	// A second waiter behind it must still get the slot.
	got := make(chan struct{})
	go func() {
		rel, err := e.Acquire(context.Background(), 9)
		if err == nil {
			close(got)
			rel()
		}
	}()
	waitFor(t, func() bool { return e.Stats().QueueDepth == 2 }, "second waiter queued")

	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if s := e.Stats(); s.QueueDepth != 1 || s.QueuedInputTokens != 9 || s.Cancelled != 1 {
		t.Fatalf("cancelled waiter was not removed cleanly: %+v", s)
	}
	first()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter behind a cancelled one never got the slot")
	}
}

func TestEngineNeverLeaksASlotWhenCancelRacesGrant(t *testing.T) {
	for i := 0; i < 500; i++ {
		e := NewEngine(1, 1)
		first := mustAcquire(t, e, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			if rel, err := e.Acquire(ctx, 1); err == nil {
				rel()
			}
		}()
		waitFor(t, func() bool { return e.Stats().QueueDepth == 1 }, "waiter queued")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); first() }()
		go func() { defer wg.Done(); cancel() }()
		wg.Wait()
		<-done
		if s := e.Stats(); s.Active != 0 || s.QueueDepth != 0 || s.QueuedInputTokens != 0 {
			t.Fatalf("iteration %d leaked state after a grant/cancel race: %+v", i, s)
		}
	}
}

func TestEngineReleaseIsIdempotent(t *testing.T) {
	e := NewEngine(2, 0)
	r1 := mustAcquire(t, e, 1)
	r2 := mustAcquire(t, e, 1)
	r1()
	r1()
	r1()
	if s := e.Stats(); s.Active != 1 {
		t.Fatalf("a repeated release freed someone else's slot: %+v", s)
	}
	r2()
	if s := e.Stats(); s.Active != 0 {
		t.Fatalf("got %+v", s)
	}
}

func TestEngineCountsOutcomes(t *testing.T) {
	e := NewEngine(1, 0)
	e.RecordOutcome(true)
	e.RecordOutcome(true)
	e.RecordOutcome(false)
	if s := e.Stats(); s.Completed != 2 || s.Failed != 1 {
		t.Fatalf("got %+v", s)
	}
}

// steady feeds a fake-clock engine tokensPerSecond tokens/s for the given
// duration in 10ms steps and returns the engine and the clock.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func feed(e *Engine, c *fakeClock, tokensPerSecond float64, d time.Duration, check func(elapsed time.Duration)) {
	const step = 10 * time.Millisecond
	perStep := tokensPerSecond * step.Seconds()
	carry := 0.0
	for elapsed := time.Duration(0); elapsed < d; elapsed += step {
		carry += perStep
		if n := int(carry); n > 0 {
			e.RecordTokens(n)
			carry -= float64(n)
		}
		c.t = c.t.Add(step)
		if check != nil {
			check(elapsed + step)
		}
	}
}

func near(t *testing.T, got, want, tolPct float64, what string) {
	t.Helper()
	if got < want*(1-tolPct/100) || got > want*(1+tolPct/100) {
		t.Fatalf("%s: got %.1f, want %.1f +/-%.0f%%", what, got, want, tolPct)
	}
}

func TestThroughputIsAccurateAtSteadyStateAndWhileRampingUp(t *testing.T) {
	c := &fakeClock{t: time.Unix(1_000_000, 0).Add(370 * time.Millisecond)} // start mid-second on purpose
	e := newEngine(1, 0, c.now)
	if r := e.Stats().RecentTokensPerSecond; r != 0 {
		t.Fatalf("no tokens yet, got %v", r)
	}
	// Check the reading continuously: it must be close to 100 from the first
	// moments and never sag as wall-clock seconds roll over (the old
	// whole-second window swung between about 50 and 100).
	feed(e, c, 100, 12*time.Second, func(elapsed time.Duration) {
		if elapsed >= 300*time.Millisecond {
			near(t, e.Stats().RecentTokensPerSecond, 100, 12, fmt.Sprintf("reading %v into steady load", elapsed))
		}
	})
}

func TestThroughputRecoversAfterAnIdleGap(t *testing.T) {
	c := &fakeClock{t: time.Unix(2_000_000, 0)}
	e := newEngine(1, 0, c.now)
	feed(e, c, 100, 2*time.Second, nil)
	c.t = c.t.Add(30 * time.Second) // a long idle gap
	if r := e.Stats().RecentTokensPerSecond; r != 0 {
		t.Fatalf("an idle worker must read 0, got %v", r)
	}
	// When work resumes the reading must follow it, not stay diluted by the
	// stale start of the previous burst.
	feed(e, c, 100, 1500*time.Millisecond, nil)
	near(t, e.Stats().RecentTokensPerSecond, 100, 12, "1.5s after resuming from idle")
}

func TestThroughputAgesOutAndAveragesOnlyTheWindow(t *testing.T) {
	c := &fakeClock{t: time.Unix(3_000_000, 0)}
	e := newEngine(1, 0, c.now)
	feed(e, c, 100, 12*time.Second, nil) // far more history than the 5s window
	near(t, e.Stats().RecentTokensPerSecond, 100, 5, "after 12s of steady load")

	// Load stops: the reading drains to 0 as the window empties, never rising.
	prev := e.Stats().RecentTokensPerSecond
	for i := 0; i < 70; i++ {
		c.t = c.t.Add(100 * time.Millisecond)
		got := e.Stats().RecentTokensPerSecond
		if got > prev+0.001 {
			t.Fatalf("the reading rose from %.1f to %.1f with no new tokens", prev, got)
		}
		prev = got
	}
	if prev != 0 {
		t.Fatalf("after the window passed the reading must be 0, got %v", prev)
	}
}

func TestThroughputTracksALoadChange(t *testing.T) {
	c := &fakeClock{t: time.Unix(4_000_000, 0)}
	e := newEngine(1, 0, c.now)
	feed(e, c, 100, 6*time.Second, nil)
	feed(e, c, 300, 6*time.Second, nil)
	near(t, e.Stats().RecentTokensPerSecond, 300, 5, "after the load tripled")
}
