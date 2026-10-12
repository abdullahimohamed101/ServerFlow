package gateway

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Breaker transitions are queued and delivered by request goroutines and refreshes. At shutdown the ones still
// queued are delivered, and the ones dropped because the queue was full are counted and logged; none is lost
// silently, and no goroutine is started for it.
func TestQueuedTransitionsAreDeliveredAtShutdownAndDropsAreLogged(t *testing.T) {
	good := newScriptWorker(t, "a-1", okJSON("a-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")}, func(c *config.Config) { c.Gateway.ShutdownTimeout = time.Second })
	var seen atomic.Int64
	WithObserver(&countingObserver{n: &seen})(e.gw)
	e.rs.models.Store(&map[string]string{"a-1": "qwen-7b"})
	// Queue more transitions than the bound without any request or refresh to deliver them.
	total := maxPendingChanges + 25
	for i := 0; i < total; i++ {
		e.rs.push(resilience.Change{Key: resilience.KeyOf("a-1", time.Time{}), From: resilience.Open, To: resilience.HalfOpen, Reason: resilience.ReasonCoolDown})
	}
	if e.rs.dropped.Load() != 25 {
		t.Fatalf("dropped %d, want 25", e.rs.dropped.Load())
	}
	before := runtime.NumGoroutine()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.gw.Serve(ctx, ln) }()
	eventually(t, 3*time.Second, func() bool { return e.gw.router.cache != nil }, "serving")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// A refresh that was delivering when the flush at shutdown ran finishes the batch itself (one deliverer at a time).
	eventually(t, 5*time.Second, func() bool { return seen.Load() == maxPendingChanges }, "every queued transition is delivered")
	time.Sleep(50 * time.Millisecond)
	if got := seen.Load(); got != maxPendingChanges {
		t.Fatalf("%d transitions delivered, want exactly the %d that were queued", got, maxPendingChanges)
	}
	if !strings.Contains(e.logs.String(), "transitions were dropped") || !strings.Contains(e.logs.String(), `"dropped":25`) {
		t.Fatal("the dropped transitions must be logged with their count")
	}
	if g := runtime.NumGoroutine(); g > before+4 { // Serve's own goroutines have ended; delivery started none
		t.Fatalf("goroutines %d, before %d", g, before)
	}
}

// A slow BreakerTransition observer delays only the request goroutine that happened to deliver: another request,
// meanwhile, is not held (the delivery is single-file, the others return at once and leave their changes to it).
func TestASlowTransitionObserverDelaysOnlyTheDeliveringRequest(t *testing.T) {
	good := newScriptWorker(t, "a-1", okJSON("a-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	WithObserver(&callbackObserver{fn: func() {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}})(e.gw)
	e.rs.push(resilience.Change{Key: resilience.KeyOf("a-1", time.Time{}), From: resilience.Closed, To: resilience.Open, Reason: resilience.ReasonConsecutive})
	slow := make(chan int, 1)
	go func() { slow <- e.do(t) }() // this request's routing flush picks the change up and sits in the observer
	<-entered
	start := time.Now()
	if st := e.do(t); st != 200 { // a second request is not held by the first one's delivery
		t.Fatalf("status %d", st)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("the second request waited %v", time.Since(start))
	}
	close(release)
	if st := <-slow; st != 200 {
		t.Fatalf("status %d", st)
	}
}

type countingObserver struct {
	NopObserver
	n *atomic.Int64
}

func (c *countingObserver) BreakerTransition(context.Context, BreakerChange) { c.n.Add(1) }
