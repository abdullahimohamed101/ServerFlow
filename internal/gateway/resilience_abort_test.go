package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Phase 15 PR D, second fix round: what an aborted stream reports, that the transition it causes is delivered
// at once, and the dropped-count reset.

func abortAfterFirstEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	_, _ = io.WriteString(w, "data: {\"i\":0}\n\n")
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}

// A stream that breaks after its first byte is an ok when its first byte arrives and a fault when it breaks, and
// AttemptEnd says what the breaker was finally told: fault, with outcome failed. The transition the abort causes
// (here the ratio rule opening the breaker) is in the observers' hands by the time the attempt has ended: the flush
// after recordAfterRelay delivers it, no later request or refresh is needed.
func TestAnAbortedStreamEndsWithAFaultVerdictAndItsTransitionIsDeliveredAtOnce(t *testing.T) {
	flag := &workerFlag{}
	flag.set(sseEvents("a-1", 2))
	a := newScriptWorker(t, "a-1", flag.serve)
	b := newScriptWorker(t, "b-2", sseEvents("b-2", 2))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, func(c *config.Config) {
		c.Resilience.Breaker.ConsecutiveFailures = 100000
		c.Resilience.Breaker.MinVolume = 4
	})
	for i := 0; i < 4; i++ {
		e.postLoose(true) // a healthy history: two outcomes on each worker
	}
	e.settle(t, "a-1", "b-2")
	flag.set(abortAfterFirstEvent)
	// The second abort is the one that trips the ratio rule (2 faults of 4 outcomes), and it does so when the ok
	// of its first byte is converted: not at the ok, which is recorded and flushed earlier.
	opened := false
	for i := 0; i < 8 && !opened; i++ {
		e.postLoose(true)
		e.settle(t, "a-1", "b-2")
		if e.state("a-1") == resilience.Open {
			opened = true
			// No request or refresh has run since the aborting stream: only its own flush can have delivered this.
			if got := strings.Join(e.rec.states(), ","); !strings.Contains(got, "a-1:open:") {
				t.Fatalf("the breaker is open but the observers have been told %q", got)
			}
		}
	}
	if !opened {
		t.Fatal("the aborting worker was not ejected")
	}
	if n := a.hits.Load(); n != 4 {
		t.Fatalf("the worker saw %d requests, want 2 healthy and 2 aborted", n)
	}
	n := 0
	for _, x := range e.rec.attemptEnds() {
		if x.WorkerID != "a-1" || x.Verdict == VerdictOK && x.Outcome == AttemptOK {
			continue
		}
		n++
		if x.Verdict != VerdictFault || x.Outcome != AttemptFailed || x.Probe {
			t.Fatalf("an aborted stream ends with verdict fault and outcome failed: %+v", x)
		}
	}
	if n != 2 {
		t.Fatalf("%d aborted attempts ended, want 2", n)
	}
}

// The count of dropped transitions is logged once and reset at the flush: later flushes do not repeat it.
func TestTheDroppedCountIsResetAfterItIsLogged(t *testing.T) {
	good := newScriptWorker(t, "a-1", okJSON("a-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	e.rs.models.Store(&map[string]string{"a-1": "qwen-7b"})
	push := func(n int) {
		for i := 0; i < n; i++ {
			e.rs.push(resilience.Change{Key: resilience.KeyOf("a-1", time.Time{}), From: resilience.Open, To: resilience.HalfOpen, Reason: resilience.ReasonCoolDown})
		}
	}
	push(maxPendingChanges + 7)
	e.rs.flush()
	if got := strings.Count(e.logs.String(), "transitions were dropped"); got != 1 || !strings.Contains(e.logs.String(), `"dropped":7`) {
		t.Fatalf("want one log line with dropped 7, got %d: %s", got, e.logs.String())
	}
	if e.rs.dropped.Load() != 0 {
		t.Fatalf("the count must be reset after it is logged, is %d", e.rs.dropped.Load())
	}
	push(3)
	e.rs.flush()
	e.rs.flush()
	if got := strings.Count(e.logs.String(), "transitions were dropped"); got != 1 {
		t.Fatalf("the dropped count was logged %d times: it must not be repeated at later flushes", got)
	}
	// A new overflow is counted from zero.
	push(maxPendingChanges + 2)
	e.rs.flush()
	if !strings.Contains(e.logs.String(), `"dropped":2`) || strings.Count(e.logs.String(), "transitions were dropped") != 2 {
		t.Fatalf("a second overflow is logged on its own with its own count: %s", e.logs.String())
	}
}
