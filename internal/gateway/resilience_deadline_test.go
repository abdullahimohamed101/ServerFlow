package gateway

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Phase 15 D6 (acceptance 8): the first-byte deadline bounds the retryable phase only.

func deadline(d time.Duration) func(*config.Config) {
	return func(c *config.Config) { c.Resilience.Retry.FirstByteDeadline = d }
}

// An attempt still waiting for its first byte when the deadline passes is cut: 504 UPSTREAM_TIMEOUT, class
// deadline, neutral for the breaker (the request ran out of time, the worker was not proven faulty).
func TestTheFirstByteDeadlineCutsAWaitingAttempt(t *testing.T) {
	hung := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) { hold(r) })
	e := newResEnv(t, []protocol.WorkerSnapshot{hung.snapshot("qwen-7b")}, deadline(150*time.Millisecond))
	start := time.Now()
	resp, body := e.chat(t, "qwen-7b")
	took := time.Since(start)
	if resp.StatusCode != 504 || errorCode(t, body) != "UPSTREAM_TIMEOUT" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if took < 100*time.Millisecond || took > 3*time.Second {
		t.Fatalf("took %v for a 150ms deadline", took)
	}
	e.settle(t, "a-1")
	ends := e.rec.attemptEnds()
	if len(ends) != 1 || ends[0].Class != classDeadline || ends[0].Verdict != VerdictNeutral || ends[0].WillRetry || ends[0].RetryDenied != "" {
		t.Fatalf("%+v", ends)
	}
	if e.state("a-1") != resilience.Closed || len(e.rs.breakers.Snapshot()) != 0 {
		t.Fatal("a deadline expiry is not a breaker fault")
	}
	want(t, e.gw.metrics, 1, "inference_deadline_exceeded_total", "model=qwen-7b")
}

// No retry starts with less time left than retry.min_remaining; the denial is reported and no attempt starts.
func TestNoRetryStartsWithLessThanTheMinimumRemaining(t *testing.T) {
	e := &resEnv{}
	slow := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) {
		e.clock.Advance(9500 * time.Millisecond) // 9.5 s of the 10 s deadline pass before this worker answers
		openAPI503("mock_failure")(w, r)
	})
	good := newScriptWorker(t, "b-2", okJSON("b-2"))
	*e = *newResEnv(t, []protocol.WorkerSnapshot{slow.snapshot("qwen-7b"), good.snapshot("qwen-7b")}, noBreaker, deadline(10*time.Second),
		func(c *config.Config) { c.Resilience.Retry.MinRemaining = time.Second })
	resp, _ := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || resp.Header.Get(headerAttempts) != "" {
		t.Fatalf("%d attempts=%q: the worker's 503 is relayed", resp.StatusCode, resp.Header.Get(headerAttempts))
	}
	e.rec.await(t)
	ends := e.rec.attemptEnds()
	if len(ends) != 1 || ends[0].RetryDenied != RetryDeniedDeadline || len(e.rec.attemptStarts()) != 1 {
		t.Fatalf("%+v", ends)
	}
	if _, r := e.rs.budget.Usage("qwen-7b"); r != 0 {
		t.Fatalf("a deadline denial reserves no budget: %d", r)
	}
	want(t, e.gw.metrics, 1, "retries_denied_total", "model=qwen-7b", "reason=deadline")
}

// The deadline has no effect after the first byte: a stream that runs far longer than the deadline completes.
func TestTheDeadlineIsIgnoredAfterTheFirstByte(t *testing.T) {
	long := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for i := 0; i < 6; i++ {
			_, _ = io.WriteString(w, "data: {\"i\":"+string(rune('0'+i))+"}\n\n")
			w.(http.Flusher).Flush()
			time.Sleep(80 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	e := newResEnv(t, []protocol.WorkerSnapshot{long.snapshot("qwen-7b")}, deadline(150*time.Millisecond))
	resp, body := e.post(t, true)
	if resp.StatusCode != 200 || !strings.Contains(body, "[DONE]") || strings.Count(body, "data:") != 7 {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	e.settle(t, "a-1")
	if ends := e.rec.attemptEnds(); len(ends) != 1 || ends[0].Outcome != AttemptOK || ends[0].Verdict != VerdictOK {
		t.Fatalf("%+v", ends)
	}
}

// The derived default (header + idle timeouts, plus the dial timeout) never shortens a single attempt that the
// gateway would have served without resilience.
func TestTheDerivedDeadlineNeverCutsASlowButServedAttempt(t *testing.T) {
	// "Just inside" is the contract: each delay is 87% of its timeout. The absolute margin (130 ms) has to be wider than
	// the scheduling delay of a loaded machine, so the timeouts are seconds, not the 300 ms with a 40 ms margin they were.
	const timeout, lag = time.Second, 870 * time.Millisecond
	slow := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(lag) // headers just inside the header timeout
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(lag) // first byte just inside the idle timeout
		_, _ = io.WriteString(w, "data: {\"i\":0}\n\ndata: [DONE]\n\n")
	})
	e := newResEnv(t, []protocol.WorkerSnapshot{slow.snapshot("qwen-7b")}, func(c *config.Config) {
		c.Gateway.UpstreamHeaderTimeout, c.Gateway.UpstreamIdleTimeout = timeout, timeout
	})
	if want := 2*timeout + workerDialTimeout; e.rs.firstByte != want {
		t.Fatalf("derived deadline %v, want header + idle + dial = %v", e.rs.firstByte, want)
	}
	resp, body := e.post(t, true)
	if resp.StatusCode != 200 || !strings.Contains(body, "[DONE]") {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
}

// A probe has its own, shorter header timeout: a blackholed worker costs the probing client the probe header
// timeout, not the gateway's, and the breaker reopens with a doubled cool-down.
func TestAProbeUsesItsOwnHeaderTimeout(t *testing.T) {
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))
	a := newScriptWorker(t, "a-flaky", flag.serve)
	b := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, func(c *config.Config) {
		c.Gateway.UpstreamHeaderTimeout = 5 * time.Second
		c.Resilience.Breaker.ConsecutiveFailures = 1
		c.Resilience.Breaker.ProbeHeaderTimeout = 120 * time.Millisecond
	})
	for i := 0; i < 3; i++ {
		e.do(t)
	}
	if e.state("a-flaky") != resilience.Open {
		t.Fatal("setup: the breaker is not open")
	}
	flag.set(func(w http.ResponseWriter, r *http.Request) { hold(r) })
	e.advance(t, 10*time.Second)
	var status int
	start := time.Now()
	for i := 0; i < 3 && status != 504; i++ {
		status = e.do(t)
	}
	took := time.Since(start)
	if status != 504 {
		t.Fatalf("the probe of a blackholed worker is a 504, got %d", status)
	}
	if took > 3*time.Second {
		t.Fatalf("took %v: the probe header timeout is 120ms, the gateway's is 5s", took)
	}
	e.settle(t, "a-flaky", "b-good")
	if e.state("a-flaky") != resilience.Open {
		t.Fatalf("the failed probe reopens: %v", e.state("a-flaky"))
	}
	ends := e.rec.attemptEnds()
	last := ends[len(ends)-1]
	if !last.Probe || last.Verdict != VerdictFault || last.WillRetry {
		t.Fatalf("%+v", last)
	}
}

// A probe's own header timeout covers the wait for headers only: a probe whose stream is longer than that timeout
// is not cut once the headers have arrived.
func TestTheProbeHeaderTimeoutEndsWithTheHeaders(t *testing.T) {
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))
	a := newScriptWorker(t, "a-flaky", flag.serve)
	b := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, func(c *config.Config) {
		c.Gateway.UpstreamHeaderTimeout = 5 * time.Second
		c.Resilience.Breaker.ConsecutiveFailures = 1
		c.Resilience.Breaker.ProbeHeaderTimeout = 450 * time.Millisecond // the headers arrive at once; this only has to outlast a stalled machine
	})
	for i := 0; i < 3; i++ {
		e.do(t)
	}
	flag.set(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(900 * time.Millisecond) // the first byte comes after the probe header timeout: that timer is over
		for i := 0; i < 5; i++ {
			_, _ = io.WriteString(w, "data: {\"i\":0}\n\n")
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	e.advance(t, 10*time.Second)
	for i := 0; i < 4 && e.state("a-flaky") != resilience.Closed; i++ {
		resp, body := e.post(t, true)
		if resp.StatusCode != 200 || !strings.Contains(body, "[DONE]") {
			t.Fatalf("%d %q", resp.StatusCode, body)
		}
	}
	if e.state("a-flaky") != resilience.Closed {
		t.Fatalf("the probe stream should have closed the breaker: %v", e.state("a-flaky"))
	}
}

// All attempts together stay within the first-byte deadline: a fast failure, then a retry that hangs, ends at the
// deadline (plus a tolerance), not at the header timeout.
func TestAttemptsTogetherNeverOutlastTheDeadline(t *testing.T) {
	a := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		openAPI503("mock_failure")(w, r)
	})
	b := newScriptWorker(t, "b-2", func(w http.ResponseWriter, r *http.Request) { hold(r) })
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, noBreaker, deadline(400*time.Millisecond),
		func(c *config.Config) { c.Resilience.Retry.MinRemaining = 50 * time.Millisecond })
	start := time.Now()
	resp, body := e.chat(t, "qwen-7b")
	took := time.Since(start)
	if resp.StatusCode != 504 || errorCode(t, body) != "UPSTREAM_TIMEOUT" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if took < 350*time.Millisecond || took > 1500*time.Millisecond {
		t.Fatalf("took %v for a 400ms deadline (header timeout is 2s)", took)
	}
	if h := resp.Header.Get(headerAttempts); h != "2" {
		t.Fatalf("attempts %q, want 2", h)
	}
}

// A response that has arrived before the deadline is not turned into a 504 by a deadline that fires in the moments
// before the caller gets to the decision point: the guard decides at the arrival, under one lock.
func TestAResponseThatArrivedBeforeTheDeadlineIsNeverTurnedInto504(t *testing.T) {
	good := newScriptWorker(t, "a-1", sseEvents("a-1", 3))
	// The deadline is wide enough for the response to arrive before it on a loaded machine; the gap is three deadlines.
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")}, deadline(500*time.Millisecond))
	afterArrivalHook = func() { time.Sleep(1500 * time.Millisecond) } // three deadlines pass in the gap
	defer func() { afterArrivalHook = nil }()
	resp, body := e.post(t, true)
	if resp.StatusCode != 200 || !strings.Contains(body, "[DONE]") {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	e.settle(t, "a-1")
	if ends := e.rec.attemptEnds(); len(ends) != 1 || ends[0].Outcome != AttemptOK || ends[0].Class != "" {
		t.Fatalf("%+v", ends)
	}
}

// The guard itself: whichever of the arrival and the timer takes the lock first wins, and the loser has no effect.
func TestPhaseGuardArrivalAndDeadlineAreMutuallyExclusive(t *testing.T) {
	var cancelled atomic.Int32
	g := newPhaseGuard(func() { cancelled.Add(1) }, 40*time.Millisecond, 0)
	if g.end() { // the arrival comes first
		t.Fatal("the deadline cannot have fired yet")
	}
	time.Sleep(120 * time.Millisecond)
	if cancelled.Load() != 0 || g.end() {
		t.Fatalf("a timer that fired after the arrival must be inert: cancels %d", cancelled.Load())
	}
	g2 := newPhaseGuard(func() { cancelled.Add(1) }, 0, 0) // fires at once
	eventually(t, 2*time.Second, func() bool { return cancelled.Load() == 1 }, "the deadline fired")
	if !g2.end() {
		t.Fatal("the deadline came first: the arrival must see it")
	}
}

// Once a response has arrived the first-byte deadline is disarmed by arrived(), on each of the three paths of
// sendOnce that end the retryable phase: a 200 whose first byte is in hand, a status that is not retried, and a
// retryable status. The hook holds the caller between sendOnce and the decision point for three deadlines; if
// arrived() were missing from a path the timer would fire in that gap and the response would become a 504. (The
// older afterArrivalHook sits inside arrived() and so cannot see a path that no longer calls it.)
func TestEveryArrivalPathDisarmsTheDeadlineBeforeTheDecisionPoint(t *testing.T) {
	cases := []struct {
		name   string
		h      func(http.ResponseWriter, *http.Request)
		code   int
		stream bool
	}{
		{"200 stream", sseEvents("a-1", 2), 200, true},
		{"200 non-stream", okJSON("a-1"), 200, false},
		{"non-retryable status", status(400, `{"error":{"message":"bad","type":"invalid_request_error","code":"bad"}}`), 400, false},
		{"retryable status", openAPI503("mock_failure"), 503, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newScriptWorker(t, "a-1", c.h)
			e := newResEnv(t, []protocol.WorkerSnapshot{w.snapshot("qwen-7b")}, noBreaker, deadline(500*time.Millisecond))
			var held atomic.Int32
			beforeEndPhaseHook = func() { held.Add(1); time.Sleep(1500 * time.Millisecond) } // three deadlines
			defer func() { beforeEndPhaseHook = nil }()
			resp, body := e.post(t, c.stream)
			if resp.StatusCode != c.code {
				t.Fatalf("%d %q, want %d: a response that arrived before the deadline must not become a 504", resp.StatusCode, body, c.code)
			}
			if held.Load() == 0 {
				t.Fatal("the hook never ran")
			}
			e.settle(t, "a-1")
			if ends := e.rec.attemptEnds(); len(ends) != 1 || ends[0].Class == classDeadline {
				t.Fatalf("%+v", ends)
			}
		})
	}
}
