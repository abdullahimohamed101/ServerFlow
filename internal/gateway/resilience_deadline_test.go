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
	if len(ends) != 1 || ends[0].Class != classDeadline || ends[0].Verdict != VerdictNeutral || ends[0].WillRetry {
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
	slow := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(260 * time.Millisecond) // headers just inside the 300 ms header timeout
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(260 * time.Millisecond) // first byte just inside the 300 ms idle timeout
		_, _ = io.WriteString(w, "data: {\"i\":0}\n\ndata: [DONE]\n\n")
	})
	e := newResEnv(t, []protocol.WorkerSnapshot{slow.snapshot("qwen-7b")}, func(c *config.Config) {
		c.Gateway.UpstreamHeaderTimeout, c.Gateway.UpstreamIdleTimeout = 300*time.Millisecond, 300*time.Millisecond
	})
	if want := 600*time.Millisecond + workerDialTimeout; e.rs.firstByte != want {
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
		c.Resilience.Breaker.ProbeHeaderTimeout = 150 * time.Millisecond
	})
	for i := 0; i < 3; i++ {
		e.do(t)
	}
	flag.set(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond) // the first byte comes after the probe header timeout: that timer is over
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
