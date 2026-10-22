package gateway

import (
	"errors"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/ratelimit"
	"serverflow/pkg/protocol"
)

// Phase 15 D5 (acceptance 7, 20): the retry budget at the gateway. The library tests the arithmetic; these tests
// pin where the gateway checks it, reserves, refunds and reports.

func noBreaker(c *config.Config) {
	c.Resilience.Breaker.ConsecutiveFailures = 100000
	c.Resilience.Breaker.MinVolume = 100000
}

func budget(min int, ratio float64) func(*config.Config) {
	return func(c *config.Config) { c.Resilience.Retry.BudgetMin, c.Resilience.Retry.BudgetRatio = min, ratio }
}

func failingAndGood(t *testing.T) []protocol.WorkerSnapshot {
	t.Helper()
	a := newScriptWorker(t, "a-1", openAPI503("mock_failure"))
	b := newScriptWorker(t, "b-2", okJSON("b-2"))
	return []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}
}

// A retry is allowed up to the budget and then denied: the denied one fires no AttemptStarted, the failed
// worker's response is relayed, the attempts header stays absent, and the first attempt reports why.
func TestADeniedRetryFiresNoAttemptAndRelaysTheFailedResponse(t *testing.T) {
	e := newResEnv(t, failingAndGood(t), noBreaker, budget(1, 0))
	resp, _ := e.chat(t, "qwen-7b")
	if resp.StatusCode != 200 || resp.Header.Get(headerAttempts) != "2" {
		t.Fatalf("the first retry is within the budget: %d attempts=%q", resp.StatusCode, resp.Header.Get(headerAttempts))
	}
	e.rec.await(t)
	startsBefore := len(e.rec.attemptStarts())

	e.rec = &resRec{}
	WithObserver(e.rec)(e.gw)
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "mock_failure" {
		t.Fatalf("a denied retry relays the worker's own 503: %d %s", resp.StatusCode, body)
	}
	if h := resp.Header.Get(headerAttempts); h != "" {
		t.Fatalf("the attempts header is %q: only one attempt ran", h)
	}
	wantSeq(t, e.rec.await(t), "start", "admit", "attempt+1:a-1", "attempt-1:a-1:failed", "done:503")
	ends := e.rec.attemptEnds()
	if len(ends) != 1 || ends[0].RetryDenied != RetryDeniedBudget || ends[0].WillRetry || ends[0].NextWorkerUnavailable || ends[0].NextRouteResult != "" {
		t.Fatalf("AttemptEnd %+v", ends)
	}
	_ = startsBefore
	if _, retries := e.rs.budget.Usage("qwen-7b"); retries != 1 {
		t.Fatalf("retries in the window: %d, the denial consumes nothing", retries)
	}
	want(t, e.gw.metrics, 1, "retries_denied_total", "model=qwen-7b", "reason=budget")
}

// The budget is checked before the retry is routed (D5): with no budget at all a denial is reported even when a
// healthy worker exists, and the routing is never attempted (no scheduling decision for the retry).
func TestTheBudgetIsCheckedBeforeTheRetryIsRouted(t *testing.T) {
	e := newResEnv(t, failingAndGood(t), noBreaker, budget(0, 0))
	resp, _ := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	e.rec.await(t)
	ends := e.rec.attemptEnds()
	if len(ends) != 1 || ends[0].RetryDenied != RetryDeniedBudget || ends[0].NextRouteResult != "" {
		t.Fatalf("%+v", ends)
	}
	if n := e.gw.router.InFlight("b-2"); n != 0 {
		t.Fatalf("a slot was reserved on the other worker: %d", n)
	}
	if got, _ := sample(t, e.gw.metrics, "scheduler_decisions_total", "result=selected"); got != 1 {
		t.Fatalf("scheduler decisions %v: only the first routing counts", got)
	}
	if _, ok := sample(t, e.gw.metrics, "scheduler_decisions_total", "result=no_capacity"); ok {
		t.Fatal("the retry was routed")
	}
}

// A retry for which no other worker exists gives its budget back, so only a retry whose send begins stays
// consumed.
func TestARetryWithNoOtherWorkerRefundsTheBudget(t *testing.T) {
	only := newScriptWorker(t, "only", openAPI503("mock_failure"))
	e := newResEnv(t, []protocol.WorkerSnapshot{only.snapshot("qwen-7b")}, noBreaker, budget(1, 0))
	for i := 0; i < 5; i++ {
		resp, _ := e.chat(t, "qwen-7b")
		if resp.StatusCode != 503 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	if f, r := e.rs.budget.Usage("qwen-7b"); r != 0 || f != 5 {
		t.Fatalf("%d first attempts, %d retries: every reserved retry was refunded", f, r)
	}
	for _, a := range e.rec.attemptEnds() {
		if a.RetryDenied != "" || a.NextRouteResult != NextRouteNoCapacity {
			t.Fatalf("no worker is not a budget denial: %+v", a)
		}
	}
}

// First attempts are routed Number-1 attempts only: a refused request counts nothing.
func TestOnlyRoutedFirstAttemptsCountTowardsTheBudget(t *testing.T) {
	good := newScriptWorker(t, "a-1", okJSON("a-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	e.chat(t, "no-such-model")       // refused before routing
	e.chat(t, "qwen-7b")             // routed
	e.chat(t, "qwen-7b")             // routed
	e.gw.router.cache.cur.Store(nil) // no snapshot: nothing is routed
	resp, _ := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if f, r := e.rs.budget.Usage("qwen-7b"); f != 2 || r != 0 {
		t.Fatalf("%d first attempts, %d retries; want 2 and 0", f, r)
	}
}

// The window rolls: after it has passed a denied retry is allowed again.
func TestTheBudgetWindowRollsWithTheClock(t *testing.T) {
	e := newResEnv(t, failingAndGood(t), noBreaker, budget(1, 0))
	e.chat(t, "qwen-7b")
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 503 {
		t.Fatalf("second request: %d", resp.StatusCode)
	}
	e.advance(t, 11*time.Second)
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("after the window: %d", resp.StatusCode)
	}
}

// Concurrent requests cannot overshoot the budget, and against a worker that always fails the attempts per request
// stay within 1 + ratio plus the minimum allowance (amplification, acceptance 7).
func TestConcurrentRetriesNeverOvershootTheBudget(t *testing.T) {
	a := newScriptWorker(t, "a-1", openAPI503("mock_failure"))
	b := newScriptWorker(t, "b-2", openAPI503("mock_failure"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, noBreaker, budget(5, 0.2))
	const reqs = 120
	var wg sync.WaitGroup
	for i := 0; i < reqs; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.chat(t, "qwen-7b") }()
	}
	wg.Wait()
	e.rec.await(t)
	allIdle(t, e.regEnv, "a-1", "b-2")
	retries := 0
	for _, s := range e.rec.attemptStarts() {
		if s.Number > 1 {
			retries++
		}
	}
	limit := max(5, int(0.2*float64(reqs)))
	if retries > limit {
		t.Fatalf("%d retries for %d requests, the budget allows %d", retries, reqs, limit)
	}
	if retries < 5 {
		t.Fatalf("only %d retries: the minimum allowance should have been used", retries)
	}
	if total := a.hits.Load() + b.hits.Load(); total > int64(reqs+limit) {
		t.Fatalf("%d attempts for %d requests: amplification above 1 + ratio plus the minimum", total, reqs)
	}
}

// RATE_LIMIT_UNAVAILABLE (and every refusal before routing) never records a breaker verdict or takes budget
// (acceptance 20).
func TestRateLimitUnavailableTouchesNeitherBreakersNorBudget(t *testing.T) {
	flag, _, _, ws := twoWorkers(t)
	_ = flag
	e := newResEnv(t, ws)
	lim := &fakeLimiter{}
	lim.decide = func(ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{}, errors.New("redis down")
	}
	e.gw.SetLimiter(lim)
	e.gw.limitRequired = true
	e.gw.cfg.Models = nil
	for i := 0; i < 10; i++ {
		resp, body := e.chat(t, "qwen-7b")
		if resp.StatusCode != 503 || errorCode(t, body) != "RATE_LIMIT_UNAVAILABLE" {
			t.Fatalf("%d %s", resp.StatusCode, body)
		}
	}
	if f, r := e.rs.budget.Usage("qwen-7b"); f != 0 || r != 0 {
		t.Fatalf("budget touched: %d %d", f, r)
	}
	if n := len(e.rs.breakers.Snapshot()); n != 0 {
		t.Fatalf("%d breakers not closed", n)
	}
	if len(e.rec.attemptStarts()) != 0 {
		t.Fatal("no attempt may start for a request the limiter refused")
	}
	if e.rs.peeks.Load() != 0 || e.rs.nonClosed.Load() != 0 {
		t.Fatal("resilience state moved")
	}
}
