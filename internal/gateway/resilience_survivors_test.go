package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Tests added after an independent mutation pass: each pins a behaviour a hand mutant had survived.

// Only routed attempts with Number 1 count as first attempts; a retry does not inflate the budget.
func TestRetriesDoNotCountAsFirstAttempts(t *testing.T) {
	e := newResEnv(t, failingAndGood(t), noBreaker, budget(1000, 0))
	for i := 0; i < 10; i++ {
		if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 200 || resp.Header.Get(headerAttempts) != "2" {
			t.Fatalf("request %d: %d", i, resp.StatusCode)
		}
	}
	if f, r := e.rs.budget.Usage("qwen-7b"); f != 10 || r != 10 {
		t.Fatalf("%d first attempts and %d retries for 10 requests that each needed a retry", f, r)
	}
}

// The ejection cap divides by the workers the registry would route to, not by every worker it lists.
func TestSyncCountsOnlyRegistryEligibleWorkersTowardsTheCap(t *testing.T) {
	var ws []protocol.WorkerSnapshot
	for _, id := range []string{"w-1", "w-2", "w-3", "w-4"} {
		w := newScriptWorker(t, id, okJSON(id)).snapshot("qwen-7b")
		if id == "w-3" || id == "w-4" {
			w.State, w.Eligible = protocol.StateDraining, false
		}
		ws = append(ws, w)
	}
	e := newResEnv(t, ws, func(c *config.Config) { c.Resilience.Breaker.ConsecutiveFailures = 1 })
	if el, _, _ := e.rs.breakers.Counts(); el != 2 {
		t.Fatalf("%d eligible workers, want 2 (two are draining)", el)
	}
	for _, id := range []string{"w-1", "w-2"} {
		if tk, ok := e.rs.breakers.Acquire(e.key(id)); ok {
			e.rs.breakers.Record(tk, resilience.Fault)
		}
	}
	if _, open, _ := e.rs.breakers.Counts(); open != 1 {
		t.Fatalf("%d of 2 eligible workers open, the 50%% cap allows 1", open)
	}
}

// Delivery is single-file: concurrent flushers deliver every queued change once and in the order it was queued.
func TestConcurrentFlushersDeliverEveryTransitionOnceAndInOrder(t *testing.T) {
	good := newScriptWorker(t, "a-1", okJSON("a-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	var mu sync.Mutex
	var got []int
	WithObserver(&transitionFuncObserver{fn: func(c BreakerChange) {
		time.Sleep(20 * time.Microsecond) // a slow observer widens the window for an overlapping deliverer
		mu.Lock()
		got = append(got, c.Failures)
		mu.Unlock()
	}})(e.gw)
	const n = 400
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					e.rs.flush()
				}
			}
		}()
	}
	for i := 1; i <= n; i++ {
		e.rs.push(resilience.Change{Key: resilience.KeyOf("a-1", time.Time{}), From: resilience.Closed, To: resilience.Open, Failures: i})
		if i%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	eventually(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) >= n }, "all delivered")
	close(stop)
	wg.Wait()
	e.rs.flush()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("%d deliveries for %d transitions", len(got), n)
	}
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("delivery %d carried transition %d: out of order or duplicated", i, v)
		}
	}
}

type transitionFuncObserver struct {
	NopObserver
	fn func(BreakerChange)
}

func (f *transitionFuncObserver) BreakerTransition(_ context.Context, e BreakerChange) { f.fn(e) }

// The error code is read only from error.code; a top-level "code" is not a worker's error shape and is ignored.
func TestErrorCodeIsReadOnlyFromTheErrorObject(t *testing.T) {
	for in, want := range map[string]string{
		`{"error":{"code":"queue_full"}}`:                    "queue_full",
		`{"code":"queue_full"}`:                              "",
		`{"message":"x","code":"draining","error":{}}`:       "",
		`{"error":{"message":"m","code":"not_ready","x":1}}`: "not_ready",
		`{"error":"queue_full"}`:                             "",
		`[{"error":{"code":"queue_full"}}]`:                  "",
		``:                                                   "",
	} {
		if got := errorCodeOf([]byte(in)); got != want {
			t.Errorf("errorCodeOf(%q) = %q, want %q", in, got, want)
		}
	}
}

// A model that leaves the registry loses its budget window at the next refresh (no slow leak of per-model state).
func TestTheBudgetOfAModelThatLeftTheRegistryIsPruned(t *testing.T) {
	a := newScriptWorker(t, "a-1", okJSON("a-1"))
	b := newScriptWorker(t, "b-1", okJSON("b-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("llama-8b")})
	e.chat(t, "qwen-7b")
	e.chat(t, "llama-8b")
	if f, _ := e.rs.budget.Usage("llama-8b"); f != 1 {
		t.Fatalf("setup: %d", f)
	}
	e.lister.set([]protocol.WorkerSnapshot{a.snapshot("qwen-7b")}, nil)
	e.refresh(t)
	if f, r := e.rs.budget.Usage("llama-8b"); f != 0 || r != 0 {
		t.Fatalf("the window of a model nobody serves is still held: %d %d", f, r)
	}
	if f, _ := e.rs.budget.Usage("qwen-7b"); f != 1 {
		t.Fatalf("the served model's window must be kept: %d", f)
	}
}

// The probe counter maps the verdict as it is: ok, fault, and neutral (also for a probe that had no verdict).
func TestTheProbeCounterKeepsNeutralApartFromFault(t *testing.T) {
	m := registryMetrics("round-robin")
	m.enableResilience(newResilienceState(config.Default(), nil, resilienceDeps{}))
	for _, v := range []string{VerdictNeutral, "", VerdictNeutral, VerdictOK, VerdictFault} {
		m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w", Model: "qwen-7b", Outcome: AttemptOK, Probe: true, Verdict: v})
	}
	want(t, m, 3, "circuit_breaker_probes_total", "model=qwen-7b", "result=neutral")
	want(t, m, 1, "circuit_breaker_probes_total", "model=qwen-7b", "result=ok")
	want(t, m, 1, "circuit_breaker_probes_total", "model=qwen-7b", "result=fault")
}

// The attempt log line carries the verdict, the probe flag and the denial.
func TestTheAttemptLogLineCarriesVerdictProbeAndDenial(t *testing.T) {
	e := newResEnv(t, failingAndGood(t), noBreaker, budget(0, 0))
	e.chat(t, "qwen-7b")
	e.rec.await(t)
	var line map[string]any
	for _, l := range e.attemptLines(t) {
		line = l
	}
	if line["verdict"] != "fault" || line["retry_denied"] != "budget" || line["outcome"] != "failed" {
		t.Fatalf("attempt line %v", line)
	}
}

// With a peek wait far above the time left, the peek is cut at the deadline, not held for its own bound.
func TestThePeekIsClampedToTheTimeLeftForTheFirstByte(t *testing.T) {
	a := newScriptWorker(t, "a-1", stall503)
	b := newScriptWorker(t, "b-2", okJSON("b-2"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, noBreaker, deadline(500*time.Millisecond),
		func(c *config.Config) { c.Resilience.Retry.MinRemaining = 50 * time.Millisecond })
	e.rs.peekWait = 30 * time.Second
	start := time.Now()
	e.chat(t, "qwen-7b")
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v: the peek ran for its own bound instead of the time left", took)
	}
}

// A deadline that fired while the client was leaving is the client's departure, not a 504 for the observers.
func TestADeadlineThatFiredForAClientThatLeftIsNotMarked(t *testing.T) {
	good := newScriptWorker(t, "a-1", okJSON("a-1"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	run := &attemptRun{target: &routed{worker: good.snapshot("qwen-7b")}}
	run.guard = newPhaseGuard(func() {}, 0, 0)
	eventually(t, 2*time.Second, func() bool { run.guard.mu.Lock(); defer run.guard.mu.Unlock(); return run.guard.deadlineHit }, "deadline fired")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("")).WithContext(ctx)
	e.gw.endPhase(r, run, &reqInfo{})
	if run.class == classDeadline || run.err != nil {
		t.Fatalf("a client that left must not become a deadline expiry: class %q err %v", run.class, run.err)
	}
	r2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	run2 := &attemptRun{target: &routed{worker: good.snapshot("qwen-7b")}, guard: newPhaseGuard(func() {}, 0, 0)}
	eventually(t, 2*time.Second, func() bool { run2.guard.mu.Lock(); defer run2.guard.mu.Unlock(); return run2.guard.deadlineHit }, "deadline fired")
	e.gw.endPhase(r2, run2, &reqInfo{})
	if run2.class != classDeadline || run2.retryable || run2.err == nil {
		t.Fatalf("a live client gets the deadline: class %q retryable %v err %v", run2.class, run2.retryable, run2.err)
	}
}

// Static mode never has probes.
func TestStaticModeAttemptsAreNeverProbes(t *testing.T) {
	rec := &recObserver{}
	env := newEnv(t, okUpstreamHandler())
	WithObserver(rec)(env.gw)
	resp := env.post(t, `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`)
	_, _ = io.Copy(io.Discard, resp.Body)
	rec.await(t)
	if len(rec.att) != 1 || rec.att[0].Probe || rec.att[0].QueueWait != 0 {
		t.Fatalf("%+v", rec.att)
	}
	for _, e := range rec.ends {
		if e.Probe || e.Verdict != "" || e.RetryDenied != "" {
			t.Fatalf("%+v", e)
		}
	}
}

func okUpstreamHandler() http.Handler { return http.HandlerFunc(okUpstream) }
