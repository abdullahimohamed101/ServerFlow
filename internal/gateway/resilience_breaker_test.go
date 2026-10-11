package gateway

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

// twoWorkers builds a flaky worker "a-flaky" (behaviour switchable) and a healthy "b-good".
func twoWorkers(t *testing.T) (*workerFlag, *scriptWorker, *scriptWorker, []protocol.WorkerSnapshot) {
	t.Helper()
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))
	a := newScriptWorker(t, "a-flaky", flag.serve)
	b := newScriptWorker(t, "b-good", okJSON("b-good"))
	return flag, a, b, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}
}

// A worker that keeps failing is ejected after consecutive_failures faults, receives no more traffic, is probed
// by one live request after the cool-down and closes when the probe passes (D7, D8, D10). Clients never saw a
// failure: every fault was retried on the healthy worker.
func TestTheBreakerEjectsProbesAndClosesAFailingWorker(t *testing.T) {
	flag, a, b, ws := twoWorkers(t)
	e := newResEnv(t, ws)
	for i := 0; i < 8; i++ {
		if st := e.do(t); st != 200 {
			t.Fatalf("request %d: %d (retries hide the faults)", i, st)
		}
	}
	if st := e.state("a-flaky"); st != resilience.Open {
		t.Fatalf("a-flaky is %v after repeated faults, want open", st)
	}
	hits := a.hits.Load()
	if hits != 3 {
		t.Fatalf("the failing worker saw %d requests: 3 faults open it, no more may follow", hits)
	}
	if got := e.rec.states(); len(got) != 1 || got[0] != "a-flaky:open:consecutive_failures" {
		t.Fatalf("transitions %v", got)
	}
	if c := e.rec.transitions()[0]; c.Model != "qwen-7b" || c.Failures != 3 || c.CoolDown != 10*time.Second {
		t.Fatalf("transition detail %+v", c)
	}

	// The cool-down passes; the worker has recovered. The next request that reaches it is a probe.
	flag.set(okJSON("a-flaky"))
	e.advance(t, 10*time.Second)
	probed := false
	for i := 0; i < 4 && !probed; i++ {
		e.do(t)
		for _, s := range e.rec.attemptStarts() {
			probed = probed || (s.Probe && s.WorkerID == "a-flaky")
		}
	}
	if !probed {
		t.Fatal("no probe reached the recovered worker")
	}
	e.settle(t, "a-flaky", "b-good")
	if st := e.state("a-flaky"); st != resilience.Closed {
		t.Fatalf("the probe passed but the breaker is %v", st)
	}
	want := []string{"a-flaky:open:consecutive_failures", "a-flaky:half_open:cool_down_elapsed", "a-flaky:closed:probes_passed"}
	if got := e.rec.states(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("transitions\n got %v\nwant %v", got, want)
	}
	// Only the probe carries Probe, and traffic returns.
	probes := 0
	for _, s := range e.rec.attemptStarts() {
		if s.Probe {
			probes++
		}
	}
	if probes != 1 {
		t.Fatalf("%d probe attempts, want exactly one", probes)
	}
	before := a.hits.Load()
	for i := 0; i < 4; i++ {
		e.do(t)
	}
	if a.hits.Load() == before {
		t.Fatal("traffic did not return to the closed worker")
	}
	_ = b
}

// A failed probe reopens the breaker with a doubled cool-down and counts as a probe fault.
func TestAFailedProbeReopensWithADoubledCoolDown(t *testing.T) {
	_, _, _, ws := twoWorkers(t)
	e := newResEnv(t, ws)
	for i := 0; i < 8; i++ {
		e.do(t)
	}
	e.advance(t, 10*time.Second)
	for i := 0; i < 4; i++ {
		e.do(t) // the probe fails (the worker still answers 503 mock_failure) and is retried on the good worker
	}
	if st := e.state("a-flaky"); st != resilience.Open {
		t.Fatalf("state %v after a failed probe, want open", st)
	}
	var reopened *BreakerChange
	for _, c := range e.rec.transitions() {
		c := c
		if c.Reason == resilience.ReasonProbeFailed {
			reopened = &c
		}
	}
	if reopened == nil || reopened.CoolDown != 20*time.Second {
		t.Fatalf("expected a probe_failed reopening with 20s, got %+v in %v", reopened, e.rec.states())
	}
	var probeEnd *AttemptEnd
	for _, a := range e.rec.attemptEnds() {
		a := a
		if a.Probe {
			probeEnd = &a
		}
	}
	if probeEnd == nil || probeEnd.Verdict != VerdictFault || probeEnd.Outcome != AttemptRetried {
		t.Fatalf("the probe attempt ended %+v", probeEnd)
	}
}

// A probe that gets a neutral 503 (the worker says it is draining) gives its ticket back without closing or
// reopening the breaker, and the next request can probe again.
func TestANeutralProbeReturnsItsTicket(t *testing.T) {
	flag, _, _, ws := twoWorkers(t)
	e := newResEnv(t, ws)
	for i := 0; i < 8; i++ {
		e.do(t)
	}
	flag.set(openAPI503("draining"))
	e.advance(t, 10*time.Second)
	for i := 0; i < 4; i++ {
		e.do(t)
	}
	e.settle(t, "a-flaky", "b-good")
	if st := e.state("a-flaky"); st != resilience.HalfOpen {
		t.Fatalf("state %v: a neutral probe neither closes nor reopens", st)
	}
	probes := 0
	for _, a := range e.rec.attemptEnds() {
		if a.Probe {
			probes++
			if a.Verdict != VerdictNeutral {
				t.Fatalf("probe verdict %q", a.Verdict)
			}
		}
	}
	if probes < 2 {
		t.Fatalf("only %d probes: the ticket was not returned, so no second probe could run", probes)
	}
}

// The ejection cap keeps half of the eligible workers: with two workers failing, only one opens, the other's trip is
// suppressed and counted, and a model with a single worker never opens.
func TestTheEjectionCapKeepsOneOfTwoWorkersAndASingleWorkerNeverOpens(t *testing.T) {
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))
	a := newScriptWorker(t, "a-1", flag.serve)
	b := newScriptWorker(t, "b-2", flag.serve)
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")})
	for i := 0; i < 20; i++ {
		e.do(t)
	}
	open := 0
	for _, id := range []string{"a-1", "b-2"} {
		if e.state(id) == resilience.Open {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("%d of 2 workers open, the cap allows 1", open)
	}
	if e.rs.breakers.Suppressed() == 0 {
		t.Fatal("the trip the cap held back is counted")
	}
	// A suppressed trip is reported to the observers as To closed, reason suppressed, and is not a state change.
	found := false
	for _, c := range e.rec.transitions() {
		found = found || (c.Reason == resilience.ReasonSuppressed && c.To == "closed")
	}
	if !found {
		t.Fatalf("no suppressed notification in %v", e.rec.states())
	}

	solo := newResEnv(t, []protocol.WorkerSnapshot{newScriptWorker(t, "only", flag.serve).snapshot("qwen-7b")})
	for i := 0; i < 20; i++ {
		solo.do(t)
	}
	if st := solo.state("only"); st != resilience.Closed {
		t.Fatalf("a single-worker model opened: %v", st)
	}
}

// With every eligible worker ejected (fraction 1) the gateway answers 503 NO_CAPACITY at once with Retry-After
// the time to the first probe, while /readyz does not look at breakers (D9, D10).
func TestAllBreakersOpenAnswersFastWithRetryAfterAndReadyzIgnoresBreakers(t *testing.T) {
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))
	a := newScriptWorker(t, "a-1", flag.serve)
	b := newScriptWorker(t, "b-2", flag.serve)
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")},
		func(c *config.Config) { c.Resilience.Breaker.MaxOpenFraction = 1 })
	for i := 0; i < 20; i++ {
		e.do(t)
	}
	if e.state("a-1") != resilience.Open || e.state("b-2") != resilience.Open {
		t.Fatalf("both workers should be open: %v %v", e.state("a-1"), e.state("b-2"))
	}
	hitsA, hitsB := a.hits.Load(), b.hits.Load()
	e.clock.Advance(3 * time.Second)
	e.refresh(t)
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "7" {
		t.Fatalf("Retry-After %q: the first breaker is half-open in 7 seconds", ra)
	}
	if a.hits.Load() != hitsA || b.hits.Load() != hitsB {
		t.Fatal("no worker may be touched while every breaker is open")
	}
	if !strings.Contains(body, `"eligible_workers":0`) && !strings.Contains(body, `"eligible_workers": 0`) {
		t.Fatalf("eligible_workers excludes the open workers: %s", body)
	}
	r, _ := e.get(t, "/readyz")
	if r.StatusCode != 200 {
		t.Fatalf("/readyz %d: breakers do not make the gateway unready", r.StatusCode)
	}
}

// A re-registered worker (a new registered_at) starts with a closed breaker; a worker that left the registry loses
// its breaker (D7).
func TestRegistrationChangeResetsAndRemovalPrunesTheBreaker(t *testing.T) {
	flag, _, _, ws := twoWorkers(t)
	for i := range ws {
		ws[i].RegisteredAt = time.Unix(1_600_000_000, 0)
	}
	e := newResEnv(t, ws)
	key := func(id string, at int64) resilience.Key { return resilience.KeyOf(id, time.Unix(at, 0)) }
	for i := 0; i < 8; i++ {
		e.do(t)
	}
	if st := e.rs.breakers.State(key("a-flaky", 1_600_000_000)); st != resilience.Open {
		t.Fatalf("setup: %v", st)
	}
	flag.set(okJSON("a-flaky"))
	re := append([]protocol.WorkerSnapshot(nil), ws...)
	re[0].RegisteredAt = time.Unix(1_600_000_500, 0)
	e.lister.set(re, nil)
	e.refresh(t)
	if st := e.rs.breakers.State(key("a-flaky", 1_600_000_500)); st != resilience.Closed {
		t.Fatalf("a re-registered worker starts closed, got %v", st)
	}
	if n := len(e.rs.breakers.Snapshot()); n != 0 {
		t.Fatalf("the old registration's breaker lingers: %d non-closed", n)
	}
	e.lister.set(ws[1:], nil)
	e.refresh(t)
	if e.rs.nonClosed.Load() != 0 {
		t.Fatalf("nonClosed %d", e.rs.nonClosed.Load())
	}
}

// Aging turns an eligible worker suspect between refreshes; the breakers' cap must see that (D9: the
// denominator is the registry-eligible set in the current, aged view).
func TestAgingUpdatesTheEligibleSetTheCapUses(t *testing.T) {
	_, _, _, ws := twoWorkers(t)
	e := newResEnv(t, ws)
	if el, _, _ := e.rs.breakers.Counts(); el != 2 {
		t.Fatalf("eligible %d", el)
	}
	// 6 s later (suspect after 5 s) without a refresh: the view ages and a routing call re-syncs the breakers.
	e.clock.Advance(6 * time.Second)
	if _, ok := e.gw.router.cache.View("qwen-7b"); !ok {
		t.Fatal("view")
	}
	if el, _, _ := e.rs.breakers.Counts(); el != 0 {
		t.Fatalf("eligible %d after aging, want 0", el)
	}
}

// A lost ticket race is resolved inside Route: the scheduler's choice has its breaker turn unavailable between the
// filter and the Acquire, Route selects again, no observer has been told, and no slot is held for the loser.
func TestALostTicketRaceReselectsInsideRouteBeforeAnyObserverFires(t *testing.T) {
	flag, _, _, ws := twoWorkers(t)
	e := newResEnv(t, ws, func(c *config.Config) { c.Resilience.Breaker.ConsecutiveFailures = 1 })
	// One fault opens a-flaky; after the cool-down it is half-open with a single probe slot.
	for i := 0; i < 4; i++ {
		e.do(t)
	}
	flag.set(okJSON("a-flaky"))
	e.advance(t, 10*time.Second)
	starts := len(e.rec.attemptStarts())
	// The scheduler is wrapped: when it is asked to choose, another request takes the last probe slot of a-flaky
	// first and then the scheduler answers a-flaky anyway (the race between the filter and the Acquire).
	var stolen resilience.Ticket
	e.gw.router.sched = &racingScheduler{inner: e.gw.router.sched, steal: func() {
		tk, ok := e.rs.breakers.Acquire(e.key("a-flaky"))
		if !ok {
			t.Error("the race setup could not take the probe slot")
		}
		stolen = tk
	}, prefer: "a-flaky"}
	req := &protocol.InferenceRequest{Model: "qwen-7b"}
	got, rerr := e.gw.router.Route(context.Background(), req)
	if rerr != nil {
		t.Fatalf("Route: %v", rerr.Error)
	}
	if got.worker.WorkerID != "b-good" {
		t.Fatalf("Route chose %s: it must re-select the healthy worker", got.worker.WorkerID)
	}
	if n := e.gw.router.InFlight("a-flaky"); n != 0 {
		t.Fatalf("a slot was left on the loser: %d", n)
	}
	if len(e.rec.attemptStarts()) != starts {
		t.Fatal("an observer fired during routing")
	}
	got.release()
	e.rs.breakers.Release(stolen)
}

// racingScheduler runs steal before its first selection and then prefers one worker as long as it is listed.
type racingScheduler struct {
	inner  scheduler.Scheduler
	steal  func()
	prefer string
	once   sync.Once
}

func (s *racingScheduler) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	s.once.Do(s.steal)
	for i := range ws {
		if ws[i].WorkerID == s.prefer && ws[i].Eligible {
			w := ws[i]
			return &w, nil
		}
	}
	return s.inner.SelectWorker(ctx, req, ws)
}

// BreakerTransition is delivered outside every router and breaker lock: an observer may call back into the
// router while a transition is being delivered (here from the routing call that caused it).
func TestBreakerTransitionsAreDeliveredOutsideTheRouterLock(t *testing.T) {
	flag, _, _, ws := twoWorkers(t)
	e := newResEnv(t, ws)
	var inside atomic.Int32
	cb := &callbackObserver{fn: func() {
		done := make(chan struct{})
		go func() { e.gw.router.InFlight("a-flaky"); close(done) }() // takes router.mu
		select {
		case <-done:
			inside.Add(1)
		case <-time.After(3 * time.Second):
			t.Error("BreakerTransition ran under the router lock: InFlight could not take it")
		}
	}}
	WithObserver(cb)(e.gw)
	for i := 0; i < 8; i++ {
		e.do(t)
	}
	flag.set(okJSON("a-flaky"))
	e.advance(t, 10*time.Second)
	for i := 0; i < 4; i++ {
		e.do(t) // the lazy open -> half-open change happens inside Route's Admissible/Acquire
	}
	if inside.Load() < 3 {
		t.Fatalf("only %d transitions delivered", inside.Load())
	}
}

type callbackObserver struct {
	NopObserver
	fn func()
}

func (c *callbackObserver) BreakerTransition(context.Context, BreakerChange) { c.fn() }

// A stream that fails after its first byte is one attempt, a breaker fault with no retry and an untouched budget
// (acceptance 6), for streaming and non-streaming bodies.
func TestAFailureAfterTheFirstByteIsOneAttemptAndAFaultWithoutRetry(t *testing.T) {
	for _, stream := range []bool{true, false} {
		name := "json"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			bad := newScriptWorker(t, "a-bad", func(w http.ResponseWriter, r *http.Request) {
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Length", "100")
				}
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "data: {\"i\":0}\n\n")
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			})
			good := newScriptWorker(t, "b-good", okJSON("b-good"))
			e := newResEnv(t, []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")},
				func(c *config.Config) { c.Resilience.Breaker.ConsecutiveFailures = 2 })
			// Round-robin: a-bad then b-good then a-bad. The success of a stream is counted when it completes, so an
			// abort is one fault and nothing else: two aborts in a row open the breaker by the streak rule.
			for i := 0; i < 4; i++ {
				e.postLoose(stream)
			}
			e.settle(t, "a-bad", "b-good")
			if st := e.state("a-bad"); st != resilience.Open {
				t.Fatalf("two aborted streams must open the breaker, got %v", st)
			}
			for _, a := range e.rec.attemptEnds() {
				if a.WorkerID == "a-bad" && (a.WillRetry || a.Outcome != AttemptFailed || a.Number != 1) {
					t.Fatalf("an aborted stream is one failed attempt, never retried: %+v", a)
				}
			}
			if f, r := e.rs.budget.Usage("qwen-7b"); r != 0 || f == 0 {
				t.Fatalf("budget usage %d first attempts, %d retries: the abort must not touch the retry budget", f, r)
			}
			if got := a2(e); got != 2 {
				t.Fatalf("the bad worker saw %d requests, want 2", got)
			}
		})
	}
}

func a2(e *resEnv) int64 {
	n := int64(0)
	for _, a := range e.rec.attemptStarts() {
		if a.WorkerID == "a-bad" {
			n++
		}
	}
	return n
}

// postLoose sends a chat request and drains the answer, ignoring a connection the gateway aborted.
func (e *resEnv) postLoose(stream bool) {
	body := `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]`
	if stream {
		body += `,"stream":true`
	}
	req, _ := http.NewRequest(http.MethodPost, e.url+chatCompletionsPath, strings.NewReader(body+"}"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// A worker with a long healthy history that starts aborting its streams is ejected after consecutive_failures aborts:
// the success of a stream is counted when it completes, so an abort is one fault and not a success followed by a
// fault, and history does not dilute it.
func TestAbortingStreamsOpenTheBreakerAfterTheConfiguredNumberDespiteAHealthyHistory(t *testing.T) {
	flag := &workerFlag{}
	flag.set(sseEvents("a-1", 2))
	a := newScriptWorker(t, "a-1", flag.serve)
	b := newScriptWorker(t, "b-2", sseEvents("b-2", 2))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")},
		func(c *config.Config) { c.Resilience.Breaker.ConsecutiveFailures = 5 })
	for i := 0; i < 40; i++ {
		e.postLoose(true) // healthy history on both workers
	}
	e.settle(t, "a-1", "b-2")
	if e.state("a-1") != resilience.Closed {
		t.Fatal("setup")
	}
	flag.set(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"i\":0}\n\n")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	before := a.hits.Load()
	for i := 0; i < 30 && e.state("a-1") != resilience.Open; i++ {
		e.postLoose(true)
		e.settle(t, "a-1", "b-2")
	}
	if e.state("a-1") != resilience.Open {
		t.Fatal("the aborting worker was not ejected")
	}
	if n := a.hits.Load() - before; n != 5 {
		t.Fatalf("%d aborted streams before the breaker opened, want exactly the configured 5", n)
	}
}

// A probe's verdict is taken at its first byte, not when its stream ends: a probe stream that is still running has
// already closed the breaker, and its ticket is not held for the length of the stream.
func TestAProbeStreamClosesTheBreakerAtItsFirstByte(t *testing.T) {
	release := make(chan struct{})
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))
	a := newScriptWorker(t, "a-flaky", flag.serve)
	b := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")},
		func(c *config.Config) { c.Resilience.Breaker.ConsecutiveFailures = 1 })
	for i := 0; i < 4; i++ {
		e.do(t)
	}
	flag.set(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"i\":0}\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	e.advance(t, 10*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 4 && e.state("a-flaky") != resilience.Closed; i++ {
			e.postLoose(true)
		}
	}()
	eventually(t, 5*time.Second, func() bool { return e.state("a-flaky") == resilience.Closed }, "the probe's first byte closes the breaker while its stream is still open")
	if probeSlots(e) != 0 {
		t.Fatal("the ticket is back")
	}
	close(release)
	<-done
}

// An unlisted 5xx counts in the ratio rule only: it never extends or trips the consecutive streak (D4), and the
// ratio rule does eject an always-500 worker, whose requests are never retried.
func TestA500CountsInTheRatioRuleOnlyAndIsNeverRetried(t *testing.T) {
	mk := func(t *testing.T, mut func(*config.Config)) (*resEnv, *scriptWorker) {
		bad := newScriptWorker(t, "a-500", status(500, `{"error":{"code":"mock_failure"}}`))
		good := newScriptWorker(t, "b-good", okJSON("b-good"))
		return newResEnv(t, []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")}, mut), bad
	}
	t.Run("consecutive rule never sees it", func(t *testing.T) {
		e, bad := mk(t, func(c *config.Config) {
			c.Resilience.Breaker.ConsecutiveFailures = 3
			c.Resilience.Breaker.MinVolume = 100000
		})
		for i := 0; i < 30; i++ {
			e.do(t)
		}
		if st := e.state("a-500"); st != resilience.Closed {
			t.Fatalf("30 500s opened the breaker through the consecutive rule: %v", st)
		}
		if bad.hits.Load() < 10 {
			t.Fatalf("only %d requests reached the 500 worker", bad.hits.Load())
		}
		for _, a := range e.rec.attemptEnds() {
			if a.WillRetry {
				t.Fatalf("a 500 is never retried: %+v", a)
			}
		}
	})
	t.Run("ratio rule ejects it", func(t *testing.T) {
		e, bad := mk(t, func(c *config.Config) {
			c.Resilience.Breaker.ConsecutiveFailures = 100000
			c.Resilience.Breaker.MinVolume = 4
		})
		for i := 0; i < 20 && e.state("a-500") != resilience.Open; i++ {
			e.do(t)
		}
		if st := e.state("a-500"); st != resilience.Open {
			t.Fatalf("the ratio rule did not eject an always-500 worker: %v", st)
		}
		if n := bad.hits.Load(); n != 4 {
			t.Fatalf("%d requests reached the worker before it opened, want min_volume 4", n)
		}
		if got := e.rec.states(); len(got) != 1 || got[0] != "a-500:open:failure_ratio" {
			t.Fatalf("transitions %v", got)
		}
	})
}

// The success of a stream is counted when it completes and it resets the consecutive streak: fault, fault, a
// complete stream, fault, fault is not three in a row.
func TestACompletedStreamResetsTheConsecutiveStreak(t *testing.T) {
	var n atomic.Int64
	var script atomic.Value
	script.Store([]string{"abort", "abort", "ok", "abort", "abort"})
	a := newScriptWorker(t, "a-1", func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		if sc := script.Load().([]string); i < len(sc) && sc[i] == "abort" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "data: {\"i\":0}\n\n")
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		sseEvents("a-1", 2)(w, r)
	})
	b := newScriptWorker(t, "b-2", sseEvents("b-2", 2))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")},
		func(c *config.Config) {
			c.Resilience.Breaker.ConsecutiveFailures = 3
			c.Resilience.Breaker.MinVolume = 100000
		})
	for i := 0; i < 40 && n.Load() < 5; i++ {
		e.postLoose(true)
		e.settle(t, "a-1", "b-2")
	}
	if n.Load() < 5 {
		t.Fatalf("a-1 saw %d requests", n.Load())
	}
	if st := e.state("a-1"); st != resilience.Closed {
		t.Fatalf("%v: a completed stream between the faults must reset the streak", st)
	}
	// One more abort is the third in a row only if the success is not counted; script ended, so make it abort.
	n.Store(0)
	script.Store([]string{"abort", "abort", "abort"})
	for i := 0; i < 40 && e.state("a-1") != resilience.Open; i++ {
		e.postLoose(true)
		e.settle(t, "a-1", "b-2")
	}
	if st := e.state("a-1"); st != resilience.Open {
		t.Fatalf("three aborts in a row must open the breaker: %v", st)
	}
}
