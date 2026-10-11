package gateway

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// halfOpenEnv returns an environment where a-flaky is half-open (its single probe slot free) and answers by flag.
func halfOpenEnv(t *testing.T, flag *workerFlag, mut ...func(*config.Config)) *resEnv {
	t.Helper()
	a := newScriptWorker(t, "a-flaky", flag.serve)
	b := newScriptWorker(t, "b-good", okJSON("b-good"))
	mut = append([]func(*config.Config){func(c *config.Config) { c.Resilience.Breaker.ConsecutiveFailures = 1 }}, mut...)
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")}, mut...)
	for i := 0; i < 4; i++ {
		e.do(t)
	}
	if e.state("a-flaky") != resilience.Open {
		t.Fatalf("setup: %v", e.state("a-flaky"))
	}
	e.advance(t, 10*time.Second)
	if e.state("a-flaky") != resilience.HalfOpen {
		t.Fatalf("setup: %v", e.state("a-flaky"))
	}
	return e
}

func probeSlots(e *resEnv) int {
	n := 0
	for _, in := range e.rs.breakers.Snapshot() {
		n += in.Probes
	}
	return n
}

// The probe ticket goes back exactly once on every way out of an attempt that owes no verdict: a panic in the
// upstream call, a client that leaves, and a deadline expiry. The next request can probe again each time.
func TestTheProbeTicketComesBackOnPanicCancelAndTimeout(t *testing.T) {
	flag := &workerFlag{}
	flag.set(openAPI503("mock_failure"))

	t.Run("panic", func(t *testing.T) {
		e := halfOpenEnv(t, flag)
		e.injectTransport(&faultTransport{panicSend: func(r *http.Request) bool {
			return strings.Contains(r.URL.Host, "") && r.Header.Get("X-Request-ID") != ""
		}})
		resp, _ := e.chat(t, "qwen-7b")
		if resp.StatusCode != 500 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		e.settle(t, "a-flaky", "b-good")
		if probeSlots(e) != 0 || e.state("a-flaky") != resilience.HalfOpen {
			t.Fatalf("probe slots %d, state %v: a panic returns the ticket and changes nothing else", probeSlots(e), e.state("a-flaky"))
		}
		ends := e.rec.attemptEnds()
		if last := ends[len(ends)-1]; !last.Probe || last.Class != classPanic || last.Verdict != VerdictNeutral {
			t.Fatalf("%+v", last)
		}
		tk, ok := e.rs.breakers.Acquire(e.key("a-flaky"))
		if !ok || !tk.Probe() {
			t.Fatal("the probe slot is free again")
		}
		e.rs.breakers.Release(tk)
	})

	t.Run("client leaves", func(t *testing.T) {
		held := &workerFlag{}
		held.set(func(w http.ResponseWriter, r *http.Request) { hold(r) })
		e := halfOpenEnv(t, flag)
		e.lister.mu.Lock()
		e.lister.mu.Unlock()
		// Swap the worker's behaviour: a-flaky now holds the request.
		flag.set(func(w http.ResponseWriter, r *http.Request) { hold(r) })
		defer flag.set(openAPI503("mock_failure"))
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath,
			strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
		done := make(chan struct{})
		go func() {
			defer close(done)
			resp, err := e.client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
		eventually(t, 5*time.Second, func() bool { return probeSlots(e) == 1 }, "the probe is in flight")
		cancel()
		<-done
		e.settle(t, "a-flaky", "b-good")
		if probeSlots(e) != 0 || e.state("a-flaky") != resilience.HalfOpen {
			t.Fatalf("probe slots %d, state %v", probeSlots(e), e.state("a-flaky"))
		}
		ends := e.rec.attemptEnds()
		last := ends[len(ends)-1]
		if !last.Probe || last.Outcome != AttemptClientClosed || last.Verdict != VerdictNeutral {
			t.Fatalf("%+v", last)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		flag.set(openAPI503("mock_failure"))
		e := halfOpenEnv(t, flag, deadline(200*time.Millisecond))
		flag.set(func(w http.ResponseWriter, r *http.Request) { hold(r) })
		defer flag.set(openAPI503("mock_failure"))
		var status int
		for i := 0; i < 3 && status != 504; i++ {
			status = e.do(t)
		}
		if status != 504 {
			t.Fatalf("status %d", status)
		}
		e.settle(t, "a-flaky", "b-good")
		if probeSlots(e) != 0 || e.state("a-flaky") != resilience.HalfOpen {
			t.Fatalf("probe slots %d, state %v: an expired deadline is neutral", probeSlots(e), e.state("a-flaky"))
		}
	})
}

// An open breaker's worker is never selected, and a breaker never makes a registry-ineligible worker eligible:
// random registry views and random breaker states, many routings, no ineligible selection.
func TestRoutingNeverSelectsAnOpenOrIneligibleWorker(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	for round := 0; round < 60; round++ {
		n := 2 + rng.IntN(6)
		ws := make([]protocol.WorkerSnapshot, n)
		for i := range ws {
			ws[i] = snapWorker(fmt.Sprintf("w-%d", i), "qwen-7b", 0)
			ws[i].MaxConcurrency = 8
			ws[i].Eligible = rng.IntN(4) != 0
			if !ws[i].Eligible {
				ws[i].State, ws[i].Health = protocol.StateDraining, protocol.HealthHealthy
			}
		}
		e := newResEnv(t, ws, func(c *config.Config) {
			c.Resilience.Breaker.ConsecutiveFailures = 1
			c.Resilience.Breaker.MaxOpenFraction = 0.75
		})
		// Drive random faults into the breakers through their own API.
		for i := 0; i < n; i++ {
			if rng.IntN(2) == 0 {
				if tk, ok := e.rs.breakers.Acquire(e.key(ws[i].WorkerID)); ok {
					e.rs.breakers.Record(tk, resilience.Fault)
				}
			}
		}
		if rng.IntN(2) == 0 {
			e.clock.Advance(10 * time.Second) // some are now half-open
			e.refresh(t)
		}
		for k := 0; k < 30; k++ {
			got, rerr := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"})
			if rerr != nil {
				continue
			}
			w := got.worker
			if !w.Eligible || w.State != protocol.StateReady {
				t.Fatalf("round %d: routed to a registry-ineligible worker %+v", round, w)
			}
			if st := e.state(w.WorkerID); st == resilience.Open {
				t.Fatalf("round %d: routed to the open worker %s", round, w.WorkerID)
			}
			if got.probe() != (e.state(w.WorkerID) == resilience.HalfOpen) {
				t.Fatalf("round %d: probe flag %v for a %v breaker", round, got.probe(), e.state(w.WorkerID))
			}
			got.release()
			e.rs.breakers.Release(got.ticket)
		}
		if v, ok := sample(t, e.gw.metrics, "scheduler_ineligible_selections_total"); ok && v != 0 {
			t.Fatalf("scheduler_ineligible_selections_total %v", v)
		}
	}
}

// Concurrent requests while snapshot refreshes flip eligibility and workers fail in assorted ways: no slot, no
// ticket and no goroutine leaks, and no selection of an ineligible worker. Run with -race.
func TestConcurrentRequestsAndRefreshesLeakNothing(t *testing.T) {
	flaky := &workerFlag{}
	flaky.set(openAPI503("mock_failure"))
	mk := func(id string, h func(http.ResponseWriter, *http.Request)) *scriptWorker {
		return newScriptWorker(t, id, h)
	}
	w1 := mk("w-1", okJSON("w-1"))
	w2 := mk("w-2", flaky.serve)
	w3 := mk("w-3", func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Err() == nil && time.Now().UnixNano()%3 == 0 {
			panic(http.ErrAbortHandler)
		}
		okJSON("w-3")(w, r)
	})
	w4 := mk("w-4", sseEvents("w-4", 3))
	snaps := []protocol.WorkerSnapshot{w1.snapshot("qwen-7b"), w2.snapshot("qwen-7b"), w3.snapshot("qwen-7b"), w4.snapshot("qwen-7b")}
	e := newResEnv(t, snaps, func(c *config.Config) {
		c.Resilience.Breaker.ConsecutiveFailures = 2
		c.Resilience.Breaker.CoolDown = 100 * time.Millisecond
		c.Resilience.Breaker.MaxCoolDown = 200 * time.Millisecond
		c.Resilience.Retry.BudgetMin = 1000
	})
	// Real time for this test: the breakers use the fake clock, advanced by the refresher.
	base := runtime.NumGoroutine()
	stop := make(chan struct{})
	var rg sync.WaitGroup
	rg.Add(1)
	go func() {
		defer rg.Done()
		rng := rand.New(rand.NewPCG(3, 4))
		for {
			select {
			case <-stop:
				return
			default:
			}
			cur := append([]protocol.WorkerSnapshot(nil), snaps...)
			for i := range cur {
				if rng.IntN(5) == 0 {
					cur[i].Eligible = false
					cur[i].State = protocol.StateDraining
				}
			}
			e.lister.set(cur, nil)
			e.clock.Advance(60 * time.Millisecond)
			_ = e.gw.router.cache.Refresh(context.Background())
			time.Sleep(2 * time.Millisecond)
		}
	}()
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				e.postLoose(g%2 == 0)
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	rg.Wait()
	e.settle(t, "w-1", "w-2", "w-3", "w-4")
	if v, ok := sample(t, e.gw.metrics, "scheduler_ineligible_selections_total"); ok && v != 0 {
		t.Fatalf("an ineligible worker was selected %v times", v)
	}
	starts, ends := len(e.rec.attemptStarts()), len(e.rec.attemptEnds())
	if starts != ends {
		t.Fatalf("%d attempts started, %d ended", starts, ends)
	}
	eventually(t, 5*time.Second, func() bool { return probeSlots(e) == 0 }, "every probe ticket must be back")
	e.client.CloseIdleConnections()
	e.gw.router.client.CloseIdleConnections()
	eventually(t, 10*time.Second, func() bool { return runtime.NumGoroutine() <= base+8 }, fmt.Sprintf("goroutines back near %d, now %d", base, runtime.NumGoroutine()))
}
