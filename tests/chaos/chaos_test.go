// Package chaos holds deterministic assertion tests for Phase 15 fault tolerance (ADR-020): a real embedded
// cluster (control plane, mock workers with their agents, a registry-mode gateway), a fault injected into one worker
// through mockworker.Faults, closed-loop clients, and assertions on what the clients saw and on the gateway's own
// counters. They are not benchmarks: the numbers come from `benchmark chaos`. Cool-downs and timeouts are scaled
// down by configuration so a scenario takes seconds, and every wait is a poll with a generous bound.
package chaos

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"serverflow/internal/bench/embedded"
	"serverflow/internal/config"
	"serverflow/internal/mockworker"
)

const (
	clients      = 6
	coolDown     = 300 * time.Millisecond
	maxCoolDown  = 600 * time.Millisecond
	consecutive  = 5
	minVolume    = 10
	probeConcur  = 1
	faultedIndex = 0
)

// rig is one running cluster with clients.
type rig struct {
	t       *testing.T
	c       *embedded.Cluster
	faulty  string
	model   string
	client  *http.Client
	stop    chan struct{}
	wg      sync.WaitGroup
	reqs    atomic.Int64
	fails   atomic.Int64
	statusM sync.Mutex
	status  map[int]int
}

type rigOpts struct {
	header  time.Duration // gateway.upstream_header_timeout (0: 30s)
	mutate  func(*config.Config)
	workers int
}

func newRig(t *testing.T, o rigOpts) *rig {
	t.Helper()
	if testing.Short() {
		t.Skip("chaos tests start a cluster; skipped with -short")
	}
	workers := o.workers
	if workers == 0 {
		workers = 3
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := embedded.Start(ctx, embedded.Config{
		Scheduler: "least-active", Workers: workers, Models: []string{"mock-model"}, MaxConcurrency: 8, TTFT: 5 * time.Millisecond,
		Resilience: "on",
		GatewayMutate: func(c *config.Config) {
			if o.header > 0 {
				c.Gateway.UpstreamHeaderTimeout = o.header
			}
			c.Resilience.Breaker.CoolDown, c.Resilience.Breaker.MaxCoolDown = coolDown, maxCoolDown
			c.Resilience.Breaker.ConsecutiveFailures, c.Resilience.Breaker.MinVolume = consecutive, minVolume
			c.Resilience.Breaker.ProbeConcurrency = probeConcur
			if o.mutate != nil {
				o.mutate(c)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	if got := cl.Resilience(); got != "on" {
		t.Fatalf("the cluster ran with resilience %q", got)
	}
	r := &rig{t: t, c: cl, faulty: cl.Specs()[faultedIndex].ID, model: "mock-model", stop: make(chan struct{}), status: map[int]int{},
		client: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}}
	t.Cleanup(func() { r.client.CloseIdleConnections() })
	if r.faults() == nil {
		t.Fatal("no fault handle for the first worker")
	}
	return r
}

func (r *rig) faults() *mockworker.Faults { return r.c.Faults(r.faulty) }

// start launches the closed-loop clients (half streaming).
func (r *rig) start() {
	for i := 0; i < clients; i++ {
		r.wg.Add(1)
		go func(i int) {
			defer r.wg.Done()
			for {
				select {
				case <-r.stop:
					return
				default:
				}
				r.one(i%2 == 0)
			}
		}(i)
	}
}

func (r *rig) one(stream bool) {
	body := fmt.Sprintf(`{"model":%q,"max_tokens":4,"messages":[{"role":"user","content":"hello"}]`, r.model)
	if stream {
		body += `,"stream":true`
	}
	req, _ := http.NewRequest(http.MethodPost, r.c.GatewayURL+"/v1/chat/completions", strings.NewReader(body+"}"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	code := 0
	if err == nil {
		b, rerr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		code = resp.StatusCode
		if rerr != nil {
			code = 0 // the body broke after the headers: a failed request
		}
		if stream && code == 200 && !strings.Contains(string(b), "[DONE]") {
			code = 0
		}
	}
	r.reqs.Add(1)
	if code != 200 {
		r.fails.Add(1)
	}
	r.statusM.Lock()
	r.status[code]++
	r.statusM.Unlock()
}

func (r *rig) finish() {
	close(r.stop)
	r.wg.Wait()
}

// window runs for d and returns the requests and failures in it.
func (r *rig) window(d time.Duration) (reqs, fails int64) {
	r0, f0 := r.reqs.Load(), r.fails.Load()
	time.Sleep(d)
	return r.reqs.Load() - r0, r.fails.Load() - f0
}

// --- gateway counters ------------------------------------------------------------------------------

func (r *rig) gather() []*dto.MetricFamily {
	r.t.Helper()
	fams, err := r.c.Metrics().Gather()
	if err != nil {
		r.t.Fatal(err)
	}
	return fams
}

// val sums the series of name whose labels include all of the given name=value pairs.
func (r *rig) val(name string, labels ...string) float64 {
	r.t.Helper()
	total := 0.0
	for _, f := range r.gather() {
		if f.GetName() != name {
			continue
		}
	series:
		for _, m := range f.GetMetric() {
			for _, want := range labels {
				k, v, _ := strings.Cut(want, "=")
				ok := false
				for _, l := range m.GetLabel() {
					ok = ok || (l.GetName() == k && l.GetValue() == v)
				}
				if !ok {
					continue series
				}
			}
			switch {
			case m.GetCounter() != nil:
				total += m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				total += m.GetGauge().GetValue()
			}
		}
	}
	return total
}

func (r *rig) selections() float64 { return r.val("scheduler_selections_total", "worker_id="+r.faulty) }
func (r *rig) opens() float64      { return r.val("circuit_breaker_transitions_total", "state=open") }
func (r *rig) closes() float64     { return r.val("circuit_breaker_transitions_total", "state=closed") }
func (r *rig) probes() float64     { return r.val("circuit_breaker_probes_total") }
func (r *rig) openNow() float64    { return r.val("circuit_breakers_open") }

func (r *rig) eventually(d time.Duration, cond func() bool, what string) {
	r.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.t.Fatalf("not within %v: %s (status counts %v)", d, what, r.statuses())
}

func (r *rig) statuses() map[int]int {
	r.statusM.Lock()
	defer r.statusM.Unlock()
	out := map[int]int{}
	for k, v := range r.status {
		out[k] = v
	}
	return out
}

// --- the scenario shape ----------------------------------------------------------------------------

// scenario injects a fault, lets the breaker act, and checks the contract of acceptance criterion 19:
//   - the breaker opens after at most maxFaultsToOpen requests reached the faulty worker (the configured number of
//     faults plus what was in flight);
//   - once it is open, client failures stop except one per probe, and the faulty worker receives only probes;
//   - after the fault ends the breaker closes and traffic returns, with no failures.
type scenario struct {
	rigOpts
	apply, clear    func(*rig) error
	maxFaultsToOpen int // requests that may reach the faulty worker before its breaker opens
	// ratioRule says only the failure-ratio rule can see this fault (an always-500 worker, a partial burst). The
	// ratio is over the sliding window, which still holds the worker's healthy history, so the faults needed are
	// that history (every attempt the worker had before the fault) plus min_volume.
	ratioRule bool
	// leaky: a probe can pass although the worker is still faulty -- a stream that breaks after its first byte has
	// already passed (the verdict is taken at the first byte so that a long stream does not hold the probe slot), and
	// with a partial fault rate a probe succeeds with the probability of the requests that are not failed. The breaker
	// then closes, lets the next requests through and opens again, so each probe can cost up to
	// consecutive_failures + 1 failures or attempts.
	leaky bool
}

func runScenario(t *testing.T, s scenario) {
	r := newRig(t, s.rigOpts)
	r.start()
	defer r.finish()

	if reqs, fails := r.window(600 * time.Millisecond); reqs < 20 || fails != 0 {
		t.Fatalf("baseline: %d requests, %d failures", reqs, fails)
	}
	sel0 := r.selections()
	if sel0 == 0 {
		t.Fatal("the worker to be faulted received no traffic in the baseline")
	}
	if err := s.apply(r); err != nil {
		t.Fatal(err)
	}
	r.eventually(20*time.Second, func() bool { return r.opens() >= 1 }, "the breaker opens")
	reached := r.selections() - sel0
	bound := float64(s.maxFaultsToOpen)
	if s.ratioRule {
		bound += sel0 + minVolume
	}
	if reached > bound {
		t.Errorf("%v requests reached the faulty worker before its breaker opened, want at most %v", reached, bound)
	}
	if r.openNow() < 1 {
		t.Errorf("circuit_breakers_open reads %v right after an open", r.openNow())
	}

	// Let what was in flight at the open finish, then watch: only probes may reach the worker and only
	// probes may cost a client a failure.
	time.Sleep(150 * time.Millisecond)
	selOpen, probesOpen := r.selections(), r.probes()
	reqs, fails := r.window(1200 * time.Millisecond)
	probes := r.probes() - probesOpen
	reachedAfter := r.selections() - selOpen
	perProbe := 1.0
	if s.leaky {
		perProbe = consecutive + 1
	}
	if reachedAfter > perProbe*(probes+1) { // +1: a probe whose verdict is still pending when the window closes
		t.Errorf("%v attempts reached the ejected worker in the window with only %v probes", reachedAfter, probes)
	}
	if float64(fails) > perProbe*(probes+1) {
		t.Errorf("%d client failures in the window (%d requests) with only %v probes: failures must stop except one per probe (statuses %v)", fails, reqs, probes, r.statuses())
	}
	if reqs < 20 {
		t.Errorf("only %d requests in the window: the survivors should keep serving", reqs)
	}
	if perCoolDown := 1200*time.Millisecond/coolDown + 2; probes > float64(perCoolDown*probeConcur) {
		t.Errorf("%v probes in 1.2s with a %v cool-down: at most %d", probes, coolDown, perCoolDown*probeConcur)
	}

	// The fault ends: the breaker closes (a probe passes) and traffic returns without failures.
	if err := s.clear(r); err != nil {
		t.Fatal(err)
	}
	r.eventually(15*time.Second, func() bool { return r.closes() >= 1 && r.openNow() == 0 }, "the breaker closes after the fault ends")
	selClosed := r.selections()
	if _, fails := r.window(800 * time.Millisecond); fails != 0 {
		t.Errorf("%d failures after recovery", fails)
	}
	if r.selections()-selClosed < 3 {
		t.Errorf("traffic did not return to the recovered worker (%v new attempts)", r.selections()-selClosed)
	}
	if r.val("scheduler_ineligible_selections_total") != 0 {
		t.Error("scheduler_ineligible_selections_total must stay 0")
	}
}

func rate(rate float64, mode mockworker.FailureMode) (func(*rig) error, func(*rig) error) {
	return func(r *rig) error { return r.faults().SetErrorRate(rate, mode) },
		func(r *rig) error { return r.faults().SetErrorRate(0, mode) }
}

// error-burst, injected 503 mock_failure on every request: a fault, retried on another worker. Clients never see it.
func TestErrorBurstUnavailableAllRequests(t *testing.T) {
	apply, clear := rate(1, mockworker.ModeUnavailable)
	runScenario(t, scenario{apply: apply, clear: clear, maxFaultsToOpen: consecutive + clients})
}

// A partial burst: four in five requests fail. The streak rule rarely sees five in a row and the ratio rule has to
// outweigh the worker's healthy history in the window. (Half of the requests failing is the ratio threshold itself
// and trips only by chance: that case is in the benchmark, not asserted here.)
func TestErrorBurstUnavailablePartial(t *testing.T) {
	apply, clear := rate(0.8, mockworker.ModeUnavailable)
	runScenario(t, scenario{apply: apply, clear: clear, maxFaultsToOpen: clients, ratioRule: true, leaky: true})
}

// An always-500 worker is invisible to the consecutive rule (a 500 is not a retryable fault) but the ratio rule
// ejects it, and its requests are never retried.
func TestErrorBurst500IsEjectedByTheRatioRuleAndNeverRetried(t *testing.T) {
	apply, clear := rate(1, mockworker.ModeError)
	runScenario(t, scenario{apply: apply, clear: clear, maxFaultsToOpen: clients, ratioRule: true})
}

func TestConnectionResets(t *testing.T) {
	apply, clear := rate(1, mockworker.ModeDrop)
	runScenario(t, scenario{apply: apply, clear: clear, maxFaultsToOpen: consecutive + clients})
}

// A stream that breaks after two tokens, on every request: the unavoidable failures happen before the breaker
// opens (five aborts in a row), and after it only a probe can break.
func TestMidstreamFailures(t *testing.T) {
	apply, clear := rate(1, mockworker.ModeMidstream)
	runScenario(t, scenario{apply: apply, clear: clear, maxFaultsToOpen: consecutive + clients, leaky: true})
}

// A frozen worker (reads requests, never answers; /stats keeps answering so the agent says READY): only the gateway's
// header timeout can see it. Each fault costs the header timeout, so the isolation bound is that times the faults
// needed (concurrently), and the probes of the frozen worker cost one client each.
func TestFrozenWorker(t *testing.T) {
	runScenario(t, scenario{
		rigOpts: rigOpts{header: 400 * time.Millisecond},
		apply:   func(r *rig) error { r.faults().SetBlackhole(true); return nil },
		clear:   func(r *rig) error { r.faults().SetBlackhole(false); return nil },
		// every request that reaches the frozen worker until it opens stalls for the header timeout
		maxFaultsToOpen: consecutive + clients,
	})
}

// A slow worker whose delay is below the header timeout is slow but successful: no breaker may open.
func TestSlowButSuccessfulWorkerNeverOpensABreaker(t *testing.T) {
	r := newRig(t, rigOpts{header: 2 * time.Second})
	r.start()
	defer r.finish()
	r.window(400 * time.Millisecond)
	r.faults().SetExtraDelay(300 * time.Millisecond)
	_, fails := r.window(2500 * time.Millisecond)
	if fails != 0 {
		t.Errorf("%d failures with a worker that is slow but within the header timeout (statuses %v)", fails, r.statuses())
	}
	if r.opens() != 0 || r.openNow() != 0 {
		t.Errorf("a breaker opened for a slow but successful worker (%v opens)", r.opens())
	}
	if r.faults().Stats().Delayed == 0 {
		t.Error("the delay never touched a request: the scenario proved nothing")
	}
}

// A slow worker whose delay is above the header timeout times out: those are faults, and the breaker ejects it.
func TestSlowWorkerBeyondTheHeaderTimeoutIsEjected(t *testing.T) {
	runScenario(t, scenario{
		rigOpts: rigOpts{header: 250 * time.Millisecond},
		apply:   func(r *rig) error { r.faults().SetExtraDelay(1500 * time.Millisecond); return nil },
		clear:   func(r *rig) error { r.faults().SetExtraDelay(0); return nil },

		maxFaultsToOpen: consecutive + clients,
	})
}
