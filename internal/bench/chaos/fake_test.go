package chaos

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/driver"
	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/report"
	"serverflow/internal/mockworker"
	"serverflow/internal/registry/client"
)

// fakeFaults records what a scenario does to a worker and keeps counters like the mock's.
type fakeFaults struct {
	mu    sync.Mutex
	calls []string
	stats mockworker.FaultStats
}

func (f *fakeFaults) log(s string, a ...any) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf(s, a...))
	f.mu.Unlock()
}

func (f *fakeFaults) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeFaults) SetErrorRate(rate float64, mode mockworker.FailureMode) error {
	f.log("errorrate %v %s", rate, mode)
	f.mu.Lock()
	if rate > 0 {
		f.stats.Errored += 7
	}
	f.mu.Unlock()
	return nil
}

func (f *fakeFaults) SetExtraDelay(d time.Duration) {
	f.log("delay %v", d)
	f.mu.Lock()
	if d > 0 {
		f.stats.Delayed += 7
	}
	f.mu.Unlock()
}

func (f *fakeFaults) SetBlackhole(on bool) {
	f.log("blackhole %v", on)
	f.mu.Lock()
	if on {
		f.stats.Blackholed += 7
	}
	f.mu.Unlock()
}

func (f *fakeFaults) Crash() error {
	f.log("crash")
	f.mu.Lock()
	f.stats.Crashes++
	f.mu.Unlock()
	return nil
}
func (f *fakeFaults) Restart() error { f.log("restart"); return nil }
func (f *fakeFaults) Clear()         { f.log("clear") }
func (f *fakeFaults) Stats() mockworker.FaultStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// fakeCluster is a cluster without a network. Its gateway counters are functions of the fake clock, so
// they read like a gateway that served the synthetic load.
type fakeCluster struct {
	clock      *FakeClock
	t0         time.Time
	specs      []embedded.WorkerSpec
	faults     map[string]*fakeFaults
	resilience string
	settled    time.Duration
	reg        *prometheus.Registry

	mu       sync.Mutex
	drained  []string
	replace  []string
	drainFn  func(ctx context.Context, id string) error
	rollDone chan struct{}
	state    string
	closed   atomic.Bool
	// perSecond is the synthetic request rate used by the counters; extra attempts (retries) accrue
	// at 10 per second between faultFrom and faultTo (seconds into the run); the faulted worker is
	// selected until isolateAt.
	perSecond          float64
	faultFrom, faultTo float64
	isolateAt          float64
}

func newFakeCluster(clock *FakeClock, workers int) *fakeCluster {
	c := &fakeCluster{clock: clock, faults: map[string]*fakeFaults{}, resilience: report.ResilienceAbsent, settled: 600 * time.Millisecond,
		reg: prometheus.NewRegistry(), state: "FAILED", perSecond: 100, isolateAt: 1e9, rollDone: make(chan struct{})}
	for i := 1; i <= workers; i++ {
		id := fmt.Sprintf("worker-%02d", i)
		c.specs = append(c.specs, embedded.WorkerSpec{ID: id, Model: "qwen-7b", MaxConcurrency: 8})
		c.faults[id] = &fakeFaults{}
	}
	return c
}

// elapsed is the fake time since the run began.
func (c *fakeCluster) elapsed() float64 { return c.clock.Now().Sub(c.t0).Seconds() }

func (c *fakeCluster) register(name string, f func(sec float64) float64, labels prometheus.Labels) {
	c.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: name, ConstLabels: labels}, func() float64 { return f(c.elapsed()) }))
}

func (c *fakeCluster) GatewayURL() string { return "http://fake" }
func (c *fakeCluster) Specs() []embedded.WorkerSpec {
	return append([]embedded.WorkerSpec(nil), c.specs...)
}
func (c *fakeCluster) Metrics() prometheus.Gatherer                        { return c.reg }
func (c *fakeCluster) ControlPlane() *client.Client                        { return nil }
func (c *fakeCluster) Settled() time.Duration                              { return c.settled }
func (c *fakeCluster) Resilience() string                                  { return c.resilience }
func (c *fakeCluster) Close()                                              { c.closed.Store(true) }
func (c *fakeCluster) ServedWhileAnnouncing(string) int64                  { return 3 }
func (c *fakeCluster) WorkerState(context.Context, string) (string, error) { return c.state, nil }

func (c *fakeCluster) Faults(id string) FaultHandle {
	if f, ok := c.faults[id]; ok {
		return f
	}
	return nil
}

func (c *fakeCluster) DrainWorker(ctx context.Context, id string) error {
	c.mu.Lock()
	c.drained = append(c.drained, id)
	fn := c.drainFn
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx, id)
	}
	return nil
}

func (c *fakeCluster) ReplaceWorker(_ context.Context, id string) error {
	c.mu.Lock()
	c.replace = append(c.replace, id)
	if len(c.replace) == len(c.specs) && c.rollDone != nil {
		close(c.rollDone)
	}
	c.mu.Unlock()
	return nil
}

// fakeLoader returns synthetic records: a request every ms-step per client, failing during
// [failFrom, failTo) at a fixed ratio, so the analysis has something to find.
type fakeLoader struct {
	failFrom, failTo time.Duration
	failEvery        int // every n-th request in the fault interval fails (0: none fail)
	calls            atomic.Int64
	err              error
}

func (l *fakeLoader) Drive(_ context.Context, cfg driver.Config) (*driver.Output, error) {
	l.calls.Add(1)
	if l.err != nil {
		return nil, l.err
	}
	var recs []collect.Record
	end := cfg.Warmup + cfg.Duration
	n := 0
	for at := time.Duration(0); at < end; at += 20 * time.Millisecond {
		r := collect.Record{Seq: n, Intended: at, Started: at, Done: at + 15*time.Millisecond, Status: 200, Attempts: 0}
		if l.failEvery > 0 && at >= l.failFrom && at < l.failTo && n%l.failEvery == 0 {
			r.Status = 503
		}
		recs = append(recs, r)
		n++
	}
	return &driver.Output{Records: recs, T0: cfg.T0, Window: collect.Window{Start: cfg.Warmup, End: end}}, nil
}

// fakeDeps wires a fake cluster, clock and loader. The cluster's counters follow the load: sec*perSecond
// requests, one attempt each plus extra during the fault.
func fakeDeps(t *testing.T, workers int, tweak func(*fakeCluster)) (Deps, *fakeCluster, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	c := newFakeCluster(clock, workers)
	if tweak != nil {
		tweak(c)
	}
	d := Deps{
		Clock: clock,
		Boot: func(_ context.Context, cfg embedded.Config) (Cluster, error) {
			c.t0 = clock.Now()
			c.reg = prometheus.NewRegistry()
			rps := c.perSecond
			extra := func(s float64) float64 { return 10 * min(max(s-c.faultFrom, 0), c.faultTo-c.faultFrom) }
			c.register("inference_requests_total", func(s float64) float64 { return s * rps }, nil)
			c.register("inference_attempts_total", func(s float64) float64 { return s*rps + extra(s) }, nil)
			c.register("inference_retries_total", extra, nil)
			c.register("scheduler_no_capacity_total", func(float64) float64 { return 0 }, nil)
			c.register("scheduler_ineligible_selections_total", func(float64) float64 { return 0 }, nil)
			c.register("scheduler_selections_total", func(s float64) float64 { return min(s, c.isolateAt) * 25 }, prometheus.Labels{"model": "qwen-7b", "worker_id": "worker-01"})
			c.register("scheduler_selections_total", func(s float64) float64 { return s * 25 }, prometheus.Labels{"model": "qwen-7b", "worker_id": "worker-02"})
			return c, nil
		},
		Load: &fakeLoader{},
		Git: func(context.Context) report.GitState {
			return report.GitState{Commit: "0123456789abcdef", Tree: "clean"}
		},
	}
	return d, c, clock
}

func testOptions(t *testing.T, scenario string) Options {
	t.Helper()
	o := Options{Scenario: scenario, Scale: 0.2, Workers: 4, MockConcurrent: 8, MockTPS: 1000, MockTTFT: 20 * time.Millisecond, Scheduler: "round-robin",
		Workload: "uniform-short", Model: "qwen-7b", StreamRatio: 0.5, Seed: 1, Repeat: 1, MaxErrorRate: DefaultMaxErrorRate, MinRequests: 20, OutDir: t.TempDir()}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	return o
}
