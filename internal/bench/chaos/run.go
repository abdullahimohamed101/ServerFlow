package chaos

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/driver"
	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/report"
	"serverflow/internal/bench/workload"
)

// Loader drives the load of a run; the real one is the Phase 7 driver. A test substitutes a fake that
// returns records without any network.
type Loader interface {
	Drive(ctx context.Context, cfg driver.Config) (*driver.Output, error)
}

type driverLoader struct{}

func (driverLoader) Drive(ctx context.Context, cfg driver.Config) (*driver.Output, error) {
	return driver.Run(ctx, cfg)
}

// Deps are the things a run needs from outside, so that a test can replace them.
type Deps struct {
	Clock Clock
	// Boot starts the system under test.
	Boot func(ctx context.Context, cfg embedded.Config) (Cluster, error)
	Load Loader
	// Git reports the commit and tree state; nil asks git in the working directory.
	Git func(ctx context.Context) report.GitState
	// LogWriter receives the cluster's logs (nil: discarded).
	LogWriter io.Writer
}

// RealDeps runs against a real embedded cluster on the system clock.
func RealDeps(verbose bool) Deps {
	d := Deps{
		Clock: RealClock(),
		Boot: func(ctx context.Context, cfg embedded.Config) (Cluster, error) {
			c, err := embedded.Start(ctx, cfg)
			if err != nil {
				return nil, err
			}
			return Adapt(c), nil
		},
		Load: driverLoader{},
	}
	if verbose {
		d.LogWriter = os.Stderr
	}
	return d
}

// Seams.
const (
	drainTimeout     = 15 * time.Second
	sampleInterval   = 250 * time.Millisecond
	extraSeedOffset  = 7919
	mockDrainTimeout = 5 * time.Second
)

// RunOnce performs one run of a scenario and builds its result. The result is valid only when the
// validity gate passes; an invalid result is returned (not an error) so that it can be recorded.
func RunOnce(ctx context.Context, o Options, sc Scenario, d Deps, id string, rep report.Repeat) (report.Result, []collect.Record, error) {
	p := sc.Params(o.Workers, o.Scale)
	wl, err := workload.New(workload.Spec{Name: o.Workload, Seed: o.Seed, Models: workload.Models{Primary: o.Model}, StreamRatio: o.StreamRatio})
	if err != nil {
		return report.Result{}, nil, err
	}
	begin := time.Now()
	wallBegin := begin.Round(0)

	ecfg := embedded.Config{Scheduler: o.Scheduler, Workers: o.Workers, Models: wl.ModelNames(), TokensPerSecond: o.MockTPS, TTFT: o.MockTTFT,
		MaxConcurrency: o.MockConcurrent, GatewayMutate: sc.GatewayMutate(p), MockDrainAnnounce: p.DrainAnnounce, MockDrainTimeout: mockDrainTimeout,
		Resilience: o.Resilience, LogWriter: d.LogWriter}
	cl, err := d.Boot(ctx, ecfg)
	if err != nil {
		return report.Result{}, nil, fmt.Errorf("embedded cluster: %w", err)
	}
	defer cl.Close()
	boot := time.Since(begin)
	specs := cl.Specs()
	plan := Clients(sc, specs, o.Concurrency)

	faulty := ""
	if sc.SingleWorker && len(specs) > 0 {
		faulty = FaultedWorker
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	hard := report.ChaosHard{GoroutinesStart: runtime.NumGoroutine(), HeapStartMiB: float64(ms.HeapAlloc) / (1 << 20)}

	t0 := d.Clock.Now()
	cfg := driver.Config{BaseURL: cl.GatewayURL(), Workload: wl, Mode: driver.Closed, Concurrency: plan.Base,
		Warmup: p.Warmup, Duration: p.Baseline + p.Fault + p.Recovery, RequestTimeout: DefaultRequestTimeout,
		DrainTimeout: drainTimeout, T0: t0}

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sampler *collect.Sampler
	samplerDone := make(chan struct{})
	sctx, stopSampler := context.WithCancel(ctx)
	defer stopSampler()
	if cp := cl.ControlPlane(); cp != nil {
		sampler = collect.NewSampler(collect.ControlPlaneFetch(cp), sampleInterval, t0)
		go func() { sampler.Run(sctx); close(samplerDone) }()
	} else {
		close(samplerDone)
	}

	var out *driver.Output
	var driveErr error
	done := make(chan struct{})
	go func() { defer close(done); out, driveErr = d.Load.Drive(lctx, cfg) }()

	xwl, err := workload.New(workload.Spec{Name: o.Workload, Seed: o.Seed + extraSeedOffset, Models: workload.Models{Primary: o.Model}, StreamRatio: o.StreamRatio})
	if err != nil {
		cancel()
		<-done
		return report.Result{}, nil, err
	}
	extra := &extraLoad{ctx: lctx, loader: d.Load, base: cfg, wl: xwl, end: p.Warmup + p.Baseline + p.Fault}
	env := &Env{C: cl, P: p, Extra: extra, ExtraClients: plan.Extra}
	cond := &conductor{clock: d.Clock, t0: t0, p: p, env: env, sc: sc,
		snap: func() Counters {
			c, _ := readCounters(cl.Metrics(), faulty)
			return c
		}}
	if faulty != "" && !sc.Rolling {
		cond.state = func(c context.Context) string {
			sctx, cancelState := context.WithTimeout(c, 2*time.Second)
			defer cancelState()
			st, err := cl.WorkerState(sctx, faulty)
			if err != nil {
				return "unknown"
			}
			return st
		}
	}
	tl := cond.run(lctx)
	if !tl.Cleared || tl.ApplyErr != nil || tl.ClearErr != nil {
		cancel() // the run cannot be valid: do not spend the rest of it
	}
	<-done
	stopSampler()
	<-samplerDone
	var records []collect.Record
	if out != nil {
		records = append(records, out.Records...)
	}
	records = append(records, extra.wait()...)
	drivenOK := driveErr == nil && out != nil
	if ctx.Err() != nil {
		return report.Result{}, nil, fmt.Errorf("interrupted: %w", ctx.Err())
	}

	runtime.GC()
	runtime.ReadMemStats(&ms)
	hard.GoroutinesEnd, hard.HeapEndMiB = runtime.NumGoroutine(), float64(ms.HeapAlloc)/(1<<20)

	// Evidence, from the mock workers' own counters, before the cluster is closed.
	ev := Evidence{ControlPlane: tl.ControlPlane, Drains: env.Drains(), ServedWhileAnnouncing: env.Served()}
	if sc.SingleWorker && !sc.Rolling {
		if f := cl.Faults(FaultedWorker); f != nil {
			ev.Stats = f.Stats()
		}
	}

	w := windowsOf(p, tl)
	last := len(tl.Ticks) - 1
	warmTick := tickAt(p, p.Warmup)
	wins := []report.ChaosWindow{
		summarizeWindow("baseline", w.Baseline, records, delta(tl, warmTick, tl.ApplyTick)),
		summarizeWindow("fault", w.Fault, records, delta(tl, tl.ApplyTick, tl.ClearTick)),
		summarizeWindow("recovery", w.Recovery, records, delta(tl, tl.ClearTick, last)),
	}
	ev.WindowFailures, ev.GatewayNoCapacity = wins[1].Failed, wins[1].NoCapacityDelta

	ser := series(p, tl, records)
	iso, isoNote := timeToIsolate(p, tl, faulty != "" && !sc.Rolling)
	rec, recNote := timeToRecover(p, tl, ser, wins[0].SuccessRate)
	if len(tl.Ticks) > 0 {
		hard.IneligibleSelections = tl.Ticks[last].Ineligible - tl.Ticks[0].Ineligible
	}
	measures := report.ChaosMeasures{
		FaultSuccessRate: wins[1].SuccessRate, FaultAttemptsPerRequest: wins[1].AttemptsPerRequest,
		TimeToIsolate: iso, TimeToIsolateNote: isoNote, TimeToRecover: rec, TimeToRecoverNote: recNote,
		FaultSuccessLatencyP95: wins[1].LatencyP95, FaultSuccessLatencyP99: wins[1].LatencyP99,
	}

	meta := metadata(ctx, o, wl, cl, plan, p, rep, d)
	input := report.Input{
		RunID: id, Metadata: meta, Records: records, Window: collect.Window{Start: p.Warmup, End: p.Total()},
		Sampled: sampler != nil, MaxErrorRate: -1, // the gate judges the baseline window, not the whole run
		Timings: report.Timings{Boot: boot.Seconds(), Warmup: p.Warmup.Seconds(), Window: (p.Total() - p.Warmup).Seconds(),
			WallClock: time.Now().Round(0).Sub(wallBegin).Seconds(), MonotonicElapsed: time.Since(begin).Seconds()},
		Notes: notes(sc, p, plan),
	}
	if sampler != nil {
		input.Samples, input.SamplePollsFailed, input.SamplesDropped = sampler.Samples()
	}
	res := report.Build(input)
	// Per-worker balance is not measured in a chaos run (workers are drained and replaced), so the
	// "could not be listed" line Build adds for a target without workers would mislead.
	delete(res.NotMeasured, "request_imbalance")
	res.NotMeasured["worker_balance"] = "not measured in chaos runs: workers are faulted, drained and replaced during the run"

	params := map[string]string{"workers": fmt.Sprint(o.Workers), "mock_concurrency": fmt.Sprint(o.MockConcurrent), "clients": fmt.Sprint(plan.Base),
		"bucket": p.Bucket.String(), "scheduler": o.Scheduler}
	if plan.Extra > 0 {
		params["extra_clients_in_fault"] = fmt.Sprint(plan.Extra)
	}
	if p.HeaderTimeout > 0 {
		params["gateway_header_timeout"] = p.HeaderTimeout.String()
	}
	if p.Delay > 0 {
		params["extra_delay"] = p.Delay.String()
	}
	if p.Mode != "" {
		params["error_rate"], params["failure_mode"] = fmt.Sprint(p.Rate), string(p.Mode)
	}
	if p.DrainAnnounce > 0 {
		params["drain_announce"] = p.DrainAnnounce.String()
	}
	chaos := &report.Chaos{
		Scenario: sc.Group, Variant: sc.Variant, Description: sc.Description, TimeScale: p.Scale, BucketSeconds: p.Bucket.Seconds(),
		Params: params, FaultWorker: faulty, FaultAppliedSeconds: tl.AppliedAt.Seconds(), FaultClearedSeconds: tl.ClearedAt.Seconds(),
		Windows: wins, Measures: measures, Hard: hard, Series: ser,
		Evidence: report.ChaosEvidence{Applied: tl.Applied && tl.ApplyErr == nil, Touched: ev.Stats.Touched(), Errored: ev.Stats.Errored, Delayed: ev.Stats.Delayed,
			Blackholed: ev.Stats.Blackholed, Crashes: ev.Stats.Crashes, Drains: ev.Drains, ServedWhileAnnouncing: ev.ServedWhileAnnouncing,
			ControlPlane: ev.ControlPlane, Clients: plan.Base + plan.Extra, SurvivingSlots: plan.Surviving},
	}
	chaos.Gate = Gate(GateInput{Scenario: sc, Options: o, Plan: plan, Params: p, Timeline: tl, Windows: wins, Evidence: ev,
		Settled: cl.Settled(), GitTree: meta.GitTree, Commit: meta.GitCommit, DrivenOK: drivenOK})
	res.Chaos = chaos
	if failed := FailedChecks(chaos.Gate); len(failed) > 0 {
		res.InvalidReasons = append(res.InvalidReasons, failed...)
	}
	if hard.IneligibleSelections != 0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf("the scheduler chose a worker the registry did not judge eligible %.0f times (scheduler_ineligible_selections_total must stay 0): a routing bug, not a property of the fault", hard.IneligibleSelections))
	}
	res.Valid = len(res.InvalidReasons) == 0
	return res, records, nil
}

func notes(sc Scenario, p Params, plan ClientPlan) []string {
	return []string{
		"Chaos run: " + sc.Description,
		"What this embedded mock cluster cannot show here: " + sc.CannotShow,
		fmt.Sprintf("Windows follow when the fault was really applied and cleared. Gateway counters (attempts per request, retries) are read at the tick that opens and closes a window (every %v), so an attempt of a request in flight at a boundary falls in the next window; the per-request figures come from the clients.", p.Bucket),
		fmt.Sprintf("Closed-loop clients: %d for the whole run%s on %d request slots (%d left serving). Closed-loop clients wait for each response, so a failing worker also slows the offered load.", plan.Base,
			map[bool]string{true: fmt.Sprintf(", plus %d in the fault window", plan.Extra), false: ""}[plan.Extra > 0], plan.Slots, plan.Surviving),
	}
}

func metadata(ctx context.Context, o Options, wl *workload.Workload, cl Cluster, plan ClientPlan, p Params, rep report.Repeat, d Deps) report.Metadata {
	n := len(cl.Specs())
	m := report.Metadata{
		Scheduler: o.Scheduler, SchedulerSource: "embedded", Models: wl.ModelNames(), GPUType: "none (mock workers)", Mode: string(driver.Closed),
		Concurrency: plan.Base, DurationSecs: (p.Total() - p.Warmup).Seconds(), WarmupSecs: p.Warmup.Seconds(), Seed: o.Seed, Workload: o.Workload,
		StreamRatio: o.StreamRatio, PlanDigest: wl.Digest(1000), Repeat: rep, Target: "embedded", TargetHost: "loopback", WorkerCount: &n,
		Resilience: cl.Resilience(),
	}
	for _, c := range wl.Classes() {
		m.Prompt = append(m.Prompt, report.PromptClass{Name: c.Name, Weight: c.Weight, InputMin: c.InMin, InputMax: c.InMax, OutputMin: c.OutMin, OutputMax: c.OutMax})
	}
	m.Environment(ctx, ".", d.Clock.Now())
	if d.Git != nil {
		g := d.Git(ctx)
		m.GitCommit, m.GitTree = g.Commit, g.Tree
	}
	c := &report.Cluster{Profile: embedded.Identical, HeartbeatMillis: embedded.HeartbeatInterval.Milliseconds(), RegistryRefreshMs: embedded.RegistryRefresh.Milliseconds()}
	for _, w := range cl.Specs() {
		c.Workers = append(c.Workers, report.ClusterWorker{ID: w.ID, Model: w.Model, TokensPerSecond: w.TokensPerSecond, TTFTMillis: w.TTFT.Milliseconds(),
			MaxConcurrency: w.MaxConcurrency, QueueSize: w.QueueSize})
	}
	m.Cluster = c
	return m
}

// extraLoad adds clients for the fault window of an overload. The driver stops sending at the planned end of
// the fault window by itself, so Stop has nothing to do; wait collects the records.
type extraLoad struct {
	ctx    context.Context
	loader Loader
	base   driver.Config
	wl     *workload.Workload
	end    time.Duration // offset from the run's start at which the extra clients stop sending

	mu   sync.Mutex
	done chan struct{}
	out  *driver.Output
}

func (x *extraLoad) Start(n int) {
	if n <= 0 {
		return
	}
	cfg := x.base
	cfg.Workload, cfg.Concurrency, cfg.Warmup, cfg.Duration, cfg.Progress = x.wl, n, 0, x.end, nil
	x.mu.Lock()
	x.done = make(chan struct{})
	done := x.done
	x.mu.Unlock()
	go func() {
		defer close(done)
		out, _ := x.loader.Drive(x.ctx, cfg)
		x.mu.Lock()
		x.out = out
		x.mu.Unlock()
	}()
}

func (x *extraLoad) Stop() {}

func (x *extraLoad) wait() []collect.Record {
	x.mu.Lock()
	done := x.done
	x.mu.Unlock()
	if done == nil {
		return nil
	}
	<-done
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.out == nil {
		return nil
	}
	return x.out.Records
}

// Run executes the scenario (or every scenario) o.Repeat times each, writing one result directory per run,
// and prints a summary per scenario. Invalid runs are recorded and listed but never summarized; Run returns an
// error when any run was invalid.
func Run(ctx context.Context, o Options, d Deps, out io.Writer) ([]report.Result, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	var todo []Scenario
	if o.Scenario == "all" {
		todo = Scenarios()
	} else {
		sc, _ := Lookup(o.Scenario)
		todo = []Scenario{sc}
	}
	var all []report.Result
	var invalid []string
	for _, sc := range todo {
		var group []report.Result
		gid := ""
		for i := 1; i <= o.Repeat; i++ {
			id, dir, err := report.CreateRun(o.OutDir)
			if err != nil {
				return all, err
			}
			if gid == "" {
				gid = fmt.Sprintf("%s-%d", id, time.Now().Unix())
			}
			loadStart := loadAverage()
			say(out, "%s: %s, repeat %d of %d (load average %s)\n", id, sc.Name, i, o.Repeat, loadStart)
			res, recs, err := RunOnce(ctx, o, sc, d, id, report.Repeat{Index: i, Of: o.Repeat, Group: gid})
			if err != nil {
				_ = os.Remove(dir)
				return all, err
			}
			if o.Resilience != "" && res.Metadata.Resilience == report.ResilienceAbsent {
				say(out, "  note: --resilience %s has no effect: this build has no resilience feature (recorded as absent)\n", o.Resilience)
			}
			res.Notes = append(res.Notes, "Load average (1, 5 and 15 minutes) at the start of the run: "+loadStart+"; at the end: "+loadAverage()+".")
			if !o.SaveRequests {
				recs = nil
			}
			if err := report.Write(dir, res, recs); err != nil {
				return all, err
			}
			group = append(group, res)
			all = append(all, res)
			say(out, "%s: %s\n", id, Headline(res))
			if !res.Valid {
				invalid = append(invalid, id)
				for _, why := range res.InvalidReasons {
					say(out, "%s: INVALID: %s\n", id, why)
				}
			}
			say(out, "%s: wrote %s\n", id, dir)
		}
		say(out, "\n%s", SpreadSummary(sc, group))
	}
	if len(invalid) > 0 {
		return all, fmt.Errorf("%d invalid run(s): %s (recorded with valid=false; never compare them)", len(invalid), strings.Join(invalid, ", "))
	}
	return all, nil
}

// Headline is the one-line outcome of a run.
func Headline(r report.Result) string {
	c := r.Chaos
	if c == nil {
		return "no chaos result"
	}
	line := ""
	if !r.Valid {
		line = "INVALID: "
	}
	line += fmt.Sprintf("fault-window success %.1f%%", 100*c.Measures.FaultSuccessRate)
	if a := c.Measures.FaultAttemptsPerRequest; a != nil {
		line += fmt.Sprintf(", attempts/request %.3f", *a)
	}
	line += ", isolate " + optSec(c.Measures.TimeToIsolate) + ", recover " + optSec(c.Measures.TimeToRecover)
	return line
}

func optSec(p *float64) string {
	if p == nil {
		return "not reached"
	}
	return fmt.Sprintf("%.1fs", *p)
}

func say(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
