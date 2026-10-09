package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/driver"
	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/report"
	"serverflow/internal/bench/workload"
	"serverflow/internal/config"
	"serverflow/internal/registry/client"
)

// Run executes o.Repeat runs of the configured benchmark, writing one result directory
// each, and returns their results. Progress goes to out. Nothing written to out or to the
// results contains an API key or the control plane token.
func Run(ctx context.Context, o Options, out io.Writer) ([]report.Result, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	var results []report.Result
	group := ""
	for i := 1; i <= o.Repeat; i++ {
		id, dir, err := report.CreateRun(o.OutDir)
		if err != nil {
			return results, err
		}
		if group == "" {
			group = id
		}
		say(out, "%s: repeat %d of %d, workload %s, scheduler %s\n", id, i, o.Repeat, o.Workload, o.Scheduler)
		res, records, err := runOnce(ctx, o, id, report.Repeat{Index: i, Of: o.Repeat, Group: group}, out)
		if err != nil {
			_ = os.Remove(dir) // nothing was written; the ID stays retired
			return results, err
		}
		if !o.SaveRequests {
			records = nil
		}
		if err := report.Write(dir, res, records); err != nil {
			return results, err
		}
		results = append(results, res)
		say(out, "%s: %s\n", id, Headline(res))
		say(out, "%s: wrote %s\n", id, dir)
	}
	if len(results) > 1 {
		say(out, "\n")
		say(out, "%s", SpreadSummary(results))
	}
	return results, nil
}

// Headline is the one-line outcome printed when a run ends.
func Headline(r report.Result) string {
	s := r.Summary
	line := fmt.Sprintf("sent %d, succeeded %d, failed %d; %.2f req/s", s.Sent, s.Succeeded, s.Failed, s.RequestsPerSecond)
	if s.Latency != nil {
		line += fmt.Sprintf("; latency p50 %.0f / p95 %.0f ms", s.Latency.P50, s.Latency.P95)
	}
	if s.TTFT != nil {
		line += fmt.Sprintf("; TTFT p95 %.0f ms", s.TTFT.P95)
	}
	if j := r.Imbalance.RequestJain; j != nil {
		line += fmt.Sprintf("; request Jain %.3f", *j)
	}
	return line
}

// SpreadSummary prints the min, median and max of the headline numbers across repeats,
// so run-to-run noise is visible next to any comparison.
func SpreadSummary(rs []report.Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Spread across %d repeats (min / median / max):\n", len(rs))
	row := func(name string, get func(report.Result) (float64, bool)) {
		var xs []float64
		for _, r := range rs {
			if v, ok := get(r); ok {
				xs = append(xs, v)
			}
		}
		if len(xs) == 0 {
			fmt.Fprintf(&b, "  %-18s not measured\n", name)
			return
		}
		sort.Float64s(xs)
		fmt.Fprintf(&b, "  %-18s %.4g / %.4g / %.4g\n", name, xs[0], median(xs), xs[len(xs)-1])
	}
	row("requests/s", func(r report.Result) (float64, bool) { return r.Summary.RequestsPerSecond, true })
	row("latency p95 (ms)", func(r report.Result) (float64, bool) {
		if d := r.Summary.Latency; d != nil {
			return d.P95, true
		}
		return 0, false
	})
	row("TTFT p95 (ms)", func(r report.Result) (float64, bool) {
		if d := r.Summary.TTFT; d != nil {
			return d.P95, true
		}
		return 0, false
	})
	row("request Jain", func(r report.Result) (float64, bool) {
		if j := r.Imbalance.RequestJain; j != nil {
			return *j, true
		}
		return 0, false
	})
	return b.String()
}

// median of sorted xs.
func median(xs []float64) float64 {
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

// runOnce performs one run and builds its result.
func runOnce(ctx context.Context, o Options, id string, rep report.Repeat, out io.Writer) (report.Result, []collect.Record, error) {
	wl, err := workload.New(workload.Spec{Name: o.Workload, Seed: o.Seed, Models: o.Models(), StreamRatio: o.StreamRatio})
	if err != nil {
		return report.Result{}, nil, err
	}
	tgt, err := connect(ctx, o, wl, out)
	if err != nil {
		return report.Result{}, nil, err
	}
	defer tgt.close()

	meta := metadata(ctx, o, wl, tgt, rep)
	cfg := driver.Config{
		BaseURL: tgt.gatewayURL, Workload: wl, Mode: o.Mode(), Concurrency: o.Concurrency, MaxInFlight: o.MaxInFlight,
		Warmup: o.Warmup, Duration: o.Duration, RequestTimeout: o.RequestTimeout, DrainTimeout: o.DrainTimeout, MaxRequests: o.MaxRequests,
	}
	if o.Mode() == driver.Open {
		cfg.Segments = workload.Segments(o.Workload, o.Rate, o.Warmup, o.Duration)
	}

	// Worker-side measurement: queue samples for the whole run, and each worker's completed
	// count at the start and end of the measurement window.
	t0 := time.Now()
	cfg.T0 = t0
	sctx, stopSampler := context.WithCancel(ctx)
	var sampler *collect.Sampler
	samplerDone := make(chan struct{})
	if tgt.cp != nil {
		sampler = collect.NewSampler(collect.ControlPlaneFetch(tgt.cp), o.SampleInterval, t0)
		go func() { sampler.Run(sctx); close(samplerDone) }()
	} else {
		close(samplerDone)
	}
	targets, missing := tgt.workers(ctx)
	if meta.WorkerCount == nil && len(targets) > 0 {
		n := len(targets)
		meta.WorkerCount = &n
	}
	type snap struct {
		counts collect.WorkerCounts
		miss   map[string]string
	}
	readable := make([]collect.WorkerTarget, 0, len(targets))
	for _, w := range targets {
		if w.Address != "" {
			readable = append(readable, w)
		}
	}
	snapshot := func() snap {
		c, m := collect.FetchCompleted(ctx, tgt.hc, readable)
		return snap{c, m}
	}
	startCh := make(chan snap, 1)
	if o.Warmup == 0 {
		startCh <- snapshot()
	} else {
		go func() {
			select {
			case <-time.After(time.Until(t0.Add(o.Warmup))):
				startCh <- snapshot()
			case <-ctx.Done():
				startCh <- snap{}
			}
		}()
	}

	say(out, "  driving %s for %v (+%v warm-up)\n", loadWord(o), o.Duration, o.Warmup)
	res, runErr := driver.Run(ctx, cfg)
	stopSampler()
	<-samplerDone
	if runErr != nil {
		return report.Result{}, nil, fmt.Errorf("interrupted: %w", runErr)
	}
	end := snapshot()
	var start snap
	select {
	case start = <-startCh:
	case <-time.After(5 * time.Second):
	}
	for _, m := range []map[string]string{start.miss, end.miss} {
		for k, v := range m {
			missing[k] = v
		}
	}

	in := report.Input{
		RunID: id, Metadata: meta, Records: res.Records, Window: res.Window, Targets: targets,
		StartCounts: start.counts, EndCounts: end.counts, StatsMissing: missing, Sampled: sampler != nil,
		Notes: notes(o, res, tgt),
	}
	if sampler != nil {
		in.Samples, in.SamplePollsFailed, in.SamplesDropped = sampler.Samples()
	}
	return report.Build(in), res.Records, nil
}

func loadWord(o Options) string {
	if o.Mode() == driver.Open {
		return fmt.Sprintf("open loop at %g requests/s", o.Rate)
	}
	return fmt.Sprintf("closed loop with %d clients", o.Concurrency)
}

// notes are the caveats attached to a result.
func notes(o Options, res *driver.Output, tgt *target) []string {
	var n []string
	if res.Truncated {
		n = append(n, fmt.Sprintf("The run stopped early at the cap of %d requests (--max-requests); the window was cut to %.1fs.",
			o.MaxRequests, res.Window.End.Seconds()))
	}
	if o.Mode() == driver.Open && res.MaxStartLag > 100*time.Millisecond {
		n = append(n, fmt.Sprintf("The load generator started some requests up to %v after they were due (in-flight limit or machine load); latency includes that wait.",
			res.MaxStartLag.Round(time.Millisecond)))
	}
	if o.Warmup > 0 && tgt.cp != nil {
		n = append(n, "Per-worker completed counts start at the end of warm-up, so requests sent in warm-up that finish later are counted in them.")
	}
	if o.Mode() == driver.Closed {
		n = append(n, "Closed-loop clients wait for each response before sending the next, so a slow server also slows the offered load; use --rate (open loop) to offer a fixed load whatever the server does.")
	}
	return n
}

// target is what a run talks to.
type target struct {
	gatewayURL string
	cp         *client.Client
	cluster    *embedded.Cluster
	hc         *http.Client
	allowAll   bool
}

func (t *target) close() {
	if t.cluster != nil {
		t.cluster.Close()
	}
}

// workers lists the workers whose /stats can be read, and why others cannot.
func (t *target) workers(ctx context.Context) ([]collect.WorkerTarget, map[string]string) {
	missing := map[string]string{}
	if t.cluster != nil {
		var out []collect.WorkerTarget
		for _, w := range t.cluster.Workers {
			out = append(out, collect.WorkerTarget{ID: w.ID, Model: w.Model, Address: w.URL})
		}
		return out, missing
	}
	if t.cp == nil {
		return nil, missing
	}
	ws, err := t.cp.Workers(ctx, client.Query{})
	if err != nil {
		missing["control-plane"] = err.Error()
		return nil, missing
	}
	var out []collect.WorkerTarget
	for _, w := range ws {
		u, err := url.Parse(w.Address)
		if err != nil || (!config.IsLoopbackHost(u.Hostname()) && !t.allowAll) {
			missing[w.WorkerID] = "worker address is not loopback; pass --allow-remote to read its /stats"
			out = append(out, collect.WorkerTarget{ID: w.WorkerID, Model: w.Model, Address: ""})
			continue
		}
		out = append(out, collect.WorkerTarget{ID: w.WorkerID, Model: w.Model, Address: w.Address})
	}
	return out, missing
}

func connect(ctx context.Context, o Options, wl *workload.Workload, out io.Writer) (*target, error) {
	hc := &http.Client{Timeout: 5 * time.Second, Transport: driver.DefaultClient(4).Transport}
	t := &target{hc: hc, allowAll: o.AllowRemote}
	if o.Embedded() {
		say(out, "  booting embedded cluster: %d workers (%s), scheduler %s\n", o.Workers, o.Profile, o.Scheduler)
		c, err := embedded.Start(ctx, o.EmbeddedConfig(wl.ModelNames()))
		if err != nil {
			return nil, fmt.Errorf("embedded cluster: %w", err)
		}
		t.cluster, t.gatewayURL, t.cp = c, c.GatewayURL, c.ControlPlane()
		return t, nil
	}
	t.gatewayURL = strings.TrimRight(o.TargetURL, "/")
	if err := preflight(ctx, hc, t.gatewayURL); err != nil {
		return nil, err
	}
	if o.ControlPlaneURL != "" {
		t.cp = client.New(o.ControlPlaneURL, o.ControlPlaneToken, nil)
	}
	return t, nil
}

// preflight checks a remote gateway answers before a long run is started.
func preflight(ctx context.Context, hc *http.Client, base string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", nil)
	if err != nil {
		return errors.New("--target is not a usable URL")
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("target is not reachable: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("target /healthz answered %d, expected 200", resp.StatusCode)
	}
	return nil
}

// metadata assembles what spec section 32 asks a run to persist.
func metadata(ctx context.Context, o Options, wl *workload.Workload, t *target, rep report.Repeat) report.Metadata {
	m := report.Metadata{
		Scheduler: o.Scheduler, Models: wl.ModelNames(), GPUType: report.Unknown, Mode: string(o.Mode()),
		Concurrency: o.Concurrency, Rate: o.Rate, MaxInFlight: o.MaxInFlight,
		DurationSecs: o.Duration.Seconds(), WarmupSecs: o.Warmup.Seconds(), Seed: o.Seed, Workload: o.Workload,
		StreamRatio: o.StreamRatio, PlanDigest: wl.Digest(MaxPlanDigest), Repeat: rep, Target: "remote", TargetHost: report.Unknown,
	}
	for _, c := range wl.Classes() {
		m.Prompt = append(m.Prompt, report.PromptClass{Name: c.Name, Weight: c.Weight, InputMin: c.InMin, InputMax: c.InMax, OutputMin: c.OutMin, OutputMax: c.OutMax})
	}
	m.Environment(ctx, ".", time.Now())
	if t.cluster != nil {
		m.Target, m.TargetHost, m.GPUType = "embedded", "loopback", "none (mock workers)"
		n := len(t.cluster.Workers)
		m.WorkerCount = &n
		c := &report.Cluster{Profile: o.Profile, HeartbeatMillis: embedded.HeartbeatInterval.Milliseconds(), RegistryRefreshMs: embedded.RegistryRefresh.Milliseconds()}
		for _, w := range t.cluster.Workers {
			c.Workers = append(c.Workers, report.ClusterWorker{ID: w.ID, Model: w.Model, TokensPerSecond: w.TokensPerSecond,
				TTFTMillis: w.TTFT.Milliseconds(), MaxConcurrency: w.MaxConcurrency, QueueSize: w.QueueSize})
		}
		m.Cluster = c
		return m
	}
	if u, err := url.Parse(o.TargetURL); err == nil {
		m.TargetHost = u.Host // never the full URL: it could carry a path or query
	}
	if o.GPUType != "" {
		m.GPUType = o.GPUType
	}
	if o.Workers > 0 {
		n := o.Workers
		m.WorkerCount = &n
	}
	return m
}

// say writes progress; a failing progress writer must not fail a benchmark.
func say(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
