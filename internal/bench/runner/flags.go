package runner

import (
	"bytes"
	"flag"
	"fmt"
	"time"

	"serverflow/internal/bench/driver"
	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/workload"
	"serverflow/internal/scheduler"
)

// RunFlags is the parsed command line of `benchmark run`.
type RunFlags struct {
	Options    Options
	ConfigPath string
}

// ParseRun parses and validates the arguments of `benchmark run`. On a bad flag the error
// carries the usage text.
func ParseRun(args []string) (RunFlags, error) {
	var rf RunFlags
	o := &rf.Options
	fs := flag.NewFlagSet("benchmark run", flag.ContinueOnError)
	var usage bytes.Buffer
	fs.SetOutput(&usage)

	fs.StringVar(&o.TargetURL, "target", "", "URL of a running gateway to test (default: boot an embedded simulated cluster)")
	embeddedFlag := fs.Bool("embedded", false, "boot an embedded simulated cluster (the default without --target)")
	fs.StringVar(&o.ControlPlaneURL, "control-plane", "", "control plane URL, so queue depth and worker balance can be measured for --target (token: SERVERFLOW_CONTROL_PLANE_TOKEN)")
	fs.BoolVar(&o.AllowRemote, "allow-remote", false, "allow a --target or --control-plane that is not on loopback (you own it and it should take this load)")
	fs.StringVar(&rf.ConfigPath, "config", "", "optional YAML configuration file (supplies the control plane token)")

	fs.StringVar(&o.Scheduler, "scheduler", scheduler.RoundRobin, "scheduling strategy for the embedded gateway; recorded as given for --target")
	fs.IntVar(&o.Workers, "workers", 4, "embedded mock workers (for --target: the count to record, 0 = unknown)")
	fs.StringVar(&o.Profile, "worker-profile", embedded.Identical, "embedded workers: identical or heterogeneous (fast, medium and slow in turn)")
	fs.Float64Var(&o.MockTPS, "mock-tps", embedded.DefaultTokensPerSecond, "embedded mock worker base speed in tokens/s")
	fs.DurationVar(&o.MockTTFT, "mock-ttft", embedded.DefaultTTFT, "embedded mock worker base time to first token")
	fs.IntVar(&o.MockConcurrent, "mock-concurrency", embedded.DefaultMaxConcurrency, "embedded mock worker concurrent requests")
	fs.IntVar(&o.MockQueue, "mock-queue", embedded.DefaultQueueSize, "embedded mock worker queue size")
	fs.StringVar(&o.GPUType, "gpu-type", "", "GPU type to record for --target (embedded mock workers have none)")

	fs.StringVar(&o.Workload, "workload", workload.Mixed, "workload: uniform-short, uniform-long, mixed, burst, hot-model, multi-tenant")
	fs.Int64Var(&o.Seed, "seed", 1, "random seed; the same seed offers the same load")
	fs.StringVar(&o.Model, "model", "qwen-7b", "model name requests ask for")
	fs.StringVar(&o.Secondary, "secondary-model", "llama-8b", "second model, for hot-model")
	fs.Float64Var(&o.StreamRatio, "stream-ratio", 0.5, "fraction of requests that stream (TTFT is measured on these)")

	fs.IntVar(&o.Concurrency, "concurrency", 0, "closed loop: this many clients, each sending when its last request ends (default 16 without --rate)")
	fs.Float64Var(&o.Rate, "rate", 0, "open loop: requests per second, sent on schedule whatever the responses do")
	fs.IntVar(&o.MaxInFlight, "max-inflight", 0, "open loop: most requests in flight at once (default: --max-concurrency)")
	fs.DurationVar(&o.Duration, "duration", 30*time.Second, "length of the measurement window")
	fs.DurationVar(&o.Warmup, "warmup", 5*time.Second, "load sent before the window that is not measured")
	fs.IntVar(&o.Repeat, "repeat", 1, "run the same configuration this many times to show run-to-run spread")

	fs.DurationVar(&o.RequestTimeout, "request-timeout", driver.DefaultRequestTimeout, "give up on one request after this long")
	fs.DurationVar(&o.DrainTimeout, "drain-timeout", driver.DefaultDrainTimeout, "after the window, wait this long for requests in flight")
	fs.DurationVar(&o.SampleInterval, "sample-interval", 250*time.Millisecond, "how often to read worker queue depth from the control plane")

	fs.IntVar(&o.MaxConcurrency, "max-concurrency", DefaultMaxConcurrency, "cap on --concurrency and --max-inflight")
	fs.Float64Var(&o.MaxRate, "max-rate", DefaultMaxRate, "cap on the request rate, including the burst peak")
	fs.IntVar(&o.MaxRequests, "max-requests", driver.DefaultMaxRequests, "cap on requests per run (bounds memory)")

	fs.StringVar(&o.OutDir, "out", "benchmark/runs", "directory for run_NNN result directories")
	fs.BoolVar(&o.SaveRequests, "save-requests", false, "also write requests.jsonl with every request (large for long runs)")

	if err := fs.Parse(args); err != nil {
		return rf, fmt.Errorf("%w\n\n%s", err, usage.String())
	}
	if fs.NArg() > 0 {
		return rf, fmt.Errorf("unexpected argument %q (flags only)\n\n%s", fs.Arg(0), usage.String())
	}
	if *embeddedFlag && o.TargetURL != "" {
		return rf, fmt.Errorf("--embedded and --target are mutually exclusive")
	}
	if o.Concurrency == 0 && o.Rate == 0 && o.Workload != workload.Burst {
		o.Concurrency = DefaultConcurrency
	}
	if o.Rate > 0 && o.MaxInFlight == 0 {
		o.MaxInFlight = o.MaxConcurrency
	}
	return rf, o.Validate()
}
