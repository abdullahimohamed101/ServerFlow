package chaos

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/workload"
	"serverflow/internal/scheduler"
)

// Defaults.
const (
	DefaultWorkers        = 4
	DefaultMaxErrorRate   = 0.05
	DefaultRequestTimeout = 30 * time.Second
	MaxRepeat             = 20
	MinScale              = 0.05
	MaxScale              = 10
)

// Options is a chaos run.
type Options struct {
	// Scenario is a name from Scenarios, or "all".
	Scenario string
	// Scale multiplies the scenario's durations (a bucket is a second times Scale); 1 is the real thing.
	Scale float64

	Workers        int
	MockConcurrent int
	MockTPS        float64
	MockTTFT       time.Duration
	// Concurrency is the number of closed-loop clients; 0 chooses a safe share of the surviving slots.
	Concurrency int

	Scheduler   string
	Workload    string
	Model       string
	StreamRatio float64
	Seed        int64
	Repeat      int

	// Resilience is "on", "off" or "" for the build's default; a no-op (recorded as absent) on a build
	// without the feature.
	Resilience string

	// MaxErrorRate is the baseline window's error rate above which the run is invalid. MinRequests is the
	// fewest requests a window may hold (0: DefaultMinRequests(Scale)).
	MaxErrorRate float64
	MinRequests  int
	// AllowDirty waives the clean-tree check, for development runs.
	AllowDirty bool

	OutDir string
	// SaveRequests also writes requests.jsonl with every request (megabytes per run).
	SaveRequests bool
	Verbose      bool
}

// DefaultMinRequests is the fewest requests a window must hold at a time scale.
func DefaultMinRequests(scale float64) int { return max(20, int(100*scale)) }

// Validate refuses an unusable configuration.
func (o *Options) Validate() error {
	if o.Scenario != "all" {
		if _, err := Lookup(o.Scenario); err != nil {
			return err
		}
	}
	switch {
	case !(o.Scale >= MinScale && o.Scale <= MaxScale):
		return fmt.Errorf("--scale must be between %g and %d, got %v", MinScale, MaxScale, o.Scale)
	case o.Workers < 2 || o.Workers > embedded.MaxWorkers:
		return fmt.Errorf("--workers must be between 2 and %d (a chaos run needs a worker left over), got %d", embedded.MaxWorkers, o.Workers)
	case o.MockConcurrent < 1 || !(o.MockTPS > 0) || o.MockTTFT <= 0:
		return errors.New("--mock-concurrency, --mock-tps and --mock-ttft must be positive")
	case o.Concurrency < 0:
		return errors.New("--concurrency must not be negative")
	case o.Repeat < 1 || o.Repeat > MaxRepeat:
		return fmt.Errorf("--repeat must be between 1 and %d, got %d", MaxRepeat, o.Repeat)
	case !(o.MaxErrorRate >= 0 && o.MaxErrorRate <= 1):
		return fmt.Errorf("--max-error-rate must be between 0 and 1, got %v", o.MaxErrorRate)
	case o.MinRequests < 0:
		return errors.New("--min-requests must not be negative")
	case o.OutDir == "":
		return errors.New("--out must not be empty")
	}
	if !contains(scheduler.Strategies(), o.Scheduler) {
		return fmt.Errorf("--scheduler %q is not a known strategy (choose one of: %s)", o.Scheduler, strings.Join(scheduler.Strategies(), ", "))
	}
	if o.Model == "" {
		return errors.New("--model must not be empty")
	}
	if _, err := workload.New(workload.Spec{Name: o.Workload, Seed: o.Seed, Models: workload.Models{Primary: o.Model}, StreamRatio: o.StreamRatio}); err != nil {
		return err
	}
	if o.Workload == workload.Burst {
		return errors.New("the burst workload needs open-loop mode, which chaos runs do not use")
	}
	return embedded.ValidResilience(o.Resilience)
}

func (o *Options) minRequests() int {
	if o.MinRequests > 0 {
		return o.MinRequests
	}
	return DefaultMinRequests(o.Scale)
}

// ParseFlags parses `benchmark chaos`. list is true for -list.
func ParseFlags(args []string) (o Options, list bool, err error) {
	fs := flag.NewFlagSet("benchmark chaos", flag.ContinueOnError)
	var usage bytes.Buffer
	fs.SetOutput(&usage)
	fs.StringVar(&o.Scenario, "scenario", "", "scenario to run: one of -list, or all")
	fs.BoolVar(&list, "list", false, "list the scenarios, what each shows and what it cannot show")
	fs.Float64Var(&o.Scale, "scale", 1, "multiply every duration of the scenario (a window bucket is a second times this); below 1 for a quick look, 1 for the record")
	fs.IntVar(&o.Workers, "workers", DefaultWorkers, "embedded mock workers")
	fs.IntVar(&o.MockConcurrent, "mock-concurrency", embedded.DefaultMaxConcurrency, "embedded mock worker concurrent requests")
	fs.Float64Var(&o.MockTPS, "mock-tps", embedded.DefaultTokensPerSecond, "embedded mock worker speed in tokens/s")
	fs.DurationVar(&o.MockTTFT, "mock-ttft", embedded.DefaultTTFT, "embedded mock worker time to first token")
	fs.IntVar(&o.Concurrency, "concurrency", 0, "closed-loop clients (default: 60% of the slots left serving once the fault takes a worker)")
	fs.StringVar(&o.Scheduler, "scheduler", scheduler.RoundRobin, "scheduling strategy of the embedded gateway")
	fs.StringVar(&o.Workload, "workload", workload.UniformShort, "workload (short requests keep every window full)")
	fs.StringVar(&o.Model, "model", "qwen-7b", "model name requests ask for")
	fs.Float64Var(&o.StreamRatio, "stream-ratio", 0.5, "fraction of requests that stream")
	fs.Int64Var(&o.Seed, "seed", 1, "random seed; the same seed offers the same load")
	fs.IntVar(&o.Repeat, "repeat", 1, "run each scenario this many times (3 for a comparison: the spread rule needs it)")
	fs.StringVar(&o.Resilience, "resilience", "", "on or off for the resilience feature (default: the build's default). Recorded as the configured mode. Until Phase 15 PR D the gateway does not read the setting, so on and off behave the same; on a build without the resilience configuration it is a no-op recorded as \"absent\"")
	fs.Float64Var(&o.MaxErrorRate, "max-error-rate", DefaultMaxErrorRate, "baseline window error rate above which the run is invalid")
	fs.IntVar(&o.MinRequests, "min-requests", 0, "fewest requests a window may hold (default: 100 times --scale, at least 20)")
	fs.BoolVar(&o.AllowDirty, "allow-dirty", false, "waive the clean-working-tree check of the validity gate (development runs)")
	fs.StringVar(&o.OutDir, "out", "benchmark/runs", "directory for run_NNN result directories")
	fs.BoolVar(&o.SaveRequests, "save-requests", false, "also write requests.jsonl with every request (large)")
	fs.BoolVar(&o.Verbose, "verbose", false, "show the embedded cluster's logs on stderr")
	if err := fs.Parse(args); err != nil {
		return o, false, fmt.Errorf("%w\n\n%s", err, usage.String())
	}
	if fs.NArg() > 0 {
		return o, false, fmt.Errorf("unexpected argument %q (flags only)\n\n%s", fs.Arg(0), usage.String())
	}
	if list {
		return o, true, nil
	}
	if o.Scenario == "" {
		return o, false, fmt.Errorf("choose a scenario with --scenario (one of: %s, or all; -list explains them)\n\n%s", strings.Join(Names(), ", "), usage.String())
	}
	return o, false, o.Validate()
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
