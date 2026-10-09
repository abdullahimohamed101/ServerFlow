// Package runner turns validated options into benchmark runs: it boots or finds the
// target, drives the workload, samples the workers, and writes the results. Flag parsing
// and every safety check live here so they can be tested; cmd/benchmark only wires them to
// the command line.
package runner

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"serverflow/internal/bench/driver"
	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/workload"
	"serverflow/internal/config"
	"serverflow/internal/scheduler"
)

// Defaults and caps. The caps stop a typo from flooding the machine or the target; flags
// raise them deliberately.
const (
	DefaultMaxConcurrency = 2000
	DefaultMaxRate        = 10000.0
	DefaultConcurrency    = 16
	DefaultMaxErrorRate   = 0.05
	MinDuration           = 100 * time.Millisecond
	MaxDuration           = 2 * time.Hour
	MaxWarmup             = 10 * time.Minute
	MaxRepeat             = 20
	// MaxPlanDigest is how many requests the plan digest covers.
	MaxPlanDigest = 1000
)

var (
	modelPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
	schedulerPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
)

// Options is everything a run needs. Zero values are not defaults: ParseRun fills those.
type Options struct {
	// TargetURL is a running gateway; empty means boot an embedded cluster.
	TargetURL string
	// ControlPlaneURL and ControlPlaneToken let the sampler read a remote target's workers.
	ControlPlaneURL   string
	ControlPlaneToken string
	AllowRemote       bool

	Scheduler string
	// Workers is the embedded worker count; for a remote target it is the count the user
	// declares for the metadata (0 for unknown).
	Workers        int
	Profile        string
	MockTPS        float64
	MockTTFT       time.Duration
	MockConcurrent int
	MockQueue      int
	GPUType        string

	Workload    string
	Seed        int64
	Model       string
	Secondary   string
	StreamRatio float64

	Concurrency int
	Rate        float64
	MaxInFlight int
	Duration    time.Duration
	Warmup      time.Duration
	Repeat      int

	RequestTimeout time.Duration
	DrainTimeout   time.Duration
	SampleInterval time.Duration

	MaxConcurrency int
	MaxRate        float64
	MaxRequests    int

	OutDir       string
	SaveRequests bool

	// MaxErrorRate is the error rate above which a run is invalid (a failing run exits non-zero
	// unless AllowErrors). AllowOverload lets an embedded closed-loop run use more clients than
	// the workers have slots. Verbose shows the embedded cluster's logs.
	MaxErrorRate  float64
	AllowErrors   bool
	AllowOverload bool
	Verbose       bool
}

// Embedded reports whether the run boots its own cluster.
func (o Options) Embedded() bool { return o.TargetURL == "" }

// Mode is the load mode the options select.
func (o Options) Mode() driver.Mode {
	if o.Rate > 0 {
		return driver.Open
	}
	return driver.Closed
}

// EmbeddedConfig is the embedded cluster the options describe, serving models.
func (o Options) EmbeddedConfig(models []string) embedded.Config {
	return embedded.Config{Scheduler: o.Scheduler, Workers: o.Workers, Profile: o.Profile, Models: models,
		TokensPerSecond: o.MockTPS, TTFT: o.MockTTFT, MaxConcurrency: o.MockConcurrent, QueueSize: o.MockQueue}
}

// Models returns the workload's model pair.
func (o Options) Models() workload.Models {
	return workload.Models{Primary: o.Model, Secondary: o.Secondary}
}

// SafeSlotFraction is the share of an embedded cluster's request slots that closed-loop clients
// can use before the run is likely to be invalid. Round-robin and random ignore load, so they
// send a slow worker its share of requests however busy it is, and a worker whose slots are all
// taken makes the gateway answer 503; heterogeneous workers and load balancing from heartbeats
// that are a second old make it worse. 100% of the slots is refused outright; above this share
// the run only warns.
const SafeSlotFraction = 0.6

// LoadWarning returns a warning when an embedded closed-loop run uses more than SafeSlotFraction
// of the cluster's slots, and "" otherwise.
func (o Options) LoadWarning() string {
	if !o.Embedded() || o.Concurrency <= 0 {
		return ""
	}
	wl, err := workload.New(workload.Spec{Name: o.Workload, Seed: o.Seed, Models: o.Models(), StreamRatio: o.StreamRatio})
	if err != nil {
		return ""
	}
	specs, err := embedded.Plan(o.EmbeddedConfig(wl.ModelNames()))
	if err != nil {
		return ""
	}
	slots := embedded.Slots(specs)
	if float64(o.Concurrency) <= SafeSlotFraction*float64(slots) {
		return ""
	}
	return fmt.Sprintf("%d clients on %d request slots is %.0f%% of them (above %.0f%%): a scheduler that ignores load will fill a slow worker's slots, the gateway will answer 503, "+
		"and this run will probably be invalid (error rate above %.0f%%). Use at most %d clients, or add workers or --mock-concurrency.",
		o.Concurrency, slots, 100*float64(o.Concurrency)/float64(slots), 100*SafeSlotFraction, 100*o.MaxErrorRate, int(SafeSlotFraction*float64(slots)))
}

// Validate refuses unsafe or inconsistent options with an error that says what to change.
func (o Options) Validate() error {
	if err := o.validateTarget(); err != nil {
		return err
	}
	if err := o.validateLoad(); err != nil {
		return err
	}
	if !(o.MaxErrorRate >= 0 && o.MaxErrorRate <= 1) {
		return fmt.Errorf("--max-error-rate must be between 0 and 1, got %v", o.MaxErrorRate)
	}
	if o.Repeat < 1 || o.Repeat > MaxRepeat {
		return fmt.Errorf("--repeat must be between 1 and %d, got %d", MaxRepeat, o.Repeat)
	}
	if o.SampleInterval < 50*time.Millisecond || o.SampleInterval > 10*time.Second {
		return fmt.Errorf("--sample-interval must be between 50ms and 10s, got %v", o.SampleInterval)
	}
	if o.RequestTimeout <= 0 || o.DrainTimeout <= 0 {
		return errors.New("--request-timeout and --drain-timeout must be positive")
	}
	if o.OutDir == "" {
		return errors.New("--out must not be empty")
	}
	if o.MaxRequests < 1 {
		return fmt.Errorf("--max-requests must be at least 1, got %d", o.MaxRequests)
	}
	return nil
}

func (o Options) validateTarget() error {
	if !schedulerPattern.MatchString(o.Scheduler) {
		return fmt.Errorf("--scheduler %q is not a valid name", o.Scheduler)
	}
	if o.Embedded() {
		if o.ControlPlaneURL != "" {
			return errors.New("--control-plane applies to --target only; an embedded cluster has its own")
		}
		if !contains(scheduler.Strategies(), o.Scheduler) {
			return fmt.Errorf("--scheduler %q is not a known strategy (choose one of: %s)", o.Scheduler, strings.Join(scheduler.Strategies(), ", "))
		}
		if o.Workers < 1 || o.Workers > embedded.MaxWorkers {
			return fmt.Errorf("--workers must be between 1 and %d, got %d", embedded.MaxWorkers, o.Workers)
		}
		if !(o.MockTPS > 0) || o.MockTTFT <= 0 || o.MockConcurrent < 1 || o.MockQueue < 1 {
			return errors.New("--mock-tps, --mock-ttft, --mock-concurrency and --mock-queue must all be positive")
		}
		if o.Profile != embedded.Identical && o.Profile != embedded.Heterogeneous {
			return fmt.Errorf("--worker-profile %q is not valid (choose %s or %s)", o.Profile, embedded.Identical, embedded.Heterogeneous)
		}
	} else {
		if err := o.checkHost("--target", o.TargetURL); err != nil {
			return err
		}
		if o.ControlPlaneURL != "" {
			if err := o.checkHost("--control-plane", o.ControlPlaneURL); err != nil {
				return err
			}
		}
		if o.Workers < 0 || o.Workers > 100000 {
			return fmt.Errorf("--workers must be 0 (unknown) or a positive count, got %d", o.Workers)
		}
	}
	return nil
}

// checkHost requires an http(s) URL without credentials, on loopback unless remote targets
// were explicitly allowed.
func (o Options) checkHost(flag, raw string) error {
	u, err := url.Parse(raw)
	// The URL may carry credentials, so error messages never echo it.
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("%s must be an http:// or https:// URL with a host", flag)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not contain credentials", flag)
	}
	if !config.IsLoopbackHost(u.Hostname()) && !o.AllowRemote {
		return fmt.Errorf("%s host %q is not loopback: load tests can hurt a shared or production system; "+
			"pass --allow-remote only if you own the target and it is meant to take this load", flag, u.Hostname())
	}
	return nil
}

func (o Options) validateLoad() error {
	if o.MaxConcurrency < 1 || o.MaxRate <= 0 || math.IsNaN(o.MaxRate) {
		return errors.New("--max-concurrency and --max-rate must be positive")
	}
	if !modelPattern.MatchString(o.Model) {
		return fmt.Errorf("--model %q is not a valid model name", o.Model)
	}
	if o.Workload == workload.HotModel && !modelPattern.MatchString(o.Secondary) {
		return fmt.Errorf("--secondary-model %q is not a valid model name", o.Secondary)
	}
	wl, err := workload.New(workload.Spec{Name: o.Workload, Seed: o.Seed, Models: o.Models(), StreamRatio: o.StreamRatio})
	if err != nil {
		return err
	}
	if o.Duration < MinDuration || o.Duration > MaxDuration {
		return fmt.Errorf("--duration must be between %v and %v, got %v", MinDuration, MaxDuration, o.Duration)
	}
	if o.Warmup < 0 || o.Warmup > MaxWarmup {
		return fmt.Errorf("--warmup must be between 0 and %v, got %v", MaxWarmup, o.Warmup)
	}
	if o.Workload == workload.Burst && o.Rate == 0 {
		return errors.New("the burst workload changes the arrival rate, so it needs open-loop mode: use --rate instead of --concurrency")
	}
	if o.Concurrency > 0 && o.Rate > 0 {
		return errors.New("--concurrency (closed loop) and --rate (open loop) are mutually exclusive: choose one")
	}
	if o.Rate == 0 && o.Concurrency == 0 {
		return errors.New("choose a load: --concurrency N (closed loop) or --rate R (open loop)")
	}
	if o.Concurrency < 0 || o.Rate < 0 || math.IsNaN(o.Rate) || math.IsInf(o.Rate, 0) {
		return errors.New("--concurrency and --rate must not be negative")
	}
	if o.Concurrency > o.MaxConcurrency {
		return fmt.Errorf("--concurrency %d is above the cap of %d; raise it with --max-concurrency if you mean it", o.Concurrency, o.MaxConcurrency)
	}
	if o.Embedded() {
		specs, err := embedded.Plan(o.EmbeddedConfig(wl.ModelNames()))
		if err != nil {
			return fmt.Errorf("embedded cluster: %w", err)
		}
		if slots := embedded.Slots(specs); o.Concurrency > slots && !o.AllowOverload {
			return fmt.Errorf("--concurrency %d is above the %d request slots of %d workers x %d concurrent requests: the gateway would answer 503 "+
				"to the excess and the run would measure rejections, not the scheduler. Use at most %d clients, add workers or --mock-concurrency, "+
				"or pass --allow-overload to run it anyway", o.Concurrency, slots, o.Workers, o.MockConcurrent, slots)
		}
	}
	if o.Rate == 0 {
		return nil
	}
	peak := o.Rate
	if o.Workload == workload.Burst {
		peak *= workload.BurstFactor
	}
	if peak > o.MaxRate {
		if o.Workload == workload.Burst {
			return fmt.Errorf("the peak rate %g/s is above the cap of %g/s (the burst workload peaks at %dx --rate); raise it with --max-rate if you mean it", peak, o.MaxRate, workload.BurstFactor)
		}
		return fmt.Errorf("--rate %g is above the cap of %g/s; raise it with --max-rate if you mean it", peak, o.MaxRate)
	}
	if o.MaxInFlight < 1 || o.MaxInFlight > o.MaxConcurrency {
		return fmt.Errorf("--max-inflight must be between 1 and %d, got %d", o.MaxConcurrency, o.MaxInFlight)
	}
	if n := workload.TotalArrivals(workload.Segments(o.Workload, o.Rate, o.Warmup, o.Duration)); n > float64(o.MaxRequests) {
		return fmt.Errorf("the schedule offers %.0f requests, above --max-requests %d (it bounds memory); shorten the run or raise the cap", n, o.MaxRequests)
	}
	return nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
