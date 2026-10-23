package mockworker

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"serverflow/internal/sim/engine"
)

// ParseFlags builds a validated Config from command-line arguments (without
// the program name) and returns the requested log level. When --seed is not
// given a random seed is chosen; the caller logs it so a run can be repeated.
func ParseFlags(args []string, usage io.Writer) (Config, string, error) {
	cfg := DefaultConfig()
	var mode, logLevel string
	fs := flag.NewFlagSet("mock-worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // errors are reported once, by the caller
	fs.StringVar(&cfg.Model, "model", cfg.Model, "model name this worker serves")
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	fs.StringVar(&cfg.WorkerID, "worker-id", "", "worker ID (default: derived from the listen port)")
	fs.DurationVar(&cfg.TTFT, "ttft", cfg.TTFT, "time to first token once a request starts running")
	fs.Float64Var(&cfg.TokensPerSecond, "tokens-per-second", cfg.TokensPerSecond, "generation speed per request")
	fs.IntVar(&cfg.OutputTokens, "output-tokens", cfg.OutputTokens, "tokens generated per request (capped by max_tokens)")
	fs.IntVar(&cfg.MaxConcurrency, "max-concurrency", cfg.MaxConcurrency, "requests generating at once")
	fs.IntVar(&cfg.QueueSize, "queue-size", cfg.QueueSize, "requests allowed to wait; more are rejected with 503")
	fs.Float64Var(&cfg.FailureRate, "failure-rate", cfg.FailureRate, "fraction of requests that fail, 0 to 1")
	fs.StringVar(&mode, "failure-mode", string(cfg.FailureMode), "how failures look: error, unavailable, drop, midstream")
	fs.Int64Var(&cfg.Seed, "seed", 0, "random seed for failure injection (default: random, logged at startup)")
	fs.DurationVar(&cfg.StartupDelay, "startup-delay", cfg.StartupDelay, "stay not-ready for this long after starting")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "how long SIGTERM waits for in-flight requests")
	fs.DurationVar(&cfg.DrainAnnounce, "drain-announce", cfg.DrainAnnounce, "on SIGTERM report draining but keep serving this long before refusing (0: refuse at once); set it above the heartbeat plus the gateway's registry refresh")
	fs.StringVar(&cfg.OTLPEndpoint, "otlp-endpoint", cfg.OTLPEndpoint, "OTLP/HTTP base URL to export traces to, e.g. http://127.0.0.1:4318 (default: tracing off)")
	fs.BoolVar(&cfg.TraceInsecureOK, "trace-insecure-ok", cfg.TraceInsecureOK, "allow plaintext http:// to a non-loopback --otlp-endpoint")
	fs.StringVar(&cfg.ModelEngine, "model-engine", cfg.ModelEngine, "how generation is timed: simple (the default; constant ttft and tokens-per-second) or sim (continuous batching, prefill vs decode cost, KV budget, noise; a simulator, see docs/development/mock-worker.md)")
	fs.StringVar(&cfg.SimProfile, "profile", "", "with --model-engine=sim: built-in profile ("+strings.Join(engine.ProfileNames(), ", ")+"; default: default)")
	fs.StringVar(&cfg.SimParamsFile, "sim-params", "", "with --model-engine=sim: path of a JSON parameter file instead of a profile (its SHA-256 is logged at startup)")
	fs.StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn, error")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(usage)
			_, _ = fmt.Fprintln(usage, "Usage: mock-worker [flags]")
			fs.PrintDefaults()
		}
		return Config{}, "", err
	}
	if fs.NArg() > 0 {
		return Config{}, "", fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	cfg.FailureMode = FailureMode(mode)
	seedSet := false
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true; seedSet = seedSet || f.Name == "seed" })
	if !seedSet {
		cfg.Seed = time.Now().UnixNano()
	}
	if cfg.ModelEngine == ModelEngineSim {
		for _, name := range []string{"ttft", "tokens-per-second"} {
			if given[name] {
				return Config{}, "", fmt.Errorf("--%s has no effect with --model-engine=sim (timing comes from the profile); remove it", name)
			}
		}
		if err := resolveSim(&cfg); err != nil {
			return Config{}, "", err
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, "", err
	}
	return cfg, logLevel, nil
}

// resolveSim turns --profile or --sim-params into cfg.Sim.
func resolveSim(cfg *Config) error {
	switch {
	case cfg.SimProfile != "" && cfg.SimParamsFile != "":
		return errors.New("--profile and --sim-params are mutually exclusive")
	case cfg.SimParamsFile != "":
		f, err := os.Open(cfg.SimParamsFile)
		if err != nil {
			return fmt.Errorf("--sim-params: %w", err)
		}
		defer func() { _ = f.Close() }()
		p, err := engine.ParseParams(f)
		if err != nil {
			return fmt.Errorf("--sim-params %s: %w", cfg.SimParamsFile, err)
		}
		cfg.Sim = &p
	case cfg.SimProfile != "":
		p, ok := engine.Profile(cfg.SimProfile)
		if !ok {
			return fmt.Errorf("--profile %q is not a built-in profile (%s)", cfg.SimProfile, strings.Join(engine.ProfileNames(), ", "))
		}
		cfg.Sim = &p
	}
	return nil
}
