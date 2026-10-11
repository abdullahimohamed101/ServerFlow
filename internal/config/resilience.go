package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// resilience.mode values.
const (
	ResilienceOn  = "on"
	ResilienceOff = "off"
)

// ResilienceConfig configures fault tolerance (Phase 15, ADR-020): per-worker circuit breakers, a retry budget,
// a bounded admission queue with load shedding, and gateway drain. See docs/operations/resilience.md.
//
// Mode: the final default is "on" (plan decision D1), but it is flipped in the last Phase 15 pull request. Until
// then the code default is "off", and nothing in the gateway reads this section yet (PR C ships the library and the
// configuration only). The feature takes effect only when gateway.worker_source is "registry" (see Effective);
// with static workers "on" is a valid no-op. Only mode is validated while the feature is not effective, so a
// malformed value cannot stop a gateway that does not use it.
type ResilienceConfig struct {
	Mode    string                  `yaml:"mode"`
	Breaker ResilienceBreakerConfig `yaml:"breaker"`
	Retry   ResilienceRetryConfig   `yaml:"retry"`
	Queue   ResilienceQueueConfig   `yaml:"queue"`
	Drain   ResilienceDrainConfig   `yaml:"drain"`
}

// ResilienceBreakerConfig configures the per-worker circuit breaker. A breaker trips on ConsecutiveFailures faults in
// a row (the first no older than Window) or, with at least MinVolume outcomes in the sliding Window, on a fault
// ratio of FailureRatio or more (0 switches the ratio rule off). It stays open for CoolDown (jittered, doubling on
// each failed probe up to MaxCoolDown), then admits ProbeConcurrency live requests at a time; ProbeSuccesses good
// ones close it. ProbeHeaderTimeout bounds a probe's wait for response headers. MaxOpenFraction caps the share of
// registry-eligible workers that may be open or half-open (0 never ejects).
type ResilienceBreakerConfig struct {
	ConsecutiveFailures int           `yaml:"consecutive_failures"`
	FailureRatio        float64       `yaml:"failure_ratio"`
	MinVolume           int           `yaml:"min_volume"`
	Window              time.Duration `yaml:"window"`
	CoolDown            time.Duration `yaml:"cool_down"`
	MaxCoolDown         time.Duration `yaml:"max_cool_down"`
	ProbeConcurrency    int           `yaml:"probe_concurrency"`
	ProbeSuccesses      int           `yaml:"probe_successes"`
	ProbeHeaderTimeout  time.Duration `yaml:"probe_header_timeout"`
	MaxOpenFraction     float64       `yaml:"max_open_fraction"`
}

// ResilienceRetryConfig configures the retry budget and the deadline of the retryable phase. A retry is allowed
// while the retries in BudgetWindow are fewer than max(BudgetMin, BudgetRatio x first attempts). FirstByteDeadline
// bounds queue wait plus all attempts until the first byte reaches the client; 0 means upstream_header_timeout
// plus upstream_idle_timeout. No retry starts with less than MinRemaining left.
type ResilienceRetryConfig struct {
	BudgetRatio       float64       `yaml:"budget_ratio"`
	BudgetMin         int           `yaml:"budget_min"`
	BudgetWindow      time.Duration `yaml:"budget_window"`
	FirstByteDeadline time.Duration `yaml:"first_byte_deadline"`
	MinRemaining      time.Duration `yaml:"min_remaining"`
}

// ResilienceQueueConfig configures the per-model admission queue. MaxModelDepth 0 means no queue. MaxWait unset (0)
// means min(3s, the effective first-byte deadline); read it through EffectiveMaxWait. MaxTenantShare is
// the largest fraction of a model's depth one authenticated tenant may hold (0 switches the guard off).
type ResilienceQueueConfig struct {
	MaxModelDepth  int           `yaml:"max_model_depth"`
	MaxWait        time.Duration `yaml:"max_wait"`
	MaxTenantShare float64       `yaml:"max_tenant_share"`
}

// ResilienceDrainConfig configures gateway drain: ReadinessGrace is how long the gateway keeps serving after
// /readyz starts failing, before it shuts down. Unset (nil, the default) it is min(5s, shutdown_timeout/2), so a
// gateway whose shutdown_timeout is short still starts; an explicit value, including 0 (no grace), is validated
// against shutdown_timeout. Read it through EffectiveReadinessGrace.
type ResilienceDrainConfig struct {
	ReadinessGrace *time.Duration `yaml:"readiness_grace"`
}

// Defaults that are clamped to the gateway's own settings when the key is not set.
const (
	defaultProbeHeaderTimeout = 10 * time.Second
	defaultReadinessGrace     = 5 * time.Second
	defaultQueueMaxWait       = 3 * time.Second
)

// EffectiveProbeHeaderTimeout returns the probe header timeout in force: the explicit value, or by default
// min(10s, the gateway's upstream header timeout).
func (b ResilienceBreakerConfig) EffectiveProbeHeaderTimeout(upstreamHeader time.Duration) time.Duration {
	if b.ProbeHeaderTimeout != 0 {
		return b.ProbeHeaderTimeout
	}
	return min(defaultProbeHeaderTimeout, upstreamHeader)
}

// EffectiveMaxWait returns the queue wait in force: the explicit value, or by default min(3s, firstByteDeadline) where
// firstByteDeadline is ResilienceRetryConfig.EffectiveFirstByteDeadline.
func (q ResilienceQueueConfig) EffectiveMaxWait(firstByteDeadline time.Duration) time.Duration {
	if q.MaxWait != 0 {
		return q.MaxWait
	}
	return min(defaultQueueMaxWait, firstByteDeadline)
}

// EffectiveReadinessGrace returns the grace in force: the explicit value, or by default min(5s, shutdown/2).
func (d ResilienceDrainConfig) EffectiveReadinessGrace(shutdownTimeout time.Duration) time.Duration {
	if d.ReadinessGrace != nil {
		return *d.ReadinessGrace
	}
	return min(defaultReadinessGrace, shutdownTimeout/2)
}

// Bounds for resilience.*.
const (
	maxConsecutiveFailures = 1000
	maxBreakerMinVolume    = 100000
	minBreakerWindow       = time.Second
	maxBreakerWindow       = 10 * time.Minute
	// minCoolDown is a test floor: scenarios and unit tests shorten the cool-down far below anything sensible.
	minCoolDown       = 100 * time.Millisecond
	maxCoolDown       = time.Hour
	maxProbeSetting   = 16
	minProbeHeader    = 100 * time.Millisecond
	maxBudgetMin      = 1000
	minBudgetWindow   = time.Second
	maxBudgetWindow   = 10 * time.Minute
	maxMinRemaining   = time.Minute
	maxQueueDepth     = 100000
	minQueueMaxWait   = 100 * time.Millisecond
	maxQueueMaxWait   = time.Minute
	maxReadinessGrace = 5 * time.Minute
)

// defaultResilience returns the built-in settings. Mode is "off" until the last Phase 15 pull request flips the
// shipped default to "on" (plan decision D26); every other value is the final default.
func defaultResilience() ResilienceConfig {
	return ResilienceConfig{
		Mode: ResilienceOff,
		Breaker: ResilienceBreakerConfig{
			ConsecutiveFailures: 5,
			FailureRatio:        0.5,
			MinVolume:           10,
			Window:              10 * time.Second,
			CoolDown:            10 * time.Second,
			MaxCoolDown:         2 * time.Minute,
			ProbeConcurrency:    1,
			ProbeSuccesses:      1,
			MaxOpenFraction:     0.5,
		},
		Retry: ResilienceRetryConfig{
			BudgetRatio:  0.2,
			BudgetMin:    10,
			BudgetWindow: 10 * time.Second,
			MinRemaining: time.Second,
		},
		Queue: ResilienceQueueConfig{
			MaxModelDepth:  128,
			MaxTenantShare: 0.5,
		},
		Drain: ResilienceDrainConfig{},
	}
}

// Effective reports whether the feature takes effect: the mode is "on" and the gateway routes through the
// registry. With static workers there is nothing to eject, queue for or drain, so "on" is a no-op.
func (r ResilienceConfig) Effective(workerSource string) bool {
	return r.Mode == ResilienceOn && workerSource == WorkerSourceRegistry
}

// EffectiveFirstByteDeadline returns the effective deadline of the retryable phase for the given upstream timeouts.
func (r ResilienceRetryConfig) EffectiveFirstByteDeadline(header, idle time.Duration) time.Duration {
	if r.FirstByteDeadline > 0 {
		return r.FirstByteDeadline
	}
	return header + idle
}

func inRange01(f float64) bool { return f >= 0 && f <= 1 } // false for NaN

// validateResilience checks resilience.*. Only the mode is checked while the feature is not effective.
func (c *Config) validateResilience() error {
	r := &c.Resilience
	switch r.Mode {
	case ResilienceOn, ResilienceOff:
	default:
		return fmt.Errorf("resilience.mode must be %q or %q, got %q", ResilienceOn, ResilienceOff, truncateForError(r.Mode))
	}
	if !r.Effective(c.Gateway.WorkerSource) {
		return nil
	}
	b := &r.Breaker
	if b.ConsecutiveFailures < 1 || b.ConsecutiveFailures > maxConsecutiveFailures {
		return fmt.Errorf("resilience.breaker.consecutive_failures must be in 1-%d, got %d", maxConsecutiveFailures, b.ConsecutiveFailures)
	}
	if !inRange01(b.FailureRatio) {
		return fmt.Errorf("resilience.breaker.failure_ratio must be between 0 and 1")
	}
	if b.MinVolume < 1 || b.MinVolume > maxBreakerMinVolume {
		return fmt.Errorf("resilience.breaker.min_volume must be in 1-%d, got %d", maxBreakerMinVolume, b.MinVolume)
	}
	if b.Window < minBreakerWindow || b.Window > maxBreakerWindow {
		return fmt.Errorf("resilience.breaker.window must be between %v and %v", minBreakerWindow, maxBreakerWindow)
	}
	if b.CoolDown < minCoolDown || b.CoolDown > maxCoolDown {
		return fmt.Errorf("resilience.breaker.cool_down must be between %v and %v", minCoolDown, maxCoolDown)
	}
	if b.MaxCoolDown < b.CoolDown || b.MaxCoolDown > maxCoolDown {
		return fmt.Errorf("resilience.breaker.max_cool_down must be between resilience.breaker.cool_down and %v", maxCoolDown)
	}
	if b.ProbeConcurrency < 1 || b.ProbeConcurrency > maxProbeSetting {
		return fmt.Errorf("resilience.breaker.probe_concurrency must be in 1-%d, got %d", maxProbeSetting, b.ProbeConcurrency)
	}
	if b.ProbeSuccesses < 1 || b.ProbeSuccesses > maxProbeSetting {
		return fmt.Errorf("resilience.breaker.probe_successes must be in 1-%d, got %d", maxProbeSetting, b.ProbeSuccesses)
	}
	// 0 derives min(10s, upstream_header_timeout); only an explicit value is checked against the gateway.
	if b.ProbeHeaderTimeout != 0 && (b.ProbeHeaderTimeout < minProbeHeader || b.ProbeHeaderTimeout > c.Gateway.UpstreamHeaderTimeout) {
		return fmt.Errorf("resilience.breaker.probe_header_timeout must be 0 (derived) or between %v and gateway.upstream_header_timeout (%v)", minProbeHeader, c.Gateway.UpstreamHeaderTimeout)
	}
	if !inRange01(b.MaxOpenFraction) {
		return fmt.Errorf("resilience.breaker.max_open_fraction must be between 0 and 1")
	}
	t := &r.Retry
	if !inRange01(t.BudgetRatio) {
		return fmt.Errorf("resilience.retry.budget_ratio must be between 0 and 1")
	}
	if t.BudgetMin < 0 || t.BudgetMin > maxBudgetMin {
		return fmt.Errorf("resilience.retry.budget_min must be in 0-%d, got %d", maxBudgetMin, t.BudgetMin)
	}
	if t.BudgetWindow < minBudgetWindow || t.BudgetWindow > maxBudgetWindow {
		return fmt.Errorf("resilience.retry.budget_window must be between %v and %v", minBudgetWindow, maxBudgetWindow)
	}
	// An explicit deadline below the header timeout would cut requests the gateway serves today (for example a
	// slow first token from a model whose header timeout was raised on purpose), so it is refused.
	if t.FirstByteDeadline != 0 && t.FirstByteDeadline < c.Gateway.UpstreamHeaderTimeout {
		return fmt.Errorf("resilience.retry.first_byte_deadline must be 0 or at least gateway.upstream_header_timeout (%v)", c.Gateway.UpstreamHeaderTimeout)
	}
	if t.MinRemaining < 0 || t.MinRemaining > maxMinRemaining {
		return fmt.Errorf("resilience.retry.min_remaining must be between 0 and %v", maxMinRemaining)
	}
	q := &r.Queue
	if q.MaxModelDepth < 0 || q.MaxModelDepth > maxQueueDepth {
		return fmt.Errorf("resilience.queue.max_model_depth must be in 0-%d, got %d", maxQueueDepth, q.MaxModelDepth)
	}
	// Unset (0) derives min(3s, first-byte deadline); only an explicit value is checked.
	if q.MaxWait != 0 {
		if q.MaxWait < minQueueMaxWait || q.MaxWait > maxQueueMaxWait {
			return fmt.Errorf("resilience.queue.max_wait must be 0 (derived) or between %v and %v", minQueueMaxWait, maxQueueMaxWait)
		}
		if deadline := t.EffectiveFirstByteDeadline(c.Gateway.UpstreamHeaderTimeout, c.Gateway.UpstreamIdleTimeout); q.MaxWait > deadline {
			return fmt.Errorf("resilience.queue.max_wait (%v) must not exceed the first-byte deadline (%v)", q.MaxWait, deadline)
		}
	}
	if !inRange01(q.MaxTenantShare) {
		return fmt.Errorf("resilience.queue.max_tenant_share must be between 0 and 1")
	}
	// Unset derives min(5s, shutdown_timeout/2); an explicit value (0 included) is checked.
	if g := r.Drain.ReadinessGrace; g != nil {
		if *g < 0 || *g > maxReadinessGrace {
			return fmt.Errorf("resilience.drain.readiness_grace must be between 0 and %v", maxReadinessGrace)
		}
		if *g >= c.Gateway.ShutdownTimeout {
			return fmt.Errorf("resilience.drain.readiness_grace must be shorter than gateway.shutdown_timeout (%v)", c.Gateway.ShutdownTimeout)
		}
	}
	return nil
}

// applyResilienceEnv overlays the SERVERFLOW_RESILIENCE_* variables. Numbers and durations that do not parse (or
// are not finite) are ignored like everywhere else. The mode is stored as given (lower-cased) so that a misspelled
// kill switch fails validation loudly instead of silently leaving the feature on.
func applyResilienceEnv(cfg *Config) {
	env := func(key string) (string, bool) {
		v, ok := os.LookupEnv("SERVERFLOW_RESILIENCE_" + key)
		return strings.TrimSpace(v), ok
	}
	r := &cfg.Resilience
	if v, ok := env("MODE"); ok {
		r.Mode = strings.ToLower(v)
	}
	for key, dst := range map[string]*time.Duration{
		"BREAKER_WINDOW":               &r.Breaker.Window,
		"BREAKER_COOL_DOWN":            &r.Breaker.CoolDown,
		"BREAKER_MAX_COOL_DOWN":        &r.Breaker.MaxCoolDown,
		"BREAKER_PROBE_HEADER_TIMEOUT": &r.Breaker.ProbeHeaderTimeout,
		"RETRY_BUDGET_WINDOW":          &r.Retry.BudgetWindow,
		"RETRY_FIRST_BYTE_DEADLINE":    &r.Retry.FirstByteDeadline,
		"RETRY_MIN_REMAINING":          &r.Retry.MinRemaining,
		"QUEUE_MAX_WAIT":               &r.Queue.MaxWait,
	} {
		if v, ok := env(key); ok {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}
	if v, ok := env("DRAIN_READINESS_GRACE"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			r.Drain.ReadinessGrace = &d
		}
	}
	for key, dst := range map[string]*int{
		"BREAKER_CONSECUTIVE_FAILURES": &r.Breaker.ConsecutiveFailures,
		"BREAKER_MIN_VOLUME":           &r.Breaker.MinVolume,
		"BREAKER_PROBE_CONCURRENCY":    &r.Breaker.ProbeConcurrency,
		"BREAKER_PROBE_SUCCESSES":      &r.Breaker.ProbeSuccesses,
		"RETRY_BUDGET_MIN":             &r.Retry.BudgetMin,
		"QUEUE_MAX_MODEL_DEPTH":        &r.Queue.MaxModelDepth,
	} {
		if v, ok := env(key); ok {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	for key, dst := range map[string]*float64{
		"BREAKER_FAILURE_RATIO":     &r.Breaker.FailureRatio,
		"BREAKER_MAX_OPEN_FRACTION": &r.Breaker.MaxOpenFraction,
		"RETRY_BUDGET_RATIO":        &r.Retry.BudgetRatio,
		"QUEUE_MAX_TENANT_SHARE":    &r.Queue.MaxTenantShare,
	} {
		if v, ok := env(key); ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
				*dst = f
			}
		}
	}
}
