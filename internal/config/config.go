// Package config provides typed, validated configuration for all
// ServerFlow components. Configuration is loaded from defaults, an
// optional YAML file, environment variables, and CLI flags, in that
// order. No component other than this package should read environment
// variables directly.
package config

import (
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"time"

	"serverflow/internal/scheduler"
)

// Config is the root configuration for every ServerFlow component.

// Each binary loads the same shape and uses only the sections it needs.
type Config struct {
	Gateway   GatewayConfig   `yaml:"gateway"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Worker    WorkerConfig    `yaml:"worker"`
	// ControlPlane configures the worker registry service.
	ControlPlane ControlPlaneConfig `yaml:"control_plane"`
	Admission    AdmissionConfig    `yaml:"admission"`
	Redis        RedisConfig        `yaml:"redis"`
	RateLimit    RateLimitConfig    `yaml:"rate_limit"`
	Postgres     PostgresConfig     `yaml:"postgres"`
	Auth         AuthConfig         `yaml:"auth"`
	Events       EventsConfig       `yaml:"events"`
	Tracing      TracingConfig      `yaml:"tracing"`
	// Resilience configures fault tolerance (Phase 15); see ResilienceConfig.
	Resilience ResilienceConfig `yaml:"resilience"`
	Log        LogConfig        `yaml:"log"`
	Metrics    MetricsConfig    `yaml:"metrics"`

	configFilePath string
}

// Format implements fmt.Formatter for the whole configuration. Its sections that hold a secret redact themselves, but
// fmt calls no method at all on a pointer it prints under a verb the pointer does not fit (%s on a *Config inside a
// slice, say) and would then print every field raw. Having a Formatter of its own makes *Config safe under every verb.
// The output is what fmt printed before: the fields of the type, each section formatting itself. One cosmetic change:
// a *Config now prints like a Config, without the leading & fmt adds for a pointer to a struct.
func (c Config) Format(f fmt.State, verb rune) {
	type plain Config // no methods, so no recursion
	out := fmt.Sprintf(fmt.FormatString(f, verb), plain(c))
	if verb == 'v' && f.Flag('#') {
		out = strings.Replace(out, "config.plain", "config.Config", 1)
	}
	_, _ = io.WriteString(f, out)
}

// Default returns the built-in defaults for every component. Callers
// must not mutate the returned value before passing it to Load.

func Default() Config {
	return Config{
		Gateway: GatewayConfig{
			Port:                  8080,
			UpstreamURL:           "http://localhost:8000",
			Models:                []string{"default"},
			ReadinessPath:         "/v1/models",
			MaxRequestBytes:       1 << 20,
			MaxTokensLimit:        4096,
			UpstreamHeaderTimeout: 60 * time.Second,
			UpstreamIdleTimeout:   120 * time.Second,
			ShutdownTimeout:       30 * time.Second,
			WorkerSource:          WorkerSourceStatic,
			ControlPlaneURL:       "http://127.0.0.1:9090",
			RegistryRefresh:       time.Second,
			RegistryMaxStaleness:  10 * time.Second,
			MaxAttempts:           2,
			RetryStatuses:         []int{502, 503},
		},
		Scheduler: defaultScheduler(),
		Worker: WorkerConfig{
			HeartbeatInterval: 2 * time.Second,
			SuspectTimeout:    5 * time.Second,
			UnhealthyTimeout:  10 * time.Second,
			LostTimeout:       30 * time.Second,
			Retention:         5 * time.Minute,
			ControlPlaneURL:   "http://127.0.0.1:9090",
		},
		ControlPlane: ControlPlaneConfig{
			Addr:       "127.0.0.1:9090",
			MaxWorkers: 1000,
		},
		Admission: AdmissionConfig{
			MaxGlobalRequests: 1000,
		},
		Redis: RedisConfig{
			Address:            "redis:6379",
			Timeout:            50 * time.Millisecond,
			Backoff:            time.Second,
			PoolSize:           64,
			OnFailure:          RedisFailClosed,
			RequestMetadataTTL: time.Hour,
		},
		RateLimit: RateLimitConfig{
			Mode:           RateLimitOff,
			BurstSeconds:   60,
			LeaseTTL:       60 * time.Second,
			MaxLocalLeases: 100_000,
		},
		Postgres: PostgresConfig{
			DSN:            "postgres://postgres:postgres@postgres:5432/serverflow?sslmode=disable",
			MaxConns:       10,
			ConnectTimeout: 5 * time.Second,
		},
		Auth: AuthConfig{
			Mode:        AuthModeOff,
			CacheTTL:    30 * time.Second,
			NegativeTTL: 5 * time.Second,
			CacheSize:   10000,
			StaleGrace:  5 * time.Minute,
		},
		Events:     defaultEvents(),
		Tracing:    defaultTracing(),
		Resilience: defaultResilience(),
		Log: LogConfig{
			Level: "info",
		},
		Metrics: defaultMetrics(),
	}
}

// Validate returns an error describing the first invalid setting, or
// nil if the configuration is valid. Callers must invoke this after
// loading and before starting any component (fail fast).
func (c *Config) Validate() error {
	if c.Gateway.Port <= 0 || c.Gateway.Port > 65535 {
		return fmt.Errorf("gateway.port must be in 1-65535, got %d", c.Gateway.Port)
	}
	if err := c.Gateway.validate(); err != nil {
		return err
	}
	if !slices.Contains(scheduler.Strategies(), c.Scheduler.Strategy) {
		return fmt.Errorf("scheduler.strategy %q is not supported; use one of %s", c.Scheduler.Strategy, strings.Join(scheduler.Strategies(), ", "))
	}
	if err := c.Scheduler.validate(); err != nil {
		return err
	}
	if err := c.validateWorkerSource(); err != nil {
		return err
	}
	if err := c.Worker.validate(); err != nil {
		return err
	}
	if err := c.ControlPlane.validate(); err != nil {
		return err
	}
	if c.Admission.MaxGlobalRequests <= 0 {
		return fmt.Errorf("admission.max_global_requests must be > 0")
	}
	// Redis is only used by rate_limit.mode=required and redis.request_metadata; with both off its settings change
	// nothing, so a malformed value must not stop a gateway that never touches Redis.
	if c.redisUsed() {
		if err := c.Redis.validate(); err != nil {
			return err
		}
	}
	if err := c.validateRateLimit(); err != nil {
		return err
	}
	if err := c.Postgres.validate(); err != nil {
		return err
	}
	if err := c.Auth.validate(); err != nil {
		return err
	}
	if c.Auth.Mode == AuthModeRequired && c.Postgres.MaxConns < minAuthPoolConns {
		// Key lookups may use all but two pool connections (so they never take the whole pool, and
		// last_used_at writes and the startup check can still run) and need two of their own: one for
		// unseen keys and one reserved for refreshing cached keys.
		return fmt.Errorf("postgres.max_conns must be at least %d when auth.mode is %q", minAuthPoolConns, AuthModeRequired)
	}
	// Event publishing settings only matter while events.mode is on (the Redis rule).
	if err := c.Events.validate(c.Gateway.ShutdownTimeout); err != nil {
		return err
	}
	if err := c.Tracing.validate(); err != nil {
		return err
	}
	if err := c.Metrics.validate(); err != nil {
		return err
	}
	if err := c.validateResilience(); err != nil {
		return err
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level %q is not supported", c.Log.Level)
	}
	return nil
}

// IsLoopbackHost reports whether host names this machine (localhost or a loopback IP).
func IsLoopbackHost(host string) bool { return isLoopbackHost(host) }

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func truncateForError(s string) string {
	if len(s) > 64 {
		return s[:64] + "..."
	}
	return s
}

// ConfigFilePath returns the path supplied via -config, or "" if none.

func (c *Config) ConfigFilePath() string {
	return c.configFilePath
}
