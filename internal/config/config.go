// Package config provides typed, validated configuration for all
// InferGrid components. Configuration is loaded from defaults, an
// optional YAML file, environment variables, and CLI flags, in that
// order. No component other than this package should read environment
// variables directly.
package config

import (
	"fmt"
	"time"
)

// Config is the root configuration for every InferGrid component.

// Each binary loads the same shape and uses only the sections it needs.
type Config struct {
	Gateway   GatewayConfig   `yaml:"gateway"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Worker    WorkerConfig    `yaml:"worker"`
	Admission AdmissionConfig `yaml:"admission"`
	Redis     RedisConfig     `yaml:"redis"`
	Postgres  PostgresConfig  `yaml:"postgres"`
	Log       LogConfig       `yaml:"log"`

	configFilePath string
}

// GatewayConfig configures the HTTP API gateway. Port is the listen
// port for the HTTP server.
type GatewayConfig struct {
	Port int `yaml:"port"`
}

// SchedulerConfig configures the request scheduling strategy. Strategy
// names the scheduling algorithm to use.
type SchedulerConfig struct {
	Strategy string `yaml:"strategy"`
}

// WorkerConfig configures inference worker agent behavior. HeartbeatInterval
// is how often workers report state; UnhealthyTimeout is how long without
// a heartbeat before a worker is considered unhealthy.
type WorkerConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	UnhealthyTimeout  time.Duration `yaml:"unhealthy_timeout"`
}

// AdmissionConfig configures cluster admission control thresholds.
type AdmissionConfig struct {
	MaxGlobalRequests int `yaml:"max_global_requests"`
}

// RedisConfig configures the shared ephemeral state store (rate limits,
// coordination, routing hints).
type RedisConfig struct {
	Address string `yaml:"address"`
}

// PostgresConfig configures the durable metadata store.
type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// LogConfig configures structured logging.
type LogConfig struct {
	Level string `yaml:"level"`
}

// Default returns the built-in defaults for every component. Callers
// must not mutate the returned value before passing it to Load.

func Default() Config {
	return Config{
		Gateway: GatewayConfig{
			Port: 8080,
		},
		Scheduler: SchedulerConfig{
			Strategy: "least-work",
		},
		Worker: WorkerConfig{
			HeartbeatInterval: 2 * time.Second,
			UnhealthyTimeout:  10 * time.Second,
		},
		Admission: AdmissionConfig{
			MaxGlobalRequests: 1000,
		},
		Redis: RedisConfig{
			Address: "redis:6379",
		},
		Postgres: PostgresConfig{
			DSN: "postgres://postgres:postgres@postgres:5432/infergrid?sslmode=disable",
		},
		Log: LogConfig{
			Level: "info",
		},
	}
}

// Validate returns an error describing the first invalid setting, or
// nil ifthe configuration is valid. Callers must invoke this after
// loading and before starting any component (fail fast).
func (c *Config) Validate() error {
	if c.Gateway.Port <= 0 || c.Gateway.Port > 65535 {
		return fmt.Errorf("gateway.port must be in 1-65535, got %d", c.Gateway.Port)
	}
	switch c.Scheduler.Strategy {
	case "random", "round-robin", "least-active", "least-queue", "least-work":
	default:
		return fmt.Errorf("scheduler.strategy %q is not supported", c.Scheduler.Strategy)
	}
	if c.Worker.HeartbeatInterval <= 0 {
		return fmt.Errorf("worker.heartbeat_interval must be > 0")
	}
	if c.Worker.UnhealthyTimeout <= 0 {
		return fmt.Errorf("worker.unhealthy_timeout must be > 0")
	}
	if c.Admission.MaxGlobalRequests <= 0 {
		return fmt.Errorf("admission.max_global_requests must be > 0")
	}
	if c.Redis.Address == "" {
		return fmt.Errorf("redis.address must not be empty")
	}
	if c.Postgres.DSN == "" {
		return fmt.Errorf("postgres.dsn must not be empty")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level %q is not supported", c.Log.Level)
	}
	return nil
}

// ConfigFilePath returns the path supplied via -config, or "" if none.

func (c *Config) ConfigFilePath() string {
	return c.configFilePath
}
