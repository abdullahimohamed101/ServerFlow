package mockworker

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
}

func TestValidateRejectsBadSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"empty model", func(c *Config) { c.Model = "" }, "model"},
		{"empty addr", func(c *Config) { c.Addr = "" }, "addr"},
		{"negative ttft", func(c *Config) { c.TTFT = -time.Millisecond }, "ttft"},
		{"zero tokens per second", func(c *Config) { c.TokensPerSecond = 0 }, "tokens-per-second"},
		{"negative tokens per second", func(c *Config) { c.TokensPerSecond = -5 }, "tokens-per-second"},
		{"NaN tokens per second", func(c *Config) { c.TokensPerSecond = math.NaN() }, "tokens-per-second"},
		{"infinite tokens per second", func(c *Config) { c.TokensPerSecond = math.Inf(1) }, "tokens-per-second"},
		{"absurdly high tokens per second", func(c *Config) { c.TokensPerSecond = 1e9 }, "tokens-per-second"},
		{"absurdly low tokens per second", func(c *Config) { c.TokensPerSecond = 1e-9 }, "tokens-per-second"},
		{"zero output tokens", func(c *Config) { c.OutputTokens = 0 }, "output-tokens"},
		{"huge output tokens", func(c *Config) { c.OutputTokens = 1 << 30 }, "output-tokens"},
		{"zero concurrency", func(c *Config) { c.MaxConcurrency = 0 }, "max-concurrency"},
		{"huge concurrency", func(c *Config) { c.MaxConcurrency = 1 << 20 }, "max-concurrency"},
		{"negative queue", func(c *Config) { c.QueueSize = -1 }, "queue-size"},
		{"huge queue", func(c *Config) { c.QueueSize = 1 << 30 }, "queue-size"},
		{"failure rate above 1", func(c *Config) { c.FailureRate = 1.01 }, "failure-rate"},
		{"negative failure rate", func(c *Config) { c.FailureRate = -0.1 }, "failure-rate"},
		{"NaN failure rate", func(c *Config) { c.FailureRate = math.NaN() }, "failure-rate"},
		{"unknown failure mode", func(c *Config) { c.FailureMode = "explode" }, "failure-mode"},
		{"negative startup delay", func(c *Config) { c.StartupDelay = -time.Second }, "startup-delay"},
		{"zero drain timeout", func(c *Config) { c.DrainTimeout = 0 }, "drain-timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q should name %q", err, tt.want)
			}
		})
	}
}

func TestValidateAcceptsBoundaryValues(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TTFT = 0
	cfg.QueueSize = 0
	cfg.FailureRate = 1
	cfg.OutputTokens = maxOutputTokens
	cfg.MaxConcurrency = maxConcurrencyCap
	if err := cfg.Validate(); err != nil {
		t.Fatalf("boundary values must be valid: %v", err)
	}
}

func TestEffectiveWorkerID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Addr = ":9001"
	if got := cfg.EffectiveWorkerID(); got != "mock-9001" {
		t.Fatalf("got %q", got)
	}
	cfg.Addr = "127.0.0.1:9002"
	if got := cfg.EffectiveWorkerID(); got != "mock-9002" {
		t.Fatalf("got %q", got)
	}
	cfg.WorkerID = "worker-qwen-01"
	if got := cfg.EffectiveWorkerID(); got != "worker-qwen-01" {
		t.Fatalf("explicit worker ID must win, got %q", got)
	}
}
