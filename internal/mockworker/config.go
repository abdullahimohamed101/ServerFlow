// Package mockworker implements a configurable fake inference worker: an
// OpenAI-compatible HTTP server whose latency, throughput, queueing, and
// failures are controlled by flags. It stands in for vLLM so the gateway,
// scheduler, and benchmarks can be developed without a GPU. It has no batching
// or contention model: every generation runs at its own configured speed.
package mockworker

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// FailureMode selects how an injected failure manifests.
type FailureMode string

const (
	// ModeError answers HTTP 500 before generating anything.
	ModeError FailureMode = "error"
	// ModeUnavailable answers HTTP 503 before generating anything.
	ModeUnavailable FailureMode = "unavailable"
	// ModeDrop closes the connection without any response.
	ModeDrop FailureMode = "drop"
	// ModeMidstream starts the response, then aborts the connection.
	ModeMidstream FailureMode = "midstream"
)

// Config configures a mock worker.
type Config struct {
	Model           string
	Addr            string
	WorkerID        string
	TTFT            time.Duration
	TokensPerSecond float64
	OutputTokens    int
	MaxConcurrency  int
	QueueSize       int
	FailureRate     float64
	FailureMode     FailureMode
	Seed            int64
	StartupDelay    time.Duration
	DrainTimeout    time.Duration
}

// Limits on flag values, to keep a typo from exhausting memory or CPU.
const (
	minTokensPerSecond = 0.001
	maxTokensPerSecond = 1e6
	maxOutputTokens    = 100000
	maxConcurrencyCap  = 1024
	maxQueueSizeCap    = 100000
)

// DefaultConfig returns the defaults used when a flag is not set.
func DefaultConfig() Config {
	return Config{
		Model:           "mock-model",
		Addr:            "127.0.0.1:9000",
		TTFT:            200 * time.Millisecond,
		TokensPerSecond: 50,
		OutputTokens:    64,
		MaxConcurrency:  4,
		QueueSize:       32,
		FailureMode:     ModeError,
		DrainTimeout:    30 * time.Second,
	}
}

// Validate returns an error describing the first invalid setting.
func (c Config) Validate() error {
	if c.Model == "" {
		return fmt.Errorf("model must not be empty")
	}
	if c.Addr == "" {
		return fmt.Errorf("addr must not be empty")
	}
	if c.TTFT < 0 {
		return fmt.Errorf("ttft must be >= 0, got %v", c.TTFT)
	}
	if math.IsNaN(c.TokensPerSecond) || c.TokensPerSecond < minTokensPerSecond || c.TokensPerSecond > maxTokensPerSecond {
		return fmt.Errorf("tokens-per-second must be in %v-%v, got %v", minTokensPerSecond, maxTokensPerSecond, c.TokensPerSecond)
	}
	if c.OutputTokens < 1 || c.OutputTokens > maxOutputTokens {
		return fmt.Errorf("output-tokens must be in 1-%d, got %d", maxOutputTokens, c.OutputTokens)
	}
	if c.MaxConcurrency < 1 || c.MaxConcurrency > maxConcurrencyCap {
		return fmt.Errorf("max-concurrency must be in 1-%d, got %d", maxConcurrencyCap, c.MaxConcurrency)
	}
	if c.QueueSize < 0 || c.QueueSize > maxQueueSizeCap {
		return fmt.Errorf("queue-size must be in 0-%d, got %d", maxQueueSizeCap, c.QueueSize)
	}
	if math.IsNaN(c.FailureRate) || c.FailureRate < 0 || c.FailureRate > 1 {
		return fmt.Errorf("failure-rate must be in [0,1], got %v", c.FailureRate)
	}
	switch c.FailureMode {
	case ModeError, ModeUnavailable, ModeDrop, ModeMidstream:
	default:
		return fmt.Errorf("failure-mode %q is not supported (error, unavailable, drop, midstream)", c.FailureMode)
	}
	if c.StartupDelay < 0 {
		return fmt.Errorf("startup-delay must be >= 0, got %v", c.StartupDelay)
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("drain-timeout must be > 0, got %v", c.DrainTimeout)
	}
	return nil
}

// EffectiveWorkerID returns WorkerID, or an ID derived from the listen
// address when none was set.
func (c Config) EffectiveWorkerID() string {
	if c.WorkerID != "" {
		return c.WorkerID
	}
	port := c.Addr
	if i := strings.LastIndex(port, ":"); i >= 0 {
		port = port[i+1:]
	}
	return "mock-" + port
}
