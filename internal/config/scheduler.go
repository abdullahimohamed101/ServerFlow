package config

import (
	"fmt"
	"math"
	"time"

	"serverflow/internal/cost"
	"serverflow/internal/scheduler"
)

// scheduler.load_overlay values (ADR-022, ADR-011).
const (
	// LoadOverlayMax is the original rule and the default: the effective active count of a worker is the larger
	// of what the worker reported and what this gateway has in flight on it.
	LoadOverlayMax = "max"
	// LoadOverlayLedger corrects the reported count for what this gateway started and finished since the
	// report was made, never going below the local count. Opt-in. The work-aware strategies need it.
	LoadOverlayLedger = "ledger"
)

// Bounds of the work-aware settings (ADR-022, D19).
const (
	maxInputWeight         = 100.0
	minCapacityHalflife    = time.Second
	maxCapacityHalflife    = 10 * time.Minute
	defaultCapacityHalflif = 60 * time.Second
)

// SchedulerConfig configures the request scheduling strategy. Strategy
// names the scheduling algorithm to use. LoadOverlay chooses how the gateway
// combines a worker's reported load with its own in-flight count: "max"
// (default, unchanged behaviour) or "ledger" (Phase 14, opt-in). It matters
// only in registry mode, like every scheduling setting.
//
// Cost and Signals tune the work-aware strategies (least-tokens, least-work). They are validated, and used, only
// when one of those strategies is selected in registry mode; with any other strategy they are inert and a bad
// value in them is not an error.
type SchedulerConfig struct {
	Strategy    string        `yaml:"strategy"`
	LoadOverlay string        `yaml:"load_overlay"`
	Cost        CostConfig    `yaml:"cost"`
	Signals     SignalsConfig `yaml:"signals"`
}

// CostConfig is how the work-aware strategies weigh a request's estimate (ADR-022, D3). The rate limiter is not
// affected: it is always charged the unweighted estimate.
type CostConfig struct {
	// OutputFraction is the share of the output budget (max_tokens, or gateway.max_tokens_limit when absent)
	// expected to be used, in (0, 1]. Default 1: charge the whole budget.
	OutputFraction float64 `yaml:"output_fraction"`
	// InputWeight scales the input tokens, in [0, 100]. Default 1.
	InputWeight float64 `yaml:"input_weight"`
}

// SignalsConfig tunes the capacity estimate of a worker (ADR-022, D6).
type SignalsConfig struct {
	// CapacityHalflife is how fast a worker's capacity peak decays toward the fleet prior, 1s to 10m. Default 60s.
	CapacityHalflife time.Duration `yaml:"capacity_halflife"`
	// CapacityFloor is the fraction of the fleet prior below which a decayed peak never goes, in (0, 1].
	// Default 0.25.
	CapacityFloor float64 `yaml:"capacity_floor"`
}

func defaultScheduler() SchedulerConfig {
	return SchedulerConfig{
		Strategy:    "round-robin",
		LoadOverlay: LoadOverlayMax,
		Cost:        CostConfig{OutputFraction: 1, InputWeight: 1},
		Signals:     SignalsConfig{CapacityHalflife: defaultCapacityHalflif, CapacityFloor: 0.25},
	}
}

// CostWeights is the cost configuration as the estimator package takes it.
func (s SchedulerConfig) CostWeights() cost.Weights {
	return cost.Weights{InputWeight: s.Cost.InputWeight, OutputFraction: s.Cost.OutputFraction}
}

// validate checks the scheduler settings other than the strategy name, which Validate and the registry-mode
// checks own.
func (s SchedulerConfig) validate() error {
	switch s.LoadOverlay {
	case LoadOverlayMax, LoadOverlayLedger:
		return nil
	}
	return fmt.Errorf("scheduler.load_overlay must be %q or %q, got %q", LoadOverlayMax, LoadOverlayLedger, s.LoadOverlay)
}

// validateWorkAware checks what a work-aware strategy needs, in registry mode: the ledger overlay (the strategy
// reads the gateway's own view of the work on each worker, which only the ledger provides) and sane weights.
// It does nothing for any other strategy. Messages never echo more than the offending number.
func (s SchedulerConfig) validateWorkAware() error {
	if !scheduler.WorkAware(s.Strategy) {
		return nil
	}
	if s.LoadOverlay != LoadOverlayLedger {
		return fmt.Errorf("scheduler.strategy %q reads the gateway's in-flight ledger and needs scheduler.load_overlay: %s (it is %q)", s.Strategy, LoadOverlayLedger, s.LoadOverlay)
	}
	if !finiteIn(s.Cost.OutputFraction, 0, 1) || s.Cost.OutputFraction == 0 {
		return fmt.Errorf("scheduler.cost.output_fraction must be in (0, 1], got %v", s.Cost.OutputFraction)
	}
	if !finiteIn(s.Cost.InputWeight, 0, maxInputWeight) {
		return fmt.Errorf("scheduler.cost.input_weight must be in [0, %v], got %v", maxInputWeight, s.Cost.InputWeight)
	}
	if s.Signals.CapacityHalflife < minCapacityHalflife || s.Signals.CapacityHalflife > maxCapacityHalflife {
		return fmt.Errorf("scheduler.signals.capacity_halflife must be %v to %v, got %v", minCapacityHalflife, maxCapacityHalflife, s.Signals.CapacityHalflife)
	}
	if !finiteIn(s.Signals.CapacityFloor, 0, 1) || s.Signals.CapacityFloor == 0 {
		return fmt.Errorf("scheduler.signals.capacity_floor must be in (0, 1], got %v", s.Signals.CapacityFloor)
	}
	return nil
}

// finiteIn reports whether v is a number within [lo, hi] (false for NaN and infinities).
func finiteIn(v, lo, hi float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= lo && v <= hi
}
