package config

import "fmt"

// scheduler.load_overlay values (ADR-022, ADR-011).
const (
	// LoadOverlayMax is the original rule and the default: the effective active count of a worker is the larger
	// of what the worker reported and what this gateway has in flight on it.
	LoadOverlayMax = "max"
	// LoadOverlayLedger corrects the reported count for what this gateway started and finished since the
	// report was made, never going below the local count. Opt-in.
	LoadOverlayLedger = "ledger"
)

// SchedulerConfig configures the request scheduling strategy. Strategy
// names the scheduling algorithm to use. LoadOverlay chooses how the gateway
// combines a worker's reported load with its own in-flight count: "max"
// (default, unchanged behaviour) or "ledger" (Phase 14, opt-in). It matters
// only in registry mode, like every scheduling setting.
type SchedulerConfig struct {
	Strategy    string `yaml:"strategy"`
	LoadOverlay string `yaml:"load_overlay"`
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
