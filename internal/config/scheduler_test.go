package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"serverflow/internal/scheduler"
)

// scheduler.load_overlay (Phase 14, ADR-022): max is the default and means the original behaviour.
func TestLoadOverlayDefaultsToMax(t *testing.T) {
	if got := Default().Scheduler.LoadOverlay; got != LoadOverlayMax {
		t.Fatalf("default load_overlay %q, want %q", got, LoadOverlayMax)
	}
	cfg, err := Load("")
	if err != nil || cfg.Scheduler.LoadOverlay != LoadOverlayMax {
		t.Fatalf("Load(\"\"): %q, %v", cfg.Scheduler.LoadOverlay, err)
	}
}

func TestLoadOverlayAcceptsMaxAndLedgerAndRejectsTheRest(t *testing.T) {
	for _, v := range []string{LoadOverlayMax, LoadOverlayLedger} {
		cfg := Default()
		cfg.Scheduler.LoadOverlay = v
		if err := cfg.Validate(); err != nil {
			t.Errorf("%q: %v", v, err)
		}
	}
	for _, v := range []string{"", "sum", "Ledger", "ledger ", "none"} {
		cfg := Default()
		cfg.Scheduler.LoadOverlay = v
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "scheduler.load_overlay") {
			t.Errorf("%q: want a scheduler.load_overlay error, got %v", v, err)
		}
	}
}

func TestLoadOverlayFromYAMLAndEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  strategy: least-active\n  load_overlay: ledger\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil || cfg.Scheduler.LoadOverlay != LoadOverlayLedger || cfg.Scheduler.Strategy != "least-active" {
		t.Fatalf("yaml: %+v, %v", cfg.Scheduler, err)
	}
	t.Setenv("SERVERFLOW_SCHEDULER_LOAD_OVERLAY", "max")
	cfg, err = Load(path)
	if err != nil || cfg.Scheduler.LoadOverlay != LoadOverlayMax {
		t.Fatalf("env over yaml: %+v, %v", cfg.Scheduler, err)
	}
	t.Setenv("SERVERFLOW_SCHEDULER_LOAD_OVERLAY", "bogus")
	if _, err := Load(path); err == nil {
		t.Fatal("an invalid load_overlay from the environment was accepted")
	}
}

// A YAML file that does not mention the key keeps the default (the key is new: existing files must not break).
func TestLoadOverlayAbsentFromYAMLKeepsTheDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  strategy: random\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil || cfg.Scheduler.LoadOverlay != LoadOverlayMax {
		t.Fatalf("%+v, %v", cfg.Scheduler, err)
	}
}

// --- the work-aware strategies (Phase 14 PR C1, ADR-022 D19) ----------------------------------------------------

func workAwareConfig(strategy string) Config {
	cfg := registryConfig()
	cfg.Scheduler.Strategy, cfg.Scheduler.LoadOverlay = strategy, LoadOverlayLedger
	return cfg
}

func TestWorkAwareDefaultsAreInert(t *testing.T) {
	s := Default().Scheduler
	if s.Strategy != "round-robin" || s.LoadOverlay != LoadOverlayMax {
		t.Fatalf("the default strategy and overlay changed: %+v", s)
	}
	if s.Cost != (CostConfig{OutputFraction: 1, InputWeight: 1}) || s.Signals != (SignalsConfig{CapacityHalflife: 60 * time.Second, CapacityFloor: 0.25}) {
		t.Fatalf("defaults: %+v", s)
	}
	if w := s.CostWeights(); w.InputWeight != 1 || w.OutputFraction != 1 {
		t.Fatalf("weights %+v", w)
	}
	// the defaults make both strategies valid once the ledger overlay is chosen
	for _, st := range []string{"least-tokens", "least-work"} {
		cfg := workAwareConfig(st)
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: %v", st, err)
		}
	}
}

func TestWorkAwareBoundsAreEnforcedOnlyWhenTheStrategyIsSelected(t *testing.T) {
	nan, inf := math.NaN(), math.Inf(1)
	tests := []struct {
		name   string
		mutate func(*SchedulerConfig)
		want   string // "" = valid
	}{
		{"output fraction 1", func(s *SchedulerConfig) { s.Cost.OutputFraction = 1 }, ""},
		{"output fraction tiny", func(s *SchedulerConfig) { s.Cost.OutputFraction = 1e-9 }, ""},
		{"output fraction zero", func(s *SchedulerConfig) { s.Cost.OutputFraction = 0 }, "output_fraction"},
		{"output fraction above 1", func(s *SchedulerConfig) { s.Cost.OutputFraction = 1.0000001 }, "output_fraction"},
		{"output fraction negative", func(s *SchedulerConfig) { s.Cost.OutputFraction = -0.5 }, "output_fraction"},
		{"output fraction NaN", func(s *SchedulerConfig) { s.Cost.OutputFraction = nan }, "output_fraction"},
		{"output fraction +Inf", func(s *SchedulerConfig) { s.Cost.OutputFraction = inf }, "output_fraction"},
		{"input weight 0", func(s *SchedulerConfig) { s.Cost.InputWeight = 0 }, ""},
		{"input weight 100", func(s *SchedulerConfig) { s.Cost.InputWeight = 100 }, ""},
		{"input weight above 100", func(s *SchedulerConfig) { s.Cost.InputWeight = 100.0001 }, "input_weight"},
		{"input weight negative", func(s *SchedulerConfig) { s.Cost.InputWeight = -0.0001 }, "input_weight"},
		{"input weight NaN", func(s *SchedulerConfig) { s.Cost.InputWeight = nan }, "input_weight"},
		{"input weight -Inf", func(s *SchedulerConfig) { s.Cost.InputWeight = math.Inf(-1) }, "input_weight"},
		{"half-life at the floor", func(s *SchedulerConfig) { s.Signals.CapacityHalflife = time.Second }, ""},
		{"half-life at the ceiling", func(s *SchedulerConfig) { s.Signals.CapacityHalflife = 10 * time.Minute }, ""},
		{"half-life below 1s", func(s *SchedulerConfig) { s.Signals.CapacityHalflife = 999 * time.Millisecond }, "capacity_halflife"},
		{"half-life above 10m", func(s *SchedulerConfig) { s.Signals.CapacityHalflife = 10*time.Minute + time.Nanosecond }, "capacity_halflife"},
		{"half-life zero", func(s *SchedulerConfig) { s.Signals.CapacityHalflife = 0 }, "capacity_halflife"},
		{"half-life negative", func(s *SchedulerConfig) { s.Signals.CapacityHalflife = -time.Second }, "capacity_halflife"},
		{"floor 1", func(s *SchedulerConfig) { s.Signals.CapacityFloor = 1 }, ""},
		{"floor tiny", func(s *SchedulerConfig) { s.Signals.CapacityFloor = 1e-9 }, ""},
		{"floor zero", func(s *SchedulerConfig) { s.Signals.CapacityFloor = 0 }, "capacity_floor"},
		{"floor above 1", func(s *SchedulerConfig) { s.Signals.CapacityFloor = 1.01 }, "capacity_floor"},
		{"floor NaN", func(s *SchedulerConfig) { s.Signals.CapacityFloor = nan }, "capacity_floor"},
		{"floor negative", func(s *SchedulerConfig) { s.Signals.CapacityFloor = -0.25 }, "capacity_floor"},
	}
	for _, tt := range tests {
		for _, st := range []string{"least-tokens", "least-work"} {
			cfg := workAwareConfig(st)
			tt.mutate(&cfg.Scheduler)
			err := cfg.Validate()
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("%s/%s: want valid, got %v", tt.name, st, err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Errorf("%s/%s: want an error mentioning %q, got %v", tt.name, st, tt.want, err)
			}
		}
		// The same values are inert for every other strategy, with either overlay, and in static mode.
		for _, st := range []string{"round-robin", "least-active", "least-queue", "random"} {
			for _, overlay := range []string{LoadOverlayMax, LoadOverlayLedger} {
				cfg := registryConfig()
				cfg.Scheduler.Strategy, cfg.Scheduler.LoadOverlay = st, overlay
				tt.mutate(&cfg.Scheduler)
				if err := cfg.Validate(); err != nil {
					t.Errorf("%s/%s/%s: unused settings must not be validated: %v", tt.name, st, overlay, err)
				}
			}
		}
		static := Default()
		static.Scheduler.Strategy = "least-work"
		tt.mutate(&static.Scheduler)
		if err := static.Validate(); err != nil {
			t.Errorf("%s: static mode has no scheduler and must ignore the settings: %v", tt.name, err)
		}
	}
}

func TestWorkAwareStrategiesNeedTheLedgerOverlayInRegistryMode(t *testing.T) {
	for _, st := range []string{"least-tokens", "least-work"} {
		cfg := workAwareConfig(st)
		cfg.Scheduler.LoadOverlay = LoadOverlayMax
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "scheduler.load_overlay: ledger") || !strings.Contains(err.Error(), st) {
			t.Errorf("%s: want an error naming the strategy and the setting to use, got %v", st, err)
		}
	}
}

func TestWorkAwareSettingsFromYAMLAndEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	yaml := `
gateway:
  worker_source: registry
scheduler:
  strategy: least-work
  load_overlay: ledger
  cost:
    output_fraction: 0.4
    input_weight: 0.25
  signals:
    capacity_halflife: 90s
    capacity_floor: 0.5
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Scheduler; got.Cost != (CostConfig{OutputFraction: 0.4, InputWeight: 0.25}) || got.Signals != (SignalsConfig{CapacityHalflife: 90 * time.Second, CapacityFloor: 0.5}) {
		t.Fatalf("yaml: %+v", got)
	}
	t.Setenv("SERVERFLOW_SCHEDULER_COST_OUTPUT_FRACTION", "0.8")
	t.Setenv("SERVERFLOW_SCHEDULER_COST_INPUT_WEIGHT", "2")
	t.Setenv("SERVERFLOW_SCHEDULER_SIGNALS_CAPACITY_HALFLIFE", "2m")
	t.Setenv("SERVERFLOW_SCHEDULER_SIGNALS_CAPACITY_FLOOR", "0.1")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Scheduler; got.Cost != (CostConfig{OutputFraction: 0.8, InputWeight: 2}) || got.Signals != (SignalsConfig{CapacityHalflife: 2 * time.Minute, CapacityFloor: 0.1}) {
		t.Fatalf("env over yaml: %+v", got)
	}
	// A value that does not parse is ignored (the repository convention for numeric overrides): the file's stays.
	for _, k := range []string{"COST_OUTPUT_FRACTION", "COST_INPUT_WEIGHT", "SIGNALS_CAPACITY_HALFLIFE", "SIGNALS_CAPACITY_FLOOR"} {
		t.Setenv("SERVERFLOW_SCHEDULER_"+k, "not a number")
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Scheduler; got.Cost != (CostConfig{OutputFraction: 0.4, InputWeight: 0.25}) || got.Signals != (SignalsConfig{CapacityHalflife: 90 * time.Second, CapacityFloor: 0.5}) {
		t.Fatalf("unparseable env values must be ignored: %+v", got)
	}
	// A value that parses but is out of range fails validation, since the strategy is work-aware ...
	t.Setenv("SERVERFLOW_SCHEDULER_COST_OUTPUT_FRACTION", "7")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "output_fraction") {
		t.Fatalf("want an output_fraction error, got %v", err)
	}
	// ... and is inert under any other strategy.
	t.Setenv("SERVERFLOW_SCHEDULER_STRATEGY", "least-active")
	if _, err := Load(path); err != nil {
		t.Fatalf("an unused out-of-range setting must not stop startup: %v", err)
	}
}

// A YAML file from before the keys existed keeps the defaults.
func TestWorkAwareKeysAbsentFromYAMLKeepTheDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  strategy: least-queue\n  cost:\n    input_weight: 3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scheduler.Cost.InputWeight != 3 || cfg.Scheduler.Cost.OutputFraction != 1 || cfg.Scheduler.Signals != (SignalsConfig{CapacityHalflife: 60 * time.Second, CapacityFloor: 0.25}) {
		t.Fatalf("%+v", cfg.Scheduler)
	}
}

func TestEveryImplementedStrategyIsAcceptedAndTheErrorListsThem(t *testing.T) {
	for _, st := range scheduler.Strategies() {
		cfg := Default()
		cfg.Scheduler.Strategy = st
		if err := cfg.Validate(); err != nil {
			t.Errorf("static mode, %s: %v", st, err)
		}
	}
	cfg := Default()
	cfg.Scheduler.Strategy = "nope"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("an unknown strategy was accepted")
	}
	for _, st := range scheduler.Strategies() {
		if !strings.Contains(err.Error(), st) {
			t.Errorf("the error does not list %s: %v", st, err)
		}
	}
}
