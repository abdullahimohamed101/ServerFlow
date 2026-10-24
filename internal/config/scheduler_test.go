package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
