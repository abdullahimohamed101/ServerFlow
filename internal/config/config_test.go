package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultsAreValid(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
}

func TestValidateRejectsBadPort(t *testing.T) {
	cfg := Default()
	cfg.Gateway.Port = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for port 0")
	}
	cfg.Gateway.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for port 70000")
	}
}

func TestValidateRejectsUnknownStrategy(t *testing.T) {
	cfg := Default()
	cfg.Scheduler.Strategy = "magic"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for unknown scheduler strategy")
	}
}

func TestValidateRejectsZeroThresholds(t *testing.T) {
	cfg := Default()
	cfg.Worker.HeartbeatInterval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero heartbeat interval")
	}
	cfg = Default()
	cfg.Admission.MaxGlobalRequests = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero max global requests")
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "gateway:\n  port: 9090\nscheduler:\n  strategy: round-robin\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q) error: %v", path, err)
	}
	if cfg.Gateway.Port != 9090 {
		t.Fatalf("expected port 9090, got %d", cfg.Gateway.Port)
	}
	if cfg.Scheduler.Strategy != "round-robin" {
		t.Fatalf("expected strategy round-robin, got %q", cfg.Scheduler.Strategy)
	}
}

func TestLoadRejectsInvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("gateway:\n  port: not-a-number\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid YAML content")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("SERVERFLOW_GATEWAY_PORT", "7070")
	t.Setenv("SERVERFLOW_SCHEDULER_STRATEGY", "random")
	t.Setenv("SERVERFLOW_WORKER_HEARTBEAT_INTERVAL", "3s")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if cfg.Gateway.Port != 7070 {
		t.Fatalf("expected port 7070, got %d", cfg.Gateway.Port)
	}
	if cfg.Scheduler.Strategy != "random" {
		t.Fatalf("expected strategy random, got %q", cfg.Scheduler.Strategy)
	}
	if cfg.Worker.HeartbeatInterval != 3*time.Second {
		t.Fatalf("expected 3s heartbeat, got %v", cfg.Worker.HeartbeatInterval)
	}
}
