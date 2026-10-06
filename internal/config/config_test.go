package config

import (
	"os"
	"path/filepath"
	"strings"
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

func TestDefaultGatewayConfig(t *testing.T) {
	g := Default().Gateway
	if g.UpstreamURL != "http://localhost:8000" {
		t.Fatalf("unexpected default upstream %q", g.UpstreamURL)
	}
	if len(g.Models) != 1 || g.Models[0] != "default" {
		t.Fatalf("unexpected default models %v", g.Models)
	}
	if g.ReadinessPath != "/v1/models" {
		t.Fatalf("unexpected default readiness path %q", g.ReadinessPath)
	}
}

func TestValidateRejectsBadGatewaySettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty upstream", func(c *Config) { c.Gateway.UpstreamURL = "" }},
		{"upstream without scheme", func(c *Config) { c.Gateway.UpstreamURL = "localhost:8000" }},
		{"upstream with unsupported scheme", func(c *Config) { c.Gateway.UpstreamURL = "ftp://localhost:8000" }},
		{"upstream without host", func(c *Config) { c.Gateway.UpstreamURL = "http://" }},
		{"no models", func(c *Config) { c.Gateway.Models = nil }},
		{"empty model name", func(c *Config) { c.Gateway.Models = []string{"a", ""} }},
		{"duplicate model", func(c *Config) { c.Gateway.Models = []string{"a", "a"} }},
		{"readiness path without slash", func(c *Config) { c.Gateway.ReadinessPath = "health" }},
		{"zero max request bytes", func(c *Config) { c.Gateway.MaxRequestBytes = 0 }},
		{"zero max tokens limit", func(c *Config) { c.Gateway.MaxTokensLimit = 0 }},
		{"zero upstream header timeout", func(c *Config) { c.Gateway.UpstreamHeaderTimeout = 0 }},
		{"zero upstream idle timeout", func(c *Config) { c.Gateway.UpstreamIdleTimeout = 0 }},
		{"zero shutdown timeout", func(c *Config) { c.Gateway.ShutdownTimeout = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadGatewayFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "gateway:\n  upstream_url: http://worker:9000\n  models: [qwen-7b, llama-8b]\n  readiness_path: /health\n  upstream_header_timeout: 5s\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	g := cfg.Gateway
	if g.UpstreamURL != "http://worker:9000" || g.ReadinessPath != "/health" {
		t.Fatalf("unexpected gateway config %+v", g)
	}
	if len(g.Models) != 2 || g.Models[0] != "qwen-7b" || g.Models[1] != "llama-8b" {
		t.Fatalf("unexpected models %v", g.Models)
	}
	if g.UpstreamHeaderTimeout != 5*time.Second {
		t.Fatalf("expected 5s header timeout, got %v", g.UpstreamHeaderTimeout)
	}
	if g.Port != 8080 {
		t.Fatalf("unset fields must keep defaults, got port %d", g.Port)
	}
}

func TestLoadGatewayEnvOverrides(t *testing.T) {
	t.Setenv("SERVERFLOW_GATEWAY_UPSTREAM_URL", "https://llm.internal:8443")
	t.Setenv("SERVERFLOW_GATEWAY_MODELS", " qwen-7b , llama-8b ")
	t.Setenv("SERVERFLOW_GATEWAY_READINESS_PATH", "/health")
	t.Setenv("SERVERFLOW_GATEWAY_MAX_REQUEST_BYTES", "2048")
	t.Setenv("SERVERFLOW_GATEWAY_MAX_TOKENS_LIMIT", "512")
	t.Setenv("SERVERFLOW_GATEWAY_UPSTREAM_HEADER_TIMEOUT", "7s")
	t.Setenv("SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT", "9s")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	g := cfg.Gateway
	if g.UpstreamURL != "https://llm.internal:8443" || g.ReadinessPath != "/health" {
		t.Fatalf("unexpected gateway config %+v", g)
	}
	if len(g.Models) != 2 || g.Models[0] != "qwen-7b" || g.Models[1] != "llama-8b" {
		t.Fatalf("expected trimmed models, got %q", g.Models)
	}
	if g.MaxRequestBytes != 2048 || g.MaxTokensLimit != 512 {
		t.Fatalf("unexpected limits %+v", g)
	}
	if g.UpstreamHeaderTimeout != 7*time.Second || g.ShutdownTimeout != 9*time.Second {
		t.Fatalf("unexpected timeouts %+v", g)
	}
}

func TestValidateRejectsUnsafeUpstreamURLs(t *testing.T) {
	for _, u := range []string{
		"http://user:secret@host:8000",
		"http://:secret@host:8000",
		"http://host:8000/?token=abc",
		"http://host:8000/v1#frag",
		"http://host:8000?",
	} {
		cfg := Default()
		cfg.Gateway.UpstreamURL = u
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected %q to be rejected", u)
		} else if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "token=abc") {
			t.Fatalf("validation error leaks the credential: %v", err)
		}
	}
	cfg := Default()
	cfg.Gateway.UpstreamURL = "https://llm.internal:8443/prefix"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a plain URL with a path prefix must stay valid: %v", err)
	}
}

func TestUpstreamIdleTimeoutConfig(t *testing.T) {
	if Default().Gateway.UpstreamIdleTimeout <= 0 {
		t.Fatal("default idle timeout must be positive")
	}
	cfg := Default()
	cfg.Gateway.UpstreamIdleTimeout = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero upstream idle timeout")
	}
	t.Setenv("SERVERFLOW_GATEWAY_UPSTREAM_IDLE_TIMEOUT", "45s")
	loaded, err := Load("")
	if err != nil || loaded.Gateway.UpstreamIdleTimeout != 45*time.Second {
		t.Fatalf("env override failed: %v %v", loaded.Gateway.UpstreamIdleTimeout, err)
	}
}
