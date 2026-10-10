package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

// --- Phase 5: worker source and scheduler ------------------------------------------------------

func registryConfig() Config {
	cfg := Default()
	cfg.Gateway.WorkerSource = WorkerSourceRegistry
	return cfg
}

func TestWorkerSourceDefaultsKeepThePhase2Behaviour(t *testing.T) {
	cfg := Default()
	if cfg.Gateway.WorkerSource != WorkerSourceStatic {
		t.Fatalf("the default must stay static, got %q", cfg.Gateway.WorkerSource)
	}
	if cfg.Scheduler.Strategy != "round-robin" {
		t.Fatalf("the default strategy must be one that exists, got %q", cfg.Scheduler.Strategy)
	}
	if cfg.Gateway.RegistryRefresh != time.Second || cfg.Gateway.RegistryMaxStaleness != 10*time.Second ||
		cfg.Gateway.ControlPlaneURL != "http://127.0.0.1:9090" || len(cfg.Gateway.WorkerNetworks) != 0 {
		t.Fatalf("registry defaults changed: %+v", cfg.Gateway)
	}
	rc := registryConfig()
	if err := rc.Validate(); err != nil {
		t.Fatalf("the registry source with defaults must be valid: %v", err)
	}
}

func TestValidateRegistrySource(t *testing.T) {
	const token = "a-sufficiently-long-shared-secret"
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string // "" means valid
	}{
		{"unknown source", func(c *Config) { c.Gateway.WorkerSource = "magic" }, "worker_source"},
		{"empty source", func(c *Config) { c.Gateway.WorkerSource = "" }, "worker_source"},
		{"least-work is not implemented", func(c *Config) { c.Scheduler.Strategy = "least-work" }, "not implemented yet"},
		{"every implemented strategy", func(c *Config) { c.Scheduler.Strategy = "least-queue" }, ""},
		{"bad control plane url", func(c *Config) { c.Gateway.ControlPlaneURL = "nonsense" }, "control_plane_url"},
		{"credentials in the url", func(c *Config) { c.Gateway.ControlPlaneURL = "http://u:topsecret@127.0.0.1:9090" }, "control_plane_url"},
		{"zero refresh", func(c *Config) { c.Gateway.RegistryRefresh = 0 }, "registry_refresh"},
		{"refresh below the floor", func(c *Config) {
			c.Gateway.RegistryRefresh, c.Gateway.RegistryMaxStaleness = 9*time.Millisecond, time.Second
		}, "hammer"},
		{"refresh at the floor", func(c *Config) {
			c.Gateway.RegistryRefresh, c.Gateway.RegistryMaxStaleness = 10*time.Millisecond, time.Second
		}, ""},
		{"staleness above the ceiling", func(c *Config) { c.Gateway.RegistryMaxStaleness = 5*time.Minute + time.Nanosecond }, "at most"},
		{"staleness at the ceiling", func(c *Config) { c.Gateway.RegistryMaxStaleness = 5 * time.Minute }, ""},
		{"staleness below twice the refresh", func(c *Config) { c.Gateway.RegistryMaxStaleness = 1999 * time.Millisecond }, "at least twice"},
		{"staleness exactly twice the refresh", func(c *Config) { c.Gateway.RegistryMaxStaleness = 2 * time.Second }, ""},
		{"token over cleartext to another host", func(c *Config) {
			c.ControlPlane.Token, c.Gateway.ControlPlaneURL = token, "http://10.0.0.5:9090"
		}, "cleartext"},
		{"token over https to another host", func(c *Config) {
			c.ControlPlane.Token, c.Gateway.ControlPlaneURL = token, "https://cp.internal:9090"
		}, ""},
		{"token over cleartext to loopback", func(c *Config) { c.ControlPlane.Token = token }, ""},
		{"short token", func(c *Config) { c.ControlPlane.Token = "short" }, "at least 16"},
		{"token one short", func(c *Config) { c.ControlPlane.Token = "0123456789abcde" }, "at least 16"},
		{"token exactly long enough", func(c *Config) { c.ControlPlane.Token = "0123456789abcdef" }, ""},
		{"suspect timeout too tight for the refresh", func(c *Config) {
			c.Gateway.RegistryRefresh, c.Gateway.RegistryMaxStaleness = 3*time.Second, 10*time.Second
			c.Worker.SuspectTimeout = c.Gateway.RegistryRefresh + c.Worker.HeartbeatInterval - time.Nanosecond
		}, "registry_refresh plus"},
		{"suspect timeout just enough", func(c *Config) {
			c.Gateway.RegistryRefresh, c.Gateway.RegistryMaxStaleness = 3*time.Second, 10*time.Second
			c.Worker.SuspectTimeout = c.Gateway.RegistryRefresh + c.Worker.HeartbeatInterval
		}, ""},
		{"zero attempts", func(c *Config) { c.Gateway.MaxAttempts = 0 }, "max_attempts"},
		{"one attempt disables retries", func(c *Config) { c.Gateway.MaxAttempts = 1 }, ""},
		{"five attempts", func(c *Config) { c.Gateway.MaxAttempts = 5 }, ""},
		{"six attempts", func(c *Config) { c.Gateway.MaxAttempts = 6 }, "max_attempts"},
		{"empty retry list", func(c *Config) { c.Gateway.RetryStatuses = nil }, ""},
		{"a 500 may be listed", func(c *Config) { c.Gateway.RetryStatuses = []int{500, 503} }, ""},
		{"a 4xx cannot be listed", func(c *Config) { c.Gateway.RetryStatuses = []int{429} }, "5xx"},
		{"a 599 may be listed", func(c *Config) { c.Gateway.RetryStatuses = []int{599} }, ""},
		{"a 600 cannot be listed", func(c *Config) { c.Gateway.RetryStatuses = []int{600} }, "5xx"},
		{"a 499 cannot be listed", func(c *Config) { c.Gateway.RetryStatuses = []int{499} }, "5xx"},
		{"duplicate status", func(c *Config) { c.Gateway.RetryStatuses = []int{503, 503} }, "twice"},
		{"too many statuses", func(c *Config) {
			c.Gateway.RetryStatuses = []int{500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510}
		}, "at most 10"},
		{"ten statuses", func(c *Config) {
			c.Gateway.RetryStatuses = []int{500, 501, 502, 503, 504, 505, 506, 507, 508, 509}
		}, ""},
		{"good cidrs", func(c *Config) { c.Gateway.WorkerNetworks = []string{"10.0.0.0/8", "fd00::/8"} }, ""},
		{"bad cidr", func(c *Config) { c.Gateway.WorkerNetworks = []string{"10.0.0.0"} }, "not a CIDR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := registryConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("want valid, got %v", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Fatalf("want an error mentioning %q, got %v", tt.want, err)
			}
			if err != nil && (strings.Contains(err.Error(), "topsecret") || strings.Contains(err.Error(), token)) {
				t.Fatalf("the error leaks a secret: %v", err)
			}
		})
	}
}

func TestStaticSourceIgnoresRegistrySettingsAndAllowsLeastWork(t *testing.T) {
	cfg := Default()
	cfg.Scheduler.Strategy = "least-work" // valid in config; only the registry source needs an implementation
	cfg.Gateway.RegistryRefresh = 0
	cfg.Gateway.ControlPlaneURL = "nonsense"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("static mode must not validate registry settings: %v", err)
	}
}

func TestLoadGatewayRegistrySettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte(`
gateway:
  worker_source: registry
  control_plane_url: http://127.0.0.1:9191
  registry_refresh: 500ms
  registry_max_staleness: 4s
  worker_networks: ["10.0.0.0/8"]
scheduler:
  strategy: least-queue
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	g := cfg.Gateway
	if g.WorkerSource != "registry" || g.ControlPlaneURL != "http://127.0.0.1:9191" || g.RegistryRefresh != 500*time.Millisecond ||
		g.RegistryMaxStaleness != 4*time.Second || len(g.WorkerNetworks) != 1 || g.WorkerNetworks[0] != "10.0.0.0/8" {
		t.Fatalf("got %+v", g)
	}
}

func TestLoadGatewayRegistryEnvOverrides(t *testing.T) {
	t.Setenv("SERVERFLOW_GATEWAY_WORKER_SOURCE", "registry")
	t.Setenv("SERVERFLOW_GATEWAY_CONTROL_PLANE_URL", "http://127.0.0.1:9292")
	t.Setenv("SERVERFLOW_GATEWAY_REGISTRY_REFRESH", "250ms")
	t.Setenv("SERVERFLOW_GATEWAY_REGISTRY_MAX_STALENESS", "3s")
	t.Setenv("SERVERFLOW_GATEWAY_WORKER_NETWORKS", "10.0.0.0/8, 192.168.0.0/16,")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	g := cfg.Gateway
	if g.WorkerSource != "registry" || g.ControlPlaneURL != "http://127.0.0.1:9292" || g.RegistryRefresh != 250*time.Millisecond ||
		g.RegistryMaxStaleness != 3*time.Second || len(g.WorkerNetworks) != 2 || g.WorkerNetworks[1] != "192.168.0.0/16" {
		t.Fatalf("got %+v", g)
	}
}

func TestRetryDefaultsFollowTheSpec(t *testing.T) {
	g := Default().Gateway
	if g.MaxAttempts != 2 || len(g.RetryStatuses) != 2 || g.RetryStatuses[0] != 502 || g.RetryStatuses[1] != 503 {
		t.Fatalf("spec section 25 says two attempts; the default statuses are 502 and 503 (a worker-reported 504 means its backend already timed out; ADR-012): %+v", g)
	}
}

func TestStaticSourceIgnoresRetrySettings(t *testing.T) {
	cfg := Default()
	cfg.Gateway.MaxAttempts, cfg.Gateway.RetryStatuses = 99, []int{200}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("static mode must not validate retry settings: %v", err)
	}
}

func TestLoadRetrySettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("gateway:\n  worker_source: registry\n  max_attempts: 3\n  retry_statuses: [503, 504]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.MaxAttempts != 3 || len(cfg.Gateway.RetryStatuses) != 2 || cfg.Gateway.RetryStatuses[0] != 503 {
		t.Fatalf("got %+v", cfg.Gateway)
	}
	t.Setenv("SERVERFLOW_GATEWAY_MAX_ATTEMPTS", "4")
	t.Setenv("SERVERFLOW_GATEWAY_RETRY_STATUSES", "503, 504")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.MaxAttempts != 4 || len(cfg.Gateway.RetryStatuses) != 2 || cfg.Gateway.RetryStatuses[0] != 503 || cfg.Gateway.RetryStatuses[1] != 504 {
		t.Fatalf("env overrides: %+v", cfg.Gateway)
	}
	t.Setenv("SERVERFLOW_GATEWAY_MAX_ATTEMPTS", "many")
	t.Setenv("SERVERFLOW_GATEWAY_RETRY_STATUSES", "502,abc")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.MaxAttempts != 2 || len(cfg.Gateway.RetryStatuses) != 2 {
		t.Fatalf("invalid values are ignored (documented): %+v", cfg.Gateway)
	}
}
