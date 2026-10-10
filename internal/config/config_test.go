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
	t.Setenv("SERVERFLOW_WORKER_HEARTBEAT_INTERVAL", "2500ms") // suspect_timeout (5s) must stay >= 2 heartbeats

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
	if cfg.Worker.HeartbeatInterval != 2500*time.Millisecond {
		t.Fatalf("expected 2.5s heartbeat, got %v", cfg.Worker.HeartbeatInterval)
	}
}

// --- Phase 4: worker registry settings ---------------------------------------------

func TestWorkerThresholdDefaultsMatchTheSpec(t *testing.T) {
	w := Default().Worker
	if w.HeartbeatInterval != 2*time.Second || w.SuspectTimeout != 5*time.Second || w.UnhealthyTimeout != 10*time.Second {
		t.Fatalf("spec section 12: 2s heartbeat, suspect at 5s, unhealthy at 10s; got %+v", w)
	}
	if w.LostTimeout <= w.UnhealthyTimeout || w.Retention <= 0 {
		t.Fatalf("unexpected lost/retention defaults %+v", w)
	}
}

func TestValidateRejectsBadWorkerThresholds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero suspect", func(c *Config) { c.Worker.SuspectTimeout = 0 }},
		{"zero lost", func(c *Config) { c.Worker.LostTimeout = 0 }},
		{"zero retention", func(c *Config) { c.Worker.Retention = 0 }},
		{"suspect not below unhealthy", func(c *Config) { c.Worker.SuspectTimeout = c.Worker.UnhealthyTimeout }},
		{"unhealthy not below lost", func(c *Config) { c.Worker.UnhealthyTimeout = c.Worker.LostTimeout }},
		{"suspect under two heartbeats", func(c *Config) { c.Worker.HeartbeatInterval = 3 * time.Second }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
	cfg := Default()
	cfg.Worker.SuspectTimeout = 2 * cfg.Worker.HeartbeatInterval // exactly twice is fine
	if err := cfg.Validate(); err != nil {
		t.Fatalf("exactly two heartbeat intervals must be accepted: %v", err)
	}
}

func TestValidateRejectsBadControlPlaneSettings(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"addr without port": func(c *Config) { c.ControlPlane.Addr = "127.0.0.1" },
		"empty addr":        func(c *Config) { c.ControlPlane.Addr = "" },
		"zero max workers":  func(c *Config) { c.ControlPlane.MaxWorkers = 0 },
		"huge max workers":  func(c *Config) { c.ControlPlane.MaxWorkers = 1 << 30 },
		"short token":       func(c *Config) { c.ControlPlane.Token = "short" },
	} {
		cfg := Default()
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		} else if strings.Contains(err.Error(), "short") && name == "short token" {
			t.Errorf("the error leaks the token: %v", err)
		}
	}
}

func TestControlPlaneRequiresATokenOffLoopback(t *testing.T) {
	token := "a-sufficiently-long-secret"
	for addr, wantErr := range map[string]bool{
		"127.0.0.1:9090": false, "[::1]:9090": false, "localhost:9090": false, "127.0.0.2:1": false,
		":9090": true, "0.0.0.0:9090": true, "10.0.0.5:9090": true, "example.com:9090": true, "[::]:9090": true,
	} {
		c := Default().ControlPlane
		c.Addr = addr
		if err := c.ValidateServe(); (err != nil) != wantErr {
			t.Errorf("%s without a token: error=%v, want error=%v", addr, err, wantErr)
		}
		c.Token = token
		if err := c.ValidateServe(); err != nil {
			t.Errorf("%s with a token must be allowed: %v", addr, err)
		}
	}
}

func TestValidateAgent(t *testing.T) {
	good := Default().Worker
	good.ID, good.Model = "worker-1", "qwen-7b"
	good.BackendURL, good.AdvertiseURL = "http://127.0.0.1:9001", "http://127.0.0.1:9001"
	if err := good.validateAgent(); err != nil {
		t.Fatalf("a complete agent config must validate: %v", err)
	}
	if err := (&WorkerConfig{}).validateAgent(); err == nil {
		t.Fatal("an empty agent config must be rejected")
	}
	for name, mutate := range map[string]func(*WorkerConfig){
		"no id": func(w *WorkerConfig) { w.ID = "" }, "bad id": func(w *WorkerConfig) { w.ID = "a b" },
		"no model": func(w *WorkerConfig) { w.Model = "" }, "no backend": func(w *WorkerConfig) { w.BackendURL = "" },
		"no advertise": func(w *WorkerConfig) { w.AdvertiseURL = "" }, "no control plane": func(w *WorkerConfig) { w.ControlPlaneURL = "" },
		"credentials in a url": func(w *WorkerConfig) { w.BackendURL = "http://u:topsecret@h:1" },
	} {
		w := good
		mutate(&w)
		err := w.validateAgent()
		if err == nil {
			t.Errorf("%s: expected an error", name)
		} else if strings.Contains(err.Error(), "topsecret") {
			t.Errorf("%s: the error leaks a credential: %v", name, err)
		}
	}
	// Other binaries must stay valid without agent identity.
	def := Default()
	if err := def.Validate(); err != nil {
		t.Fatalf("defaults without an agent identity must stay valid: %v", err)
	}
}

func TestLoadRegistryAndAgentSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "worker:\n  id: w1\n  model: qwen-7b\n  backend_url: http://127.0.0.1:9001\n  advertise_url: http://127.0.0.1:9001\n  suspect_timeout: 6s\ncontrol_plane:\n  addr: 127.0.0.1:9191\n  max_workers: 10\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.ID != "w1" || cfg.Worker.SuspectTimeout != 6*time.Second || cfg.ControlPlane.Addr != "127.0.0.1:9191" || cfg.ControlPlane.MaxWorkers != 10 {
		t.Fatalf("unexpected config %+v %+v", cfg.Worker, cfg.ControlPlane)
	}
	if cfg.Worker.UnhealthyTimeout != 10*time.Second {
		t.Fatal("unset fields must keep their defaults")
	}
}

func TestLoadRegistryAndAgentEnvOverrides(t *testing.T) {
	t.Setenv("SERVERFLOW_WORKER_ID", "w-env")
	t.Setenv("SERVERFLOW_WORKER_MODEL", "llama-8b")
	t.Setenv("SERVERFLOW_WORKER_BACKEND_URL", "http://127.0.0.1:1")
	t.Setenv("SERVERFLOW_WORKER_ADVERTISE_URL", "http://127.0.0.1:2")
	t.Setenv("SERVERFLOW_WORKER_CONTROL_PLANE_URL", "http://127.0.0.1:3")
	t.Setenv("SERVERFLOW_WORKER_SUSPECT_TIMEOUT", "7s")
	t.Setenv("SERVERFLOW_WORKER_LOST_TIMEOUT", "40s")
	t.Setenv("SERVERFLOW_WORKER_RETENTION", "2m")
	t.Setenv("SERVERFLOW_CONTROL_PLANE_ADDR", "127.0.0.1:9999")
	t.Setenv("SERVERFLOW_CONTROL_PLANE_TOKEN", "an-environment-secret-token")
	t.Setenv("SERVERFLOW_CONTROL_PLANE_MAX_WORKERS", "25")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	w, c := cfg.Worker, cfg.ControlPlane
	if w.ID != "w-env" || w.Model != "llama-8b" || w.BackendURL != "http://127.0.0.1:1" || w.AdvertiseURL != "http://127.0.0.1:2" || w.ControlPlaneURL != "http://127.0.0.1:3" {
		t.Fatalf("worker identity not applied: %+v", w)
	}
	if w.SuspectTimeout != 7*time.Second || w.LostTimeout != 40*time.Second || w.Retention != 2*time.Minute {
		t.Fatalf("thresholds not applied: %+v", w)
	}
	if c.Addr != "127.0.0.1:9999" || c.Token != "an-environment-secret-token" || c.MaxWorkers != 25 {
		t.Fatalf("control plane settings not applied: %+v", c)
	}
}

// --- independent review and verification findings -----------------------------------------------

func TestSecurityRelevantDefaults(t *testing.T) {
	d := Default()
	if d.ControlPlane.Addr != "127.0.0.1:9090" {
		t.Fatalf("the control plane must default to loopback, got %q", d.ControlPlane.Addr)
	}
	if d.Worker.LostTimeout != 30*time.Second || d.Worker.Retention != 5*time.Minute || d.ControlPlane.MaxWorkers != 1000 {
		t.Fatalf("unexpected defaults: %+v %+v", d.Worker, d.ControlPlane)
	}
	if d.ControlPlane.Token != "" {
		t.Fatal("there must be no default token")
	}
}

func TestExactBoundaries(t *testing.T) {
	ok := func(mut func(*Config)) error { c := Default(); mut(&c); return c.Validate() }
	if err := ok(func(c *Config) { c.ControlPlane.Token = strings.Repeat("t", 16) }); err != nil {
		t.Fatalf("a 16 character token is the minimum and must be accepted: %v", err)
	}
	if err := ok(func(c *Config) { c.ControlPlane.Token = strings.Repeat("t", 15) }); err == nil {
		t.Fatal("a 15 character token must be rejected")
	}
	if err := ok(func(c *Config) { c.ControlPlane.MaxWorkers = 100000 }); err != nil {
		t.Fatalf("100000 workers is the cap: %v", err)
	}
	if err := ok(func(c *Config) { c.ControlPlane.MaxWorkers = 100001 }); err == nil {
		t.Fatal("100001 workers must be rejected")
	}
	if err := ok(func(c *Config) { c.ControlPlane.MaxWorkers = 1 }); err != nil {
		t.Fatalf("one worker is allowed: %v", err)
	}
}

func TestHeartbeatIntervalHasAFloor(t *testing.T) {
	// A 1ms interval would make every agent hammer the control plane.
	c := Default()
	c.Worker.HeartbeatInterval = time.Millisecond
	c.Worker.SuspectTimeout, c.Worker.UnhealthyTimeout, c.Worker.LostTimeout = 5*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "heartbeat_interval") {
		t.Fatalf("a 1ms heartbeat interval must be rejected, got %v", err)
	}
	c.Worker.HeartbeatInterval = 10 * time.Millisecond
	c.Worker.SuspectTimeout, c.Worker.UnhealthyTimeout, c.Worker.LostTimeout = 20*time.Millisecond, 40*time.Millisecond, 80*time.Millisecond
	if err := c.Validate(); err != nil {
		t.Fatalf("10ms is the floor and must be accepted: %v", err)
	}
}

func TestAnAgentWontSendTheTokenOverCleartextToAnotherMachine(t *testing.T) {
	base := func() Config {
		c := Default()
		c.Worker.ID, c.Worker.Model = "w1", "qwen-7b"
		c.Worker.BackendURL, c.Worker.AdvertiseURL = "http://127.0.0.1:9001", "http://127.0.0.1:9001"
		return c
	}
	for url, tok := range map[string]string{
		"http://10.0.0.5:9090": "a-sufficiently-long-token", "http://control-plane.internal:9090": "a-sufficiently-long-token",
		"http://[2001:db8::1]:9090": "a-sufficiently-long-token",
	} {
		c := base()
		c.Worker.ControlPlaneURL, c.ControlPlane.Token = url, tok
		err := c.ValidateAgent()
		if err == nil || !strings.Contains(err.Error(), "https") {
			t.Errorf("%s with a token must be refused (cleartext), got %v", url, err)
		}
		if err != nil && strings.Contains(err.Error(), tok) {
			t.Errorf("the error leaks the token: %v", err)
		}
	}
	for url, tok := range map[string]string{
		"https://control-plane.internal:9090": "a-sufficiently-long-token", "http://127.0.0.1:9090": "a-sufficiently-long-token",
		"http://localhost:9090": "a-sufficiently-long-token", "http://[::1]:9090": "a-sufficiently-long-token",
		"http://10.0.0.5:9090": "", // no token, nothing secret to protect
	} {
		c := base()
		c.Worker.ControlPlaneURL, c.ControlPlane.Token = url, tok
		if err := c.ValidateAgent(); err != nil {
			t.Errorf("%s (token %q) must be accepted: %v", url, tok, err)
		}
	}
}

func TestTruncateForErrorBoundsWhatIsEchoed(t *testing.T) {
	if got := truncateForError(strings.Repeat("a", 64)); len(got) != 64 {
		t.Fatalf("64 characters are kept whole: %d", len(got))
	}
	if got := truncateForError(strings.Repeat("a", 65)); len(got) != 67 || !strings.HasSuffix(got, "...") {
		t.Fatalf("longer text is cut: %q", got)
	}
}
