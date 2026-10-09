package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuthDefaultsKeepTheGatewayOpen(t *testing.T) {
	cfg := Default()
	if cfg.Auth.Mode != AuthModeOff {
		t.Fatalf("authentication must be off by default, got %q", cfg.Auth.Mode)
	}
	a := cfg.Auth
	if a.CacheTTL != 30*time.Second || a.NegativeTTL != 5*time.Second || a.CacheSize != 10000 || a.StaleGrace != 5*time.Minute {
		t.Fatalf("defaults differ from ADR-014: %+v", a)
	}
	if cfg.Postgres.MaxConns != 10 || cfg.Postgres.ConnectTimeout != 5*time.Second {
		t.Fatalf("postgres defaults: %+v", cfg.Postgres)
	}
}

func TestValidateAuth(t *testing.T) {
	cases := map[string]func(*Config){
		"unknown mode":         func(c *Config) { c.Auth.Mode = "optional" },
		"empty mode":           func(c *Config) { c.Auth.Mode = "" },
		"zero cache ttl":       func(c *Config) { c.Auth.CacheTTL = 0 },
		"huge cache ttl":       func(c *Config) { c.Auth.CacheTTL = 2 * time.Hour },
		"zero negative ttl":    func(c *Config) { c.Auth.NegativeTTL = 0 },
		"negative ttl > ttl":   func(c *Config) { c.Auth.NegativeTTL = c.Auth.CacheTTL + time.Second },
		"zero cache size":      func(c *Config) { c.Auth.CacheSize = 0 },
		"huge cache size":      func(c *Config) { c.Auth.CacheSize = 10_000_000 },
		"grace below ttl":      func(c *Config) { c.Auth.StaleGrace = c.Auth.CacheTTL - time.Second },
		"huge grace":           func(c *Config) { c.Auth.StaleGrace = 48 * time.Hour },
		"zero max conns":       func(c *Config) { c.Postgres.MaxConns = 0 },
		"too many conns":       func(c *Config) { c.Postgres.MaxConns = 101 },
		"zero connect timeout": func(c *Config) { c.Postgres.ConnectTimeout = 0 },
		"huge connect timeout": func(c *Config) { c.Postgres.ConnectTimeout = time.Hour },
		"empty dsn":            func(c *Config) { c.Postgres.DSN = "" },
	}
	for name, mut := range cases {
		cfg := Default()
		mut(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	cfg := Default()
	cfg.Auth.Mode = AuthModeRequired
	cfg.Auth.StaleGrace = cfg.Auth.CacheTTL // the boundary is allowed
	if err := cfg.Validate(); err != nil {
		t.Fatalf("required mode with boundary values: %v", err)
	}
}

func TestAuthLoadFromFileAndEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	yaml := "auth:\n  mode: required\n  cache_ttl: 10s\n  negative_ttl: 2s\n  cache_size: 50\n  stale_grace: 1m\npostgres:\n  dsn: postgres://u:p@127.0.0.1/x\n  max_conns: 3\n  connect_timeout: 2s\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.Mode != "required" || cfg.Auth.CacheTTL != 10*time.Second || cfg.Auth.CacheSize != 50 || cfg.Postgres.MaxConns != 3 {
		t.Fatalf("file not applied: %+v %v", cfg.Auth, cfg.Postgres)
	}
	t.Setenv("SERVERFLOW_AUTH_MODE", "OFF")
	t.Setenv("SERVERFLOW_AUTH_CACHE_TTL", "20s")
	t.Setenv("SERVERFLOW_AUTH_NEGATIVE_TTL", "3s")
	t.Setenv("SERVERFLOW_AUTH_CACHE_SIZE", "77")
	t.Setenv("SERVERFLOW_AUTH_STALE_GRACE", "2m")
	t.Setenv("SERVERFLOW_POSTGRES_MAX_CONNS", "4")
	t.Setenv("SERVERFLOW_POSTGRES_CONNECT_TIMEOUT", "3s")
	t.Setenv("SERVERFLOW_POSTGRES_ALLOW_INSECURE_TRANSPORT", "true")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.Mode != "off" || cfg.Auth.CacheTTL != 20*time.Second || cfg.Auth.NegativeTTL != 3*time.Second || cfg.Auth.CacheSize != 77 ||
		cfg.Auth.StaleGrace != 2*time.Minute || cfg.Postgres.MaxConns != 4 || cfg.Postgres.ConnectTimeout != 3*time.Second || !cfg.Postgres.AllowInsecureTransport {
		t.Fatalf("env not applied: %+v %v", cfg.Auth, cfg.Postgres)
	}
	t.Setenv("SERVERFLOW_AUTH_MODE", "requierd") // a typo must not silently disable authentication
	if _, err := Load(path); err == nil {
		t.Fatal("a mistyped auth.mode must fail validation")
	}
}

func TestDSNIsNeverEchoedOrPrinted(t *testing.T) {
	const pw = "hunter2-very-secret"
	dsn := "postgres://app:" + pw + "@db.internal:5432/serverflow?sslmode=disable"
	cfg := Default()
	cfg.Postgres.DSN = dsn

	// Validation errors.
	cfg.Postgres.MaxConns = 0
	if err := cfg.Validate(); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("validate error: %v", err)
	}
	cfg.Postgres.MaxConns = 10

	// Every way of printing the config.
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, nil))
	log.Info("cfg", "postgres", cfg.Postgres, "all", fmt.Sprintf("%+v", cfg))
	for _, s := range []string{
		fmt.Sprintf("%v", cfg), fmt.Sprintf("%+v", cfg), fmt.Sprintf("%#v", cfg.Postgres), cfg.Postgres.String(), buf.String(),
		fmt.Sprint(cfg.Postgres), fmt.Sprintf("%v", &cfg.Postgres),
	} {
		if strings.Contains(s, pw) {
			t.Fatalf("the DSN password was printed: %s", s)
		}
	}
}
