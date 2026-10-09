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

func TestRateLimitDefaultsAreOff(t *testing.T) {
	c := Default()
	if c.RateLimit.Mode != RateLimitOff {
		t.Fatalf("rate limiting must be off by default, got %q", c.RateLimit.Mode)
	}
	r := c.RateLimit
	if r.BurstSeconds != 60 || r.LeaseTTL != 60*time.Second || r.MaxLocalLeases != 100_000 || len(r.ModelRequestsPerMinute) != 0 {
		t.Fatalf("defaults differ from ADR-015: %+v", r)
	}
	d := c.Redis
	if d.Timeout != 50*time.Millisecond || d.Backoff != time.Second || d.OnFailure != RedisFailClosed || d.RequestMetadata || d.RequestMetadataTTL != time.Hour {
		t.Fatalf("redis defaults: %+v", d)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRedisAndRateLimit(t *testing.T) {
	cases := map[string]func(*Config){
		"empty address":          func(c *Config) { c.Redis.Address = "" },
		"no port":                func(c *Config) { c.Redis.Address = "redis" },
		"bad scheme":             func(c *Config) { c.Redis.Address = "http://redis:6379" },
		"negative db":            func(c *Config) { c.Redis.DB = -1 },
		"huge db":                func(c *Config) { c.Redis.DB = 5000 },
		"zero timeout":           func(c *Config) { c.Redis.Timeout = 0 },
		"huge timeout":           func(c *Config) { c.Redis.Timeout = time.Minute },
		"tiny backoff":           func(c *Config) { c.Redis.Backoff = time.Millisecond },
		"huge backoff":           func(c *Config) { c.Redis.Backoff = time.Hour },
		"bad failure mode":       func(c *Config) { c.Redis.OnFailure = "ajar" },
		"empty failure mode":     func(c *Config) { c.Redis.OnFailure = "" },
		"zero metadata ttl":      func(c *Config) { c.Redis.RequestMetadataTTL = 0 },
		"huge metadata ttl":      func(c *Config) { c.Redis.RequestMetadataTTL = 30 * 24 * time.Hour },
		"bad mode":               func(c *Config) { c.RateLimit.Mode = "optional" },
		"zero burst":             func(c *Config) { c.RateLimit.BurstSeconds = 0 },
		"huge burst":             func(c *Config) { c.RateLimit.BurstSeconds = 3601 },
		"short lease":            func(c *Config) { c.RateLimit.LeaseTTL = 5 * time.Second },
		"huge lease":             func(c *Config) { c.RateLimit.LeaseTTL = 2 * time.Hour },
		"zero pool size":         func(c *Config) { c.Redis.PoolSize = 0 },
		"negative pool size":     func(c *Config) { c.Redis.PoolSize = -1 },
		"huge pool size":         func(c *Config) { c.Redis.PoolSize = 1001 },
		"lease below 3 timeouts": func(c *Config) { c.Redis.Timeout = 5 * time.Second; c.RateLimit.LeaseTTL = 12 * time.Second },
		"zero local leases":      func(c *Config) { c.RateLimit.MaxLocalLeases = 0 },
		"huge local leases":      func(c *Config) { c.RateLimit.MaxLocalLeases = 100_000_000 },
		"zero model cap":         func(c *Config) { c.RateLimit.ModelRequestsPerMinute = map[string]int{"m": 0} },
		"huge model cap":         func(c *Config) { c.RateLimit.ModelRequestsPerMinute = map[string]int{"m": 2_000_000_000} },
		"empty model name":       func(c *Config) { c.RateLimit.ModelRequestsPerMinute = map[string]int{"": 5} },
		"model name with space":  func(c *Config) { c.RateLimit.ModelRequestsPerMinute = map[string]int{"a b": 5} },
		"model name control":     func(c *Config) { c.RateLimit.ModelRequestsPerMinute = map[string]int{"a\nb": 5} },
		"long model name":        func(c *Config) { c.RateLimit.ModelRequestsPerMinute = map[string]int{strings.Repeat("m", 129): 5} },
		"too many model caps": func(c *Config) {
			c.RateLimit.ModelRequestsPerMinute = map[string]int{}
			for i := 0; i < 1001; i++ {
				c.RateLimit.ModelRequestsPerMinute[fmt.Sprintf("m%d", i)] = 1
			}
		},
		"required with nothing to enforce": func(c *Config) { c.RateLimit.Mode = RateLimitRequired },
	}
	usesRedis := map[string]bool{}
	for _, n := range []string{"empty address", "no port", "bad scheme", "negative db", "huge db", "zero timeout", "huge timeout", "tiny backoff", "huge backoff", "bad failure mode", "empty failure mode", "zero metadata ttl", "huge metadata ttl", "zero pool size", "negative pool size", "huge pool size", "lease below 3 timeouts"} {
		usesRedis[n] = true
	}
	for name, mut := range cases {
		cfg := Default()
		if usesRedis[name] { // these settings are only checked when something uses Redis
			cfg.RateLimit.Mode, cfg.RateLimit.ModelRequestsPerMinute = RateLimitRequired, map[string]int{"m": 5}
		}
		mut(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ok := map[string]func(*Config){
		"required with auth": func(c *Config) { c.RateLimit.Mode = RateLimitRequired; c.Auth.Mode = AuthModeRequired },
		"required with model caps and no auth": func(c *Config) {
			c.RateLimit.Mode = RateLimitRequired
			c.RateLimit.ModelRequestsPerMinute = map[string]int{"big": 100}
		},
		"open":               func(c *Config) { c.Redis.OnFailure = RedisFailOpen },
		"url address":        func(c *Config) { c.Redis.Address = "rediss://:pw@redis.example.com:6380/2" },
		"unix address":       func(c *Config) { c.Redis.Address = "unix:///tmp/redis.sock" },
		"boundary lease":     func(c *Config) { c.RateLimit.LeaseTTL = 10 * time.Second },
		"boundary burst":     func(c *Config) { c.RateLimit.BurstSeconds = 3600 },
		"lease = 3 timeouts": func(c *Config) { c.Redis.Timeout = 5 * time.Second; c.RateLimit.LeaseTTL = 15 * time.Second },
	}
	for name, mut := range ok {
		cfg := Default()
		mut(&cfg)
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRedisConfigNeverPrintsOrEchoesSecrets(t *testing.T) {
	const pw = "hunter2-very-secret"
	cfg := Default()
	cfg.Redis.Password = pw
	cfg.Redis.Address = "rediss://user:" + pw + "@redis.example.com:6380"
	outs := []string{fmt.Sprintf("%v", cfg.Redis), fmt.Sprintf("%+v", cfg.Redis), fmt.Sprintf("%#v", cfg.Redis), fmt.Sprintf("%+v", cfg), fmt.Sprintf("%#v", cfg)}
	var b strings.Builder
	slog.New(slog.NewJSONHandler(&b, nil)).Info("x", "redis", cfg.Redis, "cfg", cfg)
	outs = append(outs, b.String())
	for _, o := range outs {
		if strings.Contains(o, pw) {
			t.Fatalf("password leaked: %s", o)
		}
	}
	// Validation errors never contain the address or the password.
	for _, mut := range []func(*Config){
		func(c *Config) { c.Redis.Address = "ftp://u:" + pw + "@h:1" },
		func(c *Config) { c.Redis.OnFailure = pw },
		func(c *Config) { c.RateLimit.Mode = pw },
	} {
		c := Default()
		c.RateLimit.Mode, c.RateLimit.ModelRequestsPerMinute = RateLimitRequired, map[string]int{"m": 5}
		mut(&c)
		if err := c.Validate(); err == nil || strings.Contains(err.Error(), pw) {
			t.Fatalf("error %v", err)
		}
	}
}

func TestRateLimitLoadFromFileAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	yaml := `
auth: {mode: required}
redis: {address: "127.0.0.1:6380", on_failure: open, timeout: 20ms, request_metadata: true}
rate_limit:
  mode: required
  burst_seconds: 30
  model_requests_per_minute: {big: 100}
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SERVERFLOW_REDIS_PASSWORD", "from-env")
	t.Setenv("SERVERFLOW_REDIS_BACKOFF", "2s")
	t.Setenv("SERVERFLOW_REDIS_DB", "4")
	t.Setenv("SERVERFLOW_REDIS_TLS", "true")
	t.Setenv("SERVERFLOW_REDIS_ALLOW_INSECURE_TRANSPORT", "1")
	t.Setenv("SERVERFLOW_RATE_LIMIT_LEASE_TTL", "45s")
	t.Setenv("SERVERFLOW_RATE_LIMIT_MAX_LOCAL_LEASES", "77")
	t.Setenv("SERVERFLOW_RATE_LIMIT_MODEL_REQUESTS_PER_MINUTE", "a=5, b=6")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	r, l := cfg.Redis, cfg.RateLimit
	if r.Address != "127.0.0.1:6380" || r.OnFailure != "open" || r.Timeout != 20*time.Millisecond || !r.RequestMetadata ||
		r.Password != "from-env" || r.Backoff != 2*time.Second || r.DB != 4 || !r.TLS || !r.AllowInsecureTransport {
		t.Fatalf("redis: %+v", r)
	}
	if l.Mode != "required" || l.BurstSeconds != 30 || l.LeaseTTL != 45*time.Second || l.MaxLocalLeases != 77 ||
		len(l.ModelRequestsPerMinute) != 2 || l.ModelRequestsPerMinute["a"] != 5 || l.ModelRequestsPerMinute["b"] != 6 {
		t.Fatalf("rate limit: %+v", l)
	}
	// Garbage in an override is ignored (the established behaviour) and the mode typo is caught by validation.
	t.Setenv("SERVERFLOW_RATE_LIMIT_MODEL_REQUESTS_PER_MINUTE", "a=5,b")
	t.Setenv("SERVERFLOW_RATE_LIMIT_MODE", "Requird")
	if _, err := Load(path); err == nil {
		t.Fatal("a mistyped mode was accepted")
	}
}

func TestRedisSettingsAreOnlyCheckedWhenRedisIsUsed(t *testing.T) {
	bad := func(c *Config) {
		c.Redis.Address = "http://not-redis"
		c.Redis.OnFailure = "ajar"
		c.Redis.Timeout = 0
		c.Redis.PoolSize = -3
	}
	cfg := Default()
	bad(&cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("with rate limiting off and metadata off, redis.* changes nothing, but: %v", err)
	}
	cfg = Default()
	bad(&cfg)
	cfg.Redis.RequestMetadata = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("request metadata uses Redis, so a malformed redis.address must be refused")
	}
	cfg = Default()
	bad(&cfg)
	cfg.RateLimit.Mode, cfg.RateLimit.ModelRequestsPerMinute = RateLimitRequired, map[string]int{"m": 5}
	if err := cfg.Validate(); err == nil {
		t.Fatal("rate_limit.mode=required uses Redis, so a malformed redis.address must be refused")
	}
}
