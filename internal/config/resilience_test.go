package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func resilienceRegistryConfig() Config {
	c := Default()
	c.Gateway.WorkerSource = WorkerSourceRegistry
	c.Resilience.Mode = ResilienceOn
	return c
}

func TestResilienceDefaults(t *testing.T) {
	r := Default().Resilience
	// The shipped default becomes "on" in the last Phase 15 pull request; until then it is "off".
	if r.Mode != ResilienceOff {
		t.Errorf("mode = %q, want off until the default flip (PR G)", r.Mode)
	}
	want := defaultResilience()
	want.Breaker = ResilienceBreakerConfig{
		ConsecutiveFailures: 5, FailureRatio: 0.5, MinVolume: 10, Window: 10 * time.Second,
		CoolDown: 10 * time.Second, MaxCoolDown: 2 * time.Minute, ProbeConcurrency: 1, ProbeSuccesses: 1,
		MaxOpenFraction: 0.5,
	}
	if r.Breaker != want.Breaker {
		t.Errorf("breaker defaults = %+v", r.Breaker)
	}
	if r.Retry != (ResilienceRetryConfig{BudgetRatio: 0.2, BudgetMin: 10, BudgetWindow: 10 * time.Second, MinRemaining: time.Second}) {
		t.Errorf("retry defaults = %+v", r.Retry)
	}
	if r.Queue != (ResilienceQueueConfig{MaxModelDepth: 128, MaxWait: 3 * time.Second, MaxTenantShare: 0.5}) {
		t.Errorf("queue defaults = %+v", r.Queue)
	}
	if r.Drain.ReadinessGrace != nil || r.Breaker.ProbeHeaderTimeout != 0 {
		t.Errorf("probe_header_timeout and readiness_grace default to unset (derived from the gateway): %+v %+v", r.Breaker, r.Drain)
	}
	if got := r.Breaker.EffectiveProbeHeaderTimeout(60 * time.Second); got != 10*time.Second {
		t.Errorf("effective probe header timeout = %v", got)
	}
	if got := r.Drain.EffectiveReadinessGrace(30 * time.Second); got != 5*time.Second {
		t.Errorf("effective readiness grace = %v", got)
	}
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	// The final defaults must also be valid once the feature is on.
	c = resilienceRegistryConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("default resilience settings invalid in registry mode: %v", err)
	}
}

func TestResilienceEffective(t *testing.T) {
	for _, tc := range []struct {
		mode, source string
		want         bool
	}{
		{ResilienceOn, WorkerSourceRegistry, true},
		{ResilienceOn, WorkerSourceStatic, false},
		{ResilienceOff, WorkerSourceRegistry, false},
		{ResilienceOff, WorkerSourceStatic, false},
		{"", WorkerSourceRegistry, false},
	} {
		if got := (ResilienceConfig{Mode: tc.mode}).Effective(tc.source); got != tc.want {
			t.Errorf("Effective(%q, %q) = %v, want %v", tc.mode, tc.source, got, tc.want)
		}
	}
}

func TestResilienceEffectiveFirstByteDeadline(t *testing.T) {
	r := ResilienceRetryConfig{}
	if got := r.EffectiveFirstByteDeadline(60*time.Second, 120*time.Second); got != 180*time.Second {
		t.Errorf("derived deadline = %v", got)
	}
	r.FirstByteDeadline = 70 * time.Second
	if got := r.EffectiveFirstByteDeadline(60*time.Second, 120*time.Second); got != 70*time.Second {
		t.Errorf("explicit deadline = %v", got)
	}
}

// resilienceEnvCase names one environment variable, a value, and a check on the result. Every setting is listed.
type resilienceEnvCase struct {
	key, value string
	check      func(ResilienceConfig) bool
}

var resilienceEnvCases = []resilienceEnvCase{
	{"MODE", "ON", func(r ResilienceConfig) bool { return r.Mode == ResilienceOn }},
	{"BREAKER_CONSECUTIVE_FAILURES", "7", func(r ResilienceConfig) bool { return r.Breaker.ConsecutiveFailures == 7 }},
	{"BREAKER_FAILURE_RATIO", "0.75", func(r ResilienceConfig) bool { return r.Breaker.FailureRatio == 0.75 }},
	{"BREAKER_MIN_VOLUME", "33", func(r ResilienceConfig) bool { return r.Breaker.MinVolume == 33 }},
	{"BREAKER_WINDOW", "20s", func(r ResilienceConfig) bool { return r.Breaker.Window == 20*time.Second }},
	{"BREAKER_COOL_DOWN", "250ms", func(r ResilienceConfig) bool { return r.Breaker.CoolDown == 250*time.Millisecond }},
	{"BREAKER_MAX_COOL_DOWN", "3m", func(r ResilienceConfig) bool { return r.Breaker.MaxCoolDown == 3*time.Minute }},
	{"BREAKER_PROBE_CONCURRENCY", "3", func(r ResilienceConfig) bool { return r.Breaker.ProbeConcurrency == 3 }},
	{"BREAKER_PROBE_SUCCESSES", "2", func(r ResilienceConfig) bool { return r.Breaker.ProbeSuccesses == 2 }},
	{"BREAKER_PROBE_HEADER_TIMEOUT", "4s", func(r ResilienceConfig) bool { return r.Breaker.ProbeHeaderTimeout == 4*time.Second }},
	{"BREAKER_MAX_OPEN_FRACTION", "0.25", func(r ResilienceConfig) bool { return r.Breaker.MaxOpenFraction == 0.25 }},
	{"RETRY_BUDGET_RATIO", "0.1", func(r ResilienceConfig) bool { return r.Retry.BudgetRatio == 0.1 }},
	{"RETRY_BUDGET_MIN", "3", func(r ResilienceConfig) bool { return r.Retry.BudgetMin == 3 }},
	{"RETRY_BUDGET_WINDOW", "30s", func(r ResilienceConfig) bool { return r.Retry.BudgetWindow == 30*time.Second }},
	{"RETRY_FIRST_BYTE_DEADLINE", "90s", func(r ResilienceConfig) bool { return r.Retry.FirstByteDeadline == 90*time.Second }},
	{"RETRY_MIN_REMAINING", "2s", func(r ResilienceConfig) bool { return r.Retry.MinRemaining == 2*time.Second }},
	{"QUEUE_MAX_MODEL_DEPTH", "64", func(r ResilienceConfig) bool { return r.Queue.MaxModelDepth == 64 }},
	{"QUEUE_MAX_WAIT", "1500ms", func(r ResilienceConfig) bool { return r.Queue.MaxWait == 1500*time.Millisecond }},
	{"QUEUE_MAX_TENANT_SHARE", "0.3", func(r ResilienceConfig) bool { return r.Queue.MaxTenantShare == 0.3 }},
	{"DRAIN_READINESS_GRACE", "2s", func(r ResilienceConfig) bool {
		return r.Drain.ReadinessGrace != nil && *r.Drain.ReadinessGrace == 2*time.Second
	}},
}

func TestResilienceEnvOverlayEveryKey(t *testing.T) {
	for _, tc := range resilienceEnvCases {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv("SERVERFLOW_RESILIENCE_"+tc.key, tc.value)
			c := Default()
			applyEnv(&c)
			if !tc.check(c.Resilience) {
				t.Errorf("SERVERFLOW_RESILIENCE_%s=%s not applied: %+v", tc.key, tc.value, c.Resilience)
			}
			// Exactly this setting changed: no key is wired to a neighbour's field.
			base := Default().Resilience
			if tc.check(base) {
				t.Errorf("the check for %s also holds for the default; the case proves nothing", tc.key)
			}
		})
	}
}

func countDifferingLeaves(a, b reflect.Value) int {
	if a.Kind() != reflect.Struct {
		if a.Interface() != b.Interface() {
			return 1
		}
		return 0
	}
	n := 0
	for i := 0; i < a.NumField(); i++ {
		n += countDifferingLeaves(a.Field(i), b.Field(i))
	}
	return n
}

// Setting one variable changes exactly one field of the section: no key is wired to a neighbour's field.
func TestResilienceEnvOverlayTouchesOnlyItsOwnKey(t *testing.T) {
	for _, tc := range resilienceEnvCases {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv("SERVERFLOW_RESILIENCE_"+tc.key, tc.value)
			c := Default()
			applyEnv(&c)
			if n := countDifferingLeaves(reflect.ValueOf(c.Resilience), reflect.ValueOf(defaultResilience())); n != 1 {
				t.Errorf("%s changed %d fields, want 1", tc.key, n)
			}
		})
	}
}

func TestResilienceEnvUnparsableLeavesDefault(t *testing.T) {
	bad := map[string]string{
		"BREAKER_CONSECUTIVE_FAILURES": "five", "BREAKER_FAILURE_RATIO": "NaN", "BREAKER_MIN_VOLUME": "1.5",
		"BREAKER_WINDOW": "ten", "BREAKER_COOL_DOWN": "10", "BREAKER_MAX_COOL_DOWN": "", "BREAKER_PROBE_CONCURRENCY": "x",
		"BREAKER_PROBE_SUCCESSES": "-", "BREAKER_PROBE_HEADER_TIMEOUT": "1 s", "BREAKER_MAX_OPEN_FRACTION": "Inf",
		"RETRY_BUDGET_RATIO": "lots", "RETRY_BUDGET_MIN": "many", "RETRY_BUDGET_WINDOW": "w", "RETRY_FIRST_BYTE_DEADLINE": "soon",
		"RETRY_MIN_REMAINING": "-", "QUEUE_MAX_MODEL_DEPTH": "deep", "QUEUE_MAX_WAIT": "forever", "QUEUE_MAX_TENANT_SHARE": "-Inf",
		"DRAIN_READINESS_GRACE": "grace",
	}
	if len(bad) != len(resilienceEnvCases)-1 { // every key except MODE
		t.Fatalf("bad-value table has %d entries, want %d", len(bad), len(resilienceEnvCases)-1)
	}
	for k, v := range bad {
		t.Setenv("SERVERFLOW_RESILIENCE_"+k, v)
	}
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Resilience != defaultResilience() {
		t.Errorf("unparsable values must leave the defaults: %+v", cfg.Resilience)
	}
}

func TestResilienceMisspelledModeFailsLoudly(t *testing.T) {
	t.Setenv("SERVERFLOW_RESILIENCE_MODE", "of")
	if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "resilience.mode") {
		t.Fatalf("a misspelled kill switch must fail validation, got %v", err)
	}
	t.Setenv("SERVERFLOW_RESILIENCE_MODE", " Off ")
	cfg, err := Load("")
	if err != nil || cfg.Resilience.Mode != ResilienceOff {
		t.Fatalf("mode off with spaces and capitals: %v %q", err, cfg.Resilience.Mode)
	}
}

func TestResilienceYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	body := `
gateway:
  worker_source: registry
resilience:
  mode: on
  breaker:
    consecutive_failures: 3
    cool_down: 500ms
    max_cool_down: 5s
    failure_ratio: 0.6
  retry:
    budget_ratio: 0.3
    first_byte_deadline: 3m
  queue:
    max_model_depth: 10
    max_wait: 2s
  drain:
    readiness_grace: 1s
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Resilience
	if r.Mode != ResilienceOn || r.Breaker.ConsecutiveFailures != 3 || r.Breaker.CoolDown != 500*time.Millisecond ||
		r.Breaker.MaxCoolDown != 5*time.Second || r.Breaker.FailureRatio != 0.6 || r.Retry.BudgetRatio != 0.3 ||
		r.Retry.FirstByteDeadline != 3*time.Minute || r.Queue.MaxModelDepth != 10 || r.Queue.MaxWait != 2*time.Second ||
		r.Drain.ReadinessGrace == nil || *r.Drain.ReadinessGrace != time.Second {
		t.Errorf("yaml not applied: %+v", r)
	}
	if r.Breaker.MinVolume != 10 { // untouched keys keep their defaults
		t.Errorf("untouched default lost: %+v", r.Breaker)
	}
}

func TestResilienceValidation(t *testing.T) {
	type mut func(*Config)
	cases := []struct {
		name    string
		mut     mut
		wantErr string // "" = valid
	}{
		{"defaults", func(*Config) {}, ""},
		{"consecutive 0", func(c *Config) { c.Resilience.Breaker.ConsecutiveFailures = 0 }, "consecutive_failures"},
		{"consecutive 1", func(c *Config) { c.Resilience.Breaker.ConsecutiveFailures = 1 }, ""},
		{"consecutive 1000", func(c *Config) { c.Resilience.Breaker.ConsecutiveFailures = 1000 }, ""},
		{"consecutive 1001", func(c *Config) { c.Resilience.Breaker.ConsecutiveFailures = 1001 }, "consecutive_failures"},
		{"ratio -0.1", func(c *Config) { c.Resilience.Breaker.FailureRatio = -0.1 }, "failure_ratio"},
		{"ratio 0", func(c *Config) { c.Resilience.Breaker.FailureRatio = 0 }, ""},
		{"ratio 1", func(c *Config) { c.Resilience.Breaker.FailureRatio = 1 }, ""},
		{"ratio 1.1", func(c *Config) { c.Resilience.Breaker.FailureRatio = 1.1 }, "failure_ratio"},
		{"ratio NaN", func(c *Config) { c.Resilience.Breaker.FailureRatio = nan() }, "failure_ratio"},
		{"min volume 0", func(c *Config) { c.Resilience.Breaker.MinVolume = 0 }, "min_volume"},
		{"min volume max", func(c *Config) { c.Resilience.Breaker.MinVolume = 100000 }, ""},
		{"min volume over", func(c *Config) { c.Resilience.Breaker.MinVolume = 100001 }, "min_volume"},
		{"window 999ms", func(c *Config) { c.Resilience.Breaker.Window = 999 * time.Millisecond }, "breaker.window"},
		{"window 1s", func(c *Config) { c.Resilience.Breaker.Window = time.Second }, ""},
		{"window 10m", func(c *Config) { c.Resilience.Breaker.Window = 10 * time.Minute }, ""},
		{"window over", func(c *Config) { c.Resilience.Breaker.Window = 10*time.Minute + 1 }, "breaker.window"},
		{"cool_down below floor", func(c *Config) { c.Resilience.Breaker.CoolDown = 99 * time.Millisecond }, "cool_down"},
		{"cool_down at the test floor", func(c *Config) { c.Resilience.Breaker.CoolDown = 100 * time.Millisecond }, ""},
		{"cool_down 1h", func(c *Config) {
			c.Resilience.Breaker.CoolDown = time.Hour
			c.Resilience.Breaker.MaxCoolDown = time.Hour
		}, ""},
		{"cool_down over 1h", func(c *Config) { c.Resilience.Breaker.CoolDown = time.Hour + 1 }, "cool_down"},
		{"max_cool_down below cool_down", func(c *Config) { c.Resilience.Breaker.MaxCoolDown = 9 * time.Second }, "max_cool_down"},
		{"max_cool_down equal", func(c *Config) { c.Resilience.Breaker.MaxCoolDown = 10 * time.Second }, ""},
		{"max_cool_down over 1h", func(c *Config) { c.Resilience.Breaker.MaxCoolDown = time.Hour + 1 }, "max_cool_down"},
		{"probe concurrency 0", func(c *Config) { c.Resilience.Breaker.ProbeConcurrency = 0 }, "probe_concurrency"},
		{"probe concurrency 16", func(c *Config) { c.Resilience.Breaker.ProbeConcurrency = 16 }, ""},
		{"probe concurrency 17", func(c *Config) { c.Resilience.Breaker.ProbeConcurrency = 17 }, "probe_concurrency"},
		{"probe successes 0", func(c *Config) { c.Resilience.Breaker.ProbeSuccesses = 0 }, "probe_successes"},
		{"probe successes 17", func(c *Config) { c.Resilience.Breaker.ProbeSuccesses = 17 }, "probe_successes"},
		{"probe header below floor", func(c *Config) { c.Resilience.Breaker.ProbeHeaderTimeout = 99 * time.Millisecond }, "probe_header_timeout"},
		{"probe header equals gateway header timeout", func(c *Config) {
			c.Resilience.Breaker.ProbeHeaderTimeout = c.Gateway.UpstreamHeaderTimeout
		}, ""},
		{"probe header above gateway header timeout", func(c *Config) {
			c.Resilience.Breaker.ProbeHeaderTimeout = c.Gateway.UpstreamHeaderTimeout + 1
		}, "probe_header_timeout"},
		{"max open fraction 0", func(c *Config) { c.Resilience.Breaker.MaxOpenFraction = 0 }, ""},
		{"max open fraction 1", func(c *Config) { c.Resilience.Breaker.MaxOpenFraction = 1 }, ""},
		{"max open fraction 1.01", func(c *Config) { c.Resilience.Breaker.MaxOpenFraction = 1.01 }, "max_open_fraction"},
		{"max open fraction negative", func(c *Config) { c.Resilience.Breaker.MaxOpenFraction = -0.5 }, "max_open_fraction"},
		{"budget ratio 1.5", func(c *Config) { c.Resilience.Retry.BudgetRatio = 1.5 }, "budget_ratio"},
		{"budget ratio NaN", func(c *Config) { c.Resilience.Retry.BudgetRatio = nan() }, "budget_ratio"},
		{"budget min -1", func(c *Config) { c.Resilience.Retry.BudgetMin = -1 }, "budget_min"},
		{"budget min 0", func(c *Config) { c.Resilience.Retry.BudgetMin = 0 }, ""},
		{"budget min 1000", func(c *Config) { c.Resilience.Retry.BudgetMin = 1000 }, ""},
		{"budget min 1001", func(c *Config) { c.Resilience.Retry.BudgetMin = 1001 }, "budget_min"},
		{"budget window 999ms", func(c *Config) { c.Resilience.Retry.BudgetWindow = 999 * time.Millisecond }, "budget_window"},
		{"budget window over", func(c *Config) { c.Resilience.Retry.BudgetWindow = 10*time.Minute + 1 }, "budget_window"},
		{"deadline below header timeout", func(c *Config) { c.Resilience.Retry.FirstByteDeadline = 59 * time.Second }, "first_byte_deadline"},
		{"deadline equals header timeout", func(c *Config) { c.Resilience.Retry.FirstByteDeadline = 60 * time.Second }, ""},
		{"deadline derived (0)", func(c *Config) { c.Resilience.Retry.FirstByteDeadline = 0 }, ""},
		{"min remaining negative", func(c *Config) { c.Resilience.Retry.MinRemaining = -1 }, "min_remaining"},
		{"min remaining 1m", func(c *Config) { c.Resilience.Retry.MinRemaining = time.Minute }, ""},
		{"min remaining over 1m", func(c *Config) { c.Resilience.Retry.MinRemaining = time.Minute + 1 }, "min_remaining"},
		{"queue depth 0 = no queue", func(c *Config) { c.Resilience.Queue.MaxModelDepth = 0 }, ""},
		{"queue depth negative", func(c *Config) { c.Resilience.Queue.MaxModelDepth = -1 }, "max_model_depth"},
		{"queue depth 100000", func(c *Config) { c.Resilience.Queue.MaxModelDepth = 100000 }, ""},
		{"queue depth over", func(c *Config) { c.Resilience.Queue.MaxModelDepth = 100001 }, "max_model_depth"},
		{"queue wait 99ms", func(c *Config) { c.Resilience.Queue.MaxWait = 99 * time.Millisecond }, "max_wait"},
		{"queue wait 60s", func(c *Config) { c.Resilience.Queue.MaxWait = time.Minute }, ""},
		{"queue wait 61s", func(c *Config) { c.Resilience.Queue.MaxWait = time.Minute + time.Second }, "max_wait"},
		{"queue wait above an explicit deadline", func(c *Config) {
			c.Gateway.UpstreamHeaderTimeout = time.Second
			c.Resilience.Breaker.ProbeHeaderTimeout = time.Second
			c.Resilience.Retry.FirstByteDeadline = 2 * time.Second
			c.Resilience.Queue.MaxWait = 3 * time.Second
		}, "first-byte deadline"},
		{"queue wait above the derived deadline", func(c *Config) {
			c.Gateway.UpstreamHeaderTimeout = time.Second
			c.Gateway.UpstreamIdleTimeout = time.Second
			c.Resilience.Breaker.ProbeHeaderTimeout = time.Second
			c.Resilience.Queue.MaxWait = 3 * time.Second
		}, "first-byte deadline"},
		{"queue wait equals the deadline", func(c *Config) {
			c.Gateway.UpstreamHeaderTimeout = time.Second
			c.Gateway.UpstreamIdleTimeout = 2 * time.Second
			c.Resilience.Breaker.ProbeHeaderTimeout = time.Second
			c.Resilience.Queue.MaxWait = 3 * time.Second
		}, ""},
		{"tenant share 0 = off", func(c *Config) { c.Resilience.Queue.MaxTenantShare = 0 }, ""},
		{"tenant share 1.1", func(c *Config) { c.Resilience.Queue.MaxTenantShare = 1.1 }, "max_tenant_share"},
		{"tenant share NaN", func(c *Config) { c.Resilience.Queue.MaxTenantShare = nan() }, "max_tenant_share"},
		{"grace negative", func(c *Config) { c.Resilience.Drain.ReadinessGrace = dur(-1) }, "readiness_grace"},
		{"grace 0", func(c *Config) { c.Resilience.Drain.ReadinessGrace = dur(0) }, ""},
		{"grace equals shutdown timeout", func(c *Config) { c.Resilience.Drain.ReadinessGrace = dur(c.Gateway.ShutdownTimeout) }, "shutdown_timeout"},
		{"grace just below shutdown timeout", func(c *Config) { c.Resilience.Drain.ReadinessGrace = dur(c.Gateway.ShutdownTimeout - 1) }, ""},
		{"grace over 5m", func(c *Config) {
			c.Gateway.ShutdownTimeout = time.Hour
			c.Resilience.Drain.ReadinessGrace = dur(5*time.Minute + 1)
		}, "readiness_grace"},
		{"mode typo", func(c *Config) { c.Resilience.Mode = "enabled" }, "resilience.mode"},
		{"mode empty", func(c *Config) { c.Resilience.Mode = "" }, "resilience.mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := resilienceRegistryConfig()
			tc.mut(&c)
			err := c.Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("want valid, got %v", err)
			case tc.wantErr != "" && err == nil:
				t.Errorf("want an error mentioning %q, got none", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

func dur(d time.Duration) *time.Duration { return &d }

func nan() float64 { var z float64; return z / z }

// While the feature is not effective only the mode is checked: static workers and mode off ignore bad settings.
func TestResilienceSettingsIgnoredUnlessEffective(t *testing.T) {
	garbage := func(c *Config) {
		r := &c.Resilience
		r.Breaker.ConsecutiveFailures = -3
		r.Breaker.CoolDown = 1
		r.Breaker.MaxCoolDown = 0
		r.Retry.BudgetRatio = 9
		r.Retry.FirstByteDeadline = time.Nanosecond
		r.Queue.MaxWait = time.Hour
		r.Drain.ReadinessGrace = dur(24 * time.Hour)
	}
	for _, tc := range []struct {
		name         string
		mode, source string
		wantErr      bool
	}{
		{"off + registry", ResilienceOff, WorkerSourceRegistry, false},
		{"off + static", ResilienceOff, WorkerSourceStatic, false},
		{"on + static is a valid no-op", ResilienceOn, WorkerSourceStatic, false},
		{"on + registry validates", ResilienceOn, WorkerSourceRegistry, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			c.Gateway.WorkerSource = tc.source
			c.Resilience.Mode = tc.mode
			garbage(&c)
			if err := c.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestResilienceModeOnWithStaticWorkersStarts(t *testing.T) {
	t.Setenv("SERVERFLOW_RESILIENCE_MODE", "on")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("mode on with static workers must load: %v", err)
	}
	if cfg.Resilience.Effective(cfg.Gateway.WorkerSource) {
		t.Error("static workers must make the feature a no-op")
	}
}

// Defaults that depend on the gateway's own timeouts are derived when unset and clamped to them, so a valid master
// configuration with a short header or shutdown timeout stays valid when the feature defaults to on.
func TestResilienceDerivedDefaultsFollowTheGateway(t *testing.T) {
	for _, tc := range []struct {
		name         string
		header       time.Duration
		shutdown     time.Duration
		wantProbe    time.Duration
		wantGrace    time.Duration
		explicitGood bool
	}{
		{"production defaults", 60 * time.Second, 30 * time.Second, 10 * time.Second, 5 * time.Second, true},
		{"header 10s: probe equals the cap", 10 * time.Second, 30 * time.Second, 10 * time.Second, 5 * time.Second, true},
		{"header 9s clamps the probe", 9 * time.Second, 30 * time.Second, 9 * time.Second, 5 * time.Second, true},
		{"header 5s", 5 * time.Second, 30 * time.Second, 5 * time.Second, 5 * time.Second, true},
		{"shutdown 10s: grace stays 5s", 60 * time.Second, 10 * time.Second, 10 * time.Second, 5 * time.Second, true},
		{"shutdown 9s halves", 60 * time.Second, 9 * time.Second, 10 * time.Second, 4500 * time.Millisecond, true},
		{"shutdown 5s", 60 * time.Second, 5 * time.Second, 10 * time.Second, 2500 * time.Millisecond, true},
		{"shutdown 1s", 60 * time.Second, time.Second, 10 * time.Second, 500 * time.Millisecond, true},
		{"both short", time.Second, 2 * time.Second, time.Second, time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := resilienceRegistryConfig()
			c.Gateway.UpstreamHeaderTimeout = tc.header
			c.Gateway.ShutdownTimeout = tc.shutdown
			// the queue wait must fit the derived first-byte deadline of a short idle timeout
			c.Gateway.UpstreamIdleTimeout = 10 * time.Second
			if err := c.Validate(); err != nil {
				t.Fatalf("a config that is valid without the feature must stay valid with it: %v", err)
			}
			if got := c.Resilience.Breaker.EffectiveProbeHeaderTimeout(c.Gateway.UpstreamHeaderTimeout); got != tc.wantProbe {
				t.Errorf("probe header timeout = %v, want %v", got, tc.wantProbe)
			}
			if got := c.Resilience.Drain.EffectiveReadinessGrace(c.Gateway.ShutdownTimeout); got != tc.wantGrace {
				t.Errorf("readiness grace = %v, want %v", got, tc.wantGrace)
			}
		})
	}
}

// Only explicit values are cross-validated against the gateway.
func TestResilienceExplicitValuesAreStillCheckedAgainstTheGateway(t *testing.T) {
	c := resilienceRegistryConfig()
	c.Gateway.UpstreamHeaderTimeout = 5 * time.Second
	c.Resilience.Breaker.ProbeHeaderTimeout = 6 * time.Second
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "probe_header_timeout") {
		t.Errorf("explicit probe header timeout above the gateway's: %v", err)
	}
	c.Resilience.Breaker.ProbeHeaderTimeout = 5 * time.Second
	if err := c.Validate(); err != nil {
		t.Errorf("explicit value equal to the gateway's: %v", err)
	}
	c = resilienceRegistryConfig()
	c.Gateway.ShutdownTimeout = 4 * time.Second
	c.Resilience.Drain.ReadinessGrace = dur(4 * time.Second)
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "readiness_grace") {
		t.Errorf("explicit grace not shorter than shutdown: %v", err)
	}
	c.Resilience.Drain.ReadinessGrace = dur(0) // an explicit 0 means no grace and is valid
	if err := c.Validate(); err != nil {
		t.Errorf("explicit zero grace: %v", err)
	}
	if got := c.Resilience.Drain.EffectiveReadinessGrace(c.Gateway.ShutdownTimeout); got != 0 {
		t.Errorf("explicit zero must not be replaced by the derived default: %v", got)
	}
}

func TestResilienceDerivedDefaultsFromYAMLAndEnv(t *testing.T) {
	t.Setenv("SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE", "0s")
	cfg, err := Load("")
	if err != nil || cfg.Resilience.Drain.ReadinessGrace == nil || *cfg.Resilience.Drain.ReadinessGrace != 0 {
		t.Fatalf("env zero grace: %v %+v", err, cfg.Resilience.Drain)
	}
}
