package resilience

import (
	"testing"
	"time"

	"serverflow/internal/config"
)

func TestParamsFromConfigCarryTheDefaults(t *testing.T) {
	c := config.Default()
	c.Resilience.Breaker.MaxOpenFraction = 0.25
	c.Metrics.MaxModels = 17
	b := BreakerParamsFrom(c)
	want := BreakerParams{ConsecutiveFailures: 5, FailureRatio: 0.5, MinVolume: 10, Window: 10 * time.Second, CoolDown: 10 * time.Second,
		MaxCoolDown: 2 * time.Minute, ProbeConcurrency: 1, ProbeSuccesses: 1, MaxOpenFraction: 0.25, HeaderTimeout: 60 * time.Second}
	if b != want {
		t.Errorf("breaker params = %+v", b)
	}
	if p := BudgetParamsFrom(c); p != (BudgetParams{Ratio: 0.2, Min: 10, Window: 10 * time.Second, MaxModels: 17}) {
		t.Errorf("budget params = %+v", p)
	}
	if p := QueueParamsFrom(c); p != (QueueParams{MaxModelDepth: 128, MaxWait: 3 * time.Second, MaxTenantShare: 0.5, MaxModels: 17}) {
		t.Errorf("queue params = %+v", p)
	}
	if got := AdmissionFrom(c).Limit(); got != c.Admission.MaxGlobalRequests {
		t.Errorf("admission limit = %d", got)
	}
}

func TestNormalizeRescuesDegenerateParams(t *testing.T) {
	p := BreakerParams{}.normalize()
	if p.ConsecutiveFailures < 1 || p.MinVolume < 1 || p.Window < time.Second || p.CoolDown <= 0 || p.MaxCoolDown < p.CoolDown ||
		p.ProbeConcurrency < 1 || p.ProbeSuccesses < 1 {
		t.Fatalf("normalize left %+v", p)
	}
	// The cool-down arithmetic must terminate for any normalised value.
	_ = p.maxLevel()
}

func TestCoolDownLevels(t *testing.T) {
	p := testParams()
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 2 * time.Minute, 2 * time.Minute}
	for l, d := range want {
		if got := p.baseCoolDown(l); got != d {
			t.Errorf("level %d = %v, want %v", l, got, d)
		}
	}
	if p.maxLevel() != 4 {
		t.Errorf("maxLevel = %d", p.maxLevel())
	}
	p.CoolDown, p.MaxCoolDown = 5*time.Second, 5*time.Second
	if p.maxLevel() != 0 || p.baseCoolDown(3) != 5*time.Second {
		t.Error("equal cool-down and cap: no doubling")
	}
}
