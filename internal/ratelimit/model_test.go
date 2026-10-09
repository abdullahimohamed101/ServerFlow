package ratelimit

import (
	"strings"
	"testing"
)

var testParams = Params{BurstSeconds: 60, LeaseTTLMs: 60_000}

func req(tenant string, lim Limits, cost int) ModelRequest {
	return ModelRequest{TenantID: tenant, Model: "m", Cost: cost, Limits: lim, LeaseID: "l"}
}

func TestModelRequestsBoundary(t *testing.T) {
	for _, n := range []int{1, 2, 7, 60, 100} {
		m := NewModel(testParams)
		lim := Limits{RequestsPerMinute: n}
		for i := 0; i < n; i++ {
			if o := m.Allow(0, req("a", lim, 1)); !o.Allowed {
				t.Fatalf("quota %d: request %d refused", n, i+1)
			}
		}
		o := m.Allow(0, req("a", lim, 1))
		if o.Allowed || o.Limit != LimitRequests {
			t.Fatalf("quota %d: request %d should be refused for requests, got %+v", n, n+1, o)
		}
	}
}

func TestModelTokensBoundary(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{TokensPerMinute: 1000}
	for i := 0; i < 10; i++ {
		if o := m.Allow(0, req("a", lim, 100)); !o.Allowed {
			t.Fatalf("request %d refused", i)
		}
	}
	if o := m.Allow(0, req("a", lim, 1)); o.Allowed || o.Limit != LimitTokens {
		t.Fatalf("1000 tokens spent, 1001st must be refused for tokens, got %+v", o)
	}
}

func TestModelConcurrencyBoundaryAndRelease(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{MaxConcurrent: 3}
	for i := 0; i < 3; i++ {
		r := req("a", lim, 1)
		r.LeaseID = string(rune('a' + i))
		if o := m.Allow(0, r); !o.Allowed {
			t.Fatalf("lease %d refused", i)
		}
	}
	if o := m.Allow(0, req("a", lim, 1)); o.Allowed || o.Limit != LimitConcurrency || o.RetryMs != 1000 {
		t.Fatalf("4th must be refused for concurrency, got %+v", o)
	}
	if !m.Release("a", "b") {
		t.Fatal("release of a held lease reported false")
	}
	if m.Release("a", "b") {
		t.Fatal("double release reported true")
	}
	r := req("a", lim, 1)
	r.LeaseID = "z"
	if o := m.Allow(0, r); !o.Allowed {
		t.Fatalf("a slot should have been free: %+v", o)
	}
}

func TestModelLeaseExpiryAndRenewal(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{MaxConcurrent: 1}
	if o := m.Allow(0, req("a", lim, 1)); !o.Allowed {
		t.Fatal("first refused")
	}
	if !m.Renew(30_000, "a", "l") {
		t.Fatal("renew of a live lease failed")
	}
	// Renewed at 30s, so alive until 90s.
	if o := m.Allow(89_999, ModelRequest{TenantID: "a", Limits: lim, Cost: 1, LeaseID: "x"}); o.Allowed {
		t.Fatal("lease expired early")
	}
	if o := m.Allow(90_000, ModelRequest{TenantID: "a", Limits: lim, Cost: 1, LeaseID: "x"}); !o.Allowed {
		t.Fatalf("lease should have expired at its expiry: %+v", o)
	}
	if m.Renew(200_000, "a", "l") {
		t.Fatal("an expired lease must not be revived")
	}
}

func TestModelRefillIsContinuous(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 60} // one per second
	for i := 0; i < 60; i++ {
		m.Allow(0, req("a", lim, 1))
	}
	o := m.Allow(0, req("a", lim, 1))
	if o.Allowed || o.RetryMs != 1000 {
		t.Fatalf("empty bucket: want refusal with 1000 ms wait, got %+v", o)
	}
	if m.Allow(999, req("a", lim, 1)).Allowed {
		t.Fatal("refilled too early")
	}
	if !m.Allow(1000, req("a", lim, 1)).Allowed {
		t.Fatal("one second later a request must pass")
	}
	// Half a second after that nothing; a full second after, one more.
	if m.Allow(1500, req("a", lim, 1)).Allowed {
		t.Fatal("0.5 s is not enough")
	}
	if !m.Allow(2000, req("a", lim, 1)).Allowed {
		t.Fatal("1 s later must pass")
	}
}

func TestModelBurstEqualsWindowAndCapsAtCapacity(t *testing.T) {
	for _, burst := range []int{10, 60, 120} {
		m := NewModel(Params{BurstSeconds: burst, LeaseTTLMs: 60_000})
		lim := Limits{RequestsPerMinute: 60}
		// Long idle: the bucket is full but never more than burst seconds' worth.
		want := burst // 1 token per second
		n := 0
		for m.Allow(10_000_000, req("a", lim, 1)).Allowed {
			n++
			if n > 1000 {
				t.Fatal("never refused")
			}
		}
		if n != want {
			t.Fatalf("burst %ds admitted %d, want %d", burst, n, want)
		}
	}
}

func TestModelRefusalConsumesNothing(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 10, TokensPerMinute: 100}
	if o := m.Allow(0, req("a", lim, 100)); !o.Allowed { // spends 1 request token and all 100 tokens
		t.Fatalf("%+v", o)
	}
	for i := 0; i < 10; i++ {
		if o := m.Allow(0, req("a", lim, 50)); o.Allowed || o.Limit != LimitTokens {
			t.Fatalf("attempt %d: %+v", i, o)
		}
	}
	// The request bucket must still hold 9 of its 10 tokens: ten refusals for tokens spent none.
	n := 0
	for m.Allow(0, req("a", Limits{RequestsPerMinute: 10}, 1)).Allowed {
		n++
		if n > 100 {
			t.Fatal("never refused")
		}
	}
	if n != 9 {
		t.Fatalf("request bucket held %d tokens, want 9", n)
	}
}

func TestModelRefusedConcurrencyConsumesNoTokens(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 5, MaxConcurrent: 1}
	m.Allow(0, req("a", lim, 1))
	for i := 0; i < 20; i++ {
		r := req("a", lim, 1)
		r.LeaseID = "x"
		if o := m.Allow(0, r); o.Allowed || o.Limit != LimitConcurrency {
			t.Fatalf("%+v", o)
		}
	}
	m.Release("a", "l")
	n := 0
	for i := 0; i < 10; i++ {
		r := req("a", lim, 1)
		r.LeaseID = string(rune('A' + i))
		if m.Allow(0, r).Allowed {
			n++
			m.Release("a", r.LeaseID)
		}
	}
	if n != 4 {
		t.Fatalf("20 refusals must not have spent request tokens: admitted %d, want 4", n)
	}
}

func TestModelOversizedCostNeedsAFullBucket(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{TokensPerMinute: 1000}
	if o := m.Allow(0, req("a", lim, 5000)); !o.Allowed {
		t.Fatalf("a request bigger than the bucket must pass on a full bucket: %+v", o)
	}
	o := m.Allow(0, req("a", lim, 5000))
	if o.Allowed || o.Limit != LimitTokens || o.RetryMs != 60_000 {
		t.Fatalf("the bucket is empty: want a 60 s wait, got %+v", o)
	}
	if !m.Allow(60_000, req("a", lim, 5000)).Allowed {
		t.Fatal("after the advertised wait the request must pass")
	}
}

func TestModelRetryAfterIsAccurate(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{TokensPerMinute: 600} // 10 tokens per second
	m.Allow(0, req("a", lim, 600))
	o := m.Allow(0, req("a", lim, 25))
	if o.Allowed {
		t.Fatal("should be refused")
	}
	if o.RetryMs != 2500 {
		t.Fatalf("wait %d ms, want 2500", o.RetryMs)
	}
	if m.Allow(o.RetryMs-1, req("a", lim, 25)).Allowed {
		t.Fatal("passed before the advertised wait")
	}
	if !m.Allow(o.RetryMs, req("a", lim, 25)).Allowed {
		t.Fatal("did not pass after the advertised wait")
	}
}

func TestModelLongestWaitIsReported(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 60, TokensPerMinute: 60}
	m.Allow(0, req("a", lim, 60)) // tokens empty, requests 59/60
	for i := 0; i < 59; i++ {
		m.Allow(0, req("a", Limits{RequestsPerMinute: 60}, 1))
	}
	// requests empty (1 s wait), tokens empty (cost 30 -> 30 s wait).
	o := m.Allow(0, req("a", lim, 30))
	if o.Allowed || o.Limit != LimitTokens || o.RetryMs != 30_000 {
		t.Fatalf("%+v", o)
	}
}

func TestModelPerModelCapIsGlobalAcrossTenants(t *testing.T) {
	m := NewModel(testParams)
	for i := 0; i < 5; i++ {
		r := ModelRequest{TenantID: string(rune('a' + i)), Model: "big", Cost: 1, ModelRequestsPerMinute: 5}
		if !m.Allow(0, r).Allowed {
			t.Fatalf("request %d refused", i)
		}
	}
	if o := m.Allow(0, ModelRequest{TenantID: "zz", Model: "big", Cost: 1, ModelRequestsPerMinute: 5}); o.Allowed || o.Limit != LimitModel {
		t.Fatalf("%+v", o)
	}
	// Another model is unaffected, and no tenant means only the model cap applies.
	if !m.Allow(0, ModelRequest{Model: "small", Cost: 1, ModelRequestsPerMinute: 5}).Allowed {
		t.Fatal("a different model was limited")
	}
}

func TestModelTenantsAreIsolated(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 3}
	for i := 0; i < 3; i++ {
		m.Allow(0, req("a", lim, 1))
	}
	if m.Allow(0, req("a", lim, 1)).Allowed {
		t.Fatal("a should be exhausted")
	}
	if !m.Allow(0, req("b", lim, 1)).Allowed {
		t.Fatal("b was affected by a")
	}
}

func TestModelQuotaLoweredClampsToNewCapacity(t *testing.T) {
	m := NewModel(testParams)
	m.Allow(0, req("a", Limits{RequestsPerMinute: 1000}, 1)) // level 999 tokens
	n := 0
	for m.Allow(0, req("a", Limits{RequestsPerMinute: 5}, 1)).Allowed {
		n++
		if n > 100 {
			t.Fatal("not clamped")
		}
	}
	if n != 5 {
		t.Fatalf("after lowering the quota to 5, admitted %d", n)
	}
}

func TestModelClockGoingBackwardsNeverRemovesTokens(t *testing.T) {
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 2}
	m.Allow(100_000, req("a", lim, 1))
	if !m.Allow(50_000, req("a", lim, 1)).Allowed {
		t.Fatal("second request of two refused after a backwards step")
	}
	if m.Allow(50_000, req("a", lim, 1)).Allowed {
		t.Fatal("a backwards step must not mint tokens")
	}
}

func TestKeysAreInjectiveAndBraceFree(t *testing.T) {
	names := []string{"\x01", "\x1f", "\x7f", "\x80", "\xff", "a\xffb", "a\nb", "a}:conc", "a}", "{a", "a}:req}:req", "%7D", "}", "{", "", "a b", "é", "\x00", strings.Repeat("a", 300)}
	seen := map[string]string{}
	for _, n := range names {
		for _, k := range []string{keyRequests(n), keyTokens(n), keyConcurrency(n), keyModel(n)} {
			if prev, dup := seen[k]; dup {
				t.Fatalf("key %q produced by %q and %q", k, prev, n)
			}
			seen[k] = n
			for i := 0; i < len(k); i++ {
				if k[i] < 0x21 || k[i] > 0x7e {
					t.Fatalf("key %q contains the unprintable byte %#x from name %q", k, k[i], n)
				}
			}
			inner := strings.TrimPrefix(strings.TrimPrefix(k, "rl:model:{"), "rl:{")
			if strings.Count(inner, "{")+strings.Count(inner, "}") != 1 {
				t.Fatalf("key %q has extra braces", k)
			}
		}
	}
}

func TestModelClockStepBackThenForwardCreditsOnlyRealTime(t *testing.T) {
	// A Redis clock that steps back 60 s and later returns (seen with a Docker VM) must not mint a minute of
	// tokens: the bucket keeps its later stamp, so the refill after the return is the real elapsed time.
	m := NewModel(testParams)
	lim := Limits{RequestsPerMinute: 60} // 1 token per second
	for i := 0; i < 60; i++ {
		m.Allow(100_000, req("a", lim, 1))
	}
	if m.Allow(100_000, req("a", lim, 1)).Allowed {
		t.Fatal("bucket should be empty")
	}
	for step := int64(0); step < 5; step++ { // the clock reads 40 s while real time passes
		if m.Allow(40_000+step, req("a", lim, 1)).Allowed {
			t.Fatal("tokens appeared while the clock was behind")
		}
	}
	// Real time is now 1.5 s after the step; the clock is right again.
	n := 0
	for m.Allow(101_500, req("a", lim, 1)).Allowed {
		n++
		if n > 100 {
			t.Fatal("unbounded")
		}
	}
	if n != 1 {
		t.Fatalf("admitted %d after a step back and forward, want the 1 token that really refilled", n)
	}
}
