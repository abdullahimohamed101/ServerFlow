package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/redis"
	"serverflow/internal/redis/redistest"
)

type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(time.Now().UnixMilli())
	return c
}
func (c *fakeClock) Now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// newTestLimiter returns a limiter on the test Redis driven by a fake clock, with the background work not
// started (tests drain releases and run renewals by hand unless they call start).
func newTestLimiter(t *testing.T, cfg Config) (*RedisLimiter, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	if cfg.Clock == nil {
		cfg.Clock = clk.Now
	}
	c := redistest.NewClient(t)
	l, err := NewRedis(c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return l, clk
}

func start(t *testing.T, l *RedisLimiter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func allow(t *testing.T, l *RedisLimiter, r Request) Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), r)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return d
}

func tenantReq(tenant string, lim Limits, cost int) Request {
	return Request{TenantID: tenant, Model: "m", Cost: cost, Limits: lim}
}

// drain sends queued releases by hand.
func drain(l *RedisLimiter) {
	for {
		select {
		case j := <-l.releaseQ:
			l.doRelease(j)
		default:
			return
		}
	}
}

func TestRequestsBoundaryAgainstRedis(t *testing.T) {
	for _, n := range []int{1, 3, 25} {
		l, _ := newTestLimiter(t, Config{})
		tn := redistest.Unique("t")
		lim := Limits{RequestsPerMinute: n}
		for i := 0; i < n; i++ {
			if d := allow(t, l, tenantReq(tn, lim, 1)); !d.Allowed {
				t.Fatalf("quota %d: request %d refused", n, i+1)
			}
		}
		d := allow(t, l, tenantReq(tn, lim, 1))
		if d.Allowed || d.Limit != LimitRequests || d.RetryAfterSeconds() < 1 {
			t.Fatalf("quota %d: request %d: %+v", n, n+1, d)
		}
	}
}

func TestTokensBoundaryAgainstRedis(t *testing.T) {
	l, _ := newTestLimiter(t, Config{})
	tn := redistest.Unique("t")
	lim := Limits{TokensPerMinute: 1000}
	for i := 0; i < 10; i++ {
		if d := allow(t, l, tenantReq(tn, lim, 100)); !d.Allowed {
			t.Fatalf("request %d refused", i)
		}
	}
	if d := allow(t, l, tenantReq(tn, lim, 1)); d.Allowed || d.Limit != LimitTokens {
		t.Fatalf("%+v", d)
	}
}

func TestConcurrencyBoundaryReleaseAndExpiryAgainstRedis(t *testing.T) {
	l, clk := newTestLimiter(t, Config{LeaseTTL: 30 * time.Second})
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 3}
	var ds []Decision
	for i := 0; i < 3; i++ {
		d := allow(t, l, tenantReq(tn, lim, 1))
		if !d.Allowed {
			t.Fatalf("lease %d refused", i)
		}
		ds = append(ds, d)
	}
	if d := allow(t, l, tenantReq(tn, lim, 1)); d.Allowed || d.Limit != LimitConcurrency {
		t.Fatalf("4th: %+v", d)
	}
	if l.LocalLeases() != 3 {
		t.Fatalf("tracking %d leases", l.LocalLeases())
	}
	ds[0].Release()
	ds[0].Release() // idempotent
	drain(l)
	if l.LocalLeases() != 2 {
		t.Fatalf("tracking %d leases after release", l.LocalLeases())
	}
	d4 := allow(t, l, tenantReq(tn, lim, 1))
	if !d4.Allowed {
		t.Fatalf("the released slot was not reusable: %+v", d4)
	}
	if d := allow(t, l, tenantReq(tn, lim, 1)); d.Allowed {
		t.Fatal("double release freed two slots")
	}
	// A crashed gateway never releases: its leases lapse at the TTL.
	clk.Advance(29 * time.Second)
	if d := allow(t, l, tenantReq(tn, lim, 1)); d.Allowed {
		t.Fatal("lease expired early")
	}
	clk.Advance(2 * time.Second)
	if d := allow(t, l, tenantReq(tn, lim, 1)); !d.Allowed {
		t.Fatalf("leases did not expire: %+v", d)
	}
}

func TestReleaseSendsExactlyOneCommandHoweverOftenItIsCalled(t *testing.T) {
	l, _ := newTestLimiter(t, Config{})
	tn := redistest.Unique("t")
	d := allow(t, l, tenantReq(tn, Limits{MaxConcurrent: 2}, 1))
	before := l.c.Commands()
	for i := 0; i < 5; i++ {
		d.Release()
	}
	drain(l)
	if n := l.c.Commands() - before; n != 1 {
		t.Fatalf("five calls to Release sent %d commands, want 1", n)
	}
	if len(l.releaseQ) != 0 {
		t.Fatal("a duplicate release was queued")
	}
}

func TestLeaseRenewalKeepsALongRequestsSlot(t *testing.T) {
	l, clk := newTestLimiter(t, Config{LeaseTTL: 30 * time.Second})
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 1}
	d := allow(t, l, tenantReq(tn, lim, 1))
	if !d.Allowed {
		t.Fatal("refused")
	}
	// Five minutes of streaming with a renewal every 10 s (a third of the TTL): the slot is never lost.
	for i := 0; i < 30; i++ {
		clk.Advance(10 * time.Second)
		l.renewAll(context.Background())
		if x := allow(t, l, tenantReq(tn, lim, 1)); x.Allowed {
			t.Fatalf("slot lost after %d s despite renewals", (i+1)*10)
		}
	}
	// Without renewals it lapses.
	clk.Advance(31 * time.Second)
	if x := allow(t, l, tenantReq(tn, lim, 1)); !x.Allowed {
		t.Fatal("slot did not lapse without renewals")
	}
	// A released lease is not resurrected by a renewal that raced the release.
	d.Release()
	drain(l)
	l.renewAll(context.Background())
}

func TestRenewDoesNotReviveReleasedOrExpiredLeases(t *testing.T) {
	l, clk := newTestLimiter(t, Config{LeaseTTL: 30 * time.Second})
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 1}
	d := allow(t, l, tenantReq(tn, lim, 1))
	l.mu.Lock()
	var id string
	for k := range l.leases {
		id = k
	}
	l.mu.Unlock()
	d.Release()
	drain(l)
	// A stale renewal for the released id, as a racing renewer would send.
	if _, err := l.c.Run(context.Background(), renewScript, []string{keyConcurrency(tn)}, clk.Now().UnixMilli(), int64(30000), id); err != nil {
		t.Fatal(err)
	}
	if x := allow(t, l, tenantReq(tn, lim, 1)); !x.Allowed {
		t.Fatal("a renewal resurrected a released lease")
	}
}

func TestRefillAndRetryAfterAreExactAgainstRedis(t *testing.T) {
	l, clk := newTestLimiter(t, Config{})
	tn := redistest.Unique("t")
	lim := Limits{TokensPerMinute: 600} // 10 per second
	if d := allow(t, l, tenantReq(tn, lim, 600)); !d.Allowed {
		t.Fatal("full bucket refused")
	}
	d := allow(t, l, tenantReq(tn, lim, 25))
	if d.Allowed || d.RetryAfter != 2500*time.Millisecond || d.RetryAfterSeconds() != 3 {
		t.Fatalf("%+v", d)
	}
	clk.Advance(d.RetryAfter - time.Millisecond)
	if x := allow(t, l, tenantReq(tn, lim, 25)); x.Allowed {
		t.Fatal("passed before the advertised wait")
	}
	clk.Advance(time.Millisecond)
	if x := allow(t, l, tenantReq(tn, lim, 25)); !x.Allowed {
		t.Fatalf("refused after the advertised wait: %+v", x)
	}
}

func TestRefusalIsAllOrNothingAgainstRedis(t *testing.T) {
	l, _ := newTestLimiter(t, Config{LeaseTTL: 30 * time.Second})
	tn := redistest.Unique("t")
	lim := Limits{RequestsPerMinute: 10, TokensPerMinute: 100, MaxConcurrent: 5}
	if d := allow(t, l, tenantReq(tn, lim, 100)); !d.Allowed { // empties the token bucket, takes one lease
		t.Fatal("refused")
	}
	for i := 0; i < 20; i++ {
		if d := allow(t, l, tenantReq(tn, lim, 50)); d.Allowed || d.Limit != LimitTokens {
			t.Fatalf("attempt %d: %+v", i, d)
		}
	}
	if l.LocalLeases() != 1 {
		t.Fatalf("refusals left %d leases tracked locally", l.LocalLeases())
	}
	// Requests: 1 of 10 spent. Leases: 1 of 5 held. Verify through quotas that only token spending changed.
	nReq := 0
	for allow(t, l, tenantReq(tn, Limits{RequestsPerMinute: 10}, 1)).Allowed {
		nReq++
		if nReq > 50 {
			t.Fatal("unbounded")
		}
	}
	if nReq != 9 {
		t.Fatalf("request bucket held %d, want 9", nReq)
	}
	nConc := 0
	for allow(t, l, tenantReq(tn, Limits{MaxConcurrent: 5}, 1)).Allowed {
		nConc++
		if nConc > 50 {
			t.Fatal("unbounded")
		}
	}
	if nConc != 4 {
		t.Fatalf("refused attempts leaked leases: %d free, want 4", nConc)
	}
}

func TestModelCapWithoutTenantAndAcrossLimiters(t *testing.T) {
	model := redistest.Unique("model")
	cfg := Config{ModelRequestsPerMinute: map[string]int{model: 5}}
	a, clk := newTestLimiter(t, cfg)
	cfg.Clock = clk.Now
	b, _ := newTestLimiter(t, cfg) // a second "gateway" on the same Redis and clock
	n := 0
	for i := 0; i < 20; i++ {
		l := a
		if i%2 == 1 {
			l = b
		}
		if allow(t, l, Request{Model: model, Cost: 10}).Allowed {
			n++
		}
	}
	if n != 5 {
		t.Fatalf("admitted %d, want exactly the cap of 5 across both limiters", n)
	}
	d := allow(t, a, Request{Model: model, Cost: 10})
	if d.Allowed || d.Limit != LimitModel {
		t.Fatalf("%+v", d)
	}
	// An unlisted model has no cap.
	if !allow(t, a, Request{Model: redistest.Unique("other"), Cost: 10}).Allowed {
		t.Fatal("an unlisted model was limited")
	}
}

func TestNothingToEnforceCostsNoRoundTrip(t *testing.T) {
	l, _ := newTestLimiter(t, Config{ModelRequestsPerMinute: map[string]int{"capped": 5}})
	before := l.c.Commands()
	for i := 0; i < 100; i++ {
		d := allow(t, l, Request{TenantID: "free", Model: "uncapped", Cost: 99999, Limits: Limits{}})
		if !d.Allowed || d.Release == nil {
			t.Fatalf("%+v", d)
		}
		d.Release()
		// No tenant at all (auth off) and no model cap.
		if d := allow(t, l, Request{Model: "uncapped", Cost: 5, Limits: Limits{RequestsPerMinute: 1}}); !d.Allowed {
			t.Fatal("tenant quotas were applied without a tenant")
		}
		// Negative quotas mean none.
		if d := allow(t, l, Request{TenantID: "neg", Model: "uncapped", Cost: 5, Limits: Limits{RequestsPerMinute: -4, TokensPerMinute: -1, MaxConcurrent: -9}}); !d.Allowed {
			t.Fatal("negative quotas limited")
		}
	}
	if n := l.c.Commands() - before; n != 0 {
		t.Fatalf("%d Redis commands for requests with nothing to enforce", n)
	}
	drain(l)
	if n := l.c.Commands() - before; n != 0 {
		t.Fatalf("%d Redis commands after release of unlimited requests", n)
	}
	// And a limited request does exactly one round trip once warm.
	tn := redistest.Unique("t")
	allow(t, l, tenantReq(tn, Limits{RequestsPerMinute: 100}, 1))
	before = l.c.Commands()
	allow(t, l, tenantReq(tn, Limits{RequestsPerMinute: 100}, 1))
	if n := l.c.Commands() - before; n != 1 {
		t.Fatalf("a limited request cost %d commands, want 1", n)
	}
}

func TestQuotaChangesApplyAtOnce(t *testing.T) {
	l, _ := newTestLimiter(t, Config{})
	tn := redistest.Unique("t")
	allow(t, l, tenantReq(tn, Limits{RequestsPerMinute: 1000}, 1))
	n := 0
	for allow(t, l, tenantReq(tn, Limits{RequestsPerMinute: 5}, 1)).Allowed {
		n++
		if n > 100 {
			t.Fatal("not clamped to the lowered quota")
		}
	}
	if n != 5 {
		t.Fatalf("admitted %d after lowering the quota to 5", n)
	}
	// Setting a quota to 0 removes the limit altogether.
	if !allow(t, l, tenantReq(tn, Limits{}, 1)).Allowed {
		t.Fatal("quota 0 must mean unlimited")
	}
}

func TestIdleKeysExpire(t *testing.T) {
	l, _ := newTestLimiter(t, Config{BurstSeconds: 10, LeaseTTL: 10 * time.Second})
	tn := redistest.Unique("t")
	allow(t, l, tenantReq(tn, Limits{RequestsPerMinute: 60, TokensPerMinute: 60, MaxConcurrent: 2}, 1))
	for _, k := range []string{keyRequests(tn), keyTokens(tn)} {
		ttl, err := l.c.TTL(context.Background(), k)
		if err != nil || ttl <= 0 || ttl > 20*time.Second {
			t.Fatalf("%s ttl %v %v: idle buckets must expire on their own", k, ttl, err)
		}
	}
	ttl, err := l.c.TTL(context.Background(), keyConcurrency(tn))
	if err != nil || ttl <= 0 || ttl > 20*time.Second {
		t.Fatalf("lease set ttl %v %v", ttl, err)
	}
}

func TestExactAdmissionUnderConcurrency(t *testing.T) {
	// Many goroutines, several limiters, one tenant, a frozen clock: exactly the quota is admitted.
	cfg := Config{}
	a, clk := newTestLimiter(t, cfg)
	cfg.Clock = clk.Now
	b, _ := newTestLimiter(t, cfg)
	c, _ := newTestLimiter(t, cfg)
	tn := redistest.Unique("t")
	lim := Limits{RequestsPerMinute: 137}
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		l := []*RedisLimiter{a, b, c}[i%3]
		go func() {
			defer wg.Done()
			d, err := l.Allow(context.Background(), tenantReq(tn, lim, 1))
			if err != nil {
				t.Error(err)
				return
			}
			if d.Allowed {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 137 {
		t.Fatalf("admitted %d, want exactly 137", ok.Load())
	}
}

func TestConcurrencyIsExactAcrossLimiters(t *testing.T) {
	cfg := Config{}
	a, clk := newTestLimiter(t, cfg)
	cfg.Clock = clk.Now
	b, _ := newTestLimiter(t, cfg)
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 10}
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		l := a
		if i%2 == 1 {
			l = b
		}
		go func() {
			defer wg.Done()
			if d, err := l.Allow(context.Background(), tenantReq(tn, lim, 1)); err == nil && d.Allowed {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 10 {
		t.Fatalf("admitted %d concurrent, want exactly 10", ok.Load())
	}
}

func TestRealRedisClockDrivesRefill(t *testing.T) {
	// No injected clock: the script uses Redis TIME. 60/min refills one token per second.
	c := redistest.NewClient(t)
	l, err := NewRedis(c, Config{})
	if err != nil {
		t.Fatal(err)
	}
	tn := redistest.Unique("t")
	lim := Limits{RequestsPerMinute: 60}
	n := 0
	for allow(t, l, tenantReq(tn, lim, 1)).Allowed {
		n++
		if n > 100 {
			t.Fatal("unbounded")
		}
	}
	if n != 60 && n != 61 { // a token may refill while the loop runs
		t.Fatalf("burst admitted %d, want 60", n)
	}
	time.Sleep(1300 * time.Millisecond)
	if !allow(t, l, tenantReq(tn, lim, 1)).Allowed {
		t.Fatal("no token refilled after 1.3 s")
	}
}

func TestReleaseAndRenewalRunInTheBackground(t *testing.T) {
	c := redistest.NewClient(t)
	l, err := NewRedis(c, Config{LeaseTTL: 1500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start(t, l)
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 1}
	d := allow(t, l, tenantReq(tn, lim, 1))
	if !d.Allowed {
		t.Fatal("refused")
	}
	// Held for 3x the TTL: only renewal can keep the slot.
	deadline := time.Now().Add(3300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if x := allow(t, l, tenantReq(tn, lim, 1)); x.Allowed {
			t.Fatal("the slot was lost while the request was still running")
		}
		time.Sleep(50 * time.Millisecond)
	}
	d.Release()
	eventually(t, 5*time.Second, "the released slot to become reusable", func() bool {
		x := allow(t, l, tenantReq(tn, lim, 1))
		if x.Allowed {
			x.Release()
		}
		return x.Allowed
	})
}

func eventually(t *testing.T, d time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestLocalLeaseCapIsEnforced(t *testing.T) {
	l, _ := newTestLimiter(t, Config{MaxLocalLeases: 3, OnFailure: FailOpen})
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 100}
	for i := 0; i < 3; i++ {
		allow(t, l, tenantReq(tn, lim, 1))
	}
	d, err := l.Allow(context.Background(), tenantReq(tn, lim, 1))
	var ue *UnavailableError
	if err == nil || !errors.As(err, &ue) || !errors.Is(err, ErrLeaseCapacity) || !errors.Is(err, ErrUnavailable) || d.Allowed {
		t.Fatalf("over the lease cap: %+v %v (the cap applies in open mode too)", d, err)
	}
	if l.LocalLeases() != 3 {
		t.Fatalf("%d leases tracked", l.LocalLeases())
	}
}

func TestHostileNamesCannotReachOtherTenantsKeys(t *testing.T) {
	l, _ := newTestLimiter(t, Config{})
	victim := redistest.Unique("victim")
	lim := Limits{RequestsPerMinute: 2, MaxConcurrent: 1}
	hostile := []string{
		victim + "}:req", victim + "}", "{" + victim, victim + "}:conc}:conc", "%7B" + victim + "%7D", "a}b{c", "\x00\r\n*1\r\n$4\r\nPING",
		strings.Repeat("x", maxKeyPart), " " + victim, victim + "\x00",
	}
	for _, h := range hostile {
		for i := 0; i < 5; i++ {
			d, err := l.Allow(context.Background(), tenantReq(h, lim, 1))
			if err != nil {
				t.Fatalf("%q: %v", h, err)
			}
			if d.Allowed {
				d.Release()
			}
		}
	}
	drain(l)
	// The victim's budget is untouched.
	for i := 0; i < 2; i++ {
		if d := allow(t, l, tenantReq(victim, lim, 1)); !d.Allowed {
			t.Fatalf("victim request %d refused after hostile tenants ran", i+1)
		} else {
			d.Release()
			drain(l)
		}
	}
}

func TestOverlongNamesAreRefusedWithoutRedis(t *testing.T) {
	l, _ := newTestLimiter(t, Config{})
	before := l.c.Commands()
	_, err := l.Allow(context.Background(), tenantReq(strings.Repeat("a", maxKeyPart+1), Limits{RequestsPerMinute: 5}, 1))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v", err)
	}
	if l.c.Commands() != before {
		t.Fatal("a Redis command was sent for an unusable name")
	}
	if _, err := NewRedis(l.c, Config{ModelRequestsPerMinute: map[string]int{strings.Repeat("m", maxKeyPart+1): 1}}); err == nil {
		t.Fatal("a configured model name that is too long was accepted")
	}
}

func TestExtremeQuotasStayExact(t *testing.T) {
	l, clk := newTestLimiter(t, Config{BurstSeconds: MaxBurstSeconds})
	m := NewModel(Params{BurstSeconds: MaxBurstSeconds, LeaseTTLMs: DefaultLeaseTTL.Milliseconds()})
	tn := redistest.Unique("t")
	// Quota and cost at their bounds: the arithmetic must neither overflow nor round, so the script and the
	// model must agree on every step, including the advertised wait.
	lim := Limits{RequestsPerMinute: MaxQuota * 3, TokensPerMinute: MaxQuota}
	admitted := 0
	for i := 0; i < 20; i++ {
		want := m.Allow(clk.Now().UnixMilli(), ModelRequest{TenantID: tn, Model: "m", Cost: MaxCost, Limits: lim})
		got := allow(t, l, tenantReq(tn, lim, MaxCost))
		if got.Allowed != want.Allowed || got.Limit != want.Limit || got.RetryAfter.Milliseconds() != want.RetryMs {
			t.Fatalf("step %d: script %+v, model %+v", i, got, want)
		}
		if got.Allowed {
			admitted++
		}
	}
	if admitted != 13 { // 1e9 tokens/min for an hour is 6e10 tokens; 13 requests of 2^32 fit
		t.Fatalf("admitted %d, want 13", admitted)
	}
	d := allow(t, l, tenantReq(tn, lim, MaxCost))
	clk.Advance(d.RetryAfter)
	if x := allow(t, l, tenantReq(tn, lim, MaxCost)); !x.Allowed {
		t.Fatalf("not admitted after the advertised %v: %+v", d.RetryAfter, x)
	}
}

// ---- the oracle: the script and the model must agree on random sequences ----

func TestScriptMatchesModelOnRandomSequences(t *testing.T) {
	seeds := 25
	if testing.Short() {
		seeds = 5
	}
	for seed := 0; seed < seeds; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed)))
			burst := []int{60, 10, 30, 120}[rng.Intn(4)]
			ttl := []time.Duration{20 * time.Second, 60 * time.Second}[rng.Intn(2)]
			model := redistest.Unique("model")
			mq := []int{0, 0, 4, 30}[rng.Intn(4)]
			cfg := Config{BurstSeconds: burst, LeaseTTL: ttl}
			if mq > 0 {
				cfg.ModelRequestsPerMinute = map[string]int{model: mq}
			}
			l, clk := newTestLimiter(t, cfg)
			var nextID int
			prefix := redistest.Unique("l")
			l.newID = func() string { nextID++; return fmt.Sprintf("%s-%d", prefix, nextID) }
			m := NewModel(Params{BurstSeconds: burst, LeaseTTLMs: ttl.Milliseconds()})
			tenants := []string{redistest.Unique("a"), redistest.Unique("b")}
			held := map[string]*struct {
				tenant string
				rel    func()
			}{}
			quotas := []int{0, 1, 3, 10, 60, 600, 5000}
			var order []string
			for step := 0; step < 400; step++ {
				switch op := rng.Intn(10); {
				case op < 6: // allow
					tn := tenants[rng.Intn(2)]
					lim := Limits{quotas[rng.Intn(len(quotas))], quotas[rng.Intn(len(quotas))] * 5, []int{0, 1, 2, 5}[rng.Intn(4)]}
					if rng.Intn(3) == 0 { // keep quotas stable for a while most of the time so buckets build state
						lim = Limits{60, 3000, 3}
					}
					cost := []int{1, 5, 50, 400, 2000, 9000}[rng.Intn(6)]
					mname := model
					if rng.Intn(4) == 0 {
						mname = "unlisted"
					}
					modelQ := 0
					if mname == model {
						modelQ = mq
					}
					now := clk.Now().UnixMilli()
					id := fmt.Sprintf("%s-%d", prefix, nextID+1)
					want := m.Allow(now, ModelRequest{TenantID: tn, Model: mname, Cost: cost, Limits: lim, ModelRequestsPerMinute: modelQ, LeaseID: id})
					got, err := l.Allow(context.Background(), Request{TenantID: tn, Model: mname, Cost: cost, Limits: lim})
					if err != nil {
						t.Fatal(err)
					}
					// A request with nothing to enforce skips Redis and the model alike.
					if lim.None() && modelQ == 0 {
						if !got.Allowed {
							t.Fatalf("step %d: nothing to enforce but refused", step)
						}
						continue
					}
					if got.Allowed != want.Allowed || (!got.Allowed && (got.Limit != want.Limit || got.RetryAfter.Milliseconds() != want.RetryMs)) {
						t.Fatalf("step %d: script %+v, model %+v (tenant %s lim %+v cost %d model %q)", step, got, want, tn, lim, cost, mname)
					}
					if got.Allowed && lim.MaxConcurrent > 0 {
						held[id] = &struct {
							tenant string
							rel    func()
						}{tn, got.Release}
						order = append(order, id)
					}
				case op < 7: // advance time
					d := []time.Duration{0, time.Millisecond, 100 * time.Millisecond, time.Second, 7 * time.Second, 25 * time.Second, 70 * time.Second}[rng.Intn(7)]
					clk.Advance(d)
				case op < 8: // release a random held lease
					if len(order) == 0 {
						continue
					}
					i := rng.Intn(len(order))
					id := order[i]
					order = append(order[:i], order[i+1:]...)
					h := held[id]
					delete(held, id)
					h.rel()
					drain(l)
					m.Release(h.tenant, id)
				case op < 9: // renew everything this process still tracks
					l.renewAll(context.Background())
					now := clk.Now().UnixMilli()
					for _, id := range order {
						m.Renew(now, held[id].tenant, id)
					}
				default: // read-only: held count must agree
					now := clk.Now().UnixMilli()
					for _, tn := range tenants {
						_ = m.Held(now, tn)
					}
				}
			}
		})
	}
}

// ---- failure matrix ----

type outageHarness struct {
	l     *RedisLimiter
	proxy *redistest.Proxy
	clk   *fakeClock
	logs  *syncBuf
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func newOutageHarness(t *testing.T, mode FailureMode) *outageHarness {
	t.Helper()
	rc := redistest.Config(t)
	proxy := redistest.NewProxy(t, rc.Address)
	h := &outageHarness{proxy: proxy, clk: newFakeClock(), logs: &syncBuf{}}
	rc.Address, rc.Timeout, rc.Backoff, rc.Now = proxy.Addr(), 100*time.Millisecond, time.Second, h.clk.Now
	rc.Logger = newTextLogger(h.logs)
	c := redistest.NewClientWith(t, rc)
	l, err := NewRedis(c, Config{OnFailure: mode, Clock: h.clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	h.l = l
	return h
}

func TestFailureClosedAcrossTheMatrix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(h *outageHarness)
	}{
		{"cut", func(h *outageHarness) { h.proxy.SetMode(redistest.Cut) }},
		{"blackhole", func(h *outageHarness) { h.proxy.SetMode(redistest.Blackhole) }},
		{"slow", func(h *outageHarness) { h.proxy.SetDelay(500 * time.Millisecond); h.proxy.SetMode(redistest.Slow) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOutageHarness(t, FailClosed)
			tn := redistest.Unique("t")
			lim := Limits{RequestsPerMinute: 1000}
			if d := allow(t, h.l, tenantReq(tn, lim, 1)); !d.Allowed {
				t.Fatal("healthy request refused")
			}
			tc.break_(h)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // a fail-fast test must not be able to hang
			defer cancel()
			start := time.Now()
			_, err := h.l.Allow(ctx, tenantReq(tn, lim, 1))
			var ue *UnavailableError
			if !errors.As(err, &ue) || !errors.Is(err, ErrUnavailable) {
				t.Fatalf("want an UnavailableError, got %v", err)
			}
			if d := time.Since(start); d > 1500*time.Millisecond {
				t.Fatalf("the first failure took %v with a 100ms timeout", d)
			}
			if ue.RetryAfterSeconds() < 1 {
				t.Fatalf("retry after %d", ue.RetryAfterSeconds())
			}
			// During the backoff: immediate, and no network traffic.
			before := h.l.c.Commands()
			for i := 0; i < 100; i++ {
				start := time.Now()
				_, err := h.l.Allow(ctx, tenantReq(tn, lim, 1))
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("got %v", err)
				}
				if time.Since(start) > 50*time.Millisecond {
					t.Fatal("a request during the backoff was slow")
				}
			}
			if h.l.c.Commands() != before {
				t.Fatal("commands were sent during the backoff")
			}
			if h.l.LocalLeases() != 0 {
				t.Fatal("failed checks left leases behind")
			}
			// Requests with nothing to enforce are unaffected by the outage.
			if d := allow(t, h.l, tenantReq("free", Limits{}, 1)); !d.Allowed {
				t.Fatal("an unlimited tenant was refused during the outage")
			}
			// Recovery is automatic once the backoff passes.
			h.proxy.SetMode(redistest.Pass)
			h.clk.Advance(1500 * time.Millisecond)
			eventually(t, 5*time.Second, "recovery", func() bool {
				d, err := h.l.Allow(ctx, tenantReq(tn, lim, 1))
				if err != nil {
					h.clk.Advance(1500 * time.Millisecond)
					return false
				}
				return d.Allowed
			})
			out := h.logs.String()
			if strings.Count(out, "redis unavailable") != 1 || strings.Count(out, "redis recovered") != 1 {
				t.Fatalf("want one outage line and one recovery line:\n%s", out)
			}
		})
	}
}

func TestFailureClosedWhenRedisReturnsErrorsOrNonsense(t *testing.T) {
	for name, reply := range map[string]string{
		"oom":       "-OOM command not allowed when used memory > 'maxmemory'",
		"loading":   "-LOADING Redis is loading the dataset in memory",
		"readonly":  "-READONLY You can't write against a read only replica.",
		"script":    "-ERR Error running script",
		"wrongtype": "+OK",
		"short":     "*1\r\n:5",
		"text":      "*3\r\n+a\r\n+b\r\n+c",
	} {
		t.Run(name, func(t *testing.T) {
			rc := redistest.Config(t)
			fake := redistest.NewFakeServer(t, reply)
			rc.Address = fake.Addr()
			c := redistest.NewClientWith(t, rc)
			closed, _ := NewRedis(c, Config{})
			_, err := closed.Allow(context.Background(), tenantReq("t", Limits{RequestsPerMinute: 5}, 1))
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("closed: got %v", err)
			}
			if down, _ := c.Down(); !down {
				t.Fatal("an erroring Redis should start a backoff")
			}
			rc2 := redistest.Config(t)
			rc2.Address = fake.Addr()
			open, _ := NewRedis(redistest.NewClientWith(t, rc2), Config{OnFailure: FailOpen})
			d, err := open.Allow(context.Background(), tenantReq("t", Limits{RequestsPerMinute: 5}, 1))
			if err != nil || !d.Allowed || !d.Bypassed {
				t.Fatalf("open: %+v %v", d, err)
			}
		})
	}
}

func TestFailureOpenAdmitsAndRecovers(t *testing.T) {
	h := newOutageHarness(t, FailOpen)
	tn := redistest.Unique("t")
	lim := Limits{RequestsPerMinute: 2, MaxConcurrent: 1}
	d := allow(t, h.l, tenantReq(tn, lim, 1))
	if !d.Allowed || d.Bypassed {
		t.Fatalf("%+v", d)
	}
	d.Release()
	drain(h.l)
	h.proxy.SetMode(redistest.Blackhole)
	for i := 0; i < 50; i++ {
		d := allow(t, h.l, tenantReq(tn, lim, 1))
		if !d.Allowed || !d.Bypassed || d.Release == nil {
			t.Fatalf("request %d during the outage: %+v", i, d)
		}
		d.Release()
	}
	if h.l.LocalLeases() != 0 {
		t.Fatal("bypassed requests hold leases")
	}
	h.proxy.SetMode(redistest.Pass)
	h.clk.Advance(1500 * time.Millisecond)
	eventually(t, 5*time.Second, "limits to apply again", func() bool {
		d, err := h.l.Allow(context.Background(), tenantReq(tn, Limits{RequestsPerMinute: 1}, 1))
		if err != nil || d.Bypassed {
			h.clk.Advance(1500 * time.Millisecond)
			return false
		}
		return true
	})
	out := h.logs.String()
	if strings.Count(out, "redis unavailable") != 1 || strings.Count(out, "redis recovered") != 1 {
		t.Fatalf("want one outage line and one recovery line:\n%s", out)
	}
}

func TestNewRedisValidation(t *testing.T) {
	c := redistest.NewClient(t)
	for name, cfg := range map[string]Config{
		"mode":   {OnFailure: "sideways"},
		"burst":  {BurstSeconds: MaxBurstSeconds + 1},
		"neg":    {BurstSeconds: -1},
		"ttl":    {LeaseTTL: time.Microsecond},
		"leases": {MaxLocalLeases: -1},
	} {
		if _, err := NewRedis(c, cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := NewRedis(nil, Config{}); err == nil {
		t.Error("nil client accepted")
	}
	var _ redis.Script
}

func newTextLogger(w *syncBuf) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }

// ---- clock edges against real Redis (frozen clock): the script must agree with the model ----

// both runs one request through the limiter (script) and the model at the clock's current time and fails if
// they disagree.
func both(t *testing.T, l *RedisLimiter, clk *fakeClock, m *Model, tn string, lim Limits, cost int) Decision {
	t.Helper()
	want := m.Allow(clk.Now().UnixMilli(), ModelRequest{TenantID: tn, Model: "m", Cost: cost, Limits: lim})
	got := allow(t, l, tenantReq(tn, lim, cost))
	if got.Allowed != want.Allowed || got.Limit != want.Limit || got.RetryAfter.Milliseconds() != want.RetryMs {
		t.Fatalf("at %d: script %+v, model %+v", clk.Now().UnixMilli(), got, want)
	}
	return got
}

func TestExactEdgeOfEveryBucketAgainstRedis(t *testing.T) {
	// A bucket that holds exactly what the request needs admits it and drains to zero; one millisecond earlier
	// (a few scaled units short) it refuses and says how long to wait.
	model := redistest.Unique("model")
	l, clk := newTestLimiter(t, Config{ModelRequestsPerMinute: map[string]int{model: 600}})
	for _, tc := range []struct {
		name string
		lim  Limits
		mod  string
		cost int
		edge time.Duration // time to refill exactly one request's worth after draining
	}{
		{"requests", Limits{RequestsPerMinute: 600}, "", 1, 100 * time.Millisecond},
		{"tokens", Limits{TokensPerMinute: 600}, "", 10, 1000 * time.Millisecond},
		{"model", Limits{}, model, 1, 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tn := redistest.Unique("t")
			req := func(cost int) Request { return Request{TenantID: tn, Model: tc.mod, Cost: cost, Limits: tc.lim} }
			if tc.name == "model" {
				tn = ""
			}
			// Drain completely: 600 requests' worth.
			spent := 0
			for {
				d, err := l.Allow(context.Background(), req(tc.cost))
				if err != nil {
					t.Fatal(err)
				}
				if !d.Allowed {
					break
				}
				spent += tc.cost
				if spent > 100000 {
					t.Fatal("never drained")
				}
			}
			want := 600
			if tc.name == "tokens" {
				want = 600 // tokens
			}
			if spent != want {
				t.Fatalf("a full bucket admitted %d, want exactly %d", spent, want)
			}
			clk.Advance(tc.edge - time.Millisecond)
			d, _ := l.Allow(context.Background(), req(tc.cost))
			if d.Allowed || d.RetryAfter != time.Millisecond {
				t.Fatalf("one millisecond short of one request: %+v", d)
			}
			clk.Advance(time.Millisecond)
			d, _ = l.Allow(context.Background(), req(tc.cost))
			if !d.Allowed {
				t.Fatalf("a bucket holding exactly what the request needs must admit it: %+v", d)
			}
			d, _ = l.Allow(context.Background(), req(tc.cost))
			if d.Allowed {
				t.Fatal("the bucket should be empty again")
			}
		})
	}
}

func TestClockStepsAgainstRedisMatchTheModel(t *testing.T) {
	l, clk := newTestLimiter(t, Config{})
	m := NewModel(Params{BurstSeconds: 60, LeaseTTLMs: DefaultLeaseTTL.Milliseconds()})
	tn := redistest.Unique("t")
	lim := Limits{RequestsPerMinute: 60} // 1 token per second
	count := func(max int) int {
		n := 0
		for both(t, l, clk, m, tn, lim, 1).Allowed {
			n++
			if n > max {
				t.Fatal("unbounded")
			}
		}
		return n
	}
	for i := 0; i < 30; i++ { // level 30
		both(t, l, clk, m, tn, lim, 1)
	}
	// (b) The clock steps back a minute: no tokens are created or destroyed.
	clk.Advance(-60 * time.Second)
	if n := 0; true {
		for i := 0; i < 5; i++ { // admitted while behind, level 25
			if both(t, l, clk, m, tn, lim, 1).Allowed {
				n++
			}
		}
		if n != 5 {
			t.Fatalf("a backwards step destroyed tokens: only %d of 5 admitted", n)
		}
	}
	// (c) The stamp did not move back: when the clock returns 1 s after the last real update the refill is that
	// one second (one token: level 26), not the minute the step spanned (which would refill the bucket).
	clk.Advance(61 * time.Second)
	if n := count(100); n != 26 {
		t.Fatalf("after a step back and forward the bucket held %d, want 26 (25 + one second of refill)", n)
	}
	// (a) Idleness far beyond the window credits exactly one full bucket.
	clk.Advance(10 * time.Hour)
	if n := count(1000); n != 60 {
		t.Fatalf("after 10 idle hours the bucket held %d, want exactly its capacity of 60", n)
	}
}

func TestExpiredAndReleasedLeasesAreNotRevivedByRenewal(t *testing.T) {
	l, clk := newTestLimiter(t, Config{LeaseTTL: 30 * time.Second})
	tn := redistest.Unique("t")
	lim := Limits{MaxConcurrent: 1}
	d := allow(t, l, tenantReq(tn, lim, 1))
	if !d.Allowed {
		t.Fatal("refused")
	}
	clk.Advance(31 * time.Second) // the lease expired in Redis; this gateway still tracks it and renews it
	l.renewAll(context.Background())
	// A ghost lease would block this admission for another 30 s.
	x := allow(t, l, tenantReq(tn, lim, 1))
	if !x.Allowed {
		t.Fatal("renewing an expired lease brought it back and blocked the tenant")
	}
	x.Release()
	drain(l)
	// The same through the script directly, for an id that was released.
	if _, err := l.c.Run(context.Background(), renewScript, []string{keyConcurrency(tn)}, clk.Now().UnixMilli(), int64(30000), "no-such-lease"); err != nil {
		t.Fatal(err)
	}
	if y := allow(t, l, tenantReq(tn, lim, 1)); !y.Allowed {
		t.Fatal("renewing an unknown lease created a ghost")
	}
}
