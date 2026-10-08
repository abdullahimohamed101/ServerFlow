package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is an in-memory KeyStore that counts lookups and can be told to fail or block.
type fakeStore struct {
	mu      sync.Mutex
	recs    map[string]KeyRecord
	lookups atomic.Int64
	fail    atomic.Bool
	gate    chan struct{} // when non-nil, LookupKey waits for it to close
	touched []string
}

func newFakeStore() *fakeStore { return &fakeStore{recs: map[string]KeyRecord{}} }

func (f *fakeStore) LookupKey(ctx context.Context, prefix string) (KeyRecord, error) {
	f.lookups.Add(1)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return KeyRecord{}, ctx.Err()
		}
	}
	if f.fail.Load() {
		return KeyRecord{}, errors.New("connection refused")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.recs[prefix]
	if !ok {
		return KeyRecord{}, ErrNotFound
	}
	return r, nil
}

func (f *fakeStore) TouchKey(_ context.Context, keyID string, _ time.Time) error {
	f.mu.Lock()
	f.touched = append(f.touched, keyID)
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) update(prefix string, fn func(*KeyRecord)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.recs[prefix]
	fn(&r)
	f.recs[prefix] = r
}

// add stores a new active key for a new tenant and returns its plaintext.
func (f *fakeStore) add(n int) (plaintext, prefix string) {
	plaintext, prefix, hash := GenerateKey()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs[prefix] = KeyRecord{
		KeyID: fmt.Sprintf("key_%d", n), TenantID: fmt.Sprintf("ten_%d", n), Prefix: prefix, SecretHash: hash, KeyStatus: KeyActive,
		Policy: TenantPolicy{Status: TenantActive, AllowedModels: []string{"m1"}, RequestsPerMinute: 7, Priority: 2},
	}
	return plaintext, prefix
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// syncBuffer is a bytes.Buffer safe for the logger and the test to share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func setup(t *testing.T, cfg Config) (*Authenticator, *fakeStore, *fakeClock, *syncBuffer) {
	t.Helper()
	st := newFakeStore()
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	logs := &syncBuffer{}
	cfg.Now = clk.now
	cfg.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	if cfg.NegativeTTL == 0 {
		cfg.NegativeTTL = 5 * time.Second
	}
	if cfg.StaleGrace == 0 {
		cfg.StaleGrace = 5 * time.Minute
	}
	return New(st, cfg), st, clk, logs
}

func TestAuthenticateSuccess(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	key, _ := st.add(1)
	p, err := a.Authenticate(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if p.TenantID != "ten_1" || p.KeyID != "key_1" || p.Policy.Status != TenantActive || !p.Policy.AllowsModel("m1") || p.Policy.AllowsModel("m2") ||
		p.Policy.RequestsPerMinute != 7 || p.Policy.Priority != 2 {
		t.Fatalf("principal: %+v", p)
	}
}

func TestEveryFailureKind(t *testing.T) {
	a, st, clk, _ := setup(t, Config{})
	good, gp := st.add(1)
	revoked, rp := st.add(2)
	st.update(rp, func(r *KeyRecord) { r.KeyStatus = KeyRevoked })
	expired, ep := st.add(3)
	past := clk.now().Add(-time.Second)
	st.update(ep, func(r *KeyRecord) { r.ExpiresAt = &past })
	suspended, sp := st.add(4)
	st.update(sp, func(r *KeyRecord) { r.Policy.Status = TenantSuspended })
	weird, wp := st.add(5)
	st.update(wp, func(r *KeyRecord) { r.KeyStatus = "something-new" })

	// A key with a right prefix and a wrong secret, for each state.
	other, _, _ := GenerateKey()
	withPrefix := func(prefix string) string { return "sf_" + prefix + "_" + other[len("sf_")+9:] }

	cases := []struct {
		name string
		key  string
		want error
	}{
		{"valid", good, nil},
		{"empty", "", ErrInvalid},
		{"garbage", "hello", ErrInvalid},
		{"unknown prefix", other, ErrInvalid},
		{"wrong secret", withPrefix(gp), ErrInvalid},
		{"revoked", revoked, ErrRevoked},
		{"expired", expired, ErrExpired},
		{"suspended", suspended, ErrSuspended},
		{"unknown key status fails closed", weird, ErrRevoked},
		// Without the secret, no state may be learned from the answer.
		{"revoked + wrong secret", withPrefix(rp), ErrInvalid},
		{"expired + wrong secret", withPrefix(ep), ErrInvalid},
		{"suspended + wrong secret", withPrefix(sp), ErrInvalid},
	}
	for _, c := range cases {
		_, err := a.Authenticate(context.Background(), c.key)
		if c.want == nil {
			if err != nil {
				t.Errorf("%s: %v", c.name, err)
			}
			continue
		}
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestExpiryBoundaryAndCrossingWithoutRelookup(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: time.Hour})
	key, p := st.add(1)
	exp := clk.now().Add(10 * time.Second)
	st.update(p, func(r *KeyRecord) { r.ExpiresAt = &exp })
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	clk.advance(10*time.Second - time.Nanosecond)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatalf("one tick before expiry must pass: %v", err)
	}
	clk.advance(time.Nanosecond) // now == expires_at: expired
	if _, err := a.Authenticate(context.Background(), key); !errors.Is(err, ErrExpired) {
		t.Fatalf("at the expiry instant the key must be expired: %v", err)
	}
	if n := st.lookups.Load(); n != 1 {
		t.Fatalf("expiry must be judged from the cached record without a lookup, got %d lookups", n)
	}
}

func TestWarmCacheDoesNotQuery(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	key, _ := st.add(1)
	for i := 0; i < 1000; i++ {
		if _, err := a.Authenticate(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	if n := st.lookups.Load(); n != 1 {
		t.Fatalf("%d lookups for 1000 warm requests, want 1", n)
	}
}

func TestRevocationTakesEffectWithinCacheTTL(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: 30 * time.Second})
	key, p := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	st.update(p, func(r *KeyRecord) { r.KeyStatus = KeyRevoked })
	clk.advance(29 * time.Second)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatalf("revocation is allowed to lag by up to cache_ttl: %v", err)
	}
	clk.advance(time.Second) // age == ttl: stale
	if _, err := a.Authenticate(context.Background(), key); !errors.Is(err, ErrRevoked) {
		t.Fatalf("at cache_ttl the revocation must be seen: %v", err)
	}
	if n := st.lookups.Load(); n != 2 {
		t.Fatalf("lookups: %d", n)
	}
	// Suspension is seen the same way.
	key2, p2 := st.add(2)
	if _, err := a.Authenticate(context.Background(), key2); err != nil {
		t.Fatal(err)
	}
	st.update(p2, func(r *KeyRecord) { r.Policy.Status = TenantSuspended })
	clk.advance(30 * time.Second)
	if _, err := a.Authenticate(context.Background(), key2); !errors.Is(err, ErrSuspended) {
		t.Fatalf("suspension: %v", err)
	}
}

func TestNegativeCache(t *testing.T) {
	a, st, clk, _ := setup(t, Config{NegativeTTL: 5 * time.Second})
	unknown, _, _ := GenerateKey()
	for i := 0; i < 500; i++ {
		if _, err := a.Authenticate(context.Background(), unknown); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if n := st.lookups.Load(); n != 1 {
		t.Fatalf("%d lookups for one repeated unknown key", n)
	}
	clk.advance(5 * time.Second)
	_, _ = a.Authenticate(context.Background(), unknown)
	if n := st.lookups.Load(); n != 2 {
		t.Fatalf("negative entry must expire after negative_ttl: %d lookups", n)
	}
}

func TestNewKeyWorksAfterNegativeTTL(t *testing.T) {
	a, st, clk, _ := setup(t, Config{NegativeTTL: 5 * time.Second})
	key, prefix, hash := GenerateKey()
	if _, err := a.Authenticate(context.Background(), key); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.recs[prefix] = KeyRecord{KeyID: "k", TenantID: "t", Prefix: prefix, SecretHash: hash, KeyStatus: KeyActive, Policy: TenantPolicy{Status: TenantActive}}
	st.mu.Unlock()
	clk.advance(5 * time.Second)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatalf("a key created after being probed must work once the negative entry expires: %v", err)
	}
}

func TestCachesAreBoundedAndFloodCannotEvictRealKeys(t *testing.T) {
	const size = 50
	a, st, _, _ := setup(t, Config{CacheSize: size})
	real, _ := st.add(1)
	if _, err := a.Authenticate(context.Background(), real); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < size*20; i++ {
		k, _, _ := GenerateKey()
		_, _ = a.Authenticate(context.Background(), k)
	}
	pos, neg := a.CacheSizes()
	if pos != 1 || neg != size {
		t.Fatalf("after a flood: positive=%d (want 1) negative=%d (want exactly the bound %d)", pos, neg, size)
	}
	mid := st.lookups.Load()
	if _, err := a.Authenticate(context.Background(), real); err != nil {
		t.Fatal(err)
	}
	if st.lookups.Load() != mid {
		t.Fatal("the real key was evicted by the flood")
	}

	// The positive cache is bounded too.
	b, st2, _, _ := setup(t, Config{CacheSize: 5})
	for i := 0; i < 30; i++ {
		k, _ := st2.add(i)
		if _, err := b.Authenticate(context.Background(), k); err != nil {
			t.Fatal(err)
		}
	}
	if pos, _ := b.CacheSizes(); pos != 5 {
		t.Fatalf("positive cache holds %d entries, bound is 5", pos)
	}
}

func TestMalformedKeysNeverTouchStoreOrCache(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	for i := 0; i < 100; i++ {
		_, _ = a.Authenticate(context.Background(), fmt.Sprintf("sf_%08x_short", i))
		_, _ = a.Authenticate(context.Background(), strings.Repeat("x", i*1000))
	}
	if st.lookups.Load() != 0 {
		t.Fatal("malformed keys must be refused before any lookup")
	}
	if pos, neg := a.CacheSizes(); pos+neg != 0 {
		t.Fatal("malformed keys must not be cached")
	}
}

func TestSingleflight(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	st.gate = make(chan struct{})
	key, _ := st.add(1)
	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := a.Authenticate(context.Background(), key)
			errs <- err
		}()
	}
	// Let everyone pile up behind the single lookup.
	deadline := time.Now().Add(5 * time.Second)
	for st.lookups.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(st.gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := st.lookups.Load(); got != 1 {
		t.Fatalf("%d concurrent first requests caused %d lookups, want 1", n, got)
	}
}

func TestLeaderCancelDoesNotFailOthers(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	st.gate = make(chan struct{})
	key, _ := st.add(1)
	ctx, cancel := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() { _, err := a.Authenticate(ctx, key); leader <- err }()
	for st.lookups.Load() < 1 {
		time.Sleep(time.Millisecond)
	}
	other := make(chan error, 1)
	go func() { _, err := a.Authenticate(context.Background(), key); other <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel() // the first client gives up while the lookup is in flight
	close(st.gate)
	if err := <-other; err != nil {
		t.Fatalf("another client's request failed because the first gave up: %v", err)
	}
	<-leader
}

func TestOutageStaleGraceAndRecovery(t *testing.T) {
	a, st, clk, logs := setup(t, Config{CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	cached, _ := st.add(1)
	uncached, _ := st.add(2)
	if _, err := a.Authenticate(context.Background(), cached); err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)

	// Within the TTL nothing is asked of the store.
	if _, err := a.Authenticate(context.Background(), cached); err != nil {
		t.Fatal(err)
	}
	// An uncached key is refused at once with ErrUnavailable (not "invalid": it may well be valid).
	if _, err := a.Authenticate(context.Background(), uncached); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("uncached key during an outage: %v", err)
	}
	// Past the TTL but inside the grace the cached key is still served.
	clk.advance(2 * time.Minute)
	before := st.lookups.Load()
	for i := 0; i < 20; i++ {
		if _, err := a.Authenticate(context.Background(), cached); err != nil {
			t.Fatalf("cached key must be served during the grace period: %v", err)
		}
	}
	// A failed lookup is not retried for every request.
	if n := st.lookups.Load() - before; n > 1 {
		t.Fatalf("a dead store was queried %d times for 20 requests within one backoff interval", n)
	}
	// Beyond TTL+grace the cached key is no longer trusted.
	clk.advance(4 * time.Minute)
	if _, err := a.Authenticate(context.Background(), cached); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("past the grace period: %v", err)
	}
	if got := strings.Count(logs.String(), "key store unavailable"); got != 1 {
		t.Fatalf("outage logged %d times, want once:\n%s", got, logs.String())
	}

	// Recovery.
	st.fail.Store(false)
	clk.advance(2 * time.Second) // past the backoff
	if _, err := a.Authenticate(context.Background(), uncached); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	if !strings.Contains(logs.String(), "recovered") {
		t.Fatal("recovery not logged")
	}
	// Logs must never carry key material.
	if strings.Contains(logs.String(), "sf_") {
		t.Fatalf("logs contain key material:\n%s", logs.String())
	}
}

func TestStaleServedKeyIsStillCheckedForExpiry(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	key, p := st.add(1)
	exp := clk.now().Add(time.Minute)
	st.update(p, func(r *KeyRecord) { r.ExpiresAt = &exp })
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)
	clk.advance(2 * time.Minute)
	if _, err := a.Authenticate(context.Background(), key); !errors.Is(err, ErrExpired) {
		t.Fatalf("expiry must hold during an outage: %v", err)
	}
}

func TestLookupHangDoesNotHangRequests(t *testing.T) {
	a, st, _, _ := setup(t, Config{LookupTimeout: 50 * time.Millisecond})
	st.gate = make(chan struct{}) // never closed
	key, _ := st.add(1)
	start := time.Now()
	_, err := a.Authenticate(context.Background(), key)
	if !errors.Is(err, ErrUnavailable) || time.Since(start) > 2*time.Second {
		t.Fatalf("hung store: %v after %v", err, time.Since(start))
	}
}

func TestLastUsedIsRecordedAtMostOncePerMinute(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: time.Hour})
	key, _ := st.add(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	for i := 0; i < 100; i++ {
		_, _ = a.Authenticate(context.Background(), key)
		clk.advance(100 * time.Millisecond)
	}
	clk.advance(time.Minute)
	_, _ = a.Authenticate(context.Background(), key)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st.mu.Lock()
		n := len(st.touched)
		st.mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.touched) != 2 {
		t.Fatalf("last_used_at written %d times for 101 requests over ~71s, want 2", len(st.touched))
	}
}

// A statistical probe, deliberately generous: an unknown prefix and a wrong secret on a real key
// must not be distinguishable by response time. Both are answered from memory, so any
// meaningful difference would mean the code took a different path.
func TestTimingUnknownPrefixVersusWrongSecret(t *testing.T) {
	if testing.Short() {
		t.Skip("timing probe")
	}
	a, st, _, _ := setup(t, Config{CacheTTL: time.Hour, NegativeTTL: time.Hour})
	_, gp := st.add(1)
	other, _, _ := GenerateKey()
	wrongSecret := "sf_" + gp + "_" + other[len("sf_")+9:]
	unknown, _, _ := GenerateKey()
	ctx := context.Background()
	_, _ = a.Authenticate(ctx, wrongSecret)
	_, _ = a.Authenticate(ctx, unknown)

	measure := func(k string) time.Duration {
		const n = 4000
		start := time.Now()
		for i := 0; i < n; i++ {
			_, _ = a.Authenticate(ctx, k)
		}
		return time.Since(start) / n
	}
	var us, ws []time.Duration
	for round := 0; round < 9; round++ {
		us = append(us, measure(unknown))
		ws = append(ws, measure(wrongSecret))
	}
	med := func(d []time.Duration) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		return d[len(d)/2]
	}
	u, w := med(us), med(ws)
	lo, hi := min(u, w), max(u, w)
	t.Logf("median per call: unknown prefix %v, wrong secret %v", u, w)
	// Both paths take a few microseconds; allow a 5x ratio or 20us absolute slack for scheduler noise.
	if hi > 5*lo && hi-lo > 20*time.Microsecond {
		t.Fatalf("unknown prefix (%v) and wrong secret (%v) differ measurably", u, w)
	}
}
