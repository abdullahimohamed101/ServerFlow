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
	// errFor, when set, may return an error for a prefix before the normal lookup.
	errFor func(prefix string) error
	// blockUnknown, when non-nil, makes lookups of prefixes with no record wait for it to close.
	blockUnknown chan struct{}
	// holdFor, when set, may return a channel the lookup of that prefix waits on.
	holdFor func(prefix string) <-chan struct{}
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
	errFor, blockUnknown := f.errFor, f.blockUnknown
	_, known := f.recs[prefix]
	f.mu.Unlock()
	if errFor != nil {
		if err := errFor(prefix); err != nil {
			return KeyRecord{}, err
		}
	}
	if hf := f.holdFor; hf != nil {
		if ch := hf(prefix); ch != nil {
			select {
			case <-ch:
			case <-ctx.Done():
				return KeyRecord{}, ctx.Err()
			}
		}
	}
	if !known && blockUnknown != nil {
		select {
		case <-blockUnknown:
		case <-ctx.Done():
			return KeyRecord{}, ctx.Err()
		}
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

func (c *fakeClock) now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) set(t time.Time) { c.mu.Lock(); c.t = t; c.mu.Unlock() }
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

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSingleflight(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	st.gate = make(chan struct{})
	var waiting atomic.Int64
	a.onWait = func() { waiting.Add(1) }
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
	// Everyone but the leader must be parked behind the single lookup before it is released.
	waitUntil(t, "all callers to join the lookup", func() bool { return waiting.Load() == n-1 && st.lookups.Load() == 1 })
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

// A caller that missed the cache just before another caller's lookup finished must find that
// result, not start a second lookup.
func TestFetchRechecksTheCacheBeforeStartingALookup(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	key, prefix := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	for _, refresh := range []bool{false, true} {
		rec, err := a.fetch(context.Background(), prefix, refresh)
		if err != nil || rec.KeyID != "key_1" {
			t.Fatalf("refresh=%v: %v", refresh, err)
		}
	}
	unknown, _, _ := GenerateKey()
	up, _, _ := ParseKey(unknown)
	_, _ = a.Authenticate(context.Background(), unknown)
	if _, err := a.fetch(context.Background(), up, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("negative entry not re-checked: %v", err)
	}
	if n := st.lookups.Load(); n != 2 {
		t.Fatalf("%d lookups, want 2 (one per distinct key)", n)
	}
}

func TestLeaderCancelDoesNotFailOthers(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	st.gate = make(chan struct{})
	joined := make(chan struct{}, 1)
	a.onWait = func() { joined <- struct{}{} }
	key, _ := st.add(1)
	ctx, cancel := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() { _, err := a.Authenticate(ctx, key); leader <- err }()
	waitUntil(t, "the leader's lookup", func() bool { return st.lookups.Load() == 1 })
	other := make(chan error, 1)
	go func() { _, err := a.Authenticate(context.Background(), key); other <- err }()
	<-joined
	cancel() // the first client gives up while the lookup is in flight
	close(st.gate)
	if err := <-other; err != nil {
		t.Fatalf("another client's request failed because the first gave up: %v", err)
	}
	<-leader
}

// An unauthenticated flood of distinct random keys must neither starve the refresh of cached
// keys (so a revocation is still noticed within cache_ttl) nor reach the database beyond the cap.
func TestFloodCannotHideARevocationAndIsShedWithoutTouchingTheStore(t *testing.T) {
	a, st, clk, _ := setup(t, Config{MaxLookups: 8, CacheTTL: 30 * time.Second}) // 6 slots for new keys, 2 reserved
	cached, cp := st.add(1)
	if _, err := a.Authenticate(context.Background(), cached); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.blockUnknown = make(chan struct{})
	st.mu.Unlock()
	base := st.lookups.Load() // 1

	const flood = 40
	var wg sync.WaitGroup
	results := make(chan error, flood)
	for i := 0; i < flood; i++ {
		k, _, _ := GenerateKey()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := a.Authenticate(context.Background(), k)
			results <- err
		}()
	}
	// Six lookups occupy the new-key slots; the other 34 are shed at once.
	waitUntil(t, "the flood to fill the new-key slots and the rest to be shed", func() bool { return st.lookups.Load() == base+6 && len(results) == flood-6 })
	if n := st.lookups.Load(); n != base+6 {
		t.Fatalf("the store saw %d lookups from the flood, cap is 6", n-base)
	}
	for i := 0; i < flood-6; i++ {
		if err := <-results; !errors.Is(err, ErrUnavailable) {
			t.Fatalf("shed request: %v", err)
		}
	}

	// A valid key the cache has never seen is shed while the new-key slots are taken (documented).
	fresh, _ := st.add(2)
	if _, err := a.Authenticate(context.Background(), fresh); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("uncached key while the flood holds every new-key slot: %v", err)
	}
	if st.lookups.Load() != base+6 {
		t.Fatal("a shed request reached the store")
	}

	// The revocation of the cached key is still seen at cache_ttl, through the reserved slots.
	st.update(cp, func(r *KeyRecord) { r.KeyStatus = KeyRevoked })
	clk.advance(30 * time.Second)
	if _, err := a.Authenticate(context.Background(), cached); !errors.Is(err, ErrRevoked) {
		t.Fatalf("revocation hidden by the flood: %v", err)
	}

	// Shedding did not arm the global backoff: once the flood drains, the valid key works at once.
	close(st.blockUnknown)
	wg.Wait()
	if _, err := a.Authenticate(context.Background(), fresh); err != nil {
		t.Fatalf("after the flood drained: %v", err)
	}
}

// Refreshes of cached keys are capped too: the store is never asked for more than MaxLookups
// things at once, and the excess keeps being served from its stale copy.
func TestRefreshesAreCappedAtMaxLookups(t *testing.T) {
	a, st, clk, _ := setup(t, Config{MaxLookups: 4, CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	const n = 10
	keys := make([]string, n)
	for i := range keys {
		keys[i], _ = st.add(i)
		if _, err := a.Authenticate(context.Background(), keys[i]); err != nil {
			t.Fatal(err)
		}
	}
	base := st.lookups.Load()
	st.gate = make(chan struct{})
	clk.advance(time.Minute) // every entry is now past its TTL and inside the grace
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for _, k := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := a.Authenticate(context.Background(), k)
			errs <- err
		}()
	}
	// Six are shed and answered from their stale copies at once; four lookups are blocked.
	waitUntil(t, "the excess refreshes to be shed", func() bool { return len(errs) >= n-4 })
	if got := st.lookups.Load() - base; got != 4 {
		t.Fatalf("%d concurrent refreshes reached the store, cap is 4", got)
	}
	close(st.gate)
	wg.Wait()
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("a refresh failed: %v", err)
		}
	}
}

func TestBusyStoreIsNotAnOutage(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	cached, _ := st.add(1)
	other, _ := st.add(2)
	if _, err := a.Authenticate(context.Background(), cached); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.errFor = func(string) error { return fmt.Errorf("%w: no connection available", ErrBusy) }
	st.mu.Unlock()
	clk.advance(time.Minute)
	// A cached key is still served within the grace (documented), an uncached one is unavailable...
	if _, err := a.Authenticate(context.Background(), cached); err != nil {
		t.Fatalf("stale key under a busy store: %v", err)
	}
	before := st.lookups.Load()
	if _, err := a.Authenticate(context.Background(), other); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("uncached key under a busy store: %v", err)
	}
	// ...but a busy store arms no global backoff: the next request still asks the store.
	st.mu.Lock()
	st.errFor = nil
	st.mu.Unlock()
	if _, err := a.Authenticate(context.Background(), other); err != nil {
		t.Fatalf("a busy store must not start a backoff: %v", err)
	}
	if st.lookups.Load() != before+2 {
		t.Fatalf("lookups %d, want %d", st.lookups.Load(), before+2)
	}
}

func TestUnreadableRecordIsPerKeyNotAnOutage(t *testing.T) {
	a, st, clk, logs := setup(t, Config{CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	good, gp := st.add(1)
	other, _ := st.add(2)
	if _, err := a.Authenticate(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.errFor = func(prefix string) error {
		if prefix == gp {
			return fmt.Errorf("%w: scan failed", ErrBadRecord)
		}
		return nil
	}
	st.mu.Unlock()
	clk.advance(time.Minute) // past the TTL, inside the grace
	// The stale copy of the broken key is NOT served in place of the unreadable row.
	if _, err := a.Authenticate(context.Background(), good); !errors.Is(err, ErrBadRecord) {
		t.Fatalf("unreadable record: %v", err)
	}
	// Other keys are unaffected and no backoff was armed.
	before := st.lookups.Load()
	if _, err := a.Authenticate(context.Background(), other); err != nil {
		t.Fatalf("another key during one key's fault: %v", err)
	}
	if st.lookups.Load() != before+1 {
		t.Fatal("the other key was not looked up normally")
	}
	if strings.Contains(logs.String(), "key store unavailable") {
		t.Fatalf("a data fault was reported as an outage:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "could not read") {
		t.Fatalf("the fault was not logged:\n%s", logs.String())
	}
	if _, neg := a.CacheSizes(); neg != 0 {
		t.Fatal("an unreadable record must not be cached as unknown")
	}
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

// With the database hanging (packets dropped, not refused) a request for a cached key must not
// wait for the lookup timeout: it waits at most RefreshWait and is served from the stale copy.
func TestHangingStoreDoesNotStallCachedKeys(t *testing.T) {
	a, st, clk, _ := setup(t, Config{RefreshWait: 20 * time.Millisecond, LookupTimeout: 5 * time.Second, CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	key, prefix := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	base := st.lookups.Load()
	st.gate = make(chan struct{}) // every lookup now hangs
	clk.advance(time.Minute)      // past the TTL, inside the grace

	const n = 30
	var wg sync.WaitGroup
	lat := make(chan time.Duration, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			_, err := a.Authenticate(context.Background(), key)
			lat <- time.Since(start)
			errs <- err
		}()
	}
	wg.Wait()
	close(lat)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a stale key must be served while the store hangs: %v", err)
		}
	}
	for d := range lat {
		if d > time.Second {
			t.Fatalf("a request waited %v for a hanging database (lookup timeout is 5s)", d)
		}
	}
	if got := st.lookups.Load() - base; got != 1 {
		t.Fatalf("%d lookups for %d concurrent refreshes, want one shared flight", got, n)
	}

	// The refresh is still running; when the database answers, a revocation made meanwhile is seen.
	st.update(prefix, func(r *KeyRecord) { r.KeyStatus = KeyRevoked })
	close(st.gate)
	waitUntil(t, "the background refresh to land", func() bool {
		_, err := a.Authenticate(context.Background(), key)
		return errors.Is(err, ErrRevoked)
	})
}

func TestStaleGraceBoundaryIsExact(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: 30 * time.Second, StaleGrace: 5 * time.Minute})
	key, _ := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)
	clk.advance(30*time.Second + 5*time.Minute - time.Nanosecond)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatalf("one tick inside the grace period: %v", err)
	}
	clk.advance(time.Nanosecond)
	if _, err := a.Authenticate(context.Background(), key); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("at exactly ttl+grace the stale copy must no longer be served: %v", err)
	}
}

// A key that the store no longer knows must not be served from its stale copy.
func TestRefreshThatFindsNothingEvictsTheKey(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: 30 * time.Second})
	key, prefix := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	delete(st.recs, prefix)
	st.mu.Unlock()
	clk.advance(time.Minute)
	if _, err := a.Authenticate(context.Background(), key); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a key the store has dropped was served from its stale copy: %v", err)
	}
	if pos, neg := a.CacheSizes(); pos != 0 || neg != 1 {
		t.Fatalf("cache after eviction: positive %d (want 0) negative %d (want 1)", pos, neg)
	}
}

// A success proves the store works, so it ends a backoff that a failure started.
func TestSuccessEndsTheBackoff(t *testing.T) {
	a, st, _, _ := setup(t, Config{})
	keyA, pa := st.add(1)
	_, pb := st.add(2)
	keyB := "sf_" + pb + "_" + keyA[len("sf_")+9:]
	keyC, _ := st.add(3)
	release := make(chan struct{})
	st.mu.Lock()
	st.holdFor = func(p string) <-chan struct{} {
		if p == pa {
			return release
		}
		return nil
	}
	st.errFor = func(p string) error {
		if p == pb {
			return errors.New("connection reset")
		}
		return nil
	}
	st.mu.Unlock()

	done := make(chan error, 1)
	go func() { _, err := a.Authenticate(context.Background(), keyA); done <- err }()
	waitUntil(t, "A's lookup to start", func() bool { return st.lookups.Load() == 1 })
	if _, err := a.Authenticate(context.Background(), keyB); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("B: %v", err)
	}
	// The failure armed the backoff: C is refused without a lookup.
	before := st.lookups.Load()
	if _, err := a.Authenticate(context.Background(), keyC); !errors.Is(err, ErrUnavailable) || st.lookups.Load() != before {
		t.Fatalf("C during the backoff: %v (lookups %d -> %d)", err, before, st.lookups.Load())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// A's success cleared it: C is looked up now, with no time having passed.
	if _, err := a.Authenticate(context.Background(), keyC); err != nil {
		t.Fatalf("after a success the backoff must be over: %v", err)
	}
}

func TestEachOutageIsLoggedOnce(t *testing.T) {
	a, st, clk, logs := setup(t, Config{CacheTTL: 30 * time.Second})
	key, _ := st.add(1)
	other, _ := st.add(2)
	for round := 1; round <= 2; round++ {
		st.fail.Store(true)
		clk.advance(2 * time.Second)
		_, _ = a.Authenticate(context.Background(), other) // fails and logs (once per outage)
		_, _ = a.Authenticate(context.Background(), other)
		st.fail.Store(false)
		clk.advance(2 * time.Second)
		if _, err := a.Authenticate(context.Background(), key); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Hour) // expire the caches so the next round looks things up again
		if got := strings.Count(logs.String(), "key store unavailable"); got != round {
			t.Fatalf("after outage %d the log has %d outage lines", round, got)
		}
		if got := strings.Count(logs.String(), "recovered"); got != round {
			t.Fatalf("after outage %d the log has %d recovery lines", round, got)
		}
	}
}

func TestEntryFromTheFutureIsNotTrusted(t *testing.T) {
	a, st, clk, _ := setup(t, Config{CacheTTL: 30 * time.Second})
	key, _ := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	clk.set(clk.now().Add(-time.Minute)) // the clock steps backwards
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if n := st.lookups.Load(); n != 2 {
		t.Fatalf("an entry whose age is negative must be looked up again, got %d lookups", n)
	}
}

// Unknown prefixes and wrong secrets must cost the same comparison.
func TestUnknownPrefixDoesTheSameComparisonAsAWrongSecret(t *testing.T) {
	var calls atomic.Int64
	old := equalHashes
	equalHashes = func(x, y []byte) bool { calls.Add(1); return HashesEqual(x, y) }
	defer func() { equalHashes = old }()
	a, st, _, _ := setup(t, Config{})
	_, gp := st.add(1)
	other, _, _ := GenerateKey()
	wrong := "sf_" + gp + "_" + other[len("sf_")+9:]
	unknown, _, _ := GenerateKey()
	_, _ = a.Authenticate(context.Background(), wrong)
	perWrong := calls.Swap(0)
	_, _ = a.Authenticate(context.Background(), unknown)
	if perUnknown := calls.Load(); perWrong != 1 || perUnknown != 1 {
		t.Fatalf("hash comparisons: wrong secret %d, unknown prefix %d, want 1 each", perWrong, perUnknown)
	}
}

// Shedding says nothing about the store's health: it must not start the backoff.
func TestShedRequestDoesNotStartABackoff(t *testing.T) {
	a, st, clk, _ := setup(t, Config{MaxLookups: 2, CacheTTL: 30 * time.Second, StaleGrace: time.Hour}) // one slot for unseen keys, one reserved for refreshes
	keyA, pa := st.add(1)
	keyB, _ := st.add(2)
	keyC, pc := st.add(3)
	if _, err := a.Authenticate(context.Background(), keyC); err != nil {
		t.Fatal(err)
	}
	st.update(pc, func(r *KeyRecord) { r.KeyStatus = KeyRevoked })
	clk.advance(time.Minute) // C is now due for a refresh
	release := make(chan struct{})
	st.mu.Lock()
	st.holdFor = func(p string) <-chan struct{} {
		if p == pa {
			return release
		}
		return nil
	}
	st.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := a.Authenticate(context.Background(), keyA); done <- err }()
	waitUntil(t, "A to occupy the only slot for unseen keys", func() bool { return st.lookups.Load() == 2 })
	before := st.lookups.Load()
	if _, err := a.Authenticate(context.Background(), keyB); !errors.Is(err, ErrUnavailable) || st.lookups.Load() != before {
		t.Fatalf("B should be shed without a lookup: %v", err)
	}
	// No time has passed. If B's shedding had armed the backoff, C would now be served from its
	// stale copy instead of being refreshed, and its revocation would be missed.
	if _, err := a.Authenticate(context.Background(), keyC); !errors.Is(err, ErrRevoked) {
		t.Fatalf("a shed request started a backoff (C should have been refreshed and found revoked): %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The counters for lookups in flight must come back to zero, and refreshes must not be counted as
// lookups of unseen keys: after many refreshes the caps still admit both kinds.
func TestRefreshesDoNotLeakLookupCapacity(t *testing.T) {
	a, st, clk, _ := setup(t, Config{MaxLookups: 4, CacheTTL: 30 * time.Second, StaleGrace: time.Hour})
	key, _ := st.add(0)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	const rounds = 25
	for i := 0; i < rounds; i++ {
		clk.advance(time.Minute) // past the TTL: a refresh each time, one at a time
		before := st.lookups.Load()
		if _, err := a.Authenticate(context.Background(), key); err != nil {
			t.Fatal(err)
		}
		if st.lookups.Load() != before+1 {
			t.Fatalf("refresh %d did not reach the store: the cap leaked (lookups %d -> %d)", i, before, st.lookups.Load())
		}
	}
	a.mu.Lock()
	r, n := a.inflightRefresh, a.inflightNew
	a.mu.Unlock()
	if r != 0 || n != 0 {
		t.Fatalf("lookups in flight after everything finished: refresh %d, new %d", r, n)
	}
	// Unseen keys are still admitted, up to their share (3 of 4 here).
	for i := 1; i <= 6; i++ {
		fresh, _ := st.add(i)
		if _, err := a.Authenticate(context.Background(), fresh); err != nil {
			t.Fatalf("unseen key %d refused after %d refreshes: %v", i, rounds, err)
		}
	}
}

// The wait for a refresh is really RefreshWait: neither skipped nor stretched.
func TestRefreshWaitIsTheConfiguredBudget(t *testing.T) {
	const budget = 150 * time.Millisecond
	a, st, clk, _ := setup(t, Config{RefreshWait: budget, LookupTimeout: 10 * time.Second, CacheTTL: 30 * time.Second, StaleGrace: time.Hour})
	key, _ := st.add(1)
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	st.gate = make(chan struct{})
	t.Cleanup(func() { close(st.gate) })
	clk.advance(time.Minute)
	start := time.Now()
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	d := time.Since(start)
	if d < budget*8/10 || d > budget*4 {
		t.Fatalf("a stale refresh waited %v, want about %v", d, budget)
	}
}
