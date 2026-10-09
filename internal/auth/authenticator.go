package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Config tunes the Authenticator's cache. See ADR-014 for why each value is what it is.
type Config struct {
	// CacheTTL is how long a looked-up key is trusted without asking the store again. A key
	// revoked in the database stops working within this time.
	CacheTTL time.Duration
	// NegativeTTL is how long an unknown key prefix is remembered, so a flood of bad keys does
	// not become a flood of queries.
	NegativeTTL time.Duration
	// CacheSize bounds each of the positive and the negative cache (entries). They are separate so
	// random keys can fill only the negative one and never push out a real customer's key.
	CacheSize int
	// StaleGrace is how long past CacheTTL a cached key may still be served when the store is down.
	StaleGrace time.Duration
	// LookupTimeout bounds one store lookup.
	LookupTimeout time.Duration
	// MaxLookups caps concurrent store lookups. Keep it below the connection pool size so lookups
	// can never take every connection. A quarter (at least one) is reserved for refreshing keys
	// that are already cached; the rest also serves keys not seen before. Default 8.
	MaxLookups int
	// Now is the clock; nil means time.Now. Tests inject a fake.
	Now func() time.Time
	// Logger receives outage and recovery messages (never keys); nil discards them.
	Logger *slog.Logger
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config {
	return Config{CacheTTL: 30 * time.Second, NegativeTTL: 5 * time.Second, CacheSize: 10000, StaleGrace: 5 * time.Minute, LookupTimeout: 3 * time.Second, MaxLookups: 8}
}

// touchEvery is the least time between last_used_at writes for one key.
const touchEvery = time.Minute

// outageBackoff is how long after a failed lookup the store is left alone, so a dead or hanging
// database costs one slow request per interval rather than one per request.
const outageBackoff = time.Second

// OutageBackoff is the shortest useful Retry-After for an AUTH_UNAVAILABLE answer.
const OutageBackoff = outageBackoff

// Authenticator verifies API keys. A warm cache answers without touching the store; misses
// share one lookup per prefix; the store being down degrades as described in ADR-014.
// It is safe for concurrent use.
type Authenticator struct {
	store   KeyStore
	toucher KeyToucher
	cfg     Config
	log     *slog.Logger
	now     func() time.Time

	mu      sync.Mutex
	pos     *lru[*posEntry]
	neg     *lru[time.Time]
	flights map[string]*flight

	outage    bool      // guarded by mu: true between the first failed lookup and the next success
	downUntil time.Time // guarded by mu: after a failed lookup, the store is not asked again until then
	touches   chan touchReq

	reserve         int       // lookup slots kept for refreshing cached keys
	inflightRefresh int       // guarded by mu
	inflightNew     int       // guarded by mu
	lastWarn        time.Time // guarded by mu
	onWait          func()    // test hook: called when a caller joins an in-flight lookup
}

type posEntry struct {
	rec       KeyRecord
	fetched   time.Time
	lastTouch time.Time
}

type flight struct {
	done chan struct{}
	rec  KeyRecord
	err  error
}

type touchReq struct {
	keyID string
	at    time.Time
}

// New returns an Authenticator reading keys from store. Zero fields of cfg take their defaults.
func New(store KeyStore, cfg Config) *Authenticator {
	d := DefaultConfig()
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = d.CacheTTL
	}
	if cfg.NegativeTTL <= 0 {
		cfg.NegativeTTL = d.NegativeTTL
	}
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = d.CacheSize
	}
	if cfg.StaleGrace < 0 {
		cfg.StaleGrace = 0
	}
	if cfg.LookupTimeout <= 0 {
		cfg.LookupTimeout = d.LookupTimeout
	}
	if cfg.MaxLookups < 2 {
		cfg.MaxLookups = d.MaxLookups // at least one slot for new keys and one reserved for refreshes
	}
	a := &Authenticator{
		store:   store,
		cfg:     cfg,
		log:     cfg.Logger,
		now:     cfg.Now,
		pos:     newLRU[*posEntry](cfg.CacheSize),
		neg:     newLRU[time.Time](cfg.CacheSize),
		flights: map[string]*flight{},
		touches: make(chan touchReq, 256),
		reserve: max(1, cfg.MaxLookups/4),
	}
	if a.log == nil {
		a.log = slog.New(slog.DiscardHandler)
	}
	if a.now == nil {
		a.now = time.Now
	}
	a.toucher, _ = store.(KeyToucher)
	return a
}

// Run records last_used_at for authenticated keys until ctx ends. It is optional: without it
// the writes are simply dropped. Writes are queued by Authenticate at most once a minute per
// key and never block a request.
func (a *Authenticator) Run(ctx context.Context) {
	if a.toucher == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-a.touches:
			c, cancel := context.WithTimeout(ctx, a.cfg.LookupTimeout)
			if err := a.toucher.TouchKey(c, t.keyID, t.at); err != nil {
				a.log.Debug("auth: could not record key use", "component", "auth", "error", err)
			}
			cancel()
		}
	}
}

// CacheSizes reports the number of cached keys and remembered unknown prefixes.
func (a *Authenticator) CacheSizes() (positive, negative int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pos.len(), a.neg.len()
}

// dummyHash stands in for a missing key's hash so an unknown prefix does the same comparison
// work as a wrong secret.
var dummyHash = make([]byte, hashLen)

// Authenticate verifies a presented key (the bearer token). On success it returns the caller.
// Failures are ErrInvalid (malformed, unknown, wrong secret), ErrRevoked, ErrExpired,
// ErrSuspended, or ErrUnavailable (the store cannot answer and the key is not cached). The
// reason for ErrRevoked, ErrExpired and ErrSuspended is returned only to a caller that proved
// it holds the secret; every other failure is ErrInvalid.
func (a *Authenticator) Authenticate(ctx context.Context, bearer string) (*Principal, error) {
	prefix, hash, err := ParseKey(bearer)
	if err != nil {
		return nil, ErrInvalid
	}
	rec, err := a.record(ctx, prefix)
	if errors.Is(err, ErrNotFound) {
		HashesEqual(hash, dummyHash) // equalise the work; the result is irrelevant
		return nil, ErrInvalid
	}
	if err != nil {
		return nil, err
	}
	if !HashesEqual(hash, rec.SecretHash) {
		return nil, ErrInvalid
	}
	// The caller holds the secret, so from here the precise reason may be reported.
	now := a.now()
	switch {
	case rec.KeyStatus != KeyActive:
		return nil, ErrRevoked
	case rec.ExpiresAt != nil && !now.Before(*rec.ExpiresAt):
		return nil, ErrExpired
	case rec.Policy.Status != TenantActive:
		return nil, ErrSuspended
	}
	a.noteUse(prefix, rec.KeyID, now)
	return &Principal{TenantID: rec.TenantID, KeyID: rec.KeyID, Policy: rec.Policy}, nil
}

// record returns the key's record from the cache or the store, or ErrNotFound, ErrUnavailable or
// ErrBadRecord.
func (a *Authenticator) record(ctx context.Context, prefix string) (KeyRecord, error) {
	now := a.now()

	a.mu.Lock()
	rec, hit, stale, err := a.cachedLocked(prefix, now)
	down := now.Before(a.downUntil)
	a.mu.Unlock()
	if hit {
		return rec, err
	}

	if down {
		err = fmt.Errorf("%w: the key store failed moments ago", ErrUnavailable)
	} else {
		// A key already in the cache is being refreshed; anything else is new to this process.
		rec, err = a.fetch(ctx, prefix, stale != nil)
	}
	switch {
	case err == nil, errors.Is(err, ErrNotFound), errors.Is(err, ErrBadRecord):
		// A record the store could not read says nothing about the store being down, so a stale
		// copy of that one key is not served in its place.
		return rec, err
	case stale != nil:
		// The store is down or too busy but this key was verified recently enough: keep serving it.
		return stale.rec, nil
	default:
		return KeyRecord{}, err
	}
}

// cachedLocked answers from the caches. hit means the answer (a record, or ErrNotFound for a
// remembered unknown prefix) is fresh. Otherwise stale is the expired-but-within-grace entry, if
// any. Caller holds a.mu.
func (a *Authenticator) cachedLocked(prefix string, now time.Time) (rec KeyRecord, hit bool, stale *posEntry, err error) {
	if e, ok := a.pos.get(prefix); ok {
		age := now.Sub(e.fetched)
		switch {
		case age < a.cfg.CacheTTL && age >= 0:
			return e.rec, true, nil, nil
		case age < a.cfg.CacheTTL+a.cfg.StaleGrace && age >= 0:
			return KeyRecord{}, false, e, nil
		default:
			a.pos.remove(prefix)
		}
	}
	if at, ok := a.neg.get(prefix); ok {
		if age := now.Sub(at); age < a.cfg.NegativeTTL && age >= 0 {
			return KeyRecord{}, true, nil, ErrNotFound
		}
		a.neg.remove(prefix)
	}
	return KeyRecord{}, false, nil, nil
}

// errShed is what a request gets when lookup capacity is exhausted. The store was not asked.
var errShed = fmt.Errorf("%w: lookup capacity exhausted", ErrUnavailable)

// fetch asks the store, sharing one lookup among concurrent callers for the same prefix, and
// updates the caches. The lookup is detached from any one caller's context so that a client
// that gives up does not fail the others waiting on the same answer.
//
// The caches are checked again under the lock before a lookup is started: another caller may
// have finished one between this caller's miss and now, and without the re-check the two would
// both query the store.
//
// At most maxLookups run at once, below the connection pool size, so the pool is never entirely
// taken by lookups. Lookups for keys not in the cache (which an unauthenticated client can create
// at will with random keys) may use only maxLookups-reserve of those slots; the rest are kept for
// refreshing cached keys, so a flood cannot stop a revocation from being noticed. Over the limit a
// request is shed without touching the store.
func (a *Authenticator) fetch(ctx context.Context, prefix string, refresh bool) (KeyRecord, error) {
	a.mu.Lock()
	if rec, hit, _, err := a.cachedLocked(prefix, a.now()); hit {
		a.mu.Unlock()
		return rec, err
	}
	if f, ok := a.flights[prefix]; ok {
		a.mu.Unlock()
		if a.onWait != nil {
			a.onWait()
		}
		select {
		case <-f.done:
			return f.rec, f.err
		case <-ctx.Done():
			return KeyRecord{}, fmt.Errorf("%w: %v", ErrUnavailable, ctx.Err())
		}
	}
	if a.inflightRefresh+a.inflightNew >= a.cfg.MaxLookups || (!refresh && a.inflightNew >= a.cfg.MaxLookups-a.reserve) {
		a.mu.Unlock()
		return KeyRecord{}, errShed
	}
	if refresh {
		a.inflightRefresh++
	} else {
		a.inflightNew++
	}
	f := &flight{done: make(chan struct{})}
	a.flights[prefix] = f
	a.mu.Unlock()

	lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.LookupTimeout)
	rec, err := a.store.LookupKey(lctx, prefix)
	cancel()

	now := a.now()
	a.mu.Lock()
	delete(a.flights, prefix)
	if refresh {
		a.inflightRefresh--
	} else {
		a.inflightNew--
	}
	switch {
	case err == nil:
		var last time.Time
		if old, ok := a.pos.get(prefix); ok {
			last = old.lastTouch
		}
		a.pos.put(prefix, &posEntry{rec: rec, fetched: now, lastTouch: last})
		a.neg.remove(prefix)
		a.recovered()
		a.downUntil = time.Time{}
	case errors.Is(err, ErrNotFound):
		a.pos.remove(prefix)
		a.neg.put(prefix, now)
		rec, err = KeyRecord{}, ErrNotFound
		a.recovered()
		a.downUntil = time.Time{}
	case errors.Is(err, ErrBadRecord):
		// The store answered, but this one key's row could not be read. That is a fault of one key,
		// not an outage: no global backoff, no stale copy, and nothing is cached.
		a.warnLimited("auth key store returned a record it could not read; that key is refused", err)
		rec, err = KeyRecord{}, ErrBadRecord
		a.recovered()
		a.downUntil = time.Time{}
	case errors.Is(err, ErrBusy):
		// The pool or the database could not take the query now (for instance because it is
		// saturated). Not evidence of an outage, so no backoff, which an attacker could otherwise
		// arm by saturating the pool.
		a.warnLimited("auth key store is busy; some keys cannot be verified right now", err)
		rec, err = KeyRecord{}, fmt.Errorf("%w: the key store is busy", ErrUnavailable)
	default:
		a.failed(err)
		a.downUntil = now.Add(outageBackoff)
		rec, err = KeyRecord{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	f.rec, f.err = rec, err
	a.mu.Unlock()
	close(f.done)
	return rec, err
}

// warnLimited logs at most one warning every ten seconds. Caller holds a.mu.
func (a *Authenticator) warnLimited(msg string, err error) {
	now := a.now()
	if now.Sub(a.lastWarn) < 10*time.Second && !a.lastWarn.IsZero() {
		return
	}
	a.lastWarn = now
	a.log.Warn(msg, "component", "auth", "error", err.Error())
}

// failed notes a store failure, logging only the first of an outage. Caller holds a.mu.
func (a *Authenticator) failed(err error) {
	if !a.outage {
		a.outage = true
		a.log.Warn("auth key store unavailable: serving cached keys within the grace period and refusing others",
			"component", "auth", "error", err.Error(), "stale_grace", a.cfg.StaleGrace.String())
	}
}

// recovered notes a store success. Caller holds a.mu.
func (a *Authenticator) recovered() {
	if a.outage {
		a.outage = false
		a.log.Info("auth key store recovered", "component", "auth")
	}
}

// noteUse queues a last_used_at update, at most once a minute per key and never blocking.
func (a *Authenticator) noteUse(prefix, keyID string, now time.Time) {
	if a.toucher == nil {
		return
	}
	a.mu.Lock()
	e, ok := a.pos.get(prefix)
	if !ok || now.Sub(e.lastTouch) < touchEvery {
		a.mu.Unlock()
		return
	}
	e.lastTouch = now
	a.mu.Unlock()
	select {
	case a.touches <- touchReq{keyID: keyID, at: now}:
	default:
	}
}
