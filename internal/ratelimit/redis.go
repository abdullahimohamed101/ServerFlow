package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/internal/redis"
)

// FailureMode is what the limiter does when Redis cannot answer.
type FailureMode string

// The failure modes (redis.on_failure). Closed protects quotas: requests that need a check are refused with 503.
// Open protects availability: they are admitted unchecked and counted.
const (
	FailClosed FailureMode = "closed"
	FailOpen   FailureMode = "open"
)

// Config tunes a RedisLimiter. Zero fields take the defaults noted.
type Config struct {
	// OnFailure defaults to FailClosed.
	OnFailure FailureMode
	// BurstSeconds is how many seconds of quota a bucket holds (default 60, at most MaxBurstSeconds).
	BurstSeconds int
	// LeaseTTL is how long a concurrency lease lives without renewal (default 60s). The limiter renews a
	// request's lease every third of it.
	LeaseTTL time.Duration
	// MaxLocalLeases bounds the leases this process tracks (default 100,000).
	MaxLocalLeases int
	// ModelRequestsPerMinute optionally caps requests per minute per model, across all tenants and gateways. Only the models
	// listed here ever become Redis keys.
	ModelRequestsPerMinute map[string]int
	// Logger receives debug and warning lines (never keys or the password); nil discards them.
	Logger *slog.Logger
	// Clock, when set, replaces Redis TIME in the scripts. It is for tests only: with it the limiter trusts
	// this process's clock instead of Redis's.
	Clock func() time.Time
}

// Defaults for Config.
const (
	DefaultBurstSeconds   = 60
	DefaultLeaseTTL       = 60 * time.Second
	DefaultMaxLocalLeases = 100_000
	releaseQueueSize      = 4096
	releaseWorkers        = 4
	renewChunk            = 500
)

// RedisLimiter enforces quotas with one atomic script per check against a shared Redis. It implements Limiter.
type RedisLimiter struct {
	c   *redis.Client
	cfg Config
	log *slog.Logger

	mu     sync.Mutex
	leases map[string]string // lease id -> tenant, for every lease this process holds

	releaseQ chan releaseJob
	dropped  atomic.Int64
	closed   atomic.Bool
	newID    func() string // lease ids; tests replace it
}

type releaseJob struct{ tenant, id string }

// NewRedis returns a limiter on c. Start its background work with Run.
func NewRedis(c *redis.Client, cfg Config) (*RedisLimiter, error) {
	if c == nil {
		return nil, errors.New("ratelimit: a Redis client is required")
	}
	if cfg.OnFailure == "" {
		cfg.OnFailure = FailClosed
	}
	if cfg.OnFailure != FailClosed && cfg.OnFailure != FailOpen {
		return nil, fmt.Errorf("ratelimit: unknown failure mode %q", cfg.OnFailure)
	}
	if cfg.BurstSeconds == 0 {
		cfg.BurstSeconds = DefaultBurstSeconds
	}
	if cfg.BurstSeconds < 1 || cfg.BurstSeconds > MaxBurstSeconds {
		return nil, fmt.Errorf("ratelimit: burst seconds must be in 1-%d", MaxBurstSeconds)
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.LeaseTTL < 3*time.Millisecond {
		return nil, errors.New("ratelimit: lease ttl is too short")
	}
	if cfg.MaxLocalLeases == 0 {
		cfg.MaxLocalLeases = DefaultMaxLocalLeases
	}
	if cfg.MaxLocalLeases < 1 {
		return nil, errors.New("ratelimit: max local leases must be positive")
	}
	models := make(map[string]int, len(cfg.ModelRequestsPerMinute))
	for m, n := range cfg.ModelRequestsPerMinute {
		if !validKeyPart(m) {
			return nil, errors.New("ratelimit: a model name is too long")
		}
		if n > 0 {
			models[m] = min(n, MaxQuota)
		}
	}
	cfg.ModelRequestsPerMinute = models
	l := &RedisLimiter{c: c, cfg: cfg, log: cfg.Logger, leases: map[string]string{}, releaseQ: make(chan releaseJob, releaseQueueSize), newID: newLeaseID}
	if l.log == nil {
		l.log = slog.New(slog.DiscardHandler)
	}
	return l, nil
}

// errBadName is returned for a tenant or model name too long to be a key.
var errBadName = fmt.Errorf("%w: the tenant or model name is not usable as a key", ErrUnavailable)

func clampQuota(n int) int {
	switch {
	case n < 0:
		return 0
	case n > MaxQuota:
		return MaxQuota
	}
	return n
}

func (l *RedisLimiter) nowOverride() int64 {
	if l.cfg.Clock == nil {
		return 0
	}
	return l.cfg.Clock().UnixMilli()
}

// LocalLeases is how many concurrency leases this process currently tracks.
func (l *RedisLimiter) LocalLeases() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.leases)
}

// DroppedReleases counts releases that were not sent because the queue was full or the limiter closed; the
// leases expire by themselves within LeaseTTL.
func (l *RedisLimiter) DroppedReleases() int64 { return l.dropped.Load() }

func newLeaseID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("ratelimit: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Allow checks r. A request with nothing to enforce (no tenant quota and no cap on its model) is admitted
// without calling Redis. Otherwise one script call either admits it (charging every quota and taking a lease
// when concurrency is limited) or refuses it (charging nothing).
//
// When Redis cannot answer, a closed limiter returns an *UnavailableError and an open one admits the request
// with Decision.Bypassed set.
func (l *RedisLimiter) Allow(ctx context.Context, r Request) (Decision, error) {
	lim := Limits{clampQuota(r.Limits.RequestsPerMinute), clampQuota(r.Limits.TokensPerMinute), clampQuota(r.Limits.MaxConcurrent)}
	if r.TenantID == "" {
		lim = Limits{} // tenant quotas need an identity
	}
	modelQuota := l.cfg.ModelRequestsPerMinute[r.Model]
	if lim.None() && modelQuota == 0 {
		return Decision{Allowed: true, Release: noopRelease}, nil
	}
	if !validKeyPart(r.TenantID) || (modelQuota > 0 && !validKeyPart(r.Model)) {
		return l.failed(errBadName)
	}
	cost := int64(max(0, min(r.Cost, MaxCost)))

	leaseID := ""
	if lim.MaxConcurrent > 0 {
		leaseID = l.newID()
		l.mu.Lock()
		if len(l.leases) >= l.cfg.MaxLocalLeases {
			l.mu.Unlock()
			return Decision{}, &UnavailableError{RetryAfter: time.Second, Cause: ErrLeaseCapacity}
		}
		l.leases[leaseID] = r.TenantID
		l.mu.Unlock()
	}

	// KEYS[4] is the model bucket and is touched only when the model has a cap. r.Model is chosen by the client,
	// so without a cap a constant placeholder goes on the wire: no escaping, no length work, and a huge model
	// name cannot make every limited request slow enough to trip the timeout for everyone.
	modelKey := keyModelUnused
	if modelQuota > 0 {
		modelKey = keyModel(r.Model)
	}
	keys := []string{keyRequests(r.TenantID), keyTokens(r.TenantID), keyConcurrency(r.TenantID), modelKey}
	res, err := l.c.Run(ctx, acquireScript, keys, l.nowOverride(), lim.RequestsPerMinute, lim.TokensPerMinute, lim.MaxConcurrent,
		modelQuota, cost, l.cfg.BurstSeconds, leaseID, l.cfg.LeaseTTL.Milliseconds())
	if err != nil {
		l.forget(leaseID)
		return l.failed(err)
	}
	if res[0] == 1 {
		d := Decision{Allowed: true, Release: noopRelease}
		if leaseID != "" {
			d.Release = l.releaser(r.TenantID, leaseID)
		}
		return d, nil
	}
	l.forget(leaseID)
	return Decision{Limit: limitFromCode(res[1]), RetryAfter: time.Duration(res[2]) * time.Millisecond, Release: noopRelease}, nil
}

func limitFromCode(code int64) Limit {
	switch code {
	case 1:
		return LimitRequests
	case 2:
		return LimitTokens
	case 3:
		return LimitConcurrency
	case 4:
		return LimitModel
	}
	return LimitUnavailable
}

// failed applies the failure mode to an error that stopped a check.
func (l *RedisLimiter) failed(err error) (Decision, error) {
	if l.cfg.OnFailure == FailOpen && !errors.Is(err, ErrLeaseCapacity) {
		return Decision{Allowed: true, Bypassed: true, Release: noopRelease}, nil
	}
	return Decision{}, &UnavailableError{RetryAfter: l.c.RetryAfter(), Cause: err}
}

func (l *RedisLimiter) forget(id string) {
	if id == "" {
		return
	}
	l.mu.Lock()
	delete(l.leases, id)
	l.mu.Unlock()
}

// releaser returns the function that gives a lease back. It is idempotent and never blocks: the lease stops
// being renewed at once, and the Redis call is queued for a background worker (or dropped when the queue is
// full; the lease then expires on its own).
func (l *RedisLimiter) releaser(tenant, id string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.forget(id)
			l.enqueue(releaseJob{tenant, id})
		})
	}
}

func (l *RedisLimiter) enqueue(j releaseJob) {
	if l.closed.Load() {
		l.dropped.Add(1)
		return
	}
	select {
	case l.releaseQ <- j:
	default:
		l.dropped.Add(1)
	}
}

func (l *RedisLimiter) doRelease(j releaseJob) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := l.c.Run(ctx, releaseScript, []string{keyConcurrency(j.tenant)}, j.id); err != nil {
		l.dropped.Add(1)
		l.log.Debug("could not release a concurrency lease; it will expire", "component", "ratelimit", "error", err.Error())
	}
}

// Run does the limiter's background work until ctx ends: it sends releases and renews the leases of requests
// that are still running. The gateway starts it with Serve.
func (l *RedisLimiter) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < releaseWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-l.releaseQ:
					l.doRelease(j)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(l.cfg.LeaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				l.renewAll(ctx)
			}
		}
	}()
	wg.Wait()
}

// Close sends the releases still queued (bounded by timeout) and stops accepting new ones. Call it after the
// gateway has stopped serving.
func (l *RedisLimiter) Close(timeout time.Duration) {
	l.closed.Store(true)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case j := <-l.releaseQ:
			l.doRelease(j)
		default:
			return
		}
	}
}

// renewAll extends every lease this process holds, one call per tenant (a tenant's keys share a slot).
func (l *RedisLimiter) renewAll(ctx context.Context) {
	l.mu.Lock()
	byTenant := map[string][]string{}
	for id, tenant := range l.leases {
		byTenant[tenant] = append(byTenant[tenant], id)
	}
	l.mu.Unlock()
	for tenant, ids := range byTenant {
		for len(ids) > 0 {
			if ctx.Err() != nil {
				return
			}
			n := min(len(ids), renewChunk)
			args := make([]any, 0, n+2)
			args = append(args, l.nowOverride(), l.cfg.LeaseTTL.Milliseconds())
			for _, id := range ids[:n] {
				args = append(args, id)
			}
			ids = ids[n:]
			if _, err := l.c.Run(ctx, renewScript, []string{keyConcurrency(tenant)}, args...); err != nil {
				l.log.Debug("could not renew concurrency leases", "component", "ratelimit", "error", err.Error())
			}
		}
	}
}
