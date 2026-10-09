package ratelimit

// This file is the algorithm as plain Go on plain integers, with no clock and no Redis. The Lua script
// in script.go implements exactly the same arithmetic; the tests run both on the same random sequences
// and require identical answers, so a change to one without the other fails a test.
//
// Token bucket arithmetic is done in integers so there is no rounding to disagree about. A quota of Q
// tokens per minute refills Q tokens in 60,000 ms, so one millisecond refills Q/60,000 of a token. Levels
// are therefore stored in "scaled" units of 1/60,000 token: a refill of e milliseconds adds e*Q scaled
// units, one token costs 60,000, and a bucket of burst B seconds holds Q*B*1000 scaled units.

const (
	// scale is how many scaled units make one token.
	scale int64 = 60_000
	// concurrencyRetryMs is the Retry-After hint for a concurrency refusal. A slot frees when a request
	// finishes, which no one can predict, so this is a hint, not a promise.
	concurrencyRetryMs int64 = 1000
)

// Params are the settings both the model and the script need.
type Params struct {
	// BurstSeconds is how many seconds of quota a bucket holds when full (and so how long it takes to
	// refill from empty).
	BurstSeconds int
	// LeaseTTLMs is how long a concurrency lease lives without being renewed.
	LeaseTTLMs int64
}

// Outcome is the model's (and the script's) answer to one check.
type Outcome struct {
	Allowed bool
	Limit   Limit
	// RetryMs is how long until the refused quota can admit the request (a hint for concurrency).
	RetryMs int64
}

// ModelRequest is one check as the model sees it: the quotas for this call and the lease to take.
type ModelRequest struct {
	TenantID string
	Model    string
	Cost     int
	Limits   Limits
	// ModelRequestsPerMinute is the per-model cap for Model, 0 for none.
	ModelRequestsPerMinute int
	// LeaseID names the concurrency lease taken when Limits.MaxConcurrent > 0.
	LeaseID string
}

type bucket struct {
	level int64 // scaled units
	at    int64 // millisecond of the last update
	set   bool
}

// Model is the reference implementation. It is not safe for concurrent use.
type Model struct {
	p       Params
	buckets map[string]*bucket
	leases  map[string]map[string]int64 // concurrency key -> lease id -> expiry (ms)
}

// NewModel returns an empty model.
func NewModel(p Params) *Model {
	return &Model{p: p, buckets: map[string]*bucket{}, leases: map[string]map[string]int64{}}
}

// capacity is the scaled size of a bucket with the given quota.
func (m *Model) capacity(quota int64) int64 { return quota * int64(m.p.BurstSeconds) * 1000 }

// peek returns a bucket's level at time now after refilling, and its capacity.
func (m *Model) peek(key string, now, quota int64) (level, capacity int64) {
	capacity = m.capacity(quota)
	b := m.buckets[key]
	if b == nil || !b.set {
		return capacity, capacity
	}
	elapsed := now - b.at
	if elapsed < 0 { // the clock went backwards (a Redis failover, a VM clock step): no refill, and the stamp is kept (see Allow)
		elapsed = 0
	}
	if window := int64(m.p.BurstSeconds) * 1000; elapsed > window {
		elapsed = window
	}
	level = b.level + elapsed*quota
	if level > capacity { // also clamps a bucket whose quota was lowered
		level = capacity
	}
	return level, capacity
}

// need is the scaled cost of cost tokens, never more than the bucket can hold: a request bigger than
// the whole bucket needs a full bucket rather than being refused forever.
func need(cost, capacity int64) int64 { return min(cost*scale, capacity) }

// ceilDiv is a/b rounded up for a >= 0, b > 0.
func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }

// Allow checks r at time nowMs (milliseconds). Either every quota admits the request and all of them
// are charged, or none is charged. When several quotas refuse, the one with the longest wait is reported
// (ties: requests, tokens, concurrency, model).
func (m *Model) Allow(nowMs int64, r ModelRequest) Outcome {
	type check struct {
		limit Limit
		key   string
		quota int64
		level int64
		need  int64
	}
	var checks []*check
	add := func(l Limit, key string, quota int, cost int) {
		if quota <= 0 {
			return
		}
		c := &check{limit: l, key: key, quota: int64(quota)}
		var capacity int64
		c.level, capacity = m.peek(key, nowMs, c.quota)
		c.need = need(int64(cost), capacity)
		checks = append(checks, c)
	}
	add(LimitRequests, keyRequests(r.TenantID), r.Limits.RequestsPerMinute, 1)
	add(LimitTokens, keyTokens(r.TenantID), r.Limits.TokensPerMinute, r.Cost)
	add(LimitModel, keyModel(r.Model), r.ModelRequestsPerMinute, 1)

	var worst Outcome
	consider := func(l Limit, wait int64) {
		if wait > worst.RetryMs {
			worst = Outcome{Limit: l, RetryMs: wait}
		}
	}
	for _, c := range checks {
		if c.level < c.need {
			if c.limit == LimitModel {
				continue // considered last, below
			}
			consider(c.limit, ceilDiv(c.need-c.level, c.quota))
		}
	}
	concKey := keyConcurrency(r.TenantID)
	if r.Limits.MaxConcurrent > 0 {
		held := m.leases[concKey]
		for id, exp := range held {
			if exp <= nowMs {
				delete(held, id)
			}
		}
		if len(held) >= r.Limits.MaxConcurrent {
			consider(LimitConcurrency, concurrencyRetryMs)
		}
	}
	for _, c := range checks {
		if c.limit == LimitModel && c.level < c.need {
			consider(c.limit, ceilDiv(c.need-c.level, c.quota))
		}
	}
	if worst.Limit != "" {
		return worst
	}

	for _, c := range checks {
		// The stamp never moves back: if the clock stepped back, the bucket keeps its later stamp, so when the
		// clock returns the refill is the real elapsed time and not the size of the step.
		at := nowMs
		if b := m.buckets[c.key]; b != nil && b.set && b.at > at {
			at = b.at
		}
		m.buckets[c.key] = &bucket{level: c.level - c.need, at: at, set: true}
	}
	if r.Limits.MaxConcurrent > 0 {
		if m.leases[concKey] == nil {
			m.leases[concKey] = map[string]int64{}
		}
		m.leases[concKey][r.LeaseID] = nowMs + m.p.LeaseTTLMs
	}
	return Outcome{Allowed: true}
}

// Release removes a lease. It reports whether the lease was held.
func (m *Model) Release(tenantID, leaseID string) bool {
	k := keyConcurrency(tenantID)
	_, ok := m.leases[k][leaseID]
	delete(m.leases[k], leaseID)
	return ok
}

// Renew extends a lease that is still held (a lease that expired or was released is not revived). It
// reports whether the lease was extended.
func (m *Model) Renew(nowMs int64, tenantID, leaseID string) bool {
	k := keyConcurrency(tenantID)
	exp, ok := m.leases[k][leaseID]
	if !ok || exp <= nowMs {
		return false
	}
	m.leases[k][leaseID] = nowMs + m.p.LeaseTTLMs
	return true
}

// Held returns how many leases the tenant holds at nowMs.
func (m *Model) Held(nowMs int64, tenantID string) int {
	n := 0
	for _, exp := range m.leases[keyConcurrency(tenantID)] {
		if exp > nowMs {
			n++
		}
	}
	return n
}
