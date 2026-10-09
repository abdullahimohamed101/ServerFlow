// Package ratelimit enforces per-tenant request, token and concurrency quotas, and an optional
// per-model request cap, across every gateway that shares one Redis (spec sections 16-18, 46).
//
// The package has three layers. model.go is a pure, clock-free model of the algorithm (token
// buckets and concurrency leases) that the tests use as an oracle. script.go is the same algorithm
// as one atomic Lua script. redis.go (RedisLimiter) runs that script through internal/redis and adds
// lease renewal, release, outage backoff and the closed/open failure modes. See ADR-015.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Limit names the quota that refused a request. The values are Prometheus label values and appear in
// error messages, so the set is fixed.
type Limit string

// The quotas, plus LimitUnavailable for requests refused because Redis could not answer.
const (
	LimitRequests    Limit = "requests"
	LimitTokens      Limit = "tokens"
	LimitConcurrency Limit = "concurrency"
	LimitModel       Limit = "model"
	LimitUnavailable Limit = "unavailable"
)

// Limits are one tenant's quotas. 0 means "not limited" for that dimension.
type Limits struct {
	RequestsPerMinute int
	TokensPerMinute   int
	MaxConcurrent     int
}

// None reports whether no dimension is limited.
func (l Limits) None() bool {
	return l.RequestsPerMinute <= 0 && l.TokensPerMinute <= 0 && l.MaxConcurrent <= 0
}

// Quotas are bounded so that every intermediate value in the Lua script stays an exactly
// representable integer (see script.go). Larger quotas are treated as this.
const (
	MaxQuota        = 1_000_000_000
	MaxBurstSeconds = 3600
)

// Request is one admission check. TenantID may be empty (authentication off), in which case only the
// per-model cap can apply. Cost is the token estimate (see EstimateCost).
type Request struct {
	TenantID string
	Model    string
	Cost     int
	Limits   Limits
}

// Decision is the answer to an admission check.
type Decision struct {
	Allowed bool
	// Limit and RetryAfter describe a refusal: which quota and how long until a retry can succeed
	// (at least one second for the Retry-After header; see RetryAfterSeconds).
	Limit      Limit
	RetryAfter time.Duration
	// Bypassed is set on an allowed request that was not checked because Redis was unavailable and the
	// limiter fails open.
	Bypassed bool
	// Release gives back the concurrency slot. It is never nil on an allowed decision, is safe to call
	// more than once, and must be called when the request ends, whatever the outcome.
	Release func()
}

// RetryAfterSeconds is RetryAfter rounded up to whole seconds, at least 1, for the Retry-After header.
func (d Decision) RetryAfterSeconds() int { return ceilSeconds(d.RetryAfter) }

func ceilSeconds(d time.Duration) int {
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}

// Limiter decides whether a request may proceed. An error means the limiter could not decide (Redis
// unavailable and the limiter fails closed); it is an *UnavailableError or wraps ErrUnavailable.
type Limiter interface {
	Allow(ctx context.Context, r Request) (Decision, error)
}

// ErrUnavailable is wrapped by every error that means "the limit could not be checked right now".
var ErrUnavailable = errors.New("ratelimit: unavailable")

// ErrLeaseCapacity is returned when this gateway already holds the maximum number of concurrency
// leases it will track (Config.MaxLocalLeases). It wraps ErrUnavailable and is returned in both failure
// modes: the cap protects the gateway's memory, not Redis.
var ErrLeaseCapacity = fmt.Errorf("%w: too many leases are held by this gateway", ErrUnavailable)

// UnavailableError is returned by a limiter that failed closed. RetryAfter is when a retry is worth
// trying (the rest of the backoff, at least a second for the header).
type UnavailableError struct {
	RetryAfter time.Duration
	Cause      error
}

func (e *UnavailableError) Error() string {
	if e.Cause == nil {
		return "ratelimit: unavailable"
	}
	return "ratelimit: unavailable: " + e.Cause.Error()
}

// Unwrap lets errors.Is(err, ErrUnavailable) see through it.
func (e *UnavailableError) Unwrap() []error { return []error{ErrUnavailable, e.Cause} }

// RetryAfterSeconds is RetryAfter rounded up to whole seconds, at least 1.
func (e *UnavailableError) RetryAfterSeconds() int { return ceilSeconds(e.RetryAfter) }

func noopRelease() {}
