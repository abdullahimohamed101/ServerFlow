package config

import (
	"fmt"
	"strings"
	"time"
)

// RateLimitConfig configures distributed rate limiting (Phase 8). In "off" mode (the default) nothing is
// limited and Redis is not used. In "required" mode the gateway enforces each tenant's quotas (which need
// auth.mode=required) and the per-model caps, and refuses to start if Redis is unreachable.
// BurstSeconds is how many seconds of quota a bucket holds. LeaseTTL is how long a concurrency slot is held
// without renewal (a running request renews it every third of this). MaxLocalLeases bounds the slots one
// gateway tracks. ModelRequestsPerMinute optionally caps requests per minute per model across everything.
type RateLimitConfig struct {
	Mode                   string         `yaml:"mode"`
	BurstSeconds           int            `yaml:"burst_seconds"`
	LeaseTTL               time.Duration  `yaml:"lease_ttl"`
	MaxLocalLeases         int            `yaml:"max_local_leases"`
	ModelRequestsPerMinute map[string]int `yaml:"model_requests_per_minute"`
}

// validateRateLimit checks rate_limit.* and how it combines with auth and redis.
func (c *Config) validateRateLimit() error {
	r := &c.RateLimit
	switch r.Mode {
	case RateLimitOff, RateLimitRequired:
	default:
		return fmt.Errorf("rate_limit.mode must be %q or %q", RateLimitOff, RateLimitRequired)
	}
	if r.BurstSeconds < 1 || r.BurstSeconds > maxBurstSeconds {
		return fmt.Errorf("rate_limit.burst_seconds must be in 1-%d, got %d", maxBurstSeconds, r.BurstSeconds)
	}
	if r.LeaseTTL < minLeaseTTL || r.LeaseTTL > maxLeaseTTL {
		return fmt.Errorf("rate_limit.lease_ttl must be between %v and %v", minLeaseTTL, maxLeaseTTL)
	}
	if c.redisUsed() && r.LeaseTTL < minLeaseTTLTimeouts*c.Redis.Timeout {
		return fmt.Errorf("rate_limit.lease_ttl must be at least %d times redis.timeout, or a lease could lapse between renewals", minLeaseTTLTimeouts)
	}
	if r.MaxLocalLeases < 1 || r.MaxLocalLeases > maxLocalLeases {
		return fmt.Errorf("rate_limit.max_local_leases must be in 1-%d, got %d", maxLocalLeases, r.MaxLocalLeases)
	}
	if len(r.ModelRequestsPerMinute) > maxModelCaps {
		return fmt.Errorf("rate_limit.model_requests_per_minute lists %d models; at most %d", len(r.ModelRequestsPerMinute), maxModelCaps)
	}
	for m, n := range r.ModelRequestsPerMinute {
		if m == "" || len(m) > maxModelNameLen || strings.ContainsFunc(m, func(r rune) bool { return r < 0x21 || r == 0x7f }) {
			return fmt.Errorf("rate_limit.model_requests_per_minute: a model name is 1-%d characters without spaces or control characters", maxModelNameLen)
		}
		if n < 1 || n > maxModelCapQuota {
			return fmt.Errorf("rate_limit.model_requests_per_minute: the cap for %q must be in 1-%d", truncateForError(m), maxModelCapQuota)
		}
	}
	if r.Mode == RateLimitRequired && c.Auth.Mode != AuthModeRequired && len(r.ModelRequestsPerMinute) == 0 {
		return fmt.Errorf("rate_limit.mode %q has nothing to enforce: tenant quotas need auth.mode %q, and no rate_limit.model_requests_per_minute caps are set",
			RateLimitRequired, AuthModeRequired)
	}
	return nil
}
