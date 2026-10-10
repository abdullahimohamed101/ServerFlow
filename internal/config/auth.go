package config

import (
	"fmt"
	"time"
)

// Authentication modes (auth.mode).
const (
	AuthModeOff      = "off"
	AuthModeRequired = "required"
)

// AuthConfig configures client authentication at the gateway (Phase 9). In "off" mode (the
// default) the gateway is open, as in Phases 2-6, and PostgreSQL is not used. In "required" mode
// /v1/* needs a valid API key kept in PostgreSQL.
//
// Keys are looked up through a cache so the database is not on the request path: a key is
// trusted for CacheTTL after a lookup (so a revoked key can work for up to that long), an unknown
// key prefix is remembered for NegativeTTL, each cache holds at most CacheSize entries, and
// when the database is down a cached key keeps working for StaleGrace beyond its TTL.
type AuthConfig struct {
	Mode        string        `yaml:"mode"`
	CacheTTL    time.Duration `yaml:"cache_ttl"`
	NegativeTTL time.Duration `yaml:"negative_ttl"`
	CacheSize   int           `yaml:"cache_size"`
	StaleGrace  time.Duration `yaml:"stale_grace"`
}

func (a *AuthConfig) validate() error {
	switch a.Mode {
	case AuthModeOff, AuthModeRequired:
	default:
		return fmt.Errorf("auth.mode must be %q or %q, got %q", AuthModeOff, AuthModeRequired, truncateForError(a.Mode))
	}
	if a.CacheTTL <= 0 || a.CacheTTL > maxAuthCacheTTL {
		return fmt.Errorf("auth.cache_ttl must be > 0 and at most %v", maxAuthCacheTTL)
	}
	if a.NegativeTTL < minAuthNegativeTTL || a.NegativeTTL > a.CacheTTL {
		return fmt.Errorf("auth.negative_ttl must be at least %v and no more than auth.cache_ttl", minAuthNegativeTTL)
	}
	if a.CacheSize < 1 || a.CacheSize > maxAuthCacheSize {
		return fmt.Errorf("auth.cache_size must be in 1-%d, got %d", maxAuthCacheSize, a.CacheSize)
	}
	if a.StaleGrace < a.CacheTTL || a.StaleGrace > maxAuthStaleGrace {
		return fmt.Errorf("auth.stale_grace must be at least auth.cache_ttl and at most %v", maxAuthStaleGrace)
	}
	return nil
}
