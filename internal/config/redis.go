package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"
)

// Redis failure modes (redis.on_failure) and rate limit modes (rate_limit.mode).
const (
	RedisFailClosed = "closed"
	RedisFailOpen   = "open"

	RateLimitOff      = "off"
	RateLimitRequired = "required"
)

// RedisConfig configures the shared ephemeral state store (rate limits, request metadata).
// Password is a secret: it is never logged or echoed in an error, and formatting a RedisConfig prints it
// redacted. Address is host:port or a redis://, rediss:// or unix:// URL (a URL may carry a password, so
// it is redacted too). Timeout bounds every Redis call; after a failure Redis is left alone for Backoff.
// OnFailure says what rate limiting does when Redis cannot answer: "closed" refuses requests that need a
// check (503), "open" admits them unchecked. RequestMetadata enables best-effort request:{id} records kept
// for RequestMetadataTTL. AllowInsecureTransport permits a remote server without TLS or a password.
type RedisConfig struct {
	Address                string        `yaml:"address"`
	Password               string        `yaml:"password"`
	DB                     int           `yaml:"db"`
	TLS                    bool          `yaml:"tls"`
	Timeout                time.Duration `yaml:"timeout"`
	Backoff                time.Duration `yaml:"backoff"`
	PoolSize               int           `yaml:"pool_size"`
	OnFailure              string        `yaml:"on_failure"`
	RequestMetadata        bool          `yaml:"request_metadata"`
	RequestMetadataTTL     time.Duration `yaml:"request_metadata_ttl"`
	AllowInsecureTransport bool          `yaml:"allow_insecure_transport"`
}

// String formats the config without the password. GoString and LogValue do the same.
func (r RedisConfig) String() string {
	pw := "<unset>"
	if r.Password != "" {
		pw = "<redacted>"
	}
	addr := r.Address
	if strings.Contains(addr, "@") || strings.Contains(addr, "://") {
		addr = "<redacted>"
	}
	return fmt.Sprintf("{address:%s password:%s db:%d tls:%t timeout:%v backoff:%v pool_size:%d on_failure:%s request_metadata:%t allow_insecure_transport:%t}",
		addr, pw, r.DB, r.TLS, r.Timeout, r.Backoff, r.PoolSize, r.OnFailure, r.RequestMetadata, r.AllowInsecureTransport)
}

// GoString implements fmt.GoStringer.
func (r RedisConfig) GoString() string { return "config.RedisConfig" + r.String() }

// LogValue implements slog.LogValuer.
func (r RedisConfig) LogValue() slog.Value { return slog.StringValue(r.String()) }

// MarshalJSON redacts the secrets, so logging a whole Config as JSON cannot leak them.
func (r RedisConfig) MarshalJSON() ([]byte, error) {
	type plain RedisConfig // no methods, so no recursion
	p := plain(r)
	if p.Password != "" {
		p.Password = "<redacted>"
	}
	if strings.Contains(p.Address, "@") || strings.Contains(p.Address, "://") {
		p.Address = "<redacted>"
	}
	return json.Marshal(p)
}

// Bounds for Redis and rate limiting.
const (
	maxRedisTimeout     = 10 * time.Second
	minRedisBackoff     = 10 * time.Millisecond
	maxRedisBackoff     = time.Minute
	maxRedisDB          = 1023
	maxRedisPoolSize    = 1000
	maxMetadataTTL      = 7 * 24 * time.Hour
	maxBurstSeconds     = 3600
	minLeaseTTL         = 10 * time.Second
	maxLeaseTTL         = time.Hour
	maxLocalLeases      = 10_000_000
	maxModelCaps        = 1000
	maxModelCapQuota    = 1_000_000_000
	maxModelNameLen     = 128
	minLeaseTTLTimeouts = 3 // lease_ttl must be at least this many redis.timeouts (a renewal happens every third of it)
)

// validate checks the Redis settings. It never echoes the address or the password.
func (r *RedisConfig) validate() error {
	if r.Address == "" {
		return fmt.Errorf("redis.address must not be empty")
	}
	if i := strings.Index(r.Address, "://"); i >= 0 {
		switch r.Address[:i] {
		case "redis", "rediss", "unix":
		default:
			return fmt.Errorf("redis.address must be host:port or a redis://, rediss:// or unix:// URL")
		}
	} else if _, port, err := net.SplitHostPort(r.Address); err != nil || port == "" {
		return fmt.Errorf("redis.address must be host:port or a redis://, rediss:// or unix:// URL")
	}
	if r.DB < 0 || r.DB > maxRedisDB {
		return fmt.Errorf("redis.db must be in 0-%d, got %d", maxRedisDB, r.DB)
	}
	if r.Timeout <= 0 || r.Timeout > maxRedisTimeout {
		return fmt.Errorf("redis.timeout must be > 0 and at most %v", maxRedisTimeout)
	}
	if r.Backoff < minRedisBackoff || r.Backoff > maxRedisBackoff {
		return fmt.Errorf("redis.backoff must be between %v and %v", minRedisBackoff, maxRedisBackoff)
	}
	switch r.OnFailure {
	case RedisFailClosed, RedisFailOpen:
	default:
		return fmt.Errorf("redis.on_failure must be %q or %q", RedisFailClosed, RedisFailOpen)
	}
	if r.PoolSize < 1 || r.PoolSize > maxRedisPoolSize {
		return fmt.Errorf("redis.pool_size must be in 1-%d, got %d", maxRedisPoolSize, r.PoolSize)
	}
	if r.RequestMetadataTTL <= 0 || r.RequestMetadataTTL > maxMetadataTTL {
		return fmt.Errorf("redis.request_metadata_ttl must be > 0 and at most %v", maxMetadataTTL)
	}
	return nil
}

// redisUsed reports whether any configured feature talks to Redis.
func (c *Config) redisUsed() bool {
	return c.RateLimit.Mode == RateLimitRequired || c.Redis.RequestMetadata
}
