package config

import (
	"fmt"
	"log/slog"
	"time"
)

// PostgresConfig configures the durable metadata store. DSN is a secret (it usually holds a
// password): it is never logged or echoed in an error, and formatting a PostgresConfig prints
// it redacted. MaxConns bounds the connection pool and ConnectTimeout how long a connection
// attempt may take. AllowInsecureTransport permits a DSN that does not require TLS (see
// postgres.CheckTransport) to a host that is not this machine; leave it off unless a trusted private
// network carries the traffic.
type PostgresConfig struct {
	DSN                    string        `yaml:"dsn"`
	MaxConns               int           `yaml:"max_conns"`
	ConnectTimeout         time.Duration `yaml:"connect_timeout"`
	AllowInsecureTransport bool          `yaml:"allow_insecure_transport"`
}

// String formats the config without the DSN. GoString and LogValue do the same, so neither
// fmt's %v and %#v nor structured logging can print the password.
func (p PostgresConfig) String() string {
	return fmt.Sprintf("{dsn:%s max_conns:%d connect_timeout:%v allow_insecure_transport:%t}", redactedDSN(p.DSN), p.MaxConns, p.ConnectTimeout, p.AllowInsecureTransport)
}

// GoString implements fmt.GoStringer.
func (p PostgresConfig) GoString() string { return "config.PostgresConfig" + p.String() }

// LogValue implements slog.LogValuer.
func (p PostgresConfig) LogValue() slog.Value { return slog.StringValue(p.String()) }

func redactedDSN(dsn string) string {
	if dsn == "" {
		return "<unset>"
	}
	return "<redacted>"
}

// Bounds for the pool and the key cache.
const (
	maxPostgresConns   = 100
	maxConnectTimeout  = time.Minute
	maxAuthCacheSize   = 1_000_000
	maxAuthCacheTTL    = time.Hour
	maxAuthStaleGrace  = 24 * time.Hour
	minAuthNegativeTTL = time.Millisecond
	minAuthPoolConns   = 4
)

// validate checks the settings that do not depend on whether the database is used. It never
// echoes the DSN.
func (p *PostgresConfig) validate() error {
	if p.DSN == "" {
		return fmt.Errorf("postgres.dsn must not be empty")
	}
	if p.MaxConns < 1 || p.MaxConns > maxPostgresConns {
		return fmt.Errorf("postgres.max_conns must be in 1-%d, got %d", maxPostgresConns, p.MaxConns)
	}
	if p.ConnectTimeout <= 0 || p.ConnectTimeout > maxConnectTimeout {
		return fmt.Errorf("postgres.connect_timeout must be > 0 and at most %v", maxConnectTimeout)
	}
	return nil
}
