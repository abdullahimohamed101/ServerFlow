// Package redis is the only package that imports the Redis driver (github.com/redis/go-redis/v9). It
// builds a client from configuration, refuses an unsafe transport by asking the driver what the
// configuration resolves to, keeps the password out of logs and errors, and tracks Redis health
// (a short backoff after a failure, one probe at a time, one log line per outage) so that callers
// on the request path pay one timeout per backoff interval rather than one per request.
package redis

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// DefaultPoolSize is the connection pool size when Config.PoolSize is zero.
const DefaultPoolSize = 64

// Config describes the connection. Password is a secret: it is never logged or put in an error, and
// formatting a Config prints it redacted under %v, %+v, %#v and slog.
type Config struct {
	// Address is host:port, or a redis://, rediss:// or unix:// URL that the driver parses (a URL
	// may carry a password, a database number and TLS via rediss).
	Address string
	// Password, DB and TLS override what a URL Address says when set.
	Password string
	DB       int
	TLS      bool
	// TLSConfig, if set, replaces the default TLS settings (system roots, TLS 1.2 or later). It is for
	// tests and embedding; it cannot be set from the configuration file.
	TLSConfig *tls.Config
	// Timeout bounds every command, and the dial, read, write and wait for a pooled connection.
	Timeout time.Duration
	// Backoff is how long Redis is left alone after a failure.
	Backoff time.Duration
	// PoolSize bounds the connections to Redis (default 64). Running out of them is the gateway's own congestion,
	// not a Redis outage: see ErrBusy. PoolTimeout is how long a call waits for a free connection (default Timeout).
	PoolSize    int
	PoolTimeout time.Duration
	// AllowInsecureTransport permits a remote server without TLS or without a password.
	AllowInsecureTransport bool
	// Logger receives one line when an outage starts and one when it ends; nil discards them.
	Logger *slog.Logger
	// Now is the clock for the backoff; nil means time.Now. Tests inject a fake.
	Now func() time.Time
}

// String formats the config without the password (or a URL address, which may carry one).
func (c Config) String() string {
	pw := "<unset>"
	if c.Password != "" {
		pw = "<redacted>"
	}
	return fmt.Sprintf("{address:%s password:%s db:%d tls:%t timeout:%v backoff:%v pool_size:%d allow_insecure_transport:%t}",
		redactAddress(c.Address), pw, c.DB, c.TLS, c.Timeout, c.Backoff, c.PoolSize, c.AllowInsecureTransport)
}

// GoString implements fmt.GoStringer.
func (c Config) GoString() string { return "redis.Config" + c.String() }

// LogValue implements slog.LogValuer.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// redactAddress hides an address that may embed credentials (a URL).
func redactAddress(a string) string {
	if strings.Contains(a, "@") || strings.Contains(a, "://") {
		return "<redacted>"
	}
	return a
}

// errBadAddress is what an unparsable address reports. It never contains the address: a URL may carry a
// password.
var errBadAddress = errors.New("redis.address could not be parsed; expected host:port or a redis://, rediss:// or unix:// URL")

// options turns the configuration into the driver's options. A URL is parsed by the driver
// (goredis.ParseURL); a plain host:port is used as is. Nothing here connects.
func (c Config) options() (*goredis.Options, error) {
	var opts *goredis.Options
	switch {
	case c.Address == "":
		return nil, errors.New("redis.address must not be empty")
	case strings.Contains(c.Address, "://"):
		o, err := goredis.ParseURL(c.Address)
		if err != nil {
			return nil, errBadAddress
		}
		opts = o
	default:
		if _, _, err := net.SplitHostPort(c.Address); err != nil {
			return nil, errBadAddress
		}
		opts = &goredis.Options{Network: "tcp", Addr: c.Address}
	}
	if c.Password != "" {
		opts.Password = c.Password
	}
	if c.DB != 0 {
		opts.DB = c.DB
	}
	if c.TLSConfig != nil {
		opts.TLSConfig = c.TLSConfig
	} else if c.TLS && opts.TLSConfig == nil {
		host, _, _ := net.SplitHostPort(opts.Addr)
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	}
	if opts.TLSConfig != nil && opts.TLSConfig.MinVersion < tls.VersionTLS12 {
		cp := opts.TLSConfig.Clone()
		cp.MinVersion = tls.VersionTLS12
		opts.TLSConfig = cp
	}
	// One attempt per command, bounded by the timeout, and no driver retries: a script that Redis already applied
	// must not be sent again after a connection reset (an acquire would be charged twice). The backoff handles failure.
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 50 * time.Millisecond
	}
	opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout, opts.PoolTimeout = timeout, timeout, timeout, timeout
	if c.PoolTimeout > 0 {
		opts.PoolTimeout = c.PoolTimeout
	}
	opts.PoolSize = c.PoolSize
	if opts.PoolSize <= 0 {
		opts.PoolSize = DefaultPoolSize
	}
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	opts.DisableIdentity = true
	return opts, nil
}

// CheckTransport refuses a configuration that would send the password or traffic in clear text to,
// or talk without authentication to, a host that is not this machine, unless AllowInsecureTransport is
// set. It asks the driver what the address resolves to (a URL's scheme, host, password and TLS are all
// parsed by goredis.ParseURL, never by hand), so a rediss:// URL satisfies the TLS requirement exactly as
// redis.tls does. Loopback addresses, localhost and unix sockets are exempt. Errors never contain the
// address or the password.
func CheckTransport(c Config) error {
	opts, err := c.options()
	if err != nil {
		return err
	}
	if c.AllowInsecureTransport || opts.Network == "unix" {
		return nil
	}
	host, _, err := net.SplitHostPort(opts.Addr)
	if err != nil {
		return errBadAddress
	}
	if isLocalHost(host) {
		return nil
	}
	if opts.TLSConfig == nil {
		return errors.New("redis would connect to a host that is not this machine without TLS; set redis.tls (or use a rediss:// address), " +
			"or set redis.allow_insecure_transport if a trusted network carries the traffic")
	}
	if opts.Password == "" {
		return errors.New("redis would connect to a host that is not this machine without a password; set redis.password (SERVERFLOW_REDIS_PASSWORD), " +
			"or set redis.allow_insecure_transport if a trusted network protects it")
	}
	return nil
}

// isLocalHost reports whether host is this machine: localhost or a loopback IP.
func isLocalHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}
