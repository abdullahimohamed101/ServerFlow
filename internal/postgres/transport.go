package postgres

import (
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// CheckTransport refuses a DSN that could send credentials or data over a connection that is not
// encrypted to a host other than this machine, unless allowInsecure is set. It asks the driver what
// the DSN resolves to (so PG* environment variables, hostaddr, comma-separated hosts and
// fallbacks are all honoured) instead of parsing the string itself.
//
// Every connection attempt the driver would make must use TLS (sslmode require, verify-ca or
// verify-full). sslmode disable, allow and prefer, or no sslmode at all (which means prefer), each
// leave at least one attempt in plain text: prefer and allow fall back to it silently, which an
// attacker on the path can force. Loopback addresses, "localhost" and unix sockets are exempt.
//
// Errors never contain the DSN.
func CheckTransport(dsn string, allowInsecure bool) error {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return errors.New("postgres.dsn could not be parsed; expected postgres://user:password@host:port/database")
	}
	if allowInsecure {
		return nil
	}
	type attempt struct {
		host string
		tls  bool
	}
	attempts := []attempt{{cfg.Host, cfg.TLSConfig != nil}}
	for _, fb := range cfg.Fallbacks {
		attempts = append(attempts, attempt{fb.Host, fb.TLSConfig != nil})
	}
	for _, a := range attempts {
		if !a.tls && !isLocalHost(a.host) {
			return errors.New("postgres.dsn would connect to a host that is not this machine without requiring TLS; " +
				"use sslmode=require, verify-ca or verify-full (an unset sslmode, allow, prefer and disable can all fall back to plain text), " +
				"or set postgres.allow_insecure_transport if a trusted network carries the traffic")
		}
	}
	return nil
}

// isLocalHost reports whether host is this machine: a unix socket directory, localhost, or a loopback IP.
func isLocalHost(host string) bool {
	if host == "" || strings.HasPrefix(host, "/") || strings.HasPrefix(host, "@") {
		// An empty host means the driver's default, which is a local socket or localhost.
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}
