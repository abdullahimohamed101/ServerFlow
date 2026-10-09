// Package redistest gives tests a Redis. Tests that need one call Config or NewClient; they skip unless
// SERVERFLOW_TEST_REDIS_ADDR names a server, so a plain `go test ./...` needs none. CI sets the variables and
// fails the build if any test printed SkipMessage.
//
// The test server must require a password (scripts/dev-redis.sh and the CI service both do), and every
// client built here passes it. Config fails the test if the password variable is missing, so a test cannot
// quietly run against an unauthenticated server and then fail in CI.
package redistest

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"testing"
	"time"

	"serverflow/internal/redis"
)

// Environment variables naming the throwaway server.
const (
	EnvAddr     = "SERVERFLOW_TEST_REDIS_ADDR"
	EnvPassword = "SERVERFLOW_TEST_REDIS_PASSWORD"
)

// SkipMessage is printed when a Redis test is skipped; CI greps for it.
const SkipMessage = "SERVERFLOW_TEST_REDIS_ADDR is not set; skipping the Redis test"

// Addr returns the server address and password from the environment, skipping the test if no server is
// configured. It fails the test if the server is not on this machine (tests write to it) or if no password
// is given.
func Addr(t testing.TB) (addr, password string) {
	t.Helper()
	addr = os.Getenv(EnvAddr)
	if addr == "" {
		t.Skip(SkipMessage)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("%s is not host:port", EnvAddr)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		t.Fatalf("%s must be a loopback address: these tests write to the server", EnvAddr)
	}
	password = os.Getenv(EnvPassword)
	if password == "" {
		t.Fatalf("%s is not set: the test Redis must require a password, like CI's, and every test client must pass it", EnvPassword)
	}
	return addr, password
}

// Config returns a client configuration for the test server with a generous timeout (tests that need a
// tight one set it themselves).
func Config(t testing.TB) redis.Config {
	t.Helper()
	addr, pw := Addr(t)
	return redis.Config{Address: addr, Password: pw, Timeout: time.Second, Backoff: 200 * time.Millisecond}
}

// NewClient builds a client for the test server and closes it when the test ends.
func NewClient(t testing.TB) *redis.Client { return NewClientWith(t, Config(t)) }

// NewClientWith is NewClient for a configuration the test adjusted.
func NewClientWith(t testing.TB, cfg redis.Config) *redis.Client {
	t.Helper()
	if cfg.Password == "" {
		t.Fatal("redistest: a test Redis client was created without a password")
	}
	c, err := redis.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Unique returns a random name for a tenant or model, so tests sharing one server never share keys.
func Unique(prefix string) string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}
