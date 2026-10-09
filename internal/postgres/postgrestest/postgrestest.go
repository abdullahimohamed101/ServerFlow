// Package postgrestest gives tests a throwaway database. Tests that need PostgreSQL call
// NewDSN; it skips them unless SERVERFLOW_TEST_POSTGRES_DSN names a server, so a plain
// `go test ./...` needs no database. CI sets the variable and fails the build if any test
// printed SkipMessage.
package postgrestest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// EnvDSN names the environment variable holding a superuser DSN for a disposable server.
const EnvDSN = "SERVERFLOW_TEST_POSTGRES_DSN"

// SkipMessage is printed when a database test is skipped; CI greps for it.
const SkipMessage = "SERVERFLOW_TEST_POSTGRES_DSN is not set; skipping the PostgreSQL test"

// AdminDSN returns the server DSN from the environment, skipping the test if unset. It
// refuses to run against a non-loopback host: these tests create and drop databases.
func AdminDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(EnvDSN)
	if dsn == "" {
		t.Skip(SkipMessage)
	}
	if err := checkLoopback(dsn); err != nil {
		t.Fatalf("%s: %v", EnvDSN, err)
	}
	return dsn
}

// checkLoopback asks the driver which hosts the DSN resolves to (so a ?host= query parameter, PGHOST
// and comma-separated hosts are all seen) and refuses any that is not this machine.
func checkLoopback(dsn string) error {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return errors.New("is not a valid PostgreSQL connection string")
	}
	hosts := []string{cfg.Host}
	for _, fb := range cfg.Fallbacks {
		hosts = append(hosts, fb.Host)
	}
	for _, h := range hosts {
		if h == "" || strings.HasPrefix(h, "/") || h == "localhost" {
			continue
		}
		if ip := net.ParseIP(h); ip != nil && ip.IsLoopback() {
			continue
		}
		return errors.New("must resolve to loopback hosts only: these tests create and drop databases")
	}
	return nil
}

// NewDSN creates an empty database with a random name on the test server and returns a DSN
// for it. The database is dropped when the test ends.
func NewDSN(t testing.TB) string {
	t.Helper()
	admin := AdminDSN(t)
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := "sf_t_" + hex.EncodeToString(b[:])

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect to the test server: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	// name is generated above from hex digits, so it is safe as an identifier.
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		conn, err := pgx.Connect(c, admin)
		if err != nil {
			t.Logf("cleanup: connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(c) }()
		if _, err := conn.Exec(c, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Logf("cleanup: drop %s: %v", name, err)
		}
	})

	u, _ := url.Parse(admin)
	u.Path = "/" + name
	return u.String()
}

// Exec runs SQL as the test server's superuser against the database dsn names, for tests that need
// to plant data the application's own validation would refuse.
func Exec(t testing.TB, dsn, sql string, args ...any) {
	t.Helper()
	if err := checkLoopback(dsn); err != nil {
		t.Fatalf("refusing to run SQL: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

// CreateRole creates a login role with a random password, lets it connect to the database that
// adminDSN names, runs each grant (SQL in which {role} stands for the new role's name), and returns
// a DSN for that role. The role is dropped when the test ends.
//
// Roles need passwords: CI's server uses password authentication, and a test that connects as a
// passwordless role passes against a trust-auth server and fails there. attrs is appended to
// CREATE ROLE, for example "CONNECTION LIMIT 1".
func CreateRole(t testing.TB, adminDSN, attrs string, grants ...string) string {
	t.Helper()
	cfg, err := pgconn.ParseConfig(adminDSN)
	if err != nil {
		t.Fatal("not a valid PostgreSQL connection string")
	}
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	role := "sf_r_" + hex.EncodeToString(b[:6])
	password := hex.EncodeToString(b[6:]) + "Pw"
	// role and password are generated from hex digits, so they are safe in these statements.
	Exec(t, adminDSN, "CREATE ROLE "+role+" LOGIN PASSWORD '"+password+"' "+attrs)
	Exec(t, adminDSN, "GRANT CONNECT ON DATABASE "+cfg.Database+" TO "+role)
	t.Cleanup(func() {
		Exec(t, adminDSN, "DROP OWNED BY "+role)
		Exec(t, adminDSN, "DROP ROLE "+role)
	})
	for _, g := range grants {
		Exec(t, adminDSN, strings.ReplaceAll(g, "{role}", role))
	}
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal("the test DSN must be a URL")
	}
	u.User = url.UserPassword(role, password)
	return u.String()
}
