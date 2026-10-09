package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/postgres/postgrestest"
)

// tcpFront forwards to a target and can be switched to a black hole that accepts connections and
// never answers, like a database host whose packets are being dropped after the handshake.
type tcpFront struct {
	target string
	addr   string
	mu     sync.Mutex
	ln     net.Listener
	conns  []net.Conn
}

func newTCPFront(t *testing.T, target string) *tcpFront {
	t.Helper()
	f := &tcpFront{target: target}
	f.listen(t, "127.0.0.1:0", false)
	t.Cleanup(f.stop)
	return f
}

func (f *tcpFront) listen(t *testing.T, addr string, blackhole bool) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.ln, f.addr = ln, ln.Addr().String()
	f.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			f.mu.Unlock()
			if blackhole {
				continue // hold it open, read and write nothing
			}
			up, err := net.Dial("tcp", f.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			f.mu.Lock()
			f.conns = append(f.conns, up)
			f.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
}

func (f *tcpFront) stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ln != nil {
		_ = f.ln.Close()
		f.ln = nil
	}
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

func (f *tcpFront) blackhole(t *testing.T) {
	addr := f.addr
	f.stop()
	f.listen(t, addr, true)
}

func dsnVia(t *testing.T, dsn, hostport string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = hostport
	return u.String()
}

func isPlainFailure(err error) bool {
	return err != nil && !errors.Is(err, auth.ErrBusy) && !errors.Is(err, auth.ErrBadRecord) && !errors.Is(err, auth.ErrNotFound)
}

// A database that accepts connections and never answers is an outage, not "busy": the authenticator
// then backs off, so later uncached keys are refused at once instead of each waiting out the timeout.
func TestBlackholedDatabaseIsAnOutageAndLaterKeysFailFast(t *testing.T) {
	direct := postgrestest.NewDSN(t)
	u, _ := url.Parse(direct)
	front := newTCPFront(t, u.Host)
	ctx := context.Background()
	s, err := Open(ctx, Config{DSN: dsnVia(t, direct, front.addr), MaxConns: 10, ConnectTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}

	front.blackhole(t)
	s.pool.Reset() // drop the idle connections so the next lookup has to connect

	c, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := s.LookupKey(c, "abcdef01"); !isPlainFailure(err) {
		t.Fatalf("a connection that never completes with spare pool capacity must be an outage, got %v", err)
	}

	a := auth.New(s, auth.Config{LookupTimeout: 300 * time.Millisecond})
	var elapsed []time.Duration
	for i := 0; i < 4; i++ {
		k, _, _ := auth.GenerateKey()
		start := time.Now()
		if _, err := a.Authenticate(ctx, k); !errors.Is(err, auth.ErrUnavailable) {
			t.Fatalf("key %d: %v", i, err)
		}
		elapsed = append(elapsed, time.Since(start))
	}
	if elapsed[0] < 250*time.Millisecond {
		t.Fatalf("the first key should have waited for the lookup timeout, took %v", elapsed[0])
	}
	for i, d := range elapsed[1:] {
		if d > 150*time.Millisecond {
			t.Fatalf("key %d took %v: after the first failure the backoff must refuse at once (all: %v)", i+1, d, elapsed)
		}
	}
}

func randomRole() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "sf_r_" + hex.EncodeToString(b[:])
}

// A server that has run out of connection slots (SQLSTATE 53300) is busy, not an outage, so it does not
// start a backoff or flap the unavailable/recovered log.
func TestTooManyConnectionsIsBusyNotAnOutage(t *testing.T) {
	admin := postgrestest.NewDSN(t)
	ctx := context.Background()
	role := randomRole()
	postgrestest.Exec(t, admin, "CREATE ROLE "+role+" LOGIN CONNECTION LIMIT 1")
	postgrestest.Exec(t, admin, "GRANT CONNECT ON DATABASE "+dbName(t, admin)+" TO "+role)
	t.Cleanup(func() {
		postgrestest.Exec(t, admin, "DROP OWNED BY "+role)
		postgrestest.Exec(t, admin, "DROP ROLE "+role)
	})

	u, _ := url.Parse(admin)
	u.User = url.User(role)
	s, err := Open(ctx, Config{DSN: u.String(), MaxConns: 5, ConnectTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	held, err := s.pool.Acquire(ctx) // the role's only slot
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err = s.LookupKey(c, "abcdef01")
	if !errors.Is(err, auth.ErrBusy) {
		t.Fatalf("too many connections must be busy, got %v", err)
	}
}

func dbName(t *testing.T, dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return u.Path[1:]
}

// If the query fails while its rows are being read, that is a failure and never "no such key": a
// negative cache entry would turn a transient fault into 401s for a valid key.
func TestFailureWhileReadingRowsIsNotKeyNotFound(t *testing.T) {
	admin := postgrestest.NewDSN(t)
	ctx := context.Background()
	st, err := Open(ctx, Config{DSN: admin, MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if _, err := st.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateTenant(ctx, TenantInput{Name: "acme", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	_, prefix, hash := auth.GenerateKey()
	if _, err := st.CreateKey(ctx, "acme", "", nil, prefix, hash); err != nil {
		t.Fatal(err)
	}
	// A non-superuser role subject to a row-level policy that raises an error when a row is evaluated.
	role := randomRole()
	db := dbName(t, admin)
	postgrestest.Exec(t, admin, "CREATE ROLE "+role+" LOGIN")
	postgrestest.Exec(t, admin, "GRANT CONNECT ON DATABASE "+db+" TO "+role)
	postgrestest.Exec(t, admin, "GRANT SELECT ON api_keys, tenants TO "+role)
	postgrestest.Exec(t, admin, "ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY")
	postgrestest.Exec(t, admin, "CREATE POLICY boom ON api_keys FOR SELECT USING (1 / (length(prefix) - 8) = 0)")
	t.Cleanup(func() {
		postgrestest.Exec(t, admin, "DROP OWNED BY "+role)
		postgrestest.Exec(t, admin, "DROP ROLE "+role)
	})

	u, _ := url.Parse(admin)
	u.User = url.User(role)
	ro, err := Open(ctx, Config{DSN: u.String(), MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ro.Close)
	_, err = ro.LookupKey(ctx, prefix)
	if err == nil || errors.Is(err, auth.ErrNotFound) || errors.Is(err, auth.ErrBadRecord) || errors.Is(err, auth.ErrBusy) {
		t.Fatalf("an error raised while the rows are read must be an outage-class failure, got %v", err)
	}
}
