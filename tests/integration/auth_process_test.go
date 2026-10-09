package integration

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/postgres/postgrestest"
)

// Process-level tests of API key authentication: the real admin and gateway binaries, a real
// PostgreSQL (SERVERFLOW_TEST_POSTGRES_DSN) and a real mock worker.

// dbProxy forwards TCP to the test database and can be cut and restored, standing in for a
// database outage without touching the shared server.
type dbProxy struct {
	target string
	addr   string
	mu     sync.Mutex
	ln     net.Listener
	conns  []net.Conn
}

func newDBProxy(t *testing.T, target string) *dbProxy {
	t.Helper()
	p := &dbProxy{target: target}
	p.listen(t, "127.0.0.1:0")
	t.Cleanup(p.cut)
	return p
}

func (p *dbProxy) listen(t *testing.T, addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.ln, p.addr = ln, ln.Addr().String()
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
}

// cut stops accepting and drops every connection.
func (p *dbProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *dbProxy) restore(t *testing.T) { p.listen(t, p.addr) }

// withHostAndPassword returns dsn pointed at hostport with the given password for its user.
func withHostAndPassword(t *testing.T, dsn, hostport, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = hostport
	u.User = url.UserPassword(u.User.Username(), password)
	return u.String()
}

func mustAdmin(t *testing.T, dsn string, args ...string) string { return execAdmin(t, dsn, args...) }

// execAdmin runs the admin binary and returns stdout only (stderr carries warnings).
func execAdmin(t *testing.T, dsn string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, binary(t, "admin"), args...)
	c.Env = append(os.Environ(), "SERVERFLOW_POSTGRES_DSN="+dsn)
	out, err := c.Output()
	if err != nil {
		t.Fatalf("admin %v: %v (%s)", args, err, out)
	}
	return string(out)
}

func authGet(t *testing.T, gw, path, key string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "http://"+gw+path, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return doReq(t, req)
}

func authChat(t *testing.T, gw, key string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+gw+"/v1/chat/completions", strings.NewReader(chat(false, "hi")))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return doReq(t, req)
}

func doReq(t *testing.T, req *http.Request) (*http.Response, string) {
	t.Helper()
	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func gatewayAuthEnv(gwAddr, dsn string, extra ...string) []string {
	_, port, _ := strings.Cut(gwAddr, ":")
	return append([]string{
		"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model, "SERVERFLOW_AUTH_MODE=required",
		"SERVERFLOW_POSTGRES_DSN=" + dsn, "SERVERFLOW_AUTH_CACHE_TTL=1s", "SERVERFLOW_AUTH_NEGATIVE_TTL=500ms", "SERVERFLOW_AUTH_STALE_GRACE=3s",
		"SERVERFLOW_POSTGRES_CONNECT_TIMEOUT=2s",
	}, extra...)
}

func TestProcessAuthEndToEnd(t *testing.T) {
	admDSN := postgrestest.NewDSN(t)
	u, _ := url.Parse(admDSN)
	proxy := newDBProxy(t, u.Host)

	mustAdmin(t, admDSN, "migrate", "up")
	// The gateway connects as the least-privilege role the operations guide describes (SELECT on
	// api_keys, tenants and schema_migrations, UPDATE of last_used_at), with a real password, as it
	// must against any password-authenticated server.
	roleDSN := postgrestest.CreateRole(t, admDSN, "",
		"GRANT SELECT ON api_keys, tenants, schema_migrations TO {role}",
		"GRANT UPDATE (last_used_at) ON api_keys TO {role}")
	ru, err := url.Parse(roleDSN)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := ru.User.Password()
	gwDSN := withHostAndPassword(t, roleDSN, proxy.addr, password)
	mustAdmin(t, admDSN, "tenant", "create", "acme", "--models", model)
	mustAdmin(t, admDSN, "tenant", "create", "restricted", "--models", "some-other-model")
	mustAdmin(t, admDSN, "tenant", "create", "frozen")
	good := strings.TrimSpace(execAdmin(t, admDSN, "key", "create", "--tenant", "acme"))
	toRevoke := strings.TrimSpace(execAdmin(t, admDSN, "key", "create", "--tenant", "acme"))
	restricted := strings.TrimSpace(execAdmin(t, admDSN, "key", "create", "--tenant", "restricted"))
	frozen := strings.TrimSpace(execAdmin(t, admDSN, "key", "create", "--tenant", "frozen"))
	expiring := strings.TrimSpace(execAdmin(t, admDSN, "key", "create", "--tenant", "acme", "--expires", "2s"))

	mockAddr := freePort(t)
	startMockProc(t, mockAddr)
	gwAddr := freePort(t)
	gw := startProc(t, "gateway", "gateway starting", gatewayAuthEnv(gwAddr, gwDSN, "SERVERFLOW_GATEWAY_UPSTREAM_URL=http://"+mockAddr))
	waitFor(t, 10*time.Second, "mock worker ready", func() bool {
		r, _ := authGet(t, gwAddr, "/readyz", "")
		return r.StatusCode == 200
	})

	// Open endpoints, and 401 for every kind of missing or bad key.
	if r, _ := authGet(t, gwAddr, "/healthz", ""); r.StatusCode != 200 {
		t.Fatalf("/healthz %d", r.StatusCode)
	}
	if r, _ := authGet(t, gwAddr, "/metrics", ""); r.StatusCode != 200 {
		t.Fatalf("/metrics %d", r.StatusCode)
	}
	r1, b1 := authChat(t, gwAddr, "")
	r2, b2 := authChat(t, gwAddr, "sf_deadbeef_"+strings.Repeat("A", 43))
	if r1.StatusCode != 401 || r2.StatusCode != 401 || b1 != b2 || r1.Header.Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("missing vs unknown key: %d %s / %d %s", r1.StatusCode, b1, r2.StatusCode, b2)
	}

	// A valid key works, and a tenant is limited to its models.
	if r, b := authChat(t, gwAddr, good); r.StatusCode != 200 {
		t.Fatalf("valid key: %d %s", r.StatusCode, b)
	}
	if r, b := authChat(t, gwAddr, restricted); r.StatusCode != 403 {
		t.Fatalf("model not allowed: %d %s", r.StatusCode, b)
	}
	if _, b := authGet(t, gwAddr, "/v1/models", restricted); strings.Contains(b, model) {
		t.Fatalf("models list not filtered: %s", b)
	}
	if _, b := authGet(t, gwAddr, "/v1/models", good); !strings.Contains(b, model) {
		t.Fatalf("models list: %s", b)
	}

	// Revocation: works before the database write, refused within cache_ttl after it.
	if r, _ := authChat(t, gwAddr, toRevoke); r.StatusCode != 200 {
		t.Fatalf("before revoke: %d", r.StatusCode)
	}
	mustAdmin(t, admDSN, "key", "revoke", toRevoke[3:11])
	waitFor(t, 3*time.Second, "revocation within cache_ttl (1s)", func() bool {
		r, _ := authChat(t, gwAddr, toRevoke)
		return r.StatusCode == 401
	})

	// Suspension: 403 for the holder of a valid key.
	if r, _ := authChat(t, gwAddr, frozen); r.StatusCode != 200 {
		t.Fatalf("before suspend: %d", r.StatusCode)
	}
	mustAdmin(t, admDSN, "tenant", "suspend", "frozen")
	waitFor(t, 3*time.Second, "suspension within cache_ttl", func() bool {
		r, _ := authChat(t, gwAddr, frozen)
		return r.StatusCode == 403
	})

	// Expiry is honoured at the instant it passes, with no new lookup needed.
	if r, _ := authChat(t, gwAddr, expiring); r.StatusCode != 200 && r.StatusCode != 401 {
		t.Fatalf("expiring key: %d", r.StatusCode)
	}
	waitFor(t, 5*time.Second, "expired key refused", func() bool {
		r, _ := authChat(t, gwAddr, expiring)
		return r.StatusCode == 401
	})

	// Outage: cached key keeps working for a while, an uncached one gets 503 at once.
	uncached := strings.TrimSpace(execAdmin(t, admDSN, "key", "create", "--tenant", "acme"))
	if r, _ := authChat(t, gwAddr, good); r.StatusCode != 200 {
		t.Fatalf("warm: %d", r.StatusCode)
	}
	proxy.cut()
	r, b := authChat(t, gwAddr, uncached)
	if r.StatusCode != 503 || r.Header.Get("Retry-After") == "" || !strings.Contains(b, "AUTH_UNAVAILABLE") {
		t.Fatalf("uncached key during an outage: %d %v %s", r.StatusCode, r.Header, b)
	}
	if r, _ := authChat(t, gwAddr, good); r.StatusCode != 200 {
		t.Fatalf("cached key during an outage: %d", r.StatusCode)
	}
	// This one is a real wall-clock wait: the gateway is a separate binary with its own clock, so
	// there is nothing to inject. cache_ttl is 1s and stale_grace 3s, so 1.3s is past the TTL and inside the grace.
	time.Sleep(1300 * time.Millisecond)
	if r, _ := authChat(t, gwAddr, good); r.StatusCode != 200 {
		t.Fatalf("stale cached key inside the grace period: %d", r.StatusCode)
	}
	waitFor(t, 8*time.Second, "cached key refused after stale_grace", func() bool {
		r, _ := authChat(t, gwAddr, good)
		return r.StatusCode == 503
	})
	proxy.restore(t)
	waitFor(t, 8*time.Second, "recovery after the database returns", func() bool {
		r, _ := authChat(t, gwAddr, uncached)
		return r.StatusCode == 200
	})

	logs := gw.stderr.String()
	if strings.Count(logs, "key store unavailable") != 1 {
		t.Errorf("the outage must be logged once:\n%s", logs)
	}
	for _, secret := range []string{good, toRevoke, restricted, frozen, expiring, uncached, password, good[len("sf_")+9:]} {
		if strings.Contains(logs, secret) {
			t.Fatalf("gateway log contains a secret (%d chars, starts %q)", len(secret), secret[:4])
		}
	}
	if !strings.Contains(logs, `"tenant_id"`) {
		t.Error("no tenant_id in the gateway log")
	}
}

func freePortNumber(t *testing.T) string {
	_, port, _ := strings.Cut(freePort(t), ":")
	return port
}

func TestProcessGatewayRequiredModeFailsFast(t *testing.T) {
	const password = "unreachable-db-pw"
	t.Run("database unreachable", func(t *testing.T) {
		dsn := "postgres://app:" + password + "@127.0.0.1:1/x?sslmode=disable"
		code, out := runBin(t, "gateway", []string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_AUTH_MODE=required", "SERVERFLOW_POSTGRES_DSN=" + dsn, "SERVERFLOW_POSTGRES_CONNECT_TIMEOUT=1s"})
		if code != 1 || !strings.Contains(out, "database") || strings.Contains(out, password) {
			t.Fatalf("exit %d: %s", code, out)
		}
	})
	t.Run("not migrated", func(t *testing.T) {
		dsn := postgrestest.NewDSN(t)
		code, out := runBin(t, "gateway", []string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_AUTH_MODE=required", "SERVERFLOW_POSTGRES_DSN=" + dsn})
		if code != 1 || !strings.Contains(out, "migrate up") {
			t.Fatalf("exit %d: %s", code, out)
		}
	})
	t.Run("insecure remote DSN", func(t *testing.T) {
		dsn := "postgres://app:" + password + "@db.example.com/x?sslmode=disable"
		code, out := runBin(t, "gateway", []string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_AUTH_MODE=required", "SERVERFLOW_POSTGRES_DSN=" + dsn})
		if code != 1 || !strings.Contains(out, "TLS") || strings.Contains(out, password) || strings.Contains(out, "db.example.com") {
			t.Fatalf("exit %d: %s", code, out)
		}
	})
	t.Run("typo in mode", func(t *testing.T) {
		code, out := runBin(t, "gateway", []string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_AUTH_MODE=requierd"})
		if code != 1 || !strings.Contains(out, "auth.mode") {
			t.Fatalf("exit %d: %s", code, out)
		}
	})
}

func TestProcessAuthOffNeverTouchesTheDatabase(t *testing.T) {
	mockAddr := freePort(t)
	startMockProc(t, mockAddr)
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	// The DSN points nowhere; with auth off it must not matter.
	gwp := startProc(t, "gateway", "gateway starting", []string{"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model,
		"SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + mockAddr, "SERVERFLOW_POSTGRES_DSN=postgres://u:p@127.0.0.1:1/x?sslmode=disable"})
	waitFor(t, 10*time.Second, "keyless request served", func() bool {
		r, _ := authChat(t, gwAddr, "")
		return r.StatusCode == 200
	})
	if !strings.Contains(gwp.stderr.String(), "authentication is OFF") {
		t.Fatalf("a gateway with authentication off must say so at startup:\n%s", gwp.stderr.String())
	}
}
