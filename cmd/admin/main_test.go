package main

import (
	"bytes"
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"serverflow/internal/auth"
	"serverflow/internal/postgres"
	"serverflow/internal/postgres/postgrestest"
)

var keyRe = regexp.MustCompile(`^sf_[0-9a-f]{8}_[A-Za-z0-9_-]{43}$`)

type result struct {
	code     int
	out, err string
}

func admin(t *testing.T, args ...string) result {
	t.Helper()
	var o, e bytes.Buffer
	code := run(context.Background(), args, &o, &e)
	return result{code, o.String(), e.String()}
}

func openStore(t *testing.T) *postgres.Store {
	t.Helper()
	st, err := postgres.Open(context.Background(), postgres.Config{DSN: os.Getenv("SERVERFLOW_POSTGRES_DSN"), MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func setupDB(t *testing.T) {
	t.Helper()
	t.Setenv("SERVERFLOW_POSTGRES_DSN", postgrestest.NewDSN(t))
	if r := admin(t, "migrate", "up"); r.code != 0 {
		t.Fatalf("migrate up: %+v", r)
	}
}

func mustOK(t *testing.T, args ...string) result {
	t.Helper()
	r := admin(t, args...)
	if r.code != 0 {
		t.Fatalf("%v: exit %d\nstdout: %s\nstderr: %s", args, r.code, r.out, r.err)
	}
	return r
}

func TestMigrateCommands(t *testing.T) {
	t.Setenv("SERVERFLOW_POSTGRES_DSN", postgrestest.NewDSN(t))
	if r := mustOK(t, "migrate", "status"); !strings.Contains(r.out, "0001_initial") || !strings.Contains(r.out, "pending") {
		t.Fatalf("status before: %s", r.out)
	}
	if r := mustOK(t, "migrate", "up"); !strings.Contains(r.out, "applied 0001_initial") {
		t.Fatalf("up: %s", r.out)
	}
	if r := mustOK(t, "migrate", "up"); !strings.Contains(r.out, "up to date") {
		t.Fatalf("second up: %s", r.out)
	}
	if r := mustOK(t, "migrate", "status"); !strings.Contains(r.out, "applied 20") {
		t.Fatalf("status after: %s", r.out)
	}
	if r := admin(t, "migrate", "down"); r.code != 2 {
		t.Fatalf("unknown subcommand: %+v", r)
	}
}

func TestFullWorkflowAndKeyShownOnce(t *testing.T) {
	setupDB(t)
	var all strings.Builder
	rec := func(r result) result { all.WriteString(r.out + "\n--\n" + r.err + "\n=====\n"); return r }

	rec(mustOK(t, "tenant", "create", "acme", "--rpm", "60", "--tpm", "9000", "--max-concurrent", "4", "--priority", "2", "--models", "m1,m2"))
	rec(mustOK(t, "tenant", "create", "open"))
	if r := rec(mustOK(t, "tenant", "list")); !strings.Contains(r.out, "acme") || !strings.Contains(r.out, "models=m1,m2") || !strings.Contains(r.out, "models=all") {
		t.Fatalf("list: %s", r.out)
	}
	if r := rec(mustOK(t, "tenant", "show", "acme")); !strings.Contains(r.out, "rpm:          60") || !strings.Contains(r.out, "keys:         0") {
		t.Fatalf("show: %s", r.out)
	}

	kr := rec(mustOK(t, "key", "create", "--tenant", "acme", "--label", "ci", "--expires", "30d"))
	key := strings.TrimSpace(kr.out)
	if !keyRe.MatchString(key) || strings.Count(kr.out, "\n") != 1 {
		t.Fatalf("stdout must be exactly the key, got %q", kr.out)
	}
	if !strings.Contains(kr.err, "only time") || strings.Contains(kr.err, key) {
		t.Fatalf("stderr must warn and must not repeat the key: %q", kr.err)
	}
	secret := key[len("sf_")+9:]
	prefix := key[3:11]

	for _, args := range [][]string{
		{"key", "list"}, {"key", "list", "--tenant", "acme"}, {"tenant", "show", "acme"}, {"tenant", "list"},
	} {
		r := rec(mustOK(t, args...))
		if strings.Contains(r.out+r.err, secret) {
			t.Fatalf("%v prints the secret", args)
		}
	}
	list := mustOK(t, "key", "list", "--tenant", "acme").out
	if !strings.Contains(list, "prefix="+prefix) || !strings.Contains(list, "active") || !strings.Contains(list, `label="ci"`) {
		t.Fatalf("key list: %s", list)
	}
	id := strings.Fields(list)[0]

	rec(mustOK(t, "tenant", "set-quota", "acme", "--rpm", "120", "--priority", "0"))
	if r := rec(mustOK(t, "tenant", "set-models", "acme", "--none")); !strings.Contains(r.out, "models=none") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "tenant", "set-models", "acme", "--all")); !strings.Contains(r.out, "models=all") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "tenant", "set-models", "acme", "--models", "x")); !strings.Contains(r.out, "models=x") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "tenant", "suspend", "acme")); !strings.Contains(r.out, "active -> suspended") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "tenant", "activate", "acme")); !strings.Contains(r.out, "suspended -> active") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "key", "revoke", id)); !strings.Contains(r.out, "revoked key") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "key", "revoke", prefix)); !strings.Contains(r.out, "already revoked") {
		t.Fatal(r.out)
	}

	rec(mustOK(t, "model", "add", "llama-3-8b", "--display-name", "Llama", "--max-tokens", "2048", "--notes", "n"))
	if r := rec(mustOK(t, "model", "list")); !strings.Contains(r.out, "llama-3-8b") || !strings.Contains(r.out, "max_tokens=2048") {
		t.Fatal(r.out)
	}
	if r := rec(mustOK(t, "model", "disable", "llama-3-8b")); !strings.Contains(r.out, "enabled -> disabled") {
		t.Fatal(r.out)
	}
	rec(mustOK(t, "model", "enable", "llama-3-8b"))

	// The plaintext key appears exactly once in everything the CLI ever printed.
	if n := strings.Count(all.String(), key); n != 1 {
		t.Fatalf("the plaintext key appears %d times in CLI output, want exactly 1", n)
	}
	if strings.Contains(all.String(), secret[:20]) && strings.Count(all.String(), secret[:20]) != 1 {
		t.Fatal("the secret appears more than once")
	}
}

func TestCreatedKeyAuthenticatesAgainstTheStore(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	key := strings.TrimSpace(mustOK(t, "key", "create", "--tenant", "acme").out)
	// The CLI's key must verify through the real authenticator and store, i.e. the hash the CLI stored is the one auth expects.
	st := openStore(t)
	a := auth.New(st, auth.Config{})
	p, err := a.Authenticate(context.Background(), key)
	if err != nil || p.Policy.Status != auth.TenantActive {
		t.Fatalf("authenticate: %v", err)
	}
	mustOK(t, "key", "revoke", key[3:11])
	b := auth.New(st, auth.Config{})
	if _, err := b.Authenticate(context.Background(), key); err != auth.ErrRevoked {
		t.Fatalf("revoked: %v", err)
	}
}

func TestBadInputIsRefusedClearly(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	cases := []struct {
		args []string
		code int
		msg  string
	}{
		{nil, 2, "usage"},
		{[]string{"frobnicate"}, 2, "unknown command"},
		{[]string{"tenant"}, 2, "needs a subcommand"},
		{[]string{"tenant", "create"}, 2, "exactly one NAME"},
		{[]string{"tenant", "create", "bad name"}, 1, "tenant name"},
		{[]string{"tenant", "create", "ten_x"}, 1, "may not start"},
		{[]string{"tenant", "create", "acme"}, 1, "already exists"},
		{[]string{"tenant", "create", "q", "--rpm", "-1"}, 1, ""},
		{[]string{"tenant", "create", "q", "--priority", "7"}, 1, ""},
		{[]string{"tenant", "create", "q", "--bogus"}, 2, "bogus"},
		{[]string{"tenant", "show", "nobody"}, 1, "not found"},
		{[]string{"tenant", "set-quota", "acme"}, 2, "at least one"},
		{[]string{"tenant", "set-models", "acme"}, 2, "exactly one of"},
		{[]string{"tenant", "set-models", "acme", "--all", "--none"}, 2, "exactly one of"},
		{[]string{"key", "create"}, 2, "--tenant"},
		{[]string{"key", "create", "--tenant", "nobody"}, 1, "not found"},
		{[]string{"key", "create", "--tenant", "acme", "--expires", "soon"}, 2, "--expires"},
		{[]string{"key", "create", "--tenant", "acme", "--expires", "-5d"}, 2, "--expires"},
		{[]string{"key", "create", "--tenant", "acme", "--expires", "99999d"}, 2, "--expires"},
		{[]string{"key", "create", "--tenant", "acme", "--label", strings.Repeat("x", 300)}, 1, "label"},
		{[]string{"key", "revoke", "key_nope"}, 1, "not found"},
		{[]string{"model", "add", "a b"}, 1, "model name"},
		{[]string{"model", "add", "x", "--max-tokens", "0"}, 1, ""},
		{[]string{"model", "enable", "nope"}, 1, "not found"},
	}
	for _, c := range cases {
		r := admin(t, c.args...)
		if r.code != c.code || !strings.Contains(r.err, c.msg) {
			t.Errorf("%v: exit %d (want %d), stderr %q (want %q)", c.args, r.code, c.code, r.err, c.msg)
		}
		if r.code != 0 && r.out != "" {
			t.Errorf("%v: failed commands must print nothing on stdout, got %q", c.args, r.out)
		}
	}
	if r := mustOK(t, "key", "list"); r.out != "" {
		t.Errorf("a refused key create must leave no key: %q", r.out)
	}
}

func TestInjectionStringsThroughEveryArgument(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "safe")
	evil := []string{`'; DROP TABLE tenants; --`, `" OR 1=1 --`, `x'); DELETE FROM api_keys; --`, `\`, `%`}
	for _, e := range evil {
		for _, args := range [][]string{
			{"tenant", "create", e}, {"tenant", "create", "t1", "--models", e}, {"tenant", "show", e}, {"tenant", "suspend", e},
			{"tenant", "set-models", "safe", "--models", e}, {"tenant", "set-quota", e, "--rpm", "1"},
			{"key", "create", "--tenant", e}, {"key", "create", "--tenant", "safe", "--label", e}, {"key", "list", "--tenant", e}, {"key", "revoke", e},
			{"model", "add", e}, {"model", "add", "okmodel", "--notes", e}, {"model", "enable", e},
		} {
			admin(t, args...) // refused or stored verbatim; never interpreted
		}
	}
	// The schema is intact and no tenant besides "safe" (and possibly t1) was created through injection.
	r := mustOK(t, "tenant", "list")
	if !strings.Contains(r.out, "safe") || strings.Contains(r.out, "DROP") {
		t.Fatalf("tenants damaged: %s", r.out)
	}
	// Labels are free text: each hostile label was stored verbatim on tenant "safe", and nothing else happened.
	k := mustOK(t, "key", "list")
	if lines := strings.Split(strings.TrimSpace(k.out), "\n"); len(lines) != len(evil) {
		t.Fatalf("want %d keys (one per hostile label), got %d:\n%s", len(evil), len(lines), k.out)
	}
	for _, l := range strings.Split(strings.TrimSpace(k.out), "\n") {
		if f := strings.Fields(l); f[1] != "safe" {
			t.Fatalf("a key landed on the wrong tenant: %s", l)
		}
	}
}

func TestDSNPasswordNeverEchoed(t *testing.T) {
	const pw = "Sup3rS3cretPw"
	for _, dsn := range []string{
		"postgres://app:" + pw + "@127.0.0.1:1/x?sslmode=disable",
		"postgres://app:" + pw + "@db.example.com:5432/x?sslmode=disable", // refused: TLS off to a remote host
		"postgres://app:" + pw + "@%zz/x",
	} {
		t.Setenv("SERVERFLOW_POSTGRES_DSN", dsn)
		for _, args := range [][]string{{"migrate", "status"}, {"tenant", "list"}} {
			r := admin(t, args...)
			if r.code == 0 || strings.Contains(r.out+r.err, pw) {
				t.Errorf("%v with %q: exit %d, output %q %q", args, dsn[:20], r.code, r.out, r.err)
			}
		}
	}
	r := admin(t, "--help")
	if strings.Contains(r.err+r.out, "postgres://") || strings.Contains(r.err, "password") {
		t.Errorf("help mentions a DSN or password: %s", r.err)
	}
}

func TestRevokeWithAPastedFullKeyUsesOnlyThePrefixAndWarns(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	key := strings.TrimSpace(mustOK(t, "key", "create", "--tenant", "acme").out)
	r := mustOK(t, "key", "revoke", key)
	if !strings.Contains(r.out, "revoked key") {
		t.Fatalf("the pasted key was not revoked by its prefix: %+v", r)
	}
	if !strings.Contains(r.err, "complete API key") || !strings.Contains(r.err, "shell history") {
		t.Fatalf("no warning: %q", r.err)
	}
	if strings.Contains(r.out+r.err, key) || strings.Contains(r.out+r.err, key[len("sf_")+9:]) {
		t.Fatal("the revoke command echoed the secret")
	}
	// An ordinary prefix gets no warning.
	k2 := strings.TrimSpace(mustOK(t, "key", "create", "--tenant", "acme").out)
	if r := mustOK(t, "key", "revoke", k2[3:11]); strings.Contains(r.err, "warning") {
		t.Fatalf("spurious warning: %q", r.err)
	}
}
