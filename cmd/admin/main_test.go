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

func assertCleanRefusal(t *testing.T, r result, args []string) {
	t.Helper()
	if r.code != 1 || !strings.Contains(r.err, "must be between") {
		t.Errorf("%v: exit %d, stderr %q; want exit 1 and a range message", args, r.code, r.err)
	}
	for _, leak := range []string{"0x", "pgx", "encode", "int4", "OID", "goroutine", "panic"} {
		if strings.Contains(r.err, leak) {
			t.Errorf("%v: the error leaks driver internals (%q): %s", args, leak, r.err)
		}
	}
	if r.out != "" {
		t.Errorf("%v: failed commands print nothing on stdout, got %q", args, r.out)
	}
}

func TestNumericFlagsAreRangeCheckedBeforeTheDatabase(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "base")
	n := 0
	name := func() string { n++; return "t" + strings.Repeat("x", n) }
	for _, flag := range []string{"--rpm", "--tpm", "--max-concurrent"} {
		for _, ok := range []string{"0", "1", "2147483647"} {
			mustOK(t, "tenant", "create", name(), flag, ok)
			mustOK(t, "tenant", "set-quota", "base", flag, ok)
		}
		for _, bad := range []string{"2147483648", "99999999999", "9223372036854775807", "-1", "-2147483649"} {
			args := []string{"tenant", "create", name(), flag, bad}
			assertCleanRefusal(t, admin(t, args...), args)
			args = []string{"tenant", "set-quota", "base", flag, bad}
			assertCleanRefusal(t, admin(t, args...), args)
		}
	}
	for _, ok := range []string{"0", "1", "2"} {
		mustOK(t, "tenant", "create", name(), "--priority", ok)
		mustOK(t, "tenant", "set-quota", "base", "--priority", ok)
	}
	for _, bad := range []string{"3", "-1", "99999999999"} {
		args := []string{"tenant", "create", name(), "--priority", bad}
		assertCleanRefusal(t, admin(t, args...), args)
		args = []string{"tenant", "set-quota", "base", "--priority", bad}
		assertCleanRefusal(t, admin(t, args...), args)
	}
	for _, ok := range []string{"1", "2147483647"} {
		mustOK(t, "model", "add", "m"+ok, "--max-tokens", ok)
	}
	for _, bad := range []string{"0", "-1", "2147483648", "99999999999"} {
		args := []string{"model", "add", "mbad", "--max-tokens", bad}
		assertCleanRefusal(t, admin(t, args...), args)
	}
	// Nothing half-created by the refusals.
	if r := mustOK(t, "model", "list"); strings.Contains(r.out, "mbad") {
		t.Fatal("a refused model was stored")
	}
}

func TestExpiryBounds(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	for _, ok := range []string{"1h", "90m", "1d", "3650d"} {
		mustOK(t, "key", "create", "--tenant", "acme", "--expires", ok)
	}
	for expires, want := range map[string]string{
		"0s": "positive", "0": "positive", "0d": "1 to 3650", "-1h": "positive", "3651d": "1 to 3650", "99999999999d": "1 to 3650",
		"87601h": "at most 10 years", "9999999999h": "duration",
	} {
		r := admin(t, "key", "create", "--tenant", "acme", "--expires", expires)
		if r.code != 2 || !strings.Contains(r.err, want) || r.out != "" {
			t.Errorf("--expires %s: exit %d, stdout %q, stderr %q; want exit 2 mentioning %q", expires, r.code, r.out, r.err, want)
		}
	}
}

func TestControlCharactersAreRefused(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	bad := []string{"\x1b[31mred\x1b[0m", "a\x00b", "line1\nline2", "tab\there", "\u202eevil", "zero\u200bwidth", "del\x7f", "c1\u009b"}
	for _, b := range bad {
		for _, args := range [][]string{
			{"tenant", "create", "n" + b}, {"key", "create", "--tenant", "acme", "--label", b}, {"model", "add", "m" + b},
			{"model", "add", "okname", "--display-name", b}, {"model", "add", "okname", "--notes", b}, {"tenant", "set-models", "acme", "--models", "x," + b},
		} {
			r := admin(t, args...)
			if r.code == 0 {
				t.Errorf("%q was accepted by %v", b, args[:2])
			}
			if strings.ContainsAny(r.out+r.err, "\x1b\x00\x7f") || strings.Contains(r.out+r.err, "\u202e") {
				t.Errorf("the refusal echoed control characters: %q %q", r.out, r.err)
			}
		}
	}
	if r := mustOK(t, "model", "list"); strings.TrimSpace(r.out) != "" {
		t.Errorf("a model was stored: %q", r.out)
	}
	// Ordinary unicode text is fine.
	mustOK(t, "key", "create", "--tenant", "acme", "--label", "Cl\u00e9 de test \u2713")
}

// Rows written around the application (an older version, a DBA) must still not reach the
// terminal as escape sequences when listed.
func TestListingNeverEmitsTerminalEscapes(t *testing.T) {
	dsn := postgrestest.NewDSN(t)
	t.Setenv("SERVERFLOW_POSTGRES_DSN", dsn)
	mustOK(t, "migrate", "up")
	mustOK(t, "tenant", "create", "acme")
	mustOK(t, "key", "create", "--tenant", "acme", "--label", "plain")
	postgrestest.Exec(t, dsn, "UPDATE api_keys SET label = E'\\x1b[2Jowned\\x1b]0;title\\x07'")
	postgrestest.Exec(t, dsn, "INSERT INTO models (name, display_name, notes) VALUES (E'm\\x1b[31m', E'd\\x1b[1m', 'n')")
	postgrestest.Exec(t, dsn, "UPDATE tenants SET allowed_models = ARRAY[E'a\\x1b[0m']")
	for _, args := range [][]string{{"key", "list"}, {"key", "list", "--tenant", "acme"}, {"model", "list"}, {"tenant", "list"}, {"tenant", "show", "acme"}} {
		r := mustOK(t, args...)
		if strings.ContainsAny(r.out+r.err, "\x1b\x07\x00") {
			t.Errorf("%v prints control characters: %q", args, r.out)
		}
	}
}

func TestEmptyModelsListMeansNoModels(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "locked", "--models", "")
	if r := mustOK(t, "tenant", "show", "locked"); !strings.Contains(r.out, "models:       none") {
		t.Fatalf("--models \"\" must mean no models, not all: %s", r.out)
	}
}

func TestInsecureRemoteDSNIsRefusedByTheCLI(t *testing.T) {
	for _, dsn := range []string{
		"postgres://app:pw@db.example.com:5432/x?sslmode=disable",
		"postgres://app:pw@db.example.com:5432/x",
		"postgres://app:pw@db.example.com:5432/x?sslmode=prefer",
	} {
		t.Setenv("SERVERFLOW_POSTGRES_DSN", dsn)
		r := admin(t, "migrate", "status")
		if r.code != 1 || !strings.Contains(r.err, "TLS") || strings.Contains(r.err, "pw") || strings.Contains(r.err, "db.example.com") {
			t.Errorf("%q: exit %d, stderr %q", dsn[len(dsn)-20:], r.code, r.err)
		}
	}
}

func TestKeyCreateRetriesAPrefixCollision(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	first := strings.TrimSpace(mustOK(t, "key", "create", "--tenant", "acme").out)
	old := generateKey
	defer func() { generateKey = old }()
	calls := 0
	generateKey = func() (string, string, []byte) {
		calls++
		if calls <= 2 { // the same prefix again, twice
			_, _, h := old()
			return "sf_" + first[3:11] + "_x", first[3:11], h
		}
		return old()
	}
	r := mustOK(t, "key", "create", "--tenant", "acme")
	if calls != 3 || !keyRe.MatchString(strings.TrimSpace(r.out)) {
		t.Fatalf("calls=%d out=%q: a prefix collision must be retried with a fresh key", calls, r.out)
	}
}
