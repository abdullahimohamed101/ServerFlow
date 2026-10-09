package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/postgres/postgrestest"
)

func ptr[T any](v T) *T { return &v }

func TestTenantLifecycle(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()

	all, err := s.CreateTenant(ctx, TenantInput{Name: "alpha", Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(all.ID, "ten_") || all.Status != auth.TenantActive || all.AllowedModels != nil {
		t.Fatalf("unexpected new tenant: %+v", all)
	}
	limited, err := s.CreateTenant(ctx, TenantInput{Name: "beta", RequestsPerMinute: 60, TokensPerMinute: 10000, MaxConcurrentRequests: 4, AllowedModels: []string{"m1", "m2"}, Priority: 2})
	if err != nil {
		t.Fatal(err)
	}
	if limited.RequestsPerMinute != 60 || limited.TokensPerMinute != 10000 || limited.MaxConcurrentRequests != 4 || limited.Priority != 2 ||
		!reflect.DeepEqual(limited.AllowedModels, []string{"m1", "m2"}) {
		t.Fatalf("round trip lost fields: %+v", limited)
	}
	none, err := s.CreateTenant(ctx, TenantInput{Name: "gamma", AllowedModels: []string{}, Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	if none.AllowedModels == nil || len(none.AllowedModels) != 0 {
		t.Fatalf("an empty allow-list (no models) must stay distinct from nil (all models): %#v", none.AllowedModels)
	}

	if _, err := s.CreateTenant(ctx, TenantInput{Name: "alpha"}); !errors.Is(err, ErrConflict) {
		t.Errorf("duplicate name: %v", err)
	}
	for _, bad := range []TenantInput{
		{Name: ""}, {Name: "a b"}, {Name: "ten_x"}, {Name: "key_x"}, {Name: strings.Repeat("a", 65)},
		{Name: "neg", RequestsPerMinute: -1}, {Name: "prio", Priority: 9}, {Name: "badmodel", AllowedModels: []string{"a b"}},
	} {
		if _, err := s.CreateTenant(ctx, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: want ErrInvalid, got %v", bad, err)
		}
	}

	got, err := s.GetTenant(ctx, "beta")
	if err != nil || got.ID != limited.ID {
		t.Fatalf("get by name: %v", err)
	}
	if got, err = s.GetTenant(ctx, limited.ID); err != nil || got.Name != "beta" {
		t.Fatalf("get by id: %v", err)
	}
	if _, err := s.GetTenant(ctx, "nobody"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing tenant: %v", err)
	}
	if list, err := s.ListTenants(ctx); err != nil || len(list) != 3 {
		t.Fatalf("list: %d %v", len(list), err)
	}

	sus, prev, err := s.SetTenantStatus(ctx, "alpha", auth.TenantSuspended)
	if err != nil || prev != auth.TenantActive || sus.Status != auth.TenantSuspended || !sus.UpdatedAt.After(all.UpdatedAt) {
		t.Fatalf("suspend: %+v prev=%q %v", sus, prev, err)
	}
	if _, prev, err = s.SetTenantStatus(ctx, "alpha", auth.TenantActive); err != nil || prev != auth.TenantSuspended {
		t.Fatalf("activate: prev=%q %v", prev, err)
	}
	if _, _, err := s.SetTenantStatus(ctx, "alpha", "banned"); !errors.Is(err, ErrInvalid) {
		t.Errorf("bad status: %v", err)
	}
	if _, _, err := s.SetTenantStatus(ctx, "nobody", auth.TenantSuspended); !errors.Is(err, ErrNotFound) {
		t.Errorf("suspend missing: %v", err)
	}

	q, err := s.SetTenantQuota(ctx, "alpha", Quota{RequestsPerMinute: ptr(100), Priority: ptr(0)})
	if err != nil || q.RequestsPerMinute != 100 || q.Priority != 0 || q.TokensPerMinute != 0 {
		t.Fatalf("set quota: %+v %v", q, err)
	}
	if _, err := s.SetTenantQuota(ctx, "alpha", Quota{TokensPerMinute: ptr(-5)}); !errors.Is(err, ErrInvalid) {
		t.Errorf("negative quota: %v", err)
	}
	m, err := s.SetTenantModels(ctx, "alpha", []string{"x"})
	if err != nil || !reflect.DeepEqual(m.AllowedModels, []string{"x"}) {
		t.Fatalf("set models: %+v %v", m, err)
	}
	if m, err = s.SetTenantModels(ctx, "alpha", nil); err != nil || m.AllowedModels != nil {
		t.Fatalf("reset to all models: %+v %v", m, err)
	}
}

func TestKeyLifecycleAndLookup(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	tn, _ := s.CreateTenant(ctx, TenantInput{Name: "acme", AllowedModels: []string{"m1"}, RequestsPerMinute: 5, Priority: 2})

	plain, prefix, hash := auth.GenerateKey()
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	k, err := s.CreateKey(ctx, "acme", "ci", &exp, prefix, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(k.ID, "key_") || k.TenantID != tn.ID || k.TenantName != "acme" || k.Prefix != prefix || k.Status != auth.KeyActive ||
		k.ExpiresAt == nil || !k.ExpiresAt.Equal(exp) || k.RevokedAt != nil || k.LastUsedAt != nil || k.Label != "ci" {
		t.Fatalf("created key: %+v", k)
	}

	// Nothing in any column may contain the plaintext or the secret part of it.
	secret := plain[len("sf_")+9:]
	var leaked int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM api_keys k WHERE k::text LIKE '%' || $1 || '%'`, secret).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatalf("plaintext secret found in a row (n=%d, err=%v)", leaked, err)
	}

	rec, err := s.LookupKey(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if rec.KeyID != k.ID || rec.TenantID != tn.ID || !auth.HashesEqual(rec.SecretHash, hash) || rec.KeyStatus != auth.KeyActive ||
		rec.Policy.Status != auth.TenantActive || !reflect.DeepEqual(rec.Policy.AllowedModels, []string{"m1"}) || rec.Policy.RequestsPerMinute != 5 || rec.Policy.Priority != 2 {
		t.Fatalf("lookup: %+v", rec)
	}
	for _, bad := range []string{"00000000", "", "zzzzzzzz", "' OR 1=1 --", strings.Repeat("a", 9)} {
		if _, err := s.LookupKey(ctx, bad); !errors.Is(err, auth.ErrNotFound) {
			t.Errorf("lookup %q: %v", bad, err)
		}
	}

	// A second key with the same prefix is refused; a second key for the tenant is fine.
	if _, err := s.CreateKey(ctx, "acme", "dup", nil, prefix, hash); !errors.Is(err, ErrConflict) {
		t.Errorf("prefix collision: %v", err)
	}
	_, p2, h2 := auth.GenerateKey()
	if _, err := s.CreateKey(ctx, tn.ID, "", nil, p2, h2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateKey(ctx, "nobody", "", nil, "abcdef01", h2); !errors.Is(err, ErrNotFound) {
		t.Errorf("key for missing tenant: %v", err)
	}
	if _, err := s.CreateKey(ctx, "acme", "", nil, "ABCDEF01", h2); !errors.Is(err, ErrInvalid) {
		t.Errorf("bad prefix: %v", err)
	}
	if _, err := s.CreateKey(ctx, "acme", "", nil, "abcdef01", h2[:3]); !errors.Is(err, ErrInvalid) {
		t.Errorf("short hash: %v", err)
	}
	if _, err := s.CreateKey(ctx, "acme", strings.Repeat("x", 200), nil, "abcdef01", h2); !errors.Is(err, ErrInvalid) {
		t.Errorf("long label: %v", err)
	}

	if keys, err := s.ListKeys(ctx, "acme"); err != nil || len(keys) != 2 {
		t.Fatalf("list tenant keys: %d %v", len(keys), err)
	}
	if keys, err := s.ListKeys(ctx, ""); err != nil || len(keys) != 2 {
		t.Fatalf("list all: %d %v", len(keys), err)
	}
	if keys, err := s.ListKeys(ctx, "other"); err != nil || len(keys) != 0 {
		t.Fatalf("list other: %d %v", len(keys), err)
	}

	rv, was, err := s.RevokeKey(ctx, prefix) // by prefix
	if err != nil || !was || rv.Status != auth.KeyRevoked || rv.RevokedAt == nil {
		t.Fatalf("revoke: %+v was=%v %v", rv, was, err)
	}
	if rv2, was, err := s.RevokeKey(ctx, k.ID); err != nil || was || rv2.Status != auth.KeyRevoked || !rv2.RevokedAt.Equal(*rv.RevokedAt) {
		t.Fatalf("second revoke must be a no-op: %+v was=%v %v", rv2, was, err)
	}
	if _, _, err := s.RevokeKey(ctx, "key_nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("revoke missing: %v", err)
	}
	if rec, err = s.LookupKey(ctx, prefix); err != nil || rec.KeyStatus != auth.KeyRevoked {
		t.Fatalf("revoked key must stay visible to lookup so it is refused, not unknown: %+v %v", rec, err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.TouchKey(ctx, k.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := s.TouchKey(ctx, k.ID, now.Add(-time.Hour)); err != nil { // never backwards
		t.Fatal(err)
	}
	keys, _ := s.ListKeys(ctx, "acme")
	if keys[0].LastUsedAt == nil || !keys[0].LastUsedAt.Equal(now) {
		t.Fatalf("last_used_at: %+v", keys[0].LastUsedAt)
	}
}

func TestModelsRoundTrip(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	full, err := s.AddModel(ctx, Model{Name: "llama-3-8b", DisplayName: "Llama 3 8B", MaxTokensLimit: ptr(2048), Notes: "main model"})
	if err != nil {
		t.Fatal(err)
	}
	if full.Status != ModelEnabled || full.MaxTokensLimit == nil || *full.MaxTokensLimit != 2048 || full.DisplayName != "Llama 3 8B" || full.Notes != "main model" {
		t.Fatalf("round trip: %+v", full)
	}
	bare, err := s.AddModel(ctx, Model{Name: "tiny", Status: ModelDisabled})
	if err != nil || bare.MaxTokensLimit != nil || bare.Status != ModelDisabled {
		t.Fatalf("nulls: %+v %v", bare, err)
	}
	if _, err := s.AddModel(ctx, Model{Name: "tiny"}); !errors.Is(err, ErrConflict) {
		t.Errorf("duplicate: %v", err)
	}
	for _, bad := range []Model{{Name: ""}, {Name: "a b"}, {Name: "x", MaxTokensLimit: ptr(0)}, {Name: "y", Status: "weird"}, {Name: "z", Notes: strings.Repeat("n", 2000)}} {
		if _, err := s.AddModel(ctx, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
	got, err := s.GetModel(ctx, "llama-3-8b")
	if err != nil || !reflect.DeepEqual(got, full) {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := s.GetModel(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	m, prev, err := s.SetModelStatus(ctx, "tiny", ModelEnabled)
	if err != nil || prev != ModelDisabled || m.Status != ModelEnabled {
		t.Fatalf("enable: %+v %q %v", m, prev, err)
	}
	if _, _, err := s.SetModelStatus(ctx, "tiny", "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("bad status: %v", err)
	}
	if _, _, err := s.SetModelStatus(ctx, "nope", ModelEnabled); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	if list, err := s.ListModels(ctx); err != nil || len(list) != 2 || list[0].Name != "llama-3-8b" {
		t.Fatalf("list: %+v %v", list, err)
	}
}

func jsonEq(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(x, y)
}

func TestBenchmarkRunsRoundTrip(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	at := time.Date(2026, 10, 1, 12, 0, 0, 123456000, time.UTC)
	in := BenchmarkRun{
		ID: "bench_fixed", CreatedAt: at, GitCommit: "abc1234", GitDirty: true, Model: "mock-model", WorkerCount: 3, GPUType: "none",
		Scheduler: "least-active", ConcurrencyOrRate: "c=64", Workload: "multi-tenant",
		PromptDistribution: json.RawMessage(`{"short":0.7,"long":0.3,"nested":{"a":[1,2,3]}}`), MaxTokens: ptr(128), DurationSeconds: 61.5, Seed: ptr(int64(-42)),
		RepeatIndex: 2, SchemaVersion: 1, Result: json.RawMessage(`{"p50_ms":12.5,"ok":true,"note":"unicode: é","none":null}`),
	}
	out, err := s.CreateBenchmarkRun(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBenchmarkRun(ctx, "bench_fixed")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range []BenchmarkRun{out, got} {
		if !g.CreatedAt.Equal(at) || g.GitCommit != "abc1234" || !g.GitDirty || g.Model != "mock-model" || g.WorkerCount != 3 || g.GPUType != "none" ||
			g.Scheduler != "least-active" || g.ConcurrencyOrRate != "c=64" || g.Workload != "multi-tenant" || g.MaxTokens == nil || *g.MaxTokens != 128 ||
			g.DurationSeconds != 61.5 || g.Seed == nil || *g.Seed != -42 || g.RepeatIndex != 2 || g.SchemaVersion != 1 ||
			!jsonEq(t, g.PromptDistribution, in.PromptDistribution) || !jsonEq(t, g.Result, in.Result) {
			t.Fatalf("round trip lost data: %+v", g)
		}
	}

	// Nulls: no distribution, no max tokens, no seed.
	min, err := s.CreateBenchmarkRun(ctx, BenchmarkRun{Model: "m", SchemaVersion: 1, Result: json.RawMessage(`[]`)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(min.ID, "bench_") || min.PromptDistribution != nil || min.MaxTokens != nil || min.Seed != nil || min.CreatedAt.IsZero() {
		t.Fatalf("null handling: %+v", min)
	}
	if _, err := s.CreateBenchmarkRun(ctx, in); !errors.Is(err, ErrConflict) {
		t.Errorf("duplicate id: %v", err)
	}
	for name, bad := range map[string]BenchmarkRun{
		"no model": {SchemaVersion: 1, Result: json.RawMessage(`{}`)}, "bad result": {Model: "m", SchemaVersion: 1, Result: json.RawMessage(`{`)},
		"no result": {Model: "m", SchemaVersion: 1}, "bad dist": {Model: "m", SchemaVersion: 1, Result: json.RawMessage(`{}`), PromptDistribution: json.RawMessage(`x`)},
		"schema 0": {Model: "m", Result: json.RawMessage(`{}`)}, "neg workers": {Model: "m", SchemaVersion: 1, WorkerCount: -1, Result: json.RawMessage(`{}`)},
	} {
		if _, err := s.CreateBenchmarkRun(ctx, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.GetBenchmarkRun(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	list, err := s.ListBenchmarkRuns(ctx, 10)
	if err != nil || len(list) != 2 || list[0].ID != min.ID {
		t.Fatalf("list newest first: %+v %v", list, err)
	}
	if list, _ := s.ListBenchmarkRuns(ctx, 1); len(list) != 1 {
		t.Errorf("limit not applied")
	}
}

// Every string a caller can supply is passed as a bind parameter. These values would change the
// meaning of a query if one were concatenated into SQL; here they are just data or are refused.
func TestInjectionStringsAreInert(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	evil := []string{
		`'; DROP TABLE tenants; --`, `" OR 1=1 --`, `x' OR '1'='1`, `\'; DELETE FROM api_keys; --`,
		`a'); DROP TABLE models; --`, "x\x00y", `$1`, `%`, `_`, `{}`, `'}'::text[]`,
	}
	tn, err := s.CreateTenant(ctx, TenantInput{Name: "safe", Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evil {
		_, _ = s.CreateTenant(ctx, TenantInput{Name: e})
		_, _ = s.GetTenant(ctx, e)
		_, _, _ = s.SetTenantStatus(ctx, e, auth.TenantSuspended)
		_, _, _ = s.SetTenantStatus(ctx, "safe", e)
		_, _ = s.SetTenantQuota(ctx, e, Quota{})
		_, _ = s.SetTenantModels(ctx, e, []string{e})
		_, _ = s.SetTenantModels(ctx, "safe", []string{e})
		_, _ = s.CreateKey(ctx, e, e, nil, "abcdef01", make([]byte, 32))
		_, _ = s.CreateKey(ctx, "safe", e, nil, e, make([]byte, 32))
		_, _ = s.ListKeys(ctx, e)
		_, _, _ = s.RevokeKey(ctx, e)
		_, _ = s.LookupKey(ctx, e)
		_, _ = s.AddModel(ctx, Model{Name: e, DisplayName: e, Notes: e})
		_, _ = s.GetModel(ctx, e)
		_, _, _ = s.SetModelStatus(ctx, e, e)
		_, _ = s.CreateBenchmarkRun(ctx, BenchmarkRun{ID: e, Model: e, GitCommit: e, Result: json.RawMessage(`{}`), SchemaVersion: 1})
		_, _ = s.GetBenchmarkRun(ctx, e)
	}
	// The tables still exist and "safe" is untouched (apart from a possible harmless model list).
	for _, tbl := range []string{"tenants", "api_keys", "models", "benchmark_runs"} {
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
			t.Fatalf("table %s damaged: %v", tbl, err)
		}
	}
	got, err := s.GetTenant(ctx, "safe")
	if err != nil || got.ID != tn.ID || got.Status != auth.TenantActive {
		t.Fatalf("safe tenant changed: %+v %v", got, err)
	}
	if list, _ := s.ListTenants(ctx); len(list) != 1 {
		t.Fatalf("an injection string created a tenant: %d", len(list))
	}
	if keys, _ := s.ListKeys(ctx, ""); len(keys) != 0 {
		t.Fatalf("an injection string created a key")
	}
	// A hostile value that is valid text is stored verbatim (model notes), not interpreted.
	if _, err := s.AddModel(ctx, Model{Name: "literal", Notes: `'; DROP TABLE models; --`}); err != nil {
		t.Fatal(err)
	}
	m, _ := s.GetModel(ctx, "literal")
	if m.Notes != `'; DROP TABLE models; --` {
		t.Fatalf("notes mangled: %q", m.Notes)
	}
}

func TestLookupKeyClassifiesFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("unreadable row is a per-key data fault", func(t *testing.T) {
		s := newMigratedStore(t)
		if _, err := s.pool.Exec(ctx, `ALTER TABLE tenants DROP CONSTRAINT tenants_allowed_models_no_null_elements`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, `INSERT INTO tenants (id, name, allowed_models) VALUES ('ten_bad', 'bad', ARRAY['a', NULL])`); err != nil {
			t.Fatal(err)
		}
		_, prefix, hash := auth.GenerateKey()
		if _, err := s.CreateKey(ctx, "bad", "", nil, prefix, hash); err != nil {
			t.Fatal(err)
		}
		_, err := s.LookupKey(ctx, prefix)
		if !errors.Is(err, auth.ErrBadRecord) || errors.Is(err, auth.ErrNotFound) {
			t.Fatalf("a row that will not decode must be ErrBadRecord, got %v", err)
		}
	})

	t.Run("a saturated pool is busy, not an outage", func(t *testing.T) {
		dsn := postgrestest.NewDSN(t)
		s, err := Open(ctx, Config{DSN: dsn, MaxConns: 1, ConnectTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		held, err := s.pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		c, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		if _, err := s.LookupKey(c, "abcdef01"); !errors.Is(err, auth.ErrBusy) {
			t.Fatalf("pool saturation must be ErrBusy, got %v", err)
		}
	})

	t.Run("a closed pool is an outage", func(t *testing.T) {
		s := newMigratedStore(t)
		s.pool.Close()
		_, err := s.LookupKey(ctx, "abcdef01")
		if err == nil || errors.Is(err, auth.ErrBusy) || errors.Is(err, auth.ErrBadRecord) || errors.Is(err, auth.ErrNotFound) {
			t.Fatalf("an unusable pool must be a plain failure, got %v", err)
		}
	})
}
