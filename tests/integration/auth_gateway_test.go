package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/postgres"
	"serverflow/internal/postgres/postgrestest"
)

// countingStore counts the queries the real store is asked to run.
type countingStore struct {
	*postgres.Store
	n atomic.Int64
}

func (c *countingStore) LookupKey(ctx context.Context, prefix string) (auth.KeyRecord, error) {
	c.n.Add(1)
	return c.Store.LookupKey(ctx, prefix)
}

// A real gateway, authenticator and PostgreSQL in one process: the hot path must not query per
// request, and hostile keys must neither reach SQL nor grow memory.
func TestGatewayWithRealPostgresHotPathAndFloods(t *testing.T) {
	dsn := postgrestest.NewDSN(t)
	ctx := context.Background()
	st, err := postgres.Open(ctx, postgres.Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if _, err := st.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateTenant(ctx, postgres.TenantInput{Name: "acme", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	key, prefix, hash := auth.GenerateKey()
	if _, err := st.CreateKey(ctx, "acme", "t", nil, prefix, hash); err != nil {
		t.Fatal(err)
	}

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(up.Close)
	cfg := config.Default().Gateway
	cfg.UpstreamURL, cfg.Models = up.URL, []string{model}
	gw := gateway.New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	cs := &countingStore{Store: st}
	const size = 64
	authn := auth.New(cs, auth.Config{CacheTTL: time.Minute, NegativeTTL: time.Minute, CacheSize: size, StaleGrace: time.Minute})
	gw.SetAuthenticator(authn)
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	for i := 0; i < 300; i++ {
		if r, b := authChat(t, addr, key); r.StatusCode != 200 {
			t.Fatalf("request %d: %d %s", i, r.StatusCode, b)
		}
	}
	if n := cs.n.Load(); n != 1 {
		t.Fatalf("%d database queries for 300 warm requests, want 1", n)
	}

	// Hostile Authorization values never reach SQL.
	before := cs.n.Load()
	for _, bad := range []string{"sf_'; DROP TABLE api_keys; --_" + strings.Repeat("A", 43), "sf_%27%20OR%201=1_" + strings.Repeat("A", 43),
		"' OR '1'='1", "sf_00000000_" + strings.Repeat("A", 42), strings.Repeat("sf_", 5000)} {
		if r, _ := authChat(t, addr, bad); r.StatusCode != 401 {
			t.Fatalf("%.20q: %d", bad, r.StatusCode)
		}
	}
	if cs.n.Load() != before {
		t.Fatal("a malformed key caused a database query")
	}

	// A flood of distinct random keys: each costs at most one lookup, answers 401, and the caches stay bounded.
	for i := 0; i < size*8; i++ {
		k, _, _ := auth.GenerateKey()
		if r, _ := authChat(t, addr, k); r.StatusCode != 401 {
			t.Fatalf("random key: %d", r.StatusCode)
		}
	}
	pos, neg := authn.CacheSizes()
	if pos != 1 || neg != size {
		t.Fatalf("cache sizes after the flood: positive %d (want 1), negative %d (want %d)", pos, neg, size)
	}
	if r, _ := authChat(t, addr, key); r.StatusCode != 200 {
		t.Fatalf("real key after flood: %d", r.StatusCode)
	}
	if cs.n.Load()-before > int64(size*8) {
		t.Fatalf("more than one lookup per random key: %d", cs.n.Load()-before)
	}
}
