package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/postgres"
	"serverflow/internal/postgres/postgrestest"
	"serverflow/internal/redis/redistest"
	"serverflow/internal/telemetry"
)

// redisAndPostgres gathers the Redis client and PostgreSQL pool collectors against the real test servers and
// requires every family the dashboards and alerts use (redis_up, postgres_pool_empty_acquires_total) plus the
// rest of their families.
func redisAndPostgres(t *testing.T) {
	t.Helper()
	rc := redistest.NewClient(t)
	if err := rc.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := postgres.Open(context.Background(), postgres.Config{DSN: postgrestest.NewDSN(t), MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(rc.Collector(), store.Collector())
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatalf("labels outside the allowlist: %v", bad)
	}
	have := map[string]bool{}
	for _, f := range fams {
		have[f.GetName()] = true
	}
	for _, want := range []string{
		"redis_up", "redis_commands_total", "redis_errors_total", "redis_pool_connections",
		"postgres_pool_connections", "postgres_pool_acquires_total", "postgres_pool_acquire_wait_seconds_total", "postgres_pool_empty_acquires_total",
	} {
		if !have[want] {
			t.Errorf("%s is not exported", want)
		}
	}
	for n := range exemptions {
		if n == "up" || n == "ALERTS" {
			continue
		}
		if !have[n] {
			t.Errorf("the exempt metric %s is not exported by the real clients either", n)
		}
	}
}
