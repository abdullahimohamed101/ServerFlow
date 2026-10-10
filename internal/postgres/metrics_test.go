package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPoolCollectorExportsTheStats(t *testing.T) {
	st := PoolStats{Acquired: 2, Idle: 3, Total: 5, Max: 10, AcquireCount: 100, AcquireDuration: 1500 * time.Millisecond, EmptyAcquireCount: 4}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewPoolCollector(func() PoolStats { return st }))
	want := `# HELP postgres_pool_acquire_wait_seconds_total Total seconds callers spent waiting for a PostgreSQL connection.
# TYPE postgres_pool_acquire_wait_seconds_total counter
postgres_pool_acquire_wait_seconds_total 1.5
# HELP postgres_pool_acquires_total Connections acquired from the PostgreSQL pool.
# TYPE postgres_pool_acquires_total counter
postgres_pool_acquires_total 100
# HELP postgres_pool_connections Connections in the PostgreSQL pool, by state (acquired, idle, total or max).
# TYPE postgres_pool_connections gauge
postgres_pool_connections{state="acquired"} 2
postgres_pool_connections{state="idle"} 3
postgres_pool_connections{state="max"} 10
postgres_pool_connections{state="total"} 5
# HELP postgres_pool_empty_acquires_total Acquires that found the pool exhausted and had to wait: the pool is too small or the database too slow.
# TYPE postgres_pool_empty_acquires_total counter
postgres_pool_empty_acquires_total 4
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
