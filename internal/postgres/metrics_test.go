package postgres

import (
	"context"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
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

// Scraping never talks to the database: with the pool's connection attempts stuck on a server that accepts and then
// says nothing, the collector still answers at once from the pool's own counters.
func TestCollectorReturnsAtOnceWhileTheDatabaseHangs(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var held []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // never answers
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	})
	pc, err := pgxpool.ParseConfig("postgres://u:p@" + ln.Addr().String() + "/d?sslmode=disable&connect_timeout=30")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &Store{pool: pool}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = pool.Ping(ctx) }() // stuck in the handshake
	time.Sleep(100 * time.Millisecond)

	reg := prometheus.NewRegistry()
	reg.MustRegister(s.Collector())
	done := make(chan struct{})
	start := time.Now()
	go func() { _, _ = reg.Gather(); close(done) }()
	select {
	case <-done:
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("a scrape took %v while the database hung", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a scrape hangs while the database hangs: the collector must read the pool's counters only")
	}
}
