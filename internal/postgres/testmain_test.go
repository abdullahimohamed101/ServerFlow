package postgres

import (
	"context"
	"testing"
	"time"

	"serverflow/internal/postgres/postgrestest"
)

// newStore returns a Store on a fresh, empty database (no migrations applied).
func newStore(t *testing.T) *Store {
	t.Helper()
	dsn := postgrestest.NewDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 8, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// newMigratedStore returns a Store on a database with every migration applied.
func newMigratedStore(t *testing.T) *Store {
	t.Helper()
	s := newStore(t)
	if _, err := s.MigrateUp(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}
