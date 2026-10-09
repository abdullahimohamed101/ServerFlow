package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"serverflow/migrations"
)

// migrateLockKey is the advisory lock that serialises migrators ("SFLW" + "MIGR").
const migrateLockKey int64 = 0x53464c574d494752

// migrateLockWait is how long a migrator waits for another one to finish before giving up. A var
// so tests can shorten it.
var migrateLockWait = 60 * time.Second

// takeMigrateLock takes the session advisory lock, polling so that a migrator that hangs while
// holding it cannot wedge every other one forever. Session-level advisory locks need a connection
// that stays the same between statements, so a transaction-pooling proxy (PgBouncer) cannot be
// placed between the migrator and the database.
func takeMigrateLock(ctx context.Context, conn *pgxpool.Conn) error {
	deadline := time.Now().Add(migrateLockWait)
	for {
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, migrateLockKey).Scan(&got); err != nil {
			return fmt.Errorf("migrate: take the migration lock: %w", err)
		}
		if got {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("migrate: another migration has held the lock for over %v; wait for it to finish or find and stop it (SELECT * FROM pg_locks WHERE locktype = 'advisory')", migrateLockWait)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: waiting for the migration lock: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// MigrationStatus describes one embedded migration against the database.
type MigrationStatus struct {
	Version   int
	Name      string
	Applied   bool
	AppliedAt time.Time // zero unless Applied
}

// MigrateUp applies every pending embedded migration, each in its own transaction, and
// returns the ones it applied. It is safe to run from several processes at once: a session
// advisory lock makes them take turns, and a later one finds nothing left to do. It refuses
// to run when an applied migration's file has changed since (checksum drift) or the
// database holds a migration this binary does not know.
func (s *Store) MigrateUp(ctx context.Context) ([]migrations.Migration, error) {
	all, err := migrations.All()
	if err != nil {
		return nil, err
	}
	return s.migrateUp(ctx, all)
}

func (s *Store) migrateUp(ctx context.Context, all []migrations.Migration) ([]migrations.Migration, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	defer conn.Release()

	if err := takeMigrateLock(ctx, conn); err != nil {
		return nil, err
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey); err != nil {
			// Closing the session releases the lock; do not hand a possibly-locked connection back.
			_ = conn.Hijack().Close(uctx)
		}
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		checksum   text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, fmt.Errorf("migrate: create schema_migrations: %w", err)
	}
	applied, err := readApplied(ctx, conn)
	if err != nil {
		return nil, err
	}
	if err := checkApplied(applied, all); err != nil {
		return nil, err
	}

	var done []migrations.Migration
	for _, m := range all[len(applied):] {
		if err := applyOne(ctx, conn, m); err != nil {
			return done, err
		}
		done = append(done, m)
	}
	return done, nil
}

type appliedRow struct {
	version   int
	name      string
	checksum  string
	appliedAt time.Time
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func readApplied(ctx context.Context, q querier) ([]appliedRow, error) {
	rows, err := q.Query(ctx, `SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
	}
	defer rows.Close()
	var out []appliedRow
	for rows.Next() {
		var r appliedRow
		if err := rows.Scan(&r.version, &r.name, &r.checksum, &r.appliedAt); err != nil {
			return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
	}
	return out, nil
}

// checkApplied makes sure what the database recorded is a prefix of what this binary embeds,
// unchanged.
func checkApplied(applied []appliedRow, all []migrations.Migration) error {
	if len(applied) > len(all) {
		return fmt.Errorf("migrate: the database is at version %d but this binary only knows %d migrations; run a newer serverflow-admin",
			applied[len(applied)-1].version, len(all))
	}
	for i, a := range applied {
		m := all[i]
		if a.version != m.Version || a.name != m.Name {
			return fmt.Errorf("migrate: the database recorded migration %04d_%s where this binary has %04d_%s", a.version, a.name, m.Version, m.Name)
		}
		if a.checksum != m.Checksum {
			return fmt.Errorf("migrate: migration %04d_%s was modified after it was applied (checksum drift); restore the file and add a new migration instead", m.Version, m.Name)
		}
	}
	return nil
}

func applyOne(ctx context.Context, conn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}, m migrations.Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate %04d_%s: %w", m.Version, m.Name, err)
	}
	// A rollback after a successful commit is a harmless no-op.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("migrate %04d_%s failed: %w", m.Version, m.Name, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`, m.Version, m.Name, m.Checksum); err != nil {
		return fmt.Errorf("migrate %04d_%s: record: %w", m.Version, m.Name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate %04d_%s: commit: %w", m.Version, m.Name, err)
	}
	return nil
}

// MigrationStatuses lists every embedded migration and whether it has been applied. It reads
// only, taking no lock and creating nothing, and reports drift as an error after the list.
func (s *Store) MigrationStatuses(ctx context.Context) ([]MigrationStatus, error) {
	all, err := migrations.All()
	if err != nil {
		return nil, err
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return nil, fmt.Errorf("migrate status: %w", err)
	}
	var applied []appliedRow
	if exists {
		if applied, err = readApplied(ctx, s.pool); err != nil {
			return nil, err
		}
	}
	out := make([]MigrationStatus, len(all))
	for i, m := range all {
		out[i] = MigrationStatus{Version: m.Version, Name: m.Name}
		if i < len(applied) {
			out[i].Applied, out[i].AppliedAt = true, applied[i].appliedAt
		}
	}
	return out, errors.Join(checkApplied(applied, all))
}
