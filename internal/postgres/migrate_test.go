package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/migrations"
)

func tableNames(t *testing.T, s *Store) map[string]bool {
	t.Helper()
	rows, err := s.pool.Query(context.Background(), `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out[n] = true
	}
	return out
}

func TestMigrateUpCreatesSchemaAndIsIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	st, err := s.MigrationStatuses(ctx)
	if err != nil || len(st) == 0 || st[0].Applied {
		t.Fatalf("status before migrating: %+v %v", st, err)
	}
	done, err := s.MigrateUp(ctx)
	if err != nil || len(done) != len(st) {
		t.Fatalf("first migrate: applied %d, err %v", len(done), err)
	}
	for _, want := range []string{"tenants", "api_keys", "models", "benchmark_runs", "schema_migrations"} {
		if !tableNames(t, s)[want] {
			t.Errorf("table %s missing", want)
		}
	}
	done, err = s.MigrateUp(ctx)
	if err != nil || len(done) != 0 {
		t.Fatalf("second migrate must change nothing: applied %d, err %v", len(done), err)
	}
	st, err = s.MigrationStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range st {
		if !m.Applied || m.AppliedAt.IsZero() {
			t.Errorf("migration %d should be applied: %+v", m.Version, m)
		}
	}
}

func TestMigrateRefusesChecksumDrift(t *testing.T) {
	s := newMigratedStore(t)
	all, _ := migrations.All()
	changed := append([]migrations.Migration(nil), all...)
	changed[0].Checksum = strings.Repeat("0", 64)
	if _, err := s.migrateUp(context.Background(), changed); err == nil || !strings.Contains(err.Error(), "checksum drift") {
		t.Fatalf("drift not refused: %v", err)
	}
	// And a drifted database still reports its status, with the error.
	if _, err := s.MigrationStatuses(context.Background()); err != nil {
		t.Fatalf("real status of an unmodified database: %v", err)
	}
}

func TestMigrateRefusesAMismatchedMigrationName(t *testing.T) {
	s := newMigratedStore(t)
	if _, err := s.pool.Exec(context.Background(), `UPDATE schema_migrations SET name = 'something_else' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MigrateUp(context.Background()); err == nil || !strings.Contains(err.Error(), "something_else") {
		t.Fatalf("a recorded name that differs from the file must be refused: %v", err)
	}
}

func TestMigrateRefusesDatabaseNewerThanBinary(t *testing.T) {
	s := newMigratedStore(t)
	all, _ := migrations.All()
	extra := append(append([]migrations.Migration(nil), all...), migrations.Migration{Version: len(all) + 1, Name: "next", SQL: "SELECT 1", Checksum: "x"})
	if _, err := s.migrateUp(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	if _, err := s.migrateUp(context.Background(), all); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("an older binary must refuse a newer database: %v", err)
	}
}

func TestMigrateFailureRollsBackThatFileOnly(t *testing.T) {
	s := newMigratedStore(t)
	all, _ := migrations.All()
	bad := append([]migrations.Migration(nil), all...)
	bad = append(bad,
		migrations.Migration{Version: len(all) + 1, Name: "good", SQL: "CREATE TABLE extra_good (id int)", Checksum: "g"},
		migrations.Migration{Version: len(all) + 2, Name: "bad", SQL: "CREATE TABLE half_done (id int); SELECT 1/0;", Checksum: "b"},
	)
	done, err := s.migrateUp(context.Background(), bad)
	if err == nil || len(done) != 1 {
		t.Fatalf("expected one applied then a failure, got %d, %v", len(done), err)
	}
	tn := tableNames(t, s)
	if !tn["extra_good"] || tn["half_done"] {
		t.Fatalf("per-file transactions broken: %v", tn)
	}
	st, _ := s.MigrationStatuses(context.Background())
	_ = st
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil || n != len(all)+1 {
		t.Fatalf("recorded %d migrations, want %d (%v)", n, len(all)+1, err)
	}
}

func TestConcurrentMigrateUpIsSerialised(t *testing.T) {
	s := newStore(t)
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	applied := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done, err := s.MigrateUp(context.Background())
			errs[i], applied[i] = err, len(done)
		}()
	}
	wg.Wait()
	total := 0
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("migrator %d: %v", i, errs[i])
		}
		total += applied[i]
	}
	all, _ := migrations.All()
	if total != len(all) {
		t.Fatalf("migrations were applied %d times in total, want exactly %d", total, len(all))
	}
}

func TestMigrateGivesUpWhenAnotherMigratorHoldsTheLock(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	holder, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		t.Fatal(err)
	}
	old := migrateLockWait
	migrateLockWait = 300 * time.Millisecond
	defer func() { migrateLockWait = old }()
	start := time.Now()
	_, err = s.MigrateUp(ctx)
	if err == nil || !strings.Contains(err.Error(), "another migration has held the lock") || time.Since(start) > 5*time.Second {
		t.Fatalf("a held lock must produce a clear timeout, got %v after %v", err, time.Since(start))
	}
	// Released, the migrator proceeds.
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MigrateUp(ctx); err != nil {
		t.Fatalf("after the lock was released: %v", err)
	}
}

func TestSchemaConstraints(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) error { _, err := s.pool.Exec(ctx, sql, args...); return err }

	if err := exec(`INSERT INTO tenants (id, name) VALUES ('ten_1', 'acme')`); err != nil {
		t.Fatal(err)
	}
	hash := make([]byte, 32)
	bad := map[string]error{
		"tenant status":                 exec(`INSERT INTO tenants (id, name, status) VALUES ('ten_2', 'b', 'deleted')`),
		"negative rpm":                  exec(`INSERT INTO tenants (id, name, requests_per_minute) VALUES ('ten_3', 'c', -1)`),
		"negative tpm":                  exec(`INSERT INTO tenants (id, name, tokens_per_minute) VALUES ('ten_4', 'd', -1)`),
		"negative conc":                 exec(`INSERT INTO tenants (id, name, max_concurrent_requests) VALUES ('ten_5', 'e', -1)`),
		"priority range":                exec(`INSERT INTO tenants (id, name, priority) VALUES ('ten_6', 'f', 3)`),
		"duplicate name":                exec(`INSERT INTO tenants (id, name) VALUES ('ten_7', 'acme')`),
		"name format":                   exec(`INSERT INTO tenants (id, name) VALUES ('ten_8', 'a b')`),
		"name looks an id":              exec(`INSERT INTO tenants (id, name) VALUES ('ten_9', 'ten_1')`),
		"key status":                    exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash, status) VALUES ('key_1', 'ten_1', 'abcdef01', $1, 'weird')`, hash),
		"key prefix":                    exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash) VALUES ('key_2', 'ten_1', 'ABCDEF01', $1)`, hash),
		"key hash length":               exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash) VALUES ('key_3', 'ten_1', 'abcdef02', $1)`, hash[:5]),
		"key orphan":                    exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash) VALUES ('key_4', 'ten_nope', 'abcdef03', $1)`, hash),
		"revoked w/o time":              exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash, status) VALUES ('key_5', 'ten_1', 'abcdef04', $1, 'revoked')`, hash),
		"model status":                  exec(`INSERT INTO models (name, status) VALUES ('m', 'x')`),
		"model max tokens":              exec(`INSERT INTO models (name, max_tokens_limit) VALUES ('m2', 0)`),
		"null model name in allow-list": exec(`INSERT INTO tenants (id, name, allowed_models) VALUES ('ten_n', 'nn', ARRAY['a', NULL])`),
		"bench workers":                 exec(`INSERT INTO benchmark_runs (id, model, worker_count, schema_version, result) VALUES ('b', 'm', -1, 1, '{}')`),
	}
	for name, err := range bad {
		if err == nil {
			t.Errorf("%s: the database accepted it", name)
		}
	}
	if err := exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash) VALUES ('key_ok', 'ten_1', 'abcdef01', $1)`, hash); err != nil {
		t.Fatalf("valid key refused: %v", err)
	}
	if err := exec(`INSERT INTO api_keys (id, tenant_id, prefix, secret_hash) VALUES ('key_dup', 'ten_1', 'abcdef01', $1)`, hash); err == nil {
		t.Error("duplicate prefix accepted")
	}
}
