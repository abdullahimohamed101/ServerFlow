package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"serverflow/internal/postgres/postgrestest"
	"serverflow/internal/usage"
)

// UsageConsumerGrants is the least privilege the usage consumer's database role needs (docs/operations/kafka-and-usage.md
// repeats it). The test applies exactly this, runs the consumer's whole workload as that role, and checks that it can
// do nothing else.
const usageConsumerGrants = `
GRANT USAGE ON SCHEMA public TO %[1]s;
GRANT SELECT ON schema_migrations TO %[1]s;
GRANT INSERT ON usage_records, usage_rejected_events TO %[1]s;
`

func TestTheUsageConsumerRunsWithTheDocumentedLeastPrivilegeRole(t *testing.T) {
	dsn := postgrestest.NewDSN(t)
	admin, err := Open(context.Background(), Config{DSN: dsn, MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx := context.Background()
	if _, err := admin.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	role := "sf_usage_" + hex.EncodeToString(b[:])
	const pw = "role-test-password"
	exec := func(sql string) {
		t.Helper()
		if _, err := admin.pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec("CREATE ROLE " + role + " LOGIN PASSWORD '" + pw + "'")
	t.Cleanup(func() {
		// Roles belong to the cluster, not the database: remove it, whatever happens.
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.pool.Exec(c, "DROP OWNED BY "+role)
		_, _ = admin.pool.Exec(c, "DROP ROLE IF EXISTS "+role)
	})
	for _, stmt := range splitStatements(usageConsumerGrants, role) {
		exec(stmt)
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, pw)
	s, err := Open(ctx, Config{DSN: u.String(), MaxConns: 2})
	if err != nil {
		t.Fatalf("the restricted role cannot connect: %v", err)
	}
	defer s.Close()

	// The consumer's startup check and workload.
	st, err := s.MigrationStatuses(ctx)
	if err != nil {
		t.Fatalf("migration check as the restricted role: %v", err)
	}
	for _, m := range st {
		if !m.Applied {
			t.Fatalf("migration %d not seen as applied", m.Version)
		}
	}
	rows := []usage.Row{usageRow(1, "ten_a", "completed"), usageRow(2, "ten_a", "failed")}
	if n, err := s.InsertUsage(ctx, rows); err != nil || n != 2 {
		t.Fatalf("insert as the restricted role: %d %v", n, err)
	}
	if n, err := s.InsertUsage(ctx, rows); err != nil || n != 0 {
		t.Fatalf("a repeated batch (ON CONFLICT DO NOTHING) as the restricted role: %d %v", n, err)
	}
	if err := s.RecordRejects(ctx, []usage.Reject{{Topic: "t", Partition: 1, Offset: 1, Reason: usage.RejectDecode}}); err != nil {
		t.Fatalf("reject as the restricted role: %v", err)
	}

	// And nothing more.
	for _, sql := range []string{
		"SELECT count(*) FROM tenants",
		"SELECT count(*) FROM api_keys",
		"SELECT count(*) FROM usage_records",
		"UPDATE usage_records SET model = 'x'",
		"DELETE FROM usage_records",
		"DELETE FROM usage_rejected_events",
		"INSERT INTO tenants (id, name) VALUES ('ten_x', 'x')",
		"CREATE TABLE sneaky (id int)",
		"DROP TABLE usage_records",
	} {
		if _, err := s.pool.Exec(ctx, sql); err == nil {
			t.Errorf("the restricted role was allowed: %s", sql)
		}
	}
}

// splitStatements fills the role name into the grants and splits them into statements.
func splitStatements(tmpl, role string) []string {
	var out []string
	for _, st := range strings.Split(fmt.Sprintf(tmpl, role), ";") {
		if st = strings.TrimSpace(st); st != "" {
			out = append(out, st)
		}
	}
	return out
}
