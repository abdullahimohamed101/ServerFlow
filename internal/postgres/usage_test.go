package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"serverflow/internal/usage"
)

func usageRow(i int, tenant string, outcome string) usage.Row {
	in, out := int64(10+i), int64(20+i)
	r := usage.Row{
		EventID: fmt.Sprintf("evt_%032x", i), RequestID: fmt.Sprintf("req_%016x", i), TenantID: tenant, APIKeyID: "key_x", Model: "m", WorkerID: "w",
		Outcome: outcome, HTTPStatus: 200, InputTokens: &in, OutputTokens: &out, TokensSource: "usage", EstimatedCostTokens: 7, Attempts: 1,
		TTFTMS: 3, DurationMS: 9, OccurredAt: time.Date(2026, 10, 1, 12, 30, 0, 0, time.UTC).Add(time.Duration(i) * time.Hour), Partition: 1, Offset: int64(i),
	}
	if outcome == "failed" {
		r.FailureClass, r.HTTPStatus, r.InputTokens, r.OutputTokens, r.TokensSource = "no_capacity", 503, nil, nil, "estimate"
	}
	return r
}

func TestMigration0003CreatesUsageTablesAndTheViewMatchesDirectSums(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	for _, want := range []string{"usage_records", "usage_rejected_events"} {
		if !tableNames(t, s)[want] {
			t.Errorf("table %s missing", want)
		}
	}
	var rows []usage.Row
	for i := 0; i < 24; i++ {
		o := "completed"
		if i%4 == 0 {
			o = "failed"
		}
		rows = append(rows, usageRow(i, []string{"ten_a", "ten_b", ""}[i%3], o))
	}
	n, err := s.InsertUsage(ctx, rows)
	if err != nil || n != 24 {
		t.Fatalf("insert: %d %v", n, err)
	}
	var directReq, directIn, directFail int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*), coalesce(sum(input_tokens),0), count(*) FILTER (WHERE outcome='failed') FROM usage_records`).Scan(&directReq, &directIn, &directFail); err != nil {
		t.Fatal(err)
	}
	var viewReq, viewIn, viewFail int64
	if err := s.pool.QueryRow(ctx, `SELECT sum(requests), coalesce(sum(input_tokens),0), sum(failures) FROM usage_hourly`).Scan(&viewReq, &viewIn, &viewFail); err != nil {
		t.Fatal(err)
	}
	if directReq != viewReq || directIn != viewIn || directFail != viewFail || directReq != 24 {
		t.Fatalf("view %d/%d/%d != direct %d/%d/%d", viewReq, viewIn, viewFail, directReq, directIn, directFail)
	}
	sums, err := s.UsageSummaries(ctx, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "ten_a")
	if err != nil || len(sums) == 0 {
		t.Fatalf("summaries: %v %v", sums, err)
	}
	var total int64
	for _, u := range sums {
		if u.TenantID != "ten_a" {
			t.Fatalf("tenant filter leaked %s", u.TenantID)
		}
		total += u.Requests
	}
	if total != 8 {
		t.Fatalf("ten_a requests = %d, want 8", total)
	}
	if sums, _ = s.UsageSummaries(ctx, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), ""); len(sums) != 0 {
		t.Fatal("the since filter must exclude old hours")
	}
}

func TestInsertUsageIsIdempotentAndUniqueOnRequest(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	batch := []usage.Row{usageRow(1, "ten_a", "completed"), usageRow(2, "ten_a", "failed")}
	if n, err := s.InsertUsage(ctx, batch); err != nil || n != 2 {
		t.Fatalf("%d %v", n, err)
	}
	if n, err := s.InsertUsage(ctx, batch); err != nil || n != 0 {
		t.Fatalf("a repeated batch must insert nothing: %d %v", n, err)
	}
	// a different event ID for the same request is also not counted again
	dup := usageRow(1, "ten_a", "completed")
	dup.EventID = fmt.Sprintf("evt_%032x", 999)
	if n, err := s.InsertUsage(ctx, []usage.Row{dup}); err != nil || n != 0 {
		t.Fatalf("second terminal event for one request: %d %v", n, err)
	}
	var c int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM usage_records`).Scan(&c)
	if c != 2 {
		t.Fatalf("rows = %d", c)
	}
	if n, err := s.InsertUsage(ctx, nil); n != 0 || err != nil {
		t.Fatal("empty batch")
	}
}

func TestInsertUsageReportsBadDataAndLeavesNothingBehind(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	bad := usageRow(2, "ten_a", "completed")
	bad.TokensSource = "guess" // violates the check constraint
	_, err := s.InsertUsage(ctx, []usage.Row{usageRow(1, "ten_a", "completed"), bad})
	if !errors.Is(err, usage.ErrBadData) {
		t.Fatalf("want ErrBadData, got %v", err)
	}
	var c int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM usage_records`).Scan(&c)
	if c != 0 {
		t.Fatalf("a failed batch must roll back entirely, found %d rows", c)
	}
	inconsistent := usageRow(3, "ten_a", "completed")
	inconsistent.FailureClass = "timeout"
	if _, err := s.InsertUsage(ctx, []usage.Row{inconsistent}); !errors.Is(err, usage.ErrBadData) {
		t.Fatalf("completed with a failure class must be refused: %v", err)
	}
}

func TestRecordRejectsIsRepeatable(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	r := []usage.Reject{{Topic: "t", Partition: 2, Offset: 5, Reason: usage.RejectDecode}, {Topic: "t", Partition: 2, Offset: 6, Reason: usage.RejectVersion}}
	for i := 0; i < 2; i++ {
		if err := s.RecordRejects(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	var c int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM usage_rejected_events`).Scan(&c)
	if c != 2 {
		t.Fatalf("rejects = %d", c)
	}
	if err := s.RecordRejects(ctx, []usage.Reject{{Topic: "t", Partition: 0, Offset: 0, Reason: "made-up"}}); err == nil {
		t.Fatal("an unknown reason must be refused by the constraint")
	}
}

func TestUsageSummariesAreExactToTheInstantAndOverflowSafe(t *testing.T) {
	s := newMigratedStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	var rows []usage.Row
	for i, at := range []time.Duration{10 * time.Minute, 40 * time.Minute, 50 * time.Minute, 90 * time.Minute} { // 12:10, 12:40, 12:50, 13:30
		r := usageRow(i, "ten_a", "completed")
		r.OccurredAt = base.Add(at)
		rows = append(rows, r)
	}
	if _, err := s.InsertUsage(ctx, rows); err != nil {
		t.Fatal(err)
	}
	// A window starting at 12:30 must count the 12:40, 12:50 and 13:30 rows: not the whole 12:00 hour, not only 13:00.
	sums, err := s.UsageSummaries(ctx, base.Add(30*time.Minute), "")
	if err != nil || len(sums) != 1 || sums[0].Requests != 3 {
		t.Fatalf("a window starting mid-hour: %+v %v", sums, err)
	}
	if sums, _ = s.UsageSummaries(ctx, base.Add(10*time.Minute), ""); len(sums) != 1 || sums[0].Requests != 4 {
		t.Fatalf("the boundary instant is included: %+v", sums)
	}
	if sums, _ = s.UsageSummaries(ctx, base.Add(10*time.Minute+time.Microsecond), ""); len(sums) != 1 || sums[0].Requests != 3 {
		t.Fatalf("just after the boundary: %+v", sums)
	}
	// Rows that bypass the event bounds (the column allows any bigint) must not break the summary for everyone.
	for i := 100; i < 102; i++ {
		_, err := s.pool.Exec(ctx, `INSERT INTO usage_records (event_id, request_id, outcome, http_status, input_tokens, output_tokens, tokens_source, occurred_at)
			VALUES ($1, $2, 'completed', 200, 9223372036854775807, 9223372036854775807, 'usage', $3)`,
			fmt.Sprintf("evt_%032x", i), fmt.Sprintf("req_%016x", i), base.Add(2*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
	}
	sums, err = s.UsageSummaries(ctx, base, "")
	if err != nil {
		t.Fatalf("a summary over huge sums must not fail: %v", err)
	}
	var found bool
	for _, u := range sums {
		if u.TenantID == "" && u.InputTokens == "18446744073709551614" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the exact numeric sum is missing: %+v", sums)
	}
}
