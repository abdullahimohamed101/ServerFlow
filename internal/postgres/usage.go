package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"serverflow/internal/usage"
)

var _ usage.Store = (*Store)(nil)

const insertUsageSQL = `INSERT INTO usage_records (
	event_id, request_id, tenant_id, api_key_id, model, worker_id, outcome, failure_class, http_status,
	input_tokens, output_tokens, tokens_source, estimated_cost_tokens, attempts, ttft_ms, duration_ms,
	occurred_at, kafka_partition, kafka_offset)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT DO NOTHING`

// dataError says whether the database refused content (a constraint or type problem) rather than failing to be
// reached. Such a failure will repeat forever for the same row.
func dataError(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	return len(pe.Code) >= 2 && (pe.Code[:2] == "22" || pe.Code[:2] == "23")
}

// InsertUsage writes the rows in one transaction and returns how many were new. A row whose event_id or
// request_id already exists is skipped, so repeating a batch (after a crash, or a replay) changes nothing.
func (s *Store) InsertUsage(ctx context.Context, rows []usage.Row) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("postgres: begin usage insert: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var batch pgx.Batch
	for _, r := range rows {
		batch.Queue(insertUsageSQL, r.EventID, r.RequestID, r.TenantID, r.APIKeyID, r.Model, r.WorkerID, r.Outcome, r.FailureClass,
			r.HTTPStatus, r.InputTokens, r.OutputTokens, r.TokensSource, r.EstimatedCostTokens, r.Attempts, r.TTFTMS, r.DurationMS,
			r.OccurredAt, r.Partition, r.Offset)
	}
	br := tx.SendBatch(ctx, &batch)
	inserted := 0
	for range rows {
		tag, err := br.Exec()
		if err != nil {
			_ = br.Close()
			if dataError(err) {
				return 0, fmt.Errorf("%w: %v", usage.ErrBadData, err)
			}
			return 0, fmt.Errorf("postgres: insert usage: %w", err)
		}
		inserted += int(tag.RowsAffected())
	}
	if err := br.Close(); err != nil {
		return 0, fmt.Errorf("postgres: insert usage: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("postgres: commit usage: %w", err)
	}
	return inserted, nil
}

// RecordRejects stores the coordinates of unusable records; repeating one is harmless.
func (s *Store) RecordRejects(ctx context.Context, rejects []usage.Reject) error {
	if len(rejects) == 0 {
		return nil
	}
	var batch pgx.Batch
	for _, r := range rejects {
		batch.Queue(`INSERT INTO usage_rejected_events (topic, kafka_partition, kafka_offset, reason) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			r.Topic, r.Partition, r.Offset, r.Reason)
	}
	br := s.pool.SendBatch(ctx, &batch)
	for range rejects {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("postgres: record rejected event: %w", err)
		}
	}
	return br.Close()
}

// UsageSummary is one row of usage_hourly, summed over the requested window.
type UsageSummary struct {
	TenantID            string
	Model               string
	TokensSource        string
	Requests            int64
	Failures            int64
	InputTokens         int64
	OutputTokens        int64
	EstimatedCostTokens int64
}

// UsageSummaries reads the usage_hourly view for hours at or after since, optionally for one tenant ID, grouped
// by tenant, model and tokens_source.
func (s *Store) UsageSummaries(ctx context.Context, since time.Time, tenantID string) ([]UsageSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id, model, tokens_source, sum(requests)::bigint, sum(failures)::bigint,
		       coalesce(sum(input_tokens), 0)::bigint, coalesce(sum(output_tokens), 0)::bigint, coalesce(sum(estimated_cost_tokens), 0)::bigint
		FROM usage_hourly
		WHERE hour >= $1 AND ($2 = '' OR tenant_id = $2)
		GROUP BY tenant_id, model, tokens_source
		ORDER BY tenant_id, model, tokens_source`, since, tenantID)
	if err != nil {
		return nil, fmt.Errorf("postgres: usage summary: %w", mapErr(err))
	}
	defer rows.Close()
	var out []UsageSummary
	for rows.Next() {
		var u UsageSummary
		if err := rows.Scan(&u.TenantID, &u.Model, &u.TokensSource, &u.Requests, &u.Failures, &u.InputTokens, &u.OutputTokens, &u.EstimatedCostTokens); err != nil {
			return nil, fmt.Errorf("postgres: usage summary: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
