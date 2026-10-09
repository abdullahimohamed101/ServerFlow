package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// BenchmarkRun is the metadata and result of one benchmark run (spec section 19). JSON columns
// are stored as jsonb, so they come back semantically equal but not byte-identical.
type BenchmarkRun struct {
	ID                 string
	CreatedAt          time.Time
	GitCommit          string
	GitDirty           bool
	Model              string
	WorkerCount        int
	GPUType            string
	Scheduler          string
	ConcurrencyOrRate  string
	Workload           string
	PromptDistribution json.RawMessage // nil: none
	MaxTokens          *int
	DurationSeconds    float64
	Seed               *int64
	RepeatIndex        int
	SchemaVersion      int
	Result             json.RawMessage
}

const benchCols = `id, created_at, git_commit, git_dirty, model, worker_count, gpu_type, scheduler, concurrency_or_rate, workload,
	prompt_distribution, max_tokens, duration_seconds, seed, repeat_index, schema_version, result`

func scanBench(row pgx.Row) (BenchmarkRun, error) {
	var b BenchmarkRun
	err := row.Scan(&b.ID, &b.CreatedAt, &b.GitCommit, &b.GitDirty, &b.Model, &b.WorkerCount, &b.GPUType, &b.Scheduler, &b.ConcurrencyOrRate, &b.Workload,
		&b.PromptDistribution, &b.MaxTokens, &b.DurationSeconds, &b.Seed, &b.RepeatIndex, &b.SchemaVersion, &b.Result)
	return b, mapErr(err)
}

// CreateBenchmarkRun stores a run. ID and CreatedAt are filled in when empty.
func (s *Store) CreateBenchmarkRun(ctx context.Context, b BenchmarkRun) (BenchmarkRun, error) {
	if b.ID == "" {
		b.ID = newID("bench_")
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	if b.Model == "" || !json.Valid(b.Result) {
		return BenchmarkRun{}, fmt.Errorf("%w: a benchmark run needs a model and a JSON result", ErrInvalid)
	}
	for _, c := range []struct {
		name  string
		v, lo int
	}{{"worker count", b.WorkerCount, 0}, {"repeat index", b.RepeatIndex, 0}, {"schema version", b.SchemaVersion, 1}} {
		if err := checkRange(c.name, c.v, c.lo, maxInt32); err != nil {
			return BenchmarkRun{}, err
		}
	}
	if err := checkRangePtr("max tokens", b.MaxTokens, 1, maxInt32); err != nil {
		return BenchmarkRun{}, err
	}
	if b.DurationSeconds < 0 || math.IsNaN(b.DurationSeconds) || math.IsInf(b.DurationSeconds, 0) {
		return BenchmarkRun{}, fmt.Errorf("%w: duration must be a finite number of seconds, zero or more", ErrInvalid)
	}
	for field, v := range map[string]string{"git commit": b.GitCommit, "gpu type": b.GPUType, "scheduler": b.Scheduler, "concurrency or rate": b.ConcurrencyOrRate, "workload": b.Workload, "model": b.Model} {
		if err := validateText(field, v, maxNameLen); err != nil {
			return BenchmarkRun{}, err
		}
	}
	var dist any // a nil interface is SQL NULL; an empty RawMessage would not be
	if len(b.PromptDistribution) > 0 {
		if !json.Valid(b.PromptDistribution) {
			return BenchmarkRun{}, fmt.Errorf("%w: prompt_distribution is not valid JSON", ErrInvalid)
		}
		dist = []byte(b.PromptDistribution)
	}
	return scanBench(s.pool.QueryRow(ctx, `INSERT INTO benchmark_runs (`+benchCols+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING `+benchCols,
		b.ID, b.CreatedAt, b.GitCommit, b.GitDirty, b.Model, b.WorkerCount, b.GPUType, b.Scheduler, b.ConcurrencyOrRate, b.Workload,
		dist, b.MaxTokens, b.DurationSeconds, b.Seed, b.RepeatIndex, b.SchemaVersion, []byte(b.Result)))
}

// GetBenchmarkRun finds a run by ID.
func (s *Store) GetBenchmarkRun(ctx context.Context, id string) (BenchmarkRun, error) {
	return scanBench(s.pool.QueryRow(ctx, `SELECT `+benchCols+` FROM benchmark_runs WHERE id = $1`, id))
}

// ListBenchmarkRuns returns up to limit runs, newest first (limit is clamped to 1-1000).
func (s *Store) ListBenchmarkRuns(ctx context.Context, limit int) ([]BenchmarkRun, error) {
	limit = min(max(limit, 1), 1000)
	rows, err := s.pool.Query(ctx, `SELECT `+benchCols+` FROM benchmark_runs ORDER BY created_at DESC, id LIMIT $1`, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []BenchmarkRun
	for rows.Next() {
		b, err := scanBench(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, mapErr(rows.Err())
}
