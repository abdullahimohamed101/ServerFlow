// Package postgres is ServerFlow's durable metadata store: tenants, API keys, model
// configs and benchmark-run metadata, plus the migration runner. It is the only package
// that imports the PostgreSQL driver. Every query uses bind parameters; no SQL is built
// from input. Errors never contain the DSN's password.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Errors callers may test with errors.Is.
var (
	ErrNotFound = errors.New("postgres: not found")
	ErrConflict = errors.New("postgres: already exists")
	ErrInvalid  = errors.New("postgres: invalid value")
)

// Config describes the connection. DSN is a secret: it is never logged or echoed.
type Config struct {
	DSN            string
	MaxConns       int
	ConnectTimeout time.Duration
}

// Store is a pool of connections plus the query methods.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects and verifies the database answers within ConnectTimeout.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		// pgx redacts passwords in its own parse errors, but nothing about the DSN is worth the risk.
		return nil, errors.New("postgres: the DSN could not be parsed (expected postgres://user:password@host:port/database)")
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = int32(cfg.MaxConns) //nolint:gosec // validated by config (1-100)
	}
	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	pc.ConnConfig.ConnectTimeout = timeout
	secrets := secretsOf(pc.ConnConfig)

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, redact(err, secrets)
	}
	s := &Store{pool: pool}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, redact(fmt.Errorf("postgres: cannot reach the database: %w", err), secrets)
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping checks the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// redactedError carries the original error for errors.Is/As while printing text with the
// DSN's secrets removed.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

func secretsOf(cc *pgx.ConnConfig) []string {
	var out []string
	if cc.Password != "" {
		out = append(out, cc.Password)
	}
	return out
}

func redact(err error, secrets []string) error {
	msg := err.Error()
	for _, s := range secrets {
		msg = strings.ReplaceAll(msg, s, "***")
	}
	return &redactedError{msg: msg, err: err}
}

// mapErr turns driver errors into this package's sentinels where the caller can act on them.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%w (%s)", ErrConflict, pe.ConstraintName)
		case "23514", "22001", "22P02", "22003", "23502": // check, too long, bad text representation, not null
			return fmt.Errorf("%w (%s)", ErrInvalid, firstNonEmpty(pe.ConstraintName, pe.ColumnName, pe.Message))
		case "23503": // foreign_key_violation
			return ErrNotFound
		}
	}
	return err
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
