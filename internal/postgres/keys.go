package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"serverflow/internal/auth"
)

// APIKey is a key's metadata. The secret is never stored, so it cannot be returned; only the
// public prefix and a hash exist.
type APIKey struct {
	ID         string
	TenantID   string
	TenantName string
	Label      string
	Prefix     string
	Status     string
	CreatedAt  time.Time
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
	LastUsedAt *time.Time
}

const keyCols = `k.id, k.tenant_id, t.name, k.label, k.prefix, k.status, k.created_at, k.expires_at, k.revoked_at, k.last_used_at`

func scanKey(row pgx.Row) (APIKey, error) {
	var k APIKey
	err := row.Scan(&k.ID, &k.TenantID, &k.TenantName, &k.Label, &k.Prefix, &k.Status, &k.CreatedAt, &k.ExpiresAt, &k.RevokedAt, &k.LastUsedAt)
	return k, mapErr(err)
}

// CreateKey stores a new key for a tenant (by ID or name). Produce prefix and hash with
// auth.GenerateKey; the plaintext never reaches this package. A prefix collision returns
// ErrConflict, and the caller should generate another key.
func (s *Store) CreateKey(ctx context.Context, tenantRef, label string, expiresAt *time.Time, prefix string, hash []byte) (APIKey, error) {
	if err := validateText("label", label, maxLabelLen); err != nil {
		return APIKey{}, err
	}
	if !auth.ValidPrefix(prefix) || len(hash) != 32 {
		return APIKey{}, fmt.Errorf("%w: malformed key material", ErrInvalid)
	}
	return scanKey(s.pool.QueryRow(ctx, `WITH ins AS (
			INSERT INTO api_keys (id, tenant_id, label, prefix, secret_hash, expires_at)
			SELECT $1, t.id, $3, $4, $5, $6 FROM tenants t WHERE t.id = $2 OR t.name = $2
			RETURNING *)
		SELECT `+keyCols+` FROM ins k JOIN tenants t ON t.id = k.tenant_id`,
		newID("key_"), tenantRef, label, prefix, hash, expiresAt))
}

// ListKeys returns the keys of a tenant (ID or name), or all keys when tenantRef is empty.
func (s *Store) ListKeys(ctx context.Context, tenantRef string) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+keyCols+` FROM api_keys k JOIN tenants t ON t.id = k.tenant_id
		WHERE $1 = '' OR t.id = $1 OR t.name = $1 ORDER BY k.created_at, k.id`, tenantRef)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, mapErr(rows.Err())
}

// RevokeKey revokes a key by ID or prefix. It is idempotent: wasActive reports whether this call
// changed anything. The row stays for audit; prefixes are never reused.
func (s *Store) RevokeKey(ctx context.Context, ref string) (k APIKey, wasActive bool, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var id, status string
		if err := tx.QueryRow(ctx, `SELECT id, status FROM api_keys WHERE id = $1 OR prefix = $1 FOR UPDATE`, ref).Scan(&id, &status); err != nil {
			return mapErr(err)
		}
		if status == auth.KeyActive {
			if _, err := tx.Exec(ctx, `UPDATE api_keys SET status = 'revoked', revoked_at = now() WHERE id = $1`, id); err != nil {
				return mapErr(err)
			}
			wasActive = true
		}
		k, err = scanKey(tx.QueryRow(ctx, `SELECT `+keyCols+` FROM api_keys k JOIN tenants t ON t.id = k.tenant_id WHERE k.id = $1`, id))
		return err
	})
	return k, wasActive, err
}

// LookupKey implements auth.KeyStore. An unknown or malformed prefix is auth.ErrNotFound without
// touching the database. Failures are classified for the authenticator:
//   - auth.ErrBusy: no connection became free before the deadline (pool saturation);
//   - auth.ErrBadRecord: the key's row was found but could not be read (a per-key data fault);
//   - anything else: the database could not be reached or failed (an outage).
func (s *Store) LookupKey(ctx context.Context, prefix string) (auth.KeyRecord, error) {
	if !auth.ValidPrefix(prefix) {
		return auth.KeyRecord{}, auth.ErrNotFound
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return auth.KeyRecord{}, fmt.Errorf("%w: no connection became available", auth.ErrBusy)
		}
		return auth.KeyRecord{}, err
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, `SELECT k.id, k.tenant_id, k.prefix, k.secret_hash, k.status, k.expires_at,
			t.status, t.allowed_models, t.requests_per_minute, t.tokens_per_minute, t.max_concurrent_requests, t.priority
		FROM api_keys k JOIN tenants t ON t.id = k.tenant_id WHERE k.prefix = $1`, prefix)
	if err != nil {
		return auth.KeyRecord{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return auth.KeyRecord{}, err // the connection failed mid-read: an outage
		}
		return auth.KeyRecord{}, auth.ErrNotFound
	}
	var r auth.KeyRecord
	if err := rows.Scan(&r.KeyID, &r.TenantID, &r.Prefix, &r.SecretHash, &r.KeyStatus, &r.ExpiresAt,
		&r.Policy.Status, &r.Policy.AllowedModels, &r.Policy.RequestsPerMinute, &r.Policy.TokensPerMinute, &r.Policy.MaxConcurrent, &r.Policy.Priority); err != nil {
		// The row arrived but would not decode (for example a NULL element in allowed_models).
		return auth.KeyRecord{}, fmt.Errorf("%w: %v", auth.ErrBadRecord, err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return auth.KeyRecord{}, err
	}
	return r, nil
}

// TouchKey records that a key was used, never moving last_used_at backwards. It implements auth.KeyToucher.
func (s *Store) TouchKey(ctx context.Context, keyID string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE api_keys SET last_used_at = $2 WHERE id = $1 AND (last_used_at IS NULL OR last_used_at < $2)`, keyID, at)
	return err
}

var (
	_ auth.KeyStore   = (*Store)(nil)
	_ auth.KeyToucher = (*Store)(nil)
)
