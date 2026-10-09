package postgres

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"serverflow/internal/auth"
)

// Tenant is a customer of the gateway. Quota fields of 0 mean "none configured"; they are stored
// and carried but not enforced until Phase 8. AllowedModels nil means every model.
type Tenant struct {
	ID                    string
	Name                  string
	Status                string
	RequestsPerMinute     int
	TokensPerMinute       int
	MaxConcurrentRequests int
	AllowedModels         []string
	Priority              int
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// TenantInput holds the settings of a new tenant.
type TenantInput struct {
	Name                  string
	RequestsPerMinute     int
	TokensPerMinute       int
	MaxConcurrentRequests int
	AllowedModels         []string // nil: every model
	Priority              int      // 0 interactive, 1 normal, 2 batch
}

const tenantCols = `id, name, status, requests_per_minute, tokens_per_minute, max_concurrent_requests, allowed_models, priority, created_at, updated_at`

func scanTenant(row pgx.Row) (Tenant, error) {
	var t Tenant
	err := row.Scan(&t.ID, &t.Name, &t.Status, &t.RequestsPerMinute, &t.TokensPerMinute, &t.MaxConcurrentRequests, &t.AllowedModels, &t.Priority, &t.CreatedAt, &t.UpdatedAt)
	return t, mapErr(err)
}

func validateModelList(models []string) error {
	for _, m := range models {
		if err := ValidateModelName(m); err != nil {
			return err
		}
	}
	return nil
}

// CreateTenant adds an active tenant.
func (s *Store) CreateTenant(ctx context.Context, in TenantInput) (Tenant, error) {
	if err := ValidateTenantName(in.Name); err != nil {
		return Tenant{}, err
	}
	if err := validateModelList(in.AllowedModels); err != nil {
		return Tenant{}, err
	}
	for _, c := range []struct {
		name string
		v    int
		lo   int
		hi   int
	}{
		{"requests per minute", in.RequestsPerMinute, 0, maxInt32}, {"tokens per minute", in.TokensPerMinute, 0, maxInt32},
		{"max concurrent requests", in.MaxConcurrentRequests, 0, maxInt32}, {"priority", in.Priority, 0, 2},
	} {
		if err := checkRange(c.name, c.v, c.lo, c.hi); err != nil {
			return Tenant{}, err
		}
	}
	return scanTenant(s.pool.QueryRow(ctx, `INSERT INTO tenants (id, name, requests_per_minute, tokens_per_minute, max_concurrent_requests, allowed_models, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING `+tenantCols,
		newID("ten_"), in.Name, in.RequestsPerMinute, in.TokensPerMinute, in.MaxConcurrentRequests, in.AllowedModels, in.Priority))
}

// GetTenant finds a tenant by ID or name.
func (s *Store) GetTenant(ctx context.Context, ref string) (Tenant, error) {
	return scanTenant(s.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE id = $1 OR name = $1`, ref))
}

// ListTenants returns every tenant, oldest first.
func (s *Store) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+tenantCols+` FROM tenants ORDER BY created_at, id`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, mapErr(rows.Err())
}

// SetTenantStatus sets a tenant active or suspended and returns the tenant after the change and
// the status it had before.
func (s *Store) SetTenantStatus(ctx context.Context, ref, status string) (t Tenant, previous string, err error) {
	if status != auth.TenantActive && status != auth.TenantSuspended {
		return Tenant{}, "", fmt.Errorf("%w: status must be %q or %q", ErrInvalid, auth.TenantActive, auth.TenantSuspended)
	}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM tenants WHERE id = $1 OR name = $1 FOR UPDATE`, ref).Scan(&previous); err != nil {
			return mapErr(err)
		}
		t, err = scanTenant(tx.QueryRow(ctx, `UPDATE tenants SET status = $2, updated_at = now() WHERE id = $1 OR name = $1 RETURNING `+tenantCols, ref, status))
		return err
	})
	return t, previous, err
}

// Quota holds optional new quota values; nil leaves a value unchanged.
type Quota struct {
	RequestsPerMinute     *int
	TokensPerMinute       *int
	MaxConcurrentRequests *int
	Priority              *int
}

// SetTenantQuota changes the given quota fields of a tenant.
func (s *Store) SetTenantQuota(ctx context.Context, ref string, q Quota) (Tenant, error) {
	for _, c := range []struct {
		name string
		v    *int
		hi   int
	}{
		{"requests per minute", q.RequestsPerMinute, maxInt32}, {"tokens per minute", q.TokensPerMinute, maxInt32},
		{"max concurrent requests", q.MaxConcurrentRequests, maxInt32}, {"priority", q.Priority, 2},
	} {
		if err := checkRangePtr(c.name, c.v, 0, c.hi); err != nil {
			return Tenant{}, err
		}
	}
	return scanTenant(s.pool.QueryRow(ctx, `UPDATE tenants SET
			requests_per_minute     = COALESCE($2, requests_per_minute),
			tokens_per_minute       = COALESCE($3, tokens_per_minute),
			max_concurrent_requests = COALESCE($4, max_concurrent_requests),
			priority                = COALESCE($5, priority),
			updated_at = now()
		WHERE id = $1 OR name = $1 RETURNING `+tenantCols,
		ref, q.RequestsPerMinute, q.TokensPerMinute, q.MaxConcurrentRequests, q.Priority))
}

// SetTenantModels sets the models a tenant may use. nil allows every model; an empty non-nil
// list allows none.
func (s *Store) SetTenantModels(ctx context.Context, ref string, models []string) (Tenant, error) {
	if err := validateModelList(models); err != nil {
		return Tenant{}, err
	}
	models = slices.Clone(models)
	return scanTenant(s.pool.QueryRow(ctx, `UPDATE tenants SET allowed_models = $2, updated_at = now() WHERE id = $1 OR name = $1 RETURNING `+tenantCols, ref, models))
}

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
