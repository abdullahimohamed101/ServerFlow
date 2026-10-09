package auth

import (
	"context"
	"errors"
	"time"
)

// Status values shared with the database constraints.
const (
	TenantActive    = "active"
	TenantSuspended = "suspended"
	KeyActive       = "active"
	KeyRevoked      = "revoked"
)

// Authentication failures. Callers map ErrInvalid, ErrExpired and ErrRevoked to the same
// client response (a key must not reveal whether it exists); the distinction is for logs
// and tests only, and is reported only after the secret has matched.
var (
	ErrInvalid     = errors.New("auth: invalid API key")
	ErrExpired     = errors.New("auth: API key expired")
	ErrRevoked     = errors.New("auth: API key revoked")
	ErrSuspended   = errors.New("auth: tenant suspended")
	ErrUnavailable = errors.New("auth: key store unavailable")
	// ErrBadRecord is returned by Authenticate (and by a KeyStore) when one key's row exists but could
	// not be read. It concerns that key only; it is not an outage.
	ErrBadRecord = errors.New("auth: key record could not be read")
	// ErrBusy is returned by a KeyStore that could not take the query now (for instance no free
	// connection before the deadline). It is not evidence that the database is down.
	ErrBusy = errors.New("auth: key store busy")
	// ErrNotFound is what a KeyStore returns for a prefix that has no key.
	ErrNotFound = errors.New("auth: key not found")
)

// TenantPolicy is what the gateway needs to know about a tenant. Quotas and Priority are
// carried for later phases; the gateway enforces only Status and AllowedModels.
type TenantPolicy struct {
	Status string
	// AllowedModels nil means every model; non-nil (possibly empty) lists the only models allowed.
	// It is shared between requests and must be treated as read-only.
	AllowedModels     []string
	RequestsPerMinute int
	TokensPerMinute   int
	MaxConcurrent     int
	Priority          int
}

// AllowsModel reports whether the tenant may use model.
func (p TenantPolicy) AllowsModel(model string) bool {
	if p.AllowedModels == nil {
		return true
	}
	for _, m := range p.AllowedModels {
		if m == model {
			return true
		}
	}
	return false
}

// Principal is an authenticated caller.
type Principal struct {
	TenantID string
	KeyID    string
	Policy   TenantPolicy
}

// KeyRecord is what a KeyStore knows about one key: never the key itself, only its hash.
type KeyRecord struct {
	KeyID      string
	TenantID   string
	Prefix     string
	SecretHash []byte
	KeyStatus  string
	ExpiresAt  *time.Time
	Policy     TenantPolicy // Policy.Status is the tenant's status
}

// KeyStore finds a key by its public prefix. It returns ErrNotFound for an unknown prefix and
// any other error when the store cannot answer. internal/postgres implements it.
type KeyStore interface {
	LookupKey(ctx context.Context, prefix string) (KeyRecord, error)
}

// KeyToucher is optionally implemented by a KeyStore to record when a key was last used.
type KeyToucher interface {
	TouchKey(ctx context.Context, keyID string, at time.Time) error
}
