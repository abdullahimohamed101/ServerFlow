package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Model statuses.
const (
	ModelEnabled  = "enabled"
	ModelDisabled = "disabled"
)

// Model is a configured model. This phase only stores them; the gateway still takes its model
// list from gateway.models or the worker registry.
type Model struct {
	Name           string
	Status         string
	DisplayName    string
	MaxTokensLimit *int
	Notes          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const modelCols = `name, status, display_name, max_tokens_limit, notes, created_at, updated_at`

func scanModel(row pgx.Row) (Model, error) {
	var m Model
	err := row.Scan(&m.Name, &m.Status, &m.DisplayName, &m.MaxTokensLimit, &m.Notes, &m.CreatedAt, &m.UpdatedAt)
	return m, mapErr(err)
}

// AddModel stores a new model. Status defaults to enabled.
func (s *Store) AddModel(ctx context.Context, m Model) (Model, error) {
	if err := ValidateModelName(m.Name); err != nil {
		return Model{}, err
	}
	if m.Status == "" {
		m.Status = ModelEnabled
	}
	if err := validateText("display name", m.DisplayName, maxLabelLen); err != nil {
		return Model{}, err
	}
	if err := validateText("notes", m.Notes, maxNotesLen); err != nil {
		return Model{}, err
	}
	return scanModel(s.pool.QueryRow(ctx, `INSERT INTO models (name, status, display_name, max_tokens_limit, notes)
		VALUES ($1, $2, $3, $4, $5) RETURNING `+modelCols, m.Name, m.Status, m.DisplayName, m.MaxTokensLimit, m.Notes))
}

// GetModel finds a model by name.
func (s *Store) GetModel(ctx context.Context, name string) (Model, error) {
	return scanModel(s.pool.QueryRow(ctx, `SELECT `+modelCols+` FROM models WHERE name = $1`, name))
}

// ListModels returns every model by name.
func (s *Store) ListModels(ctx context.Context) ([]Model, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+modelCols+` FROM models ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []Model
	for rows.Next() {
		m, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, mapErr(rows.Err())
}

// SetModelStatus enables or disables a model and returns it with its previous status.
func (s *Store) SetModelStatus(ctx context.Context, name, status string) (m Model, previous string, err error) {
	if status != ModelEnabled && status != ModelDisabled {
		return Model{}, "", fmt.Errorf("%w: status must be %q or %q", ErrInvalid, ModelEnabled, ModelDisabled)
	}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM models WHERE name = $1 FOR UPDATE`, name).Scan(&previous); err != nil {
			return mapErr(err)
		}
		m, err = scanModel(tx.QueryRow(ctx, `UPDATE models SET status = $2, updated_at = now() WHERE name = $1 RETURNING `+modelCols, name, status))
		return err
	})
	return m, previous, err
}
