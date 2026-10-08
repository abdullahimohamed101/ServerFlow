// Package migrations embeds ServerFlow's PostgreSQL schema migrations. Files are
// named NNNN_description.sql, numbered from 0001 without gaps, and are forward-only:
// a file that has been applied anywhere is never edited (its checksum is recorded and
// checked); a change is a new file.
package migrations

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

//go:embed *.sql
var files embed.FS

// Migration is one embedded SQL file.
type Migration struct {
	Version  int
	Name     string // the description part of the file name
	SQL      string
	Checksum string // hex SHA-256 of the file contents
}

var namePattern = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// All returns every embedded migration in version order. It fails if the files are
// misnamed, duplicated, empty or not numbered 1..N without gaps.
func All() ([]Migration, error) { return load(files) }

func load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []Migration
	for _, e := range entries {
		m := namePattern.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration file %q does not match NNNN_description.sql", e.Name())
		}
		v, _ := strconv.Atoi(m[1])
		b, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		if len(b) == 0 {
			return nil, fmt.Errorf("migration %s is empty", e.Name())
		}
		sum := sha256.Sum256(b)
		out = append(out, Migration{Version: v, Name: m[2], SQL: string(b), Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migrations must be numbered 0001.. without gaps or duplicates; found version %04d at position %d", m.Version, i+1)
		}
	}
	return out, nil
}
