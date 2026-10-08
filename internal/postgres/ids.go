package postgres

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("postgres: crypto/rand unavailable: " + err.Error())
	}
	return prefix + hex.EncodeToString(b[:])
}

// Limits on text that clients of this package supply (the CLI, mostly). The database repeats
// the important ones as constraints.
const (
	maxLabelLen = 128
	maxNotesLen = 1024
	maxNameLen  = 128
)

var tenantNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidateTenantName checks a tenant name. Names look like "acme" or "team-a.prod"; they may not
// start with "ten_" or "key_" so a name can never be mistaken for an ID.
func ValidateTenantName(name string) error {
	if !tenantNamePattern.MatchString(name) {
		return fmt.Errorf("%w: a tenant name is 1-64 letters, digits, '.', '_' or '-', starting with a letter or digit", ErrInvalid)
	}
	if strings.HasPrefix(name, "ten_") || strings.HasPrefix(name, "key_") {
		return fmt.Errorf("%w: a tenant name may not start with \"ten_\" or \"key_\"", ErrInvalid)
	}
	return nil
}

// ValidateModelName checks a model name: 1-128 printable characters without spaces or control characters.
func ValidateModelName(name string) error {
	if name == "" || len(name) > maxNameLen {
		return fmt.Errorf("%w: a model name is 1-%d characters", ErrInvalid, maxNameLen)
	}
	for _, r := range name {
		if r == unicode.ReplacementChar || unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("%w: a model name may not contain spaces or control characters", ErrInvalid)
		}
	}
	return nil
}

func validateText(field, s string, max int) error {
	if len(s) > max {
		return fmt.Errorf("%w: %s is longer than %d bytes", ErrInvalid, field, max)
	}
	for _, r := range s {
		if r == 0 || r == unicode.ReplacementChar {
			return fmt.Errorf("%w: %s contains an invalid character", ErrInvalid, field)
		}
	}
	return nil
}
