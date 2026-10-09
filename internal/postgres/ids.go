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

// ValidateModelName checks a model name: 1-128 printable characters without spaces or control
// characters.
func ValidateModelName(name string) error {
	if name == "" || len(name) > maxNameLen {
		return fmt.Errorf("%w: a model name is 1-%d characters", ErrInvalid, maxNameLen)
	}
	for _, r := range name {
		if unicode.IsSpace(r) || badRune(r) {
			return fmt.Errorf("%w: a model name may not contain spaces or control characters", ErrInvalid)
		}
	}
	return nil
}

// badRune reports characters that must never be stored in free text because they can forge terminal
// output or reorder text: control characters (NUL, ESC, newline, ...), invalid UTF-8 (U+FFFD), format
// characters (bidirectional overrides, zero-width joiners) and line or paragraph separators.
func badRune(r rune) bool {
	return r == unicode.ReplacementChar || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r)
}

func validateText(field, s string, max int) error {
	if len(s) > max {
		return fmt.Errorf("%w: %s is longer than %d bytes", ErrInvalid, field, max)
	}
	for _, r := range s {
		if badRune(r) {
			return fmt.Errorf("%w: %s contains a control or invisible formatting character", ErrInvalid, field)
		}
	}
	return nil
}

// maxInt32 is the largest value of the integer columns.
const maxInt32 = 1<<31 - 1

// checkRange validates a number before it reaches the database, so an out-of-range value is a clear
// message and not a driver error.
func checkRange(field string, v, lo, hi int) error {
	if v < lo || v > hi {
		return fmt.Errorf("%w: %s must be between %d and %d", ErrInvalid, field, lo, hi)
	}
	return nil
}

func checkRangePtr(field string, v *int, lo, hi int) error {
	if v == nil {
		return nil
	}
	return checkRange(field, *v, lo, hi)
}
