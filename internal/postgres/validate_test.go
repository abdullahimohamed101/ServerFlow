package postgres

import (
	"errors"
	"strings"
	"testing"
)

// These need no database.
func TestValidateTenantNameRejectsIDLookalikesAndJunk(t *testing.T) {
	for _, bad := range []string{"", "ten_x", "key_x", "ten_", "a b", "a/b", "-lead", ".lead", strings.Repeat("a", 65), "a\x00", "a\x1b[0m", "é", "a\n"} {
		if err := ValidateTenantName(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q accepted", bad)
		}
	}
	for _, ok := range []string{"acme", "team-a.prod", "A1", "tenant_with_underscore", "tenxx", "keyboard", strings.Repeat("a", 64)} {
		if err := ValidateTenantName(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	// "key_" is refused by the Go check on its own, not only by the database constraint.
	if err := ValidateTenantName("key_abc"); err == nil || !strings.Contains(err.Error(), "key_") {
		t.Errorf("key_ prefix not named in the error: %v", err)
	}
}

func TestValidateTextRejectsControlAndInvisibleCharacters(t *testing.T) {
	for _, bad := range []string{"a\x00b", "\x1b[31m", "x\ny", "x\ty", "\x7f", "\u009b", "‮", "​", " ", " ", "bad\xffutf8", "\ufeff"} {
		if err := validateText("label", bad, 100); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q accepted", bad)
		}
		if err := ValidateModelName("m" + bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("model name with %q accepted", bad)
		}
	}
	for _, ok := range []string{"", "plain", "Clé de test ✓", "日本語", "with spaces and, punctuation!"} {
		if err := validateText("label", ok, 100); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	if err := validateText("label", strings.Repeat("a", 101), 100); !errors.Is(err, ErrInvalid) {
		t.Error("over-long text accepted")
	}
}

func TestCheckRange(t *testing.T) {
	for _, c := range []struct {
		v, lo, hi int
		ok        bool
	}{{0, 0, 5, true}, {5, 0, 5, true}, {-1, 0, 5, false}, {6, 0, 5, false}, {maxInt32, 0, maxInt32, true}, {maxInt32 + 1, 0, maxInt32, false}} {
		err := checkRange("x", c.v, c.lo, c.hi)
		if (err == nil) != c.ok || (err != nil && (!errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "must be between"))) {
			t.Errorf("%+v: %v", c, err)
		}
	}
	if checkRangePtr("x", nil, 1, 2) != nil {
		t.Error("a nil pointer means unchanged")
	}
	one := 9
	if checkRangePtr("x", &one, 1, 2) == nil {
		t.Error("out-of-range pointer accepted")
	}
}
