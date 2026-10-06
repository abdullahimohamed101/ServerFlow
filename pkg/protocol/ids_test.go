package protocol

import (
	"regexp"
	"testing"
)

func TestIDFormat(t *testing.T) {
	tests := []struct {
		name string
		gen  func() string
		re   string
	}{
		{"request", NewRequestID, `^req_[0-9a-f]{16}$`},
		{"attempt", NewAttemptID, `^att_[0-9a-f]{16}$`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := tt.gen()
			if !regexp.MustCompile(tt.re).MatchString(id) {
				t.Fatalf("id %q does not match %s", id, tt.re)
			}
		})
	}
}

func TestIDsAreUnique(t *testing.T) {
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		id := NewRequestID()
		if seen[id] {
			t.Fatalf("duplicate id %q after %d ids", id, i)
		}
		seen[id] = true
	}
}
