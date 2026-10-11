package tracing

import (
	"testing"

	"serverflow/internal/version"
)

func TestBuildVersionPrefersLinkerStamp(t *testing.T) {
	old := version.Version
	t.Cleanup(func() { version.Version = old })
	version.Version = ""
	if BuildVersion() == "" {
		t.Fatal("BuildVersion must never be empty")
	}
	version.Version = "v9.9.9-test"
	if got := BuildVersion(); got != "v9.9.9-test" {
		t.Fatalf("BuildVersion = %q, want the stamp", got)
	}
}
