// Package deploy holds the static tests of the container deployment (Phase 16, ADR-021). They need no Docker: they
// parse the Dockerfile, the compose files, the Prometheus configuration and the scripts and fail when a
// property the deployment promises (hardening, loopback-only ports, pinned images, no secrets in files) is lost.
package deploy

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"gopkg.in/yaml.v3"
)

// root is the repository root, found from this file's location.
func root(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the repository")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func read(t testing.TB, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func readYAML(t testing.TB, rel string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := yaml.Unmarshal([]byte(read(t, rel)), &out); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return out
}
