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

// TestDeploymentYAMLParses: every YAML file of the deployment parses with the repository's YAML library, so a syntax
// error fails `quality.sh deploy` whether or not PyYAML is installed.
func TestDeploymentYAMLParses(t *testing.T) {
	files := []string{
		"docker-compose.yml", "docker-compose.persist.yml", "docker-compose.scale.yml", "docker-compose.private-control-plane.yml",
		"observability/prometheus/compose/prometheus.yml", "observability/prometheus/compose/targets.yml",
		".github/workflows/ci.yml", ".github/workflows/nightly.yml", ".github/dependabot.yml",
	}
	for _, f := range files {
		var out any
		if err := yaml.Unmarshal([]byte(read(t, f)), &out); err != nil || out == nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
