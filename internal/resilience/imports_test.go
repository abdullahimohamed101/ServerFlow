package resilience

import (
	"os/exec"
	"strings"
	"testing"
)

// The package is a pure library: the gateway adapts it, never the other way round. Its only non-standard import is
// the configuration package, and nothing it depends on (directly or not) may be the gateway.
func TestResilienceDoesNotImportTheGateway(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not on PATH")
	}
	out, err := exec.Command(goBin, "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Skipf("go list failed (offline module cache?): %v\n%s", err, out)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if strings.HasPrefix(pkg, "serverflow/internal/gateway") {
			t.Errorf("internal/resilience depends on %s", pkg)
		}
	}
}

func TestResilienceImportsOnlyTheStandardLibraryAndConfig(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not on PATH")
	}
	out, err := exec.Command(goBin, "list", "-f", `{{join .Imports "\n"}}`, ".").CombinedOutput()
	if err != nil {
		t.Skipf("go list failed: %v\n%s", err, out)
	}
	for _, pkg := range strings.Fields(string(out)) {
		first, _, _ := strings.Cut(pkg, "/")
		isStd := !strings.Contains(first, ".") && first != "serverflow"
		if !isStd && pkg != "serverflow/internal/config" {
			t.Errorf("internal/resilience imports %s; only the standard library and internal/config are allowed", pkg)
		}
	}
}
