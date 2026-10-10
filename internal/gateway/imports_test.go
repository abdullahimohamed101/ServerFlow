package gateway

import (
	"os/exec"
	"strings"
	"testing"
)

// OpenTelemetry may be imported only under internal/tracing (ADR-018), so the request path stays free of
// it: no import of the gateway package, direct or transitive, may name go.opentelemetry.io.
func TestGatewayDoesNotDependOnOpenTelemetry(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not on PATH")
	}
	out, err := exec.Command(goBin, "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Skipf("go list failed (offline module cache?): %v\n%s", err, out)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if strings.HasPrefix(pkg, "go.opentelemetry.io") {
			t.Errorf("internal/gateway depends on %s; OpenTelemetry belongs under internal/tracing only", pkg)
		}
	}
}
