package ciguard

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// scriptFunction returns the body of the shell function name in a bash script: from "name() {" to the first
// line that is exactly "}".
func scriptFunction(t *testing.T, script, name string) string {
	t.Helper()
	start := strings.Index(script, "\n"+name+"() {")
	if start < 0 {
		t.Fatalf("quality.sh has no function %s: update this test with the script", name)
	}
	rest := script[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("quality.sh function %s has no closing brace", name)
	}
	return rest[:end]
}

var testName = regexp.MustCompile(`\bTest[A-Za-z0-9_]+`)

// scripts/quality.ps1 is the Windows mirror of scripts/quality.sh. It cannot be run here, so this guard keeps it from
// drifting: every acceptance test the shell gate insists on in its integration and tracing stages must be named in the
// PowerShell gate too, and the stages that need an external tool must be present.
func TestQualityPS1MirrorsTheNamedTestsOfQualitySh(t *testing.T) {
	sh, err := os.ReadFile("../../scripts/quality.sh")
	if err != nil {
		t.Fatal(err)
	}
	ps, err := os.ReadFile("../../scripts/quality.ps1")
	if err != nil {
		t.Fatal(err)
	}
	script, mirror := string(sh), string(ps)
	for _, fn := range []string{"integration", "tracing_tests"} {
		names := testName.FindAllString(scriptFunction(t, script, fn), -1)
		if len(names) == 0 {
			t.Fatalf("no test names found in quality.sh %s: the guard would check nothing", fn)
		}
		for _, n := range names {
			if !regexp.MustCompile(`\b` + n + `\b`).MatchString(mirror) {
				t.Errorf("quality.sh %s requires %s but scripts/quality.ps1 does not name it", fn, n)
			}
		}
	}
	for _, want := range []string{"gofmt", "golangci-lint", "govulncheck", "go test -race", "go mod verify", "go mod tidy", "SERVERFLOW_TEST_KAFKA_BROKERS", "SERVERFLOW_TEST_POSTGRES_DSN", "SERVERFLOW_TEST_REDIS_ADDR"} {
		if !strings.Contains(mirror, want) {
			t.Errorf("scripts/quality.ps1 never mentions %q, which quality.sh uses", want)
		}
	}
}
