package ciguard

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// scripts/quality.ps1 is the Windows mirror of scripts/quality.sh. It cannot be run in CI or on the machines that
// develop this repository, so this file is a cheap drift check, not a behavioural test of the script: it checks that the
// acceptance test names quality.sh insists on are named in the .ps1, and that the .ps1 still contains the actual command
// invocations (not just words that may sit in a comment or a message) of the stages it claims to mirror. It cannot tell
// whether the PowerShell around them is correct.

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

// code returns the PowerShell text without whole-line comments.
func code(ps string) string {
	var out []string
	for _, l := range strings.Split(ps, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

// invocations are the command lines the .ps1 must contain, each matched as a whole non-comment line so a mention in a
// message or a comment does not count. what names the stage for the failure message.
var invocations = []struct{ what, re string }{
	{"gofmt -l", `(?m)^[ \t]*\$\w+\s*=\s*gofmt -l \.[ \t]*$`},
	{"golangci-lint run", `(?m)^[ \t]*golangci-lint run \./\.\.\.[ \t]*$`},
	{"go test -race", `(?m)^[ \t]*go test -race @GoPackages[ \t]*$`},
	{"-race for the must-run stages", `(?m)^[ \t]*\$RaceFlags\s*=\s*@\("-race"\)[ \t]*$`},
	{"go vet", `(?m)^[ \t]*go vet @GoPackages[ \t]*$`},
	{"go test", `(?m)^[ \t]*go test @GoPackages[ \t]*$`},
	{"go build", `(?m)^[ \t]*go build \./\.\.\.[ \t]*$`},
	{"the cross-compile loop", `(?m)^[ \t]*foreach \(\$target in @\([^\n]*"linux/amd64"[^\n]*"linux/arm64"[^\n]*"darwin/arm64"[^\n]*"windows/amd64"[^\n]*\)\) \{[ \t]*$`},
	{"GOOS set in the cross-compile loop", `(?m)^[ \t]*\$env:GOOS\s*=\s*\$targetOS[ \t]*$`},
	{"GOARCH set in the cross-compile loop", `(?m)^[ \t]*\$env:GOARCH\s*=\s*\$targetArch[ \t]*$`},
	{"go mod verify", `(?m)^[ \t]*go mod verify[ \t]*$`},
	{"go mod tidy -diff", `(?m)^[ \t]*go mod tidy -diff[ \t]*$`},
	{"govulncheck", `(?m)^[ \t]*govulncheck \./\.\.\.[ \t]*$`},
	{"the must-run helper call for tracing", `(?m)^[ \t]*-Label "tracing" ` + "`" + `[ \t]*$`},
	{"the must-run helper call for postgres", `(?m)^[ \t]*-Label "postgres" ` + "`" + `[ \t]*$`},
	{"the must-run helper call for redis", `(?m)^[ \t]*-Label "redis" ` + "`" + `[ \t]*$`},
	{"the must-run helper call for kafka", `(?m)^[ \t]*-Label "kafka" ` + "`" + `[ \t]*$`},
	{"the must-run helper call for observability", `(?m)^[ \t]*-Label "observability" ` + "`" + `[ \t]*$`},
	{"the integration servers' env gates", `(?m)^[ \t]*\$HasKafka\s*=.*\$env:SERVERFLOW_TEST_KAFKA_BROKERS`},
	{"the Postgres env gate", `(?m)^[ \t]*\$HasPostgres\s*=.*\$env:SERVERFLOW_TEST_POSTGRES_DSN`},
	{"the Redis env gate", `(?m)^[ \t]*\$HasRedis\s*=.*\$env:SERVERFLOW_TEST_REDIS_ADDR`},
	{"the declared skip of deploy", `(?m)^[ \t]*"Not mirrored: [^"\n]*quality\.sh deploy"[ \t]*$`},
	{"the declared skip of observability", `(?m)^[ \t]*"Not mirrored: [^"\n]*quality\.sh observability"[ \t]*$`},
}

// parityProblems lists what is wrong with the .ps1 text relative to the shell script. Empty means in step.
func parityProblems(t *testing.T, script, ps string) []string {
	t.Helper()
	var problems []string
	body := code(ps)
	for _, fn := range []string{"integration", "tracing_tests", "observability_integration"} {
		names := testName.FindAllString(scriptFunction(t, script, fn), -1)
		if len(names) == 0 {
			t.Fatalf("no test names found in quality.sh %s: the guard would check nothing", fn)
		}
		for _, n := range names {
			if !regexp.MustCompile(`"` + n + `"`).MatchString(body) {
				problems = append(problems, "quality.sh "+fn+" requires "+n+" but quality.ps1 does not list it as a string")
			}
		}
	}
	// deploy cannot be mirrored (hadolint, shellcheck, docker compose, bash). Its test list is only read so a change of
	// the script's shape is noticed; the .ps1 must declare the skip (see invocations).
	if len(testName.FindAllString(scriptFunction(t, script, "deploy"), -1)) == 0 {
		t.Fatal("no test names found in quality.sh deploy: the guard would check nothing")
	}
	for _, inv := range invocations {
		if !regexp.MustCompile(inv.re).MatchString(body) {
			problems = append(problems, "quality.ps1 has no "+inv.what+" invocation")
		}
	}
	return problems
}

func readScripts(t *testing.T) (string, string) {
	t.Helper()
	sh, err := os.ReadFile("../../scripts/quality.sh")
	if err != nil {
		t.Fatal(err)
	}
	ps, err := os.ReadFile("../../scripts/quality.ps1")
	if err != nil {
		t.Fatal(err)
	}
	return string(sh), string(ps)
}

func TestQualityPS1MirrorsQualitySh(t *testing.T) {
	sh, ps := readScripts(t)
	for _, p := range parityProblems(t, sh, ps) {
		t.Error(p)
	}
}

// The guard itself: removing any mirrored invocation (every line the pattern matches), or commenting it out, or
// dropping one named test, must be reported.
func TestQualityPS1GuardCatchesDeletions(t *testing.T) {
	sh, ps := readScripts(t)
	for _, inv := range invocations {
		re := regexp.MustCompile(inv.re)
		if !re.MatchString(ps) {
			t.Fatalf("pattern for %s matches nothing in quality.ps1", inv.what)
		}
		deleted := re.ReplaceAllString(ps, "")
		if len(parityProblems(t, sh, deleted)) == 0 {
			t.Errorf("deleting the %s invocation was not noticed", inv.what)
		}
		commented := re.ReplaceAllStringFunc(ps, func(m string) string { return "# " + strings.TrimSpace(m) })
		if len(parityProblems(t, sh, commented)) == 0 {
			t.Errorf("commenting out the %s invocation was not noticed", inv.what)
		}
	}
	for _, fn := range []string{"integration", "tracing_tests", "observability_integration"} {
		for _, n := range testName.FindAllString(scriptFunction(t, sh, fn), -1) {
			if len(parityProblems(t, sh, strings.ReplaceAll(ps, `"`+n+`"`, `"x"`))) == 0 {
				t.Errorf("dropping %s from quality.ps1 was not noticed", n)
			}
		}
	}
	// A mention in a comment or in a message is not an invocation.
	if len(parityProblems(t, sh, strings.ReplaceAll(ps, "govulncheck ./...", "# govulncheck ./..."))) == 0 {
		t.Error("a commented-out govulncheck was accepted")
	}
}
