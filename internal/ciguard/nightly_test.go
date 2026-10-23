package ciguard

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The nightly workflow's job "race tests under CPU load" runs the race detector three times next to four processes that
// burn CPU. A test whose assertion is a performance budget (TestGatewayOverhead: the p95 latency the gateway adds) is
// invalid there, so the job skips it, and the workflow measures it in a job of its own and in every pull request's test
// steps. This file keeps that arrangement from rotting in either direction:
//
//   - the CPU-load job must keep skipping the budget test (otherwise the nightly is red again for a reason that says
//     nothing about the code), and every `go test` in it must pass the skip and an explicit -timeout;
//   - the budget test must keep running where it measures something: nowhere may it be skipped with -short or -skip,
//     the quiet nightly job must run it by name, and quality.sh must insist that it ran and passed (a skip would
//     otherwise look like a green build);
//   - the job's time limits must add up, so a step cannot be killed by a job limit shorter than its own.

const (
	budgetTest = "TestGatewayOverhead"
	stressJob  = "stress"
	quietJob   = "overhead"
)

type nightlyStep struct {
	Name           string `yaml:"name"`
	Run            string `yaml:"run"`
	TimeoutMinutes int    `yaml:"timeout-minutes"`
}

type nightlyJob struct {
	TimeoutMinutes int               `yaml:"timeout-minutes"`
	Env            map[string]string `yaml:"env"`
	Steps          []nightlyStep     `yaml:"steps"`
}

type nightlyFile struct {
	Jobs map[string]nightlyJob `yaml:"jobs"`
}

var (
	timeoutFlag = regexp.MustCompile(`(?:^|\s)-timeout[ =](\d+)m(?:\s|$)`)
	skipFlag    = regexp.MustCompile(`(?:^|\s)-skip[ =]("\$LOAD_SKIP"|\$LOAD_SKIP)(?:\s|$)`)
	shortFlag   = regexp.MustCompile(`(?:^|\s)-short(?:[ =]|\s|$)`)
)

// goTestLines returns the `go test` command lines of a run script (a line may continue the previous one with a backslash;
// the workflow does not use that, and a continued line is rejected below so it cannot hide a flag).
func goTestLines(run string) []string {
	var out []string
	for _, l := range strings.Split(run, "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "#") {
			continue
		}
		if strings.Contains(l, "go test") {
			out = append(out, l)
		}
	}
	return out
}

// nightlyProblems lists what is wrong with the text of nightly.yml.
func nightlyProblems(text string) []string {
	var f nightlyFile
	if err := yaml.Unmarshal([]byte(text), &f); err != nil {
		return []string{"nightly.yml does not parse: " + err.Error()}
	}
	var out []string

	stress, ok := f.Jobs[stressJob]
	if !ok {
		return []string{"nightly.yml has no job " + stressJob + " (race tests under CPU load): update this guard with the workflow"}
	}
	skip := stress.Env["LOAD_SKIP"]
	if skip == "" {
		out = append(out, "job "+stressJob+": env LOAD_SKIP is missing, so the budget test is not left out of the CPU-load run")
	} else if re, err := regexp.Compile(skip); err != nil {
		out = append(out, "job "+stressJob+": LOAD_SKIP is not a regular expression: "+err.Error())
	} else if !re.MatchString(budgetTest) {
		out = append(out, "job "+stressJob+": LOAD_SKIP "+skip+" does not match "+budgetTest+": it would fail every night under load")
	}
	// -skip is a regexp, so a pattern that matches everything would silently turn the job into a no-op.
	if skip != "" {
		if re, err := regexp.Compile(skip); err == nil {
			for _, other := range []string{"TestProcessAuthEndToEnd", "TestRealScenarios", "TestDifferentialAgainstRecordedFixtures", "TestTheOverheadHistogramExcludesTheQueueWait"} {
				if re.MatchString(other) {
					out = append(out, "job "+stressJob+": LOAD_SKIP "+skip+" also skips "+other+"; only performance-budget tests may be left out")
				}
			}
		}
	}

	total, ran := 0, 0
	for _, s := range stress.Steps {
		lines := goTestLines(s.Run)
		if len(lines) == 0 {
			continue
		}
		ran += len(lines)
		total += s.TimeoutMinutes
		for _, l := range lines {
			where := "job " + stressJob + ", step \"" + s.Name + "\": "
			if strings.HasSuffix(l, `\`) {
				out = append(out, where+"a continued go test line cannot be checked; write it on one line")
			}
			if !skipFlag.MatchString(l) {
				out = append(out, where+`go test lacks -skip "$LOAD_SKIP": `+l)
			}
			if shortFlag.MatchString(l) {
				out = append(out, where+"go test uses -short, which would skip far more than the budget test: "+l)
			}
			m := timeoutFlag.FindStringSubmatch(l)
			if m == nil {
				out = append(out, where+"go test has no explicit -timeout (the 10 minute default per package cannot hold -count=3): "+l)
			} else if s.TimeoutMinutes == 0 {
				out = append(out, where+"the step has no timeout-minutes")
			} else if limit := mustAtoi(m[1]); limit >= s.TimeoutMinutes {
				out = append(out, where+"-timeout "+m[1]+"m is not shorter than the step's timeout-minutes "+strconv.Itoa(s.TimeoutMinutes)+": the runner would kill the step before go test could print its goroutine dump")
			}
		}
	}
	if ran == 0 {
		out = append(out, "job "+stressJob+" runs no go test: the guard would check nothing")
	}
	if total >= stress.TimeoutMinutes {
		out = append(out, "job "+stressJob+": the step timeouts add up to "+strconv.Itoa(total)+" minutes, not less than the job's timeout-minutes "+strconv.Itoa(stress.TimeoutMinutes))
	}

	quiet, ok := f.Jobs[quietJob]
	if !ok {
		return append(out, "nightly.yml has no job "+quietJob+": the budget test would not be measured anywhere at night")
	}
	found := false
	for _, s := range quiet.Steps {
		for _, l := range goTestLines(s.Run) {
			if !regexp.MustCompile(`(?:^|\s)-run[ =]` + budgetTest + `(?:\s|$)`).MatchString(l) {
				continue
			}
			found = true
			if shortFlag.MatchString(l) || strings.Contains(l, "-skip") || strings.Contains(l, "-race") {
				out = append(out, "job "+quietJob+": the budget test must run without -short, -skip and -race: "+l)
			}
		}
	}
	if !found {
		out = append(out, "job "+quietJob+" does not run "+budgetTest+" by name")
	}
	return out
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestNightlyLeavesTheBudgetTestOutOfTheLoadJobAndTimesItsSteps(t *testing.T) {
	for _, p := range nightlyProblems(readFile(t, "../../.github/workflows/nightly.yml")) {
		t.Error(p)
	}
}

// The budget test is not skipped anywhere that is meant to measure it: no workflow or script passes -short, or -skip with
// its name, outside the one CPU-load job (which is checked above), and quality.sh insists that it ran and passed.
func TestTheBudgetTestIsNeverSilentlySkippedWhereItMeasuresSomething(t *testing.T) {
	files := []string{"../../.github/workflows/ci.yml", "../../.github/workflows/kubernetes.yml", "../../scripts/quality.sh", "../../scripts/quality.ps1", "../../Makefile"}
	for _, f := range files {
		for i, l := range strings.Split(readFile(t, f), "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), "#") || !strings.Contains(l, "go test") {
				continue
			}
			if shortFlag.MatchString(l) || strings.Contains(l, "-skip") {
				t.Errorf("%s:%d: go test with -short or -skip would leave %s out of a run that must measure it: %s", f, i+1, budgetTest, strings.TrimSpace(l))
			}
		}
	}
	sh := readFile(t, "../../scripts/quality.sh")
	if !regexp.MustCompile(`\bTestGatewayOverhead\b`).MatchString(scriptFunction(t, sh, "tracing_tests")) {
		t.Error("quality.sh tracing_tests does not name " + budgetTest + ": a skip of it would pass as green")
	}
	// The test skips itself only under -short (so that `go test -short` stays quick) and nowhere else.
	src := readFile(t, "../gateway/overhead_test.go")
	start := strings.Index(src, "func "+budgetTest+"(")
	if start < 0 {
		t.Fatalf("internal/gateway/overhead_test.go has no %s: update this guard", budgetTest)
	}
	body := src[start:]
	if end := strings.Index(body[1:], "\nfunc "); end >= 0 {
		body = body[:end+1]
	}
	if n := strings.Count(body, ".Skip"); n != 1 || !regexp.MustCompile(`if testing\.Short\(\) \{\s*t\.Skip\(`).MatchString(body) {
		t.Errorf("%s must skip only under testing.Short(), found %d skip calls", budgetTest, n)
	}
}

// The guard itself: each way of undoing the arrangement must be reported.
func TestNightlyGuardCatchesTheMistakes(t *testing.T) {
	text := readFile(t, "../../.github/workflows/nightly.yml")
	if p := nightlyProblems(text); len(p) != 0 {
		t.Fatalf("the real workflow is not clean, the mutations below prove nothing: %v", p)
	}
	mutations := map[string]func(string) string{
		"LOAD_SKIP removed": func(s string) string { return strings.Replace(s, "      LOAD_SKIP: ^(TestGatewayOverhead)$\n", "", 1) },
		"LOAD_SKIP names another": func(s string) string {
			return strings.Replace(s, "^(TestGatewayOverhead)$", "^(TestSomethingElse)$", 1)
		},
		"LOAD_SKIP matches too much": func(s string) string {
			return strings.Replace(s, "^(TestGatewayOverhead)$", "^(TestGatewayOverhead|TestProcess.*|TestReal.*)$", 1)
		},
		"-skip dropped from the integration step": func(s string) string {
			return strings.Replace(s, `-timeout 60m -skip "$LOAD_SKIP" ./tests/integration`, `-timeout 60m ./tests/integration`, 1)
		},
		"-skip dropped from the other step": func(s string) string {
			return strings.Replace(s, `-timeout 30m -skip "$LOAD_SKIP" `, `-timeout 30m `, 1)
		},
		"-timeout dropped": func(s string) string { return strings.Replace(s, "-timeout 60m ", "", 1) },
		"-timeout equals the step limit": func(s string) string {
			return strings.Replace(s, "-timeout 60m", "-timeout 65m", 1)
		},
		"job limit too short": func(s string) string { return strings.Replace(s, "timeout-minutes: 125", "timeout-minutes: 100", 1) },
		"-short added": func(s string) string {
			return strings.Replace(s, "-count=3 -timeout 60m", "-count=3 -short -timeout 60m", 1)
		},
		"overhead job dropped": func(s string) string {
			return strings.Replace(s, "-run TestGatewayOverhead ./internal/gateway", "./internal/gateway", 1)
		},
		"overhead job skips it": func(s string) string {
			return strings.Replace(s, "-run TestGatewayOverhead ./internal/gateway", "-short -run TestGatewayOverhead ./internal/gateway", 1)
		},
	}
	for name, mutate := range mutations {
		mutated := mutate(text)
		if mutated == text {
			t.Errorf("%s: the mutation changed nothing (the workflow text moved: update this test)", name)
			continue
		}
		if len(nightlyProblems(mutated)) == 0 {
			t.Errorf("%s: not caught", name)
		}
	}
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 1 << 30
	}
	return n
}
