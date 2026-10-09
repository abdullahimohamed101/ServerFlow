package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/report"
)

func exec(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var o, e bytes.Buffer
	code = execute(context.Background(), args, &o, &e)
	return code, o.String(), e.String()
}

func TestUsageAndUnknownCommands(t *testing.T) {
	if code, _, errOut := exec(t); code != 2 || !strings.Contains(errOut, "benchmark run") {
		t.Fatalf("%d %q", code, errOut)
	}
	if code, _, errOut := exec(t, "frobnicate"); code != 2 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("%d %q", code, errOut)
	}
	if code, out, _ := exec(t, "help"); code != 0 || !strings.Contains(out, "compare") {
		t.Fatalf("%d %q", code, out)
	}
}

func TestRunRejectsABadFlagWithAClearErrorAndNonZeroExit(t *testing.T) {
	code, _, errOut := exec(t, "run", "--workload", "nope")
	if code != 1 || !strings.Contains(errOut, "unknown workload") {
		t.Fatalf("%d %q", code, errOut)
	}
	if code, _, errOut := exec(t, "run", "--concurrency", "5", "--rate", "5"); code != 1 || !strings.Contains(errOut, "mutually exclusive") {
		t.Fatalf("%d %q", code, errOut)
	}
}

func writeRun(t *testing.T, base, scheduler string, rps float64, group string, idx, of int) string {
	t.Helper()
	id, dir, err := report.CreateRun(base)
	if err != nil {
		t.Fatal(err)
	}
	n := 3
	r := report.Result{SchemaVersion: report.SchemaVersion, RunID: id, Valid: true, NotMeasured: map[string]string{}}
	r.Metadata = report.Metadata{Date: "d", Scheduler: scheduler, Workload: "mixed", Mode: "closed", Concurrency: 4, Seed: 1,
		GitCommit: "abc", GitTree: "clean", WorkerCount: &n, Models: []string{"m"}, Repeat: report.Repeat{Index: idx, Of: of, Group: group}}
	r.Summary = collect.Summary{Sent: 10, Succeeded: 10, RequestsPerSecond: rps, Latency: &collect.Dist{P95: 100}}
	if err := report.Write(dir, r, nil); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCompareAndListWorkOnWrittenRuns(t *testing.T) {
	base := t.TempDir()
	a := writeRun(t, base, "round-robin", 100, "", 1, 1)
	b := writeRun(t, base, "least-active", 110, "", 1, 1)

	code, out, errOut := exec(t, "compare", "--dir", base, a, b)
	if code != 0 || !strings.Contains(out, "+10.00%") || !strings.Contains(out, "scheduler: round-robin -> least-active") || strings.Contains(out, "WARNING") {
		t.Fatalf("%d %q %q", code, out, errOut)
	}
	// A run compared with itself has zero deltas.
	if _, out, _ := exec(t, "compare", "--dir", base, a, a); !strings.Contains(out, "+0.00%") || strings.Contains(out, "+10.00%") {
		t.Fatalf("%s", out)
	}
	// By path.
	if code, out, _ := exec(t, "compare", filepath.Join(base, a), filepath.Join(base, b, "result.json")); code != 0 || !strings.Contains(out, "+10.00%") {
		t.Fatalf("%d %s", code, out)
	}
	code, out, _ = exec(t, "list", "--dir", base)
	if code != 0 || !strings.Contains(out, a) || !strings.Contains(out, "least-active") {
		t.Fatalf("%d %s", code, out)
	}
	if _, out, _ := exec(t, "list", "--dir", filepath.Join(base, "none")); !strings.Contains(out, "no runs") {
		t.Fatalf("%s", out)
	}
}

func TestCompareUsesRepeatGroupsOfThreeForSpreadAndMediansForTheDelta(t *testing.T) {
	base := t.TempDir()
	a1 := writeRun(t, base, "round-robin", 100, "g1", 1, 3)
	writeRun(t, base, "round-robin", 104, "g1", 2, 3)
	writeRun(t, base, "round-robin", 98, "g1", 3, 3)
	b1 := writeRun(t, base, "least-active", 200, "g2", 1, 3)
	writeRun(t, base, "least-active", 204, "g2", 2, 3)
	writeRun(t, base, "least-active", 198, "g2", 3, 3)
	_, out, _ := exec(t, "compare", "--dir", base, a1, b1)
	// medians 100 and 200: +100%, although the two named runs are 100 and 200 only by coincidence of order.
	if !strings.Contains(out, "ranges do not overlap") || !strings.Contains(out, "+100.00%") || !strings.Contains(out, "medians of 3 and 3") {
		t.Fatalf("%s", out)
	}
	// Two repeats are not enough for a verdict.
	c1 := writeRun(t, base, "x", 100, "g3", 1, 2)
	writeRun(t, base, "x", 101, "g3", 2, 2)
	_, out, _ = exec(t, "compare", "--dir", base, c1, b1)
	if !strings.Contains(out, "insufficient repeats") || strings.Contains(out, "ranges") {
		t.Fatalf("%s", out)
	}
}

func TestCompareWarnsAboutAnInvalidRunAboveTheTableAndSurvivesABadSibling(t *testing.T) {
	base := t.TempDir()
	a := writeRun(t, base, "round-robin", 100, "", 1, 1)
	b := writeRun(t, base, "least-active", 110, "", 1, 1)
	// Mark b invalid on disk.
	r, _ := report.Load(filepath.Join(base, b))
	r.Valid, r.InvalidReasons = false, []string{"error rate 50.00% exceeds 5.00%"}
	if err := report.Write(filepath.Join(base, b), r, nil); err != nil {
		t.Fatal(err)
	}
	// A corrupt run directory beside them must not stop the comparison.
	_ = os.MkdirAll(filepath.Join(base, "run_009"), 0o755)
	_ = os.WriteFile(filepath.Join(base, "run_009", "result.json"), []byte("{broken"), 0o644)
	code, out, errOut := exec(t, "compare", "--dir", base, a, b)
	if code != 0 || !strings.HasPrefix(out, "WARNING: "+b+" is INVALID") {
		t.Fatalf("%d %q %q", code, out, errOut)
	}
}

func TestCompareErrors(t *testing.T) {
	base := t.TempDir()
	a := writeRun(t, base, "x", 1, "", 1, 1)
	for name, args := range map[string][]string{
		"one arg":     {"compare", "--dir", base, a},
		"three args":  {"compare", "--dir", base, a, a, a},
		"missing run": {"compare", "--dir", base, a, "run_099"},
		"not a run":   {"compare", "--dir", base, a, "nonsense"},
		"bad flag":    {"compare", "--nope"},
		"list arg":    {"list", "--dir", base, "extra"},
	} {
		if code, _, errOut := exec(t, args...); code != 1 || errOut == "" {
			t.Errorf("%s: code %d %q", name, code, errOut)
		}
	}
}
