package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/report"
	"serverflow/internal/bench/verdict"
)

var update = flag.Bool("update", false, "rewrite testdata/verdict.golden")

func vblock(j string) string { return "```verdict\n" + j + "\n```\n" }

func seeds(n int) string {
	var s []string
	for i := 1; i <= n; i++ {
		s = append(s, fmt.Sprint(i))
	}
	return "[" + strings.Join(s, ",") + "]"
}

func cmp(id, kind, workload, a, b string, n int, extra string) string {
	return vblock(fmt.Sprintf(`{"id":%q,"kind":%q,"claim":"%s vs %s on %s","metric":"latency_p95","a":{"scheduler":%q,"workload":%q},"b":{"scheduler":%q,"workload":%q},"seeds":%s,%s}`,
		id, kind, a, b, workload, a, workload, b, workload, seeds(n), extra))
}

// writeVerdictRuns writes baseline (100 ms) and treatment (100*(1-effect)) runs for seeds 1..len(effects).
func writeVerdictRuns(t *testing.T, dir, workload, a, b string, effects []float64) {
	t.Helper()
	for i, e := range effects {
		for _, arm := range []struct {
			s   string
			p95 float64
		}{{a, 100}, {b, 100 * (1 - e)}} {
			id, rd, err := report.CreateRun(dir)
			if err != nil {
				t.Fatal(err)
			}
			r := report.Result{SchemaVersion: report.SchemaVersion, RunID: id, Valid: true, NotMeasured: map[string]string{}}
			r.Metadata = report.Metadata{Date: "d", Scheduler: arm.s, Workload: workload, Seed: int64(i + 1), Repeat: report.Repeat{Index: 1, Of: 1, Group: id},
				Experiment: &report.Experiment{Strategy: arm.s, World: "sim:fast", ParameterHash: "abc", Options: &report.SchedulerOptions{LoadOverlay: "max", OutputFraction: 1, InputWeight: 1}}}
			r.Summary = collect.Summary{Sent: 10, Succeeded: 10, Latency: &collect.Dist{P95: arm.p95}}
			if err := report.Write(rd, r, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
}

type verdictFixture struct{ dir, predictions, freeze, runs string }

func newVerdictFixture(t *testing.T) verdictFixture {
	t.Helper()
	dir := t.TempDir()
	f := verdictFixture{dir: dir, predictions: filepath.Join(dir, "predictions.md"), freeze: filepath.Join(dir, "freeze.json"), runs: filepath.Join(dir, "runs")}
	pred := "# Predictions (test fixture)\n\n" +
		cmp("H2", "equivalence", "uniform-short", "least-active", "least-tokens", 8, `"margin":0.05`) +
		cmp("H3", "superiority", "heavy-tail", "least-active", "least-tokens", 10, `"direction":"lower","confirmatory":true,"confirm_min_effect":0.10,"refute_max_effect":0.05`) +
		cmp("H4", "superiority", "mixed", "least-tokens", "least-work", 8, `"direction":"lower","confirm_min_effect":0.15,"refute_max_effect":0.05`) +
		cmp("H7", "superiority", "burst", "least-work", "least-tokens", 8, `"direction":"lower","confirm_min_effect":0.10,"refute_max_effect":0.05`) +
		"\n" + verdict.AmendmentsHeading + "\n\nNone.\n"
	must(t, os.WriteFile(f.predictions, []byte(pred), 0o600))
	sum := sha256.Sum256([]byte(verdict.FrozenText([]byte(pred))))
	must(t, os.WriteFile(f.freeze, []byte(fmt.Sprintf(`{"predictions_sha256":%q,"frozen_at":"2026-11-02T10:00:00Z"}`, hex.EncodeToString(sum[:]))), 0o600))
	must(t, os.Mkdir(f.runs, 0o700))
	writeVerdictRuns(t, f.runs, "uniform-short", "least-active", "least-tokens", []float64{0.01, -0.01, 0.02, 0, -0.02, 0.01, -0.005, 0.005})
	writeVerdictRuns(t, f.runs, "heavy-tail", "least-active", "least-tokens", []float64{0.14, 0.15, 0.16, 0.15, 0.17, 0.13, 0.15, 0.16, 0.14, 0.15})
	writeVerdictRuns(t, f.runs, "mixed", "least-tokens", "least-work", []float64{0.01, 0.02, 0.015, 0.02, 0.01, 0.025, 0.02, 0.015})
	writeVerdictRuns(t, f.runs, "burst", "least-work", "least-tokens", []float64{-0.2, 0.3, -0.1, 0.25, -0.3, 0.2, 0.1, -0.15})
	return f
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestVerdictGolden(t *testing.T) {
	f := newVerdictFixture(t)
	code, out, errOut := exec(t, "verdict", "--predictions", f.predictions, "--freeze", f.freeze, "--runs", f.runs)
	if code != 0 || errOut != "" {
		t.Fatalf("%d %q\n%s", code, errOut, out)
	}
	golden := filepath.Join("testdata", "verdict.golden")
	if *update {
		must(t, os.WriteFile(golden, []byte(out), 0o644))
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if out != string(want) {
		t.Fatalf("output differs from %s (rerun with -update to accept a deliberate change)\n--- got\n%s\n--- want\n%s", golden, out, want)
	}
	// Same inputs, same bytes.
	_, again, _ := exec(t, "verdict", "--predictions", f.predictions, "--freeze", f.freeze, "--runs", f.runs)
	if again != out {
		t.Fatal("not deterministic")
	}
	for _, s := range []string{"simulator-validated, not hardware-validated", "H2  equivalence  CONFIRMED", "H3  superiority  CONFIRMED", "H4  superiority  REFUTED", "H7  superiority  INCONCLUSIVE"} {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q", s)
		}
	}
	// The tool never edits a prediction or the freeze.
	if b, _ := os.ReadFile(f.predictions); !strings.Contains(string(b), `"id":"H3"`) {
		t.Fatal("predictions changed")
	}
}

func TestVerdictFailsOnInvalidEvidenceAndOnEditedPredictions(t *testing.T) {
	f := newVerdictFixture(t)
	// Edit a threshold after the freeze.
	b, _ := os.ReadFile(f.predictions)
	edited := strings.Replace(string(b), `"confirm_min_effect":0.10`, `"confirm_min_effect":0.05`, 1)
	must(t, os.WriteFile(f.predictions, []byte(edited), 0o600))
	code, out, errOut := exec(t, "verdict", "--predictions", f.predictions, "--freeze", f.freeze, "--runs", f.runs)
	if code != 1 || !strings.Contains(out, "PREDICTIONS CHANGED AFTER THE FREEZE") || !strings.Contains(errOut, "INVALID") || strings.Contains(out, "CONFIRMED") {
		t.Fatalf("%d %q\n%s", code, errOut, out)
	}
	// A missing group.
	f = newVerdictFixture(t)
	ents, _ := os.ReadDir(f.runs)
	for _, e := range ents[:2] {
		must(t, os.RemoveAll(filepath.Join(f.runs, e.Name())))
	}
	code, out, errOut = exec(t, "verdict", "--predictions", f.predictions, "--freeze", f.freeze, "--runs", f.runs)
	if code != 1 || !strings.Contains(out, "INVALID") || !strings.Contains(errOut, "INVALID") {
		t.Fatalf("%d %q\n%s", code, errOut, out)
	}
}

func TestVerdictFlagsAndInputChecks(t *testing.T) {
	f := newVerdictFixture(t)
	for _, c := range []struct {
		args []string
		want string
	}{
		{nil, "needs --predictions"},
		{[]string{"--predictions", f.predictions}, "needs --runs"},
		{[]string{"--predictions", f.predictions, "--runs", f.runs}, "needs --freeze"},
		{[]string{"--predictions", f.predictions, "--runs", f.runs, "--freeze", filepath.Join(f.dir, "none.json")}, "no such file"},
		{[]string{"--predictions", f.predictions, "--runs", filepath.Join(f.dir, "none"), "--freeze", f.freeze}, "no such file"},
		{[]string{"--predictions", f.predictions, "--runs", f.runs, "--freeze", f.freeze, "stray"}, "flags only"},
		{[]string{"--bogus"}, "usage"},
		{[]string{"--predictions", f.predictions, "--runs", f.runs, "--freeze", f.freeze, "--tuned", f.freeze}, "tuned values"},
	} {
		code, _, errOut := exec(t, append([]string{"verdict"}, c.args...)...)
		if code != 1 || !strings.Contains(errOut, c.want) {
			t.Errorf("%v: %d %q, want %q", c.args, code, errOut, c.want)
		}
	}
	// The hash a freeze record pins.
	code, out, _ := exec(t, "verdict", "--predictions", f.predictions, "--print-hash")
	b, _ := os.ReadFile(f.freeze)
	if code != 0 || !strings.Contains(string(b), strings.TrimSpace(out)) || len(strings.TrimSpace(out)) != 64 {
		t.Fatalf("%d %q", code, out)
	}
	// Unfrozen smoke check is allowed, and said so.
	code, out, _ = exec(t, "verdict", "--predictions", f.predictions, "--runs", f.runs, "--allow-unfrozen")
	if code != 0 || !strings.Contains(out, "NO FREEZE RECORD") {
		t.Fatalf("%d %s", code, out)
	}
	// A symbolic link is not followed.
	link := filepath.Join(f.dir, "link.md")
	must(t, os.Symlink(f.predictions, link))
	if code, _, errOut := exec(t, "verdict", "--predictions", link, "--print-hash"); code != 1 || !strings.Contains(errOut, "not a regular file") {
		t.Fatalf("%d %q", code, errOut)
	}
	if code, _, errOut := exec(t, "help"); code != 0 || errOut != "" {
		t.Fatal("help")
	}
	if _, out, _ := exec(t, "help"); !strings.Contains(out, "benchmark verdict") {
		t.Fatal("usage lists verdict")
	}
}
