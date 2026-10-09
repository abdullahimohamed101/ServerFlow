package runner

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"serverflow/internal/bench/report"
)

func quickOptions(t *testing.T, extra ...string) Options {
	t.Helper()
	args := append([]string{"--out", t.TempDir(), "--workers", "3", "--duration", "1s", "--warmup", "0s", "--concurrency", "4",
		"--mock-tps", "5000", "--mock-ttft", "1ms", "--sample-interval", "50ms"}, extra...)
	rf, err := ParseRun(args)
	if err != nil {
		t.Fatal(err)
	}
	return rf.Options
}

func TestRunWritesACompleteSelfConsistentResult(t *testing.T) {
	o := quickOptions(t, "--workload", "uniform-short", "--save-requests")
	var out bytes.Buffer
	rs, err := Run(context.Background(), o, &out)
	if err != nil || len(rs) != 1 {
		t.Fatalf("%v %v", rs, err)
	}
	r := rs[0]
	if r.RunID != "run_001" {
		t.Fatal(r.RunID)
	}
	for _, f := range []string{report.ResultFile, report.ReportFile, report.RequestsFile} {
		if _, err := os.Stat(filepath.Join(o.OutDir, "run_001", f)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	s := r.Summary
	if s.Sent == 0 || s.Sent != s.Succeeded+s.Failed || s.Failed != 0 {
		t.Fatalf("%+v", s)
	}
	var completed int64
	for _, w := range r.Workers {
		if w.Completed == nil {
			t.Fatalf("every embedded worker's count must be measured: %+v", w)
		}
		completed += *w.Completed
	}
	if completed != int64(s.Succeeded) {
		t.Fatalf("per-worker completed counts sum to %d, successes are %d", completed, s.Succeeded)
	}
	if r.Queue == nil || r.Imbalance.RequestJain == nil {
		t.Fatalf("queue and balance must be measured in an embedded run: %v", r.NotMeasured)
	}
	if got := r.Metadata.Missing(); len(got) != 0 {
		t.Fatalf("metadata incomplete: %v", got)
	}
	if r.Metadata.Cluster == nil || len(r.Metadata.Cluster.Workers) != 3 || *r.Metadata.WorkerCount != 3 {
		t.Fatalf("%+v", r.Metadata)
	}
	loaded, err := report.Load(filepath.Join(o.OutDir, "run_001"))
	if err != nil || loaded.Summary.Sent != s.Sent {
		t.Fatalf("%v", err)
	}
	if strings.Contains(out.String(), "bench-") || strings.Contains(out.String(), "sk-bench") {
		t.Fatal("no secret may reach the output")
	}
	for _, f := range []string{report.ResultFile, report.ReportFile, report.RequestsFile} {
		b, _ := os.ReadFile(filepath.Join(o.OutDir, "run_001", f))
		if bytes.Contains(b, []byte("sk-bench")) || bytes.Contains(b, []byte("Bearer")) {
			t.Errorf("%s contains a credential", f)
		}
	}
}

func TestRepeatWritesOneRunPerRepeatInOneGroupAndPrintsTheSpread(t *testing.T) {
	o := quickOptions(t, "--workload", "uniform-short", "--repeat", "2", "--duration", "500ms")
	var out bytes.Buffer
	rs, err := Run(context.Background(), o, &out)
	if err != nil || len(rs) != 2 {
		t.Fatalf("%v %v", rs, err)
	}
	if rs[0].RunID != "run_001" || rs[1].RunID != "run_002" || rs[0].Metadata.Repeat != (report.Repeat{Index: 1, Of: 2, Group: "run_001"}) ||
		rs[1].Metadata.Repeat != (report.Repeat{Index: 2, Of: 2, Group: "run_001"}) {
		t.Fatalf("%+v %+v", rs[0].Metadata.Repeat, rs[1].Metadata.Repeat)
	}
	if rs[0].Metadata.PlanDigest != rs[1].Metadata.PlanDigest {
		t.Fatal("the same seed must offer the same load")
	}
	if !strings.Contains(out.String(), "Spread across 2 repeats") {
		t.Fatalf("%s", out.String())
	}
	g, err := report.LoadGroup(o.OutDir, rs[1])
	if err != nil || len(g) != 2 {
		t.Fatalf("%d %v", len(g), err)
	}
}

func TestAnInterruptedRunWritesNothingAndRetiresItsID(t *testing.T) {
	o := quickOptions(t, "--duration", "1m")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(700*time.Millisecond, cancel)
	if _, err := Run(ctx, o, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("%v", err)
	}
	ids, _ := report.RunIDs(o.OutDir)
	if len(ids) != 0 {
		t.Fatalf("no partial result may be left behind: %v", ids)
	}
	if id, _, _ := report.CreateRun(o.OutDir); id != "run_002" {
		t.Fatalf("the interrupted run's ID must stay retired, got %s", id)
	}
}

func TestSpreadSummaryUsesMinMedianMax(t *testing.T) {
	mk := func(rps float64) report.Result {
		var r report.Result
		r.Summary.RequestsPerSecond = rps
		return r
	}
	s := SpreadSummary([]report.Result{mk(30), mk(10), mk(20), mk(40)})
	if !strings.Contains(s, "10 / 25 / 40") || !strings.Contains(s, "not measured") {
		t.Fatalf("%s", s)
	}
	if !strings.Contains(SpreadSummary([]report.Result{mk(5), mk(7), mk(9)}), "5 / 7 / 9") {
		t.Fatal("odd count median")
	}
}

func TestPreflightRefusesAnUnreachableTarget(t *testing.T) {
	rf, err := ParseRun([]string{"--target", "http://127.0.0.1:1", "--out", t.TempDir(), "--duration", "1s", "--warmup", "0s"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), rf.Options, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "not reachable") {
		t.Fatalf("%v", err)
	}
}
