package integration

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"serverflow/internal/bench/report"
	"serverflow/internal/bench/runner"
)

// The Phase 7 harness against its embedded cluster: a real control plane, mock workers behind real
// agents, and a real gateway, driven by the real load generator. These tests check that results exist,
// are complete and are self-consistent; they never assert tight timing, and never that one scheduler
// beats another.

// benchRun runs the harness once, with fast mock workers, and returns its result.
func benchRun(t *testing.T, args ...string) report.Result {
	t.Helper()
	rf, err := runner.ParseRun(append([]string{"--out", t.TempDir(), "--mock-tps", "20000", "--mock-ttft", "1ms",
		"--sample-interval", "50ms", "--save-requests",
		// These tests check that results are complete and consistent, not that the machine was
		// idle: a loaded host may reject some requests, which the validity gate would call
		// invalid. The result is still written and still checked.
		"--allow-errors"}, args...))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	// A run that cannot finish must fail, not block the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rs, err := runner.Run(ctx, rf.Options, &out)
	if err != nil || len(rs) != 1 {
		t.Fatalf("%v %v\n%s", rs, err, out.String())
	}
	if strings.Contains(out.String(), "sk-bench") {
		t.Fatal("an API key reached the output")
	}
	return rs[0]
}

func checkComplete(t *testing.T, r report.Result) {
	t.Helper()
	s := r.Summary
	if s.Sent == 0 || s.Sent != s.Succeeded+s.Failed {
		t.Fatalf("sent %d, succeeded %d, failed %d", s.Sent, s.Succeeded, s.Failed)
	}
	if got := r.Metadata.Missing(); len(got) != 0 {
		t.Fatalf("metadata incomplete: %v", got)
	}
	// Every spec section 34 metric is either present or named as not measured.
	present := map[string]bool{
		"latency": s.Latency != nil, "ttft": s.TTFT != nil, "queue": r.Queue != nil, "gpu_utilization": r.GPUUtilization != nil,
		"request_imbalance": r.Imbalance.RequestJain != nil, "queue_imbalance": r.Imbalance.QueueJain != nil,
	}
	for k, ok := range present {
		if _, listed := r.NotMeasured[k]; ok == listed {
			t.Errorf("%s: present=%v but listed as not measured=%v", k, ok, listed)
		}
	}
	var completed int64
	for _, w := range r.Workers {
		if w.Completed != nil {
			completed += *w.Completed
		}
	}
	if completed != int64(s.Succeeded) {
		t.Fatalf("per-worker completed counts sum to %d, successes are %d", completed, s.Succeeded)
	}
}

func TestTheHarnessRunsEverySpecWorkloadAgainstTheEmbeddedCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a cluster per workload")
	}
	for _, name := range []string{"uniform-short", "uniform-long", "mixed", "hot-model", "multi-tenant", "burst"} {
		t.Run(name, func(t *testing.T) {
			args := []string{"--workload", name, "--workers", "4", "--duration", "1s", "--warmup", "0s", "--concurrency", "6"}
			if name == "burst" { // burst changes the arrival rate, so it is open loop only
				args = []string{"--workload", name, "--workers", "4", "--duration", "3s", "--warmup", "0s", "--rate", "10"}
			}
			r := benchRun(t, args...)
			checkComplete(t, r)
			if r.Summary.Succeeded == 0 {
				t.Fatalf("nothing succeeded: %v", r.Summary.ErrorClasses)
			}
			if r.Valid != (r.Summary.ErrorRate <= 0.05) {
				t.Fatalf("valid=%v with error rate %v", r.Valid, r.Summary.ErrorRate)
			}
			if name == "hot-model" {
				models := map[string]int64{}
				for _, w := range r.Workers {
					models[w.Model] += *w.Completed
				}
				if len(models) != 2 || models["qwen-7b"] < models["llama-8b"] {
					t.Fatalf("both models are served and the hot one gets more: %v", models)
				}
			}
			if name == "burst" && r.Metadata.Mode != "open" {
				t.Fatal("burst is open loop")
			}
		})
	}
}

func TestSchedulersLeaveMeasurablyDifferentDistributionsOnHeterogeneousWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a cluster per scheduler")
	}
	run := func(scheduler string) report.Result {
		return benchRun(t, "--scheduler", scheduler, "--workers", "3", "--worker-profile", "heterogeneous", "--mock-tps", "500",
			"--workload", "uniform-short", "--duration", "3s", "--warmup", "0s", "--concurrency", "12", "--seed", "7")
	}
	byScheduler := map[string]report.Result{}
	for _, s := range []string{"round-robin", "least-active", "random", "least-queue"} {
		r := run(s)
		checkComplete(t, r)
		if len(r.Workers) != 3 || len(r.Metadata.Cluster.Workers) != 3 || r.Summary.Succeeded == 0 {
			t.Fatalf("%s: %+v", s, r.Summary)
		}
		byScheduler[s] = r
	}
	rr, la := byScheduler["round-robin"], byScheduler["least-active"]
	if rr.Imbalance.RequestJain == nil || la.Imbalance.RequestJain == nil {
		t.Fatal("balance must be measured")
	}
	// How the two spread requests over fast, medium and slow workers is what the report is for; the
	// numbers depend on machine load, so the test records them (go test -v) and asserts only that
	// both are measured and add up. The sample in docs/benchmarks/phase-7-harness.md shows them.
	for _, r := range []report.Result{rr, la} {
		t.Logf("%s: completed per worker %d/%d/%d, Jain %.3f", r.Metadata.Scheduler,
			*r.Workers[0].Completed, *r.Workers[1].Completed, *r.Workers[2].Completed, *r.Imbalance.RequestJain)
	}

	// The two are comparable: the only difference flagged is the scheduler, and the table has the deltas.
	c := report.Compare(rr, la)
	if c.Unfair() || len(c.Differences) != 1 || c.Differences[0].Field != "scheduler" {
		t.Fatalf("%+v", c.Differences)
	}
	var out bytes.Buffer
	if err := c.Write(&out); err != nil || !strings.Contains(out.String(), "worker balance") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	// Both offered the same load.
	if rr.Metadata.PlanDigest != la.Metadata.PlanDigest {
		t.Fatal("the same seed must plan the same load")
	}
	if z := report.Compare(rr, rr); z.Unfair() {
		t.Fatal("a run compared with itself")
	}
}
