package gateway

import (
	"net/http"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Phase 15, PR A, debt 3: a retry whose second routing fails is a scheduling decision like any other, so it is
// counted in scheduler_decisions_total and scheduler_no_capacity_total. Nothing else changes.

func decisionTotals(t *testing.T, e *regEnv) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, r := range []string{"selected", "no_capacity", "no_model", "error"} {
		v, _ := sample(t, e.gw.metrics, "scheduler_decisions_total", "result="+r)
		out[r] = v
	}
	return out
}

func TestARetryWithNoOtherWorkerCountsAsANoCapacityDecision(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{"error":{"code":"x"}}`))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b")})
	if resp, _ := e.post(t, false); resp.StatusCode != 503 {
		t.Fatalf("the worker's own answer is the client's: %d", resp.StatusCode)
	}
	got := decisionTotals(t, e)
	if got["selected"] != 1 || got["no_capacity"] != 1 || got["no_model"] != 0 || got["error"] != 0 {
		t.Fatalf("decisions: %v", got)
	}
	want(t, e.gw.metrics, 1, "scheduler_no_capacity_total", "model=qwen-7b", "reason=no_capacity")
	if n := sumSeries(t, e.gw.metrics, "scheduler_no_capacity_total"); n != 1 {
		t.Fatalf("scheduler_no_capacity_total: %v", n)
	}
	// What was already counted is unchanged.
	want(t, e.gw.metrics, 1, "inference_attempts_total", "outcome=failed")
	want(t, e.gw.metrics, 1, "inference_requests_total", "status=503")
}

func TestARetryWhoseSecondRoutingFindsTheSnapshotStaleCountsAsAnError(t *testing.T) {
	var e *regEnv
	bad := newScriptWorker(t, "a-bad", func(w http.ResponseWriter, r *http.Request) {
		e.clock.Advance(time.Hour) // the registry view is now too old to use
		status(503, `{"error":{"code":"x"}}`)(w, r)
	})
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e = newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	if resp, _ := e.post(t, false); resp.StatusCode != 503 {
		t.Fatalf("%d", resp.StatusCode)
	}
	got := decisionTotals(t, e)
	if got["selected"] != 1 || got["error"] != 1 || got["no_capacity"] != 0 {
		t.Fatalf("decisions: %v", got)
	}
	want(t, e.gw.metrics, 1, "scheduler_no_capacity_total", "model=qwen-7b", "reason=worker_unavailable")
}

func TestARetryThatSucceedsAddsOnlyTheSecondSelection(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "a-bad"), good.snapshot("qwen-7b")})
	if resp, _ := e.post(t, false); resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	}
	got := decisionTotals(t, e)
	if got["selected"] != 2 || got["no_capacity"] != 0 || got["error"] != 0 {
		t.Fatalf("decisions: %v", got)
	}
	if n := sumSeries(t, e.gw.metrics, "scheduler_no_capacity_total"); n != 0 {
		t.Fatalf("scheduler_no_capacity_total: %v", n)
	}
}
