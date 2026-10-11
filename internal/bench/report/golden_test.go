package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func keysOf(t *testing.T, v any) []string {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("not an object: %T", v)
	}
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func same(t *testing.T, where string, got, want []string) {
	t.Helper()
	want = append([]string(nil), want...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Errorf("%s keys changed:\n got %v\nwant %v", where, got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s keys changed:\n got %v\nwant %v", where, got, want)
			return
		}
	}
}

// The names in a chaos result are a stored format: the committed baselines (docs/benchmarks/phase-15-baseline)
// and any later group compared with them use them. Renaming one must be a decision, not an accident.
func TestChaosResultKeysAreAStoredFormat(t *testing.T) {
	r := chaosResult("run_001", "absent", 0.9)
	r.Chaos.Windows = []ChaosWindow{{Name: "fault"}}
	r.Chaos.Series = []ChaosBucket{{}}
	r.Chaos.Gate = []ChaosCheck{{}}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	c := m["chaos"].(map[string]any)
	same(t, "chaos", keysOf(t, c), []string{"bucket_seconds", "description", "evidence", "fault_applied_seconds", "fault_cleared_seconds", "fault_worker", "hard_bounds",
		"measures", "params", "scenario", "series", "time_scale", "validity_checks", "variant", "windows"})
	same(t, "chaos.measures", keysOf(t, c["measures"]), []string{"fault_success_latency_p95_ms", "fault_success_latency_p99_ms", "primary_fault_attempts_per_request",
		"primary_fault_success_rate", "time_to_isolate_note", "time_to_isolate_seconds", "time_to_recover_note", "time_to_recover_seconds"})
	same(t, "chaos.evidence", keysOf(t, c["evidence"]), []string{"applied", "blackholed", "clients_in_fault", "control_plane_state", "crashes", "delayed", "drains",
		"errored", "requests_touched", "served_while_announcing", "surviving_slots"})
	same(t, "chaos.hard_bounds", keysOf(t, c["hard_bounds"]), []string{"goroutines_end", "goroutines_start", "heap_end_mib", "heap_start_mib", "ineligible_selections"})
	same(t, "chaos.windows[0]", keysOf(t, c["windows"].([]any)[0]), []string{"attempts_per_request", "end_seconds", "error_classes", "failed", "failure_latency_p50_ms",
		"gateway_attempts", "gateway_ineligible_selections", "gateway_no_capacity", "gateway_retries", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "name",
		"retried", "sent", "start_seconds", "status_classes", "success_rate", "succeeded"})
	same(t, "chaos.series[0]", keysOf(t, c["series"].([]any)[0]), []string{"faulty_selections", "failed", "ok", "probes", "t"})
	same(t, "chaos.validity_checks[0]", keysOf(t, c["validity_checks"].([]any)[0]), []string{"detail", "name", "ok"})
	if m["metadata"].(map[string]any)["resilience"] != "absent" {
		t.Error("metadata.resilience")
	}

	// And they are the keys of the committed baseline.
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "benchmarks", "phase-15-baseline", "run_001", "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var base map[string]any
	_ = json.Unmarshal(raw, &base)
	bc := base["chaos"].(map[string]any)
	same(t, "baseline chaos", keysOf(t, bc), keysOf(t, c))
	same(t, "baseline measures", keysOf(t, bc["measures"]), keysOf(t, c["measures"]))
	same(t, "baseline windows[0]", keysOf(t, bc["windows"].([]any)[0]), keysOf(t, c["windows"].([]any)[0]))
	same(t, "baseline evidence", keysOf(t, bc["evidence"]), keysOf(t, c["evidence"]))
	same(t, "baseline hard_bounds", keysOf(t, bc["hard_bounds"]), keysOf(t, c["hard_bounds"]))
}

func TestCompareLeavesOutTheResilienceLineWhenOneSideHasNoValue(t *testing.T) {
	old, now, other := chaosResult("run_001", "", 1), chaosResult("run_002", "absent", 1), chaosResult("run_003", "on", 1)
	for _, d := range differences(old.Metadata, now.Metadata) {
		if d.Field == "resilience" {
			t.Fatal("a Phase 7 result against a new one must not flag resilience")
		}
	}
	found := false
	for _, d := range differences(now.Metadata, other.Metadata) {
		found = found || d.Field == "resilience"
	}
	if !found {
		t.Fatal("two recorded values that differ must be listed")
	}
}
