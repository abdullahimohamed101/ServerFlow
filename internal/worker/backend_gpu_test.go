package worker

import (
	"context"
	"testing"
)

// A mock worker running the simulator engine reports GPU figures in /stats; the agent forwards them.
func TestMockBackendForwardsOptionalGPUFields(t *testing.T) {
	b := statsServer(t, 200, `{"status":"ready","active_requests":1,"max_concurrency":8,"queue_size":16,"gpu_utilization":37.5,"gpu_memory_used_mb":15234}`)
	got, err := b.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Metrics.GPUUtilization == nil || *got.Metrics.GPUUtilization != 37.5 || got.Metrics.GPUMemoryUsedMB == nil || *got.Metrics.GPUMemoryUsedMB != 15234 {
		t.Fatalf("metrics = %+v", got.Metrics)
	}
	if err := got.Metrics.Validate(); err != nil {
		t.Fatal(err)
	}
}

// Without the fields (every worker that is not a simulator) both stay nil, so heartbeats are unchanged.
func TestMockBackendLeavesGPUFieldsNilWhenAbsent(t *testing.T) {
	b := statsServer(t, 200, `{"status":"ready","active_requests":1,"max_concurrency":8,"queue_size":16}`)
	got, err := b.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Metrics.GPUUtilization != nil || got.Metrics.GPUMemoryUsedMB != nil {
		t.Fatalf("metrics = %+v", got.Metrics)
	}
}

// An absurd figure from a worker is rejected by the same validation as every heartbeat.
func TestMockBackendGPUOutOfRangeFailsValidation(t *testing.T) {
	b := statsServer(t, 200, `{"status":"ready","max_concurrency":8,"queue_size":16,"gpu_utilization":900}`)
	got, err := b.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Metrics.Validate() == nil {
		t.Fatal("utilisation 900 accepted")
	}
}
