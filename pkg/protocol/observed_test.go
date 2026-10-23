package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// masterSnapshots are the two snapshots whose JSON was recorded from master (32b20d4), before WorkerSnapshot had an
// Observed field. testdata/worker_snapshots_master.json is that recorded output.
func masterSnapshots() []WorkerSnapshot {
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC)
	gpu, mem := 87.5, int64(12000)
	return []WorkerSnapshot{{
		WorkerID: "w-1", Model: "qwen-7b", Address: "http://10.0.0.5:9001", MaxConcurrency: 8, QueueSize: 16,
		State: StateReady, Health: HealthHealthy, Eligible: true,
		Metrics:      Metrics{ActiveRequests: 3, QueueDepth: 2, QueuedInputTokens: 410, RecentTokensPerSecond: 61.5, GPUUtilization: &gpu, GPUMemoryUsedMB: &mem},
		RegisteredAt: t0, LastHeartbeat: t0.Add(time.Second), HeartbeatAgeSeconds: 1.25,
	}, {
		WorkerID: "w-2", Model: "llama-8b", Address: "http://10.0.0.6:9001", MaxConcurrency: 1, State: StateDraining,
		Health: HealthSuspect, Reason: "warming up", RegisteredAt: t0, LastHeartbeat: t0,
	}}
}

func forged() ObservedLoad {
	return ObservedLoad{InflightRequests: 9, WorkAheadTokens: 1e9, CapacityTPS: 5, TTFTSeconds: 1, EffectiveActive: 7, Flags: ObservedInvalid}
}

func TestWorkerSnapshotWireFormatIsByteIdenticalToMaster(t *testing.T) {
	want, err := os.ReadFile("testdata/worker_snapshots_master.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"zero Observed", "populated Observed"} {
		ws := masterSnapshots()
		if name == "populated Observed" {
			for i := range ws {
				ws[i].Observed = forged()
			}
		}
		got, err := json.MarshalIndent(ws, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(bytes.TrimSpace(want), got) {
			t.Fatalf("%s: JSON differs from the master recording:\nwant:\n%s\ngot:\n%s", name, want, got)
		}
	}
}

func TestWorkerSnapshotIgnoresAForgedObservedKeyOnDecode(t *testing.T) {
	obs := `{"InflightRequests":9,"WorkAheadTokens":1e9,"CapacityTPS":5,"TTFTSeconds":1,"EffectiveActive":7,"Flags":8}`
	for _, key := range []string{"Observed", "observed", "OBSERVED", "observed_load", "Observed.InflightRequests"} {
		doc := `{"worker_id":"w-1","model":"m","eligible":true,"` + key + `":` + obs + `}`
		var s WorkerSnapshot
		if err := json.Unmarshal([]byte(doc), &s); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if s.Observed != (ObservedLoad{}) {
			t.Fatalf("a control plane forged Observed through the key %q: %+v", key, s.Observed)
		}
		if s.WorkerID != "w-1" || !s.Eligible {
			t.Fatalf("the rest of the document must still decode: %+v", s)
		}
	}
	// Also inside a list, which is how the gateway reads the registry.
	var out struct{ Workers []WorkerSnapshot }
	if err := json.Unmarshal([]byte(`{"Workers":[{"worker_id":"a","observed":`+obs+`},{"worker_id":"b","Observed":`+obs+`}]}`), &out); err != nil {
		t.Fatal(err)
	}
	for _, w := range out.Workers {
		if w.Observed != (ObservedLoad{}) {
			t.Fatalf("forged in a list: %+v", w)
		}
	}
}

func TestOnlyObservedIsExcludedFromWorkerSnapshotJSON(t *testing.T) {
	typ := reflect.TypeOf(WorkerSnapshot{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("json")
		if f.Name == "Observed" {
			if tag != "-" {
				t.Fatalf("Observed must be tagged json:\"-\", has %q", tag)
			}
			continue
		}
		if tag == "" || tag == "-" {
			t.Fatalf("field %s must be on the wire with an explicit name, tag %q", f.Name, tag)
		}
	}
}

func TestInferenceRequestEstimateFieldsAreNeverOnAnyWire(t *testing.T) {
	r := InferenceRequest{RequestID: "r", Model: "m", EstInputTokens: 123456, EstOutputTokens: 654321}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "123456") || strings.Contains(string(b), "EstInput") || strings.Contains(string(b), "EstOutput") || strings.Contains(string(b), "654321") {
		t.Fatalf("estimate fields leaked into JSON: %s", b)
	}
	var back InferenceRequest
	if err := json.Unmarshal([]byte(`{"EstInputTokens":5,"EstOutputTokens":6,"est_input_tokens":5}`), &back); err != nil {
		t.Fatal(err)
	}
	if back.EstInputTokens != 0 || back.EstOutputTokens != 0 {
		t.Fatalf("a client set an estimate through JSON: %+v", back)
	}
}
