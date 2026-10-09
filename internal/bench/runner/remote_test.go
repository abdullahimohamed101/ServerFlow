package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"serverflow/internal/bench/driver"
	"serverflow/internal/bench/report"
	"serverflow/pkg/protocol"
)

// fakeGateway answers /healthz and non-streaming chat completions at once.
func fakeGateway(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":7}}`))
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}

// hang answers nothing until the test ends.
func hang(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); ts.Close() })
	return ts
}

func remoteOptions(t *testing.T, gw string, extra ...string) Options {
	t.Helper()
	rf, err := ParseRun(append([]string{"--target", gw, "--out", t.TempDir(), "--duration", "1s", "--warmup", "0s", "--concurrency", "2",
		"--stream-ratio", "0", "--workers", "1", "--sample-interval", "100ms"}, extra...))
	if err != nil {
		t.Fatal(err)
	}
	return rf.Options
}

// The pre-run calls (listing the workers, reading their first counts) can take as long as the
// client timeout. They must delay the start of the run, never shorten its window.

func TestASlowControlPlaneDelaysTheRunButDoesNotShortenTheWindow(t *testing.T) {
	t.Parallel()
	cp := hang(t)
	o := remoteOptions(t, fakeGateway(t), "--control-plane", cp.URL)
	rs, err := Run(context.Background(), o, &bytes.Buffer{})
	if err != nil || len(rs) != 1 {
		t.Fatalf("%v", err)
	}
	r := rs[0]
	if r.Timings.PreRun < 4 {
		t.Fatalf("the control plane took %.1fs to give up and that time must be recorded as pre-run, got %.1fs", 5.0, r.Timings.PreRun)
	}
	if r.Summary.Sent == 0 || !r.Valid || r.Summary.WindowSeconds != 1 {
		t.Fatalf("the whole window must still have carried load: sent %d valid %v %v", r.Summary.Sent, r.Valid, r.InvalidReasons)
	}
	if r.NotMeasured["queue"] == "" || r.NotMeasured["request_imbalance"] == "" {
		t.Fatalf("an unreachable control plane means no queue or balance: %v", r.NotMeasured)
	}
}

func TestAHangingWorkerStatsEndpointDelaysTheRunButDoesNotShortenTheWindow(t *testing.T) {
	t.Parallel()
	stats := hang(t)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws := []protocol.WorkerSnapshot{{WorkerID: "w1", Model: "qwen-7b", Address: stats.URL, State: protocol.StateReady, Eligible: true, Health: protocol.HealthHealthy}}
		_ = json.NewEncoder(w).Encode(map[string]any{"workers": ws})
	}))
	t.Cleanup(cp.Close)
	o := remoteOptions(t, fakeGateway(t), "--control-plane", cp.URL)
	rs, err := Run(context.Background(), o, &bytes.Buffer{})
	if err != nil || len(rs) != 1 {
		t.Fatalf("%v", err)
	}
	r := rs[0]
	if r.Timings.PreRun < 4 {
		t.Fatalf("the first /stats read hung for the client timeout: pre-run %.1fs", r.Timings.PreRun)
	}
	if r.Summary.Sent == 0 || !r.Valid || r.Summary.CompletedInWindow == 0 {
		t.Fatalf("sent %d valid %v %v", r.Summary.Sent, r.Valid, r.InvalidReasons)
	}
	if _, bad := r.StatsMissing["w1"]; !bad || r.Imbalance.RequestJain != nil {
		t.Fatalf("the unreadable worker is recorded as missing: %v", r.StatsMissing)
	}
}

func TestAnEmptyWindowFailsEvenWithAllowErrorsAndSaysWhy(t *testing.T) {
	// One arrival every two seconds: request 0 is due at 0, inside the 1s warm-up; nothing is due in 1s-1.2s.
	o := quickOptions(t, "--rate", "0.5", "--concurrency", "0", "--warmup", "1s", "--duration", "200ms", "--allow-errors")
	o.Concurrency = 0
	_, err := Run(context.Background(), o, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "sent 0") || !strings.Contains(err.Error(), "none was due inside") {
		t.Fatalf("%v", err)
	}
}

func TestTheStartSnapshotIsTakenAtTheEndOfWarmupSoWorkerCountsAreMeasured(t *testing.T) {
	o := quickOptions(t, "--workload", "uniform-short", "--warmup", "1s", "--duration", "1s")
	rs, err := Run(context.Background(), o, &bytes.Buffer{})
	if err != nil || len(rs) != 1 {
		t.Fatalf("%v", err)
	}
	r := rs[0]
	var completed int64
	for _, w := range r.Workers {
		if w.Completed == nil {
			t.Fatalf("with a warm-up the per-worker counts must still be measured: %+v %v", w, r.StatsMissing)
		}
		completed += *w.Completed
	}
	// Counts start when warm-up ends, so they hold every request sent in the window (all succeeded)
	// plus the warm-up requests that finished late.
	if completed < int64(r.Summary.Succeeded) || len(r.StatsMissing) != 0 {
		t.Fatalf("completed %d, window successes %d, missing %v", completed, r.Summary.Succeeded, r.StatsMissing)
	}
	if !strings.Contains(strings.Join(r.Notes, " "), "warm-up requests that finished after that") {
		t.Fatalf("%v", r.Notes)
	}
}

func TestTheLoadWarningAppearsAboveSixtyPercentOfTheSlots(t *testing.T) {
	parseOpts := func(args ...string) Options {
		rf, err := ParseRun(args)
		if err != nil {
			t.Fatal(err)
		}
		return rf.Options
	}
	// 4 workers x 8 = 32 slots: 60% is 19.2, so 19 clients are fine and 20 are not.
	if w := parseOpts("--workers", "4", "--concurrency", "19").LoadWarning(); w != "" {
		t.Fatalf("%q", w)
	}
	w := parseOpts("--workers", "4", "--concurrency", "20").LoadWarning()
	if !strings.Contains(w, "20 clients on 32 request slots") || !strings.Contains(w, "probably be invalid") || !strings.Contains(w, "at most 19 clients") {
		t.Fatalf("%q", w)
	}
	if w := parseOpts("--workers", "4", "--rate", "50").LoadWarning(); w != "" {
		t.Fatalf("an open loop has no client count: %q", w)
	}
	if w := parseOpts("--target", "http://127.0.0.1:1", "--concurrency", "500").LoadWarning(); w != "" {
		t.Fatalf("the slots of a remote target are unknown: %q", w)
	}
	// It is printed at the start of a run and kept in the result's notes.
	o := parseOpts("--workers", "2", "--mock-concurrency", "4", "--concurrency", "6", "--duration", "300ms", "--warmup", "0s", "--mock-tps", "20000", "--mock-ttft", "1ms", "--out", t.TempDir())
	var out bytes.Buffer
	rs, _ := Run(context.Background(), o, &out)
	if !strings.Contains(out.String(), "WARNING: 6 clients on 8 request slots") {
		t.Fatalf("%s", out.String())
	}
	if len(rs) == 1 && !strings.Contains(strings.Join(rs[0].Notes, " "), "6 clients on 8 request slots") {
		t.Fatalf("%v", rs[0].Notes)
	}
}

func TestNotesReportAClockSkewOnlyAboveTwoSeconds(t *testing.T) {
	o := Options{Warmup: 0}
	for _, tc := range []struct {
		wall, mono float64
		want       bool
	}{{100, 10, true}, {10, 100, true}, {10, 11.9, false}, {10, 12.1, true}, {10, 10, false}, {12.1, 10, true}} {
		notes := notes(o, &driver.Output{}, &target{}, report.Timings{WallClock: tc.wall, MonotonicElapsed: tc.mono})
		got := strings.Contains(strings.Join(notes, " "), "machine probably slept")
		if got != tc.want {
			t.Errorf("wall %v monotonic %v: note present=%v, want %v", tc.wall, tc.mono, got, tc.want)
		}
	}
}
