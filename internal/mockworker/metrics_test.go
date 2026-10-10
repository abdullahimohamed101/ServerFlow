package mockworker

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"serverflow/internal/telemetry"
)

func scrapeWorker(t *testing.T, e *env) string {
	t.Helper()
	resp, err := e.client.Get(e.ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("/metrics: %d", resp.StatusCode)
	}
	return string(b)
}

func TestWorkerMetricsCountTokensRequestsAndLatency(t *testing.T) {
	e := newEnv(t)
	idle := scrapeWorker(t, e)
	for _, want := range []string{`worker_requests_total{result="ok"} 0`, `worker_requests_total{result="rejected"} 0`, "worker_output_tokens_total 0", "worker_input_tokens_total 0", "serverflow_build_info{"} {
		if !strings.Contains(idle, want) {
			t.Fatalf("an idle worker must export %q:\n%s", want, idle)
		}
	}
	readAll(t, e.mustPost(t, chatBody(false, "one two three", "")).Body)
	readAll(t, e.mustPost(t, chatBody(true, "four five", "")).Body)
	out := scrapeWorker(t, e)
	for _, want := range []string{
		`worker_requests_total{result="ok"} 2`,
		"worker_output_tokens_total 20", // 2 requests x 10 output tokens
		"worker_input_tokens_total 5",   // 3 + 2 words
		"worker_request_duration_seconds_count 2",
		"worker_ttft_seconds_count 1", // only the stream has a time to first token
		"worker_queue_duration_seconds_count 2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q\n%s", want, out)
		}
	}
	fams, err := e.srv.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatalf("labels outside the allowlist: %v", bad)
	}
}

func TestWorkerMetricsCountFailuresAndOnlyServeGET(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.FailureRate, c.FailureMode = 1, ModeError })
	resp := e.mustPost(t, chatBody(false, "x", ""))
	if resp.StatusCode != 500 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	readAll(t, resp.Body)
	if out := scrapeWorker(t, e); !strings.Contains(out, `worker_requests_total{result="failed"} 1`) {
		t.Fatalf("a failed request must be counted:\n%s", out)
	}
	post, err := e.client.Post(e.ts.URL+"/metrics", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	_ = post.Body.Close()
	if post.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics: %d", post.StatusCode)
	}
}

func readAll(t *testing.T, r io.ReadCloser) {
	t.Helper()
	defer func() { _ = r.Close() }()
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
}

// The worker's histograms are in seconds: a stream with a 250 ms time to first token must show about 0.25 in
// worker_ttft_seconds_sum (a millisecond or microsecond slip would show 250 or 250000).
func TestWorkerHistogramsAreInSeconds(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.TTFT = 250 * time.Millisecond })
	readAll(t, e.mustPost(t, chatBody(true, "one two", "")).Body)
	fams, err := e.srv.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]float64{}
	for _, f := range fams {
		if h := f.GetMetric(); len(h) == 1 && h[0].GetHistogram() != nil {
			sums[f.GetName()] = h[0].GetHistogram().GetSampleSum()
		}
	}
	if v := sums["worker_ttft_seconds"]; v < 0.24 || v > 0.6 {
		t.Errorf("worker_ttft_seconds sum %v for a 250 ms time to first token", v)
	}
	if v := sums["worker_request_duration_seconds"]; v < 0.25 || v > 5 {
		t.Errorf("worker_request_duration_seconds sum %v for a request of at least 250 ms", v)
	}
	if v, ok := sums["worker_queue_duration_seconds"]; !ok || v < 0 || v > 0.1 {
		t.Errorf("worker_queue_duration_seconds sum %v for an unqueued request", v)
	}
}
