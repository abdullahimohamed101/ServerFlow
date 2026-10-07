// Package integration holds end-to-end tests that run several ServerFlow
// components together in one process: the real gateway in front of real mock
// workers, over real HTTP.
package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
)

const model = "qwen-7b"

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func startMock(t *testing.T, mutate ...func(*mockworker.Config)) (*httptest.Server, *mockworker.Server) {
	t.Helper()
	cfg := mockworker.DefaultConfig()
	cfg.Model = model
	cfg.TTFT = 30 * time.Millisecond
	cfg.TokensPerSecond = 200
	cfg.OutputTokens = 10
	cfg.Seed = 1
	for _, m := range mutate {
		m(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv := mockworker.New(cfg, quiet())
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, srv
}

func startGateway(t *testing.T, upstream string) *httptest.Server {
	t.Helper()
	cfg := config.Default().Gateway
	cfg.UpstreamURL = upstream
	cfg.Models = []string{model}
	cfg.UpstreamHeaderTimeout = 5 * time.Second
	ts := httptest.NewServer(gateway.New(cfg, quiet()).Handler())
	t.Cleanup(ts.Close)
	return ts
}

func chat(stream bool, prompt string) string {
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}],"stream":%v}`, model, prompt, stream)
}

func post(ctx context.Context, url, body string) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func mustPost(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := post(context.Background(), url, body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

type event struct {
	data string
	at   time.Time
}

func readEvents(r io.Reader) ([]event, error) {
	br := bufio.NewReader(r)
	var out []event
	for {
		line, err := br.ReadString('\n')
		if strings.HasPrefix(line, "data: ") {
			out = append(out, event{strings.TrimSpace(strings.TrimPrefix(line, "data: ")), time.Now()})
		}
		if err != nil {
			return out, err
		}
	}
}

func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, msg)
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	var e struct{ Error struct{ Code string } }
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("not an error body: %q", body)
	}
	return e.Error.Code
}

// --- criterion 10: through the gateway ------------------------------------------------

func TestStreamThroughTheGateway(t *testing.T) {
	mock, _ := startMock(t)
	gw := startGateway(t, mock.URL)

	start := time.Now()
	resp := mustPost(t, gw.URL, chat(true, "hello there"))
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("%d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if id := resp.Header.Get("X-Request-ID"); !strings.HasPrefix(id, "req_") {
		t.Fatalf("gateway request ID missing: %q", id)
	}
	events, err := readEvents(resp.Body)
	if err != io.EOF {
		t.Fatalf("stream should end cleanly: %v", err)
	}
	if len(events) != 13 || events[len(events)-1].data != "[DONE]" {
		t.Fatalf("expected role + 10 tokens + finish + [DONE], got %d events", len(events))
	}
	if first := events[0].at.Sub(start); first < 30*time.Millisecond {
		t.Fatalf("first chunk after %v, before the worker's 30ms TTFT", first)
	}
	// Tokens are paced by the worker and relayed without buffering: the last
	// token cannot arrive before the worker has generated it.
	if last := events[10].at.Sub(start); last < 30*time.Millisecond+9*5*time.Millisecond {
		t.Fatalf("last token after %v; the gateway cannot deliver tokens early", last)
	}
}

func TestNonStreamThroughTheGateway(t *testing.T) {
	mock, _ := startMock(t)
	gw := startGateway(t, mock.URL)
	resp := mustPost(t, gw.URL, chat(false, "one two three"))
	body, _ := io.ReadAll(resp.Body)
	var c struct {
		Object string
		Usage  struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		}
	}
	if err := json.Unmarshal(body, &c); err != nil || resp.StatusCode != 200 || c.Object != "chat.completion" ||
		c.Usage.PromptTokens != 3 || c.Usage.CompletionTokens != 10 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestWorkerFailuresThroughTheGateway(t *testing.T) {
	tests := []struct {
		name   string
		mode   mockworker.FailureMode
		stream bool
		check  func(t *testing.T, resp *http.Response)
	}{
		{"error is passed through", mockworker.ModeError, false, func(t *testing.T, resp *http.Response) {
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 500 || errorCode(t, b) != "mock_failure" {
				t.Fatalf("got %d %s", resp.StatusCode, b)
			}
		}},
		{"unavailable is passed through", mockworker.ModeUnavailable, false, func(t *testing.T, resp *http.Response) {
			if resp.StatusCode != 503 {
				t.Fatalf("got %d", resp.StatusCode)
			}
		}},
		{"a dropped connection becomes a gateway 502", mockworker.ModeDrop, false, func(t *testing.T, resp *http.Response) {
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 502 || errorCode(t, b) != "INFERENCE_FAILED" {
				t.Fatalf("got %d %s", resp.StatusCode, b)
			}
		}},
		{"a midstream failure ends the stream with the gateway's error event", mockworker.ModeMidstream, true, func(t *testing.T, resp *http.Response) {
			events, _ := readEvents(resp.Body)
			if len(events) < 2 {
				t.Fatalf("expected partial tokens then an error event, got %v", events)
			}
			last := events[len(events)-1].data
			if !strings.Contains(last, `"code":"INFERENCE_FAILED"`) {
				t.Fatalf("stream must end with the gateway's error event, got %q", last)
			}
			for _, e := range events {
				if e.data == "[DONE]" {
					t.Fatal("a failed stream must not look complete")
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, _ := startMock(t, func(c *mockworker.Config) { c.FailureRate = 1; c.FailureMode = tt.mode })
			gw := startGateway(t, mock.URL)
			tt.check(t, mustPost(t, gw.URL, chat(tt.stream, "x")))
		})
	}
}

func TestQueueFullIsPassedThroughByTheGateway(t *testing.T) {
	mock, srv := startMock(t, func(c *mockworker.Config) { c.MaxConcurrency = 1; c.QueueSize = 0; c.TTFT = 300 * time.Millisecond })
	gw := startGateway(t, mock.URL)

	go func() {
		if resp, err := post(context.Background(), gw.URL, chat(false, "busy")); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	eventually(t, 2*time.Second, func() bool { return srv.Stats().ActiveRequests == 1 }, "the worker to be busy")

	resp := mustPost(t, gw.URL, chat(false, "second"))
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || errorCode(t, b) != "queue_full" {
		t.Fatalf("a full worker must surface as 503 queue_full, got %d %s", resp.StatusCode, b)
	}
}

func TestClientCancelThroughTheGatewayFreesTheWorkerSlot(t *testing.T) {
	mock, srv := startMock(t, func(c *mockworker.Config) { c.MaxConcurrency = 1; c.TokensPerSecond = 20; c.OutputTokens = 200 }) // ~10s if left alone
	gw := startGateway(t, mock.URL)

	ctx, cancel := context.WithCancel(context.Background())
	resp, err := post(ctx, gw.URL, chat(true, "hi"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = bufio.NewReader(resp.Body).ReadString('\n') // generation is under way
	if srv.Stats().ActiveRequests != 1 {
		t.Fatalf("expected the worker to be busy: %+v", srv.Stats())
	}
	cancel()
	_ = resp.Body.Close()

	// Cancellation propagates client -> gateway -> worker (spec section 24).
	eventually(t, time.Second, func() bool { return srv.Stats().ActiveRequests == 0 }, "the worker slot to be freed")
	if s := srv.Stats(); s.TokensGenerated >= 200 || s.Cancelled != 1 {
		t.Fatalf("worker kept generating for a client that left: %+v", s)
	}
}

func TestGatewayReadinessFollowsTheWorker(t *testing.T) {
	mock, _ := startMock(t, func(c *mockworker.Config) { c.StartupDelay = 1200 * time.Millisecond })
	gw := startGateway(t, mock.URL)

	status := func() int {
		r, err := http.Get(gw.URL + "/readyz")
		if err != nil {
			return 0
		}
		defer func() { _ = r.Body.Close() }()
		return r.StatusCode
	}
	if got := status(); got != 503 {
		t.Fatalf("gateway must not be ready while its worker is starting, got %d", got)
	}
	// The gateway caches probe results briefly, so allow for that.
	eventually(t, 5*time.Second, func() bool { return status() == 200 }, "gateway readiness after the worker became ready")
}

// --- criterion 11: three workers at once ----------------------------------------------

// measuredRate streams one request and returns the achieved tokens per second
// between the first and last content chunk.
func measuredRate(t *testing.T, url string) float64 {
	t.Helper()
	resp, err := post(context.Background(), url, chat(true, "hello"))
	if err != nil {
		t.Errorf("POST: %v", err)
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	events, _ := readEvents(resp.Body)
	var times []time.Time
	for _, e := range events {
		if strings.Contains(e.data, `"content":"tok`) {
			times = append(times, e.at)
		}
	}
	if len(times) < 2 {
		t.Errorf("only %d tokens received", len(times))
		return 0
	}
	return float64(len(times)-1) / times[len(times)-1].Sub(times[0]).Seconds()
}

func TestThreeWorkersRunConcurrentlyAtTheirOwnSpeeds(t *testing.T) {
	speeds := []float64{100, 60, 20} // simulation mode, spec section 54
	urls := make([]string, len(speeds))
	for i, tps := range speeds {
		tps := tps
		ts, _ := startMock(t, func(c *mockworker.Config) {
			c.TokensPerSecond = tps
			c.TTFT = 20 * time.Millisecond
			c.OutputTokens = 60
			c.WorkerID = fmt.Sprintf("mock-%d", int(tps))
		})
		urls[i] = ts.URL
	}

	got := make([]float64, len(speeds))
	var wg sync.WaitGroup
	for i := range speeds {
		wg.Add(1)
		go func() { defer wg.Done(); got[i] = measuredRate(t, urls[i]) }()
	}
	wg.Wait()

	for i, want := range speeds {
		if got[i] < want*0.85 || got[i] > want*1.15 {
			t.Errorf("worker %.0f tok/s measured %.1f tok/s, outside 15%% of configured", want, got[i])
		}
	}
	if !(got[0] > got[1] && got[1] > got[2]) {
		t.Fatalf("throughput must be ordered like the configuration, got %.1f / %.1f / %.1f", got[0], got[1], got[2])
	}
	t.Logf("configured %v tok/s, measured %.1f / %.1f / %.1f", speeds, got[0], got[1], got[2])
}
