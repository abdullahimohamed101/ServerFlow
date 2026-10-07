package mockworker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testConfig() Config {
	c := DefaultConfig()
	c.Model = "qwen-7b"
	c.TTFT = 30 * time.Millisecond
	c.TokensPerSecond = 200 // 5ms between tokens
	c.OutputTokens = 10
	c.MaxConcurrency = 2
	c.QueueSize = 2
	c.Seed = 1
	return c
}

type env struct {
	srv    *Server
	ts     *httptest.Server
	client *http.Client
	logs   *lockedBuffer
}

func newEnv(t *testing.T, mutate ...func(*Config)) *env {
	t.Helper()
	cfg := testConfig()
	for _, m := range mutate {
		m(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	logs := &lockedBuffer{}
	srv := New(cfg, slog.New(slog.NewJSONHandler(logs, nil)))
	ts := httptest.NewServer(srv.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); ts.Close() })
	return &env{srv: srv, ts: ts, client: &http.Client{Transport: tr}, logs: logs}
}

func chatBody(stream bool, prompt string, extra string) string {
	return fmt.Sprintf(`{"model":"qwen-7b","messages":[{"role":"user","content":%q}],"stream":%v%s}`, prompt, stream, extra)
}

func (e *env) post(ctx context.Context, body string) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.ts.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return e.client.Do(req)
}

func (e *env) mustPost(t *testing.T, body string) *http.Response {
	t.Helper()
	resp, err := e.post(context.Background(), body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (e *env) get(t *testing.T, path string) (int, string) {
	t.Helper()
	resp, err := e.client.Get(e.ts.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (e *env) stats(t *testing.T) Stats {
	t.Helper()
	_, body := e.get(t, "/stats")
	var s Stats
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("bad /stats body %q: %v", body, err)
	}
	return s
}

func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, msg)
}

type sseEvent struct {
	data string
	at   time.Duration // since start
}

// readSSE reads events until the stream ends, returning them and the error
// that ended the read (io.EOF for a clean end).
func readSSE(r io.Reader, start time.Time) ([]sseEvent, error) {
	br := bufio.NewReader(r)
	var events []sseEvent
	for {
		line, err := br.ReadString('\n')
		if strings.HasPrefix(line, "data: ") {
			events = append(events, sseEvent{strings.TrimSpace(strings.TrimPrefix(line, "data: ")), time.Since(start)})
		}
		if err != nil {
			return events, err
		}
	}
}

func contentOf(t *testing.T, ev sseEvent) (content string, hasContent bool, finish *string) {
	t.Helper()
	var c struct {
		Choices []struct {
			Delta        struct{ Content *string } `json:"delta"`
			FinishReason *string                   `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(ev.data), &c); err != nil || len(c.Choices) != 1 {
		t.Fatalf("bad chunk %q: %v", ev.data, err)
	}
	ch := c.Choices[0]
	if ch.Delta.Content != nil {
		return *ch.Delta.Content, true, ch.FinishReason
	}
	return "", false, ch.FinishReason
}

func errCode(t *testing.T, body string) string {
	t.Helper()
	var e struct{ Error struct{ Code string } }
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("not an error body: %q", body)
	}
	return e.Error.Code
}

// --- basic endpoints --------------------------------------------------------------

func TestModelsHealthAndStatsShape(t *testing.T) {
	e := newEnv(t)
	if code, body := e.get(t, "/v1/models"); code != 200 || !strings.Contains(body, `"qwen-7b"`) {
		t.Fatalf("models: %d %q", code, body)
	}
	if code, _ := e.get(t, "/health"); code != 200 {
		t.Fatalf("health: %d", code)
	}
	if code, body := e.get(t, "/readyz"); code != 200 || !strings.Contains(body, "ready") {
		t.Fatalf("readyz: %d %q", code, body)
	}
	_, body := e.get(t, "/stats")
	for _, k := range []string{"worker_id", "model", "status", "active_requests", "queue_depth", "queued_input_tokens",
		"recent_tokens_per_second", "completed", "failed", "rejected", "cancelled", "tokens_generated", "configured_ttft_ms", "configured_tokens_per_second"} {
		if !strings.Contains(body, `"`+k+`"`) {
			t.Fatalf("/stats is missing %q: %s", k, body)
		}
	}
	s := e.stats(t)
	if s.Model != "qwen-7b" || s.Status != "ready" || s.ConfiguredTTFTMillis != 30 || s.ConfiguredTokensPerSecond != 200 {
		t.Fatalf("unexpected stats %+v", s)
	}
}

func TestRequestValidationMatchesTheGateway(t *testing.T) {
	e := newEnv(t)
	tests := []struct {
		name, body string
		status     int
		code       string
	}{
		{"unknown model", `{"model":"nope","messages":[{"role":"user","content":"x"}]}`, 404, "MODEL_NOT_FOUND"},
		{"invalid json", `{nope`, 400, "INVALID_REQUEST"},
		{"ambiguous key", `{"model":"qwen-7b","MODEL":"x","messages":[{"role":"user","content":"x"}]}`, 400, "INVALID_REQUEST"},
		{"too large", `{"model":"qwen-7b","messages":[{"role":"user","content":"` + strings.Repeat("a", 2<<20) + `"}]}`, 413, "INVALID_REQUEST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := e.mustPost(t, tt.body)
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tt.status || errCode(t, string(b)) != tt.code {
				t.Fatalf("got %d %s", resp.StatusCode, b)
			}
		})
	}
}

// --- timing ------------------------------------------------------------------------

func TestStreamTimingAndShape(t *testing.T) {
	e := newEnv(t)
	start := time.Now()
	resp := e.mustPost(t, chatBody(true, "hello world", ""))
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}
	events, err := readSSE(resp.Body, start)
	if err != io.EOF {
		t.Fatalf("stream should end cleanly, got %v", err)
	}
	// role chunk + 10 token chunks + finish chunk + [DONE]
	if len(events) != 13 {
		t.Fatalf("got %d events: %v", len(events), events)
	}
	if events[len(events)-1].data != "[DONE]" {
		t.Fatalf("stream must end with [DONE], got %q", events[len(events)-1].data)
	}
	if c, has, _ := contentOf(t, events[0]); !has || c != "" {
		t.Fatalf("first chunk must be the role chunk with empty content, got %q", events[0].data)
	}
	if !strings.Contains(events[0].data, `"role":"assistant"`) {
		t.Fatalf("role chunk missing role: %s", events[0].data)
	}
	var text strings.Builder
	for i := 1; i <= 10; i++ {
		c, has, fin := contentOf(t, events[i])
		if !has || fin != nil {
			t.Fatalf("event %d should be a content chunk: %s", i, events[i].data)
		}
		text.WriteString(c)
	}
	if want := "tok0 tok1 tok2 tok3 tok4 tok5 tok6 tok7 tok8 tok9 "; text.String() != want {
		t.Fatalf("content %q, want %q", text.String(), want)
	}
	if _, _, fin := contentOf(t, events[11]); fin == nil || *fin != "stop" {
		t.Fatalf("final chunk must finish with stop: %s", events[11].data)
	}

	// Hard lower bounds from the pacing contract: nothing before the TTFT, and
	// token i not before TTFT + i/tokens-per-second. Upper bounds stay loose
	// so a loaded machine does not fail the test.
	const ttft, interval = 30 * time.Millisecond, 5 * time.Millisecond
	if events[0].at < ttft {
		t.Fatalf("first chunk arrived at %v, before the %v TTFT", events[0].at, ttft)
	}
	for i := 1; i <= 10; i++ {
		if min := ttft + time.Duration(i-1)*interval; events[i].at < min {
			t.Fatalf("token %d arrived at %v, before its deadline %v", i-1, events[i].at, min)
		}
	}
	if total := events[10].at; total > ttft+9*interval+time.Second {
		t.Fatalf("stream took %v, far longer than configured", total)
	}
}

func TestMaxTokensCapsOutputAndSetsFinishReason(t *testing.T) {
	e := newEnv(t)
	start := time.Now()
	resp := e.mustPost(t, chatBody(true, "hi", `,"max_tokens":3`))
	events, _ := readSSE(resp.Body, start)
	// role + 3 tokens + finish + [DONE]
	if len(events) != 6 {
		t.Fatalf("got %d events: %v", len(events), events)
	}
	if _, _, fin := contentOf(t, events[4]); fin == nil || *fin != "length" {
		t.Fatalf("a request cut short by max_tokens must finish with length: %s", events[4].data)
	}
	// A limit at or above the natural length does not truncate.
	resp2 := e.mustPost(t, chatBody(true, "hi", `,"max_tokens":50`))
	events2, _ := readSSE(resp2.Body, start)
	if _, _, fin := contentOf(t, events2[len(events2)-2]); fin == nil || *fin != "stop" {
		t.Fatalf("expected stop, got %s", events2[len(events2)-2].data)
	}
}

func TestNonStreamResponse(t *testing.T) {
	e := newEnv(t)
	start := time.Now()
	resp := e.mustPost(t, chatBody(false, "one two three", ""))
	body, err := io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("%d %v", resp.StatusCode, err)
	}
	if min := 30*time.Millisecond + 9*5*time.Millisecond; elapsed < min {
		t.Fatalf("non-stream response after %v, before the last token's deadline %v", elapsed, min)
	}
	var c struct {
		Object  string
		Choices []struct {
			Message      struct{ Role, Content string }
			FinishReason string `json:"finish_reason"`
		}
		Usage struct{ Prompt, Completion, Total int }
	}
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	_ = json.Unmarshal(body, &c)
	if c.Object != "chat.completion" || len(c.Choices) != 1 || c.Choices[0].Message.Role != "assistant" || c.Choices[0].FinishReason != "stop" {
		t.Fatalf("unexpected body %s", body)
	}
	if got := strings.Fields(c.Choices[0].Message.Content); len(got) != 10 {
		t.Fatalf("expected 10 tokens, got %v", got)
	}
	u := raw["usage"].(map[string]any)
	if u["prompt_tokens"] != float64(3) || u["completion_tokens"] != float64(10) || u["total_tokens"] != float64(13) {
		t.Fatalf("unexpected usage %v", u)
	}
}

// --- queueing ------------------------------------------------------------------------

func TestQueueIsFIFOAndBounded(t *testing.T) {
	e := newEnv(t, func(c *Config) {
		c.MaxConcurrency = 1
		c.QueueSize = 2
		c.TTFT = 120 * time.Millisecond
		c.OutputTokens = 2
	})

	type done struct {
		name string
		at   time.Time
	}
	finished := make(chan done, 3)
	launch := func(name, prompt string) {
		go func() {
			resp, err := e.post(context.Background(), chatBody(false, prompt, ""))
			if err != nil {
				t.Errorf("%s: %v", name, err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			finished <- done{name, time.Now()}
		}()
	}
	launch("A", "a")
	eventually(t, 2*time.Second, func() bool { return e.stats(t).ActiveRequests == 1 }, "A running")
	launch("B", "b1 b2")
	eventually(t, 2*time.Second, func() bool { return e.stats(t).QueueDepth == 1 }, "B queued")
	launch("C", "c1 c2 c3 c4")
	eventually(t, 2*time.Second, func() bool { return e.stats(t).QueueDepth == 2 }, "C queued")

	s := e.stats(t)
	if s.ActiveRequests != 1 || s.QueueDepth != 2 || s.QueuedInputTokens != 2+4 {
		t.Fatalf("stats while saturated: %+v", s)
	}

	// A fourth request does not fit: it is rejected at once, not queued.
	t0 := time.Now()
	resp := e.mustPost(t, chatBody(false, "d", ""))
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || errCode(t, string(b)) != "queue_full" {
		t.Fatalf("expected 503 queue_full, got %d %s", resp.StatusCode, b)
	}
	if time.Since(t0) > 300*time.Millisecond { // far below the 500ms a queued request would wait
		t.Fatalf("rejection took %v; it must be immediate", time.Since(t0))
	}
	if e.stats(t).Rejected != 1 {
		t.Fatalf("rejected counter: %+v", e.stats(t))
	}

	var order []string
	for i := 0; i < 3; i++ {
		select {
		case d := <-finished:
			order = append(order, d.name)
		case <-time.After(5 * time.Second):
			t.Fatal("a request never finished")
		}
	}
	if strings.Join(order, "") != "ABC" {
		t.Fatalf("requests completed out of arrival order: %v", order)
	}
	if s := e.stats(t); s.ActiveRequests != 0 || s.QueueDepth != 0 || s.QueuedInputTokens != 0 || s.Completed != 3 {
		t.Fatalf("worker not idle at the end: %+v", s)
	}
}

func TestClientCancelWhileQueuedLeavesTheQueue(t *testing.T) {
	// A runs for a long time, so B can only leave the queue because it was
	// cancelled, never because it got its turn.
	e := newEnv(t, func(c *Config) { c.MaxConcurrency = 1; c.QueueSize = 2; c.TTFT = 5 * time.Second })
	actx, acancel := context.WithCancel(context.Background())
	defer acancel()
	go func() {
		if resp, err := e.post(actx, chatBody(false, "a", "")); err == nil {
			_ = resp.Body.Close()
		}
	}()
	eventually(t, 2*time.Second, func() bool { return e.stats(t).ActiveRequests == 1 }, "A running")

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = e.post(ctx, chatBody(false, "queued prompt here", "")) }()
	eventually(t, 2*time.Second, func() bool { return e.stats(t).QueueDepth == 1 }, "B queued")

	cancel()
	eventually(t, time.Second, func() bool { s := e.stats(t); return s.QueueDepth == 0 && s.QueuedInputTokens == 0 }, "B to leave the queue promptly")
	if s := e.stats(t); s.Cancelled != 1 || s.ActiveRequests != 1 || s.Completed != 0 {
		t.Fatalf("B must be cancelled, not served, and A must be unaffected: %+v", s)
	}
}

func TestClientCancelMidGenerationStopsAndFreesTheSlot(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.MaxConcurrency = 1; c.TokensPerSecond = 50; c.OutputTokens = 200 }) // ~4s if left alone
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := e.post(ctx, chatBody(true, "hi", ""))
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(resp.Body)
	_, _ = br.ReadString('\n') // the role chunk: generation is under way
	cancel()
	_ = resp.Body.Close()

	eventually(t, time.Second, func() bool { return e.stats(t).ActiveRequests == 0 }, "slot to be freed")
	before := e.stats(t).TokensGenerated
	time.Sleep(150 * time.Millisecond)
	after := e.stats(t)
	if after.TokensGenerated != before {
		t.Fatalf("generation continued after the client left: %d -> %d", before, after.TokensGenerated)
	}
	if after.TokensGenerated >= 200 || after.Cancelled != 1 || after.Completed != 0 {
		t.Fatalf("unexpected stats after cancel: %+v", after)
	}
}

// --- failure injection --------------------------------------------------------------

func TestFailureModes(t *testing.T) {
	failing := func(mode FailureMode) func(*Config) {
		return func(c *Config) { c.FailureRate = 1; c.FailureMode = mode }
	}

	t.Run("error", func(t *testing.T) {
		e := newEnv(t, failing(ModeError))
		resp := e.mustPost(t, chatBody(false, "x", ""))
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 500 || errCode(t, string(b)) != "mock_failure" {
			t.Fatalf("got %d %s", resp.StatusCode, b)
		}
		if s := e.stats(t); s.Failed != 1 || s.ActiveRequests != 0 {
			t.Fatalf("stats %+v", s)
		}
	})
	t.Run("unavailable", func(t *testing.T) {
		e := newEnv(t, failing(ModeUnavailable))
		resp := e.mustPost(t, chatBody(true, "x", ""))
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 503 || errCode(t, string(b)) != "mock_failure" {
			t.Fatalf("got %d %s", resp.StatusCode, b)
		}
		if s := e.stats(t); s.Failed != 1 || s.ActiveRequests != 0 {
			t.Fatalf("stats %+v", s)
		}
	})
	t.Run("drop", func(t *testing.T) {
		e := newEnv(t, failing(ModeDrop))
		if resp, err := e.post(context.Background(), chatBody(false, "x", "")); err == nil {
			_ = resp.Body.Close()
			t.Fatalf("expected the connection to be dropped, got status %d", resp.StatusCode)
		}
		eventually(t, time.Second, func() bool { return e.stats(t).Failed == 1 }, "failure to be counted")
	})
	t.Run("midstream stream", func(t *testing.T) {
		e := newEnv(t, failing(ModeMidstream))
		resp := e.mustPost(t, chatBody(true, "x", ""))
		events, err := readSSE(resp.Body, time.Now())
		if err == io.EOF {
			t.Fatal("a midstream failure must break the connection, not end the stream cleanly")
		}
		// role chunk plus the tokens let through, but no finish chunk and no [DONE].
		if len(events) != 3 { // exactly: the role chunk and two tokens
			t.Fatalf("got %d events before the abort: %v", len(events), events)
		}
		for _, ev := range events {
			if ev.data == "[DONE]" {
				t.Fatal("a failed stream must not look complete")
			}
		}
		eventually(t, time.Second, func() bool { s := e.stats(t); return s.Failed == 1 && s.ActiveRequests == 0 }, "failure counted and slot freed")
	})
	t.Run("midstream non-stream", func(t *testing.T) {
		e := newEnv(t, failing(ModeMidstream))
		resp, err := e.post(context.Background(), chatBody(false, "x", ""))
		if err != nil {
			t.Fatalf("headers must arrive before the body is cut: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 200 || resp.ContentLength <= 0 {
			t.Fatalf("expected a 200 with a declared length, got %d (length %d)", resp.StatusCode, resp.ContentLength)
		}
		if _, rerr := io.ReadAll(resp.Body); !errors.Is(rerr, io.ErrUnexpectedEOF) {
			t.Fatalf("a truncated body must be an unexpected EOF, got %v", rerr)
		}
		eventually(t, time.Second, func() bool { s := e.stats(t); return s.Failed == 1 && s.ActiveRequests == 0 }, "failure counted and slot freed")
	})
	t.Run("midstream with a single output token still fails", func(t *testing.T) {
		for _, stream := range []bool{true, false} {
			e := newEnv(t, failing(ModeMidstream), func(c *Config) { c.OutputTokens = 1 })
			resp, err := e.post(context.Background(), chatBody(stream, "x", ""))
			if err == nil {
				body, rerr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if rerr == nil && (!stream || strings.Contains(string(body), "[DONE]")) {
					t.Fatalf("stream=%v: an injected midstream failure silently succeeded: %q", stream, body)
				}
			}
			eventually(t, time.Second, func() bool { return e.stats(t).Failed == 1 }, "failure counted")
		}
	})
}

func TestRateZeroNeverFails(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.FailureRate = 0; c.OutputTokens = 1; c.TTFT = 0; c.MaxConcurrency = 8 })
	for i := 0; i < 40; i++ {
		resp := e.mustPost(t, chatBody(false, "x", ""))
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("request %d failed with %d at rate 0", i, resp.StatusCode)
		}
	}
}

func TestSameSeedReproducesTheFailureSequence(t *testing.T) {
	sequence := func() []int {
		e := newEnv(t, func(c *Config) {
			c.Seed = 1234
			c.FailureRate = 0.5
			c.FailureMode = ModeUnavailable
			c.OutputTokens = 1
			c.TTFT = 0
		})
		var out []int
		for i := 0; i < 40; i++ {
			resp := e.mustPost(t, chatBody(false, "x", ""))
			_, _ = io.Copy(io.Discard, resp.Body)
			out = append(out, resp.StatusCode)
		}
		return out
	}
	a, b := sequence(), sequence()
	saw200, saw503 := false, false
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("sequences diverged at request %d: %d vs %d", i, a[i], b[i])
		}
		saw200 = saw200 || a[i] == 200
		saw503 = saw503 || a[i] == 503
	}
	if !saw200 || !saw503 {
		t.Fatalf("expected both successes and failures at rate 0.5, got %v", a)
	}
}

// --- lifecycle -----------------------------------------------------------------------

func TestStartupDelayGatesReadiness(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.StartupDelay = 1500 * time.Millisecond })
	if code, body := e.get(t, "/readyz"); code != 503 || !strings.Contains(body, "starting") {
		t.Fatalf("readyz while starting: %d %q", code, body)
	}
	if st := e.stats(t).Status; st != "starting" {
		t.Fatalf("/stats status while starting is %q", st)
	}
	if code, _ := e.get(t, "/health"); code != 200 {
		t.Fatalf("liveness must not depend on readiness, got %d", code)
	}
	if code, _ := e.get(t, "/v1/models"); code != 503 {
		t.Fatalf("models while starting: %d", code)
	}
	resp := e.mustPost(t, chatBody(false, "x", ""))
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || errCode(t, string(b)) != "not_ready" {
		t.Fatalf("chat while starting: %d %s", resp.StatusCode, b)
	}
	eventually(t, 5*time.Second, func() bool { code, _ := e.get(t, "/readyz"); return code == 200 }, "readiness after the startup delay")
	if st := e.stats(t).Status; st != "ready" {
		t.Fatalf("/stats status after startup is %q", st)
	}
	if code, _ := e.get(t, "/v1/models"); code != 200 {
		t.Fatalf("models after startup: %d", code)
	}
}

func serve(t *testing.T, cfg Config) (url string, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancelFn := context.WithCancel(context.Background())
	d := make(chan error, 1)
	go func() { d <- srv.Serve(ctx, ln) }()
	t.Cleanup(cancelFn)
	return "http://" + ln.Addr().String(), cancelFn, d
}

func TestGracefulDrainLetsInFlightFinish(t *testing.T) {
	cfg := testConfig()
	cfg.TokensPerSecond = 25 // 10 tokens ~ 400ms
	url, stop, done := serve(t, cfg)

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	defer tr.CloseIdleConnections()
	resp, err := client.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chatBody(true, "hi", "")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)
	_, _ = br.ReadString('\n') // stream in flight

	stop() // SIGTERM equivalent
	eventually(t, 2*time.Second, func() bool {
		r, err := http.Get(url + "/readyz")
		if err != nil {
			return false
		}
		defer func() { _ = r.Body.Close() }()
		return r.StatusCode == 503
	}, "readiness to report draining")
	if sr, err := client.Get(url + "/stats"); err == nil {
		var st Stats
		_ = json.NewDecoder(sr.Body).Decode(&st)
		_ = sr.Body.Close()
		if st.Status != "draining" {
			t.Fatalf("/stats status while draining is %q", st.Status)
		}
	}
	r, err := client.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chatBody(false, "x", "")))
	if err != nil {
		t.Fatalf("a draining worker should answer, not refuse the connection: %v", err)
	}
	b, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if r.StatusCode != 503 || errCode(t, string(b)) != "draining" {
		t.Fatalf("new work while draining: %d %s", r.StatusCode, b)
	}

	rest, _ := io.ReadAll(br)
	if !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("the in-flight stream was cut off: %q", rest)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a clean drain must return nil, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after the drain")
	}
}

func TestDrainTimeoutCutsOffStragglers(t *testing.T) {
	cfg := testConfig()
	cfg.TokensPerSecond = 1
	cfg.OutputTokens = 100 // would take ~100s
	cfg.DrainTimeout = 500 * time.Millisecond
	url, stop, done := serve(t, cfg)

	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chatBody(true, "hi", "")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = bufio.NewReader(resp.Body).ReadString('\n')

	stop()
	stopped := time.Now()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "drain timed out") {
			t.Fatalf("expected a drain timeout error, got %v", err)
		}
		if took := time.Since(stopped); took > 800*time.Millisecond { // a doubled deadline would take ~1s
			t.Fatalf("a 500ms drain timeout took %v to give up", took)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not give up after the drain timeout")
	}
}

func TestStopWithNothingInFlightExitsCleanly(t *testing.T) {
	_, stop, done := serve(t, testConfig())
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle worker did not exit")
	}
}

// --- stats and resources -------------------------------------------------------------

func TestRecentThroughputTracksOutput(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.TTFT = 0; c.TokensPerSecond = 100; c.OutputTokens = 200; c.MaxConcurrency = 1 })
	start := time.Now()
	resp := e.mustPost(t, chatBody(false, "hi", ""))
	_, _ = io.Copy(io.Discard, resp.Body)
	elapsed := time.Since(start)
	s := e.stats(t)
	if s.TokensGenerated != 200 {
		t.Fatalf("tokens generated %d, want 200", s.TokensGenerated)
	}
	want := 200 / elapsed.Seconds() // the rate actually achieved
	if s.RecentTokensPerSecond < want*0.5 || s.RecentTokensPerSecond > want*2 {
		t.Fatalf("recent_tokens_per_second %.1f is nowhere near the achieved %.1f", s.RecentTokensPerSecond, want)
	}
}

func TestNoGoroutineLeakAcrossFailuresAndCancels(t *testing.T) {
	modes := []FailureMode{ModeError, ModeUnavailable, ModeDrop, ModeMidstream}
	var envs []*env
	for _, m := range modes {
		m := m
		envs = append(envs, newEnv(t, func(c *Config) {
			c.FailureRate = 1
			c.FailureMode = m
			c.TTFT = 5 * time.Millisecond
			c.OutputTokens = 6
		}))
	}
	healthy := newEnv(t, func(c *Config) { c.TTFT = 200 * time.Millisecond })

	settle := func() int {
		for _, e := range append(envs, healthy) {
			e.client.CloseIdleConnections()
		}
		time.Sleep(200 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	base := settle()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		for _, e := range envs {
			e := e
			wg.Add(1)
			go func() {
				defer wg.Done()
				if resp, err := e.post(context.Background(), chatBody(i%2 == 0, "x", "")); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			}()
		}
		wg.Add(1)
		go func() { // a client that gives up while waiting for the TTFT
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if resp, err := healthy.post(ctx, chatBody(true, "x", "")); err == nil {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	eventually(t, 5*time.Second, func() bool { return settle() <= base+5 }, fmt.Sprintf("goroutines to return near the baseline of %d", base))
	for _, e := range append(envs, healthy) {
		eventually(t, 2*time.Second, func() bool { return e.stats(t).ActiveRequests == 0 && e.stats(t).QueueDepth == 0 }, "every slot to be released")
	}
}

// --- drain races (independent review and verification) ------------------------------

func shortGrace(t *testing.T) {
	t.Helper()
	old := shutdownGrace
	shutdownGrace = 200 * time.Millisecond
	t.Cleanup(func() { shutdownGrace = old })
}

func TestDrainWaitsForARequestStillSendingItsBody(t *testing.T) {
	shortGrace(t)
	cfg := testConfig()
	cfg.TokensPerSecond = 12 // 10 tokens ~ 0.8s of generation
	cfg.TTFT = 0
	url, stop, done := serve(t, cfg)

	conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	body := chatBody(false, "slow sender", "")
	// Headers and half the body: the handler is running but has not reached the
	// queue yet, which is the window the old drain could not see.
	_, _ = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body[:len(body)/2])
	time.Sleep(100 * time.Millisecond)

	stop() // the drain begins while the request is still arriving
	time.Sleep(100 * time.Millisecond)
	_, _ = io.WriteString(conn, body[len(body)/2:]) // ...and the body completes

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("a request admitted before the drain must still be answered: %v", err)
	}
	b, rerr := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || rerr != nil || !strings.Contains(string(b), "tok9") {
		t.Fatalf("response was cut off or wrong: %d %v %q", resp.StatusCode, rerr, b)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a clean drain must return nil, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return")
	}
}

func TestDrainIgnoresStrayIdleConnections(t *testing.T) {
	shortGrace(t)
	url, stop, done := serve(t, testConfig())

	// A connection that connects and never sends a request.
	conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("an idle connection must not turn a clean drain into a failure: %v", err)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("drain took %v because of an idle connection", took)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Serve did not return")
	}
}

func TestDrainWaitsForQueuedRequestsToo(t *testing.T) {
	shortGrace(t)
	cfg := testConfig()
	cfg.MaxConcurrency = 1
	cfg.QueueSize = 2
	cfg.TTFT = 300 * time.Millisecond
	cfg.OutputTokens = 2
	url, stop, done := serve(t, cfg)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chatBody(false, "x", "")))
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			statuses[i] = resp.StatusCode
		}()
		time.Sleep(60 * time.Millisecond) // fix arrival order: one running, one queued
	}
	stop()
	wg.Wait()
	if statuses[0] != 200 || statuses[1] != 200 {
		t.Fatalf("the running and the queued request must both finish during the drain, got %v", statuses)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return")
	}
}

// Drain timing at the real (default) grace period, not the shortened test one.

func TestStrayConnectionDoesNotDelayTheDrainAtTheDefaultGrace(t *testing.T) {
	url, stop, done := serve(t, testConfig())
	conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > 1500*time.Millisecond {
			t.Fatalf("a never-used connection held the drain for %v", took)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Serve did not return")
	}
}

func TestDrainTimeoutBoundsTheWholeShutdown(t *testing.T) {
	cfg := testConfig()
	cfg.TTFT = 200 * time.Millisecond
	cfg.OutputTokens = 1
	cfg.DrainTimeout = 300 * time.Millisecond
	url, stop, done := serve(t, cfg)

	conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://")) // a stray connection
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	go func() {
		if resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chatBody(false, "x", ""))); err == nil {
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	stop()
	select {
	case <-done:
		// The request ends ~100ms in; the grace is then capped by the ~200ms of
		// budget left, so the whole shutdown is ~300ms. An uncapped 500ms grace
		// would make it ~600ms.
		if took := time.Since(start); took > 480*time.Millisecond {
			t.Fatalf("shutdown took %v with a 300ms drain timeout", took)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Serve did not return")
	}
}
