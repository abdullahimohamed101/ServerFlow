package gateway

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

// scriptWorker is a worker whose behaviour a test writes, recording what it was sent.
type scriptWorker struct {
	id         string
	srv        *httptest.Server
	hits       atomic.Int64
	mu         sync.Mutex
	requestIDs []string
	attemptIDs []string
	auths      []string
}

func newScriptWorker(t *testing.T, id string, h func(w http.ResponseWriter, r *http.Request)) *scriptWorker {
	t.Helper()
	sw := &scriptWorker{id: id}
	sw.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw.hits.Add(1)
		sw.mu.Lock()
		sw.requestIDs = append(sw.requestIDs, r.Header.Get("X-Request-ID"))
		sw.attemptIDs = append(sw.attemptIDs, r.Header.Get("X-Attempt-ID"))
		sw.auths = append(sw.auths, r.Header.Get("Authorization"))
		sw.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(sw.srv.Close)
	return sw
}

func (sw *scriptWorker) snapshot(model string) protocol.WorkerSnapshot {
	s := snapWorker(sw.id, model, 0)
	s.Address, s.MaxConcurrency = sw.srv.URL, 8
	return s
}

func okJSON(id string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"worker":"`+id+`"}`)
	}
}

func status(code int, body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
}

// refusing returns a snapshot for a worker whose port nobody listens on.
func refusing(t *testing.T, id string) protocol.WorkerSnapshot {
	t.Helper()
	sw := newScriptWorker(t, id, okJSON(id))
	s := sw.snapshot("qwen-7b")
	sw.srv.Close()
	return s
}

func dropConnection(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) }

func sseEvents(id string, n int) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for i := 0; i < n; i++ {
			_, _ = io.WriteString(w, `data: {"worker":"`+id+`","i":`+string(rune('0'+i))+"}\n\n")
			w.(http.Flusher).Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}
}

func emptyStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler) // closes with nothing sent
}

func (e *regEnv) post(t *testing.T, stream bool, hdr ...string) (*http.Response, string) {
	t.Helper()
	body := `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]`
	if stream {
		body += `,"stream":true`
	}
	req, _ := http.NewRequest(http.MethodPost, e.url+chatCompletionsPath, strings.NewReader(body+"}"))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func (e *regEnv) attemptLines(t *testing.T) []map[string]any {
	var out []map[string]any
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "attempt finished" {
			out = append(out, l)
		}
	}
	return out
}

func (e *regEnv) requestLine(t *testing.T) map[string]any {
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "request" && l["path"] == chatCompletionsPath {
			return l
		}
	}
	return nil
}

func allIdle(t *testing.T, e *regEnv, ids ...string) {
	t.Helper()
	eventually(t, 5*time.Second, func() bool {
		for _, id := range ids {
			if e.gw.router.InFlight(id) != 0 {
				return false
			}
		}
		return true
	}, "every slot must be released")
}

// "a-" workers sort first, so round-robin and the least-* strategies (all tied) try them first.

func TestAConnectionFailureIsRetriedOnAnotherWorker(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "a-bad"), good.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("X-ServerFlow-Attempts") != "2" {
		t.Fatalf("a retry must be visible to the client: %v", resp.Header)
	}
	lines := e.attemptLines(t)
	if len(lines) != 2 {
		t.Fatalf("two attempts on record, got %d:\n%s", len(lines), e.logs.String())
	}
	first, second := lines[0], lines[1]
	if first["outcome"] != "retried" || first["class"] != "connect" || first["worker_id"] != "a-bad" ||
		second["outcome"] != "ok" || second["worker_id"] != "b-good" || first["attempt_id"] == second["attempt_id"] ||
		first["request_id"] != second["request_id"] || first["attempt"] != float64(1) || second["attempt"] != float64(2) {
		t.Fatalf("attempt history: %v / %v", first, second)
	}
	if rl := e.requestLine(t); rl == nil || rl["attempts"] != float64(2) || rl["worker_id"] != "b-good" {
		t.Fatalf("the request line must summarise the attempts: %v", rl)
	}
	allIdle(t, e, "a-bad", "b-good")
}

func TestRetryableWorkerStatusesAreRetried(t *testing.T) {
	for _, code := range []int{502, 503} {
		bad := newScriptWorker(t, "a-bad", status(code, `{"error":{"code":"x"}}`))
		good := newScriptWorker(t, "b-good", okJSON("b-good"))
		e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
		resp, body := e.post(t, false)
		if resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" || bad.hits.Load() != 1 {
			t.Fatalf("%d: %d %s (bad hits %d)", code, resp.StatusCode, body, bad.hits.Load())
		}
		if got := e.attemptLines(t)[0]["class"]; got != "status_"+strconv.Itoa(code) {
			t.Fatalf("class %v", got)
		}
	}
}

func TestFinalStatusesAreNeverRetried(t *testing.T) {
	for _, code := range []int{400, 401, 404, 422, 429, 500, 501, 504} {
		bad := newScriptWorker(t, "a-bad", status(code, `{"error":{"message":"worker says no"}}`))
		good := newScriptWorker(t, "b-good", okJSON("b-good"))
		e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
		resp, body := e.post(t, false)
		if resp.StatusCode != code || !strings.Contains(body, "worker says no") {
			t.Fatalf("%d must be relayed unchanged: %d %s", code, resp.StatusCode, body)
		}
		if good.hits.Load() != 0 || resp.Header.Get("X-ServerFlow-Attempts") != "" || len(e.attemptLines(t)) != 1 {
			t.Fatalf("%d: no second attempt may start (good hits %d)", code, good.hits.Load())
		}
	}
}

func TestAConfiguredStatusListChangesWhatIsRetried(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(500, `{}`))
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")}, func(c *config.GatewayConfig) {
		c.RetryStatuses = []int{500}
	})
	if resp, body := e.post(t, false); resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" {
		t.Fatalf("a configured 500 is retried: %d %s", resp.StatusCode, body)
	}
	bad2 := newScriptWorker(t, "a-bad", status(503, `{"error":{"message":"unavailable"}}`))
	good2 := newScriptWorker(t, "b-good", okJSON("b-good"))
	e2 := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad2.snapshot("qwen-7b"), good2.snapshot("qwen-7b")}, func(c *config.GatewayConfig) {
		c.RetryStatuses = nil
	})
	if resp, _ := e2.post(t, false); resp.StatusCode != 503 || good2.hits.Load() != 0 {
		t.Fatalf("with an empty list nothing is retried: %d", resp.StatusCode)
	}
}

func TestAConnectionDroppedBeforeAnyResponseIsRetried(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", dropConnection)
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if got := e.attemptLines(t)[0]["class"]; got != "reset" {
		t.Fatalf("class %v", got)
	}
}

func TestAStreamThatEndsBeforeItsFirstByteIsRetriedAndTheClientSeesOnlyTheGoodStream(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", emptyStream)
	good := newScriptWorker(t, "b-good", sseEvents("b-good", 3))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	resp, body := e.post(t, true)
	want := `data: {"worker":"b-good","i":0}` + "\n\n" + `data: {"worker":"b-good","i":1}` + "\n\n" + `data: {"worker":"b-good","i":2}` + "\n\n" + "data: [DONE]\n\n"
	if resp.StatusCode != 200 || body != want {
		t.Fatalf("the stream must arrive intact and in order, from the good worker only:\n%q\nwant\n%q", body, want)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") || resp.Header.Get("X-ServerFlow-Attempts") != "2" {
		t.Fatalf("headers: %v", resp.Header)
	}
	if got := e.attemptLines(t)[0]["class"]; got != "empty_stream" {
		t.Fatalf("class %v", got)
	}
	if rl := e.requestLine(t); rl["ttft_ms"] == nil {
		t.Fatalf("TTFT must still be measured: %v", rl)
	}
}

func TestOnceTheFirstChunkIsOutAFailureIsNeverRetried(t *testing.T) {
	dies := newScriptWorker(t, "a-dies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"worker\":\"a-dies\"}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(50 * time.Millisecond)
		panic(http.ErrAbortHandler)
	})
	good := newScriptWorker(t, "b-good", sseEvents("b-good", 2))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{dies.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	resp, body := e.post(t, true)
	if resp.StatusCode != 200 || !strings.Contains(body, "a-dies") || !strings.Contains(body, "INFERENCE_FAILED") {
		t.Fatalf("the stream ends with the error event: %d %q", resp.StatusCode, body)
	}
	if strings.Contains(body, "b-good") || good.hits.Load() != 0 {
		t.Fatal("output had begun, so a second worker must never be contacted")
	}
	if lines := e.attemptLines(t); len(lines) != 1 || lines[0]["outcome"] != "failed" {
		t.Fatalf("exactly one failed attempt: %v", lines)
	}
	if resp.Header.Get("X-ServerFlow-Attempts") != "" {
		t.Fatal("one attempt is not worth a header")
	}
}

func TestAStalledStreamIsCutByTheIdleTimeoutAndNotRetried(t *testing.T) {
	stall := newScriptWorker(t, "a-stall", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	good := newScriptWorker(t, "b-good", sseEvents("b-good", 1))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{stall.snapshot("qwen-7b"), good.snapshot("qwen-7b")}, func(c *config.GatewayConfig) {
		c.UpstreamIdleTimeout = 300 * time.Millisecond
	})
	start := time.Now()
	resp, body := e.post(t, true)
	if resp.StatusCode != http.StatusGatewayTimeout || !strings.Contains(body, "UPSTREAM_TIMEOUT") {
		t.Fatalf("a worker that never sends a byte is a timeout: %d %s", resp.StatusCode, body)
	}
	if good.hits.Load() != 0 || time.Since(start) > 3*time.Second {
		t.Fatalf("timeouts are not retried (ADR-012): good hits %d after %v", good.hits.Load(), time.Since(start))
	}
	allIdle(t, e, "a-stall", "b-good")
}

func TestWhenNoOtherWorkerExistsTheWorkersOwnResponseIsRelayed(t *testing.T) {
	only := newScriptWorker(t, "only", status(503, `{"error":{"code":"queue_full","message":"the worker queue is full"}}`))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{only.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 503 || !strings.Contains(body, "queue_full") {
		t.Fatalf("the real error is shown, not a gateway-invented one: %d %s", resp.StatusCode, body)
	}
	if only.hits.Load() != 1 || len(e.attemptLines(t)) != 1 || e.attemptLines(t)[0]["outcome"] != "failed" {
		t.Fatalf("one attempt, failed: %d hits, %v", only.hits.Load(), e.attemptLines(t))
	}
	allIdle(t, e, "only")

	// A single worker that refuses connections is the gateway's own 503.
	e2 := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "gone")})
	resp, body = e2.post(t, false)
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestMaxAttemptsBoundsTheTries(t *testing.T) {
	// max_attempts 1: no retry even though a good worker exists.
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "a-bad"), good.snapshot("qwen-7b")}, func(c *config.GatewayConfig) {
		c.MaxAttempts = 1
	})
	if resp, _ := e.post(t, false); resp.StatusCode != 503 || good.hits.Load() != 0 {
		t.Fatalf("one attempt means one attempt: %d (good hits %d)", resp.StatusCode, good.hits.Load())
	}

	// max_attempts 2 with two bad workers and a good one: the third is never tried.
	b1, b2 := newScriptWorker(t, "a-1", status(503, `{}`)), newScriptWorker(t, "b-2", status(503, `{}`))
	g := newScriptWorker(t, "c-good", okJSON("c-good"))
	e2 := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{b1.snapshot("qwen-7b"), b2.snapshot("qwen-7b"), g.snapshot("qwen-7b")})
	resp, _ := e2.post(t, false)
	if resp.StatusCode != 503 || g.hits.Load() != 0 || b1.hits.Load() != 1 || b2.hits.Load() != 1 {
		t.Fatalf("exactly two attempts: %d hits %d/%d/%d", resp.StatusCode, b1.hits.Load(), b2.hits.Load(), g.hits.Load())
	}
	if resp.Header.Get("X-ServerFlow-Attempts") != "2" {
		t.Fatalf("headers: %v", resp.Header)
	}
	lines := e2.attemptLines(t)
	if len(lines) != 2 || lines[0]["outcome"] != "retried" || lines[1]["outcome"] != "failed" {
		t.Fatalf("history: %v", lines)
	}
	allIdle(t, e2, "a-1", "b-2", "c-good")

	// max_attempts 3: the third worker is reached, and no worker is tried twice.
	b3, b4 := newScriptWorker(t, "a-1", status(503, `{}`)), newScriptWorker(t, "b-2", status(503, `{}`))
	g3 := newScriptWorker(t, "c-good", okJSON("c-good"))
	e3 := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{b3.snapshot("qwen-7b"), b4.snapshot("qwen-7b"), g3.snapshot("qwen-7b")}, func(c *config.GatewayConfig) {
		c.MaxAttempts = 3
	})
	resp, body := e3.post(t, false)
	if resp.StatusCode != 200 || whoAnswered(t, body) != "c-good" || resp.Header.Get("X-ServerFlow-Attempts") != "3" ||
		b3.hits.Load() != 1 || b4.hits.Load() != 1 || g3.hits.Load() != 1 {
		t.Fatalf("%d %s hits %d/%d/%d", resp.StatusCode, body, b3.hits.Load(), b4.hits.Load(), g3.hits.Load())
	}
}

func TestEveryAttemptCarriesTheRequestIDAndItsOwnAttemptIDAndNoClientCredentials(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{}`))
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	resp, _ := e.post(t, false, "Authorization", "Bearer client-secret", "X-Attempt-ID", "client-chosen")
	reqID := resp.Header.Get("X-Request-ID")
	bad.mu.Lock()
	good.mu.Lock()
	defer bad.mu.Unlock()
	defer good.mu.Unlock()
	if len(bad.requestIDs) != 1 || len(good.requestIDs) != 1 || bad.requestIDs[0] != reqID || good.requestIDs[0] != reqID {
		t.Fatalf("one request ID across attempts: %q %q vs %q", bad.requestIDs, good.requestIDs, reqID)
	}
	a1, a2 := bad.attemptIDs[0], good.attemptIDs[0]
	if !strings.HasPrefix(a1, "att_") || !strings.HasPrefix(a2, "att_") || a1 == a2 || a1 == "client-chosen" || a2 == "client-chosen" {
		t.Fatalf("gateway-minted, distinct attempt IDs, never the client's: %q %q", a1, a2)
	}
	if bad.auths[0] != "" || good.auths[0] != "" {
		t.Fatal("the client's credentials must never reach a worker")
	}
	lines := e.attemptLines(t)
	if lines[0]["attempt_id"] != a1 || lines[1]["attempt_id"] != a2 {
		t.Fatalf("the logged attempt IDs must be the ones the workers saw: %v vs %q %q", lines, a1, a2)
	}
}

func TestAClientWhoLeavesStopsTheRetries(t *testing.T) {
	release := make(chan struct{})
	// gone is closed when the gateway cancels the first attempt, which it does only after it noticed the client left.
	// The test must wait for that before letting the worker answer 503: the gateway learns of a disconnect
	// asynchronously, and a 503 that arrives first would be a retryable failure of a client that is still
	// (as far as the gateway knows) there. Waiting on the event, not on time, makes the test deterministic.
	gone := make(chan struct{})
	var goneOnce sync.Once
	slow := newScriptWorker(t, "a-slow", func(w http.ResponseWriter, r *http.Request) {
		// net/http only watches a connection for closing once the handler has read the request body.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
			goneOnce.Do(func() { close(gone) })
			return
		}
		w.WriteHeader(503)
	})
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{slow.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath,
		strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	errc := make(chan error, 1)
	go func() { _, err := e.client.Do(req); errc <- err }()
	eventually(t, 5*time.Second, func() bool { return slow.hits.Load() == 1 }, "first attempt in flight")
	cancel()
	<-errc
	select {
	case <-gone:
	case <-time.After(10 * time.Second):
		t.Fatal("the gateway never cancelled the first attempt after the client left")
	}
	close(release)
	allIdle(t, e, "a-slow", "b-good")
	time.Sleep(100 * time.Millisecond)
	if good.hits.Load() != 0 {
		t.Fatal("no attempt may start for a client that left")
	}
	eventually(t, 5*time.Second, func() bool {
		for _, l := range e.attemptLines(t) {
			if l["outcome"] == "client_closed" {
				return true
			}
		}
		return false
	}, "the attempt is recorded as client_closed")
}

func TestAttemptMetricsUseBoundedLabels(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{}`))
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	e.post(t, false)
	e.chat(t, "attacker-chosen-model-name")
	metrics := scrape(t, e.gw)
	for _, want := range []string{
		`inference_attempts_total{model="qwen-7b",outcome="retried"} 1`,
		`inference_attempts_total{model="qwen-7b",outcome="ok"} 1`,
		`inference_retries_total{model="qwen-7b",reason="status_503"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("missing %q in metrics", want)
		}
	}
	if strings.Contains(metrics, "attacker-chosen-model-name") {
		t.Fatal("client-chosen model names must never become labels")
	}
}

func TestStaticModeStillMakesOneTryAndSendsNoAttemptHeader(t *testing.T) {
	var gotAttempt atomic.Value
	var hits atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotAttempt.Store(r.Header.Get("X-Attempt-ID"))
		w.WriteHeader(503)
	}))
	t.Cleanup(up.Close)
	env := newEnvForURL(t, up.URL, up)
	resp := env.post(t, validBody)
	if resp.StatusCode != 503 || hits.Load() != 1 {
		t.Fatalf("static mode never retries: %d hits=%d", resp.StatusCode, hits.Load())
	}
	if v, _ := gotAttempt.Load().(string); v != "" {
		t.Fatalf("static mode is unchanged on the wire: X-Attempt-ID=%q", v)
	}
	if resp.Header.Get("X-ServerFlow-Attempts") != "" {
		t.Fatal("no attempts header in static mode")
	}
}

func TestTheFirstChunkIsDeliveredEvenWhenTheWorkerSendsOneByteAtATime(t *testing.T) {
	trickle := newScriptWorker(t, "a-trickle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for _, b := range []byte("data: hello\n\ndata: [DONE]\n\n") {
			_, _ = w.Write([]byte{b})
			w.(http.Flusher).Flush()
			time.Sleep(2 * time.Millisecond)
		}
	})
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{trickle.snapshot("qwen-7b")})
	resp, body := e.post(t, true)
	if resp.StatusCode != 200 || body != "data: hello\n\ndata: [DONE]\n\n" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
}

func TestHeadersAreHeldUntilTheFirstByteOfAStream(t *testing.T) {
	gate := make(chan struct{})
	slow := newScriptWorker(t, "a-slow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush() // headers go out at once, the first byte only when the test allows
		<-gate
		_, _ = io.WriteString(w, "data: first\n\n")
	})
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{slow.snapshot("qwen-7b")})
	req, _ := http.NewRequest(http.MethodPost, e.url+chatCompletionsPath,
		strings.NewReader(`{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	got := make(chan *http.Response, 1)
	go func() {
		if resp, err := e.client.Do(req); err == nil {
			got <- resp
		}
	}()
	select {
	case <-got:
		t.Fatal("the client got response headers before the worker produced a single byte")
	case <-time.After(300 * time.Millisecond):
	}
	close(gate)
	select {
	case resp := <-got:
		line, _ := bufio.NewReader(resp.Body).ReadString('\n')
		_ = resp.Body.Close()
		if resp.StatusCode != 200 || line != "data: first\n" {
			t.Fatalf("%d %q", resp.StatusCode, line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no response after the first byte")
	}
}

func TestARetryNeverLeavesASlotBehindUnderConcurrency(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{}`))
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	sb, sg := bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")
	sb.MaxConcurrency, sg.MaxConcurrency = 100, 100 // room for the whole burst
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sb, sg})
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, _ := e.post(t, false); resp.StatusCode != 200 {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d requests failed although a healthy worker existed", failures.Load())
	}
	allIdle(t, e, "a-bad", "b-good")
	if n := len(e.gw.router.inflight); n != 0 {
		t.Fatalf("idle counters must be removed, %d left", n)
	}
}

func TestA504IsRetriedOnlyWhenConfigured(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(504, `{"error":{"message":"backend timed out"}}`))
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")}, func(c *config.GatewayConfig) {
		c.RetryStatuses = []int{502, 503, 504}
	})
	if resp, body := e.post(t, false); resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" {
		t.Fatalf("a listed 504 is retried: %d %s", resp.StatusCode, body)
	}
}

func TestAJSONResponseThatDiesBeforeItsFirstBodyByteIsRetried(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "50")
		w.WriteHeader(200)
		w.(http.Flusher).Flush() // headers out, then the worker dies without a single body byte
		panic(http.ErrAbortHandler)
	})
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" {
		t.Fatalf("no byte reached the client, so this is retryable: %d %q", resp.StatusCode, body)
	}
	if got := e.attemptLines(t)[0]["class"]; got != "empty_stream" {
		t.Fatalf("class %v", got)
	}
	allIdle(t, e, "a-bad", "b-good")
}

func TestAnEmptyJSON200WithNoAlternativeIsAGatewayErrorNotADroppedConnection(t *testing.T) {
	bad := newScriptWorker(t, "only", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "50")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != http.StatusBadGateway || errorCode(t, body) != "INFERENCE_FAILED" {
		t.Fatalf("nothing was sent, so the client gets a clean 502: %d %s", resp.StatusCode, body)
	}
	allIdle(t, e, "only")
}

func TestALargeJSONResponseIsRelayedWholeEvenThoughItsFirstChunkIsHeld(t *testing.T) {
	big := strings.Repeat("x", 3*copyBufferSize+123)
	w1 := newScriptWorker(t, "a-big", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"pad":"`+big+`"}`)
	})
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{w1.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 200 || body != `{"pad":"`+big+`"}` {
		t.Fatalf("%d, %d bytes", resp.StatusCode, len(body))
	}
}

func TestAnInformationalStatusFromAWorkerIsAGatewayError(t *testing.T) {
	for _, code := range []int{101} {
		bad := newScriptWorker(t, "only", func(w http.ResponseWriter, r *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				return
			}
			conn, buf, _ := hj.Hijack()
			_, _ = buf.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\nConnection: Upgrade\r\n\r\nraw")
			_ = buf.Flush()
			time.Sleep(50 * time.Millisecond)
			_ = conn.Close()
		})
		e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b")})
		resp, body := e.post(t, false)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("%d: a worker must not be able to answer %d: got %d %s", code, code, resp.StatusCode, body)
		}
		allIdle(t, e, "only")
	}
}

func TestAClientWhoLeavesDuringTheFirstByteWaitStopsEverything(t *testing.T) {
	gate := make(chan struct{})
	slow := newScriptWorker(t, "a-slow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case <-gate:
		case <-r.Context().Done():
		}
	})
	good := newScriptWorker(t, "b-good", sseEvents("b-good", 1))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{slow.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath,
		strings.NewReader(`{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	errc := make(chan error, 1)
	go func() { _, err := e.client.Do(req); errc <- err }()
	eventually(t, 5*time.Second, func() bool { return slow.hits.Load() == 1 }, "the first attempt is waiting for its first byte")
	cancel()
	<-errc
	allIdle(t, e, "a-slow", "b-good")
	close(gate)
	time.Sleep(100 * time.Millisecond)
	if good.hits.Load() != 0 {
		t.Fatal("a client that left during the hold must not trigger a second attempt")
	}
}

// peakWorker counts how many requests it is serving at once.
func peakWorker(t *testing.T, id string, h func(http.ResponseWriter, *http.Request)) (*scriptWorker, *atomic.Int64) {
	var cur, peak atomic.Int64
	sw := newScriptWorker(t, id, func(w http.ResponseWriter, r *http.Request) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer cur.Add(-1)
		time.Sleep(20 * time.Millisecond)
		h(w, r)
	})
	return sw, &peak
}

func TestNoWorkerEverSeesMoreThanItsMaxConcurrencyFromOneGatewayEvenWithRetries(t *testing.T) {
	bad, badPeak := peakWorker(t, "a-bad", status(503, `{}`))
	g1, p1 := peakWorker(t, "b-good", okJSON("b-good"))
	g2, p2 := peakWorker(t, "c-good", okJSON("c-good"))
	sb, s1, s2 := bad.snapshot("qwen-7b"), g1.snapshot("qwen-7b"), g2.snapshot("qwen-7b")
	sb.MaxConcurrency, s1.MaxConcurrency, s2.MaxConcurrency = 3, 3, 3
	e := newRegEnv(t, "least-active", []protocol.WorkerSnapshot{sb, s1, s2})
	var wg sync.WaitGroup
	var ok, refused atomic.Int64
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _ := e.post(t, false)
			switch resp.StatusCode {
			case 200:
				ok.Add(1)
			case 503:
				refused.Add(1)
			}
		}()
	}
	wg.Wait()
	for name, p := range map[string]*atomic.Int64{"bad": badPeak, "good1": p1, "good2": p2} {
		if p.Load() > 3 {
			t.Fatalf("%s served %d requests at once; its limit is 3", name, p.Load())
		}
	}
	if ok.Load()+refused.Load() != 60 || ok.Load() == 0 {
		t.Fatalf("every request is answered or turned away: ok %d refused %d", ok.Load(), refused.Load())
	}
	allIdle(t, e, "a-bad", "b-good", "c-good")
}

func TestRetriesCancellationsAndFailuresLeakNoGoroutines(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{}`))
	drop := newScriptWorker(t, "b-drop", dropConnection)
	empty := newScriptWorker(t, "c-empty", emptyStream)
	good := newScriptWorker(t, "d-good", sseEvents("d-good", 2))
	sn := []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), drop.snapshot("qwen-7b"), empty.snapshot("qwen-7b"), good.snapshot("qwen-7b")}
	for i := range sn {
		sn[i].MaxConcurrency = 100
	}
	e := newRegEnv(t, "round-robin", sn)
	// Warm up so lazily started goroutines (connection pools, timers) exist before the baseline.
	for i := 0; i < 8; i++ {
		e.post(t, i%2 == 0)
	}
	e.client.CloseIdleConnections()
	e.gw.router.client.CloseIdleConnections() // pooled keep-alive connections are not leaks
	time.Sleep(300 * time.Millisecond)
	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%5)*10*time.Millisecond+time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath,
				strings.NewReader(`{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := e.client.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()
	allIdle(t, e, "a-bad", "b-drop", "c-empty", "d-good")
	settled := func() bool {
		e.client.CloseIdleConnections()
		e.gw.router.client.CloseIdleConnections()
		return runtime.NumGoroutine() <= before+8
	}
	deadline := time.Now().Add(10 * time.Second)
	for !settled() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+8 {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutines grew from %d to %d:\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}

// lowestFirst always offers the lowest worker ID, so a retry can only avoid a worker by exclusion.
type lowestFirst struct{}

func (lowestFirst) SelectWorker(_ context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	var best *protocol.WorkerSnapshot
	for i := range ws {
		if ws[i].Model == req.Model && ws[i].Eligible && (best == nil || ws[i].WorkerID < best.WorkerID) {
			c := ws[i]
			best = &c
		}
	}
	if best == nil {
		return nil, &scheduler.NoWorkerError{Model: req.Model, Serving: 1}
	}
	return best, nil
}

func TestAWorkerIsNeverTriedTwiceForOneRequestAcrossManyAttempts(t *testing.T) {
	var bad []*scriptWorker
	var snaps []protocol.WorkerSnapshot
	for _, id := range []string{"a", "b", "c"} {
		w := newScriptWorker(t, id, status(503, `{}`))
		bad = append(bad, w)
		snaps = append(snaps, w.snapshot("qwen-7b"))
	}
	good := newScriptWorker(t, "d", okJSON("d"))
	snaps = append(snaps, good.snapshot("qwen-7b"))
	e := newRegEnv(t, "round-robin", snaps, func(c *config.GatewayConfig) { c.MaxAttempts = 4 })
	e.gw.router.sched = lowestFirst{} // would pick "a" forever if exclusion forgot it
	resp, body := e.post(t, false)
	if resp.StatusCode != 200 || whoAnswered(t, body) != "d" || resp.Header.Get("X-ServerFlow-Attempts") != "4" {
		t.Fatalf("%d %s %v", resp.StatusCode, body, resp.Header)
	}
	for _, w := range bad {
		if w.hits.Load() != 1 {
			t.Fatalf("%s was tried %d times; every attempt must go to a worker not yet tried", w.id, w.hits.Load())
		}
	}
}

func TestAGatewayErrorAfterSeveralAttemptsStillSaysHowManyWereUsed(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{}`))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), refusing(t, "b-gone")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("the second attempt could not connect: %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("X-ServerFlow-Attempts") != "2" {
		t.Fatalf("an error produced by the gateway after two attempts must say so: %v", resp.Header)
	}
}

func TestAnEmptyClientErrorBodyIsRelayedAsIs(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400) })
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	resp, body := e.post(t, false)
	if resp.StatusCode != 400 || body != "" || good.hits.Load() != 0 {
		t.Fatalf("only a 200 is held for its first byte; a 400 is final: %d %q (good hits %d)", resp.StatusCode, body, good.hits.Load())
	}
}

func TestStatusesOutsideTheDefinedClassesAreGatewayErrors(t *testing.T) {
	for _, code := range []int{600, 799, 999} {
		w := newScriptWorker(t, "only", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
		e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{w.snapshot("qwen-7b")})
		resp, _ := e.post(t, false)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("%d must not be relayed: got %d", code, resp.StatusCode)
		}
		metrics := scrape(t, e.gw)
		if strings.Contains(metrics, `status="`+strconv.Itoa(code)+`"`) {
			t.Fatalf("a worker-chosen status must not become a label: %d", code)
		}
		allIdle(t, e, "only")
	}
}

func TestReleaseIsIdempotentAndOnlyReleasesItsOwnSlot(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	r1, _ := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"})
	r2, _ := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"})
	if e.gw.router.InFlight("a") != 2 {
		t.Fatalf("two slots taken: %d", e.gw.router.InFlight("a"))
	}
	r1.release()
	r1.release() // a second release of the same slot must not free r2's
	if got := e.gw.router.InFlight("a"); got != 1 {
		t.Fatalf("a double release freed someone else's slot: %d", got)
	}
	r2.release()
	if got := e.gw.router.InFlight("a"); got != 0 {
		t.Fatalf("%d", got)
	}
}

func TestAStatus599IsRelayedAndAnEmpty2xxIsNotHeld(t *testing.T) {
	for _, code := range []int{599, 201, 204} {
		bad := newScriptWorker(t, "a-first", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
		good := newScriptWorker(t, "b-good", okJSON("b-good"))
		e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
		resp, _ := e.post(t, false)
		if resp.StatusCode != code || good.hits.Load() != 0 {
			t.Fatalf("%d is a final answer, relayed as is: got %d (good hits %d)", code, resp.StatusCode, good.hits.Load())
		}
	}
}

func TestARelayedClientErrorCountsAsAnOKAttempt(t *testing.T) {
	w := newScriptWorker(t, "only", status(429, `{"error":{"message":"slow down"}}`))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{w.snapshot("qwen-7b")})
	if resp, _ := e.post(t, false); resp.StatusCode != 429 {
		t.Fatal(resp.StatusCode)
	}
	metrics := scrape(t, e.gw)
	if !strings.Contains(metrics, `inference_attempts_total{model="qwen-7b",outcome="ok"} 1`) {
		t.Fatalf("attempt outcomes describe the gateway's work (ADR-012):\n%s", metrics)
	}
	if strings.Contains(metrics, `outcome="failed"`) {
		t.Fatal("a relayed 4xx is not a failed attempt")
	}
}

func TestEarlyFailureLogsNameTheAttemptTheyBelongTo(t *testing.T) {
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "gone")})
	e.post(t, false)
	var failed, finished string
	for _, l := range e.logs.logLines(t) {
		switch l["msg"] {
		case "worker request failed":
			failed, _ = l["attempt_id"].(string)
		case "attempt finished":
			finished, _ = l["attempt_id"].(string)
		}
	}
	if !strings.HasPrefix(failed, "att_") || failed != finished {
		t.Fatalf("the failure log must carry the attempt's own ID: %q vs %q", failed, finished)
	}
}
