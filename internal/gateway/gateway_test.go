package gateway

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
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/config"
)

const validBody = `{"model":"qwen-7b","messages":[{"role":"user","content":"hello"}],"top_p":0.9}`

var requestIDRe = regexp.MustCompile(`^req_[0-9a-f]{16}$`)

// lockedBuffer is a bytes.Buffer safe for concurrent log writes and reads.
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

// logLines returns the parsed JSON log lines.
func (b *lockedBuffer) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

type testEnv struct {
	gw     *Server
	url    string
	logs   *lockedBuffer
	client *http.Client
	fake   *httptest.Server
}

func testConfig(upstreamURL string) config.GatewayConfig {
	cfg := config.Default().Gateway
	cfg.UpstreamURL = upstreamURL
	cfg.Models = []string{"qwen-7b"}
	cfg.UpstreamHeaderTimeout = 2 * time.Second
	cfg.ShutdownTimeout = 5 * time.Second
	return cfg
}

// newEnv starts a fake upstream running h and a gateway in front of it.
func newEnv(t *testing.T, h http.Handler, mutate ...func(*config.GatewayConfig)) *testEnv {
	t.Helper()
	fake := httptest.NewServer(h)
	t.Cleanup(fake.Close)
	return newEnvForURL(t, fake.URL, fake, mutate...)
}

func newEnvForURL(t *testing.T, upstreamURL string, fake *httptest.Server, mutate ...func(*config.GatewayConfig)) *testEnv {
	t.Helper()
	cfg := testConfig(upstreamURL)
	for _, m := range mutate {
		m(&cfg)
	}
	logs := &lockedBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	gw := New(cfg, log)
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	return &testEnv{gw: gw, url: srv.URL, logs: logs, client: &http.Client{Transport: tr}, fake: fake}
}

func (e *testEnv) post(t *testing.T, body string, hdr ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.url+chatCompletionsPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("POST error: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (e *testEnv) get(t *testing.T, path string) (*http.Response, string) {
	t.Helper()
	resp, err := e.client.Get(e.url + path)
	if err != nil {
		t.Fatalf("GET %s error: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func errorCode(t *testing.T, body string) string {
	t.Helper()
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("not an error body: %q", body)
	}
	return e.Error.Code
}

// readLine reads one line from br, failing the test if it takes longer than d.
func readLine(t *testing.T, br *bufio.Reader, d time.Duration) string {
	t.Helper()
	type result struct {
		s   string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := br.ReadString('\n')
		ch <- result{s, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil && r.s == "" {
			t.Fatalf("read line: %v", r.err)
		}
		return r.s
	case <-time.After(d):
		t.Fatalf("timed out after %v waiting for a line", d)
		return ""
	}
}

func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", d, msg)
}

func jsonOK(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
}

// --- health, readiness, models -------------------------------------------

func TestHealthzIgnoresUpstream(t *testing.T) {
	fake := httptest.NewServer(jsonOK(`{}`))
	fake.Close() // upstream is down
	env := newEnvForURL(t, fake.URL, fake)
	resp, body := env.get(t, "/healthz")
	if resp.StatusCode != 200 || !strings.Contains(body, "ok") {
		t.Fatalf("healthz: %d %q", resp.StatusCode, body)
	}
}

func TestReadyz(t *testing.T) {
	var status atomic.Int32
	status.Store(200)
	var probes atomic.Int32
	var gotPath atomic.Value
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		gotPath.Store(r.URL.Path)
		w.WriteHeader(int(status.Load()))
	}), func(c *config.GatewayConfig) { c.ReadinessPath = "/health" })

	for i := 0; i < 3; i++ {
		if resp, _ := env.get(t, "/readyz"); resp.StatusCode != 200 {
			t.Fatalf("readyz %d: status %d", i, resp.StatusCode)
		}
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("expected the probe result to be cached (1 probe), got %d", n)
	}
	if p := gotPath.Load(); p != "/health" {
		t.Fatalf("probe used path %v, want /health", p)
	}

	// Expire the cache and make the upstream unhealthy.
	env.gw.ready.mu.Lock()
	env.gw.ready.checked = time.Time{}
	env.gw.ready.mu.Unlock()
	status.Store(500)
	resp, body := env.get(t, "/readyz")
	if resp.StatusCode != 503 || errorCode(t, body) != api.CodeWorkerUnavailable {
		t.Fatalf("expected 503 WORKER_UNAVAILABLE, got %d %q", resp.StatusCode, body)
	}
}

func TestReadyzUpstreamDown(t *testing.T) {
	fake := httptest.NewServer(jsonOK(`{}`))
	fake.Close()
	env := newEnvForURL(t, fake.URL, fake)
	if resp, _ := env.get(t, "/readyz"); resp.StatusCode != 503 {
		t.Fatalf("expected 503 with upstream down, got %d", resp.StatusCode)
	}
}

func TestModels(t *testing.T) {
	env := newEnv(t, jsonOK(`{}`))
	resp, body := env.get(t, "/v1/models")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var list struct {
		Object string `json:"object"`
		Data   []struct{ ID string }
	}
	if err := json.Unmarshal([]byte(body), &list); err != nil || list.Object != "list" || len(list.Data) != 1 || list.Data[0].ID != "qwen-7b" {
		t.Fatalf("unexpected models response %q (%v)", body, err)
	}
}

// --- non-streaming proxy ---------------------------------------------------

func TestChatNonStream(t *testing.T) {
	const upstreamBody = `{"id":"chatcmpl-1","object":"chat.completion","choices":[]}`
	var gotBody []byte
	var gotHdr http.Header
	var gotPath string
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotHdr = r.Header.Clone()
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, upstreamBody)
	}))

	resp := env.post(t, validBody, "Authorization", "Bearer secret-key", "Cookie", "a=b", "X-Request-ID", "client-chosen")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := readAll(t, resp.Body); got != upstreamBody {
		t.Fatalf("response body changed: %q", got)
	}
	id := resp.Header.Get("X-Request-ID")
	if !requestIDRe.MatchString(id) {
		t.Fatalf("response X-Request-ID %q is not gateway-generated", id)
	}
	if gotPath != chatCompletionsPath {
		t.Fatalf("upstream path %q", gotPath)
	}
	if string(gotBody) != validBody {
		t.Fatalf("upstream must receive the original body unchanged, got %q", gotBody)
	}
	if gotHdr.Get("X-Request-ID") != id {
		t.Fatalf("upstream X-Request-ID %q, want %q", gotHdr.Get("X-Request-ID"), id)
	}
	for _, h := range []string{"Authorization", "Cookie"} {
		if gotHdr.Get(h) != "" {
			t.Fatalf("%s leaked to the upstream", h)
		}
	}
}

func TestRequestIDsAreUniquePerRequest(t *testing.T) {
	env := newEnv(t, jsonOK(`{}`))
	a := env.post(t, validBody).Header.Get("X-Request-ID")
	b := env.post(t, validBody).Header.Get("X-Request-ID")
	if a == b {
		t.Fatalf("request IDs must differ, both %q", a)
	}
}

func TestUpstreamErrorStatusPassedThrough(t *testing.T) {
	const body = `{"error":{"message":"slow down","type":"rate_limit","code":"x"}}`
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, body)
	}))
	resp := env.post(t, validBody)
	if resp.StatusCode != 429 || readAll(t, resp.Body) != body {
		t.Fatalf("upstream 429 must pass through unchanged, got %d", resp.StatusCode)
	}
}

func TestInvalidRequestsNeverReachUpstream(t *testing.T) {
	var calls atomic.Int32
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }),
		func(c *config.GatewayConfig) { c.MaxRequestBytes = 200; c.MaxTokensLimit = 100 })

	tests := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"invalid json", `{nope`, 400, api.CodeInvalidRequest},
		{"missing messages", `{"model":"qwen-7b"}`, 400, api.CodeInvalidRequest},
		{"unknown model", `{"model":"nope","messages":[{"role":"user","content":"x"}]}`, 404, api.CodeModelNotFound},
		{"max_tokens over limit", `{"model":"qwen-7b","messages":[{"role":"user","content":"x"}],"max_tokens":101}`, 400, api.CodeInvalidRequest},
		{"body too large", `{"model":"qwen-7b","messages":[{"role":"user","content":"` + strings.Repeat("a", 300) + `"}]}`, 413, api.CodeInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := env.post(t, tt.body)
			body := readAll(t, resp.Body)
			if resp.StatusCode != tt.status || errorCode(t, body) != tt.code {
				t.Fatalf("got %d %q, want %d %s", resp.StatusCode, body, tt.status, tt.code)
			}
			if !requestIDRe.MatchString(resp.Header.Get("X-Request-ID")) {
				t.Fatal("error responses must carry X-Request-ID")
			}
		})
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("invalid requests reached the upstream %d times", n)
	}
}

// --- streaming ---------------------------------------------------------------

func sseHandler(release <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = fmt.Fprint(w, "data: {\"n\":2}\n\ndata: [DONE]\n\n")
	})
}

func TestStreamIsRelayedWithoutBuffering(t *testing.T) {
	release := make(chan struct{})
	env := newEnv(t, sseHandler(release))

	resp := env.post(t, strings.Replace(validBody, `"top_p"`, `"stream":true,"top_p"`, 1))
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("SSE responses must be Cache-Control: no-cache, got %q", cc)
	}
	if ab := resp.Header.Get("X-Accel-Buffering"); ab != "no" {
		t.Fatalf("SSE responses must disable proxy buffering, got X-Accel-Buffering %q", ab)
	}
	br := bufio.NewReader(resp.Body)

	// The first chunk must arrive while the upstream is still blocked.
	if line := readLine(t, br, 2*time.Second); line != "data: {\"n\":1}\n" {
		t.Fatalf("unexpected first line %q", line)
	}
	readLine(t, br, time.Second) // blank separator

	close(release)
	rest := readAll(t, br)
	if rest != "data: {\"n\":2}\n\ndata: [DONE]\n\n" {
		t.Fatalf("unexpected remainder %q", rest)
	}

	eventually(t, 2*time.Second, func() bool {
		for _, l := range env.logs.logLines(t) {
			if l["path"] == chatCompletionsPath && l["stream"] == true && l["ttft_ms"] != nil {
				return true
			}
		}
		return false
	}, "stream request log line with ttft_ms")
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	sawCancel := make(chan struct{})
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(sawCancel)
		case <-time.After(10 * time.Second):
		}
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(validBody))
	resp, err := env.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(resp.Body)
	readLine(t, br, 2*time.Second)
	cancel()
	_ = resp.Body.Close()

	select {
	case <-sawCancel:
	case <-time.After(time.Second):
		t.Fatal("upstream request was not cancelled within 1s of the client disconnecting")
	}

	eventually(t, 2*time.Second, func() bool {
		for _, l := range env.logs.logLines(t) {
			if l["path"] == chatCompletionsPath && l["status"] == float64(statusClientClosed) {
				return true
			}
		}
		return false
	}, "request logged as 499")
}

func TestUpstreamFailureMidStreamEndsWithErrorEvent(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // drop the connection mid-stream
	}))
	resp := env.post(t, validBody)
	got := readAll(t, resp.Body)
	if !strings.HasPrefix(got, "data: {\"n\":1}\n\n") {
		t.Fatalf("first chunk missing: %q", got)
	}
	if !strings.Contains(got, `"code":"`+api.CodeInferenceFailed+`"`) || !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("expected a terminating SSE error event, got %q", got)
	}
	eventually(t, 2*time.Second, func() bool {
		for _, l := range env.logs.logLines(t) {
			if l["error_code"] == api.CodeInferenceFailed {
				return true
			}
		}
		return false
	}, "error_code logged")
}

func TestTruncatedNonStreamBodyAbortsConnection(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		_, _ = io.WriteString(w, `{"partial":`)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	// The failure may surface on the request itself (connection dropped before
	// headers were flushed) or while reading the body; either is correct. What
	// must never happen is a clean, complete-looking response.
	req, _ := http.NewRequest(http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(validBody))
	resp, err := env.client.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("client must see an error for a truncated response, not a clean end")
	}
}

// --- upstream failures -------------------------------------------------------

func TestUpstreamDown(t *testing.T) {
	fake := httptest.NewServer(jsonOK(`{}`))
	fake.Close()
	env := newEnvForURL(t, fake.URL, fake)
	resp := env.post(t, validBody)
	body := readAll(t, resp.Body)
	if resp.StatusCode != 503 || errorCode(t, body) != api.CodeWorkerUnavailable {
		t.Fatalf("got %d %q", resp.StatusCode, body)
	}
}

func TestUpstreamHeaderTimeout(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}), func(c *config.GatewayConfig) { c.UpstreamHeaderTimeout = 100 * time.Millisecond })
	resp := env.post(t, validBody)
	body := readAll(t, resp.Body)
	if resp.StatusCode != 504 || errorCode(t, body) != api.CodeUpstreamTimeout {
		t.Fatalf("got %d %q", resp.StatusCode, body)
	}
}

func TestMapUpstreamError(t *testing.T) {
	if got := mapUpstreamError(errors.New("boom")); got.Code != api.CodeInferenceFailed || got.HTTPStatus != 502 {
		t.Fatalf("generic error mapped to %+v", got)
	}
	if got := mapUpstreamError(&net.OpError{Op: "dial", Err: errors.New("no route")}); got.Code != api.CodeWorkerUnavailable {
		t.Fatalf("dial error mapped to %+v", got)
	}
	if got := mapUpstreamError(&net.DNSError{Err: "no such host", Name: "x"}); got.Code != api.CodeWorkerUnavailable {
		t.Fatalf("dns error mapped to %+v", got)
	}
}

type panicUpstream struct{}

func (panicUpstream) Do(context.Context, string, []byte, string) (*http.Response, error) {
	panic("boom")
}
func (panicUpstream) Probe(context.Context) error { return nil }

func TestHandlerPanicBecomes500(t *testing.T) {
	logs := &lockedBuffer{}
	gw := newWithUpstream(testConfig("http://unused"), slog.New(slog.NewJSONHandler(logs, nil)), panicUpstream{})
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+chatCompletionsPath, "application/json", strings.NewReader(validBody))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := readAll(t, resp.Body)
	if resp.StatusCode != 500 || errorCode(t, body) != api.CodeInternalError {
		t.Fatalf("got %d %q", resp.StatusCode, body)
	}
	// The server must keep serving afterwards.
	if r2, err := http.Get(srv.URL + "/healthz"); err != nil || r2.StatusCode != 200 {
		t.Fatalf("server unhealthy after a panic: %v", err)
	}
}

// --- observability -----------------------------------------------------------

func TestLogsCarryRequestContextAndNoPrompt(t *testing.T) {
	env := newEnv(t, jsonOK(`{"ok":true}`))
	resp := env.post(t, validBody)
	id := resp.Header.Get("X-Request-ID")
	readAll(t, resp.Body)

	eventually(t, 2*time.Second, func() bool { return strings.Contains(env.logs.String(), id) }, "log line for request")
	var line map[string]any
	for _, l := range env.logs.logLines(t) {
		if l["request_id"] == id {
			line = l
		}
	}
	for _, k := range []string{"request_id", "attempt_id", "model", "status", "duration_ms", "path", "method"} {
		if line[k] == nil {
			t.Fatalf("log line missing %q: %v", k, line)
		}
	}
	if line["model"] != "qwen-7b" || line["status"] != float64(200) || line["stream"] != false {
		t.Fatalf("unexpected log line %v", line)
	}
	if strings.Contains(env.logs.String(), "hello") {
		t.Fatal("prompt content must not be logged")
	}
}

func TestMetrics(t *testing.T) {
	release := make(chan struct{})
	close(release)
	env := newEnv(t, sseHandler(release))
	readAll(t, env.post(t, validBody).Body)                                                     // stream request
	readAll(t, env.post(t, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`).Body) // 404

	eventually(t, 2*time.Second, func() bool {
		_, body := env.get(t, "/metrics")
		return strings.Contains(body, `inference_requests_total{model="qwen-7b",status="200"} 1`)
	}, "request counter")
	_, body := env.get(t, "/metrics")
	for _, want := range []string{
		`inference_requests_total{model="unknown",status="404"} 1`,
		`inference_requests_active 0`,
		`inference_request_duration_seconds_count{model="qwen-7b"} 1`,
		`inference_ttft_seconds_count{model="qwen-7b"} 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n%s", want, body)
		}
	}
	if strings.Contains(body, `model="nope"`) {
		t.Fatal("unvalidated model names must not become metric labels")
	}
}

// --- lifecycle ---------------------------------------------------------------

func TestGracefulShutdownLetsStreamFinish(t *testing.T) {
	release := make(chan struct{})
	fake := httptest.NewServer(sseHandler(release))
	t.Cleanup(fake.Close)
	gw := New(testConfig(fake.URL), slog.New(slog.NewJSONHandler(io.Discard, nil)))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()

	resp, err := http.Post("http://"+ln.Addr().String()+chatCompletionsPath, "application/json", strings.NewReader(validBody))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)
	readLine(t, br, 2*time.Second) // stream is in flight

	stop() // SIGTERM equivalent
	select {
	case err := <-done:
		t.Fatalf("Serve returned (%v) while a stream was still in flight", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	if rest := readAll(t, br); !strings.Contains(rest, "[DONE]") {
		t.Fatalf("in-flight stream was cut off: %q", rest)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve should return nil after a clean shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after the stream finished")
	}
}

func TestShutdownTimeoutForcesClose(t *testing.T) {
	fake := httptest.NewServer(sseHandler(make(chan struct{}))) // never released
	t.Cleanup(fake.Close)
	cfg := testConfig(fake.URL)
	cfg.ShutdownTimeout = 200 * time.Millisecond
	gw := New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()

	resp, err := http.Post("http://"+ln.Addr().String()+chatCompletionsPath, "application/json", strings.NewReader(validBody))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	readLine(t, bufio.NewReader(resp.Body), 2*time.Second)

	stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a shutdown timeout error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after the shutdown timeout")
	}
}

func TestNoGoroutineLeakAfterManyCancelledStreams(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, "data: {}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	// Warm up pools so the baseline includes steady-state goroutines.
	readAll(t, env.post(t, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`).Body)
	settle := func() int {
		env.client.CloseIdleConnections()
		env.gw.upstream.(*httpUpstream).client.CloseIdleConnections()
		time.Sleep(200 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	base := settle()

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(validBody))
		resp, err := env.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		readLine(t, bufio.NewReader(resp.Body), 2*time.Second)
		cancel()
		_ = resp.Body.Close()
	}
	eventually(t, 5*time.Second, func() bool { return settle() <= base+5 },
		fmt.Sprintf("goroutines to return near the baseline of %d", base))
}

// --- review findings -----------------------------------------------------------

// ctxProbeUpstream fails its probe if the probe context is already cancelled.
type ctxProbeUpstream struct{ panicUpstream }

func (ctxProbeUpstream) Probe(ctx context.Context) error { return ctx.Err() }

func TestReadinessProbeSurvivesCallerCancellation(t *testing.T) {
	r := &readiness{upstream: ctxProbeUpstream{}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	// One client giving up must not poison the cached result for everyone.
	if err := r.check(cancelled); err != nil {
		t.Fatalf("probe failed because the first caller was cancelled: %v", err)
	}
	if err := r.check(context.Background()); err != nil {
		t.Fatalf("cached result is poisoned: %v", err)
	}
}

func TestSlowRequestBodyIsCutOff(t *testing.T) {
	env := newEnv(t, jsonOK(`{}`))
	env.gw.bodyReadTimeout = 200 * time.Millisecond

	conn, err := net.Dial("tcp", strings.TrimPrefix(env.url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// Promise 100 bytes, send 5, then stall.
	_, _ = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"mod", chatCompletionsPath)

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Logf("gateway logs: %s", env.logs.String())
		t.Fatalf("expected the gateway to answer once the body read times out, got %v", err)
	}
	if !strings.Contains(line, "400") {
		t.Fatalf("expected a 400 status line, got %q", line)
	}
}

func TestAbortedNonStreamResponseIsRecordedAsFailure(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		_, _ = io.WriteString(w, `{"partial":`)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	req, _ := http.NewRequest(http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(validBody))
	if resp, err := env.client.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	eventually(t, 2*time.Second, func() bool {
		for _, l := range env.logs.logLines(t) {
			if l["path"] == chatCompletionsPath {
				return l["status"] == float64(http.StatusBadGateway) && l["error_code"] == api.CodeInferenceFailed
			}
		}
		return false
	}, "aborted response logged as 502 INFERENCE_FAILED")
	_, body := env.get(t, "/metrics")
	if !strings.Contains(body, `inference_requests_total{model="qwen-7b",status="502"} 1`) {
		t.Fatalf("aborted response must be counted as 502 in metrics\n%s", body)
	}
}

// --- independent verify/review findings ------------------------------------------

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestDialTimeoutMapsToUnavailableNotTimeout(t *testing.T) {
	// An unreachable host (dial timeout) is "no upstream", not a slow upstream.
	got := mapUpstreamError(&net.OpError{Op: "dial", Err: timeoutErr{}})
	if got.Code != api.CodeWorkerUnavailable || got.HTTPStatus != 503 {
		t.Fatalf("dial timeout mapped to %+v", got)
	}
}

func TestUpstreamRedirectIsNotFollowed(t *testing.T) {
	var elsewhere atomic.Int32
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case chatCompletionsPath:
			http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
		case "/v1/models":
			http.Redirect(w, r, "/ok", http.StatusFound)
		default:
			elsewhere.Add(1)
			w.WriteHeader(200)
		}
	}))
	resp := env.post(t, validBody)
	body := readAll(t, resp.Body)
	if resp.StatusCode != 502 || errorCode(t, body) != api.CodeInferenceFailed {
		t.Fatalf("a redirecting upstream must yield 502 INFERENCE_FAILED, got %d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("Location") != "" {
		t.Fatal("upstream Location header leaked to the client")
	}
	if r, _ := env.get(t, "/readyz"); r.StatusCode != 503 {
		t.Fatalf("a redirecting readiness probe must not count as ready, got %d", r.StatusCode)
	}
	if n := elsewhere.Load(); n != 0 {
		t.Fatalf("the redirect target was contacted %d times", n)
	}
}

func TestMidStreamFailureKeepsSSEFraming(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `data: {"par`) // dies in the middle of an event
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	got := readAll(t, env.post(t, validBody).Body)
	if !strings.Contains(got, "data: {\"par\n\ndata: {\"error\"") {
		t.Fatalf("the error event must start on its own line, got %q", got)
	}
	if !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("stream must end on an event boundary, got %q", got)
	}
}

func TestStalledUpstreamStreamIsCutOff(t *testing.T) {
	sawCancel := make(chan struct{})
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		select { // stall forever
		case <-r.Context().Done():
			close(sawCancel)
		case <-time.After(10 * time.Second):
		}
	}), func(c *config.GatewayConfig) { c.UpstreamIdleTimeout = 200 * time.Millisecond })

	start := time.Now()
	got := readAll(t, env.post(t, validBody).Body)
	if time.Since(start) > 3*time.Second {
		t.Fatalf("stalled stream was not cut off promptly (%v)", time.Since(start))
	}
	if !strings.HasPrefix(got, "data: {\"n\":1}\n\n") || !strings.Contains(got, api.CodeInferenceFailed) {
		t.Fatalf("expected the first chunk then an error event, got %q", got)
	}
	select {
	case <-sawCancel:
	case <-time.After(time.Second):
		t.Fatal("the stalled upstream request was not cancelled")
	}
}

func TestStreamsOutliveTheBodyReadTimeout(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-time.After(700 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: {\"n\":2}\n\ndata: [DONE]\n\n")
	}))
	env.gw.bodyReadTimeout = 200 * time.Millisecond
	got := readAll(t, env.post(t, validBody).Body)
	if !strings.Contains(got, `{"n":2}`) || !strings.Contains(got, "[DONE]") {
		t.Fatalf("a request-body timeout must not cut a long response short, got %q", got)
	}
}

func TestClientThatStopsReadingIsCutOff(t *testing.T) {
	sawStop := make(chan struct{})
	chunk := strings.Repeat("x", 64<<10)
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		defer close(sawStop)
		for {
			if r.Context().Err() != nil {
				return
			}
			if _, err := io.WriteString(w, "data: "+chunk+"\n\n"); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}))
	env.gw.clientWriteTimeout = 200 * time.Millisecond

	resp := env.post(t, validBody) // headers arrive, then we never read the body
	_ = resp
	select {
	case <-sawStop:
	case <-time.After(10 * time.Second):
		t.Fatal("a client that stopped reading kept the upstream stream alive")
	}
}

func TestClientDropDuringNonStreamIsNotAnUpstreamFailure(t *testing.T) {
	big := strings.Repeat("a", 1<<20)
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for i := 0; i < 64; i++ {
			if _, err := io.WriteString(w, big); err != nil {
				return
			}
			if r.Context().Err() != nil {
				return
			}
		}
	}))
	const n = 10
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(validBody))
		resp, err := env.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadFull(resp.Body, make([]byte, 1024))
		cancel()
		_ = resp.Body.Close()
	}
	eventually(t, 5*time.Second, func() bool {
		c := 0
		for _, l := range env.logs.logLines(t) {
			if l["path"] == chatCompletionsPath {
				c++
			}
		}
		return c == n
	}, "all requests logged")
	for _, l := range env.logs.logLines(t) {
		if l["path"] == chatCompletionsPath && l["status"] != float64(statusClientClosed) {
			t.Fatalf("a client disconnect was recorded as %v (error_code %v), want 499", l["status"], l["error_code"])
		}
	}
}

func TestNoGoroutineLeakOnUpstreamFailures(t *testing.T) {
	var n atomic.Int32
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) % 3 {
		case 0: // dies mid-stream
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "data: {}\n\n")
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		case 1: // plain upstream error
			w.WriteHeader(500)
		default: // never answers (hits the header timeout)
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}
	}), func(c *config.GatewayConfig) { c.UpstreamHeaderTimeout = 100 * time.Millisecond })

	settle := func() int {
		env.client.CloseIdleConnections()
		env.gw.upstream.(*httpUpstream).client.CloseIdleConnections()
		time.Sleep(200 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	readAll(t, env.post(t, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`).Body)
	base := settle()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				req, _ := http.NewRequest(http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(validBody))
				if resp, err := env.client.Do(req); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			}
		}()
	}
	wg.Wait()
	eventually(t, 5*time.Second, func() bool { return settle() <= base+5 },
		fmt.Sprintf("goroutines to return near the baseline of %d after upstream failures", base))
}

// A slow-but-alive client must not be mistaken for a stalled upstream: the
// idle clock covers only time spent waiting on the upstream, not time spent
// blocked writing to the client.
func TestSlowClientIsNotMistakenForStalledUpstream(t *testing.T) {
	chunk := strings.Repeat("x", 64<<10)
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for i := 0; i < 100; i++ { // ~6 MB, far beyond the socket buffers
			if _, err := io.WriteString(w, "data: "+chunk+"\n\n"); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}), func(c *config.GatewayConfig) { c.UpstreamIdleTimeout = 300 * time.Millisecond })

	resp := env.post(t, validBody)
	br := bufio.NewReader(resp.Body)
	readLine(t, br, 2*time.Second)
	time.Sleep(900 * time.Millisecond) // the client stalls for 3x the idle timeout
	got := readAll(t, br)
	if strings.Contains(got, api.CodeInferenceFailed) {
		t.Fatal("a slow client was reported as an upstream failure")
	}
	if !strings.HasSuffix(got, "data: [DONE]\n\n") {
		t.Fatalf("stream did not complete (tail %q)", got[max(0, len(got)-60):])
	}
}
