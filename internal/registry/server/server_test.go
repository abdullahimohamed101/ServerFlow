package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/registry"
	"serverflow/pkg/protocol"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

const testToken = "a-sufficiently-long-shared-secret"

type env struct {
	srv   *Server
	reg   *registry.Registry
	clock *fakeClock
	ts    *httptest.Server
	logs  *lockedBuf
}

func newEnv(t *testing.T, token string, mutate ...func(*registry.Config)) *env {
	t.Helper()
	cfg := registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second,
		Retention: time.Minute, MaxWorkers: 50, HeartbeatInterval: 2 * time.Second}
	for _, m := range mutate {
		m(&cfg)
	}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	logs := &lockedBuf{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	reg, err := registry.NewWithClock(cfg, log, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(reg, Config{Token: token}, log)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &env{srv: srv, reg: reg, clock: clock, ts: ts, logs: logs}
}

func (e *env) do(t *testing.T, method, path, body string, hdr ...string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func errCode(t *testing.T, body string) string {
	t.Helper()
	var e struct{ Error struct{ Code string } }
	if err := json.Unmarshal([]byte(body), &e); err != nil || e.Error.Code == "" {
		t.Fatalf("not an error body: %q", body)
	}
	return e.Error.Code
}

const validRegistration = `{"worker_id":"w1","model":"qwen-7b","address":"http://127.0.0.1:9001","max_concurrency":4,"queue_size":8}`

func (e *env) register(t *testing.T, body string) string {
	t.Helper()
	code, resp, _ := e.do(t, "POST", "/v1/workers/register", body)
	if code != http.StatusCreated {
		t.Fatalf("register: %d %s", code, resp)
	}
	var out protocol.RegisterResponse
	if err := json.Unmarshal([]byte(resp), &out); err != nil || out.RegistrationID == "" {
		t.Fatalf("bad register response %q", resp)
	}
	return out.RegistrationID
}

func beat(reg, state string) string {
	return `{"registration_id":"` + reg + `","state":"` + state + `","metrics":{"active_requests":1,"queue_depth":2,"queued_input_tokens":3,"recent_tokens_per_second":4.5}}`
}

// --- registration -----------------------------------------------------------------------

func TestRegisterReturnsARegistrationAndTheHeartbeatInterval(t *testing.T) {
	e := newEnv(t, "")
	code, body, hdr := e.do(t, "POST", "/v1/workers/register", validRegistration)
	if code != 201 || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") {
		t.Fatalf("%d %s", code, body)
	}
	var out protocol.RegisterResponse
	_ = json.Unmarshal([]byte(body), &out)
	if !strings.HasPrefix(out.RegistrationID, "reg_") || out.HeartbeatIntervalSeconds != 2 {
		t.Fatalf("unexpected response %+v", out)
	}
}

func TestRegisterRejectsBadInput(t *testing.T) {
	e := newEnv(t, "")
	tests := []struct {
		name, body string
		status     int
		code       string
	}{
		{"not json", `{nope`, 400, "invalid_request"},
		{"empty body", ``, 400, "invalid_request"},
		{"unknown field", `{"worker_id":"w1","model":"m","address":"http://h:1","max_concurrency":1,"queue_size":0,"extra":1}`, 400, "invalid_request"},
		{"trailing data", validRegistration + ` {}`, 400, "invalid_request"},
		{"bad id", `{"worker_id":"a b","model":"m","address":"http://h:1","max_concurrency":1,"queue_size":0}`, 400, "invalid_request"},
		{"no model", `{"worker_id":"w1","address":"http://h:1","max_concurrency":1,"queue_size":0}`, 400, "invalid_request"},
		{"credentials in address", `{"worker_id":"w1","model":"m","address":"http://u:topsecret@h:1","max_concurrency":1,"queue_size":0}`, 400, "invalid_request"},
		{"zero concurrency", `{"worker_id":"w1","model":"m","address":"http://h:1","max_concurrency":0,"queue_size":0}`, 400, "invalid_request"},
		{"too large", `{"worker_id":"w1","model":"` + strings.Repeat("m", 100<<10) + `","address":"http://h:1","max_concurrency":1,"queue_size":0}`, 413, "payload_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body, _ := e.do(t, "POST", "/v1/workers/register", tt.body)
			if code != tt.status || errCode(t, body) != tt.code {
				t.Fatalf("got %d %s", code, body)
			}
			if strings.Contains(body, "topsecret") {
				t.Fatalf("the error echoes a credential: %s", body)
			}
		})
	}
	if e.reg.Len() != 0 {
		t.Fatal("a rejected registration must not be stored")
	}
}

func TestWorkerCapIsReportedAs503(t *testing.T) {
	e := newEnv(t, "", func(c *registry.Config) { c.MaxWorkers = 1 })
	e.register(t, validRegistration)
	code, body, _ := e.do(t, "POST", "/v1/workers/register", strings.Replace(validRegistration, `"w1"`, `"w2"`, 1))
	if code != 503 || errCode(t, body) != "registry_full" {
		t.Fatalf("%d %s", code, body)
	}
}

// --- heartbeat / deregister ----------------------------------------------------------------

func TestHeartbeatStatusCodes(t *testing.T) {
	e := newEnv(t, "")
	reg := e.register(t, validRegistration)

	if code, body, _ := e.do(t, "POST", "/v1/workers/w1/heartbeat", beat(reg, "READY")); code != 204 || body != "" {
		t.Fatalf("a valid heartbeat is 204 with no body, got %d %q", code, body)
	}
	snap, _ := e.reg.Get("w1")
	if snap.State != protocol.StateReady || snap.Metrics.QueueDepth != 2 || snap.Metrics.RecentTokensPerSecond != 4.5 {
		t.Fatalf("the heartbeat was not recorded: %+v", snap)
	}
	tests := []struct {
		name, path, body string
		status           int
		code             string
	}{
		{"unknown worker", "/v1/workers/nobody/heartbeat", beat(reg, "READY"), 404, "unknown_worker"},
		{"stale registration", "/v1/workers/w1/heartbeat", beat("reg_other", "READY"), 409, "stale_registration"},
		{"illegal transition", "/v1/workers/w1/heartbeat", beat(reg, "LOADING_MODEL"), 409, "illegal_transition"},
		{"unreportable state", "/v1/workers/w1/heartbeat", beat(reg, "LOST"), 400, "invalid_request"},
		{"bad metrics", "/v1/workers/w1/heartbeat", `{"registration_id":"` + reg + `","state":"READY","metrics":{"queue_depth":-1}}`, 400, "invalid_request"},
		{"bad worker id in the path", "/v1/workers/a%20b/heartbeat", beat(reg, "READY"), 400, "invalid_request"},
		{"unknown field", "/v1/workers/w1/heartbeat", `{"registration_id":"` + reg + `","state":"READY","timestamp":"2020-01-01T00:00:00Z"}`, 400, "invalid_request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body, _ := e.do(t, "POST", tt.path, tt.body)
			if code != tt.status || errCode(t, body) != tt.code {
				t.Fatalf("got %d %s", code, body)
			}
		})
	}
}

func TestHeartbeatWithAStaleRegistrationDoesNotRefreshTheWorker(t *testing.T) {
	e := newEnv(t, "")
	reg := e.register(t, validRegistration)
	e.do(t, "POST", "/v1/workers/w1/heartbeat", beat(reg, "READY"))
	e.clock.Advance(7 * time.Second)
	e.do(t, "POST", "/v1/workers/w1/heartbeat", beat("reg_impostor", "READY"))
	if s, _ := e.reg.Get("w1"); s.Health != protocol.HealthSuspect || s.Eligible {
		t.Fatalf("an impostor's heartbeat must not keep the real worker alive: %+v", s)
	}
}

func TestDeregister(t *testing.T) {
	e := newEnv(t, "")
	reg := e.register(t, validRegistration)
	if code, body, _ := e.do(t, "DELETE", "/v1/workers/w1", ""); code != 400 || errCode(t, body) != "invalid_request" {
		t.Fatalf("a missing registration header is a 400, got %d %s", code, body)
	}
	if code, body, _ := e.do(t, "DELETE", "/v1/workers/w1", "", "X-Registration-ID", "reg_wrong"); code != 409 || errCode(t, body) != "stale_registration" {
		t.Fatalf("got %d %s", code, body)
	}
	if code, _, _ := e.do(t, "DELETE", "/v1/workers/w1", "", "X-Registration-ID", reg); code != 204 {
		t.Fatalf("got %d", code)
	}
	if code, body, _ := e.do(t, "DELETE", "/v1/workers/w1", "", "X-Registration-ID", reg); code != 404 || errCode(t, body) != "unknown_worker" {
		t.Fatalf("got %d %s", code, body)
	}
}

// --- lookups ---------------------------------------------------------------------------------

func TestLookupEndpoints(t *testing.T) {
	e := newEnv(t, "")
	regs := map[string]string{}
	for _, w := range []struct{ id, model string }{{"a", "qwen"}, {"b", "qwen"}, {"c", "llama"}} {
		regs[w.id] = e.register(t, `{"worker_id":"`+w.id+`","model":"`+w.model+`","address":"http://h:1","max_concurrency":2,"queue_size":1}`)
	}
	e.do(t, "POST", "/v1/workers/a/heartbeat", beat(regs["a"], "READY"))
	e.do(t, "POST", "/v1/workers/b/heartbeat", beat(regs["b"], "DRAINING"))
	e.do(t, "POST", "/v1/workers/c/heartbeat", beat(regs["c"], "READY"))

	list := func(q string) []string {
		code, body, _ := e.do(t, "GET", "/v1/workers"+q, "")
		if code != 200 {
			t.Fatalf("%s: %d %s", q, code, body)
		}
		var out struct{ Workers []protocol.WorkerSnapshot }
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, w := range out.Workers {
			ids = append(ids, w.WorkerID)
		}
		return ids
	}
	for q, want := range map[string]string{
		"": "a,b,c", "?model=qwen": "a,b", "?model=qwen&eligible=true": "a", "?eligible=true": "a,c",
		"?state=DRAINING": "b", "?model=nope": "", "?eligible=false": "a,b,c",
	} {
		if got := strings.Join(list(q), ","); got != want {
			t.Errorf("GET /v1/workers%s = %q, want %q", q, got, want)
		}
	}
	for _, q := range []string{"?state=BOGUS", "?eligible=maybe"} {
		if code, body, _ := e.do(t, "GET", "/v1/workers"+q, ""); code != 400 || errCode(t, body) != "invalid_request" {
			t.Errorf("%s: %d %s", q, code, body)
		}
	}

	code, body, _ := e.do(t, "GET", "/v1/workers/a", "")
	if code != 200 || strings.Contains(body, regs["a"]) || strings.Contains(body, "registration") {
		t.Fatalf("a snapshot must never expose the registration ID: %d %s", code, body)
	}
	if code, body, _ := e.do(t, "GET", "/v1/workers/missing", ""); code != 404 || errCode(t, body) != "unknown_worker" {
		t.Fatalf("%d %s", code, body)
	}
	code, body, _ = e.do(t, "GET", "/v1/models", "")
	var models struct{ Models []protocol.ModelInfo }
	_ = json.Unmarshal([]byte(body), &models)
	if code != 200 || len(models.Models) != 2 || models.Models[1] != (protocol.ModelInfo{Model: "qwen", Workers: 2, Eligible: 1}) {
		t.Fatalf("%d %s", code, body)
	}
}

// --- authentication ----------------------------------------------------------------------------

func TestEveryV1EndpointRequiresTheTokenWhenOneIsConfigured(t *testing.T) {
	e := newEnv(t, testToken)
	endpoints := []struct{ method, path, body string }{
		{"POST", "/v1/workers/register", validRegistration},
		{"POST", "/v1/workers/w1/heartbeat", beat("reg_x", "READY")},
		{"DELETE", "/v1/workers/w1", ""},
		{"GET", "/v1/workers", ""},
		{"GET", "/v1/workers/w1", ""},
		{"GET", "/v1/models", ""},
	}
	bad := [][]string{
		nil,
		{"Authorization", ""},
		{"Authorization", "Bearer "},
		{"Authorization", "Bearer wrong"},
		{"Authorization", "Bearer " + testToken + "x"},
		{"Authorization", "Bearer " + testToken[:len(testToken)-1]},
		{"Authorization", "Basic " + testToken},
		{"Authorization", testToken},
	}
	for _, ep := range endpoints {
		for _, h := range bad {
			code, body, hdr := e.do(t, ep.method, ep.path, ep.body, h...)
			if code != 401 || errCode(t, body) != "unauthorized" || hdr.Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("%s %s with %v: got %d %s", ep.method, ep.path, h, code, body)
			}
		}
	}
	if e.reg.Len() != 0 {
		t.Fatal("an unauthenticated request must have no effect")
	}
	// With the token everything works.
	auth := []string{"Authorization", "Bearer " + testToken}
	code, body, _ := e.do(t, "POST", "/v1/workers/register", validRegistration, auth...)
	if code != 201 {
		t.Fatalf("%d %s", code, body)
	}
	if code, _, _ := e.do(t, "GET", "/v1/workers", "", auth...); code != 200 {
		t.Fatalf("%d", code)
	}
}

func TestHealthEndpointsStayOpenAndTheTokenNeverAppearsInLogsOrResponses(t *testing.T) {
	e := newEnv(t, testToken)
	for _, p := range []string{"/healthz", "/readyz"} {
		if code, body, _ := e.do(t, "GET", p, ""); code != 200 || !strings.Contains(body, "ok") {
			t.Fatalf("%s must be open: %d %s", p, code, body)
		}
	}
	// A request that sends the right token in the wrong place, and a wrong one.
	_, body1, _ := e.do(t, "GET", "/v1/workers", "", "Authorization", "Bearer "+testToken+"-nope")
	_, body2, _ := e.do(t, "GET", "/v1/workers?token="+testToken, "")
	for _, s := range []string{body1, body2, e.logs.String()} {
		if strings.Contains(s, testToken) {
			t.Fatalf("the token leaked: %s", s)
		}
	}
	if !strings.Contains(e.logs.String(), "request without a valid token") {
		t.Fatalf("rejected requests should be logged: %s", e.logs.String())
	}
}

func TestNoTokenMeansNoAuthentication(t *testing.T) {
	e := newEnv(t, "")
	if code, _, _ := e.do(t, "GET", "/v1/workers", ""); code != 200 {
		t.Fatalf("got %d", code)
	}
	// Sending a token anyway is harmless.
	if code, _, _ := e.do(t, "GET", "/v1/workers", "", "Authorization", "Bearer whatever"); code != 200 {
		t.Fatalf("got %d", code)
	}
}

func TestWrongMethodsAndPaths(t *testing.T) {
	e := newEnv(t, "")
	if code, _, _ := e.do(t, "GET", "/v1/workers/register", ""); code == 200 {
		t.Fatal("GET on register must not succeed")
	}
	if code, _, _ := e.do(t, "PUT", "/v1/workers", ""); code != 405 {
		t.Fatalf("got %d", code)
	}
	if code, _, _ := e.do(t, "GET", "/v1/nothing", ""); code != 404 {
		t.Fatalf("got %d", code)
	}
}

// --- serving ---------------------------------------------------------------------------------------

func TestServeSweepsAndShutsDownCleanly(t *testing.T) {
	cfg := registry.Config{Suspect: 40 * time.Millisecond, Unhealthy: 80 * time.Millisecond, Lost: 160 * time.Millisecond,
		Retention: 100 * time.Millisecond, MaxWorkers: 10, HeartbeatInterval: 20 * time.Millisecond}
	logs := &lockedBuf{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	reg, err := registry.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(reg, Config{SweepInterval: 10 * time.Millisecond}, log)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	if _, err := reg.Register(protocol.WorkerInfo{WorkerID: "w1", Model: "m", Address: "http://h:1", MaxConcurrency: 1}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for reg.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond) // the sweeper must evict it after lost + retention
	}
	if reg.Len() != 0 {
		t.Fatal("the sweeper did not evict a dead worker")
	}
	if !strings.Contains(logs.String(), `"to":"LOST"`) && !strings.Contains(logs.String(), "evicted") {
		t.Fatalf("the sweeper should have logged the worker's death: %s", logs.String())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a clean shutdown returns nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
}

// --- independent review and verification findings ------------------------------------------------

// raw sends a request with full control of the headers, which http.Client
// would otherwise normalize (Host, Content-Type).
func (e *env) raw(t *testing.T, method, path, body string, hdr map[string]string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
			continue
		}
		if v == "" {
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestBodiesMustBeJSON(t *testing.T) {
	// A web page can send a "simple" cross-site POST (text/plain, form types)
	// without a preflight, so anything that is not JSON is refused outright.
	e := newEnv(t, "")
	for _, ct := range []string{"text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "", "application/xml", "text/json"} {
		code, body := e.raw(t, "POST", "/v1/workers/register", validRegistration, map[string]string{"Content-Type": ct})
		if code != 415 || errCode(t, body) != "unsupported_media_type" {
			t.Errorf("Content-Type %q: got %d %s, want 415 unsupported_media_type", ct, code, body)
		}
	}
	if e.reg.Len() != 0 {
		t.Fatal("a refused registration must not be stored")
	}
	reg := e.register(t, validRegistration)
	if code, _ := e.raw(t, "POST", "/v1/workers/w1/heartbeat", beat(reg, "READY"), map[string]string{"Content-Type": "text/plain"}); code != 415 {
		t.Fatalf("heartbeats need JSON too, got %d", code)
	}
	for _, ct := range []string{"application/json", "application/json; charset=utf-8", "Application/JSON", "application/json;charset=UTF-8"} {
		if code, _ := e.raw(t, "POST", "/v1/workers/w1/heartbeat", beat(reg, "READY"), map[string]string{"Content-Type": ct}); code != 204 {
			t.Errorf("Content-Type %q must be accepted, got %d", ct, code)
		}
	}
}

func TestBrowserRequestsAreRefused(t *testing.T) {
	// Browsers attach Origin to cross-origin requests; agents and the gateway never do.
	for _, token := range []string{"", testToken} {
		e := newEnv(t, token)
		auth := map[string]string{}
		if token != "" {
			auth["Authorization"] = "Bearer " + token
		}
		for _, path := range []string{"/v1/workers", "/v1/models"} {
			h := map[string]string{"Origin": "http://evil.example"}
			for k, v := range auth {
				h[k] = v
			}
			if code, body := e.raw(t, "GET", path, "", h); code != 403 || errCode(t, body) != "forbidden" {
				t.Errorf("token=%q GET %s with an Origin header: got %d %s", token, path, code, body)
			}
		}
		h := map[string]string{"Origin": "null", "Content-Type": "application/json"}
		for k, v := range auth {
			h[k] = v
		}
		if code, _ := e.raw(t, "POST", "/v1/workers/register", validRegistration, h); code != 403 {
			t.Errorf("token=%q: a registration carrying an Origin must be refused, got %d", token, code)
		}
		if e.reg.Len() != 0 {
			t.Fatalf("token=%q: a browser-originated registration must not be stored", token)
		}
	}
}

func TestTokenlessModeRejectsForeignHostHeaders(t *testing.T) {
	// DNS rebinding: a page on attacker.example that resolves to 127.0.0.1 sends
	// requests with that Host. Without a token the Host must be a loopback name.
	e := newEnv(t, "")
	for host, want := range map[string]int{
		"": 200, "127.0.0.1:9090": 200, "localhost:9090": 200, "localhost": 200, "[::1]:9090": 200, "127.0.0.2": 200,
		"evil.example": 421, "evil.example:9090": 421, "localhost.evil.example": 421, "10.0.0.5:9090": 421, "0.0.0.0:9090": 421,
	} {
		code, body := e.raw(t, "GET", "/v1/workers", "", map[string]string{"Host": host})
		if code != want {
			t.Errorf("Host %q: got %d %s, want %d", host, code, body, want)
		}
	}
	if code, _ := e.raw(t, "POST", "/v1/workers/register", validRegistration, map[string]string{"Host": "evil.example", "Content-Type": "application/json"}); code != 421 {
		t.Fatalf("a foreign Host must not be able to register a worker, got %d", code)
	}
	if e.reg.Len() != 0 {
		t.Fatal("nothing may be registered through a foreign Host")
	}
}

func TestWithATokenAnyHostIsFine(t *testing.T) {
	// A deployed control plane is reached by its real name; the token is the defence.
	e := newEnv(t, testToken)
	code, _ := e.raw(t, "GET", "/v1/workers", "", map[string]string{"Host": "control-plane.internal:9090", "Authorization": "Bearer " + testToken})
	if code != 200 {
		t.Fatalf("got %d", code)
	}
}

func TestBearerSchemeIsCaseInsensitive(t *testing.T) {
	e := newEnv(t, testToken)
	for _, scheme := range []string{"Bearer", "bearer", "BEARER", "bEaReR"} {
		if code, _ := e.raw(t, "GET", "/v1/workers", "", map[string]string{"Authorization": scheme + " " + testToken}); code != 200 {
			t.Errorf("scheme %q must be accepted (RFC 7235), got %d", scheme, code)
		}
	}
	for _, bad := range []string{"Bearer", "Bearer  " + testToken + "x", "Basic " + testToken, "Bearer" + testToken} {
		if code, _ := e.raw(t, "GET", "/v1/workers", "", map[string]string{"Authorization": bad}); code != 401 {
			t.Errorf("%q must be refused, got %d", bad, code)
		}
	}
}

func TestRepeatedAuthFailuresDoNotFloodTheLog(t *testing.T) {
	e := newEnv(t, testToken)
	for i := 0; i < 200; i++ {
		e.do(t, "GET", "/v1/workers", "")
	}
	n := strings.Count(e.logs.String(), "request without a valid token")
	if n < 1 || n > 5 {
		t.Fatalf("200 rejected requests produced %d warning lines; the log must be rate limited", n)
	}
	// The suppressed ones are accounted for, not silently lost.
	time.Sleep(1100 * time.Millisecond)
	e.do(t, "GET", "/v1/workers", "")
	var last map[string]any
	for _, line := range strings.Split(strings.TrimSpace(e.logs.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && strings.Contains(line, "request without a valid token") {
			last = m
		}
	}
	if sup, _ := last["suppressed"].(float64); sup < float64(200-n) {
		t.Fatalf("the next warning must report the %d suppressed ones, got %v: %v", 200-n, last["suppressed"], last)
	}
}

func TestRateLimitCountsAndResetsWhatItSuppresses(t *testing.T) {
	var l rateLimit
	t0 := time.Unix(1_700_000_000, 0)
	if ok, sup := l.allow(t0); !ok || sup != 0 {
		t.Fatalf("the first call passes with nothing suppressed, got %v %d", ok, sup)
	}
	for i := 0; i < 3; i++ {
		if ok, _ := l.allow(t0.Add(500 * time.Millisecond)); ok {
			t.Fatal("calls inside the interval are suppressed")
		}
	}
	if ok, sup := l.allow(t0.Add(time.Second)); !ok || sup != 3 {
		t.Fatalf("the next allowed call reports 3 suppressed, got %v %d", ok, sup)
	}
	if ok, sup := l.allow(t0.Add(2 * time.Second)); !ok || sup != 0 {
		t.Fatalf("the count must reset after being reported, got %v %d", ok, sup)
	}
}

func TestProductionDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.MaxBodyBytes != 64<<10 || c.SweepInterval != time.Second || c.ShutdownTimeout != 15*time.Second ||
		c.ReadTimeout != 10*time.Second || c.WriteTimeout != 20*time.Second {
		t.Fatalf("defaults changed: %+v", c)
	}
	keep := Config{MaxBodyBytes: 1, SweepInterval: time.Minute, ShutdownTimeout: time.Minute, ReadTimeout: time.Minute, WriteTimeout: time.Minute}.withDefaults()
	if keep.MaxBodyBytes != 1 || keep.ReadTimeout != time.Minute || keep.WriteTimeout != time.Minute {
		t.Fatalf("explicit settings must be kept: %+v", keep)
	}
}

func TestLoopbackHostForms(t *testing.T) {
	for host, want := range map[string]bool{
		"localhost": true, "LOCALHOST": true, "LocalHost:9090": true, "127.0.0.1": true, "127.0.0.1:9": true,
		"[::1]": true, "[::1]:9090": true, "::1": true,
		"evil.com": false, "127.0.0.1.evil.com": false, "localhost.evil.com": false, "0.0.0.0": false,
		"10.0.0.5:80": false, "": false, "[fe80::1%25eth0]": false,
	} {
		if got := loopbackHost(host); got != want {
			t.Errorf("loopbackHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func newServing(t *testing.T, cfg Config) (net.Listener, context.CancelFunc, chan error) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg, _ := registry.New(registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second, Retention: time.Minute, MaxWorkers: 5, HeartbeatInterval: 2 * time.Second}, log)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(reg, cfg, log).Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second): // a test may already have consumed it
		}
	})
	return ln, cancel, done
}

func TestOversizedHeadersAreRefused(t *testing.T) {
	ln, _, _ := newServing(t, Config{})
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Pad: "+strings.Repeat("a", 64<<10)+"\r\n\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	out, _ := io.ReadAll(conn)
	if !strings.Contains(string(out), "431") {
		t.Fatalf("a 64KB header must be refused with 431, got %q", firstLine(string(out)))
	}
}

func firstLine(s string) string { l, _, _ := strings.Cut(s, "\r\n"); return l }

func TestShutdownForcesConnectionsClosedWhenTheGraceRunsOut(t *testing.T) {
	// A stalled request outlasts the shutdown grace period (the read timeout is far longer),
	// so only the forced close can end it.
	ln, cancel, done := newServing(t, Config{ReadTimeout: time.Minute, ShutdownTimeout: 150 * time.Millisecond})
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = io.WriteString(conn, "POST /v1/workers/register HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{")
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a shutdown that had to give up reports it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("the stalled connection must have been closed by the server, not left to time out: %v", err)
	}
}

func TestStateFilterOverHTTP(t *testing.T) {
	e := newEnv(t, "")
	reg := e.register(t, validRegistration)
	e.do(t, "POST", "/v1/workers/w1/heartbeat", beat(reg, "READY"))
	e.clock.Advance(11 * time.Second) // UNHEALTHY
	count := func(q string) int {
		code, body, _ := e.do(t, "GET", "/v1/workers"+q, "")
		if code != 200 {
			t.Fatalf("%s: %d %s", q, code, body)
		}
		var out struct{ Workers []protocol.WorkerSnapshot }
		_ = json.Unmarshal([]byte(body), &out)
		return len(out.Workers)
	}
	if count("?state=UNHEALTHY") != 1 || count("?state=READY") != 0 || count("?state=LOST") != 0 {
		t.Fatal("the state filter must use the effective state")
	}
	e.clock.Advance(30 * time.Second)
	if count("?state=LOST") != 1 {
		t.Fatal("LOST must be a valid filter")
	}
	if code, body, _ := e.do(t, "GET", "/v1/workers?state=TERMINATED", ""); code != 400 || errCode(t, body) != "invalid_request" {
		t.Fatalf("TERMINATED is never a state a worker can be in: %d %s", code, body)
	}
	if code, _, _ := e.do(t, "GET", "/v1/workers/a%20b", ""); code != 400 {
		t.Fatalf("an invalid id on GET must be a 400, got %d", code)
	}
}

func TestServeRefusesToRunWithoutATokenOffLoopback(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg, _ := registry.New(registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second, Retention: time.Minute, MaxWorkers: 5, HeartbeatInterval: 2 * time.Second}, log)
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Skip("cannot listen on all interfaces here")
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- New(reg, Config{}, log).Serve(ctx, ln) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "token") {
			t.Fatalf("a tokenless server must refuse a non-loopback listener, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a tokenless server started serving on a non-loopback listener instead of refusing")
	}
}

func TestSlowBodiesAreCutOff(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg, _ := registry.New(registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second, Retention: time.Minute, MaxWorkers: 5, HeartbeatInterval: 2 * time.Second}, log)
	srv := New(reg, Config{ReadTimeout: 300 * time.Millisecond}, log)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	defer func() { cancel(); <-done }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// Promise 500 bytes, send a few, then stall: a slow-body client.
	_, _ = io.WriteString(conn, "POST /v1/workers/register HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{\"worker")
	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = io.ReadAll(conn) // the server must give up on us and close
	if time.Since(start) > 3*time.Second {
		t.Fatalf("a stalled body held the connection for %v (err %v)", time.Since(start), err)
	}
}

func TestShutdownIsNotHeldHostageByAStalledBody(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg, _ := registry.New(registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second, Retention: time.Minute, MaxWorkers: 5, HeartbeatInterval: 2 * time.Second}, log)
	srv := New(reg, Config{ReadTimeout: 400 * time.Millisecond, ShutdownTimeout: 5 * time.Second}, log)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	conn, _ := net.Dial("tcp", ln.Addr().String())
	defer func() { _ = conn.Close() }()
	_, _ = io.WriteString(conn, "POST /v1/workers/register HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{")
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a stalled body must be cut off by the read timeout so shutdown is clean, got %v", err)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("shutdown took %v", took)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("shutdown hung behind a stalled request")
	}
}

func TestEmptyOriginHeaderIsStillABrowserSignal(t *testing.T) {
	// Header.Get returns "" for a present-but-empty Origin; the check must see the header itself.
	for _, token := range []string{"", testToken} {
		e := newEnv(t, token)
		req, _ := http.NewRequest("GET", e.ts.URL+"/v1/workers", nil)
		req.Header["Origin"] = []string{""}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 403 {
			t.Errorf("token=%q: an empty Origin header must be refused, got %d", token, resp.StatusCode)
		}
	}
}
