package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

// authStore is an in-memory auth.KeyStore that counts lookups and can be switched off.
type authStore struct {
	mu      sync.Mutex
	recs    map[string]auth.KeyRecord
	lookups atomic.Int64
	down    atomic.Bool
	touched chan string
	bad     atomic.Bool // LookupKey reports an unreadable record
	clock   *testClock
}

func newAuthStore() *authStore {
	return &authStore{recs: map[string]auth.KeyRecord{}, touched: make(chan string, 16)}
}

// TouchKey makes authStore an auth.KeyToucher, so last_used_at writes can be observed.
func (s *authStore) TouchKey(_ context.Context, keyID string, _ time.Time) error {
	select {
	case s.touched <- keyID:
	default:
	}
	return nil
}

func (s *authStore) LookupKey(_ context.Context, prefix string) (auth.KeyRecord, error) {
	s.lookups.Add(1)
	if s.down.Load() {
		return auth.KeyRecord{}, errors.New("connection refused")
	}
	if s.bad.Load() {
		return auth.KeyRecord{}, auth.ErrBadRecord
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.recs[prefix]
	if !ok {
		return auth.KeyRecord{}, auth.ErrNotFound
	}
	return r, nil
}

// add creates an active key for tenant (an active tenant allowing the given models; nil means all).
func (s *authStore) add(tenant string, models []string, mutate ...func(*auth.KeyRecord)) string {
	key, prefix, hash := auth.GenerateKey()
	rec := auth.KeyRecord{
		KeyID: "key_" + tenant, TenantID: "ten_" + tenant, Prefix: prefix, SecretHash: hash, KeyStatus: auth.KeyActive,
		Policy: auth.TenantPolicy{Status: auth.TenantActive, AllowedModels: models, Priority: 2},
	}
	for _, m := range mutate {
		m(&rec)
	}
	s.mu.Lock()
	s.recs[prefix] = rec
	s.mu.Unlock()
	return key
}

type authEnv struct {
	url    string
	store  *authStore
	logs   *lockedBuffer
	client *http.Client
	gw     *Server
	worker *fakeWorker // registry mode only
	sched  *captureSched
	clock  *testClock // static mode: the authenticator's clock
}

func newAuthenticator(store *authStore, logs *lockedBuffer, clk *testClock) *auth.Authenticator {
	return auth.New(store, auth.Config{Now: clk.Now, CacheTTL: time.Minute, NegativeTTL: 30 * time.Second, StaleGrace: 5 * time.Minute, CacheSize: 100,
		Logger: slog.New(slog.NewJSONHandler(logs, nil))})
}

func serve(t *testing.T, gw *Server) (string, *http.Client) {
	t.Helper()
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	return srv.URL, &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// newStaticAuthEnv is a static-mode gateway with authentication required.
func newStaticAuthEnv(t *testing.T) *authEnv {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Seen-Auth", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(up.Close)
	cfg := testConfig(up.URL)
	cfg.Models = []string{"qwen-7b", "llama-8b"}
	logs := &lockedBuffer{}
	store := newAuthStore()
	clk := &testClock{t: time.Now()}
	store.clock = clk
	gw := New(cfg, slog.New(slog.NewJSONHandler(logs, nil)), WithAuthenticator(newAuthenticator(store, logs, clk)))
	url, c := serve(t, gw)
	return &authEnv{url: url, store: store, logs: logs, client: c, gw: gw, clock: clk}
}

// captureSched wraps a scheduler and records the requests it is asked about.
type captureSched struct {
	scheduler.Scheduler
	mu   sync.Mutex
	reqs []protocol.InferenceRequest
}

func (c *captureSched) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	c.mu.Lock()
	c.reqs = append(c.reqs, *req)
	c.mu.Unlock()
	return c.Scheduler.SelectWorker(ctx, req, ws)
}

// newRegistryAuthEnv is a registry-mode gateway with one worker serving model "qwen-7b" and "llama-8b".
func newRegistryAuthEnv(t *testing.T) *authEnv {
	t.Helper()
	w1 := newFakeWorker(t, "w1", nil)
	w2 := newFakeWorker(t, "w2", nil)
	cfg := testConfig("http://127.0.0.1:1")
	cfg.WorkerSource = config.WorkerSourceRegistry
	logs := &lockedBuffer{}
	lister := &fakeLister{workers: []protocol.WorkerSnapshot{w1.snapshot("qwen-7b", 4), w2.snapshot("llama-8b", 4)}}
	gw, err := newRegistryServer(cfg, "round-robin", 5*time.Second, lister, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	gw.router.cache.now = clock.Now
	if err := gw.router.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	cs := &captureSched{Scheduler: gw.router.sched}
	gw.router.sched = cs
	store := newAuthStore()
	gw.SetAuthenticator(newAuthenticator(store, logs, &testClock{t: time.Now()}))
	url, c := serve(t, gw)
	return &authEnv{url: url, store: store, logs: logs, client: c, gw: gw, worker: w1, sched: cs}
}

func (e *authEnv) do(t *testing.T, method, path, body string, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, e.url+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func chatBody(model string) string {
	return `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`
}

func (e *authEnv) chat(t *testing.T, key, model string) (*http.Response, string) {
	t.Helper()
	if key == "" {
		return e.do(t, http.MethodPost, chatCompletionsPath, chatBody(model))
	}
	return e.do(t, http.MethodPost, chatCompletionsPath, chatBody(model), "Authorization", "Bearer "+key)
}

func TestAuthOffByDefaultNeedsNoKey(t *testing.T) {
	env := newEnv(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	if resp := env.post(t, validBody); resp.StatusCode != 200 {
		t.Fatalf("with auth off a request needs no key: %d", resp.StatusCode)
	}
	if resp, _ := env.get(t, "/v1/models"); resp.StatusCode != 200 {
		t.Fatalf("models: %d", resp.StatusCode)
	}
	if env.gw.authn != nil {
		t.Fatal("authentication must be off unless SetAuthenticator is called")
	}
}

func TestEveryKeyFailureIsTheSame401(t *testing.T) {
	e := newStaticAuthEnv(t)
	good := e.store.add("a", nil)
	revoked := e.store.add("r", nil, func(r *auth.KeyRecord) { r.KeyStatus = auth.KeyRevoked })
	past := time.Now().Add(-time.Hour)
	expired := e.store.add("e", nil, func(r *auth.KeyRecord) { r.ExpiresAt = &past })
	unknown, _, _ := auth.GenerateKey()
	other, _, _ := auth.GenerateKey()
	prefix := good[len("sf_") : len("sf_")+8]
	wrongSecret := "sf_" + prefix + "_" + other[len("sf_")+9:]

	attempts := map[string][]string{
		"no header":         nil,
		"empty bearer":      {"Authorization", "Bearer "},
		"wrong scheme":      {"Authorization", "Basic " + good},
		"bare key":          {"Authorization", good},
		"malformed":         {"Authorization", "Bearer sf_nonsense"},
		"huge":              {"Authorization", "Bearer " + strings.Repeat("A", 100000)},
		"unknown":           {"Authorization", "Bearer " + unknown},
		"wrong secret":      {"Authorization", "Bearer " + wrongSecret},
		"revoked":           {"Authorization", "Bearer " + revoked},
		"expired":           {"Authorization", "Bearer " + expired},
		"two headers":       {"Authorization", "Bearer " + good, "Authorization", "Bearer " + good},
		"x-api-key instead": {"X-Api-Key", good},
	}

	var wantBody, wantWWW string
	for name, hdr := range attempts {
		for _, path := range []string{chatCompletionsPath, "/v1/models"} {
			method, body := http.MethodPost, chatBody("qwen-7b")
			if path == "/v1/models" {
				method, body = http.MethodGet, ""
			}
			resp, got := e.do(t, method, path, body, hdr...)
			if resp.StatusCode != 401 {
				t.Errorf("%s %s: status %d, want 401", name, path, resp.StatusCode)
				continue
			}
			if wantBody == "" {
				wantBody, wantWWW = got, resp.Header.Get("WWW-Authenticate")
			}
			if got != wantBody {
				t.Errorf("%s %s: body differs from the others:\n%s\nvs\n%s", name, path, got, wantBody)
			}
			if w := resp.Header.Get("WWW-Authenticate"); w != wantWWW || w != "Bearer" {
				t.Errorf("%s %s: WWW-Authenticate %q", name, path, w)
			}
			if !strings.Contains(got, `"code":"UNAUTHORIZED"`) || resp.Header.Get(protocol.HeaderRequestID) == "" {
				t.Errorf("%s %s: body %s, request id %q", name, path, got, resp.Header.Get(protocol.HeaderRequestID))
			}
		}
	}
	// The body must not hint at why.
	for _, hint := range []string{"revoked", "expired", "unknown", "secret", "prefix", "malformed", "suspend"} {
		if strings.Contains(strings.ToLower(wantBody), hint) {
			t.Errorf("401 body hints at %q: %s", hint, wantBody)
		}
	}
	// And the valid key still works.
	if resp, _ := e.chat(t, good, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("valid key: %d", resp.StatusCode)
	}
}

func TestBearerSchemeIsCaseInsensitive(t *testing.T) {
	e := newStaticAuthEnv(t)
	good := e.store.add("a", nil)
	for _, scheme := range []string{"Bearer", "bearer", "BEARER"} {
		resp, _ := e.do(t, http.MethodPost, chatCompletionsPath, chatBody("qwen-7b"), "Authorization", scheme+" "+good)
		if resp.StatusCode != 200 {
			t.Errorf("%s: %d", scheme, resp.StatusCode)
		}
	}
}

func TestSuspendedTenantIs403AndUnknownSecretIsNot(t *testing.T) {
	e := newStaticAuthEnv(t)
	key := e.store.add("s", nil, func(r *auth.KeyRecord) { r.Policy.Status = auth.TenantSuspended })
	resp, body := e.chat(t, key, "qwen-7b")
	if resp.StatusCode != 403 || !strings.Contains(body, `"code":"FORBIDDEN"`) || resp.Header.Get("WWW-Authenticate") != "" {
		t.Fatalf("suspended: %d %s", resp.StatusCode, body)
	}
	// Without the secret the same prefix is just a 401: suspension is not observable to outsiders.
	other, _, _ := auth.GenerateKey()
	forged := "sf_" + key[len("sf_"):len("sf_")+8] + "_" + other[len("sf_")+9:]
	if resp, _ := e.chat(t, forged, "qwen-7b"); resp.StatusCode != 401 {
		t.Fatalf("forged key for a suspended tenant: %d", resp.StatusCode)
	}
}

func TestValidKeyReachesTheWorkerAndNeverLeaks(t *testing.T) {
	e := newStaticAuthEnv(t)
	key := e.store.add("acme", nil)
	resp, body := e.chat(t, key, "qwen-7b")
	if resp.StatusCode != 200 || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Seen-Auth"); got != "" {
		t.Fatalf("the client's Authorization reached the upstream: %q", got)
	}
	var found bool
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "request" && l["path"] == chatCompletionsPath {
			found = true
			if l["tenant_id"] != "ten_acme" || l["api_key_id"] != "key_acme" {
				t.Errorf("request log lacks the tenant: %v", l)
			}
		}
	}
	if !found {
		t.Fatal("no request log line")
	}
	secret := key[len("sf_")+9:]
	prefix := key[len("sf_") : len("sf_")+8]
	for _, bad := range []string{key, secret, prefix} {
		if strings.Contains(e.logs.String(), bad) {
			t.Fatalf("logs contain key material %q", bad[:4])
		}
	}
}

func TestRejectedRequestsAreLoggedWithoutKeyMaterial(t *testing.T) {
	e := newStaticAuthEnv(t)
	revoked := e.store.add("r", nil, func(r *auth.KeyRecord) { r.KeyStatus = auth.KeyRevoked })
	e.chat(t, revoked, "qwen-7b")
	e.chat(t, "sf_not_a_key_at_all", "qwen-7b")
	var reasons []string
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "request" {
			if l["status"] != float64(401) || l["error_code"] != "UNAUTHORIZED" {
				t.Errorf("log line: %v", l)
			}
			reasons = append(reasons, fmt.Sprint(l["auth_failure"]))
			if _, has := l["tenant_id"]; has {
				t.Errorf("an unauthenticated request must not be attributed to a tenant: %v", l)
			}
		}
	}
	if len(reasons) != 2 || reasons[0] != "revoked" || reasons[1] != "invalid" {
		t.Fatalf("reasons: %v", reasons)
	}
	if strings.Contains(e.logs.String(), revoked[len("sf_"):]) || strings.Contains(e.logs.String(), "not_a_key") {
		t.Fatalf("logs contain what the client sent as a key:\n%s", e.logs.String())
	}
}

func TestOperationalEndpointsStayOpen(t *testing.T) {
	e := newStaticAuthEnv(t)
	for _, p := range []string{"/healthz", "/metrics"} {
		if resp, _ := e.do(t, http.MethodGet, p, ""); resp.StatusCode != 200 {
			t.Errorf("%s: %d", p, resp.StatusCode)
		}
	}
	// /readyz depends on the upstream, which is up in this environment.
	if resp, _ := e.do(t, http.MethodGet, "/readyz", ""); resp.StatusCode != 200 {
		t.Errorf("/readyz: %d", resp.StatusCode)
	}
}

func TestAuthRejectionMetricHasBoundedLabels(t *testing.T) {
	e := newStaticAuthEnv(t)
	for i := 0; i < 3; i++ {
		e.chat(t, "", "qwen-7b")
	}
	e.store.down.Store(true)
	k, _, _ := auth.GenerateKey()
	e.chat(t, k, "qwen-7b")
	_, m := e.do(t, http.MethodGet, "/metrics", "")
	for _, want := range []string{`auth_rejections_total{status="401"} 3`, `auth_rejections_total{status="503"} 1`} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	if strings.Contains(m, "ten_") || strings.Contains(m, "key_") {
		t.Error("tenant or key IDs leaked into metrics labels")
	}
}

func TestAuthUnavailableWhenStoreDownAndKeyUncached(t *testing.T) {
	e := newStaticAuthEnv(t)
	cached := e.store.add("c", nil)
	uncached := e.store.add("u", nil)
	if resp, _ := e.chat(t, cached, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	e.store.down.Store(true)
	// The cached key is still served...
	if resp, _ := e.chat(t, cached, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("cached key during an outage: %d", resp.StatusCode)
	}
	// ...an uncached one gets 503 AUTH_UNAVAILABLE with Retry-After, not a 401.
	for i := 0; i < 5; i++ {
		resp, body := e.chat(t, uncached, "qwen-7b")
		if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" || !strings.Contains(body, `"code":"AUTH_UNAVAILABLE"`) {
			t.Fatalf("uncached key during an outage: %d %v %s", resp.StatusCode, resp.Header, body)
		}
	}
	// Recovery needs no restart.
	e.store.down.Store(false)
	e.clock.Advance(auth.OutageBackoff + time.Millisecond) // past the backoff; no wall-clock wait
	if resp, _ := e.chat(t, uncached, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("after recovery: %d", resp.StatusCode)
	}
	// The outage produced one log line, not one per request.
	n := strings.Count(e.logs.String(), "key store unavailable")
	if n != 1 {
		t.Fatalf("outage logged %d times", n)
	}
}

func TestWarmCacheMeansNoStoreQueriesPerRequest(t *testing.T) {
	e := newStaticAuthEnv(t)
	key := e.store.add("a", nil)
	for i := 0; i < 200; i++ {
		if resp, _ := e.chat(t, key, "qwen-7b"); resp.StatusCode != 200 {
			t.Fatal(resp.StatusCode)
		}
	}
	if n := e.store.lookups.Load(); n != 1 {
		t.Fatalf("%d store lookups for 200 requests with a warm cache", n)
	}
	// A flood of requests with the same unknown key is answered from the negative cache.
	bad, _, _ := auth.GenerateKey()
	before := e.store.lookups.Load()
	for i := 0; i < 200; i++ {
		if resp, _ := e.chat(t, bad, "qwen-7b"); resp.StatusCode != 401 {
			t.Fatal(resp.StatusCode)
		}
	}
	if n := e.store.lookups.Load() - before; n != 1 {
		t.Fatalf("%d lookups for 200 requests with one unknown key", n)
	}
}

func TestConcurrentFirstRequestsShareOneLookup(t *testing.T) {
	e := newStaticAuthEnv(t)
	key := e.store.add("a", nil)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.chat(t, key, "qwen-7b")
		}()
	}
	wg.Wait()
	if n := e.store.lookups.Load(); n != 1 {
		t.Fatalf("40 concurrent first requests caused %d lookups", n)
	}
}

func TestAllowedModelsAreEnforcedAndModelsListIsFiltered(t *testing.T) {
	e := newStaticAuthEnv(t)
	all := e.store.add("all", nil)
	one := e.store.add("one", []string{"qwen-7b"})
	none := e.store.add("none", []string{})

	if resp, _ := e.chat(t, one, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("allowed model: %d", resp.StatusCode)
	}
	resp, forbidden := e.chat(t, one, "llama-8b") // exists, not allowed
	if resp.StatusCode != 403 || !strings.Contains(forbidden, `"code":"FORBIDDEN"`) {
		t.Fatalf("disallowed model: %d %s", resp.StatusCode, forbidden)
	}
	// A model that does not exist looks exactly the same to a tenant with an allow-list...
	resp, missing := e.chat(t, one, "llama-8b")
	_ = resp
	_, nonexistent := e.chat(t, one, "does-not-exist")
	if strings.ReplaceAll(missing, "llama-8b", "X") != strings.ReplaceAll(nonexistent, "does-not-exist", "X") {
		t.Fatalf("existing-but-forbidden and nonexistent models are distinguishable:\n%s\n%s", missing, nonexistent)
	}
	// ...and a tenant with no restriction gets the normal 404 for a nonexistent model.
	if resp, _ := e.chat(t, all, "does-not-exist"); resp.StatusCode != 404 {
		t.Fatalf("unrestricted tenant, unknown model: %d", resp.StatusCode)
	}
	if resp, _ := e.chat(t, none, "qwen-7b"); resp.StatusCode != 403 {
		t.Fatalf("empty allow-list: %d", resp.StatusCode)
	}

	list := func(key string) string {
		_, b := e.do(t, http.MethodGet, "/v1/models", "", "Authorization", "Bearer "+key)
		return b
	}
	if l := list(all); !strings.Contains(l, "qwen-7b") || !strings.Contains(l, "llama-8b") {
		t.Fatalf("unrestricted list: %s", l)
	}
	if l := list(one); !strings.Contains(l, "qwen-7b") || strings.Contains(l, "llama-8b") {
		t.Fatalf("filtered list: %s", l)
	}
	if l := list(none); strings.Contains(l, "qwen-7b") || !strings.Contains(l, `"data":[]`) {
		t.Fatalf("empty list: %s", l)
	}
}

func TestRegistryModeCarriesTheTenantToTheSchedulerAndLogs(t *testing.T) {
	e := newRegistryAuthEnv(t)
	key := e.store.add("acme", []string{"qwen-7b"})
	resp, _ := e.chat(t, key, "qwen-7b")
	if resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	}
	if got, _ := e.worker.lastAuth.Load().(string); got != "" {
		t.Fatalf("the client's Authorization reached a worker: %q", got)
	}
	e.sched.mu.Lock()
	reqs := append([]protocol.InferenceRequest(nil), e.sched.reqs...)
	e.sched.mu.Unlock()
	if len(reqs) != 1 || reqs[0].TenantID != "ten_acme" || reqs[0].Priority != 2 || reqs[0].Model != "qwen-7b" {
		t.Fatalf("scheduler saw %+v", reqs)
	}
	seen := map[string]bool{}
	for _, l := range e.logs.logLines(t) {
		switch l["msg"] {
		case "request", "attempt finished", "worker selected":
			if l["tenant_id"] != "ten_acme" || l["api_key_id"] != "key_acme" {
				t.Errorf("%v lacks the tenant: %v", l["msg"], l)
			}
			seen[fmt.Sprint(l["msg"])] = true
		}
	}
	if len(seen) != 3 {
		t.Fatalf("expected request, attempt and selection log lines, saw %v", seen)
	}
	// A disallowed model never reaches the scheduler.
	if resp, _ := e.chat(t, key, "llama-8b"); resp.StatusCode != 403 {
		t.Fatalf("disallowed: %d", resp.StatusCode)
	}
	e.sched.mu.Lock()
	n := len(e.sched.reqs)
	e.sched.mu.Unlock()
	if n != 1 {
		t.Fatalf("a forbidden model was routed (%d scheduler calls)", n)
	}
	// The registry-mode models list is filtered too.
	_, l := e.do(t, http.MethodGet, "/v1/models", "", "Authorization", "Bearer "+key)
	if !strings.Contains(l, "qwen-7b") || strings.Contains(l, "llama-8b") {
		t.Fatalf("filtered registry list: %s", l)
	}
}

// Authentication must complete before the body is read: a client that sends headers and then
// stalls must get its 401 at once, not tie up the gateway waiting for a body.
func TestRejectionHappensBeforeTheBodyIsRead(t *testing.T) {
	e := newStaticAuthEnv(t)
	conn, err := net.Dial("tcp", strings.TrimPrefix(e.url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n", chatCompletionsPath)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response before the body was sent: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestRequiredButNotWiredFailsClosed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") }))
	t.Cleanup(up.Close)
	logs := &lockedBuffer{}
	gw := New(testConfig(up.URL), slog.New(slog.NewJSONHandler(logs, nil)), WithAuthenticator(nil))
	if !gw.AuthRequired() {
		t.Fatal("WithAuthenticator(nil) must still mean authentication is required")
	}
	url, c := serve(t, gw)
	resp, err := c.Post(url+chatCompletionsPath, "application/json", strings.NewReader(chatBody("qwen-7b")))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("a gateway that needs authentication and has none must not serve: %d", resp.StatusCode)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if err := gw.Serve(context.Background(), ln); err == nil || !strings.Contains(err.Error(), "no authenticator") {
		t.Fatalf("Serve must refuse: %v", err)
	}
	// And the default really is open.
	if New(testConfig(up.URL), slog.New(slog.NewJSONHandler(logs, nil))).AuthRequired() {
		t.Fatal("authentication must be off by default")
	}
}

func TestUnreadableKeyRecordIsA500ForThatKeyOnly(t *testing.T) {
	e := newStaticAuthEnv(t)
	good := e.store.add("a", nil)
	e.store.bad.Store(true)
	resp, body := e.chat(t, good, "qwen-7b")
	if resp.StatusCode != 500 || !strings.Contains(body, "INTERNAL_ERROR") || strings.Contains(body, "UNAUTHORIZED") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	found := false
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "request" && l["auth_failure"] == "fault" {
			found = true
		}
	}
	if !found {
		t.Fatal("the fault was not logged with its reason")
	}
	// It was not an outage: no global backoff, so the next key is looked up at once.
	e.store.bad.Store(false)
	other := e.store.add("b", nil)
	if resp, _ := e.chat(t, other, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("another key after one key's data fault: %d", resp.StatusCode)
	}
}

// Serve must run the authenticator's background writer, or last_used_at is never recorded.
func TestServeRunsTheAuthenticatorsBackgroundWork(t *testing.T) {
	e := newStaticAuthEnv(t)
	key := e.store.add("acme", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- e.gw.Serve(ctx, ln) }()
	req, _ := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	select {
	case id := <-e.store.touched:
		if id != "key_acme" {
			t.Fatalf("touched %q", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("last_used_at was never recorded: Serve did not start the authenticator's writer")
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
