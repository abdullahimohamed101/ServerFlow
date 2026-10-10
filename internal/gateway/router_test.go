package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/config"
	"serverflow/pkg/protocol"
)

// fakeWorker is an inference worker that says who answered and can be held open.
type fakeWorker struct {
	id       string
	srv      *httptest.Server
	hits     atomic.Int64
	lastAuth atomic.Value  // string
	hold     chan struct{} // when non-nil, requests wait for it to close
}

func newFakeWorker(t *testing.T, id string, hold chan struct{}) *fakeWorker {
	t.Helper()
	w := &fakeWorker{id: id, hold: hold}
	w.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != chatCompletionsPath { // a mangled worker address must show up as a failure
			http.NotFound(rw, r)
			return
		}
		w.hits.Add(1)
		w.lastAuth.Store(r.Header.Get("Authorization"))
		if w.hold != nil {
			select {
			case <-w.hold:
			case <-r.Context().Done():
				return
			}
		}
		rw.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(rw, `{"worker":"`+id+`"}`)
	}))
	t.Cleanup(w.srv.Close)
	return w
}

func (w *fakeWorker) snapshot(model string, maxConc int) protocol.WorkerSnapshot {
	s := snapWorker(w.id, model, 0)
	s.Address, s.MaxConcurrency = w.srv.URL, maxConc
	return s
}

type regEnv struct {
	gw     *Server
	url    string
	lister *fakeLister
	clock  *testClock
	logs   *lockedBuffer
	client *http.Client
}

func newRegEnv(t *testing.T, strategy string, ws []protocol.WorkerSnapshot, mutate ...func(*config.GatewayConfig)) *regEnv {
	t.Helper()
	cfg := testConfig("http://127.0.0.1:1")
	cfg.WorkerSource = config.WorkerSourceRegistry
	for _, m := range mutate {
		m(&cfg)
	}
	logs := &lockedBuffer{}
	lister := &fakeLister{workers: ws}
	gw, err := newRegistryServer(cfg, strategy, 5*time.Second, lister, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	gw.router.cache.now = clock.Now
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	e := &regEnv{gw: gw, url: srv.URL, lister: lister, clock: clock, logs: logs, client: &http.Client{Transport: tr, Timeout: 30 * time.Second}}
	if len(ws) > 0 {
		e.refresh(t)
	}
	return e
}

func (e *regEnv) refresh(t *testing.T) {
	t.Helper()
	if err := e.gw.router.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (e *regEnv) chat(t *testing.T, model string, hdr ...string) (*http.Response, string) {
	t.Helper()
	body := `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest(http.MethodPost, e.url+chatCompletionsPath, strings.NewReader(body))
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

func (e *regEnv) get(t *testing.T, path string) (*http.Response, string) {
	t.Helper()
	resp, err := e.client.Get(e.url + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func whoAnswered(t *testing.T, body string) string {
	t.Helper()
	var v struct{ Worker string }
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("not a worker answer: %q", body)
	}
	return v.Worker
}

func TestRegistryModeRoutesByTheConfiguredStrategy(t *testing.T) {
	a, b, c := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil), newFakeWorker(t, "c", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{c.snapshot("qwen-7b", 4), a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4)})
	var got []string
	for i := 0; i < 6; i++ {
		resp, body := e.chat(t, "qwen-7b")
		if resp.StatusCode != 200 {
			t.Fatalf("status %d %s", resp.StatusCode, body)
		}
		got = append(got, whoAnswered(t, body))
	}
	if strings.Join(got, "") != "abcabc" {
		t.Fatalf("round-robin order: %v", got)
	}
	if a.hits.Load() != 2 || b.hits.Load() != 2 || c.hits.Load() != 2 {
		t.Fatalf("hits %d %d %d", a.hits.Load(), b.hits.Load(), c.hits.Load())
	}
	if !strings.Contains(e.logs.String(), `"worker_id":"a"`) || !strings.Contains(e.logs.String(), `"strategy":"round-robin"`) {
		t.Fatalf("the selection must be logged with worker and strategy:\n%s", e.logs.String())
	}
}

func TestRegistryModeLeastActiveFollowsReportedLoad(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 8), b.snapshot("qwen-7b", 8)
	sa.Metrics.ActiveRequests, sb.Metrics.ActiveRequests = 5, 1
	e := newRegEnv(t, "least-active", []protocol.WorkerSnapshot{sa, sb})
	_, body := e.chat(t, "qwen-7b")
	if whoAnswered(t, body) != "b" {
		t.Fatalf("the less loaded worker must win: %s", body)
	}
}

func TestOnlyEligibleWorkersOfTheRightModelAreEverUsed(t *testing.T) {
	good, draining, wrong := newFakeWorker(t, "good", nil), newFakeWorker(t, "drain", nil), newFakeWorker(t, "wrong", nil)
	sd := draining.snapshot("qwen-7b", 4)
	sd.State, sd.Eligible = protocol.StateDraining, false
	e := newRegEnv(t, "random", []protocol.WorkerSnapshot{good.snapshot("qwen-7b", 4), sd, wrong.snapshot("llama", 4)})
	for i := 0; i < 30; i++ {
		if _, body := e.chat(t, "qwen-7b"); whoAnswered(t, body) != "good" {
			t.Fatalf("routed to %s", body)
		}
	}
	if draining.hits.Load() != 0 || wrong.hits.Load() != 0 {
		t.Fatalf("an ineligible or wrong-model worker was contacted: %d %d", draining.hits.Load(), wrong.hits.Load())
	}
}

func TestUnknownModelIsA404AndNeverBecomesAMetricLabel(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	resp, body := e.chat(t, "attacker-chosen-model-name")
	if resp.StatusCode != 404 || errorCode(t, body) != "MODEL_NOT_FOUND" {
		t.Fatalf("got %d %s", resp.StatusCode, body)
	}
	if a.hits.Load() != 0 {
		t.Fatal("no worker may see a request for a model it does not serve")
	}
	metrics := scrape(t, e.gw)
	if strings.Contains(metrics, "attacker-chosen-model-name") {
		t.Fatal("client-chosen model names must not become Prometheus labels (unbounded cardinality)")
	}
	if !strings.Contains(metrics, `model="unknown"`) {
		t.Fatal("an unverified model is recorded as unknown")
	}
}

func TestNoCapacityIsA503WithRetryAfterAndTheSpecDetail(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4)
	sa.State, sa.Eligible = protocol.StateDraining, false
	sb.State, sb.Eligible, sb.Health = protocol.StateUnhealthy, false, protocol.HealthUnhealthy
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sa, sb})
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("got %d %v %s", resp.StatusCode, resp.Header, body)
	}
	var v struct {
		Error struct {
			Model           string
			EligibleWorkers *int `json:"eligible_workers"`
		}
	}
	_ = json.Unmarshal([]byte(body), &v)
	if v.Error.Model != "qwen-7b" || v.Error.EligibleWorkers == nil || *v.Error.EligibleWorkers != 0 {
		t.Fatalf("detail: %s", body)
	}
	metrics := scrape(t, e.gw)
	if !strings.Contains(metrics, `model="qwen-7b"`) {
		t.Fatal("a confirmed model is a safe label")
	}
}

func TestFailsClosedWithoutAUsableSnapshot(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", nil)
	e.lister.set(nil, errors.New("connection refused"))
	if err := e.gw.router.cache.Refresh(context.Background()); err == nil {
		t.Fatal("the refresh should fail")
	}
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("no snapshot yet: %d %s", resp.StatusCode, body)
	}
	if r, _ := e.get(t, "/readyz"); r.StatusCode != 503 {
		t.Fatalf("not ready without a snapshot: %d", r.StatusCode)
	}
	if r, _ := e.get(t, "/v1/models"); r.StatusCode != 503 {
		t.Fatalf("models unavailable without a snapshot: %d", r.StatusCode)
	}

	// Once a snapshot exists traffic flows; once it is too old the gateway fails closed again.
	e.lister.set([]protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)}, nil)
	e.refresh(t)
	if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("with a snapshot: %d %s", resp.StatusCode, body)
	}
	e.lister.set(nil, errors.New("control plane down"))
	e.clock.Advance(4 * time.Second) // within the bound and before the suspect threshold
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("the gateway keeps serving from a recent snapshot during an outage: %d", resp.StatusCode)
	}
	hitsBefore := a.hits.Load()
	e.clock.Advance(7 * time.Second) // 11s: past the 10s staleness bound
	resp, body = e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("past the staleness bound the gateway must fail closed: %d %s", resp.StatusCode, body)
	}
	if a.hits.Load() != hitsBefore {
		t.Fatal("no request may reach a worker on an untrusted snapshot")
	}
	// The control plane returns and traffic resumes.
	e.lister.set([]protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)}, nil)
	e.refresh(t)
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("recovery: %d", resp.StatusCode)
	}
}

func TestAWorkerThatWouldHaveTurnedSuspectIsNotUsedFromAStaleSnapshot(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4)
	sa.HeartbeatAgeSeconds = 3 // already 3s since its last heartbeat when the snapshot was taken
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sa, sb})
	e.clock.Advance(2 * time.Second) // a is now 5s old: suspect. b too, in fact.
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 200 || whoAnswered(t, body) != "b" {
		t.Fatalf("only b (2s old) is still trusted: %d %s", resp.StatusCode, body)
	}
	e.clock.Advance(3 * time.Second)
	if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" {
		t.Fatalf("now both are suspect: %d %s", resp.StatusCode, body)
	}
}

func TestInFlightOverlayStopsABurstPilingOntoOneWorkerAndEnforcesCapacity(t *testing.T) {
	hold := make(chan struct{})
	a, b := newFakeWorker(t, "a", hold), newFakeWorker(t, "b", hold)
	// Both report zero active requests (a stale report); each can take 2.
	e := newRegEnv(t, "least-active", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 2), b.snapshot("qwen-7b", 2)})

	var wg sync.WaitGroup
	codes := make(chan int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _ := e.chat(t, "qwen-7b")
			codes <- resp.StatusCode
		}()
	}
	eventually(t, 5*time.Second, func() bool { return a.hits.Load() == 2 && b.hits.Load() == 2 }, "the burst should split 2/2 across the workers")
	if e.gw.router.InFlight("a") != 2 || e.gw.router.InFlight("b") != 2 {
		t.Fatalf("in flight: %d %d", e.gw.router.InFlight("a"), e.gw.router.InFlight("b"))
	}
	// Every slot is taken: a fifth request is turned away rather than queued blindly.
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" {
		t.Fatalf("a full pool must say so: %d %s", resp.StatusCode, body)
	}
	close(hold)
	wg.Wait()
	close(codes)
	for c := range codes {
		if c != 200 {
			t.Fatalf("a held request failed with %d", c)
		}
	}
	eventually(t, 5*time.Second, func() bool { return e.gw.router.InFlight("a") == 0 && e.gw.router.InFlight("b") == 0 }, "in-flight counters must return to zero")
	if n := len(e.gw.router.inflight); n != 0 {
		t.Fatalf("the counter map must not keep idle workers: %d", n)
	}
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("capacity is back after the burst: %d", resp.StatusCode)
	}
}

func TestInFlightUsesTheLargerOfReportedAndLocalNotTheSum(t *testing.T) {
	hold := make(chan struct{})
	a := newFakeWorker(t, "a", hold)
	s := a.snapshot("qwen-7b", 2)
	s.Metrics.ActiveRequests = 1 // the worker already reports one request (it may be ours)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{s})
	done := make(chan int, 1)
	go func() { resp, _ := e.chat(t, "qwen-7b"); done <- resp.StatusCode }()
	eventually(t, 5*time.Second, func() bool { return a.hits.Load() == 1 }, "first request admitted (reported 1 < max 2)")
	// local 1, reported 1: effective 1, still room. Summing would say 2 and refuse.
	go func() { resp, _ := e.chat(t, "qwen-7b"); done <- resp.StatusCode }()
	eventually(t, 5*time.Second, func() bool { return a.hits.Load() == 2 }, "second request admitted: max(1,1) is not 2")
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 503 {
		t.Fatalf("local 2 reaches the limit: %d", resp.StatusCode)
	}
	close(hold)
	<-done
	<-done
}

func TestTheSlotIsReleasedWhenTheWorkerFailsOrTheClientLeaves(t *testing.T) {
	dead := newFakeWorker(t, "dead", nil)
	snap := dead.snapshot("qwen-7b", 1)
	dead.srv.Close() // connection refused
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{snap})
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("a dead worker: %d %s", resp.StatusCode, body)
	}
	if e.gw.router.InFlight("dead") != 0 {
		t.Fatal("a failed request must give its slot back (capacity is 1, so a leak would lock the worker out)")
	}
	if !strings.Contains(e.logs.String(), "worker request failed") {
		t.Fatal("the failure must be logged")
	}

	hold := make(chan struct{})
	live := newFakeWorker(t, "live", hold)
	e2 := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{live.snapshot("qwen-7b", 1)})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e2.url+chatCompletionsPath,
		strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	errc := make(chan error, 1)
	go func() { _, err := e2.client.Do(req); errc <- err }()
	eventually(t, 5*time.Second, func() bool { return e2.gw.router.InFlight("live") == 1 }, "slot taken")
	cancel()
	<-errc
	eventually(t, 5*time.Second, func() bool { return e2.gw.router.InFlight("live") == 0 }, "a client disconnect must release the slot")
	close(hold)
}

func TestTheMetadataAddressIsNeverDialedEvenIfARegistryNamesIt(t *testing.T) {
	bad := snapWorker("evil", "qwen-7b", 0)
	bad.Address = "http://169.254.169.254"
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad})
	start := time.Now()
	resp, body := e.chat(t, "qwen-7b")
	// The snapshot check refuses the address before it is ever selected (NO_CAPACITY); the dial
	// guard is the second line (WORKER_UNAVAILABLE) and is exercised with names in dialguard_test.go.
	if code := errorCode(t, body); resp.StatusCode != 503 || (code != "NO_CAPACITY" && code != "WORKER_UNAVAILABLE") {
		t.Fatalf("got %d %s", resp.StatusCode, body)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("a forbidden address must be refused at once, not timed out: %v", time.Since(start))
	}
	if e.gw.router.InFlight("evil") != 0 {
		t.Fatal("slot leaked")
	}
	if strings.Contains(body, "169.254") {
		t.Fatalf("a worker-supplied address must never reach the client: %s", body)
	}

}

func TestWorkerNetworksRestrictWhereRequestsGo(t *testing.T) {
	a := newFakeWorker(t, "a", nil) // listens on 127.0.0.1
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)}, func(c *config.GatewayConfig) {
		c.WorkerNetworks = []string{"10.0.0.0/8"}
	})
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || a.hits.Load() != 0 {
		t.Fatalf("a worker outside worker_networks must not be contacted: %d %s hits=%d", resp.StatusCode, body, a.hits.Load())
	}
}

func TestClientCredentialsAreNotForwardedToWorkers(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	if resp, _ := e.chat(t, "qwen-7b", "Authorization", "Bearer client-secret"); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got, _ := a.lastAuth.Load().(string); got != "" {
		t.Fatalf("the Authorization header reached a worker: %q", got)
	}
}

func TestAWorkerRedirectIsNotFollowed(t *testing.T) {
	target := newFakeWorker(t, "target", nil)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.srv.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)
	s := snapWorker("redir", "qwen-7b", 0)
	s.Address, s.MaxConcurrency = redirector.URL, 4
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{s})
	resp, _ := e.chat(t, "qwen-7b")
	if resp.StatusCode != http.StatusBadGateway || target.hits.Load() != 0 {
		t.Fatalf("a redirecting worker is a failure, never followed: %d hits=%d", resp.StatusCode, target.hits.Load())
	}
}

func TestModelsAndReadinessFollowTheRegistry(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sb := b.snapshot("llama", 4)
	sb.State, sb.Eligible = protocol.StateDraining, false
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4), sb})
	resp, body := e.get(t, "/v1/models")
	if resp.StatusCode != 200 || !strings.Contains(body, "qwen-7b") || strings.Contains(body, "llama") {
		t.Fatalf("only models with an eligible worker are listed: %d %s", resp.StatusCode, body)
	}
	if r, _ := e.get(t, "/readyz"); r.StatusCode != 200 {
		t.Fatalf("ready with an eligible worker: %d", r.StatusCode)
	}
	e.lister.set([]protocol.WorkerSnapshot{sb}, nil)
	e.refresh(t)
	e.clock.Advance(3 * time.Second) // past the readiness cache
	e.gw.ready.mu.Lock()
	e.gw.ready.checked = time.Time{}
	e.gw.ready.mu.Unlock()
	if r, _ := e.get(t, "/readyz"); r.StatusCode != 503 {
		t.Fatalf("not ready when nothing is eligible: %d", r.StatusCode)
	}
}

func TestRegistryModeRejectsUnusableSetup(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1")
	for name, run := range map[string]func() error{
		"unimplemented strategy": func() error {
			_, err := newRegistryServer(cfg, "least-work", time.Second, &fakeLister{}, slog.Default())
			return err
		},
		"bad cidr": func() error {
			c := cfg
			c.WorkerNetworks = []string{"nope"}
			_, err := newRegistryServer(c, "random", time.Second, &fakeLister{}, slog.Default())
			return err
		},
	} {
		if run() == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}

func TestServeRunsTheRefresherAndStopsIt(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	lister := &fakeLister{workers: []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)}}
	cfg := testConfig("http://127.0.0.1:1")
	cfg.RegistryRefresh, cfg.RegistryMaxStaleness = 20*time.Millisecond, time.Second
	gw, err := newRegistryServer(cfg, "round-robin", 5*time.Second, lister, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := newLoopbackListener()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	eventually(t, 5*time.Second, func() bool { _, ok := gw.router.cache.Fresh(); return ok }, "Serve must start the refresher")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop")
	}
	lister.mu.Lock()
	calls := lister.calls
	lister.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	lister.mu.Lock()
	defer lister.mu.Unlock()
	if lister.calls != calls {
		t.Fatal("the refresher must stop when Serve returns")
	}
}

func newLoopbackListener() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

func BenchmarkRoute(b *testing.B) {
	for _, n := range []int{3, 50, 1000} {
		b.Run("w"+strconv.Itoa(n), func(b *testing.B) {
			var ws []protocol.WorkerSnapshot
			for i := 0; i < n; i++ {
				s := snapWorker("w"+strconv.Itoa(i), "m", 0)
				s.Address, s.MaxConcurrency = "http://127.0.0.1:1", 1<<30
				ws = append(ws, s)
			}
			lister := &fakeLister{workers: ws}
			gw, err := newRegistryServer(testConfig("http://127.0.0.1:1"), "least-active", 5*time.Second, lister, slog.New(slog.NewJSONHandler(io.Discard, nil)))
			if err != nil {
				b.Fatal(err)
			}
			if err := gw.router.cache.Refresh(context.Background()); err != nil {
				b.Fatal(err)
			}
			req := &protocol.InferenceRequest{Model: "m"}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					rt, apiErr := gw.router.Route(context.Background(), req)
					if apiErr != nil {
						b.Fatal(apiErr)
					}
					rt.release()
				}
			})
		})
	}
}

// --- review findings ---------------------------------------------------------------------------

type panicScheduler struct{}

func (panicScheduler) SelectWorker(context.Context, *protocol.InferenceRequest, []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	panic("boom")
}

func TestAPanickingSchedulerDoesNotWedgeTheRouter(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	e.gw.router.sched = panicScheduler{}
	func() {
		defer func() { _ = recover() }()
		_, _ = e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"})
	}()
	done := make(chan struct{})
	go func() { _ = e.gw.router.InFlight("a"); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the router lock was left held after a scheduler panic")
	}
}

func TestAFullPoolReportsHowManyWorkersAreEligibleButFull(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 2), b.snapshot("qwen-7b", 2)
	sa.Metrics.ActiveRequests, sb.Metrics.ActiveRequests = 2, 2
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sa, sb})
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	var v struct {
		Error struct {
			EligibleWorkers *int `json:"eligible_workers"`
		}
	}
	_ = json.Unmarshal([]byte(body), &v)
	if v.Error.EligibleWorkers == nil || *v.Error.EligibleWorkers != 2 {
		t.Fatalf("two workers are eligible but full, and the body should say so: %s", body)
	}
}

func TestAnOutageIsReportedAsUnavailableNotAsAShortageOfCapacity(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	e.lister.set(nil, errors.New("control plane down"))
	_ = e.gw.router.cache.Refresh(context.Background()) // the refresh fails, so the view is degraded
	e.clock.Advance(6 * time.Second)                    // past the suspect threshold, inside the staleness bound
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" || resp.Header.Get("Retry-After") != "" {
		t.Fatalf("a registry outage must not look like NO_CAPACITY: %d %v %s", resp.StatusCode, resp.Header, body)
	}
	// With a healthy registry the same shortage is a capacity problem.
	e.lister.set([]protocol.WorkerSnapshot{func() protocol.WorkerSnapshot {
		s := a.snapshot("qwen-7b", 4)
		s.State, s.Eligible = protocol.StateDraining, false
		return s
	}()}, nil)
	e.refresh(t)
	if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" {
		t.Fatalf("with a working registry: %d %s", resp.StatusCode, body)
	}
}

func TestTheSlotIsReleasedWhenAWorkerResponseIsCutShort(t *testing.T) {
	cut := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, `{"par`)
	}))
	t.Cleanup(cut.Close)
	s := snapWorker("cut", "qwen-7b", 0)
	s.Address, s.MaxConcurrency = cut.URL, 1
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{s})
	req, _ := http.NewRequest(http.MethodPost, e.url+chatCompletionsPath,
		strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	if resp, err := e.client.Do(req); err == nil {
		_, err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err == nil {
			t.Fatal("a truncated worker response must reach the client as a failure")
		}
	}
	eventually(t, 5*time.Second, func() bool { return e.gw.router.InFlight("cut") == 0 }, "an aborted response must still release its slot")
}

func TestWorkerResponsesAreMarkedNosniff(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	resp, _ := e.chat(t, "qwen-7b")
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("worker-controlled content types must not be sniffed: %v", resp.Header)
	}
}

func TestAWorkerCannotLoopRequestsBackThroughTheGateway(t *testing.T) {
	lister := &fakeLister{}
	cfg := testConfig("http://127.0.0.1:1")
	cfg.RegistryRefresh, cfg.RegistryMaxStaleness = 20*time.Millisecond, time.Second
	logs := &lockedBuffer{}
	gw, err := newRegistryServer(cfg, "round-robin", 5*time.Second, lister, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := newLoopbackListener()
	self := snapWorker("me", "qwen-7b", 0)
	self.Address, self.MaxConcurrency = "http://"+ln.Addr().String(), 8 // the gateway's own address
	lister.set([]protocol.WorkerSnapshot{self}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	eventually(t, 5*time.Second, func() bool { _, ok := gw.router.cache.Fresh(); return ok }, "snapshot")

	body := `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`
	resp, err := http.Post("http://"+ln.Addr().String()+chatCompletionsPath, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || errorCode(t, string(b)) != "WORKER_UNAVAILABLE" {
		t.Fatalf("a request must not be routed back into the gateway: %d %s", resp.StatusCode, b)
	}
	if gw.router.InFlight("me") != 0 {
		t.Fatal("slot leaked")
	}
}

func TestStartupWarnsWhenWorkerNetworksIsEmptyAndTheControlPlaneIsRemote(t *testing.T) {
	for url, wantWarn := range map[string]bool{"http://127.0.0.1:9090": false, "https://cp.internal:9090": true} {
		cfg := config.Default()
		cfg.Gateway.WorkerSource, cfg.Gateway.ControlPlaneURL = config.WorkerSourceRegistry, url
		logs := &lockedBuffer{}
		if _, err := NewRegistry(cfg, slog.New(slog.NewJSONHandler(logs, nil))); err != nil {
			t.Fatal(err)
		}
		out := logs.String()
		if !strings.Contains(out, "worker_networks is empty") || strings.Contains(out, `"level":"WARN"`) != wantWarn {
			t.Errorf("%s: warn=%v, log: %s", url, wantWarn, out)
		}
	}
	cfg := config.Default()
	cfg.Gateway.WorkerSource, cfg.Gateway.WorkerNetworks = config.WorkerSourceRegistry, []string{"10.0.0.0/8"}
	logs := &lockedBuffer{}
	_, _ = NewRegistry(cfg, slog.New(slog.NewJSONHandler(logs, nil)))
	if strings.Contains(logs.String(), "worker_networks is empty") {
		t.Error("no warning when worker_networks is set")
	}
}

// --- second verification ------------------------------------------------------------------------

func TestTheOverlayTakesTheLargerOfReportedAndLocalNotTheirSum(t *testing.T) {
	hold := make(chan struct{})
	a := newFakeWorker(t, "a", hold)
	s := a.snapshot("qwen-7b", 3)
	s.Metrics.ActiveRequests = 1
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{s})
	done := make(chan int, 3)
	for i := 1; i <= 3; i++ {
		go func() { resp, _ := e.chat(t, "qwen-7b"); done <- resp.StatusCode }()
		want := int64(i)
		eventually(t, 5*time.Second, func() bool { return a.hits.Load() == want }, "request admitted")
	}
	// Three held (local 3, reported 1): effective 3 reaches the limit. A sum (1+2=3) would have refused the third.
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 503 {
		t.Fatalf("the fourth request must be refused: %d", resp.StatusCode)
	}
	close(hold)
	for i := 0; i < 3; i++ {
		<-done
	}
}

func TestRouteWithACancelledContextIsAnInternalErrorNotACapacityAnswer(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rt, apiErr := e.gw.router.Route(ctx, &protocol.InferenceRequest{Model: "qwen-7b"})
	if rt != nil || apiErr == nil || apiErr.Code != "INTERNAL_ERROR" {
		t.Fatalf("got %v %v", rt, apiErr)
	}
	if e.gw.router.InFlight("a") != 0 {
		t.Fatal("no slot may be taken for a cancelled request")
	}
}

func TestWorkerAddressesWithTrailingSlashesStillWork(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	s := a.snapshot("qwen-7b", 4)
	s.Address += "//"
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{s})
	if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestSnapshotsWithUnbelievableNumbersOrAddressesAreNeverRoutedTo(t *testing.T) {
	good := newFakeWorker(t, "good", nil)
	var bad []protocol.WorkerSnapshot
	mk := func(id string, mutate func(*protocol.WorkerSnapshot)) {
		w := newFakeWorker(t, id, nil)
		s := w.snapshot("qwen-7b", 4)
		mutate(&s)
		bad = append(bad, s)
		t.Cleanup(func() {
			if w.hits.Load() != 0 {
				t.Errorf("%s was contacted", id)
			}
		})
	}
	mk("neg-age", func(s *protocol.WorkerSnapshot) { s.HeartbeatAgeSeconds = -1e9 })
	mk("huge-age", func(s *protocol.WorkerSnapshot) { s.HeartbeatAgeSeconds = 1e300 })
	mk("neg-active", func(s *protocol.WorkerSnapshot) { s.Metrics.ActiveRequests = -1000000000 })
	mk("neg-queue", func(s *protocol.WorkerSnapshot) { s.Metrics.QueueDepth = -5 })
	mk("zero-conc", func(s *protocol.WorkerSnapshot) { s.MaxConcurrency = 0 })
	mk("userinfo", func(s *protocol.WorkerSnapshot) {
		s.Address = strings.Replace(s.Address, "http://", "http://user:pw@", 1)
	})
	mk("query", func(s *protocol.WorkerSnapshot) { s.Address += "/p?x=1" })
	mk("unhealthy-but-eligible", func(s *protocol.WorkerSnapshot) { s.Health = protocol.HealthUnhealthy })
	e := newRegEnv(t, "least-active", append(bad, good.snapshot("qwen-7b", 4)))
	for i := 0; i < 20; i++ {
		if _, body := e.chat(t, "qwen-7b"); whoAnswered(t, body) != "good" {
			t.Fatalf("routed to %s", body)
		}
	}
	nan := math.NaN()
	if believable(&protocol.WorkerSnapshot{HeartbeatAgeSeconds: nan, MaxConcurrency: 1, Address: "http://127.0.0.1:1"}) {
		t.Fatal("NaN age is not believable")
	}
}

func TestTheWorkerTransportNeverUsesProxiesOrCompression(t *testing.T) {
	p, _ := NewAddressPolicy(nil)
	tr := newWorkerTransport(p, 3*time.Second)
	if tr.Proxy != nil || !tr.DisableCompression || tr.MaxIdleConnsPerHost != 256 || tr.ResponseHeaderTimeout != 3*time.Second || tr.DialContext == nil {
		t.Fatalf("unexpected transport settings: %+v", tr)
	}
	d := newWorkerDialer(p, nil)
	if d.Timeout != 5*time.Second || d.KeepAlive <= 0 || d.Control == nil {
		t.Fatalf("the dial timeout bounds how long a blackholed address holds a slot, and Control is the guard: %+v", d)
	}
}

func TestAGenuineShortageDuringAMomentaryRefreshFailureIsStillNoCapacity(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	s := a.snapshot("qwen-7b", 1)
	s.Metrics.ActiveRequests = 1 // full
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{s})
	e.lister.set(nil, errors.New("blip"))
	_ = e.gw.router.cache.Refresh(context.Background())
	resp, body := e.chat(t, "qwen-7b")
	if resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("a full pool seen through a fresh snapshot is capacity, even after one failed refresh: %d %v %s", resp.StatusCode, resp.Header, body)
	}
}

func TestAClientThatLeavesWhileAWorkerIsBeingChosenIsRecordedAsClosedNotAnError(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, chatCompletionsPath, strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.gw.Handler().ServeHTTP(rec, req)
	if rec.Body.Len() != 0 {
		t.Fatalf("nothing is written to a client that left: %q", rec.Body.String())
	}
	var found bool
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "request" && l["status"] == float64(statusClientClosed) {
			found = true
		}
	}
	if !found {
		t.Fatalf("the request must be logged as client-closed (499):\n%s", e.logs.String())
	}
	if a.hits.Load() != 0 || e.gw.router.InFlight("a") != 0 {
		t.Fatal("no worker contact and no slot for a request nobody is waiting for")
	}
	if strings.Contains(e.logs.String(), "no worker selected") {
		t.Fatal("a departed client is not a worker-selection failure")
	}
}

func TestUnknownModelRejectionsAreDebugLevelAndOthersInfo(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	sa := a.snapshot("qwen-7b", 4)
	sa.State, sa.Eligible = protocol.StateDraining, false
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sa})
	e.chat(t, "typo-model")
	e.chat(t, "qwen-7b")
	levels := map[string]string{}
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "no worker selected" {
			levels[l["error_code"].(string)] = l["level"].(string)
		}
	}
	if levels["MODEL_NOT_FOUND"] != "DEBUG" || levels["NO_CAPACITY"] != "INFO" {
		t.Fatalf("a client can pick the model name, so typos must not be loud: %v", levels)
	}
}

// --- Phase 6: exclusion --------------------------------------------------------------------------

func TestRouteNeverChoosesAnExcludedWorker(t *testing.T) {
	a, b, c := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil), newFakeWorker(t, "c", nil)
	for _, strategy := range []string{"random", "round-robin", "least-active", "least-queue"} {
		e := newRegEnv(t, strategy, []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4), c.snapshot("qwen-7b", 4)})
		for i := 0; i < 60; i++ {
			rt, apiErr := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"}, "a", "c")
			if apiErr != nil || rt.worker.WorkerID != "b" {
				t.Fatalf("%s: got %v %v, only b is allowed", strategy, rt, apiErr)
			}
			rt.release()
		}
	}
}

func TestExcludingEverythingIsNoCapacityNotModelNotFound(t *testing.T) {
	a := newFakeWorker(t, "a", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
	rt, apiErr := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"}, "a")
	if rt != nil || apiErr == nil || apiErr.Code != api.CodeNoCapacity {
		t.Fatalf("got %v %v", rt, apiErr)
	}
	if e.gw.router.InFlight("a") != 0 {
		t.Fatal("no slot may be taken")
	}
	// An unknown model is still unknown, with or without exclusions.
	if _, apiErr := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "nope"}, "a"); apiErr == nil || apiErr.Code != api.CodeModelNotFound {
		t.Fatalf("got %v", apiErr)
	}
	// Excluding a worker that is not there changes nothing.
	rt, apiErr = e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"}, "ghost")
	if apiErr != nil || rt.worker.WorkerID != "a" {
		t.Fatalf("got %v %v", rt, apiErr)
	}
	rt.release()
}

func TestExclusionDoesNotCorruptTheSnapshot(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4)})
	if rt, apiErr := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"}, "a"); apiErr != nil {
		t.Fatal(apiErr)
	} else {
		rt.release()
	}
	ws, _ := e.gw.router.cache.View("qwen-7b")
	if len(ws) != 2 || ws[0].WorkerID != "a" || ws[1].WorkerID != "b" {
		t.Fatalf("an exclusion for one request must not change what later requests see: %v", ws)
	}
}
