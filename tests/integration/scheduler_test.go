package integration

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
	"serverflow/internal/worker"
)

// countedNode is a mock worker behind a real agent, counting the chat requests it serves.
type countedNode struct {
	id    string
	mock  *httptest.Server
	chats atomic.Int64
	stop  context.CancelFunc // stops the agent without deregistering: a crash
}

func (c *controlPlane) startCountedNode(t *testing.T, id string) *countedNode {
	return c.startModelNode(t, id, model, nil)
}

// startModelNode starts a mock worker for a model behind a real agent. mutate may adjust the mock.
func (c *controlPlane) startModelNode(t *testing.T, id, modelName string, mutate func(*mockworker.Config)) *countedNode {
	t.Helper()
	mcfg := mockworker.DefaultConfig()
	mcfg.Model, mcfg.TTFT, mcfg.TokensPerSecond, mcfg.OutputTokens, mcfg.Seed = modelName, 2*time.Millisecond, 2000, 4, 1
	mcfg.MaxConcurrency, mcfg.QueueSize = 64, 64
	if mutate != nil {
		mutate(&mcfg)
	}
	inner := mockworker.New(mcfg, quiet()).Handler()
	n := &countedNode{id: id}
	n.mock = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat/completions") {
			n.chats.Add(1)
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(n.mock.Close)
	agent := worker.New(worker.Config{WorkerID: id, Model: modelName, AdvertiseURL: n.mock.URL, Interval: hbInterval},
		worker.NewMockBackend(n.mock.URL), c.cp, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	n.stop = cancel
	go func() { _ = agent.Run(ctx) }()
	t.Cleanup(cancel)
	return n
}

// startRegistryGateway runs a real gateway in registry mode against the control plane.
func startRegistryGateway(t *testing.T, c *controlPlane, strategy string, mutate ...func(*config.Config)) string {
	t.Helper()
	cfg := config.Default()
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Gateway.ControlPlaneURL = c.url
	cfg.Gateway.RegistryRefresh = 25 * time.Millisecond
	cfg.Gateway.RegistryMaxStaleness = time.Second
	cfg.Gateway.UpstreamHeaderTimeout = 5 * time.Second
	cfg.Worker.SuspectTimeout = suspectAfter
	cfg.ControlPlane.Token = testToken
	cfg.Scheduler.Strategy = strategy
	for _, m := range mutate {
		m(&cfg)
	}
	gw, err := gateway.NewRegistry(cfg, quiet())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	url := "http://" + ln.Addr().String()
	// /metrics is not on the data listener (ADR-017): serve it separately and remember where, by gateway URL.
	ms := httptest.NewServer(gw.MetricsHandler())
	t.Cleanup(ms.Close)
	gatewayMetrics.Store(url, ms.URL)
	return url
}

// gatewayMetrics maps a gateway's URL (from startRegistryGateway) to its metrics listener's URL.
var gatewayMetrics sync.Map

func metricsOf(gw string) string {
	v, _ := gatewayMetrics.Load(gw)
	s, _ := v.(string)
	return s
}

func gwPost(url string, stream bool) (int, string) {
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chat(stream, "hello")))
	if err != nil {
		return -1, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func waitReady(t *testing.T, gw string, models ...string) {
	t.Helper()
	waitFor(t, 5*time.Second, "the gateway sees an eligible worker", func() bool {
		resp, err := http.Get(gw + "/v1/models")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == 200 && strings.Contains(string(b), model)
	})
}

func TestTheGatewaySpreadsRequestsAcrossRegisteredWorkersByStrategy(t *testing.T) {
	c := startControlPlane(t)
	nodes := []*countedNode{c.startCountedNode(t, "w1"), c.startCountedNode(t, "w2"), c.startCountedNode(t, "w3")}
	gw := startRegistryGateway(t, c, "round-robin")
	waitFor(t, 5*time.Second, "all three eligible", func() bool {
		return c.eligible("w1") && c.eligible("w2") && c.eligible("w3")
	})
	waitReady(t, gw)
	// The cache may still hold a view with fewer workers; let it see all three.
	waitFor(t, 5*time.Second, "the gateway sees all three", func() bool {
		for _, n := range nodes {
			n.chats.Store(0)
		}
		for i := 0; i < 3; i++ {
			if code, _ := gwPost(gw, i%2 == 0); code != 200 {
				return false
			}
		}
		return nodes[0].chats.Load() == 1 && nodes[1].chats.Load() == 1 && nodes[2].chats.Load() == 1
	})
	for _, n := range nodes {
		n.chats.Store(0)
	}
	for i := 0; i < 30; i++ {
		if code, body := gwPost(gw, i%2 == 0); code != 200 {
			t.Fatalf("request %d: %d %s", i, code, body)
		}
	}
	for _, n := range nodes {
		if got := n.chats.Load(); got != 10 {
			t.Errorf("%s served %d of 30, want 10 (round-robin)", n.id, got)
		}
	}
}

func TestTheGatewayStopsUsingAWorkerWhoseBackendDies(t *testing.T) {
	c := startControlPlane(t)
	a, b := c.startCountedNode(t, "w1"), c.startCountedNode(t, "w2")
	gw := startRegistryGateway(t, c, "round-robin")
	waitFor(t, 5*time.Second, "both eligible", func() bool { return c.eligible("w1") && c.eligible("w2") })
	waitFor(t, 5*time.Second, "both receive traffic", func() bool {
		a.chats.Store(0)
		b.chats.Store(0)
		gwPost(gw, false)
		gwPost(gw, false)
		return a.chats.Load() > 0 && b.chats.Load() > 0
	})

	b.mock.CloseClientConnections()
	b.mock.Close() // the backend dies; its agent reports FAILED at once
	waitFor(t, 5*time.Second, "the registry marks w2 not eligible", func() bool { return !c.eligible("w2") })
	// One more refresh, then no request may be sent to w2.
	time.Sleep(150 * time.Millisecond)
	before := b.chats.Load()
	for i := 0; i < 30; i++ {
		if code, body := gwPost(gw, false); code != 200 {
			t.Fatalf("request %d failed with %d %s although w1 is healthy", i, code, body)
		}
	}
	if b.chats.Load() != before {
		t.Fatal("traffic still reached the dead worker")
	}
}

func TestTheGatewayStopsUsingAWorkerWhoseAgentCrashes(t *testing.T) {
	c := startControlPlane(t)
	a, b := c.startCountedNode(t, "w1"), c.startCountedNode(t, "w2")
	gw := startRegistryGateway(t, c, "round-robin")
	waitFor(t, 5*time.Second, "both eligible", func() bool { return c.eligible("w1") && c.eligible("w2") })
	waitFor(t, 5*time.Second, "both receive traffic", func() bool {
		a.chats.Store(0)
		b.chats.Store(0)
		gwPost(gw, false)
		gwPost(gw, false)
		return a.chats.Load() > 0 && b.chats.Load() > 0
	})

	b.stop() // the agent stops heartbeating but the worker itself is fine: nobody can vouch for it
	// Within the suspect threshold plus a refresh the gateway must stop trusting w2, whether or not
	// the registry has caught up (the cached heartbeat age keeps growing).
	waitFor(t, 3*time.Second, "w2 stops receiving traffic", func() bool {
		before := b.chats.Load()
		for i := 0; i < 4; i++ {
			gwPost(gw, false)
		}
		return b.chats.Load() == before
	})
	for i := 0; i < 20; i++ {
		if code, body := gwPost(gw, false); code != 200 {
			t.Fatalf("w1 is healthy but got %d %s", code, body)
		}
	}
}

func TestAControlPlaneOutageIsSurvivedBrieflyThenTheGatewayFailsClosedAndRecovers(t *testing.T) {
	c := startControlPlane(t)
	n := c.startCountedNode(t, "w1")
	gw := startRegistryGateway(t, c, "round-robin", func(cfg *config.Config) {
		cfg.Worker.SuspectTimeout = time.Second // trust a cached worker for up to a second of silence
		cfg.Gateway.RegistryMaxStaleness = 2 * time.Second
	})
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") })
	waitReady(t, gw)
	if code, body := gwPost(gw, false); code != 200 {
		t.Fatalf("baseline: %d %s", code, body)
	}

	c.outage.Store(true)
	time.Sleep(100 * time.Millisecond) // a few failed refreshes later, the cache still vouches for w1
	if code, body := gwPost(gw, false); code != 200 {
		t.Fatalf("a brief outage must not stop traffic: %d %s", code, body)
	}

	// Silence is not health: with no heartbeats getting through, the cached view must expire.
	waitFor(t, 5*time.Second, "the gateway fails closed", func() bool {
		code, _ := gwPost(gw, false)
		return code == 503
	})
	before := n.chats.Load()
	if code, body := gwPost(gw, false); code != 503 || !strings.Contains(body, "WORKER_UNAVAILABLE") && !strings.Contains(body, "NO_CAPACITY") {
		t.Fatalf("failing closed must be a clear 503: %d %s", code, body)
	}
	if n.chats.Load() != before {
		t.Fatal("a request reached a worker while failing closed")
	}

	c.outage.Store(false)
	waitFor(t, 8*time.Second, "traffic resumes after the control plane returns", func() bool {
		code, _ := gwPost(gw, false)
		return code == 200
	})
}

func TestAWorkerThatReregistersIsRoutedToAgain(t *testing.T) {
	c := startControlPlane(t)
	n := c.startCountedNode(t, "w1")
	gw := startRegistryGateway(t, c, "least-active")
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") })
	waitReady(t, gw)

	c.restart(t) // an empty registry: the agent notices and registers again
	waitFor(t, 8*time.Second, "w1 is routable again after the control plane restarted", func() bool {
		code, _ := gwPost(gw, true)
		return code == 200
	})
	if n.chats.Load() == 0 {
		t.Fatal("the worker never served anything")
	}
}
