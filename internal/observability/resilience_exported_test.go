package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
	"serverflow/internal/registry"
	"serverflow/internal/registry/server"
	"serverflow/internal/telemetry/metricsserver"
	"serverflow/pkg/protocol"
)

// resilienceFamilies are the series of Phase 15 (ADR-020) that the Resilience dashboard and the circuit and shedding
// alerts read. Each must be exported by a running gateway: the exported-by-a-running-server check (ADR-017 D14.3) would
// otherwise accept a dashboard panel on a series nothing produces, because most of them only appear after the event
// they count (a breaker opening, a shed, a queue wait).
var resilienceFamilies = map[string]string{
	"circuit_breaker_state":             "GAUGE",
	"circuit_breakers_open":             "GAUGE",
	"circuit_breaker_transitions_total": "COUNTER",
	"circuit_breaker_probes_total":      "COUNTER",
	"breaker_trips_suppressed_total":    "COUNTER",
	"retries_denied_total":              "COUNTER",
	"admission_queue_depth":             "GAUGE",
	"admission_rejections_total":        "COUNTER",
	"inference_queue_duration_seconds":  "HISTOGRAM",
	"gateway_draining":                  "GAUGE",
}

// runResilienceCluster runs a gateway with the shipped default resilience settings (on, registry mode; only the timings
// are shortened so the test is quick) in front of mock workers, makes every resilience event happen, and returns what the
// gateway exposes over HTTP. Model qwen-7b has one healthy worker and two that refuse every request (so a breaker opens,
// probes fail and the second trip is held back by the ejection cap, and with a retry budget of zero every retry is
// denied); model slow-model has one worker with one slot and a slow answer (so a second request waits and times out in the
// admission queue).
func runResilienceCluster(t *testing.T) (gw exported, body string) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg, err := registry.New(registry.Config{Suspect: time.Minute, Unhealthy: 2 * time.Minute, Lost: 3 * time.Minute, // the test registers once and never heartbeats again
		Retention: 10 * time.Minute, MaxWorkers: 10, HeartbeatInterval: 100 * time.Millisecond}, log)
	if err != nil {
		t.Fatal(err)
	}
	cpSrv := httptest.NewServer(server.New(reg, server.Config{Token: cpToken}, log).Handler())
	t.Cleanup(cpSrv.Close)

	start := func(id, model string, mutate func(*mockworker.Config)) string {
		cfg := mockworker.DefaultConfig()
		cfg.Model, cfg.WorkerID, cfg.TTFT, cfg.TokensPerSecond, cfg.OutputTokens, cfg.Seed = model, id, 5*time.Millisecond, 2000, 4, 1
		if mutate != nil {
			mutate(&cfg)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(mockworker.New(cfg, log).Handler())
		t.Cleanup(ts.Close)
		return ts.URL
	}
	refuse := func(c *mockworker.Config) { c.FailureRate, c.FailureMode = 1, mockworker.ModeUnavailable }
	register := func(id, model, url string, slots int) {
		rid, err := reg.Register(protocol.WorkerInfo{WorkerID: id, Model: model, Address: url, MaxConcurrency: slots, QueueSize: 8})
		if err != nil {
			t.Fatal(err)
		}
		if err := reg.Heartbeat(id, rid, protocol.Heartbeat{RegistrationID: rid, State: protocol.StateReady}); err != nil {
			t.Fatal(err)
		}
	}
	register("w-good", goodModel, start("w-good", goodModel, nil), 4)
	register("w-bad-1", goodModel, start("w-bad-1", goodModel, refuse), 4)
	register("w-bad-2", goodModel, start("w-bad-2", goodModel, refuse), 4)
	slowModel := "slow-model"
	register("w-slow", slowModel, start("w-slow", slowModel, func(c *mockworker.Config) { c.TTFT = 800 * time.Millisecond }), 1)

	cfg := config.Default() // resilience.mode is the shipped default: on
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Gateway.ControlPlaneURL, cfg.ControlPlane.Token = cpSrv.URL, cpToken
	cfg.Gateway.RegistryRefresh, cfg.Gateway.RegistryMaxStaleness = 25*time.Millisecond, 2*time.Second
	cfg.Worker.SuspectTimeout = time.Minute
	cfg.Scheduler.Strategy = "round-robin"
	cfg.Resilience.Breaker.CoolDown = 100 * time.Millisecond // the configuration's floor
	cfg.Resilience.Retry.BudgetRatio, cfg.Resilience.Retry.BudgetMin = 0, 0
	cfg.Resilience.Queue.MaxWait = 300 * time.Millisecond
	if !cfg.Resilience.Effective(cfg.Gateway.WorkerSource) {
		t.Fatal("the shipped default must make the feature effective in registry mode (plan D1)")
	}
	gws, err := gateway.NewRegistry(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gws.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	base := "http://" + ln.Addr().String()

	chat := func(model string) int {
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions",
			strings.NewReader(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello there"}]}`, model)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	ms, err := metricsserver.Listen("127.0.0.1:0", config.MetricsConfig{}, gws.MetricsGatherer(), log)
	if err != nil {
		t.Fatal(err)
	}
	mctx, mcancel := context.WithCancel(context.Background())
	go func() { _ = ms.Serve(mctx) }()
	t.Cleanup(mcancel)
	scrape := func() (string, exported) {
		resp := httpGet(t, "http://"+ms.Addr()+"/metrics", "")
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return string(raw), parse(t, strings.NewReader(string(raw)))
	}

	deadline := time.Now().Add(10 * time.Second)
	for { // the gateway needs a first snapshot
		if resp := httpGet(t, base+"/readyz", ""); resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			break
		} else {
			_ = resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("the gateway never became ready")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// The queue: one request holds the slow model's only slot for 0.8 s, the next waits 300 ms and is shed.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); chat(slowModel) }()
	time.Sleep(100 * time.Millisecond)
	if c := chat(slowModel); c != http.StatusServiceUnavailable {
		t.Fatalf("a request behind a full slot: status %d, want 503 after the queue wait", c)
	}
	wg.Wait()

	// The breakers: faults until one opens, then requests spread over time so probes reach the open breaker.
	for time.Now().Before(deadline) {
		chat(goodModel)
		time.Sleep(30 * time.Millisecond)
		body, fams := scrape()
		missing := 0
		for name := range resilienceFamilies {
			if _, ok := fams[name]; !ok {
				missing++
			}
		}
		if missing == 0 && strings.Contains(body, `circuit_breaker_probes_total{model="qwen-7b",result="fault"}`) &&
			strings.Contains(body, "breaker_trips_suppressed_total{") {
			return fams, body
		}
	}
	body, fams := scrape()
	t.Fatalf("not every resilience series was exported after the events:\n%s", grep(body, "breaker")+"\n"+grep(body, "admission")+"\n"+grep(body, "retries_denied")+
		"\n"+grep(body, "queue_duration")+fmt.Sprint(len(fams)))
	return nil, ""
}

// TestRunningGatewayExportsTheResilienceSeries pins the names, types and labels of the series the Resilience dashboard
// and the circuit-breaker and shedding alerts read, on a gateway that runs with the shipped defaults.
func TestRunningGatewayExportsTheResilienceSeries(t *testing.T) {
	gw, body := runResilienceCluster(t)
	for name, typ := range resilienceFamilies {
		if gw[name].Type != typ {
			t.Errorf("%s is %q, want %s", name, gw[name].Type, typ)
		}
	}
	for _, want := range []string{
		`circuit_breaker_transitions_total{model="qwen-7b",state="open"}`,
		`circuit_breaker_probes_total{model="qwen-7b",result="fault"}`,
		`retries_denied_total{model="qwen-7b",reason="budget"}`,
		`admission_rejections_total{model="slow-model",reason="queue_timeout"} 1`,
		`admission_queue_depth{model="slow-model"} 0`,
		`gateway_draining 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the gateway scrape lacks %s\n%s", want, grep(body, "breaker")+"\n"+grep(body, "admission")+"\n"+grep(body, "retries_denied"))
		}
	}
	// A breaker that is not closed has a series with the worker and the model; a closed one has none.
	if !strings.Contains(grep(body, "circuit_breaker_state{"), `model="qwen-7b"`) || !strings.Contains(grep(body, "circuit_breaker_state{"), `worker_id="w-bad-`) {
		t.Errorf("circuit_breaker_state has no series for an ejected worker:\n%s", grep(body, "circuit_breaker_state"))
	}
	if strings.Contains(grep(body, "circuit_breaker_state{"), `worker_id="w-good"`) {
		t.Errorf("a healthy worker must have no circuit_breaker_state series:\n%s", grep(body, "circuit_breaker_state"))
	}
	for _, l := range []string{"model", "reason"} {
		if !gw["admission_rejections_total"].Labels[l] {
			t.Errorf("admission_rejections_total lacks the label %s", l)
		}
	}
}
