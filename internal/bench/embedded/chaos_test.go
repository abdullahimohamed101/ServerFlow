package embedded

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/registry/client"
)

func startCluster(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if cfg.Scheduler == "" {
		cfg.Scheduler = "round-robin"
	}
	if cfg.Models == nil {
		cfg.Models = []string{"m"}
	}
	cfg.TTFT = time.Millisecond
	if cfg.TokensPerSecond == 0 {
		cfg.TokensPerSecond = 5000
	}
	c, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func chat(t *testing.T, base string, maxTokens int, stream bool) (*http.Response, error) {
	t.Helper()
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":` + itoa(maxTokens)
	if stream {
		body += `,"stream":true`
	}
	return http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(body+"}"))
}

func itoa(n int) string { return strconv.Itoa(n) }

func eligible(t *testing.T, c *Cluster) map[string]bool {
	t.Helper()
	ws, err := c.ControlPlane().Workers(context.Background(), client.Query{EligibleOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, w := range ws {
		out[w.WorkerID] = true
	}
	return out
}

func TestGatewayMutateReachesTheGatewayAndFaultsReachTheWorker(t *testing.T) {
	c := startCluster(t, Config{Workers: 1, GatewayMutate: func(g *config.Config) { g.Gateway.UpstreamHeaderTimeout = 400 * time.Millisecond }})
	f := c.Faults("worker-01")
	if f == nil || c.Faults("nope") != nil {
		t.Fatalf("Faults lookup: %v", f)
	}
	f.SetBlackhole(true)
	begin := time.Now()
	resp, err := chat(t, c.GatewayURL, 5, false)
	took := time.Since(begin)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Fatal("a blackholed worker answered 200")
		}
	}
	if took > 5*time.Second {
		t.Fatalf("took %v: the mutated 400ms header timeout did not apply (the default is 30s)", took)
	}
	if f.Stats().Blackholed == 0 {
		t.Fatal("the fault did not touch a request")
	}
	f.SetBlackhole(false)
}

func TestMetricsExposeTheGatewaySeries(t *testing.T) {
	c := startCluster(t, Config{Workers: 1})
	resp, err := chat(t, c.GatewayURL, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	fams, err := c.Metrics().Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range fams {
		found = found || f.GetName() == "scheduler_selections_total"
	}
	if !found {
		t.Fatal("scheduler_selections_total is not gathered")
	}
}

func TestDrainWorkerReallyDrains(t *testing.T) {
	c := startCluster(t, Config{Workers: 2, TokensPerSecond: 500, MockDrainAnnounce: 700 * time.Millisecond, MockDrainTimeout: 10 * time.Second})
	w1 := c.Specs()[0]
	// A stream in flight on the worker being drained, started directly so it is certain to be there.
	drained := make(chan error, 1)
	slow := slowStream(t, w1.URL)
	go func() { drained <- c.DrainWorker(context.Background(), "worker-01") }()

	// During the announce period the worker reports draining yet still admits work...
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, err := http.Get(w1.URL + "/readyz")
		if err == nil {
			_ = r.Body.Close()
			if r.StatusCode == 503 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the worker never reported draining")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if r, err := chat(t, w1.URL, 3, false); err != nil || r.StatusCode != 200 {
		t.Fatalf("during the announce period: %v %v", r, err)
	}
	// ...and the registry already lists only the other worker.
	if got := eligible(t, c); got["worker-01"] || !got["worker-02"] {
		t.Fatalf("eligible after the agent said goodbye: %v", got)
	}
	// Through the gateway every request is served by the survivor.
	for range 10 {
		r, err := chat(t, c.GatewayURL, 3, false)
		if err != nil || r.StatusCode != 200 {
			t.Fatalf("via the gateway: %v %v", r, err)
		}
		_ = r.Body.Close()
	}
	select {
	case err := <-drained:
		t.Fatalf("DrainWorker returned (%v) while a stream was still in flight", err)
	default:
	}
	// The in-flight stream finishes: the drain does not cut it.
	if !strings.Contains(slow(), "[DONE]") {
		t.Fatal("the in-flight stream was cut off by the drain")
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("a clean drain returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DrainWorker did not return")
	}
	if n := c.ServedWhileAnnouncing("worker-01"); n < 1 {
		t.Fatalf("served while announcing: %d", n)
	}
	if _, err := http.Get(w1.URL + "/stats"); err == nil {
		t.Fatal("the drained worker still accepts connections")
	}
	if err := c.DrainWorker(context.Background(), "worker-01"); err == nil {
		t.Fatal("draining a drained worker should be an error")
	}
	if err := c.DrainWorker(context.Background(), "nope"); err == nil {
		t.Fatal("draining an unknown worker should be an error")
	}
}

// slowStream starts a stream of 2000 tokens (4s at the 500 tokens/s the drain tests use) and returns a function that waits for its end.
func slowStream(t *testing.T, base string) func() string {
	t.Helper()
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":2000,"stream":true}`
	resp, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(resp.Body)
	_, _ = br.ReadString('\n')
	return func() string {
		defer func() { _ = resp.Body.Close() }()
		rest, _ := io.ReadAll(br)
		return string(rest)
	}
}

func TestDrainWorkerWithoutAnnounceRefusesAtOnce(t *testing.T) {
	c := startCluster(t, Config{Workers: 2, TokensPerSecond: 500, MockDrainTimeout: 10 * time.Second})
	w1 := c.Specs()[0]
	slow := slowStream(t, w1.URL)
	go func() { _ = c.DrainWorker(context.Background(), "worker-01") }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, err := chat(t, w1.URL, 3, false)
		if err == nil {
			code := r.StatusCode
			_ = r.Body.Close()
			if code == 503 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("with announce 0 the draining worker kept serving")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = slow()
}

func TestReplaceWorkerBringsADrainedWorkerBack(t *testing.T) {
	c := startCluster(t, Config{Workers: 2})
	old := c.Specs()[0].URL
	if err := c.ReplaceWorker(context.Background(), "worker-01"); err == nil {
		t.Fatal("replacing a worker that was not drained should be an error")
	}
	if err := c.DrainWorker(context.Background(), "worker-01"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReplaceWorker(context.Background(), "worker-01"); err != nil {
		t.Fatal(err)
	}
	if got := eligible(t, c); !got["worker-01"] || !got["worker-02"] {
		t.Fatalf("eligible: %v", got)
	}
	if now := c.Specs()[0].URL; now == old || now == "" {
		t.Fatalf("the replacement should listen elsewhere: %q -> %q", old, now)
	}
	if f := c.Faults("worker-01"); f == nil || f.Stats().Touched() != 0 {
		t.Fatal("the replacement is a fresh mock")
	}
	for range 6 {
		r, err := chat(t, c.GatewayURL, 3, false)
		if err != nil || r.StatusCode != 200 {
			t.Fatalf("after the replacement: %v %v", r, err)
		}
		_ = r.Body.Close()
	}
}
