package embedded

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPlanIdenticalWorkersAllMatch(t *testing.T) {
	ws, err := Plan(Config{Workers: 3, Models: []string{"m"}})
	if err != nil || len(ws) != 3 {
		t.Fatalf("%v %v", ws, err)
	}
	for i, w := range ws {
		if w.TokensPerSecond != DefaultTokensPerSecond || w.TTFT != DefaultTTFT || w.Model != "m" || w.ID != []string{"worker-01", "worker-02", "worker-03"}[i] {
			t.Fatalf("%+v", w)
		}
	}
}

func TestPlanHeterogeneousCyclesFastMediumSlow(t *testing.T) {
	ws, err := Plan(Config{Workers: 4, Models: []string{"m"}, Profile: Heterogeneous, TokensPerSecond: 100, TTFT: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	wantTPS := []float64{100, 60, 20, 100}
	wantTTFT := []time.Duration{100 * time.Millisecond, 150 * time.Millisecond, 300 * time.Millisecond, 100 * time.Millisecond}
	for i, w := range ws {
		if w.TokensPerSecond != wantTPS[i] || w.TTFT != wantTTFT[i] {
			t.Errorf("worker %d: %v tok/s %v", i, w.TokensPerSecond, w.TTFT)
		}
	}
}

func TestPlanGivesEveryModelAWorkerAndSecondariesAQuarter(t *testing.T) {
	count := func(n int, models ...string) map[string]int {
		ws, err := Plan(Config{Workers: n, Models: models})
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]int{}
		for _, w := range ws {
			out[w.Model]++
		}
		return out
	}
	for n, want := range map[int][2]int{2: {1, 1}, 3: {2, 1}, 4: {3, 1}, 8: {6, 2}, 9: {7, 2}, 64: {48, 16}} {
		got := count(n, "q", "l")
		if got["q"] != want[0] || got["l"] != want[1] {
			t.Errorf("%d workers: %v, want %v", n, got, want)
		}
	}
	if got := count(3, "a", "b", "c"); got["a"] != 1 || got["b"] != 1 || got["c"] != 1 {
		t.Errorf("%v", got)
	}
}

func TestPlanRejectsBadConfigs(t *testing.T) {
	ok := Config{Workers: 2, Models: []string{"a", "b"}}
	for name, mutate := range map[string]func(*Config){
		"no workers":                func(c *Config) { c.Workers = 0 },
		"too many":                  func(c *Config) { c.Workers = MaxWorkers + 1 },
		"no models":                 func(c *Config) { c.Models = nil },
		"fewer workers than models": func(c *Config) { c.Workers = 1 },
		"profile":                   func(c *Config) { c.Profile = "weird" },
		"duplicate model":           func(c *Config) { c.Models = []string{"a", "a"} },
		"empty model":               func(c *Config) { c.Models = []string{"a", ""} },
		"tps":                       func(c *Config) { c.TokensPerSecond = 2e6 },
		"ttft":                      func(c *Config) { c.TTFT = time.Hour },
		"concurrency":               func(c *Config) { c.MaxConcurrency = 5000 },
		"queue":                     func(c *Config) { c.QueueSize = -1 },
	} {
		c := ok
		mutate(&c)
		if _, err := Plan(c); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := Plan(ok); err != nil {
		t.Fatal(err)
	}
}

func TestStartBootsAWorkingClusterAndCloseStopsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := Start(ctx, Config{Scheduler: "round-robin", Workers: 3, Models: []string{"qwen-7b", "llama-8b"}, TTFT: time.Millisecond, TokensPerSecond: 5000})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"qwen-7b", "llama-8b"} {
		resp, err := http.Post(c.GatewayURL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"`+m+`","messages":[{"role":"user","content":"hi there"}],"max_tokens":5}`))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(b), `"completion_tokens":5`) {
			t.Fatalf("%s: %d %s", m, resp.StatusCode, b)
		}
	}
	if len(c.Workers) != 3 || c.Workers[0].URL == "" || c.Token == "" || c.ControlPlane() == nil {
		t.Fatalf("%+v", c)
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(20 * time.Second):
		t.Fatal("Close did not return")
	}
	if _, err := http.Get(c.GatewayURL + "/healthz"); err == nil {
		t.Fatal("the gateway should be down after Close")
	}
}

func TestStartRejectsUnknownSchedulerWithoutLeaking(t *testing.T) {
	if _, err := Start(context.Background(), Config{Scheduler: "nope", Workers: 1, Models: []string{"m"}}); err == nil {
		t.Fatal("want an error")
	}
}

// Start must not return before the gateway sees every worker: the first requests after it used to
// reach a gateway that knew one worker and got 503. Round-robin sends consecutive requests to
// different workers, so three requests made right after Start must reach three workers.
func TestTheGatewaySeesEveryWorkerTheMomentStartReturns(t *testing.T) {
	for n := 0; n < 4; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		c, err := Start(ctx, Config{Scheduler: "round-robin", Workers: 3, Models: []string{"m"}, TTFT: time.Millisecond, TokensPerSecond: 20000})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			resp, err := http.Post(c.GatewayURL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":2}`))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("start %d request %d: %d", n, i, resp.StatusCode)
			}
		}
		for _, w := range c.Workers {
			r, err := http.Get(w.URL + "/stats")
			if err != nil {
				t.Fatal(err)
			}
			var st struct{ Completed int64 }
			_ = json.NewDecoder(r.Body).Decode(&st)
			_ = r.Body.Close()
			if st.Completed != 1 {
				t.Fatalf("start %d: %s completed %d, want 1: the gateway did not know every worker yet", n, w.ID, st.Completed)
			}
		}
		c.Close()
		cancel()
	}
}
