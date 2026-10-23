package embedded

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/registry/client"
)

func TestPlanSimProfilesCycleAndAreChecked(t *testing.T) {
	specs, err := Plan(Config{Workers: 4, Models: []string{"m"}, SimProfiles: []string{"fast", "medium", "slow"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"fast", "medium", "slow", "fast"} {
		if specs[i].SimProfile != want {
			t.Errorf("worker %d runs %q, want %q", i, specs[i].SimProfile, want)
		}
	}
	plain, _ := Plan(Config{Workers: 2, Models: []string{"m"}})
	for _, w := range plain {
		if w.SimProfile != "" {
			t.Errorf("without SimProfiles a worker got %q", w.SimProfile)
		}
	}
	if _, err := Plan(Config{Workers: 2, Models: []string{"m"}, SimProfiles: []string{"fast", "nope"}}); err == nil {
		t.Error("an unknown simulator profile was accepted")
	}
}

// A cluster of simulator workers serves through the real gateway, and the GPU figures reach the control
// plane through the worker agent's heartbeat.
func TestSimClusterServesAndReportsGPUFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := Start(ctx, Config{Scheduler: "least-active", Workers: 2, Models: []string{"m"}, SimProfiles: []string{"fast", "slow"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(c.GatewayURL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi there"}],"max_tokens":300}`))
			if err != nil {
				t.Error(err)
				return
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || !strings.Contains(string(b), `"completion_tokens":300`) {
				t.Errorf("%d %s", resp.StatusCode, b)
			}
		}()
	}
	// While the requests run, a heartbeat should carry a non-nil GPU utilisation above zero.
	seen := false
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline) && !seen; time.Sleep(100 * time.Millisecond) {
		ws, err := c.ControlPlane().Workers(ctx, client.Query{})
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			if g := w.Metrics.GPUUtilization; g != nil && *g > 0 && w.Metrics.GPUMemoryUsedMB != nil {
				seen = true
			}
		}
	}
	wg.Wait()
	if !seen {
		t.Error("no heartbeat carried gpu_utilization above zero while requests were running")
	}
}
