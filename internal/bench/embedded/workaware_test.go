package embedded

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
)

// least-tokens and least-work run end to end in the embedded cluster (Phase 14 PR C1): the harness switches the
// ledger overlay on for them, requests are served, and the gateway reports predictions. Which worker wins is a
// matter for the experiments (PR J), not for this test; this one proves the strategies run against real heartbeats,
// registry refreshes and mock workers without refusing or leaking.
func TestWorkAwareStrategiesServeTrafficInTheEmbeddedCluster(t *testing.T) {
	for _, tc := range []struct {
		strategy string
		profile  string
	}{{"least-tokens", Identical}, {"least-work", Heterogeneous}} {
		t.Run(tc.strategy, func(t *testing.T) {
			c := startCluster(t, Config{Workers: 3, MaxConcurrency: 8, Scheduler: tc.strategy, Profile: tc.profile})
			defer c.Close()
			hc := &http.Client{Timeout: 20 * time.Second}
			defer hc.CloseIdleConnections()
			var ok, bad atomic.Int64
			var wg sync.WaitGroup
			stop := time.Now().Add(2500 * time.Millisecond)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					for n := 0; time.Now().Before(stop); n++ {
						mt := []int{10, 40, 200}[(i+n)%3]
						resp, err := hc.Post(c.GatewayURL+"/v1/chat/completions", "application/json",
							strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hello there"}],"max_tokens":`+itoa(mt)+`}`))
						if err != nil {
							bad.Add(1)
							continue
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode == 200 {
							ok.Add(1)
						} else {
							bad.Add(1)
						}
					}
				}(i)
			}
			wg.Wait()
			if bad.Load() != 0 || ok.Load() < 20 {
				t.Fatalf("%d ok, %d failed", ok.Load(), bad.Load())
			}
			rec := httptest.NewRecorder()
			c.gw.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			text := rec.Body.String()
			if !strings.Contains(text, "scheduler_predicted_wait_seconds_count") {
				t.Fatalf("no predictions were observed:\n%s", text)
			}
			if strings.Contains(text, "scheduler_ineligible_selections_total 0") == false {
				t.Fatalf("an ineligible worker was selected:\n%s", text)
			}
		})
	}
}

// A work-aware strategy without the ledger overlay is refused when the gateway starts.
func TestWorkAwareStrategyNeedsTheLedgerEvenInTheHarness(t *testing.T) {
	_, err := Start(t.Context(), Config{Workers: 1, MaxConcurrency: 2, Scheduler: "least-work", Models: []string{"m"}, TTFT: time.Millisecond, TokensPerSecond: 1000,
		GatewayMutate: func(g *config.Config) { g.Scheduler.LoadOverlay = config.LoadOverlayMax }})
	if err == nil || !strings.Contains(err.Error(), "load_overlay") {
		t.Fatalf("want a load_overlay error, got %v", err)
	}
}
