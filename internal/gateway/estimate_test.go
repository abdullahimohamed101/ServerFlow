package gateway

import (
	"net/http"
	"strings"
	"testing"

	"serverflow/internal/cost"
	"serverflow/pkg/protocol"
)

// The handler estimates a request's cost once, whether or not a limiter is configured, and hands the two parts to
// the scheduler on the InferenceRequest (Phase 14 PR A, criterion 2). The limiter is charged exactly their sum.
func TestHandlerSetsTheCostEstimateWithAndWithoutALimiter(t *testing.T) {
	good := newScriptWorker(t, "w1", okJSON("w1"))
	bodies := map[string]string{
		"no max_tokens":      `{"model":"qwen-7b","messages":[{"role":"user","content":"hello world"}]}`,
		"max_tokens":         `{"model":"qwen-7b","messages":[{"role":"user","content":"hello world"}],"max_tokens":77}`,
		"non-ascii":          `{"model":"qwen-7b","messages":[{"role":"system","content":"être"},{"role":"user","content":"你好 世界"}],"max_tokens":5}`,
		"extra body fields":  `{"model":"qwen-7b","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"` + strings.Repeat("f", 900) + `"}}]}`,
		"max_tokens clamped": `{"model":"qwen-7b","messages":[{"role":"user","content":"x"}],"max_tokens":4096}`,
	}
	for _, withLimiter := range []bool{false, true} {
		name := "no limiter"
		if withLimiter {
			name = "limiter"
		}
		t.Run(name, func(t *testing.T) {
			env := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
			cs := &captureSched{Scheduler: env.gw.router.sched}
			env.gw.router.sched = cs
			lim := &fakeLimiter{}
			if withLimiter {
				env.gw.SetLimiter(lim)
			}
			limit := env.gw.cfg.MaxTokensLimit
			n := 0
			for label, body := range bodies {
				req, _ := http.NewRequest(http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				resp, err := env.client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("%s: status %d", label, resp.StatusCode)
				}
				n++
				cs.mu.Lock()
				got := cs.reqs[len(cs.reqs)-1]
				cs.mu.Unlock()
				in, out := cost.Estimate(got.Messages, got.Prompt, len(body), got.MaxTokens, limit)
				if got.EstInputTokens != in || got.EstOutputTokens != out || in <= 0 || out <= 0 {
					t.Fatalf("%s: request carries (%d, %d), want (%d, %d)", label, got.EstInputTokens, got.EstOutputTokens, in, out)
				}
				wantOut := got.MaxTokens
				if wantOut <= 0 {
					wantOut = limit
				}
				if out != wantOut {
					t.Fatalf("%s: output budget %d, want %d", label, out, wantOut)
				}
				if withLimiter {
					calls := lim.calls()
					if c := calls[len(calls)-1].Cost; int(c) != in+out || int(c) != cost.EstimateRequestCost(got.Messages, got.Prompt, len(body), got.MaxTokens, limit) {
						t.Fatalf("%s: the limiter was charged %d, estimate is %d + %d", label, c, in, out)
					}
				}
			}
			if withLimiter && len(lim.calls()) != n {
				t.Fatalf("limiter saw %d calls for %d requests", len(lim.calls()), n)
			}
		})
	}
}
