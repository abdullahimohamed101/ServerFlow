package gateway

import (
	"net/http"
	"strings"
	"sync/atomic"
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
	sentMaxTokens := map[string]int{"no max_tokens": 0, "max_tokens": 77, "non-ascii": 5, "extra body fields": 0, "max_tokens clamped": 4096}
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
				if sent := sentMaxTokens[label]; got.MaxTokens != sent {
					t.Fatalf("%s: the handler changed InferenceRequest.MaxTokens: client sent %d, scheduler got %d (0 must stay 'client set no limit')", label, sent, got.MaxTokens)
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

// The estimate is computed exactly once per request, with and without a limiter, and not at all for requests
// refused before it (bad JSON, unknown model).
func TestTheEstimatorRunsOncePerRequest(t *testing.T) {
	good := newScriptWorker(t, "w1", okJSON("w1"))
	for _, withLimiter := range []bool{false, true} {
		env := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
		var calls atomic.Int64
		env.gw.estimator = func(m []protocol.Message, p string, b, mt, l int) (int, int) {
			calls.Add(1)
			return cost.Estimate(m, p, b, mt, l)
		}
		if withLimiter {
			env.gw.SetLimiter(&fakeLimiter{})
		}
		for i := 1; i <= 3; i++ {
			resp, _ := env.chat(t, "qwen-7b")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d", resp.StatusCode)
			}
			if got := calls.Load(); got != int64(i) {
				t.Fatalf("limiter=%v: after %d requests the estimator ran %d times", withLimiter, i, got)
			}
		}
		before := calls.Load()
		env.chat(t, "no-such-model")
		if calls.Load() != before {
			t.Fatalf("limiter=%v: a request refused for its model must not be estimated", withLimiter)
		}
	}
}
