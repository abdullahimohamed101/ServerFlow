package mockworker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/sim/engine"
)

// fastSim is a simulator world fast enough for unit tests: step 2ms+0.5ms*b, prefill 1ms+0.02ms/token.
func fastSim() engine.Params {
	return engine.Params{
		Name: "unit", Provenance: "assumed: test",
		StepBaseMs: 2, StepPerSeqMs: 0.5,
		PrefillFixedMs: 1, PrefillPerTokenMs: 0.02,
		KVBudgetTokens: 100000, KVBytesPerToken: 1 << 20, WeightsMB: 500,
	}
}

func simEnv(t *testing.T, mutate ...func(*Config)) *env {
	t.Helper()
	return newEnv(t, append([]func(*Config){func(c *Config) {
		p := fastSim()
		c.ModelEngine, c.Sim = ModelEngineSim, &p
		c.MaxConcurrency, c.QueueSize, c.OutputTokens = 8, 64, 20
	}}, mutate...)...)
}

func words(n int) string { return strings.TrimSpace(strings.Repeat("w ", n)) }

func TestSimStreamShapeMatchesTheDefaultEngine(t *testing.T) {
	e := simEnv(t)
	resp := e.mustPost(t, chatBody(true, "hello there", `,"max_tokens":5`))
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	events, err := readSSE(resp.Body, time.Now())
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	var text strings.Builder
	finish := ""
	for _, ev := range events {
		if ev.data == "[DONE]" {
			continue
		}
		var c struct {
			Choices []struct {
				Delta        struct{ Content *string }
				FinishReason *string `json:"finish_reason"`
			}
		}
		if err := json.Unmarshal([]byte(ev.data), &c); err != nil {
			t.Fatal(err)
		}
		if c.Choices[0].Delta.Content != nil {
			text.WriteString(*c.Choices[0].Delta.Content)
		}
		if c.Choices[0].FinishReason != nil {
			finish = *c.Choices[0].FinishReason
		}
	}
	if text.String() != "tok0 tok1 tok2 tok3 tok4 " || finish != "length" || events[len(events)-1].data != "[DONE]" {
		t.Fatalf("text %q finish %q last %q", text.String(), finish, events[len(events)-1].data)
	}
}

func TestSimNonStreamUsageAndNaturalStop(t *testing.T) {
	e := simEnv(t)
	resp := e.mustPost(t, chatBody(false, "one two three", ``))
	var body struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
		}
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Usage.PromptTokens != 3 || body.Usage.CompletionTokens != 20 || body.Usage.TotalTokens != 23 || body.Choices[0].FinishReason != "stop" {
		t.Fatalf("body = %+v", body)
	}
}

func timeOne(t *testing.T, e *env, prompt string, max string) time.Duration {
	t.Helper()
	start := time.Now()
	resp := e.mustPost(t, chatBody(false, prompt, max))
	_, _ = io.Copy(io.Discard, resp.Body)
	return time.Since(start)
}

func TestSimBatchingSlowsEachRequestAsTheBatchGrows(t *testing.T) {
	e := simEnv(t, func(c *Config) { c.OutputTokens = 40 })
	single := timeOne(t, e, "hi", "")
	var wg sync.WaitGroup
	times := make([]time.Duration, 8)
	for i := range times {
		wg.Add(1)
		go func() { defer wg.Done(); times[i] = timeOne(t, e, "hi", "") }()
	}
	wg.Wait()
	var sum time.Duration
	for _, d := range times {
		sum += d
	}
	mean := sum / 8
	// 40 steps at b=1 cost 2.5ms each, at b=8 cost 6ms: expect roughly 2.4x; require clearly more than 1.4x.
	if float64(mean) < 1.4*float64(single) {
		t.Errorf("mean latency at batch 8 is %v against %v alone: no contention shows", mean, single)
	}
}

func TestSimPrefillTimeGrowsWithPromptLength(t *testing.T) {
	e := simEnv(t)
	ttft := func(prompt string) time.Duration {
		start := time.Now()
		resp := e.mustPost(t, chatBody(true, prompt, `,"max_tokens":2`))
		br := make([]byte, 64)
		if _, err := resp.Body.Read(br); err != nil { // headers go out first; wait for a body byte
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return time.Since(start)
	}
	short, long := ttft("hi"), ttft(words(5000)) // 5000 words: 100ms of prefill
	if long < short+60*time.Millisecond {
		t.Errorf("a 5000-token prompt took %v against %v for a 1-token prompt; expected about 100ms more", long, short)
	}
}

func TestSimKVBudgetQueuesBeforeMaxConcurrency(t *testing.T) {
	e := simEnv(t, func(c *Config) {
		p := fastSim()
		p.KVBudgetTokens = 3000 // two 1200-token prompts fit with room to grow, three do not
		c.Sim, c.OutputTokens = &p, 400
	})
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() { defer wg.Done(); timeOne(t, e, words(1200), "") }()
	}
	eventually(t, 5*time.Second, func() bool {
		s := e.stats(t)
		return s.QueueDepth == 1 && s.QueuedInputTokens == 1200 && s.ActiveRequests == 2
	}, "the third request waiting for KV while only 2 of 8 slots are used")
	s := e.stats(t)
	if s.Sim == nil || s.Sim.KVUsedTokens <= 2400 || s.Sim.KVBudgetTokens != 3000 {
		t.Errorf("sim stats = %+v", s.Sim)
	}
	wg.Wait()
	if s := e.stats(t); s.Completed != 3 || s.ActiveRequests != 0 || s.QueueDepth != 0 {
		t.Errorf("final stats %+v", s)
	}
}

func TestSimStatsCarryGPUFieldsAndDefaultStatsDoNot(t *testing.T) {
	_, simBody := simEnv(t).get(t, "/stats")
	for _, key := range []string{`"gpu_utilization":`, `"gpu_memory_used_mb":`, `"sim":{"engine":"sim"`, `"params_hash":"`} {
		if !strings.Contains(simBody, key) {
			t.Errorf("sim /stats lacks %s: %s", key, simBody)
		}
	}
	_, def := newEnv(t).get(t, "/stats")
	for _, key := range []string{"gpu", "sim", "kv_"} {
		if strings.Contains(def, key) {
			t.Errorf("default /stats mentions %q: %s", key, def)
		}
	}
}

func TestSimQueueFullAndOversizedPrompt(t *testing.T) {
	e := simEnv(t, func(c *Config) {
		p := fastSim()
		p.KVBudgetTokens = 2000
		c.Sim, c.MaxConcurrency, c.QueueSize, c.OutputTokens = &p, 1, 0, 300
	})
	done := make(chan struct{})
	go func() { defer close(done); timeOne(t, e, "hi", "") }()
	eventually(t, 3*time.Second, func() bool { return e.stats(t).ActiveRequests == 1 }, "first request running")
	resp := e.mustPost(t, chatBody(false, "again", ""))
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || !strings.Contains(string(b), "queue_full") {
		t.Errorf("queue overflow: %d %s", resp.StatusCode, b)
	}
	resp = e.mustPost(t, chatBody(false, words(2500), ""))
	b, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "KV cache") {
		t.Errorf("oversized prompt: %d %s", resp.StatusCode, b)
	}
	<-done
	if s := e.stats(t); s.Rejected != 1 || s.Completed != 1 {
		t.Errorf("stats %+v", s)
	}
}

func TestSimClientCancelFreesTheEngine(t *testing.T) {
	before := runtime.NumGoroutine()
	e := simEnv(t, func(c *Config) { c.OutputTokens = 100000 })
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan struct{})
	go func() {
		defer close(got)
		resp, err := e.post(ctx, chatBody(true, "hi", `,"max_tokens":100000`))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	eventually(t, 3*time.Second, func() bool { return e.stats(t).ActiveRequests == 1 }, "request running")
	cancel()
	<-got
	eventually(t, 3*time.Second, func() bool {
		s := e.stats(t)
		return s.ActiveRequests == 0 && s.Cancelled == 1 && s.Sim.KVUsedTokens == 0
	}, "slot and KV released")
	e.ts.Close()
	eventually(t, 3*time.Second, func() bool { return runtime.NumGoroutine() <= before+2 }, "no goroutine left behind by the sim driver")
}

func TestSimInjectedFailuresStillApply(t *testing.T) {
	e := simEnv(t, func(c *Config) { c.FailureRate, c.FailureMode = 1, ModeError })
	resp := e.mustPost(t, chatBody(false, "hi", ""))
	if resp.StatusCode != 500 {
		t.Errorf("status %d", resp.StatusCode)
	}
	m := simEnv(t, func(c *Config) { c.FailureRate, c.FailureMode, c.OutputTokens = 1, ModeMidstream, 50 })
	resp = m.mustPost(t, chatBody(true, "hi", ""))
	events, err := readSSE(resp.Body, time.Now())
	if err == nil || errors.Is(err, io.EOF) {
		t.Errorf("midstream failure ended cleanly (%d events)", len(events))
	}
	if len(events) != 3 { // role chunk, token 0, token 1; the connection is cut at token 2
		t.Errorf("%d events before the abort, want 3", len(events))
	}
}

func TestSimSameSeedGivesTheSameOutputLengths(t *testing.T) {
	lens := func(seed int64) []int {
		e := simEnv(t, func(c *Config) {
			p := fastSim()
			p.OutputCV = 0.8
			c.Sim, c.Seed, c.OutputTokens = &p, seed, 30
		})
		var out []int
		for range 6 {
			resp := e.mustPost(t, chatBody(false, "hi", ""))
			var b struct {
				Usage struct {
					CompletionTokens int `json:"completion_tokens"`
				}
			}
			_ = json.NewDecoder(resp.Body).Decode(&b)
			out = append(out, b.Usage.CompletionTokens)
		}
		return out
	}
	a, b, c := lens(5), lens(5), lens(6)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed, different lengths: %v vs %v", a, b)
		}
	}
	same := true
	for i := range a {
		same = same && a[i] == c[i]
	}
	if same {
		t.Errorf("different seeds gave identical lengths %v", a)
	}
}

func TestSimLogLinesKeepTheirShape(t *testing.T) {
	e := simEnv(t)
	resp := e.mustPost(t, chatBody(true, "hi", `,"max_tokens":3`))
	_, _ = io.Copy(io.Discard, resp.Body)
	eventually(t, 2*time.Second, func() bool { return strings.Contains(e.logs.String(), `"msg":"chat"`) }, "chat log")
	line := e.logs.String()
	for _, k := range []string{`"outcome":"ok"`, `"prompt_tokens":1`, `"output_tokens":3`, `"queue_ms"`, `"ttft_ms"`, `"stream":true`} {
		if !strings.Contains(line, k) {
			t.Errorf("log lacks %s: %s", k, line)
		}
	}
}

// --- flags and config ---------------------------------------------------------------------

func TestParseFlagsSimProfileAndParamsFile(t *testing.T) {
	cfg, _, err := ParseFlags([]string{"--model-engine=sim", "--profile=slow"}, io.Discard)
	if err != nil || !cfg.UsesSim() || cfg.Sim == nil || cfg.Sim.Name != "slow" {
		t.Fatalf("profile: %+v %v", cfg.Sim, err)
	}
	cfg, _, err = ParseFlags([]string{"--model-engine=sim"}, io.Discard)
	if err != nil || cfg.Sim != nil || cfg.SimParams().Name != "default" {
		t.Fatalf("no profile should mean the default profile: %v", err)
	}

	p, _ := engine.Profile("kv-tight")
	b, _ := p.MarshalFile()
	path := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, _, err = ParseFlags([]string{"--model-engine=sim", "--sim-params=" + path}, io.Discard)
	if err != nil || cfg.Sim == nil || cfg.Sim.Hash() != p.Hash() {
		t.Fatalf("params file: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"bogus":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = ParseFlags([]string{"--model-engine=sim", "--sim-params=" + path}, io.Discard); err == nil {
		t.Error("a parameter file with an unknown field was accepted")
	}
}

func validParamsFile(t *testing.T) string {
	t.Helper()
	p, _ := engine.Profile("fast")
	b, _ := p.MarshalFile()
	path := filepath.Join(t.TempDir(), "ok.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseFlagsSimRejectsConflicts(t *testing.T) {
	for name, args := range map[string][]string{
		"ttft with sim":         {"--model-engine=sim", "--ttft=1s"},
		"tps with sim":          {"--model-engine=sim", "--tokens-per-second=5"},
		"profile and file":      {"--model-engine=sim", "--profile=slow", "--sim-params=" + validParamsFile(t)},
		"unknown profile":       {"--model-engine=sim", "--profile=nope"},
		"missing file":          {"--model-engine=sim", "--sim-params=/nonexistent/p.json"},
		"profile without sim":   {"--profile=slow"},
		"params without sim":    {"--sim-params=x"},
		"unknown engine":        {"--model-engine=gpu"},
		"explicit simple+prof":  {"--model-engine=simple", "--profile=slow"},
		"sim, concurrency 9999": {"--model-engine=sim", "--max-concurrency=9999"},
	} {
		if _, _, err := ParseFlags(args, io.Discard); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The explicit default is allowed, and keeps --ttft meaningful.
	if _, _, err := ParseFlags([]string{"--model-engine=simple", "--ttft=1s"}, io.Discard); err != nil {
		t.Errorf("simple with ttft: %v", err)
	}
}

// With a one-token output the midstream cut lands before the only token (min(2, n-1) = 0).
func TestSimMidstreamFailureOnAOneTokenOutput(t *testing.T) {
	e := simEnv(t, func(c *Config) { c.FailureRate, c.FailureMode, c.OutputTokens = 1, ModeMidstream, 1 })
	resp := e.mustPost(t, chatBody(true, "hi", ""))
	events, err := readSSE(resp.Body, time.Now())
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("the stream ended cleanly with %d events", len(events))
	}
	if len(events) != 0 {
		t.Errorf("%d events before the abort, want none", len(events))
	}
}

// --- leaks, advancing, token counters ----------------------------------------------------------------

func (e *env) simStreams() int {
	e.srv.sim.mu.Lock()
	defer e.srv.sim.mu.Unlock()
	return len(e.srv.sim.streams)
}

func TestSimDoesNotLeakStreams(t *testing.T) {
	e := simEnv(t, func(c *Config) { c.OutputTokens = 30; c.FailureRate, c.FailureMode = 0.3, ModeMidstream })
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20+i*15)*time.Millisecond)
			defer cancel()
			if resp, err := e.post(ctx, chatBody(i%2 == 0, "hi", "")); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	eventually(t, 5*time.Second, func() bool { return e.simStreams() == 0 }, "every request's stream forgotten (completed, cancelled or aborted)")
}

// /stats must advance the engine itself: while the only handler is held in an injected delay nobody else
// drives it, yet the request (a few milliseconds of work) must show as finished.
func TestSimStatsAdvanceTheEngine(t *testing.T) {
	e := simEnv(t, func(c *Config) { c.OutputTokens = 3 })
	e.srv.Faults().SetExtraDelay(600 * time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); timeOne(t, e, "hi", "") }()
	eventually(t, 2*time.Second, func() bool { // read without advancing: the point is that /stats does it
		e.srv.sim.mu.Lock()
		defer e.srv.sim.mu.Unlock()
		return e.srv.sim.eng.Stats().Submitted == 1
	}, "request submitted")
	time.Sleep(200 * time.Millisecond)
	if s := e.stats(t); s.ActiveRequests != 0 || s.Sim.Running != 0 {
		t.Errorf("stats show the request still active (%+v): /stats did not advance the engine", s)
	}
	<-done
}

func TestSimCountsGeneratedTokensAndReportsARate(t *testing.T) {
	e := simEnv(t, func(c *Config) { c.OutputTokens = 400 })
	done := make(chan struct{})
	go func() { defer close(done); timeOne(t, e, "hi", "") }()
	// 400 tokens at ~2.5ms each takes about a second; the 5 s window needs complete 100ms buckets.
	eventually(t, 3*time.Second, func() bool { return e.stats(t).RecentTokensPerSecond > 100 }, "a positive recent rate")
	<-done
	if s := e.stats(t); s.TokensGenerated != 400 {
		t.Errorf("tokens_generated = %d, want 400", s.TokensGenerated)
	}
	_, body := e.get(t, "/metrics")
	if !strings.Contains(body, "worker_output_tokens_total 400") {
		t.Errorf("worker_output_tokens_total is not 400 in sim mode")
	}
}
