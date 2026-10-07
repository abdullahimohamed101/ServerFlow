package mockworker

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Tests added after independent mutation testing found behavior that nothing
// pinned down.

// --- engine -----------------------------------------------------------------------

// The cancel-versus-grant interleaving is made deterministic here instead of
// relying on a goroutine race: the waiter is granted the slot, then abandons it.
func TestAbandoningAGrantedWaiterPassesTheSlotOn(t *testing.T) {
	e := NewEngine(1, 2)
	first := mustAcquire(t, e, 1)

	w := &waiter{ready: make(chan struct{}), promptTokens: 5}
	e.mu.Lock()
	e.queue = append(e.queue, w)
	e.queuedTokens += w.promptTokens
	e.mu.Unlock()

	first() // grants the slot to w
	if !w.granted || e.Stats().Active != 1 {
		t.Fatalf("setup: the waiter should hold the slot: %+v", e.Stats())
	}
	e.abandon(w) // ...but its client gave up at the same moment
	if s := e.Stats(); s.Active != 0 || s.QueueDepth != 0 || s.QueuedInputTokens != 0 || s.Cancelled != 1 {
		t.Fatalf("the slot leaked: %+v", s)
	}
}

func TestAbandoningAQueuedWaiterRemovesIt(t *testing.T) {
	e := NewEngine(1, 2)
	mustAcquire(t, e, 1)
	w := &waiter{ready: make(chan struct{}), promptTokens: 5}
	e.mu.Lock()
	e.queue = append(e.queue, w)
	e.queuedTokens += w.promptTokens
	e.mu.Unlock()

	e.abandon(w)
	if s := e.Stats(); s.Active != 1 || s.QueueDepth != 0 || s.QueuedInputTokens != 0 {
		t.Fatalf("got %+v", s)
	}
}

// --- pacing -------------------------------------------------------------------------

func recordingServer(t *testing.T) (*Server, *[]time.Time) {
	t.Helper()
	cfg := testConfig()
	cfg.TTFT = 80 * time.Millisecond
	cfg.TokensPerSecond = 50 // 20ms apart
	s := New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var deadlines []time.Time
	s.sleep = func(ctx context.Context, until time.Time) error {
		deadlines = append(deadlines, until)
		return ctx.Err()
	}
	return s, &deadlines
}

func TestGeneratePacesAgainstAbsoluteDeadlines(t *testing.T) {
	s, deadlines := recordingServer(t)
	before := time.Now()
	var emitted []int
	if err := s.generate(context.Background(), 6, -1, func(i int) error { emitted = append(emitted, i); return nil }); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if len(*deadlines) != 6 || len(emitted) != 6 {
		t.Fatalf("expected 6 deadlines and 6 tokens, got %d and %d", len(*deadlines), len(emitted))
	}
	// Every deadline is the first token's deadline plus exactly i intervals, so
	// the time spent between tokens can never accumulate into drift.
	for i := 1; i < 6; i++ {
		if gap := (*deadlines)[i].Sub((*deadlines)[i-1]); gap != 20*time.Millisecond {
			t.Fatalf("deadline %d is %v after the previous one, want exactly 20ms", i, gap)
		}
	}
	first := (*deadlines)[0]
	if first.Before(before.Add(80*time.Millisecond)) || first.After(after.Add(80*time.Millisecond)) {
		t.Fatalf("the first token's deadline %v is not TTFT after the start", first.Sub(before))
	}
}

func TestGenerateStopsWhenTheWaitIsInterrupted(t *testing.T) {
	s, _ := recordingServer(t)
	calls := 0
	s.sleep = func(ctx context.Context, until time.Time) error {
		calls++
		if calls == 4 {
			return context.Canceled
		}
		return nil
	}
	var emitted []int
	err := s.generate(context.Background(), 10, -1, func(i int) error { emitted = append(emitted, i); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("an interrupted wait must be reported, got %v", err)
	}
	if len(emitted) != 3 {
		t.Fatalf("tokens must stop at the interruption, emitted %v", emitted)
	}
}

func TestGenerateAbortsAtTheInjectedIndex(t *testing.T) {
	s, _ := recordingServer(t)
	s.sleep = func(context.Context, time.Time) error { return nil }
	var emitted []int
	err := s.generate(context.Background(), 10, 2, func(i int) error { emitted = append(emitted, i); return nil })
	if !errors.Is(err, errMidstream) || len(emitted) != 2 {
		t.Fatalf("got %v after %v", err, emitted)
	}
}

func TestSleepUntilHonorsTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepUntil(ctx, time.Now().Add(10*time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("a cancelled context must end the wait immediately")
	}
	if err := sleepUntil(ctx, time.Now().Add(-time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatalf("a past deadline must still report a cancelled context, got %v", err)
	}
	if err := sleepUntil(context.Background(), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("a past deadline on a live context is not an error, got %v", err)
	}
	if err := sleepUntil(context.Background(), time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
}

// --- cancellation of non-stream requests ----------------------------------------------

func TestNonStreamCancelDuringTTFTFreesTheSlot(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.MaxConcurrency = 1; c.TTFT = 10 * time.Second })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = e.post(ctx, chatBody(false, "x", "")) }()
	eventually(t, 2*time.Second, func() bool { return e.stats(t).ActiveRequests == 1 }, "request running")
	cancel()
	eventually(t, time.Second, func() bool { return e.stats(t).ActiveRequests == 0 }, "slot freed while still waiting for the first token")
	if s := e.stats(t); s.TokensGenerated != 0 || s.Cancelled != 1 || s.Completed != 0 {
		t.Fatalf("got %+v", s)
	}
}

func TestNonStreamCancelMidGenerationStopsAndFreesTheSlot(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.MaxConcurrency = 1; c.TTFT = 0; c.TokensPerSecond = 50; c.OutputTokens = 500 })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = e.post(ctx, chatBody(false, "x", "")) }()
	eventually(t, 2*time.Second, func() bool { return e.stats(t).TokensGenerated >= 3 }, "generation under way")
	cancel()
	eventually(t, time.Second, func() bool { return e.stats(t).ActiveRequests == 0 }, "slot freed mid-generation")
	before := e.stats(t).TokensGenerated
	time.Sleep(120 * time.Millisecond)
	if after := e.stats(t); after.TokensGenerated != before || after.TokensGenerated >= 500 {
		t.Fatalf("generation continued for a client that left: %d -> %d", before, after.TokensGenerated)
	}
}

// --- smaller contracts --------------------------------------------------------------------

func TestInjectorConsumesRandomnessRegardlessOfRate(t *testing.T) {
	// If a rate-0 roll skipped the generator, raising the rate later would
	// replay a different sequence than a worker that ran at that rate all along.
	a := NewInjector(5, 0, ModeError)
	for i := 0; i < 7; i++ {
		a.Roll()
	}
	a.rate = 0.5
	b := NewInjector(5, 0.5, ModeError)
	for i := 0; i < 7; i++ {
		b.Roll()
	}
	for i := 0; i < 100; i++ {
		_, fa := a.Roll()
		_, fb := b.Roll()
		if fa != fb {
			t.Fatalf("roll %d diverged: a rate-0 roll must still consume one random number", i)
		}
	}
}

func TestMaxTokensAtTheNaturalLengthReportsLength(t *testing.T) {
	e := newEnv(t) // 10 output tokens
	for _, c := range []struct {
		max    string
		finish string
	}{{`,"max_tokens":9`, "length"}, {`,"max_tokens":10`, "length"}, {`,"max_tokens":11`, "stop"}, {``, "stop"}} {
		start := time.Now()
		resp := e.mustPost(t, chatBody(true, "hi", c.max))
		events, _ := readSSE(resp.Body, start)
		_, _, fin := contentOf(t, events[len(events)-2])
		if fin == nil || *fin != c.finish {
			t.Fatalf("max_tokens %q: finish_reason %v, want %s", c.max, fin, c.finish)
		}
	}
}

func TestPromptTokensCountWordsWithAFloorOfOne(t *testing.T) {
	msgs := func(texts ...string) []protocol.Message {
		var m []protocol.Message
		for _, s := range texts {
			m = append(m, protocol.Message{Role: "user", Content: s})
		}
		return m
	}
	for _, c := range []struct {
		msgs []protocol.Message
		want int
	}{
		{msgs(""), 1}, {msgs("   "), 1}, {msgs("a"), 1}, {msgs("a b  c"), 3}, {msgs("a b", "c d e"), 5},
	} {
		if got := promptTokens(c.msgs); got != c.want {
			t.Fatalf("promptTokens(%v) = %d, want %d", c.msgs, got, c.want)
		}
	}
}

func TestStreamHeadersAndCaching(t *testing.T) {
	e := newEnv(t)
	resp := e.mustPost(t, chatBody(true, "hi", ""))
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("SSE responses must not be cached, got %q", cc)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestChatLogCarriesTheGatewayRequestID(t *testing.T) {
	e := newEnv(t)
	req, _ := http.NewRequest(http.MethodPost, e.ts.URL+"/v1/chat/completions", strings.NewReader(chatBody(false, "hi", "")))
	req.Header.Set("X-Request-ID", "req_0123456789abcdef")
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	eventually(t, 2*time.Second, func() bool { return strings.Contains(e.logs.String(), "req_0123456789abcdef") }, "the gateway's request ID in the worker's log")
	log := e.logs.String()
	for _, want := range []string{`"component":"mock-worker"`, `"request_id":"req_0123456789abcdef"`, `"outcome":"ok"`} {
		if !strings.Contains(log, want) {
			t.Fatalf("log line missing %s: %s", want, log)
		}
	}
}

// --- flags --------------------------------------------------------------------------------

func TestParseFlagsHelpPrintsUsageOnceAndIsNotAnError(t *testing.T) {
	var out bytes.Buffer
	_, _, err := ParseFlags([]string{"--help"}, &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(out.String(), "-ttft") || strings.Count(out.String(), "Usage:") != 1 {
		t.Fatalf("usage output wrong: %q", out.String())
	}
}

func TestParseFlagsErrorsAreNotPrintedByTheFlagPackage(t *testing.T) {
	var out bytes.Buffer
	if _, _, err := ParseFlags([]string{"--nope"}, &out); err == nil {
		t.Fatal("expected an error")
	}
	if out.Len() != 0 {
		t.Fatalf("the caller reports errors; ParseFlags must not also print them: %q", out.String())
	}
}

func TestDefaultListenAddressIsLoopbackOnly(t *testing.T) {
	cfg, _, _ := ParseFlags(nil, io.Discard)
	if !strings.HasPrefix(cfg.Addr, "127.0.0.1:") {
		t.Fatalf("a dev tool should not listen on every interface by default, got %q", cfg.Addr)
	}
}

// --- remaining gaps from the second verification ----------------------------------------

func TestWorkerIDFromAddressesIncludingIPv6(t *testing.T) {
	for addr, want := range map[string]string{
		":9001": "mock-9001", "127.0.0.1:9002": "mock-9002", "[::]:9003": "mock-9003", "[::1]:9004": "mock-9004",
	} {
		cfg := DefaultConfig()
		cfg.Addr = addr
		if got := cfg.EffectiveWorkerID(); got != want {
			t.Fatalf("%s -> %q, want %q", addr, got, want)
		}
	}
}

func TestRequestBodyLimitIsOneMebibyte(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.TTFT = 0; c.OutputTokens = 1 })
	pad := func(n int) string { return strings.Repeat("a", n) }
	// Just under the limit is accepted; just over is refused.
	under := e.mustPost(t, chatBody(false, pad(1<<20-1024), ""))
	_, _ = io.Copy(io.Discard, under.Body)
	if under.StatusCode != 200 {
		t.Fatalf("a body just under 1 MiB must be accepted, got %d", under.StatusCode)
	}
	over := e.mustPost(t, chatBody(false, pad(1<<20+1024), ""))
	_, _ = io.Copy(io.Discard, over.Body)
	if over.StatusCode != 413 {
		t.Fatalf("a body just over 1 MiB must be refused, got %d", over.StatusCode)
	}
}

func TestStreamHeadersArriveBeforeTheFirstToken(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.TTFT = 600 * time.Millisecond })
	start := time.Now()
	resp, err := e.post(context.Background(), chatBody(true, "x", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if took := time.Since(start); took > 400*time.Millisecond {
		t.Fatalf("headers took %v; a stream must open as soon as it has a slot, not at the first token", took)
	}
}

func TestQueuedCancelIsLoggedAsCancelled(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.MaxConcurrency = 1; c.QueueSize = 1; c.TTFT = 3 * time.Second })
	actx, acancel := context.WithCancel(context.Background())
	defer acancel()
	go func() {
		if resp, err := e.post(actx, chatBody(false, "a", "")); err == nil {
			_ = resp.Body.Close()
		}
	}()
	eventually(t, 2*time.Second, func() bool { return e.stats(t).ActiveRequests == 1 }, "A running")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = e.post(ctx, chatBody(false, "queued", "")) }()
	eventually(t, 2*time.Second, func() bool { return e.stats(t).QueueDepth == 1 }, "B queued")
	cancel()
	eventually(t, 2*time.Second, func() bool { return strings.Contains(e.logs.String(), `"outcome":"cancelled"`) }, "the cancelled outcome in the log")
}
