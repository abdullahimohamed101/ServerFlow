package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/ratelimit"
)

// fakeLimiter answers from a function and counts calls and releases.
type fakeLimiter struct {
	mu       sync.Mutex
	reqs     []ratelimit.Request
	decide   func(ratelimit.Request) (ratelimit.Decision, error)
	releases atomic.Int64
	allowed  atomic.Int64
	ran      atomic.Bool
}

func (f *fakeLimiter) Allow(_ context.Context, r ratelimit.Request) (ratelimit.Decision, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, r)
	f.mu.Unlock()
	if f.decide == nil {
		return f.admit(false), nil
	}
	d, err := f.decide(r)
	if err == nil && d.Allowed && d.Release == nil {
		d.Release = f.releaseOnce()
		f.allowed.Add(1)
	}
	return d, err
}

func (f *fakeLimiter) admit(bypassed bool) ratelimit.Decision {
	f.allowed.Add(1)
	return ratelimit.Decision{Allowed: true, Bypassed: bypassed, Release: f.releaseOnce()}
}

func (f *fakeLimiter) releaseOnce() func() {
	var once sync.Once
	return func() { once.Do(func() { f.releases.Add(1) }) }
}

func (f *fakeLimiter) Run(context.Context) { f.ran.Store(true) }

func (f *fakeLimiter) calls() []ratelimit.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ratelimit.Request(nil), f.reqs...)
}

type fakeRecorder struct {
	mu    sync.Mutex
	calls []string
	boom  bool
}

func (f *fakeRecorder) Record(id, tenant, model, worker string, attempt int) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("%s|%s|%s|%d", tenant, model, worker, attempt))
	f.mu.Unlock()
	if f.boom {
		panic("recorder blew up")
	}
}

// limitEnv is a static-mode gateway with auth and a fake limiter in front of a configurable upstream.
func newLimitEnv(t *testing.T, up http.HandlerFunc, lim *fakeLimiter, opts ...Option) *authEnv {
	t.Helper()
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	cfg := testConfig(srv.URL)
	cfg.Models = []string{"qwen-7b", "llama-8b"}
	logs := &lockedBuffer{}
	store := newAuthStore()
	clk := &testClock{t: time.Now()}
	all := append([]Option{WithAuthenticator(newAuthenticator(store, logs, clk)), WithLimiter(lim)}, opts...)
	gw := New(cfg, slog.New(slog.NewJSONHandler(logs, nil)), all...)
	url, c := serve(t, gw)
	return &authEnv{url: url, store: store, logs: logs, client: c, gw: gw, clock: clk}
}

func okUpstream(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func quotas(rpm, tpm, conc int) func(*auth.KeyRecord) {
	return func(r *auth.KeyRecord) {
		r.Policy.RequestsPerMinute, r.Policy.TokensPerMinute, r.Policy.MaxConcurrent = rpm, tpm, conc
	}
}

func TestRateLimitingIsOffByDefault(t *testing.T) {
	gw := New(testConfig("http://127.0.0.1:1"), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if gw.RateLimitRequired() {
		t.Fatal("rate limiting must be off by default")
	}
	cfg := config.Default()
	if NewFromConfig(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))).RateLimitRequired() {
		t.Fatal("rate_limit.mode=off must not require a limiter")
	}
	cfg.RateLimit.Mode = config.RateLimitRequired
	if !NewFromConfig(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))).RateLimitRequired() {
		t.Fatal("rate_limit.mode=required must require a limiter")
	}
}

func TestLimiterSeesTenantQuotasCostAndModel(t *testing.T) {
	lim := &fakeLimiter{}
	env := newLimitEnv(t, okUpstream, lim)
	key := env.store.add("acme", nil, quotas(7, 800, 3))
	body := `{"model":"qwen-7b","max_tokens":100,"messages":[{"role":"user","content":"hello world"}]}`
	resp, _ := env.do(t, http.MethodPost, chatCompletionsPath, body, "Authorization", "Bearer "+key)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	calls := lim.calls()
	if len(calls) != 1 {
		t.Fatalf("%d limiter calls", len(calls))
	}
	c := calls[0]
	if c.TenantID != "ten_acme" || c.Model != "qwen-7b" || c.Limits != (ratelimit.Limits{RequestsPerMinute: 7, TokensPerMinute: 800, MaxConcurrent: 3}) {
		t.Fatalf("limiter saw %+v", c)
	}
	if c.Cost < 100 || c.Cost > 200 {
		t.Fatalf("cost %d, want about max_tokens plus a few input tokens", c.Cost)
	}
	// An absent max_tokens is charged the gateway limit.
	env.chat(t, key, "qwen-7b")
	if c := lim.calls()[1]; c.Cost < testConfig("").MaxTokensLimit {
		t.Fatalf("absent max_tokens cost %d, want at least the limit %d", c.Cost, testConfig("").MaxTokensLimit)
	}
}

func TestEveryLimitMapsTo429WithRetryAfterAndBoundedMetrics(t *testing.T) {
	for _, tc := range []struct {
		limit ratelimit.Limit
		after time.Duration
		want  string
	}{
		{ratelimit.LimitRequests, 1500 * time.Millisecond, "2"},
		{ratelimit.LimitTokens, 30 * time.Second, "30"},
		{ratelimit.LimitConcurrency, time.Second, "1"},
		{ratelimit.LimitModel, 0, "1"},
	} {
		lim := &fakeLimiter{decide: func(ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{Limit: tc.limit, RetryAfter: tc.after}, nil
		}}
		env := newLimitEnv(t, okUpstream, lim)
		key := env.store.add("acme", nil, quotas(1, 1, 1))
		resp, body := env.chat(t, key, "qwen-7b")
		if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != tc.want {
			t.Fatalf("%s: %d retry-after %q", tc.limit, resp.StatusCode, resp.Header.Get("Retry-After"))
		}
		if !strings.Contains(body, `"code":"RATE_LIMITED"`) || strings.Contains(body, "acme") || strings.Contains(body, "ten_") {
			t.Fatalf("%s: body %s", tc.limit, body)
		}
		if n := testutil.ToFloat64(env.gw.metrics.rateRejects.WithLabelValues(string(tc.limit))); n != 1 {
			t.Fatalf("%s: rejection counter %v", tc.limit, n)
		}
		if lim.allowed.Load() != 0 || lim.releases.Load() != 0 {
			t.Fatal("a refused request must hold nothing")
		}
		out := env.logs.String()
		if !strings.Contains(out, `"limit":"`+string(tc.limit)+`"`) || !strings.Contains(out, `"tenant_id":"ten_acme"`) || strings.Contains(out, key) {
			t.Fatalf("%s: log lacks tenant and limit, or leaks the key:\n%s", tc.limit, out)
		}
		if n := testutil.CollectAndCount(env.gw.metrics.rateRejects); n != 1 {
			t.Fatalf("label cardinality %d", n)
		}
	}
}

func TestUnavailableMapsTo503AndHonoursTheFailureMode(t *testing.T) {
	lim := &fakeLimiter{decide: func(ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{}, &ratelimit.UnavailableError{RetryAfter: 2400 * time.Millisecond, Cause: errors.New("dial tcp 10.9.9.9:6379: refused")}
	}}
	env := newLimitEnv(t, okUpstream, lim)
	key := env.store.add("acme", nil, quotas(1, 1, 1))
	resp, body := env.chat(t, key, "qwen-7b")
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") != "3" || !strings.Contains(body, `"code":"RATE_LIMIT_UNAVAILABLE"`) {
		t.Fatalf("%d %q %s", resp.StatusCode, resp.Header.Get("Retry-After"), body)
	}
	if strings.Contains(body, "10.9.9.9") || strings.Contains(body, "redis") {
		t.Fatalf("the response leaks infrastructure detail: %s", body)
	}
	if n := testutil.ToFloat64(env.gw.metrics.rateRejects.WithLabelValues("unavailable")); n != 1 {
		t.Fatalf("unavailable counter %v", n)
	}
	// A plain error is also a 503 with a 1 s retry.
	lim.decide = func(ratelimit.Request) (ratelimit.Decision, error) { return ratelimit.Decision{}, errors.New("boom") }
	resp, _ = env.chat(t, key, "qwen-7b")
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("%d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	// Open mode: the limiter admits and flags the bypass.
	lim.decide = func(ratelimit.Request) (ratelimit.Decision, error) { return lim.admit(true), nil }
	lim.allowed.Store(0)
	resp, _ = env.chat(t, key, "qwen-7b")
	if resp.StatusCode != 200 {
		t.Fatalf("bypassed request: %d", resp.StatusCode)
	}
	if n := testutil.ToFloat64(env.gw.metrics.rateBypassed); n != 1 {
		t.Fatalf("bypassed counter %v", n)
	}
	if !strings.Contains(env.logs.String(), `"rate_limit_bypassed":true`) {
		t.Fatal("a bypassed request must be visible in the log")
	}
}

func TestLimiterIsAskedAfterParsingAndModelChecksOnly(t *testing.T) {
	lim := &fakeLimiter{}
	env := newLimitEnv(t, okUpstream, lim)
	allowedOnly := env.store.add("narrow", []string{"qwen-7b"}, quotas(5, 5, 5))
	key := env.store.add("acme", nil, quotas(5, 5, 5))
	check := func(what string, resp *http.Response) {
		t.Helper()
		if n := len(lim.calls()); n != 0 {
			t.Fatalf("%s (%d): the limiter was asked %d times", what, resp.StatusCode, n)
		}
	}
	resp, _ := env.chat(t, "", "qwen-7b")
	check("no key", resp)
	resp, _ = env.chat(t, "sf_bad", "qwen-7b")
	check("bad key", resp)
	resp, _ = env.do(t, http.MethodPost, chatCompletionsPath, `{"model":`, "Authorization", "Bearer "+key)
	check("malformed body", resp)
	resp, _ = env.do(t, http.MethodPost, chatCompletionsPath, `{"model":"qwen-7b","messages":[]}`, "Authorization", "Bearer "+key)
	check("invalid request", resp)
	resp, _ = env.chat(t, key, "no-such-model")
	check("unknown model", resp)
	resp, _ = env.chat(t, allowedOnly, "llama-8b")
	check("forbidden model", resp)
	if resp.StatusCode != 403 {
		t.Fatalf("forbidden model: %d", resp.StatusCode)
	}
	if resp, _ := env.chat(t, key, "qwen-7b"); resp.StatusCode != 200 || len(lim.calls()) != 1 {
		t.Fatalf("a good request: %d, %d limiter calls", resp.StatusCode, len(lim.calls()))
	}
}

func TestSlotIsReleasedOnEveryExitPath(t *testing.T) {
	var mode atomic.Value
	mode.Store("ok")
	started := make(chan struct{}, 1)
	stop := make(chan struct{})
	up := func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load().(string) {
		case "ok":
			okUpstream(w, r)
		case "error":
			w.WriteHeader(500)
			_, _ = io.WriteString(w, `{"error":{"message":"x"}}`)
		case "reset":
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			_ = c.Close()
		case "hang":
			started <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-stop:
			}
		case "stream-cut":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "data: {}\n\n")
			w.(http.Flusher).Flush()
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			_ = c.Close()
		}
	}
	lim := &fakeLimiter{}
	rec := &fakeRecorder{}
	env := newLimitEnv(t, up, lim, WithRequestRecorder(rec))
	// Registered after the servers, so it runs before their Close: a failed assertion must not leave the
	// hanging upstream request blocking it.
	t.Cleanup(func() { close(stop) })
	key := env.store.add("acme", nil, quotas(5, 5, 5))

	wait := func(want int64, what string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for lim.releases.Load() != want && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if lim.releases.Load() != want || lim.allowed.Load() != want {
			t.Fatalf("%s: %d admitted, %d released, want %d of each", what, lim.allowed.Load(), lim.releases.Load(), want)
		}
	}
	env.chat(t, key, "qwen-7b")
	wait(1, "success")
	mode.Store("error")
	env.chat(t, key, "qwen-7b")
	wait(2, "upstream error")
	mode.Store("reset")
	env.chat(t, key, "qwen-7b")
	wait(3, "upstream reset")
	mode.Store("stream-cut")
	env.chat(t, key, "qwen-7b")
	wait(4, "stream cut mid-way")

	// Client disconnect while the upstream hangs.
	mode.Store("hang")
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(chatBody("qwen-7b")))
	req.Header.Set("Authorization", "Bearer "+key)
	errc := make(chan error, 1)
	go func() {
		resp, err := env.client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		errc <- err
	}()
	<-started
	if lim.releases.Load() != 4 {
		t.Fatal("the slot was released while the request was still running")
	}
	cancel()
	<-errc
	wait(5, "client disconnect")

	// A handler panic. The recorder runs after admission, so a panic there is a panic with a slot held.
	mode.Store("ok")
	rec.boom = true
	resp, _ := env.chat(t, key, "qwen-7b")
	if resp != nil && resp.StatusCode != 500 {
		t.Fatalf("panic status %d", resp.StatusCode)
	}
	wait(6, "handler panic")
}

func TestRegistryModeRecordsTheWorkerAndReleases(t *testing.T) {
	env := newRegistryAuthEnv(t)
	lim := &fakeLimiter{}
	rec := &fakeRecorder{}
	env.gw.SetLimiter(lim)
	WithRequestRecorder(rec)(env.gw)
	key := env.store.add("acme", nil, quotas(5, 5, 5))
	resp, body := env.chat(t, key, "qwen-7b")
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if lim.releases.Load() != 1 {
		t.Fatalf("%d releases", lim.releases.Load())
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 1 || !strings.HasPrefix(rec.calls[0], "ten_acme|qwen-7b|w") || !strings.HasSuffix(rec.calls[0], "|1") {
		t.Fatalf("recorded %v", rec.calls)
	}
}

func TestRequiredButNotWiredFailsClosedForRateLimiting(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(okUpstream))
	t.Cleanup(up.Close)
	gw := New(testConfig(up.URL), slog.New(slog.NewJSONHandler(io.Discard, nil)), WithLimiter(nil))
	if !gw.RateLimitRequired() {
		t.Fatal("WithLimiter(nil) must still mean limiting is required")
	}
	url, c := serve(t, gw)
	resp, err := c.Post(url+chatCompletionsPath, "application/json", strings.NewReader(chatBody("qwen-7b")))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("a gateway that needs a limiter and has none must not serve: %d", resp.StatusCode)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if err := gw.Serve(context.Background(), ln); err == nil || !strings.Contains(err.Error(), "no limiter") {
		t.Fatalf("Serve must refuse: %v", err)
	}
}

func TestServeRunsTheLimitersBackgroundWork(t *testing.T) {
	lim := &fakeLimiter{}
	gw := New(testConfig("http://127.0.0.1:1"), slog.New(slog.NewJSONHandler(io.Discard, nil)), WithLimiter(lim))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	deadline := time.Now().Add(5 * time.Second)
	for !lim.ran.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !lim.ran.Load() {
		t.Fatal("Serve did not run the limiter")
	}
}

func TestAuthOffModelCapStillReachesTheLimiter(t *testing.T) {
	lim := &fakeLimiter{}
	srv := httptest.NewServer(http.HandlerFunc(okUpstream))
	t.Cleanup(srv.Close)
	cfg := testConfig(srv.URL)
	cfg.Models = []string{"qwen-7b"}
	gw := New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), WithLimiter(lim))
	url, c := serve(t, gw)
	resp, err := c.Post(url+chatCompletionsPath, "application/json", strings.NewReader(chatBody("qwen-7b")))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	calls := lim.calls()
	if resp.StatusCode != 200 || len(calls) != 1 || calls[0].TenantID != "" || calls[0].Model != "qwen-7b" || !calls[0].Limits.None() {
		t.Fatalf("%d %+v", resp.StatusCode, calls)
	}
}
