package client

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/registry"
	"serverflow/internal/registry/server"
	"serverflow/pkg/protocol"
)

const token = "a-sufficiently-long-shared-secret"

func start(t *testing.T, tok string) (*Client, *registry.Registry) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg, err := registry.New(registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second,
		Retention: time.Minute, MaxWorkers: 20, HeartbeatInterval: 2 * time.Second}, log)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.New(reg, server.Config{Token: tok}, log).Handler())
	t.Cleanup(ts.Close)
	return New(ts.URL, tok, nil), reg
}

func info(id string) protocol.WorkerInfo {
	return protocol.WorkerInfo{WorkerID: id, Model: "qwen-7b", Address: "http://127.0.0.1:9001", MaxConcurrency: 4, QueueSize: 8}
}

func TestRoundTrip(t *testing.T) {
	c, _ := start(t, token)
	ctx := context.Background()
	reg, err := c.Register(ctx, info("w1"))
	if err != nil || !strings.HasPrefix(reg.RegistrationID, "reg_") || reg.HeartbeatIntervalSeconds != 2 {
		t.Fatalf("%+v %v", reg, err)
	}
	if err := c.Heartbeat(ctx, "w1", protocol.Heartbeat{RegistrationID: reg.RegistrationID, State: protocol.StateReady,
		Metrics: protocol.Metrics{ActiveRequests: 2, RecentTokensPerSecond: 60}}); err != nil {
		t.Fatal(err)
	}
	w, err := c.Worker(ctx, "w1")
	if err != nil || w.State != protocol.StateReady || !w.Eligible || w.Metrics.ActiveRequests != 2 || w.Address != "http://127.0.0.1:9001" {
		t.Fatalf("%+v %v", w, err)
	}
	all, err := c.Workers(ctx, Query{})
	if err != nil || len(all) != 1 {
		t.Fatalf("%v %v", all, err)
	}
	if got, _ := c.Workers(ctx, Query{Model: "qwen-7b", EligibleOnly: true}); len(got) != 1 {
		t.Fatalf("eligible lookup: %v", got)
	}
	if got, _ := c.Workers(ctx, Query{Model: "other"}); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	if got, _ := c.Workers(ctx, Query{State: protocol.StateDraining}); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	models, err := c.Models(ctx)
	if err != nil || len(models) != 1 || models[0] != (protocol.ModelInfo{Model: "qwen-7b", Workers: 1, Eligible: 1}) {
		t.Fatalf("%v %v", models, err)
	}
	if err := c.Deregister(ctx, "w1", reg.RegistrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Worker(ctx, "w1"); !IsUnknownWorker(err) {
		t.Fatalf("a deregistered worker is unknown, got %v", err)
	}
}

func TestTypedErrors(t *testing.T) {
	c, _ := start(t, token)
	ctx := context.Background()
	reg, _ := c.Register(ctx, info("w1"))
	hb := func(id, regID string, st protocol.WorkerState) error {
		return c.Heartbeat(ctx, id, protocol.Heartbeat{RegistrationID: regID, State: st})
	}
	if err := hb("ghost", "reg_x", protocol.StateReady); !IsUnknownWorker(err) || IsConflict(err) {
		t.Fatalf("got %v", err)
	}
	if err := hb("w1", "reg_wrong", protocol.StateReady); !IsConflict(err) || IsUnknownWorker(err) {
		t.Fatalf("got %v", err)
	}
	_ = hb("w1", reg.RegistrationID, protocol.StateDraining)
	if err := hb("w1", reg.RegistrationID, protocol.StateReady); !IsConflict(err) {
		t.Fatalf("an illegal transition is a conflict, got %v", err)
	}
	if err := hb("w1", reg.RegistrationID, "BOGUS"); err == nil || IsConflict(err) || IsUnknownWorker(err) {
		t.Fatalf("a bad request is none of the retryable kinds, got %v", err)
	}
	bad := info("w2")
	bad.MaxConcurrency = 0
	if _, err := c.Register(ctx, bad); err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("got %v", err)
	}
}

func TestAuthentication(t *testing.T) {
	c, _ := start(t, token)
	wrong := New(c.base, "not-the-token-not-the-token", nil)
	if _, err := wrong.Workers(context.Background(), Query{}); !IsUnauthorized(err) {
		t.Fatalf("a wrong token is unauthorized, got %v", err)
	}
	none := New(c.base, "", nil)
	if _, err := none.Workers(context.Background(), Query{}); !IsUnauthorized(err) {
		t.Fatalf("no token is unauthorized, got %v", err)
	}
	if IsUnauthorized(nil) || IsConflict(nil) || IsUnknownWorker(nil) {
		t.Fatal("a nil error is none of them")
	}
}

func TestInvalidWorkerIDsNeverReachTheNetwork(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	t.Cleanup(ts.Close)
	c := New(ts.URL, "", nil)
	ctx := context.Background()
	for _, id := range []string{"", "a/b", "../x", "a b", "a?b=c", "a#b", "%2e%2e", strings.Repeat("a", 65)} {
		if err := c.Heartbeat(ctx, id, protocol.Heartbeat{}); err == nil {
			t.Errorf("heartbeat accepted %q", id)
		}
		if err := c.Deregister(ctx, id, "reg"); err == nil {
			t.Errorf("deregister accepted %q", id)
		}
		if _, err := c.Worker(ctx, id); err == nil {
			t.Errorf("worker lookup accepted %q", id)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("%d requests were sent for invalid IDs", hits.Load())
	}
}

func TestRedirectsAreNeverFollowedSoTheTokenCannotBeReplayedElsewhere(t *testing.T) {
	var leaked atomic.Bool
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(evil.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	c := New(redirector.URL, token, nil)
	if _, err := c.Register(context.Background(), info("w1")); err == nil {
		t.Fatal("a redirect must not be treated as success")
	}
	if _, err := c.Workers(context.Background(), Query{}); err == nil {
		t.Fatal("a redirect must not be treated as success")
	}
	if leaked.Load() {
		t.Fatal("the bearer token was sent to the redirect target")
	}
}

func TestUnreachableServerAndContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	url := ts.URL
	ts.Close()
	c := New(url, "", nil)
	if _, err := c.Register(context.Background(), info("w1")); err == nil || IsUnknownWorker(err) || IsConflict(err) {
		t.Fatalf("a connection failure is a plain error, got %v", err)
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(slow.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := New(slow.URL, "", nil).Workers(ctx, Query{}); err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("a cancelled context must stop the call promptly, got %v after %v", err, time.Since(start))
	}
}

func TestOversizedResponsesAreBounded(t *testing.T) {
	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"workers":[`)
		chunk := strings.Repeat(" ", 1<<20)
		for i := 0; i < 8; i++ {
			_, _ = io.WriteString(w, chunk)
		}
	}))
	t.Cleanup(huge.Close)
	if _, err := New(huge.URL, "", nil).Workers(context.Background(), Query{}); err == nil {
		t.Fatal("an oversized or truncated response must be an error, not silently accepted")
	}
}

func TestNonJSONErrorBodiesStillYieldAnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "<html>bad gateway</html>", http.StatusBadGateway)
	}))
	t.Cleanup(ts.Close)
	_, err := New(ts.URL, "", nil).Models(context.Background())
	e, ok := err.(*Error)
	if !ok || e.Status != 502 {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(e.Error(), "502") {
		t.Fatalf("the error should say the status: %v", e)
	}
}

// --- independent review and verification findings ----------------------------------------------

func TestConflictKindsAreDistinguishable(t *testing.T) {
	stale := &Error{Status: 409, Code: "stale_registration"}
	illegal := &Error{Status: 409, Code: "illegal_transition"}
	if !IsStale(stale) || IsStale(illegal) || IsStale(&Error{Status: 404, Code: "stale_registration"}) {
		t.Fatal("IsStale must match exactly a 409 stale_registration")
	}
	if !IsIllegalTransition(illegal) || IsIllegalTransition(stale) {
		t.Fatal("IsIllegalTransition must match exactly a 409 illegal_transition")
	}
	if !IsConflict(stale) || !IsConflict(illegal) || IsConflict(&Error{Status: 400}) {
		t.Fatal("IsConflict matches any 409")
	}
	if !IsInvalid(&Error{Status: 400, Code: "invalid_request"}) || IsInvalid(stale) || IsInvalid(nil) {
		t.Fatal("IsInvalid matches a 400")
	}
}

func TestOnlyTheRealUnknownWorkerResponseCountsAsUnknown(t *testing.T) {
	for _, e := range []*Error{
		{Status: 404, Code: "not_found"}, {Status: 404}, {Status: 400, Code: "unknown_worker"}, {Status: 500, Code: "unknown_worker"},
	} {
		if IsUnknownWorker(e) {
			t.Errorf("%+v must not count as an unknown worker (it would trigger a re-registration)", e)
		}
	}
	if !IsUnknownWorker(&Error{Status: 404, Code: "unknown_worker"}) {
		t.Fatal("404 unknown_worker must match")
	}
	if IsUnauthorized(&Error{Status: 403}) || IsUnauthorized(&Error{Status: 400}) || !IsUnauthorized(&Error{Status: 401}) {
		t.Fatal("only 401 is unauthorized")
	}
}

func TestQueryEncoding(t *testing.T) {
	var got atomic.Value
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"workers":[]}`)
	}))
	t.Cleanup(ts.Close)
	c := New(ts.URL+"/", "", nil) // a trailing slash on the base URL must not double up
	cases := []struct {
		q    Query
		want string
	}{
		{Query{}, ""},
		{Query{EligibleOnly: true}, "eligible=true"},
		{Query{Model: "qwen-7b"}, "model=qwen-7b"},
		{Query{Model: "a&b=c d/e?"}, "model=a%26b%3Dc+d%2Fe%3F"},
		{Query{State: protocol.StateDraining}, "state=DRAINING"},
		{Query{Model: "m", State: protocol.StateReady, EligibleOnly: true}, "eligible=true&model=m&state=READY"},
	}
	for _, tc := range cases {
		if _, err := c.Workers(context.Background(), tc.q); err != nil {
			t.Fatal(err)
		}
		if g, _ := got.Load().(string); g != tc.want {
			t.Errorf("%+v sent %q, want %q", tc.q, g, tc.want)
		}
	}
}

func TestDefaultTimeoutAndResponseCap(t *testing.T) {
	if c := New("http://x", "", nil); c.hc.Timeout != 5*time.Second {
		t.Fatalf("the default client needs a timeout, got %v", c.hc.Timeout)
	}
	if c := New("http://x///", "", nil); c.base != "http://x" {
		t.Fatalf("trailing slashes must be trimmed, got %q", c.base)
	}
	// A VALID response just over the cap must still be refused, not read without bound.
	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"workers":[`)
		_, _ = io.WriteString(w, strings.Repeat(" ", maxResponseBytes+10))
		_, _ = io.WriteString(w, `]}`)
	}))
	t.Cleanup(big.Close)
	if _, err := New(big.URL, "", nil).Workers(context.Background(), Query{}); err == nil {
		t.Fatal("a response over the size cap must be an error")
	}
}

func TestTheDefaultClientIgnoresProxyEnvironmentVariables(t *testing.T) {
	c := New("http://127.0.0.1:1", "token-token-token-token", nil)
	tr, ok := c.hc.Transport.(*http.Transport)
	if !ok || tr.Proxy != nil {
		t.Fatalf("the bearer token must not be handed to a proxy named in the environment: %#v", c.hc.Transport)
	}
}

func TestAnEmptyOrListlessSuccessIsAnErrorNotAnEmptyRegistry(t *testing.T) {
	for name, body := range map[string]string{"empty body": "", "null list": `{"workers":null}`, "no list": `{}`} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		}))
		ws, err := New(ts.URL, "tok-tok-tok-tok-tok-tok", nil).Workers(context.Background(), Query{})
		ts.Close()
		if err == nil || ws != nil {
			t.Errorf("%s must fail instead of reading as zero workers: %v %v", name, ws, err)
		}
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{"workers":[]}`) }))
	defer ts.Close()
	if ws, err := New(ts.URL, "tok-tok-tok-tok-tok-tok", nil).Workers(context.Background(), Query{}); err != nil || ws == nil || len(ws) != 0 {
		t.Fatalf("an explicit empty list is a real answer: %v %v", ws, err)
	}
}

func TestAnEmptySuccessBodyIsAnErrorForEveryCallThatExpectsOne(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ts.Close()
	c := New(ts.URL, "tok-tok-tok-tok-tok-tok", nil)
	if _, err := c.Worker(context.Background(), "w1"); err == nil {
		t.Error("Worker must not read an empty body as a zero worker")
	}
	if _, err := c.Models(context.Background()); err == nil {
		t.Error("Models must not read an empty body as no models")
	}
	if _, err := c.Register(context.Background(), protocol.WorkerInfo{}); err == nil {
		t.Error("Register must not read an empty body as a registration")
	}
	// Calls that expect no body are unaffected.
	if err := c.Heartbeat(context.Background(), "w1", protocol.Heartbeat{}); err != nil {
		t.Errorf("Heartbeat expects no body: %v", err)
	}
}
