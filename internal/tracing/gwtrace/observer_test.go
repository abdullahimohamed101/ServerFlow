package gwtrace_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/ratelimit"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/tracingtest"
)

func attrOf(s tracetest.SpanStub, k attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes {
		if kv.Key == k {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func mustAttr(t *testing.T, s tracetest.SpanStub, k attribute.Key) attribute.Value {
	t.Helper()
	v, ok := attrOf(s, k)
	if !ok {
		t.Fatalf("span %q has no %s attribute: %v", s.Name, k, s.Attributes)
	}
	return v
}

// allowLimiter admits everything and releases nothing.
type allowLimiter struct{ d ratelimit.Decision }

func (l allowLimiter) Allow(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	d := l.d
	if d.Allowed {
		d.Release = func() {}
	}
	return d, nil
}

func TestStreamingRequestSpanTree(t *testing.T) {
	e := newEnv(t, envOpts{gwOpts: []gateway.Option{gateway.WithLimiter(allowLimiter{ratelimit.Decision{Allowed: true}})}})
	resp, _ := e.post(streamBody)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	reqID := resp.Header.Get("X-Request-ID")
	spans := e.spans()
	if len(spans) != 5 {
		t.Fatalf("want 5 spans, got %v", names(spans))
	}
	root := one(t, spans, tracing.SpanReceive)
	rl := one(t, spans, tracing.SpanRateLimit)
	fwd := one(t, spans, tracing.SpanForward)
	ft := one(t, spans, tracing.SpanFirstToken)
	comp := one(t, spans, tracing.SpanCompletion)

	if root.Parent.IsValid() {
		t.Error("the root must start a new trace")
	}
	if root.SpanKind != trace.SpanKindServer || fwd.SpanKind != trace.SpanKindClient {
		t.Errorf("kinds: root %v forward %v", root.SpanKind, fwd.SpanKind)
	}
	for _, c := range []tracetest.SpanStub{rl, fwd} {
		if c.Parent.SpanID() != root.SpanContext.SpanID() {
			t.Errorf("%s must be a child of the root", c.Name)
		}
	}
	for _, c := range []tracetest.SpanStub{ft, comp} {
		if c.Parent.SpanID() != fwd.SpanContext.SpanID() {
			t.Errorf("%s must be a child of worker.forward", c.Name)
		}
	}
	for _, s := range spans {
		if s.SpanContext.TraceID() != root.SpanContext.TraceID() {
			t.Errorf("%s is in another trace", s.Name)
		}
		if s.Name != root.Name && (s.StartTime.Before(root.StartTime) || s.EndTime.After(root.EndTime.Add(time.Millisecond))) {
			t.Errorf("%s [%v,%v] is not inside the root [%v,%v]", s.Name, s.StartTime, s.EndTime, root.StartTime, root.EndTime)
		}
	}
	for _, c := range []tracetest.SpanStub{ft, comp} {
		if c.StartTime.Before(fwd.StartTime) || c.EndTime.After(fwd.EndTime.Add(time.Millisecond)) {
			t.Errorf("%s is not inside worker.forward", c.Name)
		}
	}
	if !ft.EndTime.Equal(comp.StartTime) {
		t.Errorf("first_token must end where completion begins: %v vs %v", ft.EndTime, comp.StartTime)
	}
	if got := mustAttr(t, root, tracing.KeyRequestID).AsString(); got != reqID {
		t.Errorf("request id %q, header %q", got, reqID)
	}
	if mustAttr(t, root, tracing.KeyHTTPStatusCode).AsInt64() != 200 || mustAttr(t, root, tracing.KeyAttempts).AsInt64() != 1 ||
		mustAttr(t, root, tracing.KeyModel).AsString() != model || !mustAttr(t, root, tracing.KeyStream).AsBool() {
		t.Errorf("root attributes: %v", root.Attributes)
	}
	if mustAttr(t, rl, tracing.KeyRateOutcome).AsString() != "allowed" {
		t.Errorf("rate limit attributes: %v", rl.Attributes)
	}
	if mustAttr(t, fwd, tracing.KeyAttempt).AsInt64() != 1 || !strings.HasPrefix(mustAttr(t, fwd, tracing.KeyAttemptID).AsString(), "att_") ||
		mustAttr(t, fwd, tracing.KeyAttemptOutcome).AsString() != "ok" {
		t.Errorf("forward attributes: %v", fwd.Attributes)
	}
	if _, ok := attrOf(root, tracing.KeyTTFTMillis); !ok {
		t.Error("the root must carry the request-level TTFT")
	}
	if root.Status.Code != codes.Unset {
		t.Errorf("a 200 leaves the status unset, got %v", root.Status)
	}

	// The worker saw the forward span as its parent, sampled.
	hs := e.up.all()
	if len(hs) != 1 {
		t.Fatalf("worker calls: %d", len(hs))
	}
	want := "00-" + fwd.SpanContext.TraceID().String() + "-" + fwd.SpanContext.SpanID().String() + "-01"
	if got := hs[0].Get("Traceparent"); got != want {
		t.Errorf("traceparent %q, want %q", got, want)
	}
	if hs[0].Get("Tracestate") != "" {
		t.Error("tracestate must be absent unless incoming=trust")
	}
	e.noLeak()
}

func TestNonStreamingRequestHasNoStreamSpans(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.post(plainBody)
	spans := e.spans()
	if got := names(spans); len(got) != 2 {
		t.Fatalf("want gateway.receive and worker.forward only, got %v", got)
	}
	if len(byName(spans, tracing.SpanRateLimit)) != 0 || len(byName(spans, tracing.SpanSelect)) != 0 {
		t.Errorf("rate limiting is off and static mode has no selection: %v", names(spans))
	}
}

func TestIncomingTraceContextPolicy(t *testing.T) {
	const clientTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	const clientSpan = "00f067aa0ba902b7"
	tp := "00-" + clientTrace + "-" + clientSpan + "-01"
	cases := []struct {
		name, mode string
		wantLink   bool
		wantSame   bool
	}{
		{"link", config.TraceIncomingLink, true, false},
		{"default is link", "", true, false},
		{"ignore", config.TraceIncomingIgnore, false, false},
		{"trust", config.TraceIncomingTrust, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t, envOpts{incoming: c.mode})
			e.post(plainBody, "Traceparent", tp, "Tracestate", "vendor=canary-state,other=x")
			spans := e.spans()
			root := one(t, spans, tracing.SpanReceive)
			same := root.SpanContext.TraceID().String() == clientTrace
			if same != c.wantSame {
				t.Fatalf("same trace = %v, want %v", same, c.wantSame)
			}
			if c.wantSame {
				if root.Parent.SpanID().String() != clientSpan || !root.Parent.IsRemote() {
					t.Errorf("trust mode must parent the root to the client's span: %v", root.Parent)
				}
			} else if root.Parent.IsValid() {
				t.Errorf("a new trace has no parent: %v", root.Parent)
			}
			if c.wantLink {
				if len(root.Links) != 1 || root.Links[0].SpanContext.TraceID().String() != clientTrace || root.Links[0].SpanContext.SpanID().String() != clientSpan {
					t.Fatalf("links: %+v", root.Links)
				}
				l := root.Links[0].SpanContext
				if l.TraceFlags() != 0 || l.TraceState().Len() != 0 {
					t.Errorf("the client's flags and tracestate must not be copied: %v", l)
				}
			} else if len(root.Links) != 0 {
				t.Errorf("unexpected links: %+v", root.Links)
			}
			got := e.up.all()[0]
			if c.wantSame {
				if !strings.Contains(got.Get("Tracestate"), "vendor=canary-state") {
					t.Errorf("trust mode forwards tracestate, got %q", got.Get("Tracestate"))
				}
			} else if got.Get("Tracestate") != "" {
				t.Errorf("tracestate forwarded in %s mode: %q", c.name, got.Get("Tracestate"))
			}
			if got.Get("Traceparent") == tp {
				t.Error("the client's traceparent must never be forwarded verbatim")
			}
		})
	}
}

func TestHostileTraceHeadersAreIgnored(t *testing.T) {
	e := newEnv(t, envOpts{})
	bad := []string{
		"garbage", "00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-01", strings.Repeat("a", 4000),
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	for _, h := range bad {
		resp, _ := e.post(plainBody, "Traceparent", h, "Tracestate", strings.Repeat("k=v,", 500))
		if resp.StatusCode != 200 {
			t.Fatalf("a bad traceparent %.20q must not fail the request: %d", h, resp.StatusCode)
		}
	}
	for _, r := range byName(e.spans(), tracing.SpanReceive) {
		if len(r.Links) != 0 {
			t.Errorf("a malformed traceparent produced a link: %+v", r.Links)
		}
	}
}

func TestUnsampledRequestRecordsNothingButStillPropagates(t *testing.T) {
	e := newEnv(t, envOpts{unsampled: true})
	resp, _ := e.post(plainBody, "Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(e.exp.GetSpans()); n != 0 || len(e.rec.Started()) > 2 {
		t.Errorf("a client's sampled=1 must not force recording in link mode: exported %d", n)
	}
	got := e.up.all()[0].Get("Traceparent")
	if len(got) != 55 || !strings.HasSuffix(got, "-00") {
		t.Errorf("an unsampled request still propagates, with flags 00: %q", got)
	}
	// trace_id is in the log even though nothing was recorded.
	traceID := got[3:35]
	if !strings.Contains(e.logs.String(), `"trace_id":"`+traceID+`"`) {
		t.Errorf("the request log must carry the trace id of an unsampled request: %s", e.logs.String())
	}
}

func TestLogsCarryTheSameTraceIDAsTheSpans(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.post(streamBody)
	root := one(t, e.spans(), tracing.SpanReceive)
	id := root.SpanContext.TraceID().String()
	if n := strings.Count(e.logs.String(), `"trace_id":"`+id+`"`); n < 1 {
		t.Errorf("no log line carries trace id %s: %s", id, e.logs.String())
	}
}

func TestRefusalsEndTheRootExactlyOnceWithTheRightStatus(t *testing.T) {
	cases := []struct {
		name       string
		opts       envOpts
		body       string
		wantStatus int
		wantCode   string // the exact serverflow.error_code
		wantSpans  []string
	}{
		{"validation", envOpts{}, `{"model":""}`, 400, "INVALID_REQUEST", []string{tracing.SpanReceive}},
		{"unknown model", envOpts{}, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`, 404, "MODEL_NOT_FOUND", []string{tracing.SpanReceive}},
		{"body too large", envOpts{}, `{"model":"qwen-7b","messages":[{"role":"user","content":"` + strings.Repeat("x", 2<<20) + `"}]}`, 413, "INVALID_REQUEST", []string{tracing.SpanReceive}},
		{"rate limited", envOpts{gwOpts: []gateway.Option{gateway.WithLimiter(allowLimiter{ratelimit.Decision{Allowed: false, Limit: ratelimit.LimitRequests, RetryAfter: time.Second}})}},
			plainBody, 429, "RATE_LIMITED", []string{tracing.SpanReceive, tracing.SpanRateLimit}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t, c.opts)
			resp, _ := e.post(c.body)
			if resp.StatusCode != c.wantStatus {
				t.Fatalf("status %d", resp.StatusCode)
			}
			spans := e.spans()
			if got := names(spans); len(got) != len(c.wantSpans) {
				t.Fatalf("spans %v, want %v", got, c.wantSpans)
			}
			root := one(t, spans, tracing.SpanReceive)
			if mustAttr(t, root, tracing.KeyHTTPStatusCode).AsInt64() != int64(c.wantStatus) {
				t.Errorf("status attribute: %v", root.Attributes)
			}
			if root.Status.Code != codes.Unset {
				t.Errorf("a 4xx is not a server error: %v", root.Status)
			}
			if got := mustAttr(t, root, tracing.KeyErrorCode).AsString(); got != c.wantCode {
				t.Errorf("serverflow.error_code = %q, want %q", got, c.wantCode)
			}
			if len(e.up.all()) != 0 {
				t.Error("a refused request must never reach the worker")
			}
			e.noLeak()
		})
	}
}

func TestRateLimitRefusalSpan(t *testing.T) {
	e := newEnv(t, envOpts{gwOpts: []gateway.Option{gateway.WithLimiter(allowLimiter{ratelimit.Decision{Allowed: false, Limit: ratelimit.LimitTokens, RetryAfter: time.Second}})}})
	e.post(plainBody)
	spans := e.spans()
	rl := one(t, spans, tracing.SpanRateLimit)
	root := one(t, spans, tracing.SpanReceive)
	if got := mustAttr(t, root, tracing.KeyErrorCode).AsString(); got != "RATE_LIMITED" {
		t.Errorf("error code %q", got)
	}
	if mustAttr(t, rl, tracing.KeyRateOutcome).AsString() != "rejected" || mustAttr(t, rl, tracing.KeyRateLimit).AsString() != "tokens" {
		t.Errorf("attributes: %v", rl.Attributes)
	}
	if mustAttr(t, root, tracing.KeyRejectKind).AsString() != "rate_limit" || mustAttr(t, root, tracing.KeyErrorCode).AsString() == "" {
		t.Errorf("root attributes: %v", root.Attributes)
	}
	if rl.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Error("rate_limit must be a child of the root")
	}
}

func TestUpstreamDownIsAnErrorSpan(t *testing.T) {
	e := newEnv(t, envOpts{upstream: func(s *seen) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.add(r.Header)
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			_ = c.Close() // drop the connection
		})
	}})
	resp, _ := e.post(plainBody)
	if resp.StatusCode < 500 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	spans := e.spans()
	root, fwd := one(t, spans, tracing.SpanReceive), one(t, spans, tracing.SpanForward)
	if root.Status.Code != codes.Error || fwd.Status.Code != codes.Error {
		t.Errorf("root %v forward %v", root.Status, fwd.Status)
	}
	code := mustAttr(t, root, tracing.KeyErrorCode).AsString()
	if code == "other" || code == "" || root.Status.Description != code || strings.ContainsAny(code, " :/") {
		t.Errorf("the status description must be the real error code, got description %q code %q", root.Status.Description, code)
	}
	if code != "WORKER_UNAVAILABLE" && code != "INFERENCE_FAILED" {
		t.Errorf("unexpected code %q for a dropped connection", code)
	}
	e.noLeak()
}

func TestMidstreamFailureEndsEverything(t *testing.T) {
	e := newEnv(t, envOpts{upstream: func(s *seen) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.add(r.Header)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"x\":1}\n\n"))
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			_ = c.Close()
		})
	}})
	e.post(streamBody)
	spans := e.spans()
	root, comp := one(t, spans, tracing.SpanReceive), one(t, spans, tracing.SpanCompletion)
	if root.Status.Code != codes.Error || root.Status.Description != "INFERENCE_FAILED" || mustAttr(t, root, tracing.KeyErrorCode).AsString() != "INFERENCE_FAILED" {
		t.Errorf("a failed stream is an INFERENCE_FAILED error: %v %v", root.Status, root.Attributes)
	}
	if comp.EndTime.After(root.EndTime.Add(time.Millisecond)) {
		t.Error("completion outlived the root")
	}
	e.noLeak()
}

func TestClientDisconnectIsNotAnError(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	e := newEnv(t, envOpts{upstream: func(s *seen) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.add(r.Header)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"x\":1}\n\n"))
			w.(http.Flusher).Flush()
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
	}})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/v1/chat/completions", strings.NewReader(streamBody))
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_, _ = resp.Body.Read(buf) // the first chunk reached us
	cancel()
	_ = resp.Body.Close()
	spans := e.spans()
	root, fwd := one(t, spans, tracing.SpanReceive), one(t, spans, tracing.SpanForward)
	if root.Status.Code != codes.Unset || !mustAttr(t, root, tracing.KeyClientClosed).AsBool() || mustAttr(t, root, tracing.KeyHTTPStatusCode).AsInt64() != 499 {
		t.Errorf("root: status %v attributes %v", root.Status, root.Attributes)
	}
	if mustAttr(t, fwd, tracing.KeyAttemptOutcome).AsString() != "client_closed" {
		t.Errorf("forward: %v", fwd.Attributes)
	}
	e.noLeak()
}

func TestBoundedAttributes(t *testing.T) {
	e := newEnv(t, envOpts{})
	huge := `{"model":"` + strings.Repeat("m", 1<<20) + `","messages":[{"role":"user","content":"x"}]}`
	e.post(huge, "Traceparent", strings.Repeat("x", 100000))
	e.post(plainBody)
	for _, s := range e.spans() {
		for _, kv := range s.Attributes {
			if kv.Value.Type() == attribute.STRING && len(kv.Value.AsString()) > 128 {
				t.Errorf("%s.%s is %d bytes", s.Name, kv.Key, len(kv.Value.AsString()))
			}
		}
	}
}

func TestTenantIDSwitch(t *testing.T) {
	// The tenant attribute is added by RequestAdmitted/Completion; with the switch off it must never appear.
	e := newEnv(t, envOpts{noTenant: true})
	e.post(plainBody)
	for _, s := range e.spans() {
		if _, ok := attrOf(s, tracing.KeyTenantID); ok {
			t.Errorf("%s carries a tenant id although include_tenant_id is off", s.Name)
		}
	}
}

// emptyStore knows no keys.
type emptyStore struct{}

func (emptyStore) LookupKey(context.Context, string) (auth.KeyRecord, error) {
	return auth.KeyRecord{}, auth.ErrNotFound
}

func TestAuthRefusalGetsARootSpan(t *testing.T) {
	a := auth.New(emptyStore{}, auth.Config{CacheTTL: time.Minute, NegativeTTL: time.Minute, CacheSize: 10, StaleGrace: time.Minute})
	e := newEnv(t, envOpts{gwOpts: []gateway.Option{gateway.WithAuthenticator(a)}})
	key, _, _ := auth.GenerateKey()
	resp, _ := e.post(plainBody, "Authorization", "Bearer "+key)
	if resp.StatusCode != 401 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	spans := e.spans()
	if len(spans) != 1 {
		t.Fatalf("a refused key gets only the root span: %v", names(spans))
	}
	root := spans[0]
	if got := mustAttr(t, root, tracing.KeyErrorCode).AsString(); got != "UNAUTHORIZED" {
		t.Errorf("error code %q", got)
	}
	if mustAttr(t, root, tracing.KeyRejectKind).AsString() != "auth" || mustAttr(t, root, tracing.KeyHTTPStatusCode).AsInt64() != 401 {
		t.Errorf("attributes: %v", root.Attributes)
	}
	if strings.Contains(tracingtest.Dump(spans), key[:12]) {
		t.Error("the API key (or its prefix) leaked into a span")
	}
	e.noLeak()
}
