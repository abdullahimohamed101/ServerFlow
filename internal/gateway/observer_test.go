package gateway

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/ratelimit"
	"serverflow/pkg/protocol"
)

// recObserver records the lifecycle as short tokens (and the full events) so tests can pin the exact call
// sequence. Optional hooks put values in the contexts it returns (plan amendment A1).
type recObserver struct {
	NopObserver
	mu     sync.Mutex
	seq    []string
	starts []RequestStart
	adm    []Admission
	rej    []Rejection
	att    []AttemptStart
	first  []FirstToken
	ends   []AttemptEnd
	done   []Completion
	// ctxSeen records, per event, whether the context carried the values this observer injected.
	ctxSeen map[string]string
}

type obsCtxKey string

func (r *recObserver) add(tok string) {
	r.mu.Lock()
	r.seq = append(r.seq, tok)
	r.mu.Unlock()
}

func (r *recObserver) see(ev string, ctx context.Context) {
	r.mu.Lock()
	if r.ctxSeen == nil {
		r.ctxSeen = map[string]string{}
	}
	req, _ := ctx.Value(obsCtxKey("req")).(string)
	att, _ := ctx.Value(obsCtxKey("att")).(string)
	r.ctxSeen[ev] = req + "|" + att
	r.mu.Unlock()
}

func (r *recObserver) RequestStarted(ctx context.Context, e RequestStart) context.Context {
	r.mu.Lock()
	r.starts = append(r.starts, e)
	r.mu.Unlock()
	r.add("start")
	return context.WithValue(ctx, obsCtxKey("req"), "R-"+e.ID)
}

func (r *recObserver) RequestAdmitted(ctx context.Context, e Admission) {
	r.mu.Lock()
	r.adm = append(r.adm, e)
	r.mu.Unlock()
	r.see("admit", ctx)
	r.add("admit")
}

func (r *recObserver) RequestRejected(ctx context.Context, e Rejection) {
	r.mu.Lock()
	r.rej = append(r.rej, e)
	r.mu.Unlock()
	r.see("reject", ctx)
	r.add(fmt.Sprintf("reject:%s:%s:%d", e.Kind, e.Reason, e.Status))
}

func (r *recObserver) AttemptStarted(ctx context.Context, e AttemptStart) context.Context {
	r.mu.Lock()
	r.att = append(r.att, e)
	r.mu.Unlock()
	r.add(fmt.Sprintf("attempt+%d:%s", e.Number, e.WorkerID))
	return context.WithValue(ctx, obsCtxKey("att"), "A-"+e.AttemptID)
}

func (r *recObserver) FirstToken(ctx context.Context, e FirstToken) {
	r.mu.Lock()
	r.first = append(r.first, e)
	r.mu.Unlock()
	r.see("first", ctx)
	r.add("first")
}

func (r *recObserver) AttemptEnded(ctx context.Context, e AttemptEnd) {
	r.mu.Lock()
	r.ends = append(r.ends, e)
	r.mu.Unlock()
	r.see(fmt.Sprintf("end%d", e.Number), ctx)
	tok := fmt.Sprintf("attempt-%d:%s:%s", e.Number, e.WorkerID, e.Outcome)
	if e.WillRetry {
		tok += ":retry"
	}
	r.add(tok)
}

func (r *recObserver) RequestCompleted(ctx context.Context, e Completion) {
	r.mu.Lock()
	r.done = append(r.done, e)
	r.mu.Unlock()
	r.see("done", ctx)
	r.add(fmt.Sprintf("done:%d", e.Status))
}

func (r *recObserver) sequence() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seq...)
}

// await waits for the request to complete (the completion fires just after the response is written) and
// returns the sequence.
func (r *recObserver) await(t *testing.T) []string {
	t.Helper()
	eventually(t, 5*time.Second, func() bool {
		s := r.sequence()
		return len(s) > 0 && strings.HasPrefix(s[len(s)-1], "done:")
	}, "RequestCompleted")
	return r.sequence()
}

func wantSeq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observer call sequence\n got: %v\nwant: %v", got, want)
	}
}

// staticObsServer is a static-mode gateway (no auth, no limiter) in front of up, with the observers attached.
func staticObsServer(t *testing.T, up http.Handler, opts ...Option) (string, *http.Client, *lockedBuffer) {
	t.Helper()
	fake := httptest.NewServer(up)
	t.Cleanup(fake.Close)
	logs := &lockedBuffer{}
	gw := New(testConfig(fake.URL), slog.New(slog.NewJSONHandler(logs, nil)), opts...)
	url, c := serve(t, gw)
	return url, c, logs
}

func postChat(t *testing.T, c *http.Client, url, body string, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+chatCompletionsPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

const (
	plainBody  = `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`
	streamBody = `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hi"}]}`
)

// --- the observer contract -----------------------------------------------------------------------------------------

type panicObserver struct {
	NopObserver
	calls int
}

func (p *panicObserver) RequestStarted(ctx context.Context, _ RequestStart) context.Context {
	p.calls++
	panic("observer blew up")
}
func (p *panicObserver) RequestCompleted(context.Context, Completion) { panic("again") }
func (p *panicObserver) AttemptStarted(context.Context, AttemptStart) context.Context {
	return nil // a nil context must not poison the request
}

func TestAPanickingObserverDoesNotFailTheRequestOrStopTheOthers(t *testing.T) {
	rec := &recObserver{}
	url, c, logs := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(&panicObserver{}), WithObserver(rec))
	for i := 0; i < 3; i++ {
		if resp, body := postChat(t, c, url, plainBody); resp.StatusCode != 200 || !strings.Contains(body, "ok") {
			t.Fatalf("request %d: %d %s", i, resp.StatusCode, body)
		}
	}
	eventually(t, 5*time.Second, func() bool { return len(rec.sequence()) >= 15 }, "all three requests observed")
	wantSeq(t, rec.sequence()[:5], "start", "admit", "attempt+1:", "attempt-1::ok", "done:200")

	// Six panics happened (RequestStarted and RequestCompleted, three requests): each method of the
	// observer is logged once, not once per request.
	n := 0
	for _, l := range logs.logLines(t) {
		if l["msg"] == "observer panicked; the request was not affected" {
			n++
			if l["observer"] != "*gateway.panicObserver" {
				t.Fatalf("log must name the observer: %v", l)
			}
		}
	}
	if n < 1 || n > 3 { // one per observer (the log limit is per observer, not per method)
		t.Fatalf("expected the panics to be logged about once, got %d lines", n)
	}
}

func TestPanicLoggingIsLimitedPerObserverPerMinute(t *testing.T) {
	logs := &lockedBuffer{}
	m := newMultiObserver(slog.New(slog.NewJSONHandler(logs, nil)))
	now := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time { return now }
	bad, good := &panicObserver{}, &recObserver{}
	m.add(bad)
	m.add(good)
	count := func() int {
		n := 0
		for _, l := range logs.logLines(t) {
			if l["msg"] == "observer panicked; the request was not affected" {
				n++
			}
		}
		return n
	}
	for i := 0; i < 50; i++ {
		m.RequestStarted(context.Background(), RequestStart{ID: "r"})
	}
	if count() != 1 {
		t.Fatalf("50 panics within a minute must log once, got %d", count())
	}
	if bad.calls != 50 {
		t.Fatalf("a panicking observer is still called every time, got %d calls", bad.calls)
	}
	if len(good.sequence()) != 50 {
		t.Fatalf("the other observer must still run for every event, got %d", len(good.sequence()))
	}
	now = now.Add(61 * time.Second)
	m.RequestStarted(context.Background(), RequestStart{ID: "r"})
	if count() != 2 {
		t.Fatalf("a minute later the observer may be logged again, got %d", count())
	}
}

func TestMultiObserverThreadsTheContextInOrder(t *testing.T) {
	m := newMultiObserver(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	m.add(NopObserver{})
	m.add(&recObserver{})
	got := m.RequestStarted(context.Background(), RequestStart{ID: "x"})
	if got.Value(obsCtxKey("req")) != "R-x" {
		t.Fatal("the context returned by an observer must reach the caller")
	}
	if m.AttemptStarted(context.Background(), AttemptStart{AttemptID: "a"}).Value(obsCtxKey("att")) != "A-a" {
		t.Fatal("AttemptStarted must return the observers' context too")
	}
}

func TestNopObserverCanBeEmbeddedToImplementOneMoment(t *testing.T) {
	var _ Observer = NopObserver{}
	var _ Observer = &recObserver{}
	var _ Observer = (*metrics)(nil)
	var _ Observer = (*multiObserver)(nil)
}

func TestNilObserverIsIgnored(t *testing.T) {
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(nil))
	if resp, _ := postChat(t, c, url, plainBody); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// --- call sequences, static mode ------------------------------------------------------------------------------------

func TestSequenceStaticSuccessNonStreaming(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec))
	postChat(t, c, url, plainBody, "Traceparent", "00-"+strings.Repeat("a", 32)+"-"+strings.Repeat("b", 16)+"-01")
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:", "attempt-1::ok", "done:200")

	// What the events carry.
	s, a, d := rec.starts[0], rec.adm[0], rec.done[0]
	if s.Method != "POST" || s.Path != chatCompletionsPath || s.ID == "" || s.Time.IsZero() || !strings.HasPrefix(s.TraceHeaders.Traceparent, "00-") || s.TraceHeaders.Tracestate != "" {
		t.Fatalf("start: %+v", s)
	}
	if a.RequestID != s.ID || a.Model != "qwen-7b" || a.Stream || a.RateLimitChecked {
		t.Fatalf("admission: %+v", a)
	}
	at, ae := rec.att[0], rec.ends[0]
	if at.WorkerID != "" || at.Number != 1 || at.Strategy != "" || at.AttemptID == "" || at.RequestID != s.ID || ae.AttemptID != at.AttemptID {
		t.Fatalf("static attempt: %+v / %+v", at, ae)
	}
	if d.RequestID != s.ID || d.Status != 200 || d.Model != "qwen-7b" || d.Attempts != 1 || d.ErrorCode != "" || !d.Handled || d.Duration <= 0 {
		t.Fatalf("completion: %+v", d)
	}
}

func TestSequenceStaticSuccessStreaming(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, sseHandler(closedChan()), WithObserver(rec))
	postChat(t, c, url, streamBody)
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:", "first", "attempt-1::ok", "done:200")
	if f := rec.first[0]; f.AttemptID != rec.att[0].AttemptID || f.TTFT <= 0 {
		t.Fatalf("first token: %+v", f)
	}
	if d := rec.done[0]; !d.Stream || d.TTFT != rec.first[0].TTFT {
		t.Fatalf("completion: %+v", d)
	}
}

func closedChan() chan struct{} { c := make(chan struct{}); close(c); return c }

func TestSequenceAuthRefusal(t *testing.T) {
	rec := &recObserver{}
	env := newLimitEnv(t, okUpstream, &fakeLimiter{}, WithObserver(rec))
	resp, _ := env.chat(t, "", "qwen-7b")
	if resp.StatusCode != 401 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	wantSeq(t, rec.await(t), "start", "reject:auth:missing:401", "done:401")
	if d := rec.done[0]; d.Handled || d.ErrorCode == "" {
		t.Fatalf("an authentication refusal never reaches the handler: %+v", d)
	}
}

func TestSequenceRateLimitRefusal(t *testing.T) {
	rec := &recObserver{}
	lim := &fakeLimiter{decide: func(ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{Limit: ratelimit.LimitTokens, RetryAfter: time.Second}, nil
	}}
	env := newLimitEnv(t, okUpstream, lim, WithObserver(rec))
	key := env.store.add("acme", nil, quotas(10, 1000, 2))
	if resp, _ := env.chat(t, key, "qwen-7b"); resp.StatusCode != 429 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	wantSeq(t, rec.await(t), "start", "reject:rate_limit:tokens:429", "done:429")
	if r := rec.rej[0]; r.TenantID == "" || r.DecisionDuration <= 0 {
		t.Fatalf("rejection: %+v", r)
	}
	if d := rec.done[0]; !d.Handled || d.Attempts != 0 {
		t.Fatalf("completion: %+v", d)
	}
}

func TestSequenceUnknownModelStatic(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec))
	postChat(t, c, url, `{"model":"nope","messages":[{"role":"user","content":"hi"}]}`)
	wantSeq(t, rec.await(t), "start", "reject:model:model_not_found:404", "done:404")
	if rec.done[0].Model != "" {
		t.Fatal("an unknown model must never be reported as the confirmed model")
	}
}

func TestSequenceValidationRefusal(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec))
	postChat(t, c, url, `{not json`)
	wantSeq(t, rec.await(t), "start", "reject:validation:invalid_request:400", "done:400")
}

func TestSequenceClientDisconnect(t *testing.T) {
	rec := &recObserver{}
	sawCancel := make(chan struct{})
	url, c, _ := staticObsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(sawCancel)
	}), WithObserver(rec))
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+chatCompletionsPath, strings.NewReader(streamBody))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	readLine(t, bufio.NewReader(resp.Body), 2*time.Second)
	cancel()
	_ = resp.Body.Close()
	<-sawCancel
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:", "first", "attempt-1::client_closed", "done:499")
}

func TestSequenceHandlerPanic(t *testing.T) {
	rec := &recObserver{}
	logs := &lockedBuffer{}
	gw := newWithUpstream(testConfig("http://unused"), slog.New(slog.NewJSONHandler(logs, nil)), panicUpstream{})
	WithObserver(rec)(gw)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+chatCompletionsPath, "application/json", strings.NewReader(plainBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:", "attempt-1::failed", "done:500")
	if d := rec.done[0]; d.ErrorCode != "INTERNAL_ERROR" {
		t.Fatalf("completion: %+v", d)
	}
}

func TestOnlyInferenceRequestsAreObserved(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec))
	for _, p := range []string{"/healthz", "/readyz", "/v1/models", "/metrics"} {
		resp, err := c.Get(url + p)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	if s := rec.sequence(); len(s) != 0 {
		t.Fatalf("operational endpoints are not part of the inference lifecycle: %v", s)
	}
}

func TestTraceHeadersAreCappedAndOtherHeadersNeverReachObservers(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec))
	postChat(t, c, url, plainBody, "Traceparent", strings.Repeat("x", 4000), "Tracestate", "a=b", "Authorization", "Bearer sk-secret")
	rec.await(t)
	s := rec.starts[0]
	if len(s.TraceHeaders.Traceparent) != MaxTraceHeaderBytes || s.TraceHeaders.Tracestate != "a=b" {
		t.Fatalf("trace headers: %d bytes, tracestate %q", len(s.TraceHeaders.Traceparent), s.TraceHeaders.Tracestate)
	}
	if strings.Contains(fmt.Sprintf("%+v%+v%+v%+v", rec.starts, rec.adm, rec.att, rec.done), "sk-secret") {
		t.Fatal("an API key reached an observer")
	}
}

// --- call sequences, registry mode -----------------------------------------------------------------------------------

func TestSequenceRegistryRetryOnASecondWorker(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "a-bad"), good.snapshot("qwen-7b")})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	if resp, body := e.post(t, false); resp.StatusCode != 200 || whoAnswered(t, body) != "b-good" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	// The next worker is found before the first attempt is closed (ADR-012), so the attempts overlap.
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:a-bad", "attempt+2:b-good", "attempt-1:a-bad:retried:retry", "attempt-2:b-good:ok", "done:200")
	if a := rec.att[0]; a.Strategy != "round-robin" || a.WorkerState != string(protocol.StateReady) || a.SelectDuration < 0 || a.SinceRequestStart < 0 {
		t.Fatalf("attempt start: %+v", a)
	}
	if e1, e2 := rec.ends[0], rec.ends[1]; e1.Class != "connect" || !e1.WillRetry || e2.WillRetry || e2.NextWorkerUnavailable || e1.AttemptID != rec.att[0].AttemptID {
		t.Fatalf("attempt ends: %+v / %+v", e1, e2)
	}
	if d := rec.done[0]; d.Attempts != 2 || d.Model != "qwen-7b" {
		t.Fatalf("completion: %+v", d)
	}
}

func TestSequenceRegistryNoSecondWorkerSaysSo(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{"error":{"code":"x"}}`))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b")})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	if resp, _ := e.post(t, false); resp.StatusCode != 503 {
		t.Fatalf("the worker's own answer is the client's: %d", resp.StatusCode)
	}
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:a-bad", "attempt-1:a-bad:failed", "done:503")
	if end := rec.ends[0]; end.WillRetry || !end.NextWorkerUnavailable || end.Class != "status_503" {
		t.Fatalf("attempt end: %+v", end)
	}
}

func TestSequenceRegistryUnknownModel(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	if resp, _ := e.chat(t, "no-such-model"); resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// In registry mode the model is only confirmed by routing, after admission.
	wantSeq(t, rec.await(t), "start", "admit", "reject:model:model_not_found:404", "done:404")
	if rec.done[0].Model != "" {
		t.Fatal("an unconfirmed model must not be reported as the model")
	}
}

func TestSequenceRegistryNoWorkerIsACapacityRejection(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4)
	sa.State, sa.Eligible = protocol.StateDraining, false
	sb.State, sb.Eligible, sb.Health = protocol.StateUnhealthy, false, protocol.HealthUnhealthy
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sa, sb})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	if resp, _ := e.chat(t, "qwen-7b"); resp.StatusCode != 503 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	wantSeq(t, rec.await(t), "start", "admit", "reject:capacity:no_capacity:503", "done:503")
	if d := rec.done[0]; d.Model != "qwen-7b" || d.Attempts != 0 || d.ErrorCode != "NO_CAPACITY" {
		t.Fatalf("a capacity refusal confirms the model: %+v", d)
	}
}

// --- context threading (A1) --------------------------------------------------------------------------------------

// ctxUpstream reports the observer-injected values it sees on the context of the upstream call.
type ctxUpstream struct {
	mu   sync.Mutex
	seen []string
}

func (u *ctxUpstream) Do(ctx context.Context, _ string, _ []byte, _ string) (*http.Response, error) {
	req, _ := ctx.Value(obsCtxKey("req")).(string)
	att, _ := ctx.Value(obsCtxKey("att")).(string)
	u.mu.Lock()
	u.seen = append(u.seen, req+"|"+att)
	u.mu.Unlock()
	return cannedUpstream{}.Do(ctx, "", nil, "")
}
func (*ctxUpstream) Probe(context.Context) error { return nil }

func TestContextFromObserversReachesTheUpstreamCall(t *testing.T) {
	up := &ctxUpstream{}
	rec := &recObserver{}
	gw := newWithUpstream(testConfig("http://unused"), slog.New(slog.NewJSONHandler(io.Discard, nil)), up)
	WithObserver(rec)(gw)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+chatCompletionsPath, "application/json", strings.NewReader(plainBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	rec.await(t)

	reqVal, attVal := "R-"+rec.starts[0].ID, "A-"+rec.att[0].AttemptID
	if len(up.seen) != 1 || up.seen[0] != reqVal+"|"+attVal {
		t.Fatalf("the upstream call must carry the request and attempt contexts the observers returned: %v (want %s|%s)", up.seen, reqVal, attVal)
	}
	for ev, want := range map[string]string{"admit": reqVal + "|", "end1": reqVal + "|" + attVal, "done": reqVal + "|"} {
		if rec.ctxSeen[ev] != want {
			t.Errorf("context at %s = %q, want %q", ev, rec.ctxSeen[ev], want)
		}
	}
}

func TestAttemptContextReachesFirstTokenAndEndInRegistryMode(t *testing.T) {
	sw := newScriptWorker(t, "a-w", sseEvents("a-w", 2))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sw.snapshot("qwen-7b")})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	// Wrap the worker transport to see the context of the upstream call.
	var mu sync.Mutex
	var seen []string
	e.gw.router.client.Transport = roundTripFunc(func(inner http.RoundTripper) func(*http.Request) (*http.Response, error) {
		return func(r *http.Request) (*http.Response, error) {
			req, _ := r.Context().Value(obsCtxKey("req")).(string)
			att, _ := r.Context().Value(obsCtxKey("att")).(string)
			mu.Lock()
			seen = append(seen, req+"|"+att)
			mu.Unlock()
			return inner.RoundTrip(r)
		}
	}(e.gw.router.client.Transport))
	if resp, _ := e.post(t, true); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	rec.await(t)
	reqVal, attVal := "R-"+rec.starts[0].ID, "A-"+rec.att[0].AttemptID
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != reqVal+"|"+attVal {
		t.Fatalf("worker call context %v, want %s|%s", seen, reqVal, attVal)
	}
	if rec.ctxSeen["first"] != reqVal+"|"+attVal || rec.ctxSeen["end1"] != reqVal+"|"+attVal {
		t.Fatalf("FirstToken and AttemptEnded must get the attempt context: %v", rec.ctxSeen)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// --- ordering, rate-limit timing and tenant on completion --------------------------------------------------------

// gaugeProbe reads the in-flight gauge from inside the lifecycle, which only works if metrics ran first.
type gaugeProbe struct {
	NopObserver
	gw           *Server
	mu           sync.Mutex
	atStart      float64
	atCompleted  float64
	sawRequestID bool
}

func (g *gaugeProbe) RequestStarted(ctx context.Context, _ RequestStart) context.Context {
	g.mu.Lock()
	g.atStart = gaugeValue(g.gw)
	g.sawRequestID = infoFrom(ctx).id != ""
	g.mu.Unlock()
	return ctx
}

func (g *gaugeProbe) RequestCompleted(context.Context, Completion) {
	g.mu.Lock()
	g.atCompleted = gaugeValue(g.gw)
	g.mu.Unlock()
}

func gaugeValue(s *Server) float64 {
	fams, _ := s.metrics.reg.Gather()
	for _, f := range fams {
		if f.GetName() == "inference_requests_active" {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	return -1
}

func TestMetricsIsTheFirstObserverEvenWithUserObservers(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(okUpstream))
	t.Cleanup(fake.Close)
	probe := &gaugeProbe{}
	gw := New(testConfig(fake.URL), slog.New(slog.NewJSONHandler(io.Discard, nil)), WithObserver(probe))
	probe.gw = gw
	if gw.obs.obs[0].o != Observer(gw.metrics) {
		t.Fatalf("metrics must be registered first, got %s", gw.obs.obs[0].name)
	}
	url, c := serve(t, gw)
	postChat(t, c, url, plainBody)
	eventually(t, 5*time.Second, func() bool {
		probe.mu.Lock()
		defer probe.mu.Unlock()
		return probe.atCompleted == 0 && probe.atStart == 1
	}, "gauge 1 at start and 0 at completion as seen by a later observer")
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if !probe.sawRequestID {
		t.Fatal("a later observer must receive the context metrics returned (it carries the request state)")
	}
}

// histogramCount returns the sample count of a histogram family, or 0 if it has no samples yet.
func histogramCount(t *testing.T, s *Server, name string) uint64 {
	t.Helper()
	fams, err := s.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func TestRateLimitRefusalsRecordTheDecisionTime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		decide func(ratelimit.Request) (ratelimit.Decision, error)
		status int
	}{
		{"limited", func(ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{Limit: ratelimit.LimitRequests, RetryAfter: time.Second}, nil
		}, 429},
		{"unavailable", func(ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, fmt.Errorf("redis down")
		}, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newLimitEnv(t, okUpstream, &fakeLimiter{decide: tc.decide})
			key := env.store.add("acme", nil, quotas(10, 1000, 2))
			before := histogramCount(t, env.gw, "rate_limit_decision_seconds")
			if resp, _ := env.chat(t, key, "qwen-7b"); resp.StatusCode != tc.status {
				t.Fatalf("status %d", resp.StatusCode)
			}
			eventually(t, 5*time.Second, func() bool {
				return histogramCount(t, env.gw, "rate_limit_decision_seconds") == before+1
			}, "one decision recorded for a refused request")
		})
	}
}

func TestCompletionCarriesTheTenant(t *testing.T) {
	// Authenticated and admitted.
	rec := &recObserver{}
	env := newLimitEnv(t, okUpstream, &fakeLimiter{}, WithObserver(rec))
	key := env.store.add("acme", nil, quotas(10, 1000, 2))
	env.chat(t, key, "qwen-7b")
	rec.await(t)
	if tenant := rec.done[0].TenantID; tenant == "" || tenant != rec.adm[0].TenantID {
		t.Fatalf("completion tenant %q, admission tenant %q", rec.done[0].TenantID, rec.adm[0].TenantID)
	}

	// Rejected after authentication.
	rec2 := &recObserver{}
	lim := &fakeLimiter{decide: func(ratelimit.Request) (ratelimit.Decision, error) {
		return ratelimit.Decision{Limit: ratelimit.LimitRequests, RetryAfter: time.Second}, nil
	}}
	env2 := newLimitEnv(t, okUpstream, lim, WithObserver(rec2))
	key2 := env2.store.add("acme", nil, quotas(10, 1000, 2))
	env2.chat(t, key2, "qwen-7b")
	rec2.await(t)
	if rec2.done[0].TenantID == "" || rec2.done[0].TenantID != rec2.rej[0].TenantID {
		t.Fatalf("a refusal after authentication still names the tenant: %+v", rec2.done[0])
	}

	// Authentication off: no tenant.
	rec3 := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec3))
	postChat(t, c, url, plainBody)
	rec3.await(t)
	if rec3.done[0].TenantID != "" {
		t.Fatalf("no authentication, no tenant: %q", rec3.done[0].TenantID)
	}
}
