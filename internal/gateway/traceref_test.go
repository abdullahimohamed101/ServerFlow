package gateway

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
)

const (
	testTraceID     = "0af7651916cd43dd8448eb211c80319c"
	testTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
)

// refObserver gives the request and each attempt a TraceRef, like a tracing observer does.
type refObserver struct {
	NopObserver
	tracestate string
}

func (o refObserver) RequestStarted(ctx context.Context, _ RequestStart) context.Context {
	return WithTraceRef(ctx, TraceRef{TraceID: testTraceID, Traceparent: "00-" + testTraceID + "-1111111111111111-01"})
}

func (o refObserver) AttemptStarted(ctx context.Context, _ AttemptStart) context.Context {
	return WithTraceRef(ctx, TraceRef{TraceID: testTraceID, Traceparent: testTraceparent, Tracestate: o.tracestate})
}

// headerCapture records the headers the worker received.
type headerCapture struct {
	mu   sync.Mutex
	last http.Header
}

func (h *headerCapture) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.last = r.Header.Clone()
		h.mu.Unlock()
		okUpstream(w, r)
	})
}

func (h *headerCapture) get() http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.last
}

func TestOnlyTheObserversTraceContextReachesTheWorker(t *testing.T) {
	cap := &headerCapture{}
	url, c, _ := staticObsServer(t, cap.handler(), WithObserver(refObserver{tracestate: "vendor=value"}))
	resp, _ := postChat(t, c, url, plainBody,
		"Traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", "Tracestate", "client=canary-state", "Baggage", "k=canary-baggage")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got := cap.get()
	if got.Get("Traceparent") != testTraceparent || got.Get("Tracestate") != "vendor=value" {
		t.Errorf("worker saw traceparent=%q tracestate=%q", got.Get("Traceparent"), got.Get("Tracestate"))
	}
	if got.Get("Baggage") != "" {
		t.Error("baggage must never cross")
	}
}

func TestNoTraceContextWithoutATracingObserver(t *testing.T) {
	cap := &headerCapture{}
	url, c, logs := staticObsServer(t, cap.handler())
	postChat(t, c, url, plainBody, "Traceparent", testTraceparent, "Tracestate", "a=b")
	got := cap.get()
	if got.Get("Traceparent") != "" || got.Get("Tracestate") != "" {
		t.Errorf("a client's trace headers must not be forwarded: %v", got)
	}
	for _, l := range logs.logLines(t) {
		if _, ok := l["trace_id"]; ok {
			t.Errorf("trace_id must be absent when tracing is off: %v", l)
		}
	}
}

func TestOversizedTraceparentIsNotForwarded(t *testing.T) {
	cap := &headerCapture{}
	long := TraceRef{TraceID: testTraceID, Traceparent: testTraceparent + strings.Repeat("x", 10)}
	obs := &funcObserver{attempt: func(ctx context.Context) context.Context { return WithTraceRef(ctx, long) }}
	url, c, _ := staticObsServer(t, cap.handler(), WithObserver(obs))
	postChat(t, c, url, plainBody)
	if v := cap.get().Get("Traceparent"); v != "" {
		t.Errorf("a traceparent over 55 bytes was forwarded: %q", v)
	}
}

type funcObserver struct {
	NopObserver
	attempt func(context.Context) context.Context
}

func (f *funcObserver) AttemptStarted(ctx context.Context, _ AttemptStart) context.Context {
	return f.attempt(ctx)
}

func TestLogsCarryTheTraceID(t *testing.T) {
	url, c, logs := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(refObserver{}))
	postChat(t, c, url, plainBody)
	var requestLine map[string]any
	eventually(t, 5e9, func() bool {
		for _, l := range logs.logLines(t) {
			if l["msg"] == "request" {
				requestLine = l
				return true
			}
		}
		return false
	}, "the request log line")
	if requestLine["trace_id"] != testTraceID {
		t.Errorf("request log line: %v", requestLine)
	}
}

func TestDuplicateTraceparentHeadersAreDropped(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(okUpstream), WithObserver(rec))
	req, _ := http.NewRequest(http.MethodPost, url+chatCompletionsPath, strings.NewReader(plainBody))
	req.Header.Add("Traceparent", testTraceparent)
	req.Header.Add("Traceparent", testTraceparent)
	req.Header.Set("Tracestate", "a=b")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	rec.await(t)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if th := rec.starts[0].TraceHeaders; th.Traceparent != "" || th.Tracestate != "a=b" {
		t.Errorf("trace headers: %+v", th)
	}
}
