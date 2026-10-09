package driver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/workload"
)

func wl(t *testing.T, name string, stream float64) *workload.Workload {
	t.Helper()
	w, err := workload.New(workload.Spec{Name: name, Seed: 1, Models: workload.Models{Primary: "m", Secondary: "m2"}, StreamRatio: stream})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// okJSON answers a non-streaming chat completion with usage.
func okJSON(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"hello world"}}],"usage":{"prompt_tokens":11,"completion_tokens":22}}`))
}

func serve(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL
}

func closedCfg(url string, w *workload.Workload, clients int, warmup, dur time.Duration) Config {
	return Config{BaseURL: url, Workload: w, Mode: Closed, Concurrency: clients, Warmup: warmup, Duration: dur, DrainTimeout: 5 * time.Second}
}

func TestClosedLoopRecordsEveryRequestOnceAndStopsAtTheEndOfTheWindow(t *testing.T) {
	var served atomic.Int64
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { served.Add(1); okJSON(w) })
	out, err := Run(context.Background(), closedCfg(url, wl(t, workload.UniformShort, 0), 4, 0, 300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(out.Records)) != served.Load() || len(out.Records) == 0 {
		t.Fatalf("records %d, requests served %d: none may be lost or counted twice", len(out.Records), served.Load())
	}
	seen := map[int]bool{}
	for _, r := range out.Records {
		if seen[r.Seq] {
			t.Fatalf("request %d recorded twice", r.Seq)
		}
		seen[r.Seq] = true
		if r.Intended > r.Started {
			t.Fatal("a request cannot start before it was due")
		}
		if r.Intended >= 300*time.Millisecond {
			t.Fatalf("request due at %v, after the window", r.Intended)
		}
		if !r.OK() || r.Done < r.Started || !r.UsageReported || r.OutputTokens != 22 || r.InputTokens != 11 {
			t.Fatalf("%+v", r)
		}
	}
	for i := 0; i < len(seen); i++ { // indices are dense: 0..n-1
		if !seen[i] {
			t.Fatalf("request index %d missing among %d", i, len(seen))
		}
	}
	s := collect.SummarizeRecords(out.Records, out.Window)
	if s.Sent != s.Succeeded+s.Failed || s.Sent != len(out.Records) {
		t.Fatalf("%+v", s)
	}
}

func TestWarmupRequestsAreRecordedButNotCounted(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { okJSON(w) })
	out, err := Run(context.Background(), closedCfg(url, wl(t, workload.UniformShort, 0), 2, 150*time.Millisecond, 150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if out.Window.Start != 150*time.Millisecond || out.Window.End != 300*time.Millisecond {
		t.Fatalf("window %+v", out.Window)
	}
	warm, measured := 0, 0
	for _, r := range out.Records {
		if r.Intended < out.Window.Start {
			warm++
		} else {
			measured++
		}
	}
	s := collect.SummarizeRecords(out.Records, out.Window)
	if warm == 0 || measured == 0 || s.Warmup != warm || s.Sent != measured {
		t.Fatalf("warm-up %d measured %d; summary %+v", warm, measured, s)
	}
}

func TestRequestsInFlightAtTheEndAreDrainedAndCounted(t *testing.T) {
	var served atomic.Int64
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		time.Sleep(300 * time.Millisecond)
		okJSON(w)
	})
	out, err := Run(context.Background(), closedCfg(url, wl(t, workload.UniformShort, 0), 3, 0, 100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	// Every request sent inside the window is recorded (a loaded machine may start a client after
	// the window closed, so the count is not fixed), none twice, none lost.
	if len(out.Records) == 0 || int64(len(out.Records)) != served.Load() {
		t.Fatalf("%d records, %d served", len(out.Records), served.Load())
	}
	s := collect.SummarizeRecords(out.Records, out.Window)
	for _, r := range out.Records {
		if !r.OK() {
			t.Fatalf("a drained request must complete: %+v", r)
		}
		if r.Intended < out.Window.End && r.Done <= out.Window.End {
			t.Fatalf("each request takes 300ms, so one due inside a 100ms window ends after it: %+v", r)
		}
	}
	if s.Succeeded != len(out.Records) || s.SpanSeconds <= s.WindowSeconds {
		t.Fatalf("%+v", s)
	}
	if s.CompletedInWindow != 0 {
		t.Fatalf("nothing finishes inside the window, so the windowed throughput counts nothing: %+v", s)
	}
}

func TestRequestsStillRunningAfterTheDrainTimeoutAreCancelledAndCountedAsFailed(t *testing.T) {
	release := make(chan struct{})
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	cfg := closedCfg(url, wl(t, workload.UniformShort, 0), 2, 0, 50*time.Millisecond)
	cfg.DrainTimeout = 100 * time.Millisecond
	start := time.Now()
	out, _ := Run(context.Background(), cfg)
	if time.Since(start) > 3*time.Second {
		t.Fatalf("drain timeout not honoured: %v", time.Since(start))
	}
	if len(out.Records) != 2 {
		t.Fatalf("%d records", len(out.Records))
	}
	for _, r := range out.Records {
		if r.OK() || r.ErrClass != ErrDrainTimeout {
			t.Fatalf("%+v", r)
		}
	}
}

func TestStreamTimeToFirstContentChunk(t *testing.T) {
	const firstContentAfter = 150 * time.Millisecond
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		w.WriteHeader(200)
		// An initial chunk with the role and no content, as vLLM and the mock worker send, must not count.
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n")
		fl.Flush()
		time.Sleep(firstContentAfter)
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a \"}}]}\n\n")
		fl.Flush()
		time.Sleep(50 * time.Millisecond)
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"b \"}}]}\n\ndata: [DONE]\n\n")
	})
	cfg := closedCfg(url, wl(t, workload.UniformShort, 1), 1, 0, 10*time.Millisecond)
	out, err := Run(context.Background(), cfg)
	if err != nil || len(out.Records) == 0 {
		t.Fatalf("%v %d", err, len(out.Records))
	}
	r := out.Records[0]
	ttft, ok := r.TTFT()
	if !ok || !r.OK() {
		t.Fatalf("%+v", r)
	}
	if ttft < firstContentAfter {
		t.Fatalf("TTFT %v is earlier than the first content chunk (%v): the role chunk was counted", ttft, firstContentAfter)
	}
	if r.Started > r.FirstByte || r.FirstByte >= r.Done || r.Done-r.FirstByte < 40*time.Millisecond {
		t.Fatalf("first byte must precede the end of the stream: %+v", r)
	}
	if r.OutputTokens != 2 || r.UsageReported {
		t.Fatalf("two content chunks, no usage: %+v", r)
	}
}

func TestResponseFailuresAreClassified(t *testing.T) {
	var n atomic.Int64
	cases := map[string]struct {
		stream  float64
		handler http.HandlerFunc
		status  int
		class   string
	}{
		"503": {0, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, 503, ""},
		"stream without DONE": {1, func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
		}, 200, ErrStreamIncomplete},
		"stream error event": {1, func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"error\":{\"message\":\"x\"}}\n\n")
		}, 200, ErrStreamError},
		"not json":           {0, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("<html>")) }, 200, ErrBadBody},
		"dropped connection": {0, func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) }, 0, ErrTransport},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			n.Store(0)
			url := serve(t, tc.handler)
			out, _ := Run(context.Background(), closedCfg(url, wl(t, workload.UniformShort, tc.stream), 1, 0, 30*time.Millisecond))
			if len(out.Records) == 0 {
				t.Fatal("no records")
			}
			r := out.Records[0]
			if r.OK() || r.Status != tc.status || r.ErrClass != tc.class {
				t.Fatalf("%+v", r)
			}
			s := collect.SummarizeRecords(out.Records, out.Window)
			if s.Succeeded != 0 || s.Failed != s.Sent || s.Latency != nil {
				t.Fatalf("a failed request has no latency sample: %+v", s)
			}
		})
	}
}

func TestConnectionRefusedAndTimeoutAreClassified(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	url := ts.URL
	ts.Close()
	out, _ := Run(context.Background(), closedCfg(url, wl(t, workload.UniformShort, 0), 1, 0, 30*time.Millisecond))
	if len(out.Records) == 0 || out.Records[0].ErrClass != ErrConnect || out.Records[0].Status != 0 {
		t.Fatalf("%+v", out.Records)
	}

	slow := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // reading the body lets the server notice the client leaving
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	cfg := closedCfg(slow, wl(t, workload.UniformShort, 0), 1, 0, 30*time.Millisecond)
	cfg.RequestTimeout = 80 * time.Millisecond
	out, _ = Run(context.Background(), cfg)
	if len(out.Records) == 0 || out.Records[0].ErrClass != ErrTimeout {
		t.Fatalf("%+v", out.Records)
	}
}

func TestEachTenantSendsItsOwnKeyAndAttemptsAreRead(t *testing.T) {
	var mu sync.Mutex
	keys := map[string]int{}
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys[r.Header.Get("Authorization")]++
		mu.Unlock()
		w.Header().Set("X-ServerFlow-Attempts", "2")
		okJSON(w)
	})
	out, _ := Run(context.Background(), closedCfg(url, wl(t, workload.MultiTenant, 0), 2, 0, 200*time.Millisecond))
	if len(keys) < 3 {
		t.Fatalf("several tenants must have sent: %v", keys)
	}
	for k := range keys {
		if !strings.HasPrefix(k, "Bearer sk-bench-tenant-") {
			t.Fatalf("unexpected credential %q", k)
		}
	}
	for _, r := range out.Records {
		if r.Attempts != 2 || !strings.HasPrefix(r.Tenant, "tenant-") {
			t.Fatalf("%+v", r)
		}
	}
	s := collect.SummarizeRecords(out.Records, out.Window)
	if s.Retried != s.Sent {
		t.Fatalf("retried %d of %d", s.Retried, s.Sent)
	}
}

// stallServer answers instantly, except that every request arriving before the stall ends
// waits for it to end. stallEnd reports when that is (zero before the first request arrives).
func stallServer(t *testing.T, stall time.Duration) (url string, stallEnd func() time.Time) {
	var once sync.Once
	var mu sync.Mutex
	var until time.Time
	url = serve(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { mu.Lock(); until = time.Now().Add(stall); mu.Unlock() })
		mu.Lock()
		wait := time.Until(until)
		mu.Unlock()
		if wait > 0 {
			time.Sleep(wait)
		}
		okJSON(w)
	})
	return url, func() time.Time { mu.Lock(); defer mu.Unlock(); return until }
}

// The assertions are about which requests the stall must have touched, derived from the
// moment it actually ended, so they hold however slow the machine is.
func TestOpenLoopLatencyIsMeasuredFromTheIntendedTimeSoAStallCannotHide(t *testing.T) {
	const stall = 600 * time.Millisecond
	w := wl(t, workload.UniformShort, 0)

	// Closed loop, one client: it waits out the stall inside its first request and then carries
	// on. Exactly one request was started before the stall ended: the stall is hidden in one
	// slow request among many.
	url, endOf := stallServer(t, stall)
	closed, err := Run(context.Background(), closedCfg(url, w, 1, 0, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cut := endOf().Sub(closed.T0)
	touchedClosed := 0
	for _, r := range closed.Records {
		if r.Started < cut {
			touchedClosed++
		}
	}
	if touchedClosed != 1 {
		t.Fatalf("closed loop: %d requests started before the stall ended, want exactly the first", touchedClosed)
	}

	// Open loop at 20 requests/s with one slot: the generator is stuck behind the stalled
	// request, so requests due during the stall are sent late. Their latency still counts from
	// when they were due, so every request due before the stall ended shows at least the time
	// left until it ended. At least 11 are due in the 600 ms the stall lasts at the very least.
	url, endOf = stallServer(t, stall)
	open, err := Run(context.Background(), Config{
		BaseURL: url, Workload: w, Mode: Open, Segments: workload.Segments(workload.UniformShort, 20, 0, time.Second),
		MaxInFlight: 1, Duration: time.Second, DrainTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(open.Records) != 20 {
		t.Fatalf("open loop offers 20 requests in a second, got %d", len(open.Records))
	}
	cut = endOf().Sub(open.T0)
	touched := 0
	for _, r := range open.Records {
		if r.Latency() < r.Started-r.Intended {
			t.Fatalf("latency must include the wait to be sent: %+v", r)
		}
		if r.Intended < cut {
			touched++
			if want := cut - r.Intended; r.Latency() < want {
				t.Fatalf("a request due %v before the stall ended must show at least that latency, got %v", want, r.Latency())
			}
		}
	}
	if touched < 11 {
		t.Fatalf("open loop: only %d requests were due during the stall", touched)
	}
	if open.MaxStartLag < 100*time.Millisecond {
		t.Fatalf("the generator was held up by the stall and must say so: %v", open.MaxStartLag)
	}
	if touched <= touchedClosed {
		t.Fatalf("open loop must expose the stall in more requests than closed loop: %d vs %d", touched, touchedClosed)
	}
}

func TestOpenLoopSendsOnScheduleAndStopsAtTheEndOfTheWindow(t *testing.T) {
	var served atomic.Int64
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { served.Add(1); okJSON(w) })
	w := wl(t, workload.UniformShort, 0)
	out, err := Run(context.Background(), Config{
		BaseURL: url, Workload: w, Mode: Open, Segments: workload.Segments(workload.UniformShort, 50, 100*time.Millisecond, 400*time.Millisecond),
		MaxInFlight: 100, Warmup: 100 * time.Millisecond, Duration: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 50/s for 0.5 s is 25 requests, 5 in warm-up.
	if len(out.Records) != 25 || served.Load() != 25 {
		t.Fatalf("%d records, %d served", len(out.Records), served.Load())
	}
	sort.Slice(out.Records, func(i, j int) bool { return out.Records[i].Seq < out.Records[j].Seq })
	for i, r := range out.Records {
		if r.Seq != i || r.Intended != time.Duration(i)*20*time.Millisecond {
			t.Fatalf("request %d intended at %v", i, r.Intended)
		}
		if r.Started < r.Intended {
			t.Fatalf("sent before it was due: %+v", r)
		}
	}
	if s := collect.SummarizeRecords(out.Records, out.Window); s.Warmup != 5 || s.Sent != 20 {
		t.Fatalf("%+v", s)
	}
}

func TestTheRequestCapEndsTheRunEarlyAndCutsTheWindow(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { okJSON(w) })
	cfg := closedCfg(url, wl(t, workload.UniformShort, 0), 4, 0, 10*time.Second)
	cfg.MaxRequests = 25
	start := time.Now()
	out, err := Run(context.Background(), cfg)
	if err != nil || len(out.Records) != 25 || !out.Truncated {
		t.Fatalf("%v %d %v", err, len(out.Records), out.Truncated)
	}
	if time.Since(start) > 5*time.Second || out.Window.End >= 10*time.Second {
		t.Fatalf("the run must stop at the cap: took %v, window %+v", time.Since(start), out.Window)
	}
}

func TestCancellationStopsTheRunPromptlyAndKeepsWhatWasRecorded(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { okJSON(w) })
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	out, err := Run(ctx, closedCfg(url, wl(t, workload.UniformShort, 0), 2, 0, time.Minute))
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("err %v after %v", err, time.Since(start))
	}
	if len(out.Records) == 0 {
		t.Fatal("records so far must be returned")
	}
}

func TestValidateRejectsBadConfigs(t *testing.T) {
	w := wl(t, workload.Mixed, 0)
	good := Config{BaseURL: "http://x", Workload: w, Mode: Closed, Concurrency: 1, Duration: time.Second}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"no url":        func(c *Config) { c.BaseURL = "" },
		"no workload":   func(c *Config) { c.Workload = nil },
		"no duration":   func(c *Config) { c.Duration = 0 },
		"neg warmup":    func(c *Config) { c.Warmup = -1 },
		"no clients":    func(c *Config) { c.Concurrency = 0 },
		"bad mode":      func(c *Config) { c.Mode = "x" },
		"open no sched": func(c *Config) { c.Mode = Open; c.MaxInFlight = 1 },
		"open no slots": func(c *Config) { c.Mode = Open; c.Segments = []workload.Segment{{End: time.Second, Rate: 1}} },
	} {
		c := good
		mutate(&c)
		if _, err := Run(context.Background(), c); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestBackoffHonoursRetryAfterWithACapAndJittersOtherwise(t *testing.T) {
	zero, one := func() float64 { return 0 }, func() float64 { return 0.999999 }
	for _, tc := range []struct {
		status int
		ra     string
		j      func() float64
		want   time.Duration
	}{
		{200, "", zero, 0}, {500, "", zero, 0}, {404, "3", zero, 0},
		{503, "", zero, BackoffBase}, {429, "", one, BackoffBase + BackoffJitter - time.Nanosecond*100},
		{503, "1", zero, BackoffCap}, {503, "0", zero, BackoffBase}, {503, "junk", zero, BackoffBase}, {429, "999", zero, BackoffCap},
	} {
		got := Backoff(tc.status, tc.ra, tc.j)
		if got < tc.want-time.Millisecond || got > tc.want+time.Millisecond {
			t.Errorf("Backoff(%d, %q) = %v, want about %v", tc.status, tc.ra, got, tc.want)
		}
	}
}

func TestClosedLoopClientsDoNotSpinAgainstAnOverloadedServer(t *testing.T) {
	for name, header := range map[string]string{"retry-after": "1", "no header": ""} {
		t.Run(name, func(t *testing.T) {
			var served atomic.Int64
			url := serve(t, func(w http.ResponseWriter, r *http.Request) {
				served.Add(1)
				if header != "" {
					w.Header().Set("Retry-After", header)
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			out, err := Run(context.Background(), closedCfg(url, wl(t, workload.UniformShort, 0), 4, 0, time.Second))
			if err != nil {
				t.Fatal(err)
			}
			// Each client waits at least BackoffBase between requests, so 4 clients cannot send more
			// than 4*(1s/50ms + 1) requests in a second. Without a backoff this is thousands.
			if limit := int64(4 * (time.Second/BackoffBase + 1)); served.Load() > limit || int64(len(out.Records)) != served.Load() {
				t.Fatalf("served %d (limit %d), recorded %d", served.Load(), limit, len(out.Records))
			}
		})
	}
}

func TestWarmupRequestsDoNotCountAgainstTheRequestCap(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { okJSON(w) })
	cfg := closedCfg(url, wl(t, workload.UniformShort, 0), 2, 300*time.Millisecond, 10*time.Second)
	cfg.MaxRequests = 20
	out, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	warm, measured := 0, 0
	for _, r := range out.Records {
		if r.Intended < cfg.Warmup {
			warm++
		} else {
			measured++
		}
	}
	// The warm-up is capped too (clients idle once it is reached) but it cannot eat the window's
	// allowance: exactly the cap is measured.
	if measured != 20 || warm > 20 || !out.Truncated {
		t.Fatalf("warm-up %d measured %d truncated %v", warm, measured, out.Truncated)
	}
	if s := collect.SummarizeRecords(out.Records, out.Window); s.Sent != 20 || s.Warmup != warm {
		t.Fatalf("%+v", s)
	}
}

func TestProgressCountsRequests(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, r *http.Request) { okJSON(w) })
	cfg := closedCfg(url, wl(t, workload.UniformShort, 0), 2, 0, 200*time.Millisecond)
	cfg.Progress = &Progress{}
	out, _ := Run(context.Background(), cfg)
	if cfg.Progress.Completed.Load() != int64(len(out.Records)) || cfg.Progress.InFlight.Load() != 0 {
		t.Fatalf("completed %d in flight %d, records %d", cfg.Progress.Completed.Load(), cfg.Progress.InFlight.Load(), len(out.Records))
	}
}
