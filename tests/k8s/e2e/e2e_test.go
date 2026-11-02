package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "sf_live_TESTKEYTESTKEYTESTKEYTESTKEY"

const okBody = `{"id":"x","choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`

func chunk(s string) string { return "data: " + s + "\n\n" }

const goodChunk = `{"choices":[{"delta":{"content":"hi"}}]}`

// fake is a gateway stand-in; handler decides what the next response is.
func fake(t *testing.T, h http.HandlerFunc) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+testKey {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func sseHandler(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}
}

// do sends one request the way a client of the load generator does.
func do(t *testing.T, srv *httptest.Server, stream bool) (int, string) {
	t.Helper()
	c := newClient(true)
	defer c.CloseIdleConnections()
	return doRequest(context.Background(), c, srv.URL, testKey, stream, 2*time.Second)
}

// TestClassification: every outcome the plan names (D20) gets its class, and a good response gets none.
func TestClassification(t *testing.T) {
	abort := func(w http.ResponseWriter, _ *http.Request) { panic(http.ErrAbortHandler) }
	closeAtOnce := func(w http.ResponseWriter, _ *http.Request) {
		c, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = c.Close()
		}
	}
	for _, tc := range []struct {
		name   string
		stream bool
		h      http.HandlerFunc
		status int
		class  string
	}{
		{"non-streaming ok", false, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, okBody) }, 200, ""},
		{"streaming ok", true, sseHandler(chunk(goodChunk) + chunk(goodChunk) + chunk(`{"choices":[],"usage":{"prompt_tokens":1}}`) + chunk("[DONE]")), 200, ""},
		{"streaming ok with comments and CRLF", true, sseHandler(": keep-alive\r\n\r\nevent: message\r\ndata: " + goodChunk + "\r\n\r\ndata: [DONE]\r\n\r\n"), 200, ""},
		{"503 with the error code", false, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(503)
			_, _ = io.WriteString(w, `{"error":{"message":"m","type":"api_error","code":"no_available_workers"}}`)
		}, 503, "http_503_no_available_workers"},
		{"502 without a JSON body", false, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(502)
			_, _ = io.WriteString(w, "bad gateway")
		}, 502, "http_502"},
		{"429 is a failure here too", true, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(429)
			_, _ = io.WriteString(w, `{"error":{"code":"Rate-Limited!"}}`)
		}, 429, "http_429_rate_limited_"},
		{"stream cut by a reset after two chunks", true, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, chunk(goodChunk)+chunk(goodChunk))
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}, 200, ClassStreamCut},
		{"stream ended cleanly without [DONE]", true, sseHandler(chunk(goodChunk) + chunk(goodChunk)), 200, ClassStreamCut},
		{"stream with no events at all", true, sseHandler(""), 200, ClassStreamCut},
		{"stream cut in the middle of an event", true, sseHandler(chunk(goodChunk) + `data: {"choices":[{"delta"`), 200, ClassSSEMalformed},
		{"malformed SSE: data is not JSON", true, sseHandler(chunk(goodChunk) + chunk("not json") + chunk("[DONE]")), 200, ClassSSEMalformed},
		{"malformed SSE: a line that is no field", true, sseHandler(chunk(goodChunk) + "garbage without colon\n\n" + chunk("[DONE]")), 200, ClassSSEMalformed},
		{"malformed SSE: unknown field", true, sseHandler(chunk(goodChunk) + "bogus: 1\n\n" + chunk("[DONE]")), 200, ClassSSEMalformed},
		{"malformed SSE: JSON without choices", true, sseHandler(chunk(`{"hello":1}`) + chunk("[DONE]")), 200, ClassSSEMalformed},
		{"malformed SSE: event after [DONE]", true, sseHandler(chunk(goodChunk) + chunk("[DONE]") + chunk(goodChunk)), 200, ClassSSEMalformed},
		{"error event inside the stream", true, sseHandler(chunk(goodChunk) + chunk(`{"error":{"message":"m","type":"api_error","code":"inference_failed"}}`)), 200, "stream_error_inference_failed"},
		{"body cut short", false, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "500")
			_, _ = io.WriteString(w, okBody[:20])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}, 200, ClassBodyCut},
		{"200 whose JSON has no choices", false, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"choices":[]}`) }, 200, ClassBodyMalformed},
		{"200 that is not JSON", false, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "<html>") }, 200, ClassBodyMalformed},
		{"connection closed before any response", false, closeAtOnce, 0, ClassClosed},
		{"connection aborted before headers", false, abort, 0, ClassClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := fake(t, tc.h)
			status, class := do(t, srv, tc.stream)
			if status != tc.status || class != tc.class {
				t.Fatalf("got status %d class %q, want %d %q", status, class, tc.status, tc.class)
			}
		})
	}
}

func TestTransportFailures(t *testing.T) {
	// Refused: a port nothing listens on.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	c := newClient(false)
	if _, class := doRequest(context.Background(), c, "http://"+addr, testKey, false, time.Second); class != ClassRefused {
		t.Errorf("refused: got %q", class)
	}
	// Timeout: the handler outlasts the client's patience, headers or stream.
	srv, _ := fake(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	})
	c = newClient(true)
	if _, class := doRequest(context.Background(), c, srv.URL, testKey, false, 150*time.Millisecond); class != ClassTimeout {
		t.Errorf("timeout before headers: got %q", class)
	}
	srv2, _ := fake(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, chunk(goodChunk))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	})
	if _, class := doRequest(context.Background(), c, srv2.URL, testKey, true, 150*time.Millisecond); class != ClassTimeout {
		t.Errorf("timeout inside a stream: got %q", class)
	}
}

// TestRequestsAreNeverRetried: one doRequest is one request on the wire, even when the server kills the connection, and
// even on a reused keep-alive connection that the server closed while it was idle.
func TestRequestsAreNeverRetried(t *testing.T) {
	var calls atomic.Int64
	srv, n := fake(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, okBody)
			return
		}
		panic(http.ErrAbortHandler)
	})
	c := newClient(true)
	defer c.CloseIdleConnections()
	if _, class := doRequest(context.Background(), c, srv.URL, testKey, false, time.Second); class != "" {
		t.Fatalf("first request: %q", class)
	}
	if _, class := doRequest(context.Background(), c, srv.URL, testKey, false, time.Second); class == "" {
		t.Fatal("the aborted request counted as a success (was it retried?)")
	}
	if n.Load() != 2 {
		t.Fatalf("the server saw %d requests for 2 calls", n.Load())
	}
}

func TestKeyIsSentAndNeverReported(t *testing.T) {
	var got atomic.Value
	srv, _ := fake(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Store(string(b))
		_, _ = io.WriteString(w, okBody)
	})
	if status, class := do(t, srv, true); status != 200 && class == "" {
		t.Fatal("unexpected")
	}
	if !strings.Contains(fmt.Sprint(got.Load()), `"stream":true`) {
		t.Errorf("the streaming request did not ask for a stream: %v", got.Load())
	}
	// A transport error that mentions the key is scrubbed.
	if e := redact(fmt.Errorf("boom %s", testKey), testKey); strings.Contains(e.Error(), testKey) {
		t.Error("redact left the key in the error")
	}
}

func TestSummaryPhasesAndVerdict(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	phases := []Phase{{"baseline", at(0)}, {"roll1", at(20)}, {"roll2", at(40)}, {"after", at(60)}}
	var out []Outcome
	add := func(sec int, class string, n int) {
		for i := 0; i < n; i++ {
			out = append(out, Outcome{Start: at(sec), End: at(sec), Class: class, Stream: i%2 == 0, KeepAlive: i%2 == 1})
		}
	}
	add(5, "", 100)
	add(25, "", 1500)
	add(45, "", 1500)
	add(65, "", 100)
	s := Summarize(phases, out, at(80))
	if s.Requests != 3200 || s.RolloutRequests != 3000 || s.Failures != 0 || len(s.Phases) != 4 || s.Phases[1].Requests != 1500 || s.Phases[3].Seconds != 20 {
		t.Fatalf("summary: %+v", s)
	}
	if pass, why := Verdict(s, 3000, false); !pass {
		t.Errorf("3000 requests and no failure should pass: %s", why)
	}
	if pass, _ := Verdict(s, 3001, false); pass {
		t.Error("too few requests during the rollouts must fail")
	}
	if pass, why := Verdict(s, 3000, true); pass || !strings.Contains(why, "cannot see the outage") {
		t.Errorf("a control without failures must fail as unable to see the outage: %v %q", pass, why)
	}
	// One failure anywhere (even in the baseline) fails the test; it passes the control.
	add(5, ClassStreamCut, 1)
	add(45, ClassReset, 2)
	s = Summarize(phases, out, at(80))
	if s.Failures != 3 || s.RolloutFailures != 2 || s.Classes[ClassStreamCut] != 1 || s.Phases[0].Failures != 1 {
		t.Fatalf("failure tally: %+v", s)
	}
	if pass, _ := Verdict(s, 3000, false); pass {
		t.Error("failures must fail the run")
	}
	if pass, _ := Verdict(s, 3000, true); !pass {
		t.Error("failures must pass the control")
	}
	var buf bytes.Buffer
	WriteText(&buf, s, 3000, false)
	for _, want := range []string{"stream_truncated", "connection_reset", "result: FAIL", "roll2"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, buf.String())
		}
	}
}

func TestRuleOfThree(t *testing.T) {
	if got := RuleOfThree(3000); got != 0.001 {
		t.Errorf("3/3000 = %v", got)
	}
	t0 := time.Now()
	s := Summarize([]Phase{{"roll1", t0}}, []Outcome{{Start: t0}, {Start: t0}, {Start: t0}}, t0.Add(time.Second))
	var buf bytes.Buffer
	WriteText(&buf, s, 1, false)
	if !strings.Contains(buf.String(), "rule of three: 0 failures in 3 requests bounds the true failure rate below 100.000%") {
		t.Errorf("rule of three line: %s", buf.String())
	}
}

func TestPodObservations(t *testing.T) {
	in := strings.Join([]string{
		"1000 gateway-a 2026-01-01T00:00:45Z 45", // SIGTERM at 00:00:00
		"1001 gateway-a 2026-01-01T00:00:45Z 45",
		"1007 gateway-a 2026-01-01T00:00:45Z 45",
		"1030 gateway-b 2026-01-01T00:01:15Z 45", // SIGTERM at 00:00:30
		"1033 gateway-b 2026-01-01T00:01:15Z 45",
		"garbage",
		"1034 gateway-c",
		"1035 gateway-d not-a-time 45",
	}, "\n")
	pods, err := ParsePodObservations(strings.NewReader(in))
	if err != nil || len(pods) != 2 {
		t.Fatalf("%v %+v", err, pods)
	}
	if pods[0].Name != "gateway-a" || !pods[0].SIGTERM.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("first pod: %+v", pods[0])
	}
	var buf bytes.Buffer
	WritePodTimings(&buf, pods)
	if !strings.Contains(buf.String(), "not obtainable") || !strings.Contains(buf.String(), "gateway-b") {
		t.Errorf("pod report: %s", buf.String())
	}
}

func TestCheckLoopback(t *testing.T) {
	for _, ok := range []string{"http://127.0.0.1:28080", "http://localhost:1", "http://[::1]:80"} {
		if err := checkLoopback(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"https://127.0.0.1", "http://10.0.0.1:80", "http://example.com", "ftp://127.0.0.1", "127.0.0.1:80", "http://127.0.0.1.evil.example"} {
		if err := checkLoopback(bad); err == nil {
			t.Errorf("%s was accepted", bad)
		}
	}
}

// TestRunEndToEnd: 24 clients against a fake gateway, phases from the control input, a mix of streaming and plain requests on
// keep-alive and per-request connections, and a result that never contains the key.
func TestRunEndToEnd(t *testing.T) {
	var conns atomic.Int64
	var streams, plain atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testKey {
			w.WriteHeader(401)
			return
		}
		b, _ := io.ReadAll(r.Body)
		time.Sleep(2 * time.Millisecond)
		if bytes.Contains(b, []byte(`"stream":true`)) {
			streams.Add(1)
			_, _ = io.WriteString(w, chunk(goodChunk)+chunk("[DONE]"))
			return
		}
		plain.Add(1)
		_, _ = io.WriteString(w, okBody)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	pr, pw := io.Pipe()
	var logBuf bytes.Buffer
	cfg := Config{URL: srv.URL, Key: testKey, Clients: 24, Timeout: 2 * time.Second, FailurePause: time.Millisecond, MinRollout: 1, Log: &logBuf}
	done := make(chan Summary, 1)
	go func() { done <- Run(context.Background(), cfg, pr) }()
	time.Sleep(150 * time.Millisecond)
	_, _ = io.WriteString(pw, "mark roll1\n")
	time.Sleep(150 * time.Millisecond)
	_, _ = io.WriteString(pw, "mark bad name!\n") // ignored
	_, _ = io.WriteString(pw, "mark after\nstop\n")
	var s Summary
	select {
	case s = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end after stop")
	}
	if s.Failures != 0 || s.Requests < 100 {
		t.Fatalf("summary: %+v", s)
	}
	if len(s.Phases) != 3 || s.Phases[0].Name != "baseline" || s.Phases[1].Name != "roll1" || s.Phases[1].Requests == 0 || s.RolloutRequests != s.Phases[1].Requests {
		t.Fatalf("phases: %+v", s.Phases)
	}
	if streams.Load() == 0 || plain.Load() == 0 || s.StreamRequests == 0 || s.KeepAliveRequest == 0 || s.KeepAliveRequest == s.Requests {
		t.Errorf("no mix: streams %d plain %d keepalive %d of %d", streams.Load(), plain.Load(), s.KeepAliveRequest, s.Requests)
	}
	// Per-request connections: far more connections than the 24 clients hold, but keep-alive clients reuse theirs.
	if conns.Load() <= 24 || conns.Load() >= int64(s.Requests) {
		t.Errorf("connections %d for %d requests: expected reuse by half the clients and a new connection per request for the rest", conns.Load(), s.Requests)
	}
	var buf bytes.Buffer
	WriteText(&buf, s, 1, false)
	j, _ := json.Marshal(s)
	if strings.Contains(buf.String()+string(j)+logBuf.String(), testKey) {
		t.Error("the key appears in the report or the log")
	}
	if lines := strings.Count(logBuf.String(), "\n"); lines != s.Requests {
		t.Errorf("%d log lines for %d requests", lines, s.Requests)
	}
}

// TestRunSeesAnOutage: the generator reports failures when the server goes away mid-run, and ends on end of input.
func TestRunSeesAnOutage(t *testing.T) {
	srv, _ := fake(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bytes.Contains(b, []byte(`"stream":true`)) {
			_, _ = io.WriteString(w, chunk(goodChunk)+chunk("[DONE]"))
			return
		}
		_, _ = io.WriteString(w, okBody)
	})
	pr, pw := io.Pipe()
	cfg := Config{URL: srv.URL, Key: testKey, Clients: 4, Timeout: time.Second, FailurePause: time.Millisecond, MinRollout: 1}
	done := make(chan Summary, 1)
	go func() { done <- Run(context.Background(), cfg, pr) }()
	time.Sleep(100 * time.Millisecond)
	_, _ = io.WriteString(pw, "mark roll1\n")
	srv.CloseClientConnections()
	srv.Close()
	time.Sleep(100 * time.Millisecond)
	_ = pw.Close() // end of input stops the run
	select {
	case s := <-done:
		if s.Failures == 0 || s.Classes[ClassRefused] == 0 {
			t.Errorf("the outage was not seen: %+v", s.Classes)
		}
		if pass, _ := Verdict(s, 1, true); !pass {
			t.Error("the control should pass when an outage is seen")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end on end of input")
	}
}

func TestMainRefusesBadInput(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key")
	if err := os.WriteFile(keyFile, []byte(testKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	_ = os.WriteFile(empty, nil, 0o600)
	for _, args := range [][]string{
		nil, {"bogus"}, {"run"}, {"run", "--key-file", empty}, {"run", "--key-file", filepath.Join(dir, "missing")},
		{"run", "--key-file", keyFile, "--url", "http://example.com"}, {"run", "--key-file", keyFile, "--expect", "maybe"},
		{"run", "--key-file", keyFile, "--clients", "1"}, {"pods"},
	} {
		var out, errb bytes.Buffer
		if code := realMain(args, strings.NewReader(""), &out, &errb); code == 0 {
			t.Errorf("%v was accepted", args)
		}
		if strings.Contains(out.String()+errb.String(), testKey) {
			t.Errorf("%v printed the key", args)
		}
	}
	// A real, short run through the command: end of input stops it, the verdict decides the exit code.
	srv, _ := fake(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bytes.Contains(b, []byte(`"stream":true`)) {
			_, _ = io.WriteString(w, chunk(goodChunk)+chunk("[DONE]"))
			return
		}
		_, _ = io.WriteString(w, okBody)
	})
	var out, errb bytes.Buffer
	js := filepath.Join(dir, "out.json")
	code := realMain([]string{"run", "--key-file", keyFile, "--url", srv.URL, "--clients", "2", "--min-rollout", "0", "--json", js}, strings.NewReader("mark roll1\nstop\n"), &out, &errb)
	if code != 0 || !strings.Contains(out.String(), "result: PASS") || strings.Contains(out.String(), testKey) {
		t.Fatalf("exit %d\n%s%s", code, out.String(), errb.String())
	}
	if b, err := os.ReadFile(js); err != nil || !json.Valid(b) {
		t.Errorf("json summary: %v", err)
	}
	code = realMain([]string{"run", "--key-file", keyFile, "--url", srv.URL, "--clients", "2", "--expect", "failures"}, strings.NewReader("stop\n"), &out, &errb)
	if code != 1 || !strings.Contains(out.String(), "cannot see the outage") {
		t.Errorf("a control run without failures must fail: exit %d", code)
	}
}

// TestControlNeedsADrainAttributableOutageInTheRollouts: the control passes only for failures inside the rollout phases that a
// missing drain explains. Capacity 503s (however many, in the baseline or the rollouts), a baseline failure, a failure after
// the rollouts and a request the generator could not build cannot make it pass.
func TestControlNeedsADrainAttributableOutageInTheRollouts(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	phases := []Phase{{"baseline", at(0)}, {"roll1", at(20)}, {"roll2", at(40)}, {"after", at(60)}}
	const capacity = "http_503_no_capacity"
	many := func(sec int, class string, n int) []Outcome {
		var o []Outcome
		for i := 0; i < n; i++ {
			o = append(o, Outcome{Start: at(sec), Class: class})
		}
		return o
	}
	for _, tc := range []struct {
		name string
		out  []Outcome
		pass bool
	}{
		{"no failure", []Outcome{{Start: at(25)}}, false},
		{"failure only in the baseline", []Outcome{{Start: at(5), Class: ClassRefused}, {Start: at(25)}}, false},
		{"failure only after the rollouts", []Outcome{{Start: at(65), Class: ClassRefused}}, false},
		{"only an unbuildable request in a rollout", []Outcome{{Start: at(25), Class: ClassBadRequest}}, false},
		{"1500 capacity 503s in the rollouts and the baseline", append(many(5, capacity, 800), many(25, capacity, 700)...), false},
		{"other 503 codes and 429s are load, not the drain", append(many(25, "http_503_no_available_workers", 5), many(45, "http_429_rate_limited", 5)...), false},
		{"transport_other and stream error events are not specific", append(many(25, ClassTransport, 3), many(26, "stream_error_inference_failed", 3)...), false},
		{"capacity 503s plus one reset in roll1", append(many(25, capacity, 500), Outcome{Start: at(26), Class: ClassReset}), true},
		{"one connection_closed in roll2", []Outcome{{Start: at(45), Class: ClassClosed}}, true},
		{"a 502 in roll1", []Outcome{{Start: at(25), Class: "http_502_upstream_error"}}, true},
		{"a 504 in roll2", []Outcome{{Start: at(45), Class: "http_504"}}, true},
		{"a cut stream in roll2", []Outcome{{Start: at(45), Class: ClassStreamCut}}, true},
	} {
		s := Summarize(phases, tc.out, at(80))
		if pass, why := Verdict(s, 3000, true); pass != tc.pass {
			t.Errorf("%s: pass=%v (%s), want %v", tc.name, pass, why, tc.pass)
		}
	}
	// A request the generator could not build also fails the real run.
	s := Summarize(phases, []Outcome{{Start: at(25), Class: ClassBadRequest}}, at(80))
	if pass, _ := Verdict(s, 0, false); pass {
		t.Error("a generator error passed the real run")
	}
}

// TestReportShowsThroughputAndSeparatesCapacity: the shortfall of requests is visible, and capacity rejections are named as a
// load effect with their baseline count.
func TestReportShowsThroughputAndSeparatesCapacity(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	phases := []Phase{{"baseline", at(0)}, {"roll1", at(20)}, {"after", at(40)}}
	var out []Outcome
	for i := 0; i < 100; i++ {
		out = append(out, Outcome{Start: at(i % 60)})
	}
	out = append(out, Outcome{Start: at(1), Class: "http_503_no_capacity"}, Outcome{Start: at(25), Class: ClassReset})
	s := Summarize(phases, out, at(60))
	var buf bytes.Buffer
	WriteText(&buf, s, 3000, true)
	for _, want := range []string{"throughput of the load generator: 1.7 requests/s overall", "during the rollouts (20 s)", "capacity rejections (503 no_capacity): 1, of them 1 in the baseline", "drain-attributable failures", "1 during the rollouts"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, buf.String())
		}
	}
}

// TestStatusFileCountsRolloutRequests: the caller that decides whether to keep rolling reads the counts from the status file.
func TestStatusFileCountsRolloutRequests(t *testing.T) {
	srv, _ := fake(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bytes.Contains(b, []byte(`"stream":true`)) {
			_, _ = io.WriteString(w, chunk(goodChunk)+chunk("[DONE]"))
			return
		}
		_, _ = io.WriteString(w, okBody)
	})
	status := filepath.Join(t.TempDir(), "status")
	pr, pw := io.Pipe()
	cfg := Config{URL: srv.URL, Key: testKey, Clients: 4, Timeout: time.Second, FailurePause: time.Millisecond, Status: status}
	done := make(chan Summary, 1)
	go func() { done <- Run(context.Background(), cfg, pr) }()
	_, _ = io.WriteString(pw, "mark roll1\n")
	var line string
	for i := 0; i < 100 && (!strings.Contains(line, "rollout_requests=") || strings.Contains(line, "rollout_requests=0 ")); i++ {
		time.Sleep(100 * time.Millisecond)
		b, _ := os.ReadFile(status)
		line = string(b)
	}
	_, _ = io.WriteString(pw, "stop\n")
	<-done
	if !regexp.MustCompile(`^requests=\d+ rollout_requests=[1-9]\d* rollout_failures=0 drain_failures=0 rollout_drain_failures=0\n$`).MatchString(line) {
		t.Errorf("status line %q", line)
	}
}
