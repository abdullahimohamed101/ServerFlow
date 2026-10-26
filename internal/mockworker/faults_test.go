package mockworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// waitServing waits until Serve has installed its listener (it runs in a goroutine).
func waitServing(t *testing.T, s *Server) {
	t.Helper()
	eventually(t, 2*time.Second, func() bool { return s.current() != nil }, "Serve to start")
}

func postStatus(t *testing.T, c *http.Client, url string, stream bool) (int, string, error) {
	t.Helper()
	resp, err := c.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chatBody(stream, "hi", "")))
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

func newClient() (*http.Client, func()) {
	tr := &http.Transport{DisableKeepAlives: true}
	return &http.Client{Transport: tr}, tr.CloseIdleConnections
}

func TestZeroFaultsInjectNothing(t *testing.T) {
	e := newEnv(t)
	for range 20 {
		if resp := e.mustPost(t, chatBody(false, "hi", "")); resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	if got := e.srv.Faults().Stats(); got != (FaultStats{}) {
		t.Fatalf("fault counters moved with no fault set: %+v", got)
	}
}

func TestFaultsErrorRateAndMode(t *testing.T) {
	cases := []struct {
		mode FailureMode
		want int
		code string
	}{{ModeError, 500, "mock_failure"}, {ModeUnavailable, 503, "mock_failure"}}
	for _, tc := range cases {
		t.Run(string(tc.mode), func(t *testing.T) {
			e := newEnv(t)
			f := e.srv.Faults()
			if err := f.SetErrorRate(1, tc.mode); err != nil {
				t.Fatal(err)
			}
			for range 5 {
				resp := e.mustPost(t, chatBody(false, "hi", ""))
				b, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != tc.want || errCode(t, string(b)) != tc.code {
					t.Fatalf("got %d %s", resp.StatusCode, b)
				}
			}
			if got := f.Stats().Errored; got != 5 {
				t.Fatalf("errored counter %d, want 5", got)
			}
			// The fault is runtime-clearable and the worker recovers.
			_ = f.SetErrorRate(0, ModeError)
			if resp := e.mustPost(t, chatBody(false, "hi", "")); resp.StatusCode != 200 {
				t.Fatalf("after clearing: %d", resp.StatusCode)
			}
			if got := f.Stats().Errored; got != 5 {
				t.Fatalf("errored counter moved after clearing: %d", got)
			}
		})
	}
}

func TestFaultsPartialRateIsSeededAndPartial(t *testing.T) {
	e := newEnv(t)
	f := e.srv.Faults()
	_ = f.SetErrorRate(0.5, ModeUnavailable)
	failed := 0
	for range 100 {
		if e.mustPost(t, chatBody(false, "hi", "")).StatusCode == 503 {
			failed++
		}
	}
	if failed < 25 || failed > 75 {
		t.Fatalf("%d of 100 failed at rate 0.5", failed)
	}
}

func TestFaultsRejectBadSettings(t *testing.T) {
	f := newEnv(t).srv.Faults()
	for _, rate := range []float64{-0.1, 1.1} {
		if f.SetErrorRate(rate, ModeError) == nil {
			t.Errorf("rate %v accepted", rate)
		}
	}
	if f.SetErrorRate(0.5, "explode") == nil {
		t.Error("unknown mode accepted")
	}
}

func TestFaultsExtraDelayHoldsTheResponseBack(t *testing.T) {
	e := newEnv(t)
	f := e.srv.Faults()
	f.SetExtraDelay(1200 * time.Millisecond)
	begin := time.Now()
	e.mustPost(t, chatBody(false, "hi", ""))
	if d := time.Since(begin); d < 1200*time.Millisecond {
		t.Fatalf("answered after %v despite a 1.2s extra delay", d)
	}
	if f.Stats().Delayed != 1 {
		t.Fatalf("delayed counter %d", f.Stats().Delayed)
	}
	f.SetExtraDelay(0)
	begin = time.Now()
	e.mustPost(t, chatBody(false, "hi", ""))
	// Half the delay that was set: a request that still waited for it would take at least all of it. (The counter
	// below is the exact proof; the time only has to be far from the delay in both directions.)
	if d := time.Since(begin); d > 600*time.Millisecond {
		t.Fatalf("still slow after clearing: %v", d)
	}
	if f.Stats().Delayed != 1 {
		t.Fatalf("the request after clearing the delay was delayed too: counter %d", f.Stats().Delayed)
	}
}

func TestFaultsDelayEndsWhenTheCallerLeaves(t *testing.T) {
	e := newEnv(t)
	e.srv.Faults().SetExtraDelay(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := e.post(ctx, chatBody(false, "hi", "")); err == nil {
		t.Fatal("expected the caller's timeout")
	}
	eventually(t, 2*time.Second, func() bool { return e.srv.inflight.Load() == 0 }, "the delayed handler to notice the caller left")
	if a := e.stats(t).ActiveRequests; a != 0 {
		t.Fatalf("slot still held: %d active", a)
	}
}

func TestFaultsBlackholeReadsAndNeverAnswers(t *testing.T) {
	e := newEnv(t)
	f := e.srv.Faults()
	f.SetBlackhole(true)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if resp, err := e.post(ctx, chatBody(false, "hi", "")); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("a blackholed worker answered %d", resp.StatusCode)
	}
	if f.Stats().Blackholed != 1 {
		t.Fatalf("blackholed counter %d", f.Stats().Blackholed)
	}
	// /stats and /health keep answering and say READY: only the caller's timeout can detect it.
	if st := e.stats(t); st.Status != "ready" || st.ActiveRequests != 0 {
		t.Fatalf("stats while blackholed: %+v", st)
	}
	if code, _ := e.get(t, "/health"); code != 200 {
		t.Fatalf("health %d", code)
	}
	f.SetBlackhole(false)
	if resp := e.mustPost(t, chatBody(false, "hi", "")); resp.StatusCode != 200 {
		t.Fatalf("after clearing: %d", resp.StatusCode)
	}
}

func TestFaultsBlackholeClearedReleasesHeldRequests(t *testing.T) {
	e := newEnv(t)
	f := e.srv.Faults()
	f.SetBlackhole(true)
	var wg sync.WaitGroup
	code := make(chan int, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := e.post(context.Background(), chatBody(false, "hi", ""))
		if err != nil {
			code <- -1
			return
		}
		_ = resp.Body.Close()
		code <- resp.StatusCode
	}()
	eventually(t, 2*time.Second, func() bool { return f.Stats().Blackholed == 1 }, "the request to be held")
	f.SetBlackhole(false)
	wg.Wait()
	if c := <-code; c != 200 {
		t.Fatalf("a released request got %d", c)
	}
}

func TestFaultsCrashResetsConnectionsAndRestartServesAgain(t *testing.T) {
	cfg := testConfig()
	cfg.TokensPerSecond = 20 // 10 tokens, 500ms: still running when the crash comes
	url, stop, done := serve(t, cfg)
	srv := lastServer
	waitServing(t, srv)
	c, closeIdle := newClient()
	defer closeIdle()

	if code, _, err := postStatus(t, c, url, false); err != nil || code != 200 {
		t.Fatalf("before the crash: %d %v", code, err)
	}
	inflight := make(chan error, 1)
	go func() { _, _, err := postStatus(t, c, url, true); inflight <- err }()
	eventually(t, 2*time.Second, func() bool { return srv.engine.Stats().Active == 1 }, "a request to be in flight")

	f := srv.Faults()
	if err := f.Crash(); err != nil {
		t.Fatal(err)
	}
	if f.Crash() == nil {
		t.Fatal("a second crash should be refused")
	}
	select {
	case err := <-inflight:
		if err == nil {
			t.Fatal("the in-flight stream survived the crash")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the in-flight request was not cut")
	}
	if _, _, err := postStatus(t, c, url, false); err == nil {
		t.Fatal("a crashed worker accepted a connection")
	}
	if f.Stats().Crashes != 1 {
		t.Fatalf("crash counter %d", f.Stats().Crashes)
	}
	select {
	case err := <-done:
		t.Fatalf("a crash must not end Serve: %v", err)
	default:
	}

	if err := f.Restart(); err != nil {
		t.Fatal(err)
	}
	if code, _, err := postStatus(t, c, url, false); err != nil || code != 200 {
		t.Fatalf("after the restart: %d %v", code, err)
	}
	if f.Restart() == nil {
		t.Fatal("restarting a running worker should be refused")
	}
	// The final drain still works after a restart.
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain after restart: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return")
	}
}

func TestFaultsCrashWithoutServeIsAnError(t *testing.T) {
	if err := newEnv(t).srv.Faults().Crash(); err == nil {
		t.Fatal("expected an error: nothing is serving through Serve")
	}
}

func TestStopAfterCrashEndsServe(t *testing.T) {
	_, stop, done := serve(t, testConfig())
	waitServing(t, lastServer)
	if err := lastServer.Faults().Crash(); err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after a crash and a stop")
	}
}

func TestDrainAnnounceKeepsServingThenRefuses(t *testing.T) {
	cfg := testConfig()
	cfg.DrainAnnounce = 400 * time.Millisecond
	url, stop, done := serve(t, cfg)
	srv := lastServer
	c, closeIdle := newClient()
	defer closeIdle()

	stop()
	eventually(t, 2*time.Second, func() bool {
		r, err := c.Get(url + "/readyz")
		if err != nil {
			return false
		}
		_ = r.Body.Close()
		return r.StatusCode == 503
	}, "readiness to report draining")
	// Announced as draining (stats), yet still serving.
	if code, _, err := postStatus(t, c, url, false); err != nil || code != 200 {
		t.Fatalf("during the announce period: %d %v", code, err)
	}
	if n := srv.ServedWhileAnnouncing(); n != 1 {
		t.Fatalf("served while announcing: %d", n)
	}
	time.Sleep(500 * time.Millisecond)
	code, body, err := postStatus(t, c, url, false)
	if err == nil && (code != 503 || errCode(t, body) != "draining") {
		t.Fatalf("after the announce period: %d %s", code, body)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Serve did not return")
	}
}

func TestDrainAnnounceZeroRefusesAtOnce(t *testing.T) {
	cfg := testConfig()
	cfg.TokensPerSecond = 10 // a 1s stream keeps the drain open
	url, stop, _ := serve(t, cfg)
	c, closeIdle := newClient()
	defer closeIdle()
	go func() { _, _, _ = postStatus(t, c, url, true) }()
	eventually(t, 2*time.Second, func() bool { return lastServer.engine.Stats().Active == 1 }, "a request in flight")
	stop()
	eventually(t, 2*time.Second, func() bool {
		code, body, err := postStatus(t, c, url, false)
		return err == nil && code == 503 && errCode(t, body) == "draining"
	}, "an immediate refusal with the default announce of 0")
	if n := lastServer.ServedWhileAnnouncing(); n != 0 {
		t.Fatalf("served while announcing with announce 0: %d", n)
	}
}

func TestDrainAnnounceValidationAndFlag(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DrainAnnounce = -time.Second
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "drain-announce") {
		t.Fatalf("negative announce: %v", err)
	}
	cfg.DrainAnnounce = time.Hour
	if err := cfg.Validate(); err == nil {
		t.Fatal("an hour-long announce accepted")
	}
	got, _, err := ParseFlags([]string{"--drain-announce=1500ms"}, io.Discard)
	if err != nil || got.DrainAnnounce != 1500*time.Millisecond {
		t.Fatalf("%v %v", got.DrainAnnounce, err)
	}
	def, _, _ := ParseFlags(nil, io.Discard)
	if def.DrainAnnounce != 0 {
		t.Fatalf("default announce %v: the flag must default to today's behaviour", def.DrainAnnounce)
	}
}

// A crash must reset connections (RST), not close them politely (FIN): a client reading from a killed process
// sees ECONNRESET, and a harness that only closed would show EOF.
func TestCrashResetsTheConnectionRatherThanClosingIt(t *testing.T) {
	cfg := testConfig()
	cfg.TokensPerSecond = 20 // a 500ms stream, still running at the crash
	url, _, _ := serve(t, cfg)
	srv := lastServer
	waitServing(t, srv)
	conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	body := chatBody(true, "hi", "")
	_, _ = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	buf := make([]byte, 64)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(buf); err != nil { // the response has begun
		t.Fatal(err)
	}
	if err := srv.Faults().Crash(); err != nil {
		t.Fatal(err)
	}
	var rerr error
	for rerr == nil {
		_, rerr = conn.Read(buf)
	}
	if !errors.Is(rerr, syscall.ECONNRESET) {
		t.Fatalf("the client saw %v, want a connection reset (a FIN would read as EOF)", rerr)
	}
}

func TestDrainReleasesRequestsHeldByABlackhole(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Second
	url, stop, done := serve(t, cfg)
	srv := lastServer
	waitServing(t, srv)
	srv.Faults().SetBlackhole(true)
	c, closeIdle := newClient()
	defer closeIdle()
	go func() { _, _, _ = postStatus(t, c, url, false) }()
	eventually(t, 2*time.Second, func() bool { return srv.Faults().Stats().Blackholed == 1 }, "a request to be held")
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the drain should be clean once the held request is released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the drain waited for a held request")
	}
}
