package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Phase 15, PR A, D2: a panic anywhere in a registry-mode attempt must release the worker slot(s) and end every
// started attempt exactly once (AttemptEnded failed, class panic), and never end an attempt that did not start.
// The panic still reaches withRequest, which answers 500.

// faultTransport wraps the router's worker transport to inject failures into the upstream call.
type faultTransport struct {
	base http.RoundTripper
	// panicSend, when it returns true for a request, panics instead of sending it.
	panicSend func(*http.Request) bool
	// panicClose, when it returns true for a response, makes closing its body panic (after closing it).
	panicClose func(*http.Response) bool
}

func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.panicSend != nil && f.panicSend(r) {
		panic("injected: the upstream call")
	}
	resp, err := f.base.RoundTrip(r)
	if err == nil && f.panicClose != nil && f.panicClose(resp) {
		resp.Body = panicCloser{resp.Body}
	}
	return resp, err
}

type panicCloser struct{ io.ReadCloser }

func (p panicCloser) Close() error {
	_ = p.ReadCloser.Close()
	panic("injected: closing a response body")
}

// nthRecorder is a RequestRecorder that panics on its panicAt-th call (1-based; 0 never).
type nthRecorder struct {
	panicAt int32
	calls   atomic.Int32
}

func (r *nthRecorder) Record(string, string, string, string, int) {
	if n := r.calls.Add(1); r.panicAt != 0 && n == r.panicAt {
		panic("injected: the request recorder")
	}
}

func (e *regEnv) injectTransport(f *faultTransport) {
	f.base = e.gw.router.client.Transport
	e.gw.router.client.Transport = f
}

// wantPanicOutcome checks the invariants common to every panic test: a 500, one AttemptEnded per started
// attempt (the ones that did not end as a retry are failed with class panic), one completion, and no slot held.
func wantPanicOutcome(t *testing.T, e *regEnv, rec *recObserver, status int, seq []string, workers ...string) {
	t.Helper()
	if status != http.StatusInternalServerError {
		t.Fatalf("the client must get a 500, got %d", status)
	}
	wantSeq(t, rec.await(t), seq...)
	starts, ends := map[string]int{}, map[string]int{}
	for _, a := range rec.att {
		starts[a.AttemptID]++
	}
	for _, a := range rec.ends {
		ends[a.AttemptID]++
		if a.Outcome == AttemptFailed && a.Class != "panic" {
			t.Fatalf("a failed attempt ended by a panic carries class panic, got %+v", a)
		}
	}
	for id, n := range starts {
		if n != 1 || ends[id] != 1 {
			t.Fatalf("attempt %s: %d starts, %d ends (want exactly one each)", id, n, ends[id])
		}
	}
	if len(ends) != len(starts) {
		t.Fatalf("an attempt that never started was ended: %d starts, %d ends", len(starts), len(ends))
	}
	if len(rec.done) != 1 {
		t.Fatalf("RequestCompleted fired %d times", len(rec.done))
	}
	allIdle(t, e, workers...)
}

func panicEnv(t *testing.T, ws ...protocol.WorkerSnapshot) (*regEnv, *recObserver) {
	t.Helper()
	e := newRegEnv(t, "round-robin", ws)
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	return e, rec
}

func TestAPanicInTheRecorderOnTheFirstAttemptReleasesTheSlotAndEndsTheAttempt(t *testing.T) {
	a := newScriptWorker(t, "a", okJSON("a"))
	e, rec := panicEnv(t, a.snapshot("qwen-7b"))
	WithRequestRecorder(&nthRecorder{panicAt: 1})(e.gw)
	resp, _ := e.post(t, false)
	wantPanicOutcome(t, e, rec, resp.StatusCode,
		[]string{"start", "admit", "attempt+1:a", "attempt-1:a:failed", "done:500"}, "a")
	if a.hits.Load() != 0 {
		t.Fatalf("the worker must not be called after the recorder panicked: %d hits", a.hits.Load())
	}
}

func TestAPanicInTheUpstreamCallReleasesTheSlotAndEndsTheAttempt(t *testing.T) {
	a := newScriptWorker(t, "a", okJSON("a"))
	e, rec := panicEnv(t, a.snapshot("qwen-7b"))
	e.injectTransport(&faultTransport{panicSend: func(*http.Request) bool { return true }})
	resp, _ := e.post(t, false)
	wantPanicOutcome(t, e, rec, resp.StatusCode,
		[]string{"start", "admit", "attempt+1:a", "attempt-1:a:failed", "done:500"}, "a")
}

func TestAPanicInTheRecorderAfterARetryEndsBothAttemptsOnceAndReleasesBothSlots(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e, rec := panicEnv(t, refusing(t, "a-bad"), good.snapshot("qwen-7b"))
	WithRequestRecorder(&nthRecorder{panicAt: 2})(e.gw)
	resp, _ := e.post(t, false)
	wantPanicOutcome(t, e, rec, resp.StatusCode, []string{
		"start", "admit", "attempt+1:a-bad", "attempt+2:b-good", "attempt-1:a-bad:retried:retry", "attempt-2:b-good:failed", "done:500",
	}, "a-bad", "b-good")
}

func TestAPanicWhileAbandoningTheFirstAttemptStillReleasesTheNextSlot(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", status(503, `{"error":{"code":"x"}}`))
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e, rec := panicEnv(t, bad.snapshot("qwen-7b"), good.snapshot("qwen-7b"))
	// Closing the abandoned 503 response panics: this is inside abandonAttempt, after the next attempt has
	// been routed and started and before it becomes the current one.
	e.injectTransport(&faultTransport{panicClose: func(r *http.Response) bool { return r.StatusCode == 503 }})
	resp, _ := e.post(t, false)
	wantPanicOutcome(t, e, rec, resp.StatusCode, []string{
		"start", "admit", "attempt+1:a-bad", "attempt+2:b-good", "attempt-1:a-bad:retried:retry", "attempt-2:b-good:failed", "done:500",
	}, "a-bad", "b-good")
}

func TestAPanickingObserverDoesNotDisturbTheAttempts(t *testing.T) {
	a := newScriptWorker(t, "a", okJSON("a"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{a.snapshot("qwen-7b")})
	WithObserver(&panickyObserver{})(e.gw)
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	resp, _ := e.post(t, false)
	if resp.StatusCode != 200 {
		t.Fatalf("an observer panic must not reach the request: %d", resp.StatusCode)
	}
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:a", "attempt-1:a:ok", "done:200")
	allIdle(t, e, "a")
}

// panickyObserver panics in every attempt event.
type panickyObserver struct{ NopObserver }

func (panickyObserver) AttemptStarted(_ context.Context, _ AttemptStart) context.Context {
	panic("injected: observer")
}
func (panickyObserver) AttemptEnded(_ context.Context, _ AttemptEnd) { panic("injected: observer") }

func TestAClientThatLeavesEndsTheAttemptOnceAndFreesTheSlot(t *testing.T) {
	started := make(chan struct{}, 1)
	a := newScriptWorker(t, "a", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // the server notices a closed connection only once the body is read
		started <- struct{}{}
		<-r.Context().Done()
	})
	e, rec := panicEnv(t, a.snapshot("qwen-7b"))
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath,
		strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := e.client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started
	cancel()
	<-done
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:a", "attempt-1:a:client_closed", "done:499")
	allIdle(t, e, "a")
}

// A panic on every request must not leak slots (a worker of capacity 1 would soon refuse everything) or goroutines.
func TestRepeatedPanicsLeakNeitherSlotsNorGoroutines(t *testing.T) {
	a := newScriptWorker(t, "a", okJSON("a"))
	snap := a.snapshot("qwen-7b")
	snap.MaxConcurrency = 1
	e, _ := panicEnv(t, snap)
	rcd := &nthRecorder{}
	WithRequestRecorder(rcd)(e.gw)
	settle := func() int {
		e.client.CloseIdleConnections()
		e.gw.router.client.CloseIdleConnections()
		time.Sleep(150 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	e.post(t, false) // warm the pools
	base := settle()

	rcd.panicAt = 0
	for i := 0; i < 40; i++ {
		rcd.calls.Store(0)
		rcd.panicAt = 1
		resp, _ := e.post(t, false)
		if resp.StatusCode != 500 {
			t.Fatalf("iteration %d: %d", i, resp.StatusCode)
		}
		if n := e.gw.router.InFlight("a"); n != 0 {
			eventually(t, 2*time.Second, func() bool { return e.gw.router.InFlight("a") == 0 }, fmt.Sprintf("the slot to be released (iteration %d, in flight %d)", i, n))
		}
	}
	rcd.panicAt = 0
	if resp, body := e.post(t, false); resp.StatusCode != 200 {
		t.Fatalf("a worker of capacity 1 must still be usable after the panics: %d %s", resp.StatusCode, body)
	}
	eventually(t, 5*time.Second, func() bool { return settle() <= base+5 }, fmt.Sprintf("goroutines to return near the baseline of %d", base))
	allIdle(t, e, "a")
}

// guardEnv reserves (and optionally starts) an attempt by hand so closeAttempts can be driven directly.
func guardEnv(t *testing.T, start bool) (*regEnv, *recObserver, *attemptGuard, *reqInfo, *attemptRun) {
	t.Helper()
	a := newScriptWorker(t, "a", okJSON("a"))
	e, rec := panicEnv(t, a.snapshot("qwen-7b"))
	r := httptest.NewRequest(http.MethodPost, chatCompletionsPath, nil)
	info := &reqInfo{id: "req_x", start: time.Now()}
	ireq := &protocol.InferenceRequest{Model: "qwen-7b"}
	run, rerr := e.gw.reserveAttempt(r, ireq, info, nil)
	if rerr != nil {
		t.Fatal(rerr.Error)
	}
	if e.gw.router.InFlight("a") != 1 {
		t.Fatal("reserving takes the slot")
	}
	g := &attemptGuard{}
	g.track(run)
	if start {
		e.gw.startAttempt(r, ireq, info, run)
	}
	return e, rec, g, info, run
}

func TestAReservedButUnstartedAttemptReleasesItsSlotWithoutEnding(t *testing.T) {
	e, rec, g, info, _ := guardEnv(t, false)
	e.gw.closeAttempts(g, info, "boom")
	if n := e.gw.router.InFlight("a"); n != 0 {
		t.Fatalf("the slot of an unstarted attempt leaked: %d", n)
	}
	if len(rec.ends) != 0 || len(rec.att) != 0 || len(info.attempts) != 0 {
		t.Fatalf("an attempt that never started has no AttemptEnded: %v %v", rec.seq, info.attempts)
	}
	e.gw.closeAttempts(g, info, "boom") // idempotent
}

func TestClosingAStartedAttemptTwiceEndsItOnce(t *testing.T) {
	e, rec, g, info, run := guardEnv(t, true)
	e.gw.closeAttempts(g, info, "boom")
	e.gw.abandonAttempt(run, info, outcomeFailed)
	e.gw.closeAttempts(g, info, nil)
	if n := e.gw.router.InFlight("a"); n != 0 {
		t.Fatalf("leaked slot: %d", n)
	}
	wantSeq(t, rec.sequence(), "attempt+1:a", "attempt-1:a:failed")
	if rec.ends[0].Class != "panic" || info.attempts[0].Outcome != outcomeFailed || info.attempts[0].Class != "panic" {
		t.Fatalf("%+v %+v", rec.ends[0], info.attempts[0])
	}
}

// http.ErrAbortHandler is the deliberate abort after a response body failed, not a panic: the attempt ends the
// way it always has, as failed with whatever class it had.
func TestTheDeliberateAbortIsNotClassifiedAsAPanic(t *testing.T) {
	e, rec, g, info, run := guardEnv(t, true)
	run.class = "reset"
	info.errCode = "INFERENCE_FAILED"
	e.gw.closeAttempts(g, info, http.ErrAbortHandler)
	if len(rec.ends) != 1 || rec.ends[0].Outcome != AttemptFailed || rec.ends[0].Class != "reset" {
		t.Fatalf("%+v", rec.ends)
	}
	allIdle(t, e, "a")
}
