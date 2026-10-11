package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Attempt outcomes (also Prometheus label values; keep the set fixed).
const (
	outcomeOK           = "ok"            // the response was relayed and completed
	outcomeFailed       = "failed"        // the response was an error, or the stream failed after output began
	outcomeRetried      = "retried"       // abandoned before output; the request moved to another worker
	outcomeClientClosed = "client_closed" // the client left
)

// headerAttempts tells a client how many attempts a request needed. It is sent only when more than one
// was used, and never names a worker.
const headerAttempts = "X-ServerFlow-Attempts"

// attemptRecord is one try of a request on one worker. The history of a request is only appended to.
type attemptRecord struct {
	ID       string
	Worker   string
	Started  time.Time
	Duration time.Duration
	Outcome  string
	Class    string // why it was retried or failed: connect, reset, empty_stream, status_NNN, or ""
}

type attemptCtxKey struct{}

func withAttemptID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, attemptCtxKey{}, id)
}

func attemptIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(attemptCtxKey{}).(string)
	return id
}

// attemptRun is one attempt in flight: its worker slot, its own cancel and idle clock, and what came back.
type attemptRun struct {
	rec       int // index into info.attempts; valid once started
	target    *routed
	selecting time.Time // when choosing the worker began
	ctx       context.Context
	cancel    context.CancelFunc
	idle      *time.Timer

	// started is set when the attempt has a record and the observers were told (AttemptStarted); ended when
	// its slot has been released and, if it started, AttemptEnded delivered. Both only ever turn true. A run
	// that holds a slot but has not started is released without an AttemptEnded: every AttemptStarted is
	// followed by exactly one AttemptEnded, and an attempt that never started has none.
	started, ended bool

	resp *http.Response // nil when err is set
	src  io.Reader      // the body to relay: resp.Body, with an already-read first chunk in front for streams
	err  error          // a transport error, or a stream that failed before its first byte

	retryable bool
	class     string

	// octx is the context the observers returned for this attempt; nextUnavailable records that a retry was
	// wanted but no other worker could take it, and nextRoute (a NextRoute* value) why.
	octx            context.Context
	nextUnavailable bool
	nextRoute       string

	// Resilience (Phase 15; all zero when the feature is off). guard bounds the retryable phase of this
	// attempt; verdict is what the breaker was told at the retry-decision point (a Verdict* value, "" until
	// then); ticketDone says the breaker ticket has been settled, by that verdict or by a release; retryDenied
	// says why a wanted retry was not made; peek is a body peek of a held 503 that may still be reading.
	guard       *phaseGuard
	verdict     string
	ticketDone  bool
	retryDenied string
	peek        *peekRead
}

// nextRouteResult names why a retry's routing failed, for AttemptEnd.NextRouteResult.
func nextRouteResult(e *api.Error) string {
	switch e.Code {
	case api.CodeNoCapacity:
		return NextRouteNoCapacity
	case api.CodeWorkerUnavailable:
		return NextRouteWorkerUnavailable
	}
	return NextRouteError
}

// errUnexpectedStatus is what an informational (1xx) worker response becomes: a failed attempt that is
// not retried and shows the client a 502.
var errUnexpectedStatus = errors.New("the worker answered with an unexpected status")

// idleTimeoutError is what a stalled stream's first read becomes, so it maps to a 504 like any timeout.
type idleTimeoutError struct{}

func (idleTimeoutError) Error() string   { return "the worker stopped sending" }
func (idleTimeoutError) Timeout() bool   { return true }
func (idleTimeoutError) Temporary() bool { return true }

// forwardRegistry serves a request in registry mode: it picks a worker, sends the request, and while nothing
// has been written to the client it may move to a different worker (ADR-012). Once output has begun there is
// no second try.
func (s *Server) forwardRegistry(w http.ResponseWriter, r *http.Request, rc *http.ResponseController, ireq *protocol.InferenceRequest, body []byte, info *reqInfo, start time.Time) {
	run, rerr := s.reserveAttempt(r, ireq, info, nil)
	if rerr != nil {
		s.refuseNoWorker(w, r, info, ireq, rerr.Error)
		return
	}
	info.model = ireq.Model
	if s.res != nil {
		// A routed first attempt: what the retry budget measures retries against (D5).
		s.res.budget.NoteFirst(ireq.Model)
		info.deadlineAt = s.res.firstByteDeadline(s.res.now())
	}

	// From here until the function returns, every run that holds a slot is released and every started attempt
	// is ended, whatever happens: a normal finish, a client that left, a panic in the recorder, the upstream
	// call or an observer, or a panic in the middle of swapping one attempt for the next (the guard keeps both
	// runs reachable). The panic itself is not swallowed; withRequest turns it into a 500.
	var g attemptGuard
	defer func() {
		p := recover()
		s.closeAttempts(&g, info, p)
		if p != nil {
			panic(p)
		}
	}()
	g.track(run)
	s.startAttempt(r, ireq, info, run)
	s.recordWorker(info, run.target.worker.WorkerID, len(info.attempts))

	var tried []string
	for {
		s.sendAttempt(r, run, body, info)
		wantRetry := r.Context().Err() == nil && run.retryable && len(info.attempts) < s.retry.max
		var reserved *resilience.Reservation
		if wantRetry && s.res != nil {
			// The deadline and the budget are checked before the retry is routed: a retry that is denied never
			// reaches AttemptStarted, and the budget is reserved at the check and refunded below if no other
			// worker is found, so only a retry whose send begins stays consumed (D5).
			reserved, wantRetry = s.permitRetry(run, info, ireq.Model)
		}
		s.settleVerdict(r, run, info, wantRetry)
		if !wantRetry {
			break
		}
		// Find the next worker before letting go of this response: if there is none, this response is
		// what the client gets (ADR-012 D7).
		next, nextErr := s.reserveAttempt(r, ireq, info, append(tried, run.target.worker.WorkerID))
		if nextErr != nil {
			s.res.refund(reserved)
			run.nextUnavailable = true
			if r.Context().Err() == nil { // a client that left is not a scheduling outcome
				run.nextRoute = nextRouteResult(nextErr.Error)
			}
			break
		}
		g.track(next)
		s.startAttempt(r, ireq, info, next)
		tried = append(tried, run.target.worker.WorkerID)
		prev := run
		run, g.cur = next, next // the guard owns both until the old one is closed
		s.abandonAttempt(prev, info, outcomeRetried)
		s.recordWorker(info, run.target.worker.WorkerID, len(info.attempts))
	}

	switch {
	case r.Context().Err() != nil:
		info.clientClosed = true
		if run.resp != nil {
			_ = run.resp.Body.Close()
		}
	case run.err != nil:
		s.fail(w, info, mapUpstreamError(run.err))
	default:
		s.joinPeek(run)
		s.relay(w, r, rc, run.resp, run.src, run.idle, info, start)
	}
}

// attemptGuard remembers the attempts of one request that may still hold a slot: at most the current one and,
// while a retry is being swapped in, the one it replaces.
type attemptGuard struct {
	runs []*attemptRun
	cur  *attemptRun
}

// track registers a run that holds a slot, and makes it the current one if it is the first.
func (g *attemptGuard) track(run *attemptRun) {
	g.runs = append(g.runs, run)
	if g.cur == nil {
		g.cur = run
	}
}

// closeAttempts is the single cleanup of forwardRegistry. p is the value recovered from a panic, or nil. A
// panic ends every attempt still open as failed with class "panic"; http.ErrAbortHandler is not a panic here
// but the deliberate abort after a response body failed, and ends the attempt as it always did.
func (s *Server) closeAttempts(g *attemptGuard, info *reqInfo, p any) {
	panicked := p != nil && p != http.ErrAbortHandler
	for _, run := range g.runs {
		switch {
		case run.ended:
		case panicked:
			run.class = classPanic
			s.abandonAttempt(run, info, outcomeFailed)
		case run == g.cur:
			s.finishAttempt(run, info)
		default:
			s.abandonAttempt(run, info, outcomeFailed)
		}
	}
}

// refuseNoWorker answers a request for which no worker was selected: it tells the observers, logs, and sends
// the error. A client that left while a worker was being chosen is recorded as closed instead.
func (s *Server) refuseNoWorker(w http.ResponseWriter, r *http.Request, info *reqInfo, ireq *protocol.InferenceRequest, apiErr *api.Error) {
	// info.model becomes a metrics label, so only a model the registry confirmed exists may be
	// recorded; arbitrary client text must not.
	if apiErr.Code == api.CodeNoCapacity {
		info.model = ireq.Model
	}
	if r.Context().Err() != nil {
		info.clientClosed = true // the client left while we were choosing
		return
	}
	s.rejectedAPI(r.Context(), info, apiErr)
	// Unknown models are client typos or probes, which a client could use to fill the log, so
	// they stay at debug.
	level := slog.LevelInfo
	if apiErr.Code == api.CodeModelNotFound {
		level = slog.LevelDebug
	}
	s.log.Log(r.Context(), level, "no worker selected", append([]any{"request_id", info.id, "attempt_id", info.attemptID,
		"strategy", s.router.strategy, "error_code", apiErr.Code}, tenantAttrs(info)...)...)
	s.fail(w, info, apiErr)
}

// refuseUnknownModel is the registry-mode model pre-check (Phase 15 D3). It runs before the rate limiter so a
// request for a model no worker serves costs the tenant no quota: it is answered 404 here, as routing would
// have answered it, without taking a request, token or concurrency charge. With no usable snapshot nothing is
// known and the request goes on to the limiter as before. It reports whether it answered.
func (s *Server) refuseUnknownModel(w http.ResponseWriter, r *http.Request, info *reqInfo, ireq *protocol.InferenceRequest) bool {
	checking := time.Now()
	serves, ok := s.router.cache.Serves(ireq.Model)
	if !ok || serves {
		return false
	}
	info.selectStart, info.selectDuration = checking, time.Since(checking)
	s.admitted(r.Context(), info) // no rate limit decision was made: the admission says so
	s.refuseNoWorker(w, r, info, ireq, api.ErrModelNotFound(ireq.Model))
	return true
}

// reserveAttempt picks a worker for the next attempt and takes its slot. It tells no observer and writes no
// record: the caller must track the run (so a panic releases the slot) and then call startAttempt. exclude
// lists the workers already tried for this request.
func (s *Server) reserveAttempt(r *http.Request, ireq *protocol.InferenceRequest, info *reqInfo, exclude []string) (*attemptRun, *routeError) {
	selecting := time.Now()
	target, rerr := s.router.Route(r.Context(), ireq, exclude...)
	info.selectStart, info.selectDuration = selecting, time.Since(selecting)
	s.res.flush() // transitions the routing itself caused (a cool-down that ended); a no-op without resilience
	if rerr != nil {
		return nil, rerr
	}
	return &attemptRun{target: target, selecting: selecting}, nil
}

// startAttempt records the attempt and tells the observers. From here the run owes an AttemptEnded.
func (s *Server) startAttempt(r *http.Request, ireq *protocol.InferenceRequest, info *reqInfo, run *attemptRun) {
	id := protocol.NewAttemptID()
	info.attemptID = id
	target := run.target
	info.attempts = append(info.attempts, attemptRecord{ID: id, Worker: target.worker.WorkerID, Started: time.Now()})
	idx := len(info.attempts) - 1
	run.rec, run.started = idx, true
	run.octx = r.Context() // until the observers return theirs; AttemptEnded needs a context even if they did not
	info.attempted = len(info.attempts)
	octx := s.obs.AttemptStarted(r.Context(), s.attemptStartEvent(info, idx+1, target.worker.WorkerID, ireq.Model, s.router.strategy,
		string(target.worker.State), target.worker.Eligible, info.attempts[idx].Started.Sub(run.selecting), target.probe()))
	info.attemptCtx, run.octx = octx, octx
	run.ctx, run.cancel = context.WithCancel(withAttemptID(octx, id))
	run.idle = time.AfterFunc(s.cfg.UpstreamIdleTimeout, run.cancel)
	run.idle.Stop()
	if s.res != nil {
		hdr := time.Duration(0)
		if target.probe() {
			hdr = s.res.probeHeader
		}
		run.guard = newPhaseGuard(run.cancel, info.deadlineAt.Sub(s.res.now()), hdr)
	}
	s.log.Debug("worker selected", append([]any{"request_id", info.id, "attempt_id", id, "attempt", idx + 1, "strategy", s.router.strategy,
		"worker_id", target.worker.WorkerID, "model", ireq.Model}, tenantAttrs(info)...)...)
}

// sendAttempt sends the request to the attempt's worker and decides whether what came back may be retried.
// For a stream it reads the first chunk before returning, so nothing has reached the client by then.
func (s *Server) sendAttempt(r *http.Request, run *attemptRun, body []byte, info *reqInfo) {
	s.sendOnce(r, run, body, info)
	if run.guard != nil && run.guard.end() && r.Context().Err() == nil {
		s.markDeadline(run, info)
	}
}

// sendOnce is the wire part of sendAttempt: the call, and for a 200 the wait for its first byte.
func (s *Server) sendOnce(r *http.Request, run *attemptRun, body []byte, info *reqInfo) {
	resp, err := run.target.up.Do(run.ctx, chatCompletionsPath, body, info.id)
	if run.guard != nil {
		run.guard.headersDone()
		if run.guard.headerExpired() && r.Context().Err() == nil {
			// The probe's own, shorter header timeout fired: a timeout like the transport's (not retried).
			if resp != nil {
				_ = resp.Body.Close()
			}
			resp, err = nil, headerTimeoutError{}
		}
	}
	if err != nil {
		run.err = err
		if r.Context().Err() == nil {
			s.log.Warn("worker request failed", append([]any{"request_id", info.id, "attempt_id", info.attemptID,
				"worker_id", run.target.worker.WorkerID, "error", errText(err)}, traceAttrs(run.octx)...)...)
			run.class, run.retryable = s.retry.transportError(err)
		}
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode > 599 {
		// A worker has no business answering with an informational status (a 101 would try to switch
		// protocols) or one outside HTTP's defined classes. Never relay it, and keep metric labels bounded.
		_ = resp.Body.Close()
		run.err = errUnexpectedStatus
		return
	}
	run.resp, run.src = resp, resp.Body

	if class, ok := s.retry.status(resp.StatusCode); ok {
		run.class, run.retryable = class, true
		return
	}
	if resp.StatusCode != http.StatusOK {
		return
	}

	// A 200, stream or not: hold the response until its first body byte arrives, so a worker that fails
	// before producing anything can still be retried. Nothing is lost by waiting: a client sees no useful
	// bytes before this either. Only the first chunk is held; the rest streams through. The idle clock
	// bounds the wait.
	buf := make([]byte, copyBufferSize)
	reader := &idleReader{r: resp.Body, timer: run.idle, d: s.cfg.UpstreamIdleTimeout}
	n, rerr := reader.Read(buf)
	for n == 0 && rerr == nil {
		n, rerr = reader.Read(buf)
	}
	if n > 0 {
		run.src = io.MultiReader(bytes.NewReader(buf[:n]), resp.Body)
		return
	}
	run.err = rerr
	if r.Context().Err() == nil {
		if run.ctx.Err() != nil {
			run.err = idleTimeoutError{} // the idle clock cut a stalled worker
		}
		run.class, run.retryable = s.retry.firstChunkError(run.err)
		s.log.Warn("worker response failed before its first byte", "request_id", info.id, "attempt_id", info.attemptID,
			"worker_id", run.target.worker.WorkerID, "error", errText(rerr))
	}
	_ = resp.Body.Close()
	run.resp = nil
}

// abandonAttempt closes an attempt: it stops the idle clock, cancels the upstream call, closes the response,
// releases the worker slot and, if the attempt started, ends it with outcome. It is idempotent. The steps are
// deferred so that each one runs even if an earlier one panics: the slot is always released and the attempt
// always ended.
func (s *Server) abandonAttempt(run *attemptRun, info *reqInfo, outcome string) {
	if run.ended {
		return
	}
	run.ended = true
	defer func() {
		if run.started {
			s.endAttempt(run, info, outcome)
		}
	}()
	defer run.target.release()
	defer s.returnTicket(run)
	defer func() {
		if run.resp != nil {
			_ = run.resp.Body.Close()
		}
	}()
	if run.guard != nil {
		run.guard.stop()
	}
	if run.idle != nil {
		run.idle.Stop()
	}
	if run.cancel != nil {
		run.cancel()
	}
}

// finishAttempt closes out the attempt whose response was relayed (or whose failure was reported).
func (s *Server) finishAttempt(run *attemptRun, info *reqInfo) {
	outcome := outcomeOK
	switch {
	case info.clientClosed:
		outcome = outcomeClientClosed
	case info.errCode != "" || run.err != nil || (run.resp != nil && run.resp.StatusCode >= 500):
		outcome = outcomeFailed
	}
	s.recordLateFault(run, info)
	s.abandonAttempt(run, info, outcome)
}

func (s *Server) endAttempt(run *attemptRun, info *reqInfo, outcome string) {
	rec := &info.attempts[run.rec]
	rec.Duration, rec.Outcome, rec.Class = time.Since(rec.Started), outcome, run.class
	s.obs.AttemptEnded(run.octx, AttemptEnd{
		RequestID: info.id, AttemptID: rec.ID, Number: run.rec + 1, WorkerID: rec.Worker, Model: info.model, Outcome: outcome,
		Duration: rec.Duration, Class: rec.Class, WillRetry: outcome == outcomeRetried, NextWorkerUnavailable: run.nextUnavailable,
		NextRouteResult: run.nextRoute, RetryDenied: run.retryDenied, Probe: run.target.probe(), Verdict: run.finalVerdict(),
	})
	level := slog.LevelDebug
	if outcome != outcomeOK {
		level = slog.LevelInfo
	}
	attrs := []any{"request_id", info.id, "attempt_id", rec.ID, "attempt", run.rec + 1, "worker_id", rec.Worker,
		"outcome", outcome, "duration_ms", rec.Duration.Milliseconds()}
	if rec.Class != "" {
		attrs = append(attrs, "class", rec.Class)
	}
	if run.resp != nil {
		attrs = append(attrs, "worker_status", run.resp.StatusCode)
	}
	if v := run.finalVerdict(); v != "" {
		attrs = append(attrs, "verdict", v)
	}
	if run.target.probe() {
		attrs = append(attrs, "probe", true)
	}
	if run.retryDenied != "" {
		attrs = append(attrs, "retry_denied", run.retryDenied)
	}
	attrs = append(attrs, traceAttrs(run.octx)...)
	attrs = append(attrs, tenantAttrs(info)...)
	s.log.Log(context.Background(), level, "attempt finished", attrs...)
}

// noteAttempts adds the attempts header when a request needed more than one try.
func noteAttempts(h http.Header, info *reqInfo) {
	if n := len(info.attempts); n > 1 {
		h.Set(headerAttempts, strconv.Itoa(n))
	}
}
