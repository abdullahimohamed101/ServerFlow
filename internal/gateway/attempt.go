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
	rec    int // index into info.attempts
	target *routed
	ctx    context.Context
	cancel context.CancelFunc
	idle   *time.Timer

	resp *http.Response // nil when err is set
	src  io.Reader      // the body to relay: resp.Body, with an already-read first chunk in front for streams
	err  error          // a transport error, or a stream that failed before its first byte

	retryable bool
	class     string
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
	run, apiErr := s.routeAttempt(r, ireq, info, nil)
	if apiErr != nil {
		// info.model becomes a metrics label, so only a model the registry confirmed exists may be
		// recorded; arbitrary client text must not.
		if apiErr.Code == api.CodeNoCapacity {
			info.model = ireq.Model
		}
		if r.Context().Err() != nil {
			info.clientClosed = true // the client left while we were choosing
			return
		}
		// Unknown models are client typos or probes, which a client could use to fill the log, so
		// they stay at debug.
		level := slog.LevelInfo
		if apiErr.Code == api.CodeModelNotFound {
			level = slog.LevelDebug
		}
		s.log.Log(r.Context(), level, "no worker selected", append([]any{"request_id", info.id, "attempt_id", info.attemptID,
			"strategy", s.router.strategy, "error_code", apiErr.Code}, tenantAttrs(info)...)...)
		s.fail(w, info, apiErr)
		return
	}
	info.model = ireq.Model

	var tried []string
	for {
		s.sendAttempt(r, run, body, info)
		if r.Context().Err() != nil || !run.retryable || len(info.attempts) >= s.retry.max {
			break
		}
		// Find the next worker before letting go of this response: if there is none, this response is
		// what the client gets (ADR-012 D7).
		next, nextErr := s.routeAttempt(r, ireq, info, append(tried, run.target.worker.WorkerID))
		if nextErr != nil {
			break
		}
		tried = append(tried, run.target.worker.WorkerID)
		s.abandonAttempt(run, info, outcomeRetried)
		run = next
	}
	defer s.finishAttempt(run, info)
	defer run.idle.Stop()
	defer run.cancel()

	switch {
	case r.Context().Err() != nil:
		info.clientClosed = true
		if run.resp != nil {
			_ = run.resp.Body.Close()
		}
	case run.err != nil:
		s.fail(w, info, mapUpstreamError(run.err))
	default:
		s.relay(w, r, rc, run.resp, run.src, run.idle, info, start)
	}
}

// routeAttempt picks a worker for the next attempt and starts its record. exclude lists the workers
// already tried for this request.
func (s *Server) routeAttempt(r *http.Request, ireq *protocol.InferenceRequest, info *reqInfo, exclude []string) (*attemptRun, *api.Error) {
	target, apiErr := s.router.Route(r.Context(), ireq, exclude...)
	if apiErr != nil {
		return nil, apiErr
	}
	id := protocol.NewAttemptID()
	info.attemptID = id
	info.attempts = append(info.attempts, attemptRecord{ID: id, Worker: target.worker.WorkerID, Started: time.Now()})
	idx := len(info.attempts) - 1
	ctx, cancel := context.WithCancel(withAttemptID(r.Context(), id))
	idle := time.AfterFunc(s.cfg.UpstreamIdleTimeout, cancel)
	idle.Stop()
	s.log.Debug("worker selected", append([]any{"request_id", info.id, "attempt_id", id, "attempt", idx + 1, "strategy", s.router.strategy,
		"worker_id", target.worker.WorkerID, "model", ireq.Model}, tenantAttrs(info)...)...)
	return &attemptRun{rec: idx, target: target, ctx: ctx, cancel: cancel, idle: idle}, nil
}

// sendAttempt sends the request to the attempt's worker and decides whether what came back may be retried.
// For a stream it reads the first chunk before returning, so nothing has reached the client by then.
func (s *Server) sendAttempt(r *http.Request, run *attemptRun, body []byte, info *reqInfo) {
	resp, err := run.target.up.Do(run.ctx, chatCompletionsPath, body, info.id)
	if err != nil {
		run.err = err
		if r.Context().Err() == nil {
			s.log.Warn("worker request failed", "request_id", info.id, "attempt_id", info.attemptID,
				"worker_id", run.target.worker.WorkerID, "error", errText(err))
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

// abandonAttempt gives up on an attempt whose response will not be used.
func (s *Server) abandonAttempt(run *attemptRun, info *reqInfo, outcome string) {
	run.idle.Stop()
	run.cancel()
	if run.resp != nil {
		_ = run.resp.Body.Close()
	}
	run.target.release()
	s.endAttempt(run, info, outcome)
	if outcome == outcomeRetried {
		s.metrics.observeRetry(info.model, run.class)
	}
}

// finishAttempt closes out the attempt whose response was relayed (or whose failure was reported).
func (s *Server) finishAttempt(run *attemptRun, info *reqInfo) {
	run.target.release()
	outcome := outcomeOK
	switch {
	case info.clientClosed:
		outcome = outcomeClientClosed
	case info.errCode != "" || run.err != nil || (run.resp != nil && run.resp.StatusCode >= 500):
		outcome = outcomeFailed
	}
	s.endAttempt(run, info, outcome)
}

func (s *Server) endAttempt(run *attemptRun, info *reqInfo, outcome string) {
	rec := &info.attempts[run.rec]
	rec.Duration, rec.Outcome, rec.Class = time.Since(rec.Started), outcome, run.class
	s.metrics.observeAttempt(info.model, outcome)
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
	attrs = append(attrs, tenantAttrs(info)...)
	s.log.Log(context.Background(), level, "attempt finished", attrs...)
}

// noteAttempts adds the attempts header when a request needed more than one try.
func noteAttempts(h http.Header, info *reqInfo) {
	if n := len(info.attempts); n > 1 {
		h.Set(headerAttempts, strconv.Itoa(n))
	}
}
