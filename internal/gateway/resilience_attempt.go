package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/resilience"
)

// The request-path half of Phase 15 resilience (ADR-020): the failure taxonomy and the verdict a breaker is told
// at the retry-decision point (D4, D7), the bounded body peek of a held 503, the retry budget and deadline checks
// that precede a retry's routing (D5, D6), and the single return of the breaker ticket. Every function returns
// before doing anything when Server.res is nil.

// headerTimeoutError is what a probe attempt's own, shorter header timeout becomes. It is a timeout like the
// transport's: not retried, a 504 for the client, a fault for the breaker.
type headerTimeoutError struct{}

func (headerTimeoutError) Error() string   { return "the worker did not send response headers in time" }
func (headerTimeoutError) Timeout() bool   { return true }
func (headerTimeoutError) Temporary() bool { return true }

// deadlineError is what the first-byte deadline becomes: a timeout, so the client gets 504 UPSTREAM_TIMEOUT.
type deadlineError struct{}

func (deadlineError) Error() string {
	return "the first-byte deadline elapsed before the worker produced output"
}
func (deadlineError) Timeout() bool   { return true }
func (deadlineError) Temporary() bool { return true }

// phaseGuard bounds the retryable phase of one attempt (D6): a timer that cancels the attempt when the
// request's first-byte deadline passes, and for a probe a second one for its own header timeout. A context
// deadline cannot be lifted once the first byte has arrived, and the attempt's context also governs the body
// stream that follows, so these are timers that end() disarms at the end of the phase ("no effect after the first
// byte"). A timer that fired before end() decides the outcome; one that has not can no longer fire.
type phaseGuard struct {
	mu          sync.Mutex
	cancel      func()
	done        bool
	headers     bool
	deadlineHit bool
	headerHit   bool
	dt, ht      *time.Timer
}

func newPhaseGuard(cancel func(), remaining, header time.Duration) *phaseGuard {
	g := &phaseGuard{cancel: cancel}
	g.dt = time.AfterFunc(max(remaining, 0), g.onDeadline)
	if header > 0 {
		g.ht = time.AfterFunc(header, g.onHeader)
	}
	return g
}

func (g *phaseGuard) onDeadline() {
	g.mu.Lock()
	if g.done {
		g.mu.Unlock()
		return
	}
	g.deadlineHit = true
	g.mu.Unlock()
	g.cancel()
}

func (g *phaseGuard) onHeader() {
	g.mu.Lock()
	if g.done || g.headers {
		g.mu.Unlock()
		return
	}
	g.headerHit = true
	g.mu.Unlock()
	g.cancel()
}

// headersDone disarms the probe header timer: the response headers arrived (or the call failed).
func (g *phaseGuard) headersDone() {
	g.mu.Lock()
	g.headers = true
	if g.ht != nil {
		g.ht.Stop()
	}
	g.mu.Unlock()
}

func (g *phaseGuard) headerExpired() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.headerHit
}

// end disarms the guard and reports whether the deadline had already fired.
func (g *phaseGuard) end() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.done = true
	g.dt.Stop()
	if g.ht != nil {
		g.ht.Stop()
	}
	return g.deadlineHit
}

func (g *phaseGuard) stop() { g.end() }

// afterArrivalHook, when set by a test, runs right after a response has arrived and the deadline has been disarmed.
var afterArrivalHook func()

// arrived ends the retryable phase of the attempt at the moment its response (headers, and for a 200 the first
// byte) is in hand. The guard decides under one lock whether the deadline or the arrival came first: if the arrival
// did, the timer is inert from here on and can no longer cancel the attempt, however long the caller takes to get
// back to the decision point; if the deadline did, the attempt is the deadline's (markDeadline).
func (s *Server) arrived(run *attemptRun) {
	if run.guard != nil {
		run.guard.end()
	}
	if afterArrivalHook != nil {
		afterArrivalHook()
	}
}

// markDeadline turns an attempt cut by the first-byte deadline into a 504 that is not retried and not a fault.
func (s *Server) markDeadline(run *attemptRun, info *reqInfo) {
	if run.resp != nil {
		_ = run.resp.Body.Close()
	}
	run.resp, run.src = nil, nil
	run.err = deadlineError{}
	run.class, run.retryable = classDeadline, false
	s.log.Warn("first-byte deadline reached before the worker produced output", "request_id", info.id, "attempt_id", info.attemptID,
		"worker_id", run.target.worker.WorkerID)
}

// permitRetry applies the deadline and the retry budget to a retry that is otherwise wanted. It runs before the
// retry is routed. The deadline is checked first, so a denial for lack of time reserves nothing; the budget is
// reserved here and refunded by the caller if no other worker is found. On a denial the held response stays the
// client's and the attempt records why.
func (s *Server) permitRetry(run *attemptRun, info *reqInfo, model string) (*resilience.Reservation, bool) {
	rs := s.res
	if rem := info.deadlineAt.Sub(rs.now()); rem <= 0 || rem < rs.minRemaining {
		run.retryDenied = RetryDeniedDeadline
		s.log.Debug("retry denied: not enough time left", "request_id", info.id, "attempt_id", info.attemptID, "model", model)
		return nil, false
	}
	resv, ok := rs.budget.Reserve(model)
	if !ok {
		run.retryDenied = RetryDeniedBudget
		s.log.Debug("retry denied: budget used", "request_id", info.id, "attempt_id", info.attemptID, "model", model)
		return nil, false
	}
	return resv, true
}

// refund gives a reserved retry back. Safe on a nil state and a nil reservation.
func (rs *resilienceState) refund(r *resilience.Reservation) {
	if rs == nil {
		return
	}
	rs.budget.Refund(r)
}

// finalVerdict is the verdict label for AttemptEnd: what the breaker was told, or neutral for a probe that ended
// before it was told anything (its ticket is returned unused).
func (r *attemptRun) finalVerdict() string {
	if r.verdict == "" && r.target != nil && r.target.probe() {
		return VerdictNeutral
	}
	return r.verdict
}

// settleVerdict gives the attempt's breaker its verdict at the retry-decision point, which is where sendAttempt
// has returned: headers plus first byte. retryPossible says a retry will be attempted (attempts left, deadline
// and budget allowing), which is the only case in which a 503's body is peeked. The ticket comes back with the
// verdict; the attempt's cleanup returns it otherwise.
func (s *Server) settleVerdict(r *http.Request, run *attemptRun, info *reqInfo, retryPossible bool) {
	rs := s.res
	if rs == nil {
		return
	}
	v, label := s.verdictOf(r, run, info, retryPossible)
	run.verdict = label
	if !run.ticketDone {
		run.ticketDone = true
		if v == resilience.OK {
			run.okAt = rs.now() // a stream that breaks later converts this ok (recordAfterRelay)
		}
		rs.breakers.Record(run.target.ticket, v)
	}
	rs.flush()
}

// verdictOf applies the failure taxonomy of D4.
func (s *Server) verdictOf(r *http.Request, run *attemptRun, info *reqInfo, retryPossible bool) (resilience.Verdict, string) {
	switch {
	case r.Context().Err() != nil, run.class == classDeadline:
		return resilience.Neutral, VerdictNeutral // the client left or the request ran out of time: nothing about the worker (a panic never reaches a verdict: the cleanup returns the ticket)
	case run.err != nil:
		return resilience.Fault, VerdictFault // connect, reset, empty first chunk, header timeout, stall, anything unexpected
	case run.resp == nil:
		return resilience.Neutral, VerdictNeutral
	}
	code := run.resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return resilience.OK, VerdictOK
	case code == http.StatusServiceUnavailable:
		if retryPossible && run.retryable {
			if s.peekIsNeutral(run, info) {
				return resilience.Neutral, VerdictNeutral
			}
		}
		return resilience.Fault, VerdictFault
	case code == http.StatusBadGateway, code == http.StatusGatewayTimeout:
		return resilience.Fault, VerdictFault
	case code >= 400 && code < 500:
		return resilience.Neutral, VerdictNeutral
	case code >= 300 && code < 400, code >= 500:
		// An unlisted 5xx (a 500 for example) feeds the ratio rule only: never retried, never a streak.
		return resilience.RatioFault, VerdictFault
	}
	return resilience.Neutral, VerdictNeutral
}

// Codes a worker uses for a 503 that says it is busy or changing state rather than broken (D4).
var neutral503Codes = map[string]bool{"queue_full": true, "draining": true, "not_ready": true}

// peekRead is the bounded read of the start of a held 503's body. The read runs on its own goroutine so that the
// wait can be bounded without cancelling the response; if the wait runs out the read carries on, and a response
// that is relayed afterwards is joined with it first (joinPeek). A response that is abandoned is closed, which
// ends the read.
type peekRead struct {
	done chan struct{}
	buf  []byte
	n    int // written before done is closed
}

// peekIsNeutral reads at most peekLimit bytes of a held retryable 503's body for at most min(peekWait, the time
// left for the first byte) and reports whether its error code says the worker is busy or draining. The bytes read
// are put back in front of the body so a relayed response is intact. A read that times out, ends short, or holds no
// recognisable code is a fault (and the retry goes ahead). The body is never logged and never used as a label.
func (s *Server) peekIsNeutral(run *attemptRun, info *reqInfo) bool {
	rs := s.res
	wait := min(rs.peekWait, info.deadlineAt.Sub(rs.now()))
	if wait <= 0 {
		return false
	}
	rs.peeks.Add(1)
	p := &peekRead{done: make(chan struct{}), buf: make([]byte, peekLimit)}
	body := run.resp.Body
	run.peek = p
	// A body that never ends is cut by the idle timeout, so a later join cannot wait forever.
	run.idle.Reset(s.cfg.UpstreamIdleTimeout)
	go func() {
		defer close(p.done)
		defer func() { _ = recover() }()
		p.n, _ = io.ReadFull(body, p.buf)
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-p.done:
		run.idle.Stop()
		s.putBack(run)
		return neutral503Codes[errorCodeOf(p.buf[:p.n])]
	case <-timer.C:
		return false
	}
}

// putBack puts the peeked bytes in front of the body. The read must have finished.
func (s *Server) putBack(run *attemptRun) {
	p := run.peek
	if p == nil {
		return
	}
	run.peek = nil
	run.src = io.MultiReader(bytes.NewReader(p.buf[:p.n]), run.resp.Body)
}

// joinPeek completes a peek that was still reading when its wait ran out, because the response is about to be
// relayed after all (no other worker took the retry). It waits for the read, which the idle timeout bounds.
func (s *Server) joinPeek(run *attemptRun) {
	if run.peek == nil {
		return
	}
	<-run.peek.done
	run.idle.Stop()
	s.putBack(run)
}

// errorCodeOf returns error.code from an OpenAI-shaped error body, or "" when it cannot be found. The input may
// be cut off at any point (a peek is at most 1 KiB): whatever is complete before the cut is honoured.
func errorCodeOf(b []byte) string {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return ""
	}
	for dec.More() {
		kt, err := dec.Token()
		key, _ := kt.(string)
		if err != nil {
			return ""
		}
		if key != "error" {
			if skip(dec) != nil {
				return ""
			}
			continue
		}
		if t, err := dec.Token(); err != nil || t != json.Delim('{') {
			return ""
		}
		for dec.More() {
			kt, err := dec.Token()
			k, _ := kt.(string)
			if err != nil {
				return ""
			}
			if k != "code" {
				if skip(dec) != nil {
					return ""
				}
				continue
			}
			var v any
			if dec.Decode(&v) != nil {
				return ""
			}
			code, _ := v.(string)
			return code
		}
		return ""
	}
	return ""
}

func skip(dec *json.Decoder) error {
	var raw json.RawMessage
	return dec.Decode(&raw)
}

// recordAfterRelay settles what the end of the response says about the worker. A stream whose first byte had arrived
// (the ok was counted then, for the streak and the ratio alike, so successes are never counted later than faults)
// and that then failed is converted: the ok becomes a fault, so each request counts once with its final outcome
// (RecordAbort; for a probe, RecordLate reopens the breaker). The client already has bytes, so nothing is retried.
// A client that left proves nothing either way.
func (s *Server) recordAfterRelay(run *attemptRun, info *reqInfo) {
	rs := s.res
	if rs == nil || run.verdict != VerdictOK || run.resp == nil || info.clientClosed || info.errCode != api.CodeInferenceFailed {
		return
	}
	run.verdict = VerdictFault
	rs.breakers.RecordAbort(run.target.ticket, run.okAt)
	rs.flush()
}

// returnTicket gives back the attempt's breaker ticket if no verdict did (the attempt was cancelled, abandoned or
// panicked before its retry-decision point). Exactly once: the ended flag of abandonAttempt and ticketDone.
func (s *Server) returnTicket(run *attemptRun) {
	rs := s.res
	if rs == nil || run.ticketDone {
		return
	}
	run.ticketDone = true
	rs.breakers.Release(run.target.ticket)
	rs.flush()
}
