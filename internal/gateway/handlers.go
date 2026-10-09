package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/ratelimit"
	"serverflow/pkg/protocol"
)

const (
	chatCompletionsPath = "/v1/chat/completions"
	copyBufferSize      = 16 << 10
)

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.ready.check(r.Context()); err != nil {
		infoFrom(r.Context()).errCode = api.CodeWorkerUnavailable
		api.WriteError(w, api.ErrWorkerUnavailable())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"ready"}`)
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	models := s.cfg.Models
	if s.router != nil {
		var ok bool
		if models, ok = s.router.cache.Models(); !ok {
			infoFrom(r.Context()).errCode = api.CodeWorkerUnavailable
			api.WriteError(w, api.ErrWorkerUnavailable())
			return
		}
	}
	if p := infoFrom(r.Context()).principal; p != nil && p.Policy.AllowedModels != nil {
		// A tenant sees only the models it may use.
		visible := make([]string, 0, len(models))
		for _, m := range models {
			if p.Policy.AllowsModel(m) {
				visible = append(visible, m)
			}
		}
		models = visible
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(api.ModelsBody(models))
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	info := infoFrom(r.Context())
	info.inference = true
	info.attemptID = protocol.NewAttemptID()
	s.metrics.active.Inc()
	defer s.metrics.active.Dec()
	start := time.Now()

	if s.limitRequired && s.limiter == nil { // required but not wired: fail closed
		w.Header().Set("Connection", "close")
		s.fail(w, info, api.ErrInternal())
		return
	}

	// Bound how long the client may take to send the body. net/http resets the
	// read deadline itself once the body is fully read, so it does not cut off
	// a long streaming response (TestStreamsOutliveTheBodyReadTimeout guards
	// that). On failure the deadline must stay expired and the connection be
	// closed: net/http would otherwise try to drain the unread body before
	// replying and could block on a client that never sends it.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(s.bodyReadTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBytes))
	if err != nil {
		w.Header().Set("Connection", "close")
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.fail(w, info, api.ErrBodyTooLarge())
		} else {
			s.fail(w, info, api.ErrInvalidRequest("could not read request body"))
		}
		return
	}

	lim := api.Limits{Models: s.cfg.Models, MaxTokensLimit: s.cfg.MaxTokensLimit, AnyModel: s.router != nil}
	if p := info.principal; p != nil && p.Policy.AllowedModels != nil {
		lim.Allowed = p.Policy.AllowsModel
	}
	ireq, err := api.ParseChatRequest(body, lim)
	if err != nil {
		var apiErr *api.Error
		if !errors.As(err, &apiErr) {
			apiErr = api.ErrInternal()
		}
		s.fail(w, info, apiErr)
		return
	}
	if s.router == nil {
		info.model = ireq.Model // already checked against the configured list
	}
	info.stream = ireq.Stream
	ireq.RequestID = info.id
	if p := info.principal; p != nil {
		// Carried for the scheduler and later phases; quotas and priority are not enforced yet.
		ireq.TenantID, ireq.Priority = p.TenantID, p.Policy.Priority
	}

	if s.limitRequired {
		// After parsing (the token cost needs the body) and the model checks, before any worker is touched.
		// The slot is given back on every way out of this function: a normal return, an error, a client that
		// left, and a panic.
		release, ok := s.admit(w, r, info, ratelimit.EstimateRequestCost(ireq.Messages, ireq.Prompt, len(body), ireq.MaxTokens, s.cfg.MaxTokensLimit), ireq.Model)
		if !ok {
			return
		}
		defer release()
	}

	if s.router != nil {
		s.forwardRegistry(w, r, rc, ireq, body, info, start)
		return
	}
	s.recordWorker(info, "static", 1)

	// upCtx lets the idle timer abort a stalled upstream without touching the
	// client's own context, which is how the two failures are told apart.
	upCtx, cancelUp := context.WithCancel(r.Context())
	defer cancelUp()
	resp, err := s.upstream.Do(upCtx, chatCompletionsPath, body, info.id)
	if err != nil {
		if r.Context().Err() != nil {
			info.clientClosed = true
			return
		}
		s.fail(w, info, mapUpstreamError(err))
		return
	}
	// The idle timer starts stopped; idleReader arms it only while waiting on
	// the upstream.
	idle := time.AfterFunc(s.cfg.UpstreamIdleTimeout, cancelUp)
	idle.Stop()
	defer idle.Stop()
	s.relay(w, r, rc, resp, resp.Body, idle, info, start)
}

// relay sends a worker's response to the client: status and headers first, then the body, streamed
// when it is SSE. body is resp.Body, possibly with bytes already read from it put back in front.
// Nothing here retries: by the time the first byte is written the response is final (spec section 25).
func (s *Server) relay(w http.ResponseWriter, r *http.Request, rc *http.ResponseController, resp *http.Response, body io.Reader, idle *time.Timer, info *reqInfo, start time.Time) {
	defer func() { _ = resp.Body.Close() }()

	// Redirects are never followed; a 3xx means the upstream is misconfigured.
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		s.log.Warn("upstream returned a redirect", "request_id", info.id, "attempt_id", info.attemptID, "status", resp.StatusCode)
		s.fail(w, info, api.ErrInferenceFailed())
		return
	}

	// Upstream error responses received before streaming starts are passed
	// through (they are already OpenAI-shaped); see ADR-003.
	copyResponseHeaders(w.Header(), resp.Header)
	if s.router != nil {
		// The Content-Type now comes from a registered worker, not from the operator.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		noteAttempts(w.Header(), info)
	}
	isSSE := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	if isSSE {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
	}
	w.WriteHeader(resp.StatusCode)
	// A write deadline set below must not leak onto the next request that
	// reuses this connection.
	defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()

	src := &idleReader{r: body, timer: idle, d: s.cfg.UpstreamIdleTimeout}
	if isSSE {
		s.relayStream(r.Context(), rc, w, src, info, start)
		return
	}

	clientErr, err := s.pump(rc, w, src, false, nil)
	switch {
	case err == nil:
	case clientErr || r.Context().Err() != nil:
		info.clientClosed = true
	default:
		// Headers are sent; abort so the client sees a failed, not a
		// truncated-but-complete, response.
		info.errCode = api.CodeInferenceFailed
		s.log.Warn("upstream body failed", "request_id", info.id, "attempt_id", info.attemptID, "error", err)
		panic(http.ErrAbortHandler)
	}
}

// pump copies src to the client, bounding each client write by
// clientWriteTimeout so a client that stops reading cannot pin the
// connection and the upstream stream. If flush is set every read is flushed
// immediately. onData, if non-nil, sees each chunk before it is written.
// clientErr reports whether a failure came from the client side.
func (s *Server) pump(rc *http.ResponseController, w http.ResponseWriter, src io.Reader, flush bool, onData func([]byte)) (clientErr bool, err error) {
	buf := make([]byte, copyBufferSize)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if onData != nil {
				onData(buf[:n])
			}
			_ = rc.SetWriteDeadline(time.Now().Add(s.clientWriteTimeout))
			if _, werr := w.Write(buf[:n]); werr != nil {
				return true, werr
			}
			if flush {
				if ferr := rc.Flush(); ferr != nil {
					return true, ferr
				}
			}
		}
		if rerr == io.EOF {
			return false, nil
		}
		if rerr != nil {
			return false, rerr
		}
	}
}

// relayStream copies an SSE body to the client, flushing every read so no
// chunk is held back. TTFT is measured to the first streamed chunk. If the
// upstream fails after streaming began, the stream is ended with an SSE
// error event and no retry (spec section 25).
func (s *Server) relayStream(ctx context.Context, rc *http.ResponseController, w http.ResponseWriter, src io.Reader, info *reqInfo, start time.Time) {
	_ = rc.Flush() // send headers immediately

	var tail []byte // last bytes forwarded, to keep SSE event framing intact
	onData := func(b []byte) {
		if info.ttft == 0 {
			info.ttft = time.Since(start)
		}
		tail = append(tail, b...)
		if len(tail) > 4 {
			tail = tail[len(tail)-4:]
		}
	}
	clientErr, err := s.pump(rc, w, src, true, onData)
	switch {
	case err == nil:
		return
	case clientErr || ctx.Err() != nil:
		info.clientClosed = true
		return
	}

	info.errCode = api.CodeInferenceFailed
	s.log.Warn("upstream stream failed", "request_id", info.id, "attempt_id", info.attemptID, "error", err)
	var out bytes.Buffer
	if len(tail) > 0 && !bytes.HasSuffix(tail, []byte("\n\n")) && !bytes.HasSuffix(tail, []byte("\r\n\r\n")) {
		out.WriteString("\n\n") // close the event the upstream left half-written
	}
	out.WriteString("data: ")
	out.Write(api.ErrInferenceFailed().Body())
	out.WriteString("\n\n")
	_ = rc.SetWriteDeadline(time.Now().Add(s.clientWriteTimeout))
	_, _ = w.Write(out.Bytes())
	_ = rc.Flush()
}

// idleReader runs the idle clock only while a Read is waiting on the upstream.
// Time the gateway spends blocked writing to a slow client falls between
// reads, when the timer is stopped, so a slow but alive client is never
// mistaken for a stalled upstream.
type idleReader struct {
	r     io.Reader
	timer *time.Timer
	d     time.Duration
}

func (i *idleReader) Read(p []byte) (int, error) {
	i.timer.Reset(i.d)
	n, err := i.r.Read(p)
	i.timer.Stop()
	return n, err
}

func (s *Server) fail(w http.ResponseWriter, info *reqInfo, e *api.Error) {
	info.errCode = e.Code
	noteAttempts(w.Header(), info)
	api.WriteError(w, e)
}

// mapUpstreamError turns a transport error into a gateway API error.
func mapUpstreamError(err error) *api.Error {
	// An unreachable host is "no upstream", even when the dial timed out.
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.Is(err, syscall.ECONNREFUSED) || errors.As(err, &dnsErr) || (errors.As(err, &opErr) && opErr.Op == "dial") {
		return api.ErrWorkerUnavailable()
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return api.ErrUpstreamTimeout()
	}
	return api.ErrInferenceFailed()
}

// copyResponseHeaders relays only the response headers the client needs.
func copyResponseHeaders(dst, src http.Header) {
	for _, k := range []string{"Content-Type", "Cache-Control"} {
		if v := src.Get(k); v != "" {
			dst.Set(k, v)
		}
	}
}
