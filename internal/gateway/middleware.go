package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"serverflow/internal/api"
	"serverflow/pkg/protocol"
)

// statusClientClosed is the conventional (nginx) status recorded in logs and
// metrics when the client disconnects before a response completes.
const statusClientClosed = 499

// reqInfo is per-request state shared between the middleware and handlers.
type reqInfo struct {
	id        string
	attemptID string
	model     string // set only after the model is validated
	stream    bool
	inference bool
	ttft      time.Duration
	errCode   string
	// clientClosed is set when the client disconnected before the response
	// finished; the request is then logged and counted as 499.
	clientClosed bool
	// attempts is the ordered history of tries (registry mode). It is only appended to,
	// never rewritten: a retry is a new attempt with its own ID (spec section 7).
	attempts []attemptRecord
}

type ctxKey struct{}

func infoFrom(ctx context.Context) *reqInfo {
	info, _ := ctx.Value(ctxKey{}).(*reqInfo)
	if info == nil {
		return &reqInfo{}
	}
	return info
}

// statusRecorder captures the response status and supports flushing via
// http.ResponseController through Unwrap.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// withRequest assigns the request ID, logs one structured line per request,
// records inference metrics, and converts handler panics into a 500 (or an
// aborted connection if the response already started).
func (s *Server) withRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		info := &reqInfo{id: protocol.NewRequestID()}
		w.Header().Set(protocol.HeaderRequestID, info.id)
		rec := &statusRecorder{ResponseWriter: w}
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, info))

		var abort any
		func() {
			defer func() {
				if p := recover(); p != nil {
					abort = p
				}
			}()
			next.ServeHTTP(rec, r)
		}()

		status := rec.status
		switch {
		case abort != nil && abort != http.ErrAbortHandler:
			info.errCode = api.CodeInternalError
			if status == 0 {
				api.WriteError(rec, api.ErrInternal())
				status = http.StatusInternalServerError
			}
			s.log.Error("handler panic", "request_id", info.id, "panic", abort)
		case abort != nil:
			// The connection is dropped after the response began; whatever
			// status was written is not what the client experienced.
			status = http.StatusBadGateway
		case info.clientClosed:
			status = statusClientClosed
		case status == 0:
			status = http.StatusOK
		}

		d := time.Since(start)
		if info.inference {
			s.metrics.observe(info, status, d)
		}
		s.logRequest(r, info, status, d)

		if abort != nil && abort == http.ErrAbortHandler {
			panic(abort) // let net/http close the connection without a log
		}
	})
}

func (s *Server) logRequest(r *http.Request, info *reqInfo, status int, d time.Duration) {
	attrs := []any{
		"request_id", info.id,
		"method", r.Method,
		"path", r.URL.Path,
		"status", status,
		"duration_ms", d.Milliseconds(),
	}
	if info.inference {
		attrs = append(attrs, "attempt_id", info.attemptID, "model", info.model, "stream", info.stream)
		if info.ttft > 0 {
			attrs = append(attrs, "ttft_ms", info.ttft.Milliseconds())
		}
		if n := len(info.attempts); n > 0 {
			attrs = append(attrs, "attempts", n, "worker_id", info.attempts[n-1].Worker)
		}
	}
	if info.errCode != "" {
		attrs = append(attrs, "error_code", info.errCode)
	}
	level := slog.LevelInfo
	if status >= 500 || status == statusClientClosed {
		level = slog.LevelWarn
	}
	s.log.Log(r.Context(), level, "request", attrs...)
}
