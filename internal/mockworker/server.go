package mockworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/tracing"
	"serverflow/pkg/protocol"
)

const (
	maxRequestBytes = 1 << 20
	maxMaxTokens    = 1 << 20
	// midstreamTokens is how many tokens a midstream failure lets through
	// before the connection is aborted.
	midstreamTokens = 2
	drainPoll       = 20 * time.Millisecond
)

// shutdownGrace is the most time Serve spends, once no request is in flight,
// letting the last responses flush and closing connections. net/http treats a
// connection that never sent a request as busy for 5s, so waiting for them to
// close on their own would hold a clean exit that long. A package variable so
// tests can shorten it.
var shutdownGrace = 500 * time.Millisecond

type state int32

const (
	stateStarting state = iota
	stateReady
	stateDraining
)

func (s state) String() string {
	switch s {
	case stateStarting:
		return "starting"
	case stateReady:
		return "ready"
	default:
		return "draining"
	}
}

// Server is the mock worker's HTTP server.
type Server struct {
	cfg    Config
	log    *slog.Logger
	engine *Engine
	inj    *Injector
	state  atomic.Int32
	// inflight counts chat handlers from the moment they start, before the
	// readiness check, so a drain also waits for requests that were admitted
	// but are still sending their body or have not yet reached the queue.
	inflight atomic.Int64
	// sleep waits until a deadline or ctx ends. A field so tests can record the
	// deadlines generate asks for instead of waiting for them.
	sleep   func(ctx context.Context, until time.Time) error
	ready   *time.Timer
	handler http.Handler
	tracing *tracing.Provider // nil: tracing off
}

// Option customises a Server.
type Option func(*Server)

// WithTracing records inference and queue_wait spans, for requests the caller's traceparent marks sampled.
func WithTracing(p *tracing.Provider) Option { return func(s *Server) { s.tracing = p } }

// New builds a Server. cfg must already be valid. If cfg.StartupDelay is set
// the server reports "starting" (not ready) until it elapses.
func New(cfg Config, log *slog.Logger, opts ...Option) *Server {
	s := &Server{
		cfg:    cfg,
		log:    log,
		engine: NewEngine(cfg.MaxConcurrency, cfg.QueueSize),
		inj:    NewInjector(cfg.Seed, cfg.FailureRate, cfg.FailureMode),
		sleep:  sleepUntil,
	}
	if cfg.StartupDelay > 0 {
		s.state.Store(int32(stateStarting))
		s.ready = time.AfterFunc(cfg.StartupDelay, func() {
			s.state.CompareAndSwap(int32(stateStarting), int32(stateReady))
		})
	} else {
		s.state.Store(int32(stateReady))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	s.handler = mux
	for _, o := range opts {
		o(s)
	}
	return s
}

// Handler returns the worker's HTTP handler.
func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) currentState() state { return state(s.state.Load()) }

// Serve serves on ln until ctx is cancelled, then drains: it reports draining
// (readiness 503, new requests 503) while requests already running or queued
// finish, then shuts down. It returns nil on a clean drain, or an error if
// the drain timeout expires first (in-flight requests are then cut off).
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: s.handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	s.state.Store(int32(stateDraining))
	if s.ready != nil {
		s.ready.Stop()
	}
	s.log.Info("draining", "component", "mock-worker", "worker_id", s.cfg.EffectiveWorkerID(), "timeout", s.cfg.DrainTimeout.String())
	deadline := time.Now().Add(s.cfg.DrainTimeout)
	for s.inflight.Load() > 0 {
		if time.Now().After(deadline) {
			_ = srv.Close()
			return fmt.Errorf("drain timed out after %v with requests still in flight", s.cfg.DrainTimeout)
		}
		time.Sleep(drainPoll)
	}
	// No request is running. Give connections a moment to close, then close
	// what is left: those are idle or never sent a request. The grace comes out
	// of the drain budget, so --drain-timeout bounds the whole shutdown.
	grace := min(shutdownGrace, max(time.Until(deadline), 0))
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		_ = srv.Close()
		return err
	}
	_ = srv.Close()
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// --- simple endpoints ---------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, `{"status":"ok"}`)
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	st := s.currentState()
	code := http.StatusOK
	if st != stateReady {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, fmt.Sprintf(`{"status":%q}`, st.String()))
}

// handleModels doubles as the readiness signal for the gateway's default
// probe path, so it is 503 until the worker is ready, like a real server that
// is not yet listening.
func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	if st := s.currentState(); st != stateReady {
		writeAPIError(w, notReadyError(st))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(api.ModelsBody([]string{s.cfg.Model}))
}

// Stats is the worker metadata served at /stats (spec section 10, without GPU
// fields). The JSON shape is provisional; Phase 4 defines the heartbeat
// contract.
type Stats struct {
	WorkerID                  string  `json:"worker_id"`
	Model                     string  `json:"model"`
	Status                    string  `json:"status"`
	ActiveRequests            int     `json:"active_requests"`
	QueueDepth                int     `json:"queue_depth"`
	QueuedInputTokens         int     `json:"queued_input_tokens"`
	RecentTokensPerSecond     float64 `json:"recent_tokens_per_second"`
	Completed                 int64   `json:"completed"`
	Failed                    int64   `json:"failed"`
	Rejected                  int64   `json:"rejected"`
	Cancelled                 int64   `json:"cancelled"`
	TokensGenerated           int64   `json:"tokens_generated"`
	MaxConcurrency            int     `json:"max_concurrency"`
	QueueSize                 int     `json:"queue_size"`
	ConfiguredTTFTMillis      int64   `json:"configured_ttft_ms"`
	ConfiguredTokensPerSecond float64 `json:"configured_tokens_per_second"`
}

// Stats returns the current worker metadata.
func (s *Server) Stats() Stats {
	e := s.engine.Stats()
	return Stats{
		WorkerID:                  s.cfg.EffectiveWorkerID(),
		Model:                     s.cfg.Model,
		Status:                    s.currentState().String(),
		ActiveRequests:            e.Active,
		QueueDepth:                e.QueueDepth,
		QueuedInputTokens:         e.QueuedInputTokens,
		RecentTokensPerSecond:     e.RecentTokensPerSecond,
		Completed:                 e.Completed,
		Failed:                    e.Failed,
		Rejected:                  e.Rejected,
		Cancelled:                 e.Cancelled,
		TokensGenerated:           e.Tokens,
		MaxConcurrency:            s.cfg.MaxConcurrency,
		QueueSize:                 s.cfg.QueueSize,
		ConfiguredTTFTMillis:      s.cfg.TTFT.Milliseconds(),
		ConfiguredTokensPerSecond: s.cfg.TokensPerSecond,
	}
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	b, _ := json.Marshal(s.Stats())
	writeJSON(w, http.StatusOK, string(b))
}

// --- chat completions -----------------------------------------------------------

// Errors this worker produces itself. They use the OpenAI error shape.
func notReadyError(st state) *api.Error {
	code := "not_ready"
	if st == stateDraining {
		code = "draining"
	}
	return &api.Error{Code: code, HTTPStatus: http.StatusServiceUnavailable, Message: "the worker is " + st.String()}
}

func queueFullError() *api.Error {
	return &api.Error{Code: "queue_full", HTTPStatus: http.StatusServiceUnavailable, Message: "the worker queue is full"}
}

func injectedError(status int) *api.Error {
	return &api.Error{Code: "mock_failure", HTTPStatus: status, Message: "injected failure"}
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	// Count the request before checking readiness: once Serve sees zero it is
	// safe to stop, because any later arrival sees "draining" and is refused.
	s.inflight.Add(1)
	defer s.inflight.Add(-1)

	start := time.Now()
	rl := &chatLog{outcome: "rejected", requestID: r.Header.Get("X-Request-ID"), attemptID: r.Header.Get("X-Attempt-ID")}
	ts := s.beginTrace(r, rl, start)
	defer func() { ts.finish(rl); s.logChat(rl, start) }()

	if st := s.currentState(); st != stateReady {
		writeAPIError(w, notReadyError(st))
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeAPIError(w, api.ErrBodyTooLarge())
		} else {
			writeAPIError(w, api.ErrInvalidRequest("could not read request body"))
		}
		return
	}
	// The mock is a strict upstream: it parses exactly as the gateway does, so
	// a parser disagreement shows up as an error here.
	ireq, err := api.ParseChatRequest(body, api.Limits{Models: []string{s.cfg.Model}, MaxTokensLimit: maxMaxTokens})
	if err != nil {
		var apiErr *api.Error
		if !errors.As(err, &apiErr) {
			apiErr = api.ErrInternal()
		}
		writeAPIError(w, apiErr)
		return
	}
	rl.model, rl.stream = ireq.Model, ireq.Stream
	rl.promptTokens = promptTokens(ireq.Messages)

	// finish_reason is "length" whenever the client's max_tokens limit is what
	// ended the output, including when it equals the natural length.
	n, finish := s.cfg.OutputTokens, "stop"
	if ireq.MaxTokens > 0 && ireq.MaxTokens <= n {
		n, finish = ireq.MaxTokens, "length"
	}
	rl.outputTokens = n

	mode, fail := s.inj.Roll()
	if fail {
		rl.injected = string(mode)
		switch mode {
		case ModeError:
			s.engine.RecordOutcome(false)
			rl.outcome = "failed"
			writeAPIError(w, injectedError(http.StatusInternalServerError))
			return
		case ModeUnavailable:
			s.engine.RecordOutcome(false)
			rl.outcome = "failed"
			writeAPIError(w, injectedError(http.StatusServiceUnavailable))
			return
		case ModeDrop:
			s.engine.RecordOutcome(false)
			rl.outcome = "failed"
			panic(http.ErrAbortHandler) // close the connection without a response
		}
	}
	abortAt := -1 // token index at which a midstream failure strikes
	if fail && mode == ModeMidstream {
		abortAt = min(midstreamTokens, n-1)
	}

	queued := time.Now()
	ts.queueStart(queued)
	release, err := s.engine.Acquire(r.Context(), rl.promptTokens)
	rl.queueMillis = time.Since(queued).Milliseconds()
	ts.queueEnd()
	if err != nil {
		if errors.Is(err, ErrQueueFull) {
			writeAPIError(w, queueFullError())
			return
		}
		rl.outcome = "cancelled" // the client left while queued
		return
	}
	defer release()

	id := "chatcmpl-" + strings.TrimPrefix(protocol.NewRequestID(), "req_")
	created := time.Now()
	var genErr error
	if ireq.Stream {
		genErr = s.streamResponse(w, r.Context(), rl, id, created, n, finish, abortAt)
	} else {
		genErr = s.fullResponse(w, r.Context(), rl, id, created, n, finish, abortAt)
	}
	switch {
	case genErr == nil:
		s.engine.RecordOutcome(true)
		rl.outcome = "ok"
	case errors.Is(genErr, errMidstream):
		s.engine.RecordOutcome(false)
		rl.outcome = "failed"
		panic(http.ErrAbortHandler)
	default:
		s.engine.RecordCancelled()
		rl.outcome = "cancelled"
	}
}

var errMidstream = errors.New("mockworker: injected midstream failure")

// generate paces n tokens against absolute deadlines (first token at TTFT,
// then one every 1/tokens-per-second), so sleep jitter does not accumulate
// into drift. emit is called as each token becomes available.
func (s *Server) generate(ctx context.Context, n, abortAt int, emit func(i int) error) error {
	interval := time.Duration(float64(time.Second) / s.cfg.TokensPerSecond)
	first := time.Now().Add(s.cfg.TTFT)
	for i := 0; i < n; i++ {
		if err := s.sleep(ctx, first.Add(time.Duration(i)*interval)); err != nil {
			return err
		}
		if i == abortAt {
			return errMidstream
		}
		s.engine.RecordTokens(1)
		if err := emit(i); err != nil {
			return err
		}
	}
	return nil
}

func tokenText(i int) string { return fmt.Sprintf("tok%d ", i) }

func (s *Server) streamResponse(w http.ResponseWriter, ctx context.Context, rl *chatLog, id string, created time.Time, n int, finish string, abortAt int) error {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return err
	}
	send := func(c api.ChatCompletion) error {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", c.JSON()); err != nil {
			return err
		}
		return rc.Flush()
	}
	started := time.Now()
	err := s.generate(ctx, n, abortAt, func(i int) error {
		if i == 0 {
			rl.ttftMillis = time.Since(started).Milliseconds()
			rl.firstTokenAt = time.Now()
			if err := send(api.NewChunk(id, s.cfg.Model, created, api.Delta{Role: "assistant", Content: api.Str("")}, "")); err != nil {
				return err
			}
		}
		return send(api.NewChunk(id, s.cfg.Model, created, api.Delta{Content: api.Str(tokenText(i))}, ""))
	})
	if err != nil {
		return err
	}
	if err := send(api.NewChunk(id, s.cfg.Model, created, api.Delta{}, finish)); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	return rc.Flush()
}

func (s *Server) fullResponse(w http.ResponseWriter, ctx context.Context, rl *chatLog, id string, created time.Time, n int, finish string, abortAt int) error {
	var content strings.Builder
	for i := 0; i < n; i++ {
		content.WriteString(tokenText(i))
	}
	body := api.NewCompletion(id, s.cfg.Model, created, content.String(), finish,
		api.Usage{PromptTokens: rl.promptTokens, CompletionTokens: n, TotalTokens: rl.promptTokens + n}).JSON()

	err := s.generate(ctx, n, abortAt, func(int) error { return nil })
	if errors.Is(err, errMidstream) {
		// Promise the whole body, deliver half, then drop the connection.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write(body[:len(body)/2])
		_ = http.NewResponseController(w).Flush()
		return errMidstream
	}
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(body)
	return err
}

// promptTokens estimates input tokens as whitespace-separated words, at least 1.
func promptTokens(msgs []protocol.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(strings.Fields(m.Content))
	}
	return max(n, 1)
}

// sleepUntil waits until t, or returns ctx.Err() if ctx ends first.
func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// --- logging and helpers ----------------------------------------------------------

type chatLog struct {
	requestID    string
	attemptID    string
	outcome      string
	injected     string
	model        string
	stream       bool
	promptTokens int
	outputTokens int
	queueMillis  int64
	ttftMillis   int64
	firstTokenAt time.Time
	traceID      string // set when the request carried a valid trace context
}

func (s *Server) logChat(rl *chatLog, start time.Time) {
	attrs := []any{
		"component", "mock-worker", "worker_id", s.cfg.EffectiveWorkerID(), "outcome", rl.outcome, "model", rl.model, "stream", rl.stream,
		"prompt_tokens", rl.promptTokens, "output_tokens", rl.outputTokens,
		"queue_ms", rl.queueMillis, "duration_ms", time.Since(start).Milliseconds(),
	}
	if rl.stream && rl.ttftMillis > 0 {
		attrs = append(attrs, "ttft_ms", rl.ttftMillis)
	}
	if rl.injected != "" {
		attrs = append(attrs, "injected_failure", rl.injected)
	}
	if rl.traceID != "" {
		attrs = append(attrs, "trace_id", rl.traceID)
	}
	if rl.requestID != "" {
		attrs = append(attrs, "request_id", rl.requestID) // the gateway's ID, for correlation
		if rl.attemptID != "" {
			attrs = append(attrs, "attempt_id", rl.attemptID)
		}
	}
	s.log.Info("chat", attrs...)
}

func writeJSON(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

func writeAPIError(w http.ResponseWriter, e *api.Error) { api.WriteError(w, e) }
