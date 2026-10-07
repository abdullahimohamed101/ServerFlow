// Package server is the control plane's HTTP API over the worker registry.
// Every /v1 endpoint requires the shared-secret bearer token when one is
// configured; /healthz and /readyz are always open.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/registry"
	"serverflow/pkg/protocol"
)

// Config configures the server.
type Config struct {
	// Token is the shared secret; empty disables authentication (loopback only,
	// enforced by the caller).
	Token string
	// MaxBodyBytes bounds request bodies.
	MaxBodyBytes int64
	// SweepInterval is how often the registry sweeps for dead workers.
	SweepInterval time.Duration
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration
	// ReadTimeout bounds reading a whole request, body included.
	ReadTimeout time.Duration
	// WriteTimeout bounds writing a response.
	WriteTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 64 << 10
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = time.Second
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 15 * time.Second
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 10 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 20 * time.Second
	}
	return c
}

// Server serves the registry API.
type Server struct {
	reg       *registry.Registry
	cfg       Config
	log       *slog.Logger
	tokenHash [sha256.Size]byte
	handler   http.Handler
	authLog   rateLimit
}

// rateLimit lets one log line through per interval and counts what it held back.
type rateLimit struct {
	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func (l *rateLimit) allow(now time.Time) (ok bool, suppressed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < time.Second {
		l.suppressed++
		return false, 0
	}
	l.last = now
	suppressed, l.suppressed = l.suppressed, 0
	return true, suppressed
}

// New builds a Server.
func New(reg *registry.Registry, cfg Config, log *slog.Logger) *Server {
	cfg = cfg.withDefaults()
	s := &Server{reg: reg, cfg: cfg, log: log.With("component", "control-plane"), tokenHash: sha256.Sum256([]byte(cfg.Token))}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleHealth)
	mux.Handle("POST /v1/workers/register", s.protect(http.HandlerFunc(s.handleRegister)))
	mux.Handle("POST /v1/workers/{id}/heartbeat", s.protect(http.HandlerFunc(s.handleHeartbeat)))
	mux.Handle("DELETE /v1/workers/{id}", s.protect(http.HandlerFunc(s.handleDeregister)))
	mux.Handle("GET /v1/workers", s.protect(http.HandlerFunc(s.handleList)))
	mux.Handle("GET /v1/workers/{id}", s.protect(http.HandlerFunc(s.handleGet)))
	mux.Handle("GET /v1/models", s.protect(http.HandlerFunc(s.handleModels)))
	s.handler = mux
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.handler }

// Serve serves on ln and sweeps the registry until ctx is cancelled, then shuts
// down gracefully. It returns nil on a clean shutdown.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	// Defense in depth: the caller is expected to enforce this too (see
	// config.ControlPlaneConfig.ValidateServe), but an open registry on a
	// reachable address would let anyone steer traffic, so refuse it here.
	if s.cfg.Token == "" && !loopbackAddr(ln.Addr()) {
		return errors.New("control plane: refusing to serve without a token on a non-loopback address")
	}
	// No streaming here, so whole-request timeouts are safe, and they stop a
	// client from holding a connection by trickling a body.
	srv := &http.Server{
		Handler: s.handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: s.cfg.ReadTimeout,
		WriteTimeout: s.cfg.WriteTimeout, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	sweepCtx, stopSweep := context.WithCancel(ctx)
	defer stopSweep()
	go func() {
		t := time.NewTicker(s.cfg.SweepInterval)
		defer t.Stop()
		for {
			select {
			case <-sweepCtx.Done():
				return
			case <-t.C:
				s.reg.Sweep()
			}
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
		return err
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// --- plumbing -----------------------------------------------------------------------

func fail(w http.ResponseWriter, status int, code, msg string) {
	api.WriteError(w, &api.Error{Code: code, HTTPStatus: status, Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// protect wraps every /v1 route. It refuses browser-originated requests (a
// web page can reach a loopback service, and the registry decides where
// prompts go), then, with no token configured, requires a loopback Host so a
// DNS-rebinding page cannot talk to it, then enforces the bearer token.
func (s *Server) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Browsers attach Origin to cross-origin requests; agents and the
		// gateway never do.
		if len(r.Header.Values("Origin")) > 0 {
			fail(w, http.StatusForbidden, "forbidden", "browser-originated requests are not accepted")
			return
		}
		if s.cfg.Token == "" && !loopbackHost(r.Host) {
			fail(w, http.StatusMisdirectedRequest, "misdirected_request", "without a token this server only answers requests addressed to a loopback host")
			return
		}
		if s.cfg.Token != "" && !s.authorized(r) {
			if ok, held := s.authLog.allow(time.Now()); ok {
				s.log.Warn("request without a valid token", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr, "suppressed", held)
			}
			w.Header().Set("WWW-Authenticate", "Bearer")
			fail(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authorized checks the bearer token. The scheme is case-insensitive (RFC
// 7235); the comparison is constant time and independent of the token's
// length because it compares SHA-256 digests.
func (s *Server) authorized(r *http.Request) bool {
	scheme, cred, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	sum := sha256.Sum256([]byte(cred))
	return ok && strings.EqualFold(scheme, "Bearer") && subtle.ConstantTimeCompare(sum[:], s.tokenHash[:]) == 1
}

// loopbackHost reports whether a Host header names this machine: localhost or
// a loopback IP, with or without a port.
func loopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func loopbackAddr(a net.Addr) bool {
	if t, ok := a.(*net.TCPAddr); ok {
		return t.IP.IsLoopback()
	}
	return false
}

// decode reads a strict JSON body: it must be declared application/json (a
// cross-site form or text/plain POST needs no preflight, JSON does), bounded,
// with no unknown fields and no trailing data.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		fail(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "the request body must be application/json")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body is too large")
		} else {
			fail(w, http.StatusBadRequest, "invalid_request", "body is not valid JSON for this endpoint")
		}
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		fail(w, http.StatusBadRequest, "invalid_request", "body has data after the JSON value")
		return false
	}
	return true
}

// registryError maps a registry error to an HTTP response.
func registryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrInvalid):
		fail(w, http.StatusBadRequest, "invalid_request", strings.TrimPrefix(err.Error(), registry.ErrInvalid.Error()+": "))
	case errors.Is(err, registry.ErrUnknownWorker):
		fail(w, http.StatusNotFound, "unknown_worker", "no such worker is registered; register again")
	case errors.Is(err, registry.ErrStaleRegistration):
		fail(w, http.StatusConflict, "stale_registration", "this registration was superseded; register again")
	case errors.Is(err, registry.ErrIllegalTransition):
		fail(w, http.StatusConflict, "illegal_transition", err.Error())
	case errors.Is(err, registry.ErrFull):
		fail(w, http.StatusServiceUnavailable, "registry_full", "the registry is at its worker limit")
	default:
		fail(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func workerID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !protocol.ValidWorkerID(id) {
		fail(w, http.StatusBadRequest, "invalid_request", "invalid worker id")
		return "", false
	}
	return id, true
}

// --- handlers -------------------------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var info protocol.WorkerInfo
	if !s.decode(w, r, &info) {
		return
	}
	regID, err := s.reg.Register(info)
	if err != nil {
		registryError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, protocol.RegisterResponse{
		RegistrationID: regID, HeartbeatIntervalSeconds: s.reg.HeartbeatInterval().Seconds(),
	})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id, ok := workerID(w, r)
	if !ok {
		return
	}
	var hb protocol.Heartbeat
	if !s.decode(w, r, &hb) {
		return
	}
	if err := s.reg.Heartbeat(id, hb.RegistrationID, hb); err != nil {
		registryError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	id, ok := workerID(w, r)
	if !ok {
		return
	}
	regID := r.Header.Get("X-Registration-ID")
	if regID == "" {
		fail(w, http.StatusBadRequest, "invalid_request", "the X-Registration-ID header is required")
		return
	}
	if err := s.reg.Deregister(id, regID); err != nil {
		registryError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := registry.ListFilter{Model: q.Get("model")}
	if st := q.Get("state"); st != "" {
		state := protocol.WorkerState(st)
		if !state.Reportable() && state != protocol.StateUnhealthy && state != protocol.StateLost {
			fail(w, http.StatusBadRequest, "invalid_request", "unknown state filter")
			return
		}
		f.State = state
	}
	if e := q.Get("eligible"); e != "" {
		b, err := strconv.ParseBool(e)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_request", "eligible must be true or false")
			return
		}
		f.EligibleOnly = b
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": s.reg.List(f)})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, ok := workerID(w, r)
	if !ok {
		return
	}
	snap, found := s.reg.Get(id)
	if !found {
		fail(w, http.StatusNotFound, "unknown_worker", "no such worker")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"models": s.reg.Models()})
}
