package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// The agent's status endpoint (Phase 17, ADR-022 D6). It exists so that something outside the process can tell a
// working agent from a hung one: the Docker healthcheck and, in Kubernetes, exec probes running the healthcheck
// binary. It is off unless worker.status_listen is set, listens on a loopback address only, serves two paths and
// returns fixed one-line texts: no worker ID, address, token, registration ID or registry data.
//
//	GET /healthz  200 while the heartbeat loop is running on schedule (or the agent is shutting down). It does not
//	              look at the control plane or the backend, so an outage of either never makes an orchestrator
//	              restart every agent. 503 only when the loop has stopped waking up.
//	GET /readyz   200 only when the agent holds a registration, the control plane accepted a heartbeat within
//	              ReadyTimeout, and the backend answered in the last step. 503 otherwise, always while draining.

// livenessIntervals is how many heartbeat intervals past its schedule the loop may be before /healthz fails. The
// schedule is the delay Step asked for (a failing step backs off up to ten intervals), plus this much; a step can
// itself take up to one and a half intervals (three calls of half an interval each).
const livenessIntervals = 3

// Reasons /readyz and /healthz give for a 503. They are fixed strings, never built from data.
const (
	reasonStalled      = "heartbeat loop stalled"
	reasonDraining     = "draining"
	reasonSuperseded   = "superseded by another process"
	reasonUnregistered = "not registered"
	reasonBackendDown  = "backend unreachable"
	reasonStaleBeat    = "no recent accepted heartbeat"
)

// statusState is what Step and Shutdown publish for the endpoint's goroutines.
type statusState struct {
	mu           sync.Mutex
	aliveUntil   time.Time
	lastAccepted time.Time
	registered   bool
	superseded   bool
	backendOK    bool
	draining     bool
}

// publish records the outcome of a step (or of Shutdown); next is the delay until the loop will wake again.
func (a *Agent) publish(next time.Duration) {
	now := a.now()
	a.st.mu.Lock()
	defer a.st.mu.Unlock()
	a.st.aliveUntil = now.Add(max(livenessIntervals*a.interval, next+2*a.interval))
	a.st.lastAccepted, a.st.registered, a.st.superseded, a.st.backendOK = a.lastAccepted, a.registered, a.superseded, a.backendOK
}

// markDraining makes /readyz fail from now on and stops /healthz from judging the loop (which has ended on purpose).
func (a *Agent) markDraining() {
	a.st.mu.Lock()
	a.st.draining = true
	a.st.mu.Unlock()
}

// Status is a snapshot for the status endpoint. Reason is empty when both Live and Ready are true; it explains
// the first failing condition otherwise.
type Status struct {
	Live        bool
	LiveReason  string
	Ready       bool
	ReadyReason string
}

// Status reports liveness and readiness. It is safe to call from any goroutine.
func (a *Agent) Status() Status {
	now := a.now()
	readyTimeout := a.cfg.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = 5 * a.cfg.Interval
	}
	a.st.mu.Lock()
	defer a.st.mu.Unlock()
	s := Status{Live: true, Ready: true}
	if !a.st.draining && now.After(a.st.aliveUntil) {
		s.Live, s.LiveReason = false, reasonStalled
	}
	switch {
	case a.st.draining:
		s.Ready, s.ReadyReason = false, reasonDraining
	case !s.Live:
		s.Ready, s.ReadyReason = false, reasonStalled
	case a.st.superseded:
		s.Ready, s.ReadyReason = false, reasonSuperseded
	case !a.st.registered:
		s.Ready, s.ReadyReason = false, reasonUnregistered
	case !a.st.backendOK:
		s.Ready, s.ReadyReason = false, reasonBackendDown
	case now.Sub(a.st.lastAccepted) > readyTimeout:
		s.Ready, s.ReadyReason = false, reasonStaleBeat
	}
	return s
}

// StatusHandler serves /healthz and /readyz for a. Only GET and HEAD are accepted.
func StatusHandler(a *Agent) http.Handler {
	mux := http.NewServeMux()
	reply := func(ok bool, reason string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			if ok {
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintln(w, "ok")
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintln(w, reason)
		}
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		s := a.Status()
		reply(s.Live, s.LiveReason)(w, r)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		s := a.Status()
		reply(s.Ready, s.ReadyReason)(w, r)
	})
	return mux
}

// StatusServer is the running endpoint.
type StatusServer struct {
	srv  *http.Server
	addr string
}

// ErrNotLoopback is returned by StartStatusServer when the listener ended up on an address that is not loopback
// (a name that resolves elsewhere): the listener is closed and nothing is served.
var ErrNotLoopback = errors.New("the status listener is not on a loopback address")

// StartStatusServer binds addr, checks that the bound address really is loopback and serves a's status on it in
// the background. The configuration validator has already checked the spelling; this checks the outcome.
func StartStatusServer(addr string, a *Agent, log *slog.Logger) (*StatusServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("worker.status_listen: %w", err)
	}
	if tcp, ok := ln.Addr().(*net.TCPAddr); !ok || !tcp.IP.IsLoopback() {
		_ = ln.Close()
		return nil, ErrNotLoopback
	}
	srv := &http.Server{
		Handler:           StatusHandler(a),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    4 << 10,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("worker status endpoint stopped", "component", "worker-agent", "error", err)
		}
	}()
	return &StatusServer{srv: srv, addr: ln.Addr().String()}, nil
}

// Addr is the address the endpoint is listening on.
func (s *StatusServer) Addr() string { return s.addr }

// Shutdown stops the endpoint within ctx: it waits for requests in flight until ctx ends, then closes whatever is
// left, so it never delays the process beyond ctx.
func (s *StatusServer) Shutdown(ctx context.Context) {
	if err := s.srv.Shutdown(ctx); err != nil {
		_ = s.srv.Close()
	}
}
