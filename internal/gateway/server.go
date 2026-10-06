// Package gateway implements the ServerFlow API gateway: an OpenAI-compatible
// HTTP front end that validates requests, assigns request IDs, relays
// streaming responses from the inference upstream, and exposes health and
// Prometheus endpoints.
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"serverflow/internal/config"
)

// defaultBodyReadTimeout bounds reading a request body (slow-client defense).
// It is applied per request rather than via http.Server.ReadTimeout, which
// would also affect long streaming responses.
const defaultBodyReadTimeout = 30 * time.Second

// defaultClientWriteTimeout bounds each write to the client, so a client that
// stops reading cannot pin a connection and an upstream stream forever.
const defaultClientWriteTimeout = 30 * time.Second

// Server is the gateway HTTP server.
type Server struct {
	cfg      config.GatewayConfig
	log      *slog.Logger
	upstream Upstream
	ready    *readiness
	metrics  *metrics
	handler  http.Handler
	// bodyReadTimeout bounds how long a client may take to send its request body.
	bodyReadTimeout time.Duration
	// clientWriteTimeout bounds each write to the client.
	clientWriteTimeout time.Duration
}

// New builds a Server that forwards to the upstream described by cfg.
func New(cfg config.GatewayConfig, log *slog.Logger) *Server {
	return newWithUpstream(cfg, log, newHTTPUpstream(cfg.UpstreamURL, cfg.ReadinessPath, cfg.UpstreamHeaderTimeout))
}

func newWithUpstream(cfg config.GatewayConfig, log *slog.Logger, up Upstream) *Server {
	s := &Server{
		cfg:                cfg,
		log:                log,
		upstream:           up,
		ready:              &readiness{upstream: up},
		metrics:            newMetrics(),
		bodyReadTimeout:    defaultBodyReadTimeout,
		clientWriteTimeout: defaultClientWriteTimeout,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", s.metrics.handler())
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("POST "+chatCompletionsPath, s.handleChatCompletions)
	s.handler = s.withRequest(mux)
	return s
}

// Handler returns the gateway's HTTP handler.
func (s *Server) Handler() http.Handler { return s.handler }

// Serve serves on ln until ctx is cancelled, then shuts down gracefully:
// it stops accepting connections and lets in-flight requests (including
// streams) finish, up to the configured shutdown timeout, after which
// remaining connections are closed. It returns nil on a clean shutdown.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: it would cut off long streaming responses.
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	s.log.Info("gateway shutting down", "component", "gateway", "timeout", s.cfg.ShutdownTimeout.String())
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
