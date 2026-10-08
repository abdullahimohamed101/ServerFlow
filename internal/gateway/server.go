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
	"net/url"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/registry/client"
	"serverflow/internal/scheduler"
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
	// router is set in registry mode; it replaces upstream for chat requests.
	router *router
	// retry decides which failed attempts may move to another worker (registry mode).
	retry retryPolicy
	// background, when set, runs for the life of Serve (the snapshot refresher).
	background func(ctx context.Context)
	ready      *readiness
	metrics    *metrics
	handler    http.Handler
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
	if s.router != nil {
		s.router.policy.SetSelf(ln.Addr()) // a worker must not be able to loop requests back through us
	}
	if s.background != nil {
		bctx, stopBackground := context.WithCancel(ctx)
		defer stopBackground()
		go s.background(bctx)
	}

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

// NewRegistry builds a Server in registry mode: requests go to the worker the
// configured scheduler picks from the control plane's registry, which the
// server polls in the background while it serves. It fails when the scheduler
// strategy or worker networks are unusable.
func NewRegistry(cfg config.Config, log *slog.Logger) (*Server, error) {
	if len(cfg.Gateway.WorkerNetworks) == 0 {
		// Registration is the trust boundary: any worker that may register can name an
		// internal address (loopback and private ranges are allowed by default).
		level := slog.LevelInfo
		if u, err := url.Parse(cfg.Gateway.ControlPlaneURL); err == nil && !config.IsLoopbackHost(u.Hostname()) {
			level = slog.LevelWarn
		}
		log.Log(context.Background(), level, "gateway.worker_networks is empty: workers may name any loopback or private address; set it to the networks your workers live on",
			"component", "gateway")
	}
	cp := client.New(cfg.Gateway.ControlPlaneURL, cfg.ControlPlane.Token, nil)
	return newRegistryServer(cfg.Gateway, cfg.Scheduler.Strategy, cfg.Worker.SuspectTimeout, cp, log)
}

func newRegistryServer(g config.GatewayConfig, strategy string, suspectAfter time.Duration, src workerLister, log *slog.Logger) (*Server, error) {
	sched, err := scheduler.New(strategy)
	if err != nil {
		return nil, err
	}
	policy, err := NewAddressPolicy(g.WorkerNetworks)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Transport: newWorkerTransport(policy, g.UpstreamHeaderTimeout),
		// No Client.Timeout (it would cut streams), and redirects are never
		// followed: they would replay the prompt to a host nobody chose.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	cache := newSnapshotCache(src, g.RegistryRefresh, g.RegistryMaxStaleness, suspectAfter, log.With("component", "gateway"))
	rt := newRouter(cache, policy, sched, strategy, httpClient, log)
	s := newWithUpstream(g, log, newHTTPUpstream(g.UpstreamURL, g.ReadinessPath, g.UpstreamHeaderTimeout))
	s.router = rt
	s.retry = newRetryPolicy(g.MaxAttempts, g.RetryStatuses)
	s.ready = &readiness{upstream: rt}
	s.background = cache.Run
	return s, nil
}
