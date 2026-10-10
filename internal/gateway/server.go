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

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/ratelimit"
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
	// obs fans lifecycle events out to the observers: metrics first, then any WithObserver (ADR-016).
	obs     *multiObserver
	handler http.Handler
	// authn, when set, requires an API key on /v1 requests (auth.mode=required).
	authn *auth.Authenticator
	// authRequired is true once authentication was asked for, even if authn is (wrongly) nil.
	authRequired bool
	// limiter, when set, enforces quotas after the request is parsed (rate_limit.mode=required).
	limiter ratelimit.Limiter
	// limitRequired is true once rate limiting was asked for, even if limiter is (wrongly) nil.
	limitRequired bool
	// recorder, when set, receives request metadata (redis.request_metadata).
	recorder RequestRecorder
	// bodyReadTimeout bounds how long a client may take to send its request body.
	bodyReadTimeout time.Duration
	// clientWriteTimeout bounds each write to the client.
	clientWriteTimeout time.Duration
}

// New builds a Server that forwards to the upstream described by cfg.
func New(cfg config.GatewayConfig, log *slog.Logger, opts ...Option) *Server {
	s := newWithUpstream(cfg, log, newHTTPUpstream(cfg.UpstreamURL, cfg.ReadinessPath, cfg.UpstreamHeaderTimeout))
	for _, o := range opts {
		o(s)
	}
	return s
}

// NewFromConfig is New for a whole configuration: it honours auth.mode, so a configuration that
// requires API keys cannot produce an open server by omitting the WithAuthenticator option. Without
// an authenticator the server then refuses to Serve and answers every /v1 request with an error.
func NewFromConfig(cfg config.Config, log *slog.Logger, opts ...Option) *Server {
	opts = append([]Option{requireIfConfigured(cfg.Auth.Mode), requireLimitIfConfigured(cfg.RateLimit.Mode), WithMetricsConfig(cfg.Metrics)}, opts...)
	return New(cfg.Gateway, log, opts...)
}

// requireIfConfigured marks authentication as required when the mode says so. A later
// WithAuthenticator supplies the authenticator.
func requireIfConfigured(mode string) Option {
	return func(s *Server) {
		if mode == config.AuthModeRequired {
			s.authRequired = true
		}
	}
}

func newWithUpstream(cfg config.GatewayConfig, log *slog.Logger, up Upstream) *Server {
	s := &Server{
		cfg:                cfg,
		log:                log,
		upstream:           up,
		ready:              &readiness{upstream: up},
		metrics:            newMetrics(),
		obs:                newMultiObserver(log),
		bodyReadTimeout:    defaultBodyReadTimeout,
		clientWriteTimeout: defaultClientWriteTimeout,
	}
	s.obs.add(s.metrics)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /v1/models", s.authenticate(s.handleModels))
	mux.Handle("POST "+chatCompletionsPath, s.authenticate(s.handleChatCompletions))
	s.handler = s.withRequest(mux)
	return s
}

// Handler returns the gateway's HTTP handler. It does not serve /metrics: the Prometheus endpoint is on a
// separate listener (MetricsHandler, ADR-017), so the data port never exposes it.
func (s *Server) Handler() http.Handler { return s.handler }

// MetricsHandler returns the plain Prometheus handler for this server's registry, without any access control.
// Serve it only through internal/telemetry/metricsserver (loopback by default, a token otherwise).
func (s *Server) MetricsHandler() http.Handler { return s.metrics.handler() }

// MetricsGatherer is the server's private registry, for the metrics listener and for tests.
func (s *Server) MetricsGatherer() prometheus.Gatherer { return s.metrics.reg }

// WithCollector registers a collector on the server's private registry (Redis, PostgreSQL and key cache
// statistics, read at scrape time). A collector that cannot be registered (a duplicate or inconsistent
// descriptor) is logged and skipped: a monitoring defect must not stop the gateway.
func WithCollector(c prometheus.Collector) Option {
	return func(s *Server) {
		if c == nil {
			return
		}
		if err := s.metrics.reg.Register(c); err != nil {
			s.log.Error("metrics collector not registered", "component", "gateway", "error", err.Error())
		}
	}
}

// WithMetricsConfig applies the metrics section (label caps, tenant series). NewFromConfig and NewRegistry do
// this themselves; the option is for servers built from a GatewayConfig alone.
func WithMetricsConfig(c config.MetricsConfig) Option {
	return func(s *Server) { s.metrics.configure(c) }
}

// Serve serves on ln until ctx is cancelled, then shuts down gracefully:
// it stops accepting connections and lets in-flight requests (including
// streams) finish, up to the configured shutdown timeout, after which
// remaining connections are closed. It returns nil on a clean shutdown.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s.authRequired && s.authn == nil {
		return errors.New("gateway: authentication is required but no authenticator was provided")
	}
	if s.limitRequired && s.limiter == nil {
		return errors.New("gateway: rate limiting is required but no limiter was provided")
	}
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
	bctx, stopBackground := context.WithCancel(ctx)
	defer stopBackground()
	if s.background != nil {
		go s.background(bctx)
	}
	if s.authn != nil {
		go s.authn.Run(bctx) // records last_used_at; never on the request path
	}
	if r, ok := s.limiter.(interface{ Run(context.Context) }); ok {
		go r.Run(bctx) // lease renewal and release
	}
	if r, ok := s.recorder.(interface{ Run(context.Context) }); ok {
		go r.Run(bctx)
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
func NewRegistry(cfg config.Config, log *slog.Logger, opts ...Option) (*Server, error) {
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
	s, err := newRegistryServer(cfg.Gateway, cfg.Scheduler.Strategy, cfg.Worker.SuspectTimeout, cp, log)
	if err != nil {
		return nil, err
	}
	requireIfConfigured(cfg.Auth.Mode)(s)
	requireLimitIfConfigured(cfg.RateLimit.Mode)(s)
	WithMetricsConfig(cfg.Metrics)(s)
	for _, o := range opts {
		o(s)
	}
	return s, nil
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
	s.metrics.setStrategy(strategy)
	s.metrics.reg.MustRegister(newSnapshotCollector(cache))
	return s, nil
}
