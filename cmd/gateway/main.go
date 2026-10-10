// Command gateway is the ServerFlow API gateway entry point. It serves the
// OpenAI-compatible API in front of the configured inference upstream and
// shuts down gracefully on SIGINT/SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/events"
	"serverflow/internal/gateway"
	"serverflow/internal/kafka"
	"serverflow/internal/postgres"
	"serverflow/internal/ratelimit"
	"serverflow/internal/redis"
	"serverflow/internal/telemetry"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// With auth.mode=required the gateway must be able to verify keys, so a database that is
	// unreachable or not migrated is a startup failure, not a surprise at the first request.
	var opts []gateway.Option
	if cfg.Auth.Mode == config.AuthModeRequired {
		store, err := openAuthStore(ctx, cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()
		authn := auth.New(store, auth.Config{
			CacheTTL: cfg.Auth.CacheTTL, NegativeTTL: cfg.Auth.NegativeTTL, CacheSize: cfg.Auth.CacheSize,
			StaleGrace: cfg.Auth.StaleGrace, Logger: logger,
			// Lookups may use all but two pool connections, so the pool is never entirely theirs.
			MaxLookups: lookupCap(cfg.Postgres.MaxConns),
		})
		opts = append(opts, gateway.WithAuthenticator(authn))
		logger.Info("api key authentication required", "component", "gateway", "cache_ttl", cfg.Auth.CacheTTL.String(),
			"negative_ttl", cfg.Auth.NegativeTTL.String(), "cache_size", cfg.Auth.CacheSize, "stale_grace", cfg.Auth.StaleGrace.String())
	}

	// Rate limiting (and optional request metadata) need Redis. With rate_limit.mode=required an
	// unreachable or unauthenticated Redis is a startup failure, like the database above.
	rc, err := openRedis(ctx, cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
	if rc != nil {
		defer func() { _ = rc.Close() }()
	}
	if cfg.RateLimit.Mode == config.RateLimitRequired {
		lim, err := ratelimit.NewRedis(rc, ratelimit.Config{
			OnFailure: ratelimit.FailureMode(cfg.Redis.OnFailure), BurstSeconds: cfg.RateLimit.BurstSeconds, LeaseTTL: cfg.RateLimit.LeaseTTL,
			MaxLocalLeases: cfg.RateLimit.MaxLocalLeases, ModelRequestsPerMinute: cfg.RateLimit.ModelRequestsPerMinute, Logger: logger,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			os.Exit(1)
		}
		defer lim.Close(5 * time.Second) // send the releases still queued
		opts = append(opts, gateway.WithLimiter(lim))
		logger.Info("rate limiting required", "component", "gateway", "on_failure", cfg.Redis.OnFailure, "burst_seconds", cfg.RateLimit.BurstSeconds,
			"lease_ttl", cfg.RateLimit.LeaseTTL.String(), "model_caps", len(cfg.RateLimit.ModelRequestsPerMinute), "tenant_quotas", cfg.Auth.Mode == config.AuthModeRequired)
		if cfg.Auth.Mode != config.AuthModeRequired {
			logger.Warn("auth.mode is off: tenant quotas cannot be enforced without identities; only per-model caps apply", "component", "gateway")
		}
	} else if len(cfg.RateLimit.ModelRequestsPerMinute) > 0 {
		logger.Warn("rate_limit.model_requests_per_minute is set but rate_limit.mode is off: no limit is enforced", "component", "gateway")
	}
	if cfg.Redis.RequestMetadata {
		opts = append(opts, gateway.WithRequestRecorder(redis.NewRecorder(rc, cfg.Redis.RequestMetadataTTL)))
	}

	// Lifecycle events (Phase 12) are best-effort: a broker that is down never stops the gateway or a request.
	evObs, evSink, err := openEvents(cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
	if evObs != nil {
		opts = append(opts, gateway.WithObserver(evObs), gateway.WithExtraCollectors(evObs.Collectors()...))
	}

	if cfg.Auth.Mode == config.AuthModeOff {
		logger.Info("api key authentication is OFF: /v1 is open to any caller that can reach this port", "component", "gateway", "auth_mode", cfg.Auth.Mode)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Gateway.Port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	var srv *gateway.Server
	if cfg.Gateway.WorkerSource == config.WorkerSourceRegistry {
		if srv, err = gateway.NewRegistry(cfg, logger, opts...); err != nil {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			os.Exit(1)
		}
		// The token itself is never logged.
		logger.Info("gateway starting", "component", "gateway", "port", cfg.Gateway.Port, "worker_source", "registry",
			"strategy", cfg.Scheduler.Strategy, "refresh", cfg.Gateway.RegistryRefresh.String(),
			"max_staleness", cfg.Gateway.RegistryMaxStaleness.String(),
			"control_plane_token", cfg.ControlPlane.Token != "", "api_key_auth", cfg.Auth.Mode)
	} else {
		srv = gateway.NewFromConfig(cfg, logger, opts...)
		logger.Info("gateway starting", "component", "gateway", "port", cfg.Gateway.Port,
			"upstream", cfg.Gateway.UpstreamURL, "models", cfg.Gateway.Models, "api_key_auth", cfg.Auth.Mode)
	}

	if err := checkAuthWiring(cfg.Auth.Mode, srv.AuthRequired()); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	if (cfg.RateLimit.Mode == config.RateLimitRequired) != srv.RateLimitRequired() {
		fmt.Fprintf(os.Stderr, "gateway: rate limiting wiring does not match rate_limit.mode %q; refusing to start\n", cfg.RateLimit.Mode)
		os.Exit(1)
	}

	serveErr := srv.Serve(ctx, ln)
	// The HTTP server has drained, so every in-flight request has emitted its terminal event: flush them now,
	// bounded by events.shutdown_flush_timeout. Whatever is not delivered in time is counted and logged.
	if evObs != nil {
		fctx, cancel := context.WithTimeout(context.Background(), cfg.Events.ShutdownFlushTimeout)
		if err := evObs.Close(fctx); err != nil {
			logger.Warn("event flush did not finish", "component", "gateway", "error", err.Error())
		}
		cancel()
		_ = evSink.Close()
	}
	if serveErr != nil {
		logger.Error("gateway stopped with error", "component", "gateway", "error", serveErr)
		os.Exit(1)
	}
	logger.Info("gateway stopped", "component", "gateway")
}

// openAuthStore connects to PostgreSQL for key lookups and checks that the schema is current.
// Errors never contain the DSN's password.
func openAuthStore(ctx context.Context, cfg config.Config) (*postgres.Store, error) {
	if err := postgres.CheckTransport(cfg.Postgres.DSN, cfg.Postgres.AllowInsecureTransport); err != nil {
		return nil, err
	}
	store, err := postgres.Open(ctx, postgres.Config{DSN: cfg.Postgres.DSN, MaxConns: cfg.Postgres.MaxConns, ConnectTimeout: cfg.Postgres.ConnectTimeout})
	if err != nil {
		return nil, fmt.Errorf("auth.mode is required but the database is not usable: %w", err)
	}
	sctx, cancel := context.WithTimeout(ctx, cfg.Postgres.ConnectTimeout)
	defer cancel()
	status, err := store.MigrationStatuses(sctx)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("auth.mode is required but the database schema is not usable: %w", err)
	}
	for _, m := range status {
		if !m.Applied {
			store.Close()
			return nil, fmt.Errorf("auth.mode is required but migration %04d_%s is not applied; run serverflow-admin migrate up", m.Version, m.Name)
		}
	}
	return store, nil
}

// lookupCap is how many key lookups may run at once: all pool connections but two, so lookups never
// take the whole pool. Configuration validation guarantees at least four connections in required mode.
func lookupCap(maxConns int) int { return maxConns - 2 }

// checkAuthWiring refuses to start when the server's idea of whether API keys are required differs
// from auth.mode, in either direction.
func checkAuthWiring(mode string, serverRequiresAuth bool) error {
	if (mode == config.AuthModeRequired) != serverRequiresAuth {
		return fmt.Errorf("authentication wiring does not match auth.mode %q; refusing to start", mode)
	}
	return nil
}

// openRedis builds the Redis client when rate limiting or request metadata needs one, and checks that Redis
// answers and accepts the password when rate limiting is required. It returns nil when Redis is not used.
// Errors never contain the address or the password.
func openRedis(ctx context.Context, cfg config.Config, logger *slog.Logger) (*redis.Client, error) {
	if cfg.RateLimit.Mode != config.RateLimitRequired && !cfg.Redis.RequestMetadata {
		return nil, nil
	}
	rcfg := redis.Config{
		Address: cfg.Redis.Address, Password: cfg.Redis.Password, DB: cfg.Redis.DB, TLS: cfg.Redis.TLS, Timeout: cfg.Redis.Timeout,
		Backoff: cfg.Redis.Backoff, PoolSize: cfg.Redis.PoolSize, AllowInsecureTransport: cfg.Redis.AllowInsecureTransport, Logger: logger,
	}
	if err := redis.CheckTransport(rcfg); err != nil {
		return nil, err
	}
	c, err := redis.New(rcfg)
	if err != nil {
		return nil, err
	}
	if cfg.RateLimit.Mode == config.RateLimitRequired {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := c.Ping(pctx); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("rate_limit.mode is required but Redis is not usable: %w", err)
		}
	}
	return c, nil
}

// openEvents builds the lifecycle event observer when events.mode is on. It does not connect: a broker that is
// down at start is logged once by the client and is not an error. Errors never contain the SASL password.
func openEvents(cfg config.Config, logger *slog.Logger) (*events.Observer, *kafka.Producer, error) {
	if cfg.Events.Mode != config.EventsOn {
		return nil, nil, nil
	}
	kcfg := kafka.ConfigFrom(cfg.Events)
	kcfg.Logger = logger
	sink, err := kafka.NewProducer(kcfg)
	if err != nil {
		return nil, nil, fmt.Errorf("events.mode is on but the producer could not be built: %w", err)
	}
	source := cfg.Events.Source
	if source == "" {
		if source, _ = os.Hostname(); source == "" {
			source = "gateway"
		}
	}
	obs := events.NewObserver(sink, events.Config{BufferSize: cfg.Events.BufferSize, Source: source}, logger)
	logger.Info("lifecycle events on", "component", "gateway", "brokers", cfg.Events.Brokers, "topic", cfg.Events.Topic,
		"buffer_size", cfg.Events.BufferSize, "sasl", cfg.Events.SASLMechanism, "tls", cfg.Events.TLS)
	return obs, sink, nil
}
