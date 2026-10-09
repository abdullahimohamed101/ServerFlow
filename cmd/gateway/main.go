// Command gateway is the ServerFlow API gateway entry point. It serves the
// OpenAI-compatible API in front of the configured inference upstream and
// shuts down gracefully on SIGINT/SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/postgres"
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
			MaxLookups: max(2, cfg.Postgres.MaxConns-2),
		})
		opts = append(opts, gateway.WithAuthenticator(authn))
		logger.Info("api key authentication required", "component", "gateway", "cache_ttl", cfg.Auth.CacheTTL.String(),
			"negative_ttl", cfg.Auth.NegativeTTL.String(), "cache_size", cfg.Auth.CacheSize, "stale_grace", cfg.Auth.StaleGrace.String())
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
		srv = gateway.New(cfg.Gateway, logger, opts...)
		logger.Info("gateway starting", "component", "gateway", "port", cfg.Gateway.Port,
			"upstream", cfg.Gateway.UpstreamURL, "models", cfg.Gateway.Models, "api_key_auth", cfg.Auth.Mode)
	}

	if (cfg.Auth.Mode == config.AuthModeRequired) != srv.AuthRequired() {
		fmt.Fprintln(os.Stderr, "gateway: authentication wiring does not match auth.mode; refusing to start")
		os.Exit(1)
	}

	if err := srv.Serve(ctx, ln); err != nil {
		logger.Error("gateway stopped with error", "component", "gateway", "error", err)
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
