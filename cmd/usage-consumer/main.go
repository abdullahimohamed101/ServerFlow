// Command usage-consumer reads the terminal lifecycle events (inference.request.completed / .failed) from Kafka
// and records one usage row per request in PostgreSQL, idempotently (Phase 12, ADR-019). Offsets are committed
// only after the database commit; a record it cannot use is rejected by coordinates and skipped. It serves
// /healthz and /metrics on events.consumer.metrics_addr.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"serverflow/internal/config"
	"serverflow/internal/kafka"
	"serverflow/internal/postgres"
	"serverflow/internal/telemetry"
	"serverflow/internal/usage"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to YAML configuration file")
	flag.Parse()
	if err := run(configPath); err != nil {
		fmt.Fprintf(os.Stderr, "usage-consumer: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if err := cfg.ValidateConsumer(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	kcfg := kafka.ConfigFrom(cfg.Events)
	kcfg.Logger = logger
	src, err := kafka.NewConsumer(kcfg)
	if err != nil {
		return err
	}
	defer src.Close()

	c := usage.New(src, store, usage.Config{
		BatchSize: cfg.Events.Consumer.BatchSize, BatchTimeout: cfg.Events.Consumer.BatchTimeout,
		GroupID: cfg.Events.Consumer.GroupID, Topic: cfg.Events.Topic, Logger: logger,
	})
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(c.Collectors()...)
	ln, err := net.Listen("tcp", cfg.Events.Consumer.MetricsAddr)
	if err != nil {
		return fmt.Errorf("metrics listener: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped", "component", "usage-consumer", "error", err)
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()

	logger.Info("usage-consumer starting", "component", "usage-consumer", "brokers", cfg.Events.Brokers, "topic", cfg.Events.Topic,
		"group", cfg.Events.Consumer.GroupID, "start_offset", cfg.Events.Consumer.StartOffset, "metrics_addr", ln.Addr().String())
	if err := c.Run(ctx); err != nil {
		return err
	}
	logger.Info("usage-consumer stopped", "component", "usage-consumer")
	return nil
}

// openStore connects to PostgreSQL and checks that every migration (including usage_records) is applied. Errors
// never contain the DSN's password.
func openStore(ctx context.Context, cfg config.Config) (*postgres.Store, error) {
	if err := postgres.CheckTransport(cfg.Postgres.DSN, cfg.Postgres.AllowInsecureTransport); err != nil {
		return nil, err
	}
	store, err := postgres.Open(ctx, postgres.Config{DSN: cfg.Postgres.DSN, MaxConns: cfg.Postgres.MaxConns, ConnectTimeout: cfg.Postgres.ConnectTimeout})
	if err != nil {
		return nil, fmt.Errorf("the database is not usable: %w", err)
	}
	sctx, cancel := context.WithTimeout(ctx, cfg.Postgres.ConnectTimeout)
	defer cancel()
	status, err := store.MigrationStatuses(sctx)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("the database schema is not usable: %w", err)
	}
	for _, m := range status {
		if !m.Applied {
			store.Close()
			return nil, fmt.Errorf("migration %04d_%s is not applied; run serverflow-admin migrate up", m.Version, m.Name)
		}
	}
	return store, nil
}
