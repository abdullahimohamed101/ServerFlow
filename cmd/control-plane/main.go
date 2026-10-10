// Command control-plane is the ServerFlow control plane: it serves the worker
// registry API that worker agents register and heartbeat with, and that the
// scheduler will read. SIGINT or SIGTERM drains it gracefully; a second signal
// forces it to quit.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"serverflow/internal/config"
	"serverflow/internal/registry"
	"serverflow/internal/registry/server"
	"serverflow/internal/telemetry"
	"serverflow/internal/telemetry/metricsserver"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.ControlPlane.ValidateServe(); err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	}
	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	}

	reg, err := registry.New(registry.Config{
		Suspect: cfg.Worker.SuspectTimeout, Unhealthy: cfg.Worker.UnhealthyTimeout, Lost: cfg.Worker.LostTimeout,
		Retention: cfg.Worker.Retention, MaxWorkers: cfg.ControlPlane.MaxWorkers, HeartbeatInterval: cfg.Worker.HeartbeatInterval,
	}, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	}
	srv := server.New(reg, server.Config{Token: cfg.ControlPlane.Token}, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal start the shutdown, and give signals back to the
	// default handler so a second Ctrl-C force-quits.
	go func() { <-ctx.Done(); stop() }()

	ln, err := net.Listen("tcp", cfg.ControlPlane.Addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	}
	// The token itself is never logged, only whether one is required.
	logger.Info("control-plane starting", "component", "control-plane", "addr", ln.Addr().String(),
		"auth_required", cfg.ControlPlane.Token != "", "max_workers", cfg.ControlPlane.MaxWorkers,
		"suspect_after", cfg.Worker.SuspectTimeout.String(), "unhealthy_after", cfg.Worker.UnhealthyTimeout.String(),
		"lost_after", cfg.Worker.LostTimeout.String(), "heartbeat_interval", cfg.Worker.HeartbeatInterval.String())

	// /metrics has a listener of its own, never the API port (ADR-017). Bound before serving so a refused
	// or busy address stops the control plane at start-up.
	mreg := prometheus.NewRegistry()
	mreg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		telemetry.BuildInfoCollector(), registry.NewCollector(reg, cfg.Metrics.MaxModels))
	if ms, err := metricsserver.Listen(cfg.Metrics.ListenAddr(config.DefaultControlPlaneMetricsListen), cfg.Metrics, mreg, logger); err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	} else if ms != nil {
		logger.Info("metrics endpoint", "component", "control-plane", "addr", ms.Addr(), "token_required", cfg.Metrics.Token != "")
		go func() {
			if err := ms.Serve(ctx); err != nil {
				logger.Error("metrics endpoint stopped with error", "component", "control-plane", "error", err.Error())
			}
		}()
	} else {
		logger.Info("metrics endpoint is off (metrics.listen is empty)", "component", "control-plane")
	}

	if err := srv.Serve(ctx, ln); err != nil {
		logger.Error("control-plane stopped with error", "component", "control-plane", "error", err)
		os.Exit(1)
	}
	logger.Info("control-plane stopped", "component", "control-plane")
}
