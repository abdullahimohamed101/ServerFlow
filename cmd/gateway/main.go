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

	"serverflow/internal/config"
	"serverflow/internal/gateway"
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

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Gateway.Port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	var srv *gateway.Server
	if cfg.Gateway.WorkerSource == config.WorkerSourceRegistry {
		if srv, err = gateway.NewRegistry(cfg, logger); err != nil {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			os.Exit(1)
		}
		// The token itself is never logged.
		logger.Info("gateway starting", "component", "gateway", "port", cfg.Gateway.Port, "worker_source", "registry",
			"strategy", cfg.Scheduler.Strategy, "refresh", cfg.Gateway.RegistryRefresh.String(),
			"max_staleness", cfg.Gateway.RegistryMaxStaleness.String(), "auth", cfg.ControlPlane.Token != "")
	} else {
		srv = gateway.New(cfg.Gateway, logger)
		logger.Info("gateway starting", "component", "gateway", "port", cfg.Gateway.Port,
			"upstream", cfg.Gateway.UpstreamURL, "models", cfg.Gateway.Models)
	}

	if err := srv.Serve(ctx, ln); err != nil {
		logger.Error("gateway stopped with error", "component", "gateway", "error", err)
		os.Exit(1)
	}
	logger.Info("gateway stopped", "component", "gateway")
}
