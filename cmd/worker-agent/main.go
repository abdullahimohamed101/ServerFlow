// Command worker-agent watches one local inference backend and keeps the
// control plane's registry informed: it registers the worker, heartbeats its
// state and load, and deregisters on a graceful shutdown. It is not in the
// data path. SIGINT or SIGTERM shuts it down gracefully; a second signal
// forces it to quit.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/registry/client"
	"serverflow/internal/telemetry"
	"serverflow/internal/worker"
)

const shutdownTimeout = 5 * time.Second

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker-agent: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.ValidateAgent(); err != nil {
		fmt.Fprintf(os.Stderr, "worker-agent: %v\n", err)
		os.Exit(1)
	}
	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker-agent: %v\n", err)
		os.Exit(1)
	}

	agent := worker.New(worker.Config{
		WorkerID: cfg.Worker.ID, Model: cfg.Worker.Model, AdvertiseURL: cfg.Worker.AdvertiseURL, Interval: cfg.Worker.HeartbeatInterval,
	}, worker.NewMockBackend(cfg.Worker.BackendURL), client.New(cfg.Worker.ControlPlaneURL, cfg.ControlPlane.Token, nil), logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }() // a second signal force-quits

	logger.Info("worker-agent starting", "component", "worker-agent", "worker_id", cfg.Worker.ID, "model", cfg.Worker.Model,
		"backend_url", cfg.Worker.BackendURL, "advertise_url", cfg.Worker.AdvertiseURL,
		"control_plane_url", cfg.Worker.ControlPlaneURL, "heartbeat_interval", cfg.Worker.HeartbeatInterval.String())

	if err := agent.Run(ctx); errors.Is(err, worker.ErrSuperseded) {
		// Another process registered under this worker ID. Do not deregister
		// (that registration is no longer ours) and do not try again.
		logger.Error("worker-agent exiting: another process is using this worker ID", "component", "worker-agent", "worker_id", cfg.Worker.ID)
		os.Exit(1)
	}

	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	agent.Shutdown(sctx)
	logger.Info("worker-agent stopped", "component", "worker-agent", "worker_id", cfg.Worker.ID)
}
