// Command worker-agent is the inference worker agent entry point. In
// Phase 0 it only exercises configuration and logging; registration
// and heartbeats arrive in Phase 4.
package main

import (
	"flag"
	"fmt"
	"os"

	"serverflow/internal/config"
	"serverflow/internal/telemetry"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker-agent: %v\n", err)
		os.Exit(1)
	}

	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker-agent: %v\n", err)
		os.Exit(1)
	}

	logger.Info("worker-agent starting", "component", "worker-agent", "heartbeat_interval", cfg.Worker.HeartbeatInterval.String(), "unhealthy_timeout", cfg.Worker.UnhealthyTimeout.String())
}
