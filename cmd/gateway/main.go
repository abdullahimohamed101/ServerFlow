// Command gateway is the ServerFlow API gateway entry point. In Phase 0
// it only exercises configuration and logging; HTTP serving arrives in Phase 2.
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
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	logger.Info("gateway starting", "component", "gateway", "port", cfg.Gateway.Port, "scheduler", cfg.Scheduler.Strategy)
}
