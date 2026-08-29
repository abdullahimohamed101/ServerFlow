// Command control-plane is the InferGrid control plane entry point. In
// Phase 0 it only exercises configuration and logging; registry and
// placement controllers arrive in later phases.
package main

import (
	"flag"
	"fmt"
	"os"

	"infergrid/internal/config"
	"infergrid/internal/telemetry"
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

	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "control-plane: %v\n", err)
		os.Exit(1)
	}

	logger.Info("control-plane starting", "component", "control-plane", "scheduler", cfg.Scheduler.Strategy)
}
