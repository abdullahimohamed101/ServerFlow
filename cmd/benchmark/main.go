// Command benchmark is the scheduler benchmark harness entry point. In
// Phase 0 it only exercises configuration and logging; load generation
// arrives in Phase 7.
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
		fmt.Fprintf(os.Stderr, "benchmark: %v\n", err)
		os.Exit(1)
	}

	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark: %v\n", err)
		os.Exit(1)
	}

	logger.Info("benchmark starting", "component", "benchmark", "scheduler", cfg.Scheduler.Strategy)
}
