// Command usage-consumer is the Kafka usage consumer entry point. In
// Phase 0 it only exercises configuration and logging; event consumption
// arrives in Phase 12.
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
		fmt.Fprintf(os.Stderr, "usage-consumer: %v\n", err)
		os.Exit(1)
	}

	logger, err := telemetry.NewDefaultLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "usage-consumer: %v\n", err)
		os.Exit(1)
	}

	logger.Info("usage-consumer starting", "component", "usage-consumer")
}
