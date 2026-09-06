package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Load builds a Config from defaults, an optional YAML file, and
// environment variables. CLI flags are handled by each binary, which
// passes the resulting config file path here.

func Load(path string) (Config, error) {
	cfg := Default()

	if path != "" {
		if err := applyFile(&cfg, path); err != nil {
			return Config{}, err
		}
	}

	applyEnv(&cfg)

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func applyFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config file %q: %w", path, err)
	}
	return nil
}

// applyEnv overlays environment variables onto the config. Only
// variables with the SERVERFLOW_ prefix are considered. Invalid values
// are ignored so a misconfigured variable cannot crash startup; the
// validation pass still catches structurally invalid config.

func applyEnv(cfg *Config) {
	env := func(key string) (string, bool) {
		return os.LookupEnv("SERVERFLOW_" + key)
	}
	if v, ok := env("GATEWAY_PORT"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Gateway.Port = n
		}
	}
	if v, ok := env("SCHEDULER_STRATEGY"); ok {
		cfg.Scheduler.Strategy = v
	}
	if v, ok := env("WORKER_HEARTBEAT_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Worker.HeartbeatInterval = d
		}
	}
	if v, ok := env("WORKER_UNHEALTHY_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Worker.UnhealthyTimeout = d
		}
	}
	if v, ok := env("ADMISSION_MAX_GLOBAL_REQUESTS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Admission.MaxGlobalRequests = n
		}
	}
	if v, ok := env("REDIS_ADDRESS"); ok {
		cfg.Redis.Address = v
	}
	if v, ok := env("POSTGRES_DSN"); ok {
		cfg.Postgres.DSN = v
	}
	if v, ok := env("LOG_LEVEL"); ok {
		cfg.Log.Level = strings.ToLower(v)
	}
}
