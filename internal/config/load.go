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
	if v, ok := env("GATEWAY_UPSTREAM_URL"); ok {
		cfg.Gateway.UpstreamURL = v
	}
	if v, ok := env("GATEWAY_MODELS"); ok {
		cfg.Gateway.Models = splitList(v)
	}
	if v, ok := env("GATEWAY_READINESS_PATH"); ok {
		cfg.Gateway.ReadinessPath = v
	}
	if v, ok := env("GATEWAY_MAX_REQUEST_BYTES"); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Gateway.MaxRequestBytes = n
		}
	}
	if v, ok := env("GATEWAY_MAX_TOKENS_LIMIT"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Gateway.MaxTokensLimit = n
		}
	}
	if v, ok := env("GATEWAY_UPSTREAM_HEADER_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Gateway.UpstreamHeaderTimeout = d
		}
	}
	if v, ok := env("GATEWAY_UPSTREAM_IDLE_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Gateway.UpstreamIdleTimeout = d
		}
	}
	if v, ok := env("GATEWAY_SHUTDOWN_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Gateway.ShutdownTimeout = d
		}
	}
	for key, dst := range map[string]*string{
		"GATEWAY_WORKER_SOURCE":     &cfg.Gateway.WorkerSource,
		"GATEWAY_CONTROL_PLANE_URL": &cfg.Gateway.ControlPlaneURL,
	} {
		if v, ok := env(key); ok {
			*dst = v
		}
	}
	for key, dst := range map[string]*time.Duration{
		"GATEWAY_REGISTRY_REFRESH":       &cfg.Gateway.RegistryRefresh,
		"GATEWAY_REGISTRY_MAX_STALENESS": &cfg.Gateway.RegistryMaxStaleness,
	} {
		if v, ok := env(key); ok {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}
	if v, ok := env("GATEWAY_MAX_ATTEMPTS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Gateway.MaxAttempts = n
		}
	}
	if v, ok := env("GATEWAY_RETRY_STATUSES"); ok {
		var list []int
		valid := true
		for _, part := range splitList(v) {
			n, err := strconv.Atoi(part)
			if err != nil {
				valid = false
				break
			}
			list = append(list, n)
		}
		if valid {
			cfg.Gateway.RetryStatuses = list
		}
	}
	if v, ok := env("GATEWAY_WORKER_NETWORKS"); ok {
		cfg.Gateway.WorkerNetworks = splitList(v)
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
	for key, dst := range map[string]*time.Duration{
		"WORKER_SUSPECT_TIMEOUT": &cfg.Worker.SuspectTimeout,
		"WORKER_LOST_TIMEOUT":    &cfg.Worker.LostTimeout,
		"WORKER_RETENTION":       &cfg.Worker.Retention,
	} {
		if v, ok := env(key); ok {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}
	for key, dst := range map[string]*string{
		"WORKER_ID":                &cfg.Worker.ID,
		"WORKER_MODEL":             &cfg.Worker.Model,
		"WORKER_BACKEND_URL":       &cfg.Worker.BackendURL,
		"WORKER_ADVERTISE_URL":     &cfg.Worker.AdvertiseURL,
		"WORKER_CONTROL_PLANE_URL": &cfg.Worker.ControlPlaneURL,
		"CONTROL_PLANE_ADDR":       &cfg.ControlPlane.Addr,
		"CONTROL_PLANE_TOKEN":      &cfg.ControlPlane.Token,
	} {
		if v, ok := env(key); ok {
			*dst = v
		}
	}
	if v, ok := env("CONTROL_PLANE_MAX_WORKERS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ControlPlane.MaxWorkers = n
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
	if v, ok := env("POSTGRES_MAX_CONNS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.MaxConns = n
		}
	}
	if v, ok := env("POSTGRES_CONNECT_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Postgres.ConnectTimeout = d
		}
	}
	if v, ok := env("POSTGRES_ALLOW_INSECURE_TRANSPORT"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.Postgres.AllowInsecureTransport = b
		}
	}
	if v, ok := env("AUTH_MODE"); ok {
		cfg.Auth.Mode = strings.ToLower(strings.TrimSpace(v))
	}
	if v, ok := env("AUTH_CACHE_SIZE"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Auth.CacheSize = n
		}
	}
	for key, dst := range map[string]*time.Duration{
		"AUTH_CACHE_TTL":    &cfg.Auth.CacheTTL,
		"AUTH_NEGATIVE_TTL": &cfg.Auth.NegativeTTL,
		"AUTH_STALE_GRACE":  &cfg.Auth.StaleGrace,
	} {
		if v, ok := env(key); ok {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}
	if v, ok := env("LOG_LEVEL"); ok {
		cfg.Log.Level = strings.ToLower(v)
	}
}

// splitList splits a comma-separated value into trimmed, non-empty items.
func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
