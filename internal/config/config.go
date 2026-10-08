// Package config provides typed, validated configuration for all
// ServerFlow components. Configuration is loaded from defaults, an
// optional YAML file, environment variables, and CLI flags, in that
// order. No component other than this package should read environment
// variables directly.
package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

// Config is the root configuration for every ServerFlow component.

// Each binary loads the same shape and uses only the sections it needs.
type Config struct {
	Gateway   GatewayConfig   `yaml:"gateway"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Worker    WorkerConfig    `yaml:"worker"`
	// ControlPlane configures the worker registry service.
	ControlPlane ControlPlaneConfig `yaml:"control_plane"`
	Admission    AdmissionConfig    `yaml:"admission"`
	Redis        RedisConfig        `yaml:"redis"`
	Postgres     PostgresConfig     `yaml:"postgres"`
	Log          LogConfig          `yaml:"log"`

	configFilePath string
}

// GatewayConfig configures the HTTP API gateway. Port is the listen
// port for the HTTP server. UpstreamURL and Models describe the single
// OpenAI-compatible upstream the gateway proxies to until the worker
// registry and scheduler replace them; ReadinessPath is probed on the
// upstream by /readyz. MaxRequestBytes and MaxTokensLimit bound client
// input. UpstreamHeaderTimeout bounds the wait for upstream response
// headers; vLLM sends headers for a non-streaming request only after the
// whole generation finishes, so raise it if long non-streaming completions
// must be supported. UpstreamIdleTimeout bounds the silence between upstream
// body reads, so a stalled stream cannot hold a connection forever.
// ShutdownTimeout bounds graceful shutdown.
type GatewayConfig struct {
	Port                  int           `yaml:"port"`
	UpstreamURL           string        `yaml:"upstream_url"`
	Models                []string      `yaml:"models"`
	ReadinessPath         string        `yaml:"readiness_path"`
	MaxRequestBytes       int64         `yaml:"max_request_bytes"`
	MaxTokensLimit        int           `yaml:"max_tokens_limit"`
	UpstreamHeaderTimeout time.Duration `yaml:"upstream_header_timeout"`
	UpstreamIdleTimeout   time.Duration `yaml:"upstream_idle_timeout"`
	ShutdownTimeout       time.Duration `yaml:"shutdown_timeout"`

	// WorkerSource chooses where requests go: "static" forwards everything to
	// UpstreamURL (Phases 2-3); "registry" asks the scheduler to pick a worker
	// from the control plane's registry (Phase 5).
	WorkerSource string `yaml:"worker_source"`
	// ControlPlaneURL, RegistryRefresh and RegistryMaxStaleness apply to the
	// registry source. The gateway polls the control plane every RegistryRefresh
	// and trusts its last answer for at most RegistryMaxStaleness, then fails
	// closed. The control plane token is control_plane.token.
	ControlPlaneURL      string        `yaml:"control_plane_url"`
	RegistryRefresh      time.Duration `yaml:"registry_refresh"`
	RegistryMaxStaleness time.Duration `yaml:"registry_max_staleness"`
	// WorkerNetworks optionally restricts the addresses the gateway will dial to
	// these CIDRs. Empty allows any address that is not forbidden outright
	// (unspecified, link-local, multicast, broadcast).
	WorkerNetworks []string `yaml:"worker_networks"`
}

// Worker sources.
const (
	WorkerSourceStatic   = "static"
	WorkerSourceRegistry = "registry"
)

// SchedulerConfig configures the request scheduling strategy. Strategy
// names the scheduling algorithm to use.
type SchedulerConfig struct {
	Strategy string `yaml:"strategy"`
}

// WorkerConfig configures worker agents and how the registry judges them.
//
// HeartbeatInterval is how often workers report state. The registry measures
// heartbeat age at receipt: a worker is suspect once SuspectTimeout passes
// without one, unhealthy after UnhealthyTimeout, and lost after LostTimeout;
// a lost worker is evicted once Retention more has passed (spec section 12).
// SuspectTimeout must be at least two heartbeat intervals so one late
// heartbeat does not flap a worker.
//
// ID, Model, BackendURL, AdvertiseURL and ControlPlaneURL identify an agent's
// worker; only the agent requires them (see ValidateAgent).
type WorkerConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	SuspectTimeout    time.Duration `yaml:"suspect_timeout"`
	UnhealthyTimeout  time.Duration `yaml:"unhealthy_timeout"`
	LostTimeout       time.Duration `yaml:"lost_timeout"`
	Retention         time.Duration `yaml:"retention"`

	ID              string `yaml:"id"`
	Model           string `yaml:"model"`
	BackendURL      string `yaml:"backend_url"`
	AdvertiseURL    string `yaml:"advertise_url"`
	ControlPlaneURL string `yaml:"control_plane_url"`
}

// ControlPlaneConfig configures the control plane service. Addr is where it
// listens (loopback by default). Token, if set, is the shared secret every
// request must present as a bearer token; it is required for any address that
// is not loopback (see ValidateServe). MaxWorkers bounds registry memory.
type ControlPlaneConfig struct {
	Addr       string `yaml:"addr"`
	Token      string `yaml:"token"`
	MaxWorkers int    `yaml:"max_workers"`
}

// AdmissionConfig configures cluster admission control thresholds.
type AdmissionConfig struct {
	MaxGlobalRequests int `yaml:"max_global_requests"`
}

// RedisConfig configures the shared ephemeral state store (rate limits,
// coordination, routing hints).
type RedisConfig struct {
	Address string `yaml:"address"`
}

// PostgresConfig configures the durable metadata store.
type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// LogConfig configures structured logging.
type LogConfig struct {
	Level string `yaml:"level"`
}

// Default returns the built-in defaults for every component. Callers
// must not mutate the returned value before passing it to Load.

func Default() Config {
	return Config{
		Gateway: GatewayConfig{
			Port:                  8080,
			UpstreamURL:           "http://localhost:8000",
			Models:                []string{"default"},
			ReadinessPath:         "/v1/models",
			MaxRequestBytes:       1 << 20,
			MaxTokensLimit:        4096,
			UpstreamHeaderTimeout: 60 * time.Second,
			UpstreamIdleTimeout:   120 * time.Second,
			ShutdownTimeout:       30 * time.Second,
			WorkerSource:          WorkerSourceStatic,
			ControlPlaneURL:       "http://127.0.0.1:9090",
			RegistryRefresh:       time.Second,
			RegistryMaxStaleness:  10 * time.Second,
		},
		Scheduler: SchedulerConfig{
			Strategy: "round-robin",
		},
		Worker: WorkerConfig{
			HeartbeatInterval: 2 * time.Second,
			SuspectTimeout:    5 * time.Second,
			UnhealthyTimeout:  10 * time.Second,
			LostTimeout:       30 * time.Second,
			Retention:         5 * time.Minute,
			ControlPlaneURL:   "http://127.0.0.1:9090",
		},
		ControlPlane: ControlPlaneConfig{
			Addr:       "127.0.0.1:9090",
			MaxWorkers: 1000,
		},
		Admission: AdmissionConfig{
			MaxGlobalRequests: 1000,
		},
		Redis: RedisConfig{
			Address: "redis:6379",
		},
		Postgres: PostgresConfig{
			DSN: "postgres://postgres:postgres@postgres:5432/serverflow?sslmode=disable",
		},
		Log: LogConfig{
			Level: "info",
		},
	}
}

// Validate returns an error describing the first invalid setting, or
// nil if the configuration is valid. Callers must invoke this after
// loading and before starting any component (fail fast).
func (c *Config) Validate() error {
	if c.Gateway.Port <= 0 || c.Gateway.Port > 65535 {
		return fmt.Errorf("gateway.port must be in 1-65535, got %d", c.Gateway.Port)
	}
	if err := c.Gateway.validate(); err != nil {
		return err
	}
	switch c.Scheduler.Strategy {
	case "random", "round-robin", "least-active", "least-queue", "least-work":
	default:
		return fmt.Errorf("scheduler.strategy %q is not supported", c.Scheduler.Strategy)
	}
	if err := c.validateWorkerSource(); err != nil {
		return err
	}
	if err := c.Worker.validate(); err != nil {
		return err
	}
	if err := c.ControlPlane.validate(); err != nil {
		return err
	}
	if c.Admission.MaxGlobalRequests <= 0 {
		return fmt.Errorf("admission.max_global_requests must be > 0")
	}
	if c.Redis.Address == "" {
		return fmt.Errorf("redis.address must not be empty")
	}
	if c.Postgres.DSN == "" {
		return fmt.Errorf("postgres.dsn must not be empty")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level %q is not supported", c.Log.Level)
	}
	return nil
}

func (w *WorkerConfig) validate() error {
	if w.HeartbeatInterval < minHeartbeatInterval {
		return fmt.Errorf("worker.heartbeat_interval must be at least %v", minHeartbeatInterval)
	}
	if w.SuspectTimeout <= 0 || w.UnhealthyTimeout <= 0 || w.LostTimeout <= 0 || w.Retention <= 0 {
		return fmt.Errorf("worker.suspect_timeout, unhealthy_timeout, lost_timeout and retention must be > 0")
	}
	if w.SuspectTimeout >= w.UnhealthyTimeout || w.UnhealthyTimeout >= w.LostTimeout {
		return fmt.Errorf("worker timeouts must satisfy suspect_timeout < unhealthy_timeout < lost_timeout")
	}
	if w.SuspectTimeout < 2*w.HeartbeatInterval {
		return fmt.Errorf("worker.suspect_timeout must be at least twice worker.heartbeat_interval")
	}
	return nil
}

// ValidateAgent checks everything an agent needs from the whole configuration:
// its own identity and addresses, and that the control plane token is not sent
// in cleartext to another machine.
func (c *Config) ValidateAgent() error {
	if err := c.Worker.validateAgent(); err != nil {
		return err
	}
	if c.ControlPlane.Token != "" {
		if u, err := url.Parse(c.Worker.ControlPlaneURL); err == nil && u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
			return fmt.Errorf("worker.control_plane_url must use https (or a loopback address) when control_plane.token is set, or the token would cross the network in cleartext")
		}
	}
	return nil
}

// validateAgent checks the settings an agent needs. Error messages never echo
// URLs, which could carry credentials.
func (w *WorkerConfig) validateAgent() error {
	if !protocol.ValidWorkerID(w.ID) {
		return fmt.Errorf("worker.id is required: 1-%d characters of letters, digits, '.', '_' or '-'", protocol.MaxWorkerIDLen)
	}
	if w.Model == "" {
		return fmt.Errorf("worker.model is required")
	}
	for name, v := range map[string]string{
		"worker.backend_url": w.BackendURL, "worker.advertise_url": w.AdvertiseURL, "worker.control_plane_url": w.ControlPlaneURL,
	} {
		if err := protocol.ValidateAddress(v); err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}
	}
	return nil
}

// minHeartbeatInterval keeps a typo from turning every agent into a hot loop.
const minHeartbeatInterval = 10 * time.Millisecond

const minTokenLen = 16

// Bounds on how often the gateway polls the registry and how long it trusts the answer.
const (
	minRegistryRefresh   = 10 * time.Millisecond
	maxRegistryStaleness = 5 * time.Minute
)

func (c *ControlPlaneConfig) validate() error {
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("control_plane.addr must be host:port")
	}
	if c.MaxWorkers < 1 || c.MaxWorkers > 100000 {
		return fmt.Errorf("control_plane.max_workers must be in 1-100000")
	}
	if c.Token != "" && len(c.Token) < minTokenLen {
		// Never echo the token itself.
		return fmt.Errorf("control_plane.token must be at least %d characters", minTokenLen)
	}
	return nil
}

// ValidateServe checks that the control plane may listen where it is
// configured to. Anything but loopback requires a token: without one any
// process on the network could register as a worker and steer traffic.
func (c *ControlPlaneConfig) ValidateServe() error {
	host, _, err := net.SplitHostPort(c.Addr)
	if err != nil {
		return fmt.Errorf("control_plane.addr must be host:port")
	}
	if c.Token == "" && !isLoopbackHost(host) {
		return fmt.Errorf("control_plane.token is required when control_plane.addr is not a loopback address")
	}
	return nil
}

// IsLoopbackHost reports whether host names this machine (localhost or a loopback IP).
func IsLoopbackHost(host string) bool { return isLoopbackHost(host) }

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (g *GatewayConfig) validate() error {
	// The URL may carry credentials, so error messages never echo it.
	u, err := url.Parse(g.UpstreamURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("gateway.upstream_url must be an http(s) URL with a host")
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("gateway.upstream_url must not contain credentials, a query, or a fragment")
	}
	if len(g.Models) == 0 {
		return fmt.Errorf("gateway.models must list at least one model")
	}
	seen := make(map[string]bool, len(g.Models))
	for _, m := range g.Models {
		if m == "" {
			return fmt.Errorf("gateway.models must not contain an empty name")
		}
		if seen[m] {
			return fmt.Errorf("gateway.models contains duplicate %q", m)
		}
		seen[m] = true
	}
	if !strings.HasPrefix(g.ReadinessPath, "/") {
		return fmt.Errorf("gateway.readiness_path must start with /, got %q", g.ReadinessPath)
	}
	if g.MaxRequestBytes <= 0 {
		return fmt.Errorf("gateway.max_request_bytes must be > 0")
	}
	if g.MaxTokensLimit <= 0 {
		return fmt.Errorf("gateway.max_tokens_limit must be > 0")
	}
	if g.UpstreamHeaderTimeout <= 0 {
		return fmt.Errorf("gateway.upstream_header_timeout must be > 0")
	}
	if g.UpstreamIdleTimeout <= 0 {
		return fmt.Errorf("gateway.upstream_idle_timeout must be > 0")
	}
	if g.ShutdownTimeout <= 0 {
		return fmt.Errorf("gateway.shutdown_timeout must be > 0")
	}
	return nil
}

// validateWorkerSource checks gateway.worker_source and, for the registry
// source, everything it needs. Error messages never echo URLs, which could
// carry credentials.
func (c *Config) validateWorkerSource() error {
	g := &c.Gateway
	switch g.WorkerSource {
	case WorkerSourceStatic:
		return nil
	case WorkerSourceRegistry:
	default:
		return fmt.Errorf("gateway.worker_source must be %q or %q, got %q", WorkerSourceStatic, WorkerSourceRegistry, g.WorkerSource)
	}
	if !slices.Contains(scheduler.Strategies(), c.Scheduler.Strategy) {
		return fmt.Errorf("scheduler.strategy %q is not implemented yet; use one of %s", c.Scheduler.Strategy, strings.Join(scheduler.Strategies(), ", "))
	}
	if err := protocol.ValidateAddress(g.ControlPlaneURL); err != nil {
		return fmt.Errorf("gateway.control_plane_url: %v", err)
	}
	if u, err := url.Parse(g.ControlPlaneURL); err == nil && c.ControlPlane.Token != "" && u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
		return fmt.Errorf("gateway.control_plane_url must use https (or a loopback address) when control_plane.token is set, or the token would cross the network in cleartext")
	}
	if g.RegistryRefresh < minRegistryRefresh {
		return fmt.Errorf("gateway.registry_refresh must be at least %v, or the gateway would hammer the control plane", minRegistryRefresh)
	}
	if g.RegistryMaxStaleness > maxRegistryStaleness {
		return fmt.Errorf("gateway.registry_max_staleness must be at most %v: a view that old is not worth trusting", maxRegistryStaleness)
	}
	if g.RegistryMaxStaleness < 2*g.RegistryRefresh {
		return fmt.Errorf("gateway.registry_max_staleness must be at least twice gateway.registry_refresh, or one slow refresh would fail every request")
	}
	if c.Worker.SuspectTimeout < g.RegistryRefresh+c.Worker.HeartbeatInterval {
		return fmt.Errorf("worker.suspect_timeout (%v) must be at least gateway.registry_refresh plus worker.heartbeat_interval (%v), or the gateway would see every worker as suspect between refreshes",
			c.Worker.SuspectTimeout, g.RegistryRefresh+c.Worker.HeartbeatInterval)
	}
	if c.ControlPlane.Token != "" && len(c.ControlPlane.Token) < minTokenLen {
		return fmt.Errorf("control_plane.token must be at least %d characters", minTokenLen)
	}
	for _, n := range g.WorkerNetworks {
		if _, err := netip.ParsePrefix(n); err != nil {
			return fmt.Errorf("gateway.worker_networks: %q is not a CIDR such as 10.0.0.0/8", truncateForError(n))
		}
	}
	return nil
}

func truncateForError(s string) string {
	if len(s) > 64 {
		return s[:64] + "..."
	}
	return s
}

// ConfigFilePath returns the path supplied via -config, or "" if none.

func (c *Config) ConfigFilePath() string {
	return c.configFilePath
}
