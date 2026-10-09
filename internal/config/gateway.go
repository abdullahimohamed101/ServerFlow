package config

import (
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

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
	// MaxAttempts is how many workers one request may try, counting the first (spec section 25).
	// 1 disables retries. RetryStatuses are the worker response statuses that may be retried on
	// another worker, before any output has reached the client. Both apply to the registry source.
	MaxAttempts   int   `yaml:"max_attempts"`
	RetryStatuses []int `yaml:"retry_statuses"`
}

// Worker sources.
const (
	WorkerSourceStatic   = "static"
	WorkerSourceRegistry = "registry"
)

// Bounds on how often the gateway polls the registry and how long it trusts the answer.
const (
	maxMaxAttempts   = 5
	maxRetryStatuses = 10

	minRegistryRefresh   = 10 * time.Millisecond
	maxRegistryStaleness = 5 * time.Minute
)

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
	if g.MaxAttempts < 1 || g.MaxAttempts > maxMaxAttempts {
		return fmt.Errorf("gateway.max_attempts must be 1-%d (1 disables retries), got %d", maxMaxAttempts, g.MaxAttempts)
	}
	if len(g.RetryStatuses) > maxRetryStatuses {
		return fmt.Errorf("gateway.retry_statuses lists %d statuses; at most %d", len(g.RetryStatuses), maxRetryStatuses)
	}
	seenStatus := map[int]bool{}
	for _, st := range g.RetryStatuses {
		if st < 500 || st > 599 {
			return fmt.Errorf("gateway.retry_statuses: %d is not a 5xx status", st)
		}
		if seenStatus[st] {
			return fmt.Errorf("gateway.retry_statuses lists %d twice", st)
		}
		seenStatus[st] = true
	}
	for _, n := range g.WorkerNetworks {
		if _, err := netip.ParsePrefix(n); err != nil {
			return fmt.Errorf("gateway.worker_networks: %q is not a CIDR such as 10.0.0.0/8", truncateForError(n))
		}
	}
	return nil
}
