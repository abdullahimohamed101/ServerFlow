package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
)

// MetricsConfig configures the Prometheus endpoint (Phase 10, ADR-017). The gateway and the control plane serve
// /metrics on a listener of its own, never on the data or API port. Listen is where: unset means the binary's
// default (gateway 127.0.0.1:9100, control plane 127.0.0.1:9101), an explicit empty string turns the endpoint
// off. A Listen address that is not loopback needs Token (a bearer token Prometheus presents) or the explicit
// AllowNonLoopback. Token is a secret: it is never logged or echoed in an error, and formatting a MetricsConfig
// prints it redacted.
//
// The Max* fields cap how many distinct values a label may take; further values become "other". TenantLabels
// adds one extra series, tenant_requests_total{tenant,outcome}, for the first MaxTenants tenants; it is off by
// default because tenants are unbounded.
type MetricsConfig struct {
	Listen           *string `yaml:"listen"`
	Token            string  `yaml:"token"`
	AllowNonLoopback bool    `yaml:"allow_non_loopback"`
	MaxModels        int     `yaml:"max_models"`
	MaxWorkersLabel  int     `yaml:"max_workers_label"`
	MaxTenants       int     `yaml:"max_tenants"`
	TenantLabels     bool    `yaml:"tenant_labels"`
}

// Default metrics listeners of the binaries that serve /metrics on a separate port.
const (
	DefaultGatewayMetricsListen      = "127.0.0.1:9100"
	DefaultControlPlaneMetricsListen = "127.0.0.1:9101"
)

// Bounds for the metrics section.
const (
	minMetricsTokenLen  = 16
	maxMetricsModels    = 1000
	maxMetricsWorkers   = 10000
	maxMetricsTenants   = 1000
	defaultMetricsModel = 64
)

func defaultMetrics() MetricsConfig {
	return MetricsConfig{MaxModels: defaultMetricsModel, MaxWorkersLabel: 256, MaxTenants: 50}
}

// ListenAddr is the address to serve /metrics on given the binary's default, or "" when it is switched off.
func (m MetricsConfig) ListenAddr(binaryDefault string) string {
	if m.Listen == nil {
		return binaryDefault
	}
	return *m.Listen
}

// ListenerNeedsAuth reports whether addr (host:port) is reachable beyond this machine, so serving /metrics on it
// without a token needs the explicit AllowNonLoopback. A host that is not a loopback IP or "localhost" counts
// as reachable, as does an empty host (all interfaces).
func ListenerNeedsAuth(addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("metrics.listen must be host:port")
	}
	return !isLoopbackHost(host), nil
}

// CheckListen applies the exposure guard to the effective address: a non-loopback listener needs a token or
// allow_non_loopback. It never echoes the token.
func (m MetricsConfig) CheckListen(addr string) error {
	if addr == "" {
		return nil
	}
	exposed, err := ListenerNeedsAuth(addr)
	if err != nil {
		return err
	}
	if exposed && m.Token == "" && !m.AllowNonLoopback {
		return fmt.Errorf("metrics.listen is not a loopback address: set metrics.token (or metrics.allow_non_loopback) so /metrics is not world-readable")
	}
	return nil
}

func (m *MetricsConfig) validate() error {
	if m.Token != "" && len(m.Token) < minMetricsTokenLen {
		return fmt.Errorf("metrics.token must be at least %d characters", minMetricsTokenLen)
	}
	if m.MaxModels < 1 || m.MaxModels > maxMetricsModels {
		return fmt.Errorf("metrics.max_models must be in 1-%d", maxMetricsModels)
	}
	if m.MaxWorkersLabel < 1 || m.MaxWorkersLabel > maxMetricsWorkers {
		return fmt.Errorf("metrics.max_workers_label must be in 1-%d", maxMetricsWorkers)
	}
	if m.MaxTenants < 1 || m.MaxTenants > maxMetricsTenants {
		return fmt.Errorf("metrics.max_tenants must be in 1-%d", maxMetricsTenants)
	}
	if m.Listen != nil {
		// The default addresses are loopback; only an explicit one can break the guard. The exact
		// address check runs again where the listener is bound, with the binary's default.
		if err := m.CheckListen(*m.Listen); err != nil {
			return err
		}
	}
	return nil
}

// String formats the config without the token. GoString and LogValue do the same.
func (m MetricsConfig) String() string {
	tok := "<unset>"
	if m.Token != "" {
		tok = "<redacted>"
	}
	listen := "<default>"
	if m.Listen != nil {
		listen = *m.Listen
		if listen == "" {
			listen = "<off>"
		}
	}
	return fmt.Sprintf("{listen:%s token:%s allow_non_loopback:%t max_models:%d max_workers_label:%d max_tenants:%d tenant_labels:%t}",
		listen, tok, m.AllowNonLoopback, m.MaxModels, m.MaxWorkersLabel, m.MaxTenants, m.TenantLabels)
}

// GoString implements fmt.GoStringer.
func (m MetricsConfig) GoString() string { return "config.MetricsConfig" + m.String() }

// LogValue implements slog.LogValuer.
func (m MetricsConfig) LogValue() slog.Value { return slog.StringValue(m.String()) }

// MarshalJSON redacts the token, so logging a whole Config as JSON cannot leak it.
func (m MetricsConfig) MarshalJSON() ([]byte, error) {
	type plain MetricsConfig // no methods, so no recursion
	p := plain(m)
	if p.Token != "" {
		p.Token = "<redacted>"
	}
	return json.Marshal(p)
}
