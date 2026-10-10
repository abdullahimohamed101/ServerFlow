package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func strp(s string) *string { return &s }

const metricsCanary = "metrics-token-canary-0123456789"

func TestMetricsDefaultsAreLoopbackAndTenantLabelsOff(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	m := c.Metrics
	if m.TenantLabels || m.AllowNonLoopback || m.Token != "" || m.Listen != nil {
		t.Fatalf("defaults must be closed: %+v", m)
	}
	if m.ListenAddr(DefaultGatewayMetricsListen) != "127.0.0.1:9100" || m.ListenAddr(DefaultControlPlaneMetricsListen) != "127.0.0.1:9101" {
		t.Fatal("per-binary default addresses")
	}
	if m.MaxModels != 64 || m.MaxWorkersLabel != 256 || m.MaxTenants != 50 {
		t.Fatalf("caps: %+v", m)
	}
}

func TestMetricsListenGuard(t *testing.T) {
	cases := []struct {
		name   string
		listen *string
		token  string
		allow  bool
		ok     bool
	}{
		{"unset", nil, "", false, true},
		{"explicit off", strp(""), "", false, true},
		{"loopback v4", strp("127.0.0.1:9100"), "", false, true},
		{"loopback v6", strp("[::1]:9100"), "", false, true},
		{"localhost", strp("localhost:9100"), "", false, true},
		{"all interfaces, no token", strp(":9100"), "", false, false},
		{"0.0.0.0, no token", strp("0.0.0.0:9100"), "", false, false},
		{"lan address, no token", strp("10.0.0.5:9100"), "", false, false},
		{"hostname, no token", strp("metrics.internal:9100"), "", false, false},
		{"0.0.0.0 with token", strp("0.0.0.0:9100"), metricsCanary, false, true},
		{"0.0.0.0 with the explicit flag", strp("0.0.0.0:9100"), "", true, true},
		{"not host:port", strp("9100"), "", false, false},
	}
	for _, tc := range cases {
		c := Default()
		c.Metrics.Listen, c.Metrics.Token, c.Metrics.AllowNonLoopback = tc.listen, tc.token, tc.allow
		err := c.Validate()
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && strings.Contains(err.Error(), metricsCanary) {
			t.Errorf("%s: error echoes the token", tc.name)
		}
	}
}

func TestMetricsBoundsAndShortToken(t *testing.T) {
	for name, mut := range map[string]func(*MetricsConfig){
		"short token":     func(m *MetricsConfig) { m.Token = "short" },
		"zero models":     func(m *MetricsConfig) { m.MaxModels = 0 },
		"huge models":     func(m *MetricsConfig) { m.MaxModels = 100000 },
		"zero workers":    func(m *MetricsConfig) { m.MaxWorkersLabel = 0 },
		"zero tenants":    func(m *MetricsConfig) { m.MaxTenants = 0 },
		"too many tenant": func(m *MetricsConfig) { m.MaxTenants = 100000 },
	} {
		c := Default()
		mut(&c.Metrics)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestMetricsTokenIsNeverFormatted(t *testing.T) {
	c := Default()
	c.Metrics.Token = metricsCanary
	c.Metrics.Listen = strp("0.0.0.0:9100")
	var logs strings.Builder
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	log.Info("config", "metrics", c.Metrics, "all", c)
	out := []string{
		c.Metrics.String(), c.Metrics.GoString(), fmt.Sprintf("%v %+v %#v %s", c.Metrics, c.Metrics, c.Metrics, c.Metrics), logs.String(),
	}
	for _, v := range []any{c.Metrics, c, &c} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(b))
	}
	for i, s := range out {
		if strings.Contains(s, metricsCanary) {
			t.Errorf("output %d contains the token: %s", i, s)
		}
	}
	b, _ := json.Marshal(c.Metrics)
	if !strings.Contains(string(b), "redacted") {
		t.Errorf("expected a redaction marker: %s", b)
	}
}

func TestMetricsEnv(t *testing.T) {
	t.Setenv("SERVERFLOW_METRICS_LISTEN", "")
	t.Setenv("SERVERFLOW_METRICS_TENANT_LABELS", "true")
	t.Setenv("SERVERFLOW_METRICS_MAX_TENANTS", "7")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Metrics.Listen == nil || *c.Metrics.Listen != "" || c.Metrics.ListenAddr("x:1") != "" {
		t.Fatal("an empty SERVERFLOW_METRICS_LISTEN must switch the endpoint off")
	}
	if !c.Metrics.TenantLabels || c.Metrics.MaxTenants != 7 {
		t.Fatalf("%+v", c.Metrics)
	}
}
