package integration

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// Process-level tests of the metrics listener (Phase 10, ADR-017): the real binaries, their start-up guard, the
// bearer token, and the token staying out of everything the process prints or serves.

const metricsCanary = "metrics-token-canary-0123456789"

func TestProcessMetricsListenerGuardFailsFast(t *testing.T) {
	for _, tt := range []struct {
		name string
		bin  string
		env  []string
		want string
	}{
		{"gateway on all interfaces without a token", "gateway", []string{"SERVERFLOW_METRICS_LISTEN=0.0.0.0:0"}, "metrics.token"},
		{"gateway on a hostname without a token", "gateway", []string{"SERVERFLOW_METRICS_LISTEN=metrics.example.internal:9100"}, "metrics.token"},
		{"gateway with a short token", "gateway", []string{"SERVERFLOW_METRICS_TOKEN=short"}, "at least 16 characters"},
		{"gateway with a malformed address", "gateway", []string{"SERVERFLOW_METRICS_LISTEN=9100"}, "host:port"},
		{"control plane on all interfaces without a token", "control-plane", []string{"SERVERFLOW_METRICS_LISTEN=0.0.0.0:0"}, "metrics.token"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runBin(t, tt.bin, tt.env)
			if code != 1 || !strings.Contains(out, tt.want) {
				t.Fatalf("exit %d, output %q; want exit 1 mentioning %q", code, out, tt.want)
			}
		})
	}
}

func TestProcessGatewayMetricsListener(t *testing.T) {
	mockAddr := freePort(t)
	startMockProc(t, mockAddr)
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	_, metricsPort, _ := strings.Cut(freePort(t), ":")

	// A non-loopback listener with a token: 401 without it, 401 with a wrong one, 200 with the right one.
	gw := startProc(t, "gateway", "gateway starting", []string{
		"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model, "SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + mockAddr,
		"SERVERFLOW_METRICS_LISTEN=0.0.0.0:" + metricsPort, "SERVERFLOW_METRICS_TOKEN=" + metricsCanary,
	})
	waitFor(t, 10*time.Second, "the gateway to be ready", func() bool {
		r, _ := authGet(t, gwAddr, "/readyz", "")
		return r.StatusCode == 200
	})
	base := gw.metricsURL(t)
	if r, _ := authGet(t, strings.TrimPrefix(base, "http://"), "/metrics", ""); r.StatusCode != 401 {
		t.Fatalf("no token: %d", r.StatusCode)
	}
	if r, _ := authGet(t, strings.TrimPrefix(base, "http://"), "/metrics", metricsCanary+"x"); r.StatusCode != 401 {
		t.Fatalf("wrong token: %d", r.StatusCode)
	}
	r, body := authGet(t, strings.TrimPrefix(base, "http://"), "/metrics", metricsCanary)
	if r.StatusCode != 200 || !strings.Contains(body, "inference_requests_active") || !strings.Contains(body, "serverflow_build_info") {
		t.Fatalf("right token: %d\n%.400s", r.StatusCode, body)
	}
	if r, _ := authGet(t, gwAddr, "/metrics", metricsCanary); r.StatusCode != http.StatusNotFound {
		t.Fatalf("the data port must not serve /metrics, got %d", r.StatusCode)
	}
	if strings.Contains(body, metricsCanary) || strings.Contains(gw.stderr.String(), metricsCanary) {
		t.Fatal("the metrics token reached the metrics output or the process log")
	}
	if !strings.Contains(gw.stderr.String(), `"token_required":true`) {
		t.Fatalf("the log should say a token is required:\n%s", gw.stderr.String())
	}
}

func TestProcessMetricsListenerCanBeSwitchedOff(t *testing.T) {
	mockAddr := freePort(t)
	startMockProc(t, mockAddr)
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	gw := startProc(t, "gateway", "gateway starting", []string{
		"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model, "SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + mockAddr,
		"SERVERFLOW_METRICS_LISTEN=",
	})
	waitFor(t, 10*time.Second, "the gateway to say its metrics endpoint is off", func() bool {
		return strings.Contains(gw.stderr.String(), "metrics endpoint is off")
	})
	if r, _ := authGet(t, gwAddr, "/metrics", ""); r.StatusCode != http.StatusNotFound {
		t.Fatalf("/metrics with the endpoint off: %d", r.StatusCode)
	}
}

func TestProcessControlPlaneServesMetricsOnItsOwnListener(t *testing.T) {
	cp := startProc(t, "control-plane", "control-plane starting", []string{"SERVERFLOW_CONTROL_PLANE_ADDR=127.0.0.1:0"})
	base := strings.TrimPrefix(cp.metricsURL(t), "http://")
	r, body := authGet(t, base, "/metrics", "")
	if r.StatusCode != 200 || !strings.Contains(body, "registry_registrations_total") || !strings.Contains(body, "registry_heartbeats_total") {
		t.Fatalf("%d\n%.400s", r.StatusCode, body)
	}
	if r, _ := authGet(t, cp.addr, "/metrics", ""); r.StatusCode == 200 {
		t.Fatal("the control plane API port must not serve /metrics")
	}
}
