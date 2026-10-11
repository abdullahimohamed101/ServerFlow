package config

import (
	"os"
	"strings"
	"testing"
)

// The control-plane token travels in an Authorization header. control_plane.allow_insecure_transport relaxes the
// "no cleartext http:// to another host" guard of the two clients (the gateway and the worker agent) and nothing
// else. These tests pin the refusal and acceptance matrix, the environment variable and that no error echoes the
// token or the URL (ADR-021, security review notes in the Phase 16 plan).

const (
	cpToken  = "tok-0123456789abcdef-canary-secret"
	cpCanary = "cp-canary-host.internal"
)

func TestControlPlaneInsecureTransportDefaultsOff(t *testing.T) {
	if Default().ControlPlane.AllowInsecureTransport {
		t.Fatal("control_plane.allow_insecure_transport must default to false")
	}
}

func TestControlPlaneInsecureTransportMatrix(t *testing.T) {
	cases := []struct {
		name     string
		url      string
		token    string
		allow    bool
		wantFail bool
	}{
		{"http remote with token, default", "http://" + cpCanary + ":9090", cpToken, false, true},
		{"http remote with token, allowed", "http://" + cpCanary + ":9090", cpToken, true, false},
		{"https remote with token, default", "https://" + cpCanary + ":9090", cpToken, false, false},
		{"http loopback with token, default", "http://127.0.0.1:9090", cpToken, false, false},
		{"http remote without token, default", "http://" + cpCanary + ":9090", "", false, false},
		{"http remote with token, flag set but short token", "http://" + cpCanary + ":9090", "short", true, true},
	}
	for _, tc := range cases {
		t.Run("gateway "+tc.name, func(t *testing.T) {
			c := Default()
			c.Gateway.WorkerSource = WorkerSourceRegistry
			c.Gateway.ControlPlaneURL = tc.url
			c.ControlPlane.Token = tc.token
			c.ControlPlane.AllowInsecureTransport = tc.allow
			err := c.Validate()
			if (err != nil) != tc.wantFail {
				t.Fatalf("Validate() = %v, want failure=%v", err, tc.wantFail)
			}
			assertNoSecret(t, err)
		})
		t.Run("agent "+tc.name, func(t *testing.T) {
			c := Default()
			c.Worker.ID, c.Worker.Model = "w1", "m"
			c.Worker.BackendURL, c.Worker.AdvertiseURL = "http://127.0.0.1:9000", "http://127.0.0.1:9000"
			c.Worker.ControlPlaneURL = tc.url
			c.ControlPlane.Token = tc.token
			c.ControlPlane.AllowInsecureTransport = tc.allow
			err := c.ValidateAgent()
			// A short token is rejected by Validate (control_plane.validate), not by the agent guard.
			if tc.token == "short" {
				err = c.Validate()
			}
			if (err != nil) != tc.wantFail {
				t.Fatalf("validation = %v, want failure=%v", err, tc.wantFail)
			}
			assertNoSecret(t, err)
		})
	}
}

func assertNoSecret(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	for _, s := range []string{cpToken, cpCanary} {
		if strings.Contains(err.Error(), s) {
			t.Errorf("error echoes a secret or the URL: %v", err)
		}
	}
}

func TestControlPlaneInsecureTransportEnv(t *testing.T) {
	t.Setenv("SERVERFLOW_CONTROL_PLANE_ALLOW_INSECURE_TRANSPORT", "true")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ControlPlane.AllowInsecureTransport {
		t.Error("SERVERFLOW_CONTROL_PLANE_ALLOW_INSECURE_TRANSPORT=true was not applied")
	}
	for _, bad := range []string{"yes please", "", "2"} {
		t.Setenv("SERVERFLOW_CONTROL_PLANE_ALLOW_INSECURE_TRANSPORT", bad)
		cfg, err = Load("")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.ControlPlane.AllowInsecureTransport {
			t.Errorf("an unparsable value %q must leave the default (false)", bad)
		}
	}
}

func TestControlPlaneInsecureTransportYAML(t *testing.T) {
	path := t.TempDir() + "/c.yaml"
	if err := os.WriteFile(path, []byte("control_plane:\n  allow_insecure_transport: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ControlPlane.AllowInsecureTransport {
		t.Error("YAML key control_plane.allow_insecure_transport was not applied")
	}
}

func TestTracingAllowInsecureTransportEnv(t *testing.T) {
	t.Setenv("SERVERFLOW_TRACING_ENABLED", "true")
	t.Setenv("SERVERFLOW_TRACING_ENDPOINT", "http://jaeger:4318")
	if _, err := Load(""); err == nil {
		t.Fatal("plaintext http to a non-loopback collector must be refused by default")
	}
	t.Setenv("SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT", "true")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Tracing.AllowInsecureTransport {
		t.Fatal("SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT was not applied")
	}
}
