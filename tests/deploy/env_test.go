package deploy

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"serverflow/internal/config"
	"serverflow/internal/mockworker"
)

// dummy values for the variables compose reads from .dev/*.env (the generator's names; the values are fixed test strings).
var secretEnv = map[string]string{
	"POSTGRES_PASSWORD":               "x",
	"SERVERFLOW_POSTGRES_DSN":         "postgres://serverflow:pw@postgres:5432/serverflow?sslmode=disable",
	"SERVERFLOW_REDIS_PASSWORD":       "test-redis-password",
	"SERVERFLOW_EVENTS_SASL_PASSWORD": "test-kafka-password",
	"SERVERFLOW_CONTROL_PLANE_TOKEN":  "0123456789abcdef0123456789abcdef",
	"SERVERFLOW_METRICS_TOKEN":        "fedcba9876543210fedcba9876543210",
	"GF_SECURITY_ADMIN_PASSWORD":      "x",
}

// Variables whose compose value equals the default and so cannot change the configuration (each entry needs a reason).
var equalsDefault = map[string]string{
	"SERVERFLOW_REDIS_ADDRESS":        "the default address is the service name redis:6379; compose pins it so a changed default cannot silently break the stack",
	"SERVERFLOW_TRACING_SAMPLE_RATIO": "compose passes TRACING_SAMPLE_RATIO from .env; its default 1.0 equals the config default",
	"SERVERFLOW_SCHEDULER_STRATEGY":   "SF_STRATEGY from .env; its default round-robin equals the config default",
	"SERVERFLOW_RESILIENCE_MODE":      "SF_RESILIENCE_MODE from .env; its default on equals the config default (the compose line makes the kill switch visible)",
}

func clearServerflowEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "SERVERFLOW_") {
			old := os.Getenv(k)
			_ = os.Unsetenv(k)
			t.Cleanup(func() { _ = os.Setenv(k, old) })
		}
	}
}

// TestEnvVariablesAreHonoured: every SERVERFLOW_* variable a compose service sets must exist in internal/config's
// environment overlay and change the loaded configuration (an unknown name is silently ignored by the binaries,
// which would make a typo in the compose file a security or correctness bug). It also pins W1-W3 of Phase 16.
func TestEnvVariablesAreHonoured(t *testing.T) {
	for name, s := range services(t, "docker-compose.yml") {
		env, _ := s["environment"].(map[string]any)
		var keys []string
		for k := range env {
			if strings.HasPrefix(k, "SERVERFLOW_") {
				keys = append(keys, k)
			}
		}
		if len(keys) == 0 {
			continue
		}
		t.Run(name, func(t *testing.T) {
			load := func(skip string) (config.Config, error) {
				clearServerflowEnv(t)
				for k, v := range secretEnv {
					t.Setenv(k, v)
				}
				for _, k := range keys {
					if k != skip {
						t.Setenv(k, expand(fmt.Sprint(env[k])))
					}
				}
				// Mode switches the profiles flip: the full-profile values are what is checked.
				return config.Load("")
			}
			full, err := load("")
			if err != nil {
				t.Fatalf("the service's environment does not produce a valid configuration: %v", err)
			}
			for _, k := range keys {
				without, err := load(k)
				if err != nil {
					continue // removing it makes the configuration invalid: the variable is honoured
				}
				if reflect.DeepEqual(full, without) {
					if _, ok := equalsDefault[k]; !ok {
						t.Errorf("%s has no effect on the loaded configuration: the name is wrong or the value equals the default", k)
					}
				}
			}
		})
	}
}

// TestMockWorkerCommandsParse: the flag lists of the mock workers parse and validate with the binary's own parser,
// for the defaults and for every documented failure mode.
func TestMockWorkerCommandsParse(t *testing.T) {
	for _, n := range []string{"1", "2", "3"} {
		cmd := strList(services(t, "docker-compose.yml")["mock-worker-"+n]["command"])
		for _, mode := range []string{"error", "unavailable", "drop", "midstream"} {
			var args []string
			for _, a := range cmd {
				a = interp.ReplaceAllStringFunc(a, func(m string) string {
					sub := interp.FindStringSubmatch(m)
					if strings.HasSuffix(sub[1], "FAILURE_MODE") {
						return mode
					}
					if strings.HasSuffix(sub[1], "FAILURE_RATE") {
						return "0.5"
					}
					return sub[2]
				})
				args = append(args, a)
			}
			cfg, _, err := mockworker.ParseFlags(args, os.Stderr)
			if err != nil {
				t.Fatalf("mock-worker-%s (%s): %v", n, mode, err)
			}
			if cfg.WorkerID != "mock-"+n || cfg.Addr != "0.0.0.0:9000" {
				t.Errorf("mock-worker-%s: unexpected worker id %q or address %q", n, cfg.WorkerID, cfg.Addr)
			}
		}
	}
}

// TestControlPlaneClientsOptIntoPlainHTTP pins W1 on the client side: the gateway and every agent reach the control
// plane over the private network in plain http, which their guard refuses unless control_plane.allow_insecure_transport
// is set. (Load does not validate an agent's configuration, so TestEnvVariablesAreHonoured cannot notice the flag
// missing from an agent.) The control plane itself must not set it: its own rule (a token off loopback) is separate.
func TestControlPlaneClientsOptIntoPlainHTTP(t *testing.T) {
	svcs := services(t, "docker-compose.yml")
	const flag = "SERVERFLOW_CONTROL_PLANE_ALLOW_INSECURE_TRANSPORT"
	for _, name := range []string{"gateway", "worker-agent-1", "worker-agent-2", "worker-agent-3"} {
		env := asMap(t, svcs[name]["environment"], name+" environment")
		if fmt.Sprint(env[flag]) != "true" {
			t.Errorf("%s must set %s=true to talk to the control plane over the private network", name, flag)
		}
		url := fmt.Sprint(env["SERVERFLOW_GATEWAY_CONTROL_PLANE_URL"])
		if name != "gateway" {
			url = fmt.Sprint(env["SERVERFLOW_WORKER_CONTROL_PLANE_URL"])
		}
		if url != "http://control-plane:9090" {
			t.Errorf("%s reaches the control plane at %q", name, url)
		}
	}
	for name, s := range svcs {
		if env, ok := s["environment"].(map[string]any); ok && name != "gateway" && !strings.HasPrefix(name, "worker-agent") {
			if _, set := env[flag]; set {
				t.Errorf("%s sets %s but is not a control-plane client", name, flag)
			}
		}
	}
}
