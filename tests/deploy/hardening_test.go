package deploy

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
)

func dur(t *testing.T, v any) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(fmt.Sprint(v))
	if err != nil {
		t.Fatalf("bad duration %v: %v", v, err)
	}
	return d
}

// TestNoServiceRunsAsRoot: nothing sets user 0 or root. (The image users are checked on the running containers by
// `make dev-audit`; ServerFlow images get 65532 from the Dockerfile, so they must not override it at all.)
func TestNoServiceRunsAsRoot(t *testing.T) {
	for name, s := range services(t, "docker-compose.yml") {
		u, set := s["user"]
		if !set {
			continue
		}
		uid := strings.SplitN(fmt.Sprint(u), ":", 2)[0]
		if uid == "0" || uid == "root" || uid == "" {
			t.Errorf("%s sets user %v (root)", name, u)
		}
		if strings.HasPrefix(fmt.Sprint(s["image"]), "serverflow/") {
			t.Errorf("%s overrides the image user %v; the Dockerfile sets 65532", name, u)
		}
	}
	for _, f := range composeFiles[1:] {
		for name, s := range services(t, f) {
			if _, ok := s["user"]; ok {
				t.Errorf("%s: %s sets a user in an override file", f, name)
			}
		}
	}
}

// TestRestartPolicies: long-running services restart only on failure (a `kill` or `stop` must stay down: the chaos
// operations rely on it) and jobs never restart.
func TestRestartPolicies(t *testing.T) {
	jobs := map[string]bool{"migrate": true, "redpanda-init": true, "admin": true}
	for name, s := range services(t, "docker-compose.yml") {
		want := "on-failure:5"
		if jobs[name] {
			want = "no"
		}
		if got := fmt.Sprint(s["restart"]); got != want {
			t.Errorf("%s restart = %q, want %q", name, got, want)
		}
	}
}

func TestGrafanaIsNotAnonymous(t *testing.T) {
	env := asMap(t, services(t, "docker-compose.yml")["grafana"]["environment"], "grafana environment")
	if fmt.Sprint(env["GF_AUTH_ANONYMOUS_ENABLED"]) != "false" {
		t.Errorf("GF_AUTH_ANONYMOUS_ENABLED = %v, must be false", env["GF_AUTH_ANONYMOUS_ENABLED"])
	}
	if fmt.Sprint(env["GF_USERS_ALLOW_SIGN_UP"]) != "false" {
		t.Error("sign-up must be off")
	}
}

// TestStopGraceCoversShutdown: the gateway drains in-flight requests for shutdown_timeout and then flushes events;
// Docker must wait longer than both before it sends SIGKILL.
func TestStopGraceCoversShutdown(t *testing.T) {
	d := config.Default()
	need := d.Gateway.ShutdownTimeout + d.Events.ShutdownFlushTimeout
	got := dur(t, services(t, "docker-compose.yml")["gateway"]["stop_grace_period"])
	if got <= need {
		t.Errorf("gateway stop_grace_period %v must exceed shutdown_timeout + events flush = %v", got, need)
	}
	for name, s := range services(t, "docker-compose.yml") {
		if g := dur(t, s["stop_grace_period"]); g < 10*time.Second {
			t.Errorf("%s stop_grace_period %v is too short", name, g)
		}
	}
}

// TestRedisPasswordNotOnTheCommandLine: the password may be written to a tmpfs config file by the shell, never
// passed to redis-server as an argument (visible in `docker top` and `ps`).
func TestRedisPasswordNotOnTheCommandLine(t *testing.T) {
	cmd := strings.Join(strList(services(t, "docker-compose.yml")["redis"]["command"]), " ")
	if strings.Contains(cmd, "--requirepass") || strings.Contains(cmd, "-a ") {
		t.Error("redis must not take the password as an argument")
	}
	i := strings.Index(cmd, "exec redis-server")
	if i < 0 {
		t.Fatal("redis command must exec redis-server")
	}
	if strings.Contains(cmd[i:], "PASSWORD") {
		t.Error("the password variable appears in the redis-server argument list")
	}
}

// TestHealthchecksAreBoundedAndAimedAtTheService derives each probe's port and path from the service's own settings.
func TestHealthchecksAreBoundedAndAimedAtTheService(t *testing.T) {
	svcs := services(t, "docker-compose.yml")
	d := config.Default()
	_, consumerPort, _ := strings.Cut(d.Events.Consumer.MetricsAddr, ":")
	want := map[string]string{
		"gateway":        fmt.Sprintf("http://127.0.0.1:%d/healthz", d.Gateway.Port),
		"usage-consumer": "http://127.0.0.1:" + consumerPort + "/healthz",
	}
	cpAddr := fmt.Sprint(asMap(t, svcs["control-plane"]["environment"], "cp env")["SERVERFLOW_CONTROL_PLANE_ADDR"])
	want["control-plane"] = "http://127.0.0.1:" + cpAddr[strings.LastIndex(cpAddr, ":")+1:] + "/healthz"
	for _, n := range []string{"1", "2", "3"} {
		// The agents' status endpoint (ADR-022 D6): readiness is registration, so a healthy agent is a registered one.
		agentEnv := asMap(t, svcs["worker-agent-"+n]["environment"], "agent env")
		listen := fmt.Sprint(agentEnv["SERVERFLOW_WORKER_STATUS_LISTEN"])
		if !strings.HasPrefix(listen, "127.0.0.1:") {
			t.Errorf("worker-agent-%s must enable its status endpoint on a loopback address, got %q", n, listen)
		}
		want["worker-agent-"+n] = "http://" + listen + "/readyz"
		var port string
		for _, a := range strList(svcs["mock-worker-"+n]["command"]) {
			if v, ok := strings.CutPrefix(a, "--addr="); ok {
				port = v[strings.LastIndex(v, ":")+1:]
			}
		}
		want["mock-worker-"+n] = "http://127.0.0.1:" + port + "/readyz"
	}
	for name, s := range svcs {
		hc, ok := s["healthcheck"].(map[string]any)
		if !ok || hc["disable"] == true {
			continue
		}
		if to := dur(t, hc["timeout"]); to <= 0 || to > 10*time.Second {
			t.Errorf("%s healthcheck timeout %v must be in (0, 10s]", name, to)
		}
		if iv := dur(t, hc["interval"]); iv <= 0 || iv > 30*time.Second {
			t.Errorf("%s healthcheck interval %v must be in (0, 30s]", name, iv)
		}
		if r, ok := hc["retries"].(int); !ok || r < 1 || r > 20 {
			t.Errorf("%s healthcheck retries %v out of range", name, hc["retries"])
		}
		test := strList(hc["test"])
		if url, ok := want[name]; ok {
			if len(test) != 3 || test[0] != "CMD" || test[1] != "/usr/local/bin/healthcheck" || test[2] != url {
				t.Errorf("%s healthcheck = %v, want the healthcheck binary on %s", name, test, url)
			}
		}
	}
	for name := range want {
		if _, ok := svcs[name]; !ok {
			t.Errorf("no service %s", name)
		}
	}
}

// TestHealthcheckBinaryIsInEveryImageThatUsesIt: distroless has no curl, so each ServerFlow image whose compose
// healthcheck runs /usr/local/bin/healthcheck must be built FROM the runtime stage, which copies it in.
func TestHealthcheckBinaryIsInEveryImageThatUsesIt(t *testing.T) {
	df := stripComments(read(t, "deploy/docker/Dockerfile"))
	if !regexp.MustCompile(`(?s)AS runtime.*?COPY --from=build /out/healthcheck /usr/local/bin/healthcheck`).MatchString(df) {
		t.Fatal("the runtime stage must copy /out/healthcheck to /usr/local/bin/healthcheck")
	}
	for name, s := range services(t, "docker-compose.yml") {
		hc, ok := s["healthcheck"].(map[string]any)
		if !ok || !strings.Contains(strings.Join(strList(hc["test"]), " "), "/usr/local/bin/healthcheck") {
			continue
		}
		b, ok := s["build"].(map[string]any)
		if !ok {
			t.Errorf("%s uses the healthcheck binary but is not built from our Dockerfile", name)
			continue
		}
		target := fmt.Sprint(b["target"])
		if !regexp.MustCompile(`(?m)^FROM runtime AS ` + regexp.QuoteMeta(target) + `\s*$`).MatchString(df) {
			t.Errorf("%s: Dockerfile target %s is not built FROM runtime, so it lacks the healthcheck binary", name, target)
		}
	}
}

// TestTraceInsecureFlagOnlyBesideATracingEndpoint: --trace-insecure-ok relaxes the plaintext check for the OTLP endpoint,
// so a worker may carry it only together with an endpoint that is empty unless the tracing profile sets it.
func TestTraceInsecureFlagOnlyBesideATracingEndpoint(t *testing.T) {
	for name, s := range services(t, "docker-compose.yml") {
		cmd := strList(s["command"])
		if !contains(cmd, "--trace-insecure-ok") {
			continue
		}
		var ep string
		for _, a := range cmd {
			if v, ok := strings.CutPrefix(a, "--otlp-endpoint="); ok {
				ep = v
			}
		}
		if ep != "${SF_OTLP_ENDPOINT:-}" {
			t.Errorf("%s: --trace-insecure-ok without --otlp-endpoint=${SF_OTLP_ENDPOINT:-} (got %q): tracing would not be tied to the profile", name, ep)
		}
	}
}
