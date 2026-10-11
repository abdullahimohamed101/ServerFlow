package deploy

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Compose files that make up the stack. The override files are merged by scripts/dev-up.sh depending on .env;
// every one of them is checked on its own and the base file is checked as a whole.
var composeFiles = []string{
	"docker-compose.yml",
	"docker-compose.persist.yml",
	"docker-compose.scale.yml",
	"docker-compose.private-control-plane.yml",
}

// Services that may publish a port, and only on 127.0.0.1.
var mayPublish = map[string]bool{"gateway": true, "lb": true, "control-plane": true, "prometheus": true, "grafana": true, "jaeger": true}

// Services without a compose health check, with the reason (ADR-021). Anything else must have one.
var noHealthcheck = map[string]string{
	"worker-agent-1": "the agent has no HTTP endpoint; restart on-failure covers crashes and dev-smoke checks heartbeats",
	"worker-agent-2": "same as worker-agent-1",
	"worker-agent-3": "same as worker-agent-1",
	"migrate":        "one-shot job",
	"redpanda-init":  "one-shot job",
	"admin":          "run on demand",
}

// Image volumes that must be covered by a tmpfs or a named volume so no anonymous volume is ever created.
var volumePaths = map[string][]string{
	"postgres":   {"/var/lib/postgresql/data"},
	"redis":      {"/data"},
	"redpanda":   {"/var/lib/redpanda/data"},
	"prometheus": {"/prometheus"},
	"jaeger":     {"/tmp"},
}

var (
	interp        = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::?-([^}]*))?\}`)
	secretName    = regexp.MustCompile(`(?i)PASSWORD|TOKEN|SECRET|KEY|DSN|CREDENTIAL`)
	pinnedImageRe = regexp.MustCompile(`^[a-z0-9.-]+(:[0-9]+)?/[a-z0-9._/-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$`)
)

// expand replaces ${VAR:-default} with its default (and ${VAR} with ""), which is what compose does when the
// variable is unset.
func expand(s string) string {
	return interp.ReplaceAllStringFunc(s, func(m string) string {
		sub := interp.FindStringSubmatch(m)
		return sub[2]
	})
}

func asMap(t testing.TB, v any, what string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("%s: expected a mapping, got %T", what, v)
	}
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func services(t testing.TB, file string) map[string]map[string]any {
	t.Helper()
	doc := readYAML(t, file)
	out := map[string]map[string]any{}
	for name, v := range asMap(t, doc["services"], file+" services") {
		out[name] = asMap(t, v, file+" service "+name)
	}
	return out
}

func strList(v any) []string {
	var out []string
	for _, e := range asList(v) {
		out = append(out, fmt.Sprint(e))
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	var k []string
	for n := range m {
		k = append(k, n)
	}
	sort.Strings(k)
	return k
}

// TestComposeStructure pins what ADR-021 promises about every container.
func TestComposeStructure(t *testing.T) {
	svcs := services(t, "docker-compose.yml")
	want := []string{"admin", "control-plane", "gateway", "grafana", "jaeger", "lb", "migrate", "mock-worker-1", "mock-worker-2", "mock-worker-3",
		"postgres", "prometheus", "redis", "redpanda", "redpanda-init", "usage-consumer", "worker-agent-1", "worker-agent-2", "worker-agent-3"}
	if got := sortedKeys(svcs); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("services = %v, want %v (a new service needs hardening, a health check and a place in this list)", got, want)
	}
	for _, name := range sortedKeys(svcs) {
		s := svcs[name]
		t.Run(name, func(t *testing.T) {
			if s["read_only"] != true {
				t.Error("read_only must be true (no exceptions: mount a tmpfs for what must be writable)")
			}
			if got := strList(s["cap_drop"]); len(got) != 1 || got[0] != "ALL" {
				t.Errorf("cap_drop must be [ALL], got %v", got)
			}
			if !contains(strList(s["security_opt"]), "no-new-privileges:true") {
				t.Error("security_opt must contain no-new-privileges:true")
			}
			if _, ok := s["cap_add"]; ok {
				t.Error("cap_add is not allowed")
			}
			for _, banned := range []string{"privileged", "network_mode", "container_name", "pid", "ipc", "devices", "userns_mode", "sysctls"} {
				if _, ok := s[banned]; ok {
					t.Errorf("%s is not allowed", banned)
				}
			}
			if s["mem_limit"] == nil {
				t.Error("mem_limit is required")
			}
			if s["pids_limit"] == nil {
				t.Error("pids_limit is required")
			}
			if s["restart"] == nil {
				t.Error("restart policy is required")
			}
			if log, ok := s["logging"].(map[string]any); !ok || log["driver"] != "json-file" || asMap(t, log["options"], "logging options")["max-size"] == nil || asMap(t, log["options"], "logging options")["max-file"] == nil {
				t.Error("logging must be json-file with max-size and max-file")
			}
			nets := strList(s["networks"])
			if !contains(nets, "backend") {
				t.Errorf("must be on the backend network, got %v", nets)
			}
			for _, n := range nets {
				if n != "backend" && n != "edge" {
					t.Errorf("unknown network %s", n)
				}
			}

			hc, hasHC := s["healthcheck"].(map[string]any)
			if reason, ok := noHealthcheck[name]; ok {
				if !hasHC || hc["disable"] != true {
					t.Errorf("%s is on the no-healthcheck list (%s) and must say healthcheck: {disable: true}", name, reason)
				}
			} else if !hasHC || hc["test"] == nil || hc["disable"] == true {
				t.Error("a healthcheck is required (add the service to noHealthcheck with a reason if it cannot have one)")
			}

			// Ports: loopback only, and only the services meant to publish.
			ports := asList(s["ports"])
			if len(ports) > 0 && !mayPublish[name] {
				t.Errorf("%s must not publish a port", name)
			}
			for _, p := range ports {
				if ps, ok := p.(string); !ok || !strings.HasPrefix(ps, "127.0.0.1:") {
					t.Errorf("published port %v must be bound to 127.0.0.1", p)
				}
			}
			if len(ports) > 0 && !contains(nets, "edge") {
				t.Error("a service that publishes a port must be on the edge network")
			}
			if len(ports) == 0 && contains(nets, "edge") {
				t.Error("only services that publish a port join the edge network")
			}

			// depends_on needs a condition on every edge, and may only name real services.
			if dep, ok := s["depends_on"]; ok {
				for dn, dv := range asMap(t, dep, "depends_on (must be the long form with conditions)") {
					if _, exists := svcs[dn]; !exists {
						t.Errorf("depends_on names unknown service %s", dn)
					}
					if asMap(t, dv, "depends_on."+dn)["condition"] == nil {
						t.Errorf("depends_on %s has no condition", dn)
					}
				}
			}

			// Image: our own (built from the Dockerfile) or pinned by digest with an explicit non-Docker-Hub host.
			img := fmt.Sprint(s["image"])
			if strings.HasPrefix(img, "serverflow/") {
				if s["build"] == nil {
					t.Error("a serverflow image must have a build section")
				}
			} else {
				ref := expand(img)
				if !pinnedImageRe.MatchString(ref) {
					t.Errorf("image %q must be host/path:tag@sha256:digest", ref)
				}
				for _, hub := range []string{"docker.io/", "index.docker.io/", "registry-1.docker.io/"} {
					if strings.HasPrefix(ref, hub) {
						t.Errorf("image %q comes from Docker Hub", ref)
					}
				}
				if strings.Contains(ref, ":latest") {
					t.Errorf("image %q uses :latest", ref)
				}
			}

			// Secrets reach containers through env_file under .dev/ only.
			for _, f := range envFiles(s["env_file"]) {
				if !strings.HasPrefix(f, ".dev/") {
					t.Errorf("env_file %q must live under .dev/", f)
				}
			}
			if env, ok := s["environment"].(map[string]any); ok {
				for k, v := range env {
					vs := fmt.Sprint(v)
					if secretName.MatchString(k) && vs != "" {
						t.Errorf("environment %s looks like a secret: use env_file", k)
					}
					for _, m := range interp.FindAllStringSubmatch(vs, -1) {
						if secretName.MatchString(m[1]) {
							t.Errorf("environment %s interpolates %s, which docker compose config would print; use env_file", k, m[1])
						}
					}
				}
			}

			// Volumes: read-only bind mounts of repository files, and no anonymous volumes.
			for _, v := range strList(s["volumes"]) {
				parts := strings.Split(v, ":")
				if strings.Contains(v, "docker.sock") {
					t.Errorf("volume %q mounts the Docker socket", v)
				}
				if len(parts) < 2 {
					t.Errorf("volume %q is anonymous", v)
					continue
				}
				if strings.HasPrefix(parts[0], "./") && parts[len(parts)-1] != "ro" {
					t.Errorf("bind mount %q must be read-only", v)
				}
				if strings.HasPrefix(parts[0], "/") || strings.HasPrefix(parts[0], "~") || strings.HasPrefix(parts[0], "..") {
					t.Errorf("bind mount %q must be a path inside the repository", v)
				}
			}
		})
	}

	// Every image volume path of a service is a tmpfs here or a named volume in the persist file.
	persist := services(t, "docker-compose.persist.yml")
	for svc, paths := range volumePaths {
		for _, p := range paths {
			if !mountsPath(svcs[svc], p) {
				t.Errorf("%s: %s is not covered by a tmpfs (an anonymous volume would be created)", svc, p)
			}
			if svc != "jaeger" && !mountsPath(persist[svc], p) {
				t.Errorf("persist file: %s %s is not mounted", svc, p)
			}
		}
	}

	// Networks: backend is internal, so data stores and workers cannot reach the internet.
	nets := asMap(t, readYAML(t, "docker-compose.yml")["networks"], "networks")
	if be := asMap(t, nets["backend"], "backend"); be["internal"] != true {
		t.Error("the backend network must be internal: true")
	}
	if name := readYAML(t, "docker-compose.yml")["name"]; name != "serverflow-dev" {
		t.Errorf("project name = %v, want serverflow-dev", name)
	}
}

func contains(l []string, s string) bool {
	for _, e := range l {
		if e == s {
			return true
		}
	}
	return false
}

func envFiles(v any) []string {
	var out []string
	switch x := v.(type) {
	case string:
		out = append(out, x)
	case []any:
		for _, e := range x {
			if m, ok := e.(map[string]any); ok {
				out = append(out, fmt.Sprint(m["path"]))
			} else {
				out = append(out, fmt.Sprint(e))
			}
		}
	}
	return out
}

// mountsPath reports whether a service mounts p as tmpfs or as a (named) volume.
func mountsPath(s map[string]any, p string) bool {
	for _, t := range strList(s["tmpfs"]) {
		if strings.SplitN(t, ":", 2)[0] == p {
			return true
		}
	}
	for _, v := range strList(s["volumes"]) {
		parts := strings.Split(v, ":")
		if len(parts) >= 2 && parts[1] == p {
			return true
		}
	}
	return false
}

// TestOverrideFilesOnlyNarrow: the override files must not weaken the base (no new ports, privileges, mounts of
// the host) and must name existing services.
func TestOverrideFilesOnlyNarrow(t *testing.T) {
	base := services(t, "docker-compose.yml")
	for _, f := range composeFiles[1:] {
		for name, s := range services(t, f) {
			if _, ok := base[name]; !ok {
				t.Errorf("%s overrides unknown service %s", f, name)
			}
			for _, banned := range []string{"privileged", "cap_add", "network_mode", "read_only", "security_opt", "cap_drop", "image", "environment", "env_file", "command"} {
				if _, ok := s[banned]; ok {
					t.Errorf("%s: %s sets %s; overrides may only change ports, mounts and networks", f, name, banned)
				}
			}
			for _, p := range asList(s["ports"]) {
				if !strings.HasPrefix(fmt.Sprint(p), "127.0.0.1:") {
					t.Errorf("%s: port %v is not loopback", f, p)
				}
			}
		}
	}
	// The persist file names every volume it uses.
	pf := readYAML(t, "docker-compose.persist.yml")
	declared := asMap(t, pf["volumes"], "persist volumes")
	for _, s := range services(t, "docker-compose.persist.yml") {
		for _, v := range strList(s["volumes"]) {
			name := strings.Split(v, ":")[0]
			if _, ok := declared[name]; !ok && !strings.HasPrefix(name, "./") {
				t.Errorf("volume %s is not declared in the persist file", name)
			}
		}
	}
}

// TestStartupOrdering pins the dependency edges of ADR-021 D12: removing one is a failing test, not a flaky start.
func TestStartupOrdering(t *testing.T) {
	svcs := services(t, "docker-compose.yml")
	edges := []struct{ from, to, cond string }{
		{"migrate", "postgres", "service_healthy"},
		{"gateway", "control-plane", "service_healthy"},
		{"gateway", "migrate", "service_completed_successfully"},
		{"gateway", "redis", "service_healthy"},
		{"gateway", "redpanda-init", "service_completed_successfully"},
		{"redpanda-init", "redpanda", "service_healthy"},
		{"usage-consumer", "migrate", "service_completed_successfully"},
		{"usage-consumer", "redpanda-init", "service_completed_successfully"},
		{"worker-agent-1", "mock-worker-1", "service_healthy"},
		{"worker-agent-2", "mock-worker-2", "service_healthy"},
		{"worker-agent-3", "mock-worker-3", "service_healthy"},
		{"worker-agent-1", "control-plane", "service_healthy"},
		{"worker-agent-2", "control-plane", "service_healthy"},
		{"worker-agent-3", "control-plane", "service_healthy"},
		{"lb", "gateway", "service_healthy"},
	}
	for _, e := range edges {
		dep := asMap(t, svcs[e.from]["depends_on"], e.from+" depends_on")
		d, ok := dep[e.to]
		if !ok {
			t.Errorf("%s must depend on %s", e.from, e.to)
			continue
		}
		if got := asMap(t, d, e.to)["condition"]; got != e.cond {
			t.Errorf("%s -> %s condition = %v, want %s", e.from, e.to, got, e.cond)
		}
	}
	// Optional services in other profiles must be optional dependencies so a smaller profile set still starts.
	for _, from := range []string{"gateway"} {
		for _, to := range []string{"migrate", "redis", "redpanda-init"} {
			d := asMap(t, asMap(t, svcs[from]["depends_on"], "depends_on")[to], to)
			if d["required"] != false {
				t.Errorf("%s -> %s must say required: false (the profile is optional)", from, to)
			}
		}
	}
	// Nothing may depend on observability or tracing: metrics and traces never gate requests.
	for name, s := range svcs {
		for dn := range asMap(t, orEmpty(s["depends_on"]), "depends_on") {
			if dn == "prometheus" || dn == "grafana" || dn == "jaeger" {
				t.Errorf("%s depends on %s: observability must never gate the system", name, dn)
			}
		}
	}
}

func orEmpty(v any) any {
	if v == nil {
		return map[string]any{}
	}
	return v
}
