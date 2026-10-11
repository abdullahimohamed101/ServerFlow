package deploy

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func scrapeJobs(t *testing.T, file string) map[string]map[string]any {
	t.Helper()
	doc := readYAML(t, file)
	jobs := map[string]map[string]any{}
	for _, j := range asList(doc["scrape_configs"]) {
		m := asMap(t, j, "scrape job")
		jobs[fmt.Sprint(m["job_name"])] = m
	}
	return jobs
}

// TestPrometheusComposeMatchesLocal: the container stack's Prometheus configuration may differ from the host-process
// one only where documented (the bearer token, DNS discovery of the gateway replicas). Everything else (job names,
// global settings, rule files, sample limits, intervals and the relabelling of the other jobs) must be identical.
func TestPrometheusComposeMatchesLocal(t *testing.T) {
	local, compose := readYAML(t, "observability/prometheus/prometheus.yml"), readYAML(t, "observability/prometheus/prometheus.compose.yml")
	for _, key := range []string{"global", "rule_files"} {
		if !reflect.DeepEqual(local[key], compose[key]) {
			t.Errorf("%s differs between prometheus.yml and prometheus.compose.yml", key)
		}
	}
	lj, cj := scrapeJobs(t, "observability/prometheus/prometheus.yml"), scrapeJobs(t, "observability/prometheus/prometheus.compose.yml")
	if strings.Join(sortedKeys(lj), ",") != strings.Join(sortedKeys(cj), ",") {
		t.Fatalf("job names differ: %v vs %v", sortedKeys(lj), sortedKeys(cj))
	}
	for name, l := range lj {
		c := cj[name]
		for _, key := range []string{"sample_limit", "scrape_interval"} {
			if !reflect.DeepEqual(l[key], c[key]) {
				t.Errorf("%s: %s differs (%v vs %v)", name, key, l[key], c[key])
			}
		}
		if name != "serverflow-gateway" { // the gateway job discovers replicas by DNS and sets the component itself
			if !reflect.DeepEqual(l["relabel_configs"], c["relabel_configs"]) {
				t.Errorf("%s: relabel_configs differ", name)
			}
			if !reflect.DeepEqual(l["file_sd_configs"], c["file_sd_configs"]) {
				t.Errorf("%s: file_sd_configs differ", name)
			}
		}
		// Token-protected listeners present the token; the mock worker's endpoint is unauthenticated.
		_, hasAuth := c["authorization"]
		wantAuth := name == "serverflow-gateway" || name == "serverflow-control-plane"
		if hasAuth != wantAuth {
			t.Errorf("%s: authorization present = %v, want %v", name, hasAuth, wantAuth)
		}
		if hasAuth {
			a := asMap(t, c["authorization"], "authorization")
			if a["type"] != "Bearer" || a["credentials_file"] != "/etc/prometheus/secrets/metrics_token" || a["credentials"] != nil {
				t.Errorf("%s: the token must come from /etc/prometheus/secrets/metrics_token, never inline: %v", name, a)
			}
		}
	}
	gw := cj["serverflow-gateway"]
	dns := asMap(t, asList(gw["dns_sd_configs"])[0], "dns_sd_configs")
	if !reflect.DeepEqual(strList(dns["names"]), []string{"gateway"}) || dns["type"] != "A" || fmt.Sprint(dns["port"]) != "9100" {
		t.Errorf("the gateway job must discover the gateway service by DNS A records on 9100: %v", dns)
	}
}

// TestTargetsMatchComposeServices: every scrape target is a compose service, the ports match the listeners the
// services bind, and worker_id equals the agent's SERVERFLOW_WORKER_ID so worker and registry series join.
func TestTargetsMatchComposeServices(t *testing.T) {
	svcs := services(t, "docker-compose.yml")
	var targets []struct {
		Targets []string          `yaml:"targets"`
		Labels  map[string]string `yaml:"labels"`
	}
	if err := yaml.Unmarshal([]byte(read(t, "observability/prometheus/targets/compose.yml")), &targets); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 4 {
		t.Fatalf("expected the control plane and three mock workers, got %d groups", len(targets))
	}
	for _, g := range targets {
		for _, target := range g.Targets {
			host, portStr, _ := strings.Cut(target, ":")
			svc, ok := svcs[host]
			if !ok {
				t.Errorf("target %s is not a compose service", target)
				continue
			}
			port, _ := strconv.Atoi(portStr)
			switch g.Labels["component"] {
			case "control-plane":
				env := asMap(t, svc["environment"], "environment")
				if got := fmt.Sprint(env["SERVERFLOW_METRICS_LISTEN"]); got != fmt.Sprintf("0.0.0.0:%d", port) {
					t.Errorf("%s: scrape port %d but SERVERFLOW_METRICS_LISTEN=%s", target, port, got)
				}
			case "mock-worker":
				if !contains(strList(svc["command"]), fmt.Sprintf("--addr=0.0.0.0:%d", port)) {
					t.Errorf("%s: scrape port %d but the worker listens elsewhere", target, port)
				}
				agent := services(t, "docker-compose.yml")["worker-agent-"+strings.TrimPrefix(host, "mock-worker-")]
				env := asMap(t, agent["environment"], "agent environment")
				if env["SERVERFLOW_WORKER_ID"] != g.Labels["worker_id"] {
					t.Errorf("%s: worker_id label %q != agent SERVERFLOW_WORKER_ID %v", host, g.Labels["worker_id"], env["SERVERFLOW_WORKER_ID"])
				}
				if !contains(strList(svc["command"]), "--worker-id="+g.Labels["worker_id"]) {
					t.Errorf("%s: --worker-id does not match the worker_id label %q", host, g.Labels["worker_id"])
				}
			default:
				t.Errorf("unexpected component %q", g.Labels["component"])
			}
		}
	}
	gwEnv := asMap(t, svcs["gateway"]["environment"], "gateway environment")
	if gwEnv["SERVERFLOW_METRICS_LISTEN"] != "0.0.0.0:9100" {
		t.Errorf("the gateway job scrapes :9100 but SERVERFLOW_METRICS_LISTEN=%v", gwEnv["SERVERFLOW_METRICS_LISTEN"])
	}
	// Only the compose targets file and the rules are mounted: host.docker.internal targets never appear.
	prom := svcs["prometheus"]
	for _, v := range strList(prom["volumes"]) {
		if strings.Contains(v, "targets/local.yml") || strings.HasPrefix(v, "./observability/prometheus:") {
			t.Errorf("prometheus mounts %q; only targets/compose.yml and rules/ belong in the container stack", v)
		}
	}
}
