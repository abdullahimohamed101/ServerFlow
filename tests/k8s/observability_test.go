package k8s

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The tests of PR 4 (Prometheus, Grafana, Jaeger): the ConfigMaps are the repository's own observability files (no fork), the
// Kubernetes Prometheus configuration does not drift from the host one, the one API credential of the namespace belongs to
// Prometheus alone and can only read pods, tracing points at the Jaeger Service, and the smoke checks K8, K9 and K11 judge
// canned answers correctly (and fail when an answer is wrong).

func yamlFile(t testing.TB, rel string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := yaml.Unmarshal([]byte(read(t, rel)), &m); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return m
}

func listOf(v any) []any { l, _ := v.([]any); return l }

func mapOf(v any) map[string]any { m, _ := v.(map[string]any); return m }

func jobs(t testing.TB, file string) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, j := range listOf(yamlFile(t, file)["scrape_configs"]) {
		m := mapOf(j)
		out[asStr(m["job_name"])] = m
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// k8sPromViolations: the Kubernetes Prometheus configuration against the host one and the manifests.
func k8sPromViolations(local, k8s map[string]any, lj, kj map[string]map[string]any, token string) []string {
	var v []string
	for _, key := range []string{"global", "rule_files"} {
		if !reflect.DeepEqual(local[key], k8s[key]) {
			v = append(v, key+" differs between prometheus.yml and kubernetes/prometheus.yml")
		}
	}
	if strings.Join(sortedKeys(lj), ",") != strings.Join(sortedKeys(kj), ",") {
		return append(v, fmt.Sprintf("job names differ: %v vs %v", sortedKeys(lj), sortedKeys(kj)))
	}
	for name, l := range lj {
		k := kj[name]
		for _, key := range []string{"sample_limit", "scrape_interval"} {
			if !reflect.DeepEqual(l[key], k[key]) {
				v = append(v, fmt.Sprintf("%s: %s differs (%v vs %v)", name, key, l[key], k[key]))
			}
		}
		if len(listOf(k["kubernetes_sd_configs"])) != 1 {
			v = append(v, name+": needs exactly one kubernetes_sd_configs entry")
		}
		for _, sd := range listOf(k["kubernetes_sd_configs"]) {
			ns := listOf(at(sd, "namespaces", "names"))
			if asStr(mapOf(sd)["role"]) != "pod" || len(ns) != 1 || ns[0] != "serverflow" {
				v = append(v, name+": discovery must be role pod in the namespace serverflow only")
			}
		}
		// The host config keeps the component by label; here it is the pod's component label and the named container port.
		wantComp := strings.TrimPrefix(name, "serverflow-")
		kept := false
		for _, r := range listOf(k["relabel_configs"]) {
			if asStr(mapOf(r)["action"]) == "keep" && strings.HasPrefix(asStr(mapOf(r)["regex"]), wantComp+";") {
				kept = true
			}
		}
		if !kept {
			v = append(v, name+": no keep rule for the component "+wantComp)
		}
		_, hasAuth := k["authorization"]
		wantAuth := name == "serverflow-gateway" || name == "serverflow-control-plane"
		if hasAuth != wantAuth {
			v = append(v, fmt.Sprintf("%s: authorization present = %v, want %v", name, hasAuth, wantAuth))
		}
		if hasAuth {
			a := mapOf(k["authorization"])
			if a["type"] != "Bearer" || a["credentials_file"] != token || a["credentials"] != nil {
				v = append(v, name+": the token must be read from "+token+" and never be inline")
			}
		}
	}
	// worker_id is the pod name: the agents register with SERVERFLOW_WORKER_ID = pod name.
	found := false
	for _, r := range listOf(kj["serverflow-mock-worker"]["relabel_configs"]) {
		m := mapOf(r)
		found = found || (m["target_label"] == "worker_id" && reflect.DeepEqual(m["source_labels"], []any{"__meta_kubernetes_pod_name"}))
	}
	if !found {
		v = append(v, "serverflow-mock-worker: worker_id must be the pod name")
	}
	return v
}

func TestPrometheusKubernetesConfigMatchesTheOthers(t *testing.T) {
	const cfg = "observability/prometheus/kubernetes/prometheus.yml"
	local, k8s := yamlFile(t, "observability/prometheus/prometheus.yml"), yamlFile(t, cfg)
	lj, kj := jobs(t, "observability/prometheus/prometheus.yml"), jobs(t, cfg)
	// The compose configuration is the third sibling: the job names are one set.
	if cj := jobs(t, "observability/prometheus/compose/prometheus.yml"); strings.Join(sortedKeys(cj), ",") != strings.Join(sortedKeys(kj), ",") {
		t.Errorf("job names differ from the Compose configuration: %v vs %v", sortedKeys(cj), sortedKeys(kj))
	}
	// The token is read from where the Prometheus pod mounts the Secret.
	prom := overlayManifests(t).pod(t, "Deployment/prometheus")
	token := ""
	for _, c := range prom.Spec.Containers {
		for _, m := range c.VolumeMounts {
			if m.Name == "metrics-token" {
				token = m.MountPath + "/metrics_token"
			}
		}
	}
	if token == "" {
		t.Fatal("the Prometheus pod does not mount the metrics-token volume")
	}
	for _, v := range k8sPromViolations(local, k8s, lj, kj, token) {
		t.Error(v)
	}
	clone := func() (map[string]any, map[string]map[string]any) {
		b, _ := yaml.Marshal(k8s)
		var g map[string]any
		_ = yaml.Unmarshal(b, &g)
		j := map[string]map[string]any{}
		for _, x := range listOf(g["scrape_configs"]) {
			j[asStr(mapOf(x)["job_name"])] = mapOf(x)
		}
		return g, j
	}
	for name, c := range map[string]struct {
		mut  func(map[string]any, map[string]map[string]any)
		want string
	}{
		"rule files dropped": {func(g map[string]any, _ map[string]map[string]any) { g["rule_files"] = []any{"other/*.yml"} }, "rule_files differs"},
		"scrape interval": {func(_ map[string]any, j map[string]map[string]any) {
			j["serverflow-control-plane"]["scrape_interval"] = "15s"
		}, "scrape_interval differs"},
		"sample limit": {func(_ map[string]any, j map[string]map[string]any) { delete(j["serverflow-gateway"], "sample_limit") }, "sample_limit differs"},
		"job renamed": {func(_ map[string]any, j map[string]map[string]any) {
			j["serverflow-gw"] = j["serverflow-gateway"]
			delete(j, "serverflow-gateway")
		}, "job names differ"},
		"token inline": {func(_ map[string]any, j map[string]map[string]any) {
			j["serverflow-gateway"]["authorization"] = map[string]any{"type": "Bearer", "credentials": "x"}
		}, "never be inline"},
		"token path": {func(_ map[string]any, j map[string]map[string]any) {
			mapOf(j["serverflow-gateway"]["authorization"])["credentials_file"] = "/etc/other"
		}, "token must be read from"},
		"worker token": {func(_ map[string]any, j map[string]map[string]any) {
			j["serverflow-mock-worker"]["authorization"] = j["serverflow-gateway"]["authorization"]
		}, "authorization present"},
		"cluster-wide": {func(_ map[string]any, j map[string]map[string]any) {
			delete(mapOf(listOf(j["serverflow-gateway"]["kubernetes_sd_configs"])[0]), "namespaces")
		}, "namespace serverflow only"},
		"node role": {func(_ map[string]any, j map[string]map[string]any) {
			mapOf(listOf(j["serverflow-gateway"]["kubernetes_sd_configs"])[0])["role"] = "node"
		}, "role pod"},
		"worker_id not the pod": {func(_ map[string]any, j map[string]map[string]any) {
			j["serverflow-mock-worker"]["relabel_configs"] = []any{}
		}, "worker_id must be the pod name"},
	} {
		g, j := clone()
		c.mut(g, j)
		if v := k8sPromViolations(local, g, lj, j, token); !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", name, c.want, v)
		}
	}
	// The keep rules select container ports that the pods declare, by the names the manifests use.
	m := overlayManifests(t)
	ports := map[string]bool{}
	for _, p := range m.Pods {
		for _, c := range p.all() {
			for _, cp := range c.Ports {
				ports[p.Labels["app.kubernetes.io/component"]+";"+cp.Name] = true
			}
		}
	}
	for name, j := range kj {
		for _, r := range listOf(j["relabel_configs"]) {
			if asStr(mapOf(r)["action"]) == "keep" && !ports[asStr(mapOf(r)["regex"])] {
				t.Errorf("%s keeps %q, which no pod declares as component;port name", name, mapOf(r)["regex"])
			}
		}
	}
}

// sourceFiles reads the files of a directory (non-recursively) by name.
func sourceFiles(t testing.TB, dir string) map[string]string {
	t.Helper()
	es, err := os.ReadDir(filepath.Join(root(t), dir))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range es {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out[e.Name()] = read(t, dir+"/"+e.Name())
		}
	}
	return out
}

// configMapData returns the data of the generated ConfigMap base-<hash>.
func configMapData(docs []map[string]any, base string) map[string]string {
	cm := findConfigMap(docs, base)
	out := map[string]string{}
	if cm == nil {
		return nil
	}
	for k, v := range mapOf(cm["data"]) {
		out[k] = asStr(v)
	}
	return out
}

// TestObservabilityConfigMapsAreTheRepositoryFiles: the rules, dashboards and Grafana provisioning in the cluster are byte for byte the
// files every other stack mounts, and every file of those directories is in the cluster (a new dashboard cannot be forgotten).
func TestObservabilityConfigMapsAreTheRepositoryFiles(t *testing.T) {
	docs := overlayManifests(t).Raw
	for _, c := range []struct {
		cm    string
		files map[string]string
	}{
		{"prometheus-config", map[string]string{"prometheus.yml": read(t, "observability/prometheus/kubernetes/prometheus.yml")}},
		{"prometheus-rules", sourceFiles(t, "observability/prometheus/rules")},
		{"grafana-dashboards", sourceFiles(t, "observability/grafana/dashboards")},
		{"grafana-datasources", map[string]string{"prometheus.yml": read(t, "observability/grafana/provisioning/datasources/prometheus.yml")}},
		{"grafana-dashboard-provider", map[string]string{"serverflow.yml": read(t, "observability/grafana/provisioning/dashboards/serverflow.yml")}},
	} {
		got := configMapData(docs, c.cm)
		if got == nil {
			t.Errorf("no ConfigMap %s in the render", c.cm)
			continue
		}
		if !reflect.DeepEqual(got, c.files) {
			t.Errorf("ConfigMap %s differs from its source files (files: %v, source: %v)", c.cm, sortedKeys(got), sortedKeys(c.files))
		}
	}
	if n := len(sourceFiles(t, "observability/grafana/dashboards")); n != 5 {
		t.Errorf("observability/grafana/dashboards has %d files; scripts/k8s-up.sh K9 expects the five dashboards", n)
	}
}

// TestEveryConfigMapAndSecretVolumeResolves: configMap volumes (also inside projected volumes) name ConfigMaps of the render (or the
// cluster's own CA bundle), and secret volumes name Secrets that scripts/k8s-up.sh creates.
func TestEveryConfigMapAndSecretVolumeResolves(t *testing.T) {
	m := overlayManifests(t)
	have := map[string]bool{"kube-root-ca.crt": true}
	for _, d := range m.Raw {
		if d["kind"] == "ConfigMap" {
			have[asStr(at(d, "metadata", "name"))] = true
		}
	}
	for _, p := range m.Pods {
		for _, vol := range p.Spec.Volumes {
			if vol.Secret != nil && !contains(createdSecrets(t), vol.Secret.SecretName) {
				t.Errorf("%s: volume %s reads the Secret %s, which k8s-up.sh does not create", p.id(), vol.Name, vol.Secret.SecretName)
			}
			var names []string
			if n := asStr(at(vol.Rest["configMap"], "name")); n != "" {
				names = append(names, n)
			}
			for _, s := range listOf(at(vol.Rest["projected"], "sources")) {
				if n := asStr(at(s, "configMap", "name")); n != "" {
					names = append(names, n)
				}
			}
			for _, n := range names {
				if !have[n] {
					t.Errorf("%s: volume %s reads the ConfigMap %s, which the render does not contain", p.id(), vol.Name, n)
				}
			}
		}
	}
}

// apiAccessViolations: the one API credential of the namespace is Prometheus' read access to pods.
func apiAccessViolations(docs []map[string]any, pods []pod) []string {
	var v []string
	for _, p := range pods {
		explicit := false
		for _, vol := range p.Spec.Volumes {
			for _, s := range listOf(at(vol.Rest["projected"], "sources")) {
				if mapOf(mapOf(s)["serviceAccountToken"]) != nil {
					explicit = true
				}
			}
		}
		if explicit != (p.Name == "prometheus") {
			v = append(v, fmt.Sprintf("%s: projects a service account token = %v; only Prometheus may", p.id(), explicit))
		}
	}
	var roles, bindings int
	for _, d := range docs {
		switch d["kind"] {
		case "Role":
			roles++
			rules := listOf(d["rules"])
			if len(rules) != 1 {
				v = append(v, "a Role needs exactly one rule")
				continue
			}
			r := mapOf(rules[0])
			if !reflect.DeepEqual(r["resources"], []any{"pods"}) || !reflect.DeepEqual(r["apiGroups"], []any{""}) {
				v = append(v, fmt.Sprintf("the Role may only cover pods of the core group: %v %v", r["apiGroups"], r["resources"]))
			}
			verbs := fmt.Sprint(r["verbs"])
			if verbs != "[get list watch]" {
				v = append(v, "the Role's verbs must be exactly get, list, watch; got "+verbs)
			}
		case "RoleBinding":
			bindings++
			s := listOf(d["subjects"])
			if len(s) != 1 || asStr(mapOf(s[0])["kind"]) != "ServiceAccount" || asStr(mapOf(s[0])["name"]) != "prometheus" || asStr(mapOf(s[0])["namespace"]) != "serverflow" {
				v = append(v, fmt.Sprintf("the RoleBinding must bind the ServiceAccount serverflow/prometheus alone: %v", s))
			}
			if asStr(at(d, "roleRef", "kind")) != "Role" {
				v = append(v, "the RoleBinding must reference a Role, not a ClusterRole")
			}
		case "ClusterRole", "ClusterRoleBinding":
			v = append(v, "cluster-scoped RBAC is not allowed")
		}
	}
	if roles != 1 || bindings != 1 {
		v = append(v, fmt.Sprintf("expected one Role and one RoleBinding, found %d and %d", roles, bindings))
	}
	return v
}

func TestPrometheusAPIAccessIsReadOnlyPodsAndPrometheusOnly(t *testing.T) {
	m := overlayManifests(t)
	for _, v := range apiAccessViolations(m.Raw, m.Pods) {
		t.Error(v)
	}
	mutate := func(f func(docs []map[string]any, pods []pod)) []string {
		docs := cloneRaw(t, m.Raw)
		pods := overlayManifests(t).Pods
		f(docs, pods)
		return apiAccessViolations(docs, pods)
	}
	role := func(docs []map[string]any) map[string]any {
		return mapOf(listOf(findDoc(docs, "Role", "prometheus-discovery")["rules"])[0])
	}
	for name, c := range map[string]struct {
		f    func([]map[string]any, []pod)
		want string
	}{
		"secrets readable": {func(d []map[string]any, _ []pod) { role(d)["resources"] = []any{"pods", "secrets"} }, "only cover pods"},
		"write verb":       {func(d []map[string]any, _ []pod) { role(d)["verbs"] = []any{"get", "list", "watch", "delete"} }, "verbs must be exactly"},
		"extra subject": {func(d []map[string]any, _ []pod) {
			b := findDoc(d, "RoleBinding", "prometheus-discovery")
			b["subjects"] = append(listOf(b["subjects"]), map[string]any{"kind": "ServiceAccount", "name": "grafana", "namespace": "serverflow"})
		}, "alone"},
		"cluster role": {func(d []map[string]any, _ []pod) {
			findDoc(d, "RoleBinding", "prometheus-discovery")["roleRef"] = map[string]any{"kind": "ClusterRole"}
		}, "not a ClusterRole"},
		"cluster-scoped": {func(d []map[string]any, _ []pod) { d[0] = map[string]any{"kind": "ClusterRoleBinding"} }, "cluster-scoped"},
		"grafana gets a token": {func(_ []map[string]any, ps []pod) {
			for i := range ps {
				if ps[i].Name == "grafana" {
					ps[i].Spec.Volumes = append(ps[i].Spec.Volumes, volume{Name: "t", Rest: map[string]any{"projected": map[string]any{"sources": []any{map[string]any{"serviceAccountToken": map[string]any{"path": "token"}}}}}})
				}
			}
		}, "only Prometheus may"},
		"prometheus without a token": {func(_ []map[string]any, ps []pod) {
			for i := range ps {
				if ps[i].Name == "prometheus" {
					ps[i].Spec.Volumes = nil
				}
			}
		}, "only Prometheus may"},
	} {
		if v := mutate(c.f); !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", name, c.want, v)
		}
	}
	// The token is never mounted by default: Pod Security and the audit still see automountServiceAccountToken false.
	if p := m.pod(t, "Deployment/prometheus"); !boolIs(p.Spec.AutomountServiceAccountToken, false) {
		t.Error("Prometheus must keep automountServiceAccountToken: false and project its token explicitly")
	}
}

// tracingViolations: the gateway and the workers export spans to the Jaeger Service's OTLP/HTTP port.
func tracingViolations(docs []map[string]any, services []service, worker pod) []string {
	var v []string
	cm := findConfigMap(docs, "gateway-config")
	data := mapOf(at(cm, "data"))
	if asStr(data["SERVERFLOW_TRACING_ENABLED"]) != "true" {
		v = append(v, "the gateway does not have tracing enabled")
	}
	endpoint := asStr(data["SERVERFLOW_TRACING_ENDPOINT"])
	port := 0
	for _, s := range services {
		if s.Metadata.Name == "jaeger" {
			for _, sp := range s.Spec.Ports {
				if sp.Name == "otlp-http" {
					port = sp.Port
				}
			}
		}
	}
	if port == 0 || endpoint != fmt.Sprintf("http://jaeger:%d", port) {
		v = append(v, fmt.Sprintf("the gateway's tracing endpoint %q is not the OTLP/HTTP port (%d) of the Service jaeger", endpoint, port))
	}
	if asStr(data["SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT"]) != "true" {
		v = append(v, "plaintext OTLP to a non-loopback host needs SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT=true")
	}
	if asStr(data["SERVERFLOW_TRACING_SAMPLE_RATIO"]) != "1.0" {
		v = append(v, "the sample ratio must be 1.0 (smoke K11 looks for one specific request)")
	}
	var args []string
	for _, c := range worker.Spec.Containers {
		if c.Name == "mock-worker" {
			args = c.Args
		}
	}
	if !contains(args, fmt.Sprintf("--otlp-endpoint=%s", endpoint)) {
		v = append(v, "the mock worker's --otlp-endpoint is not the gateway's tracing endpoint")
	}
	if !contains(args, "--trace-insecure-ok") {
		v = append(v, "the mock worker needs --trace-insecure-ok for plaintext OTLP inside the cluster")
	}
	return v
}

func TestTracingPointsAtJaeger(t *testing.T) {
	m := overlayManifests(t)
	worker := m.pod(t, "Deployment/mock-worker")
	for _, v := range tracingViolations(m.Raw, m.Services, worker) {
		t.Error(v)
	}
	for name, c := range map[string]struct {
		f    func(data map[string]any, svcs []service, w *pod)
		want string
	}{
		"tracing off":     {func(d map[string]any, _ []service, _ *pod) { d["SERVERFLOW_TRACING_ENABLED"] = "false" }, "does not have tracing enabled"},
		"wrong endpoint":  {func(d map[string]any, _ []service, _ *pod) { d["SERVERFLOW_TRACING_ENDPOINT"] = "http://jaeger:14318" }, "not the OTLP/HTTP port"},
		"secure required": {func(d map[string]any, _ []service, _ *pod) { delete(d, "SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT") }, "ALLOW_INSECURE"},
		"sampling":        {func(d map[string]any, _ []service, _ *pod) { d["SERVERFLOW_TRACING_SAMPLE_RATIO"] = "0.1" }, "sample ratio"},
		"worker silent": {func(_ map[string]any, _ []service, w *pod) {
			for i := range w.Spec.Containers {
				w.Spec.Containers[i].Args = nil
			}
		}, "--otlp-endpoint"},
		"service port moved": {func(_ map[string]any, s []service, _ *pod) { s[0].Spec.Ports = nil }, "not the OTLP/HTTP port"},
	} {
		docs := cloneRaw(t, m.Raw)
		w := overlayManifests(t).pod(t, "Deployment/mock-worker")
		svcs := overlayManifests(t).Services
		for i := range svcs {
			if svcs[i].Metadata.Name == "jaeger" {
				svcs[0], svcs[i] = svcs[i], svcs[0]
			}
		}
		c.f(mapOf(at(findConfigMap(docs, "gateway-config"), "data")), svcs, &w)
		if v := tracingViolations(docs, svcs, w); !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", name, c.want, v)
		}
	}
	// Jaeger's OTLP port is not published on the node; only the UI is.
	for _, s := range m.Services {
		if s.Metadata.Name == "jaeger" && s.Spec.Type != "ClusterIP" {
			t.Error("the Service jaeger (OTLP) must stay ClusterIP; only jaeger-ui is a NodePort")
		}
	}
}

// ---- the smoke checks, run against canned answers ----

// runK runs shell code that sources the extracted functions with stubs in front of them, under every bash, and reports success.
func runK(t *testing.T, funcs []string, prelude, call string) []bool {
	t.Helper()
	var out []bool
	for _, bash := range bashes(t) {
		var b strings.Builder
		b.WriteString("set -u\nDEV_DIR=" + t.TempDir() + "\nSCRATCH=" + t.TempDir() + "\nPROMETHEUS_URL=http://p GRAFANA_URL=http://g JAEGER_URL=http://j\n")
		b.WriteString(prelude + "\n")
		for _, f := range funcs {
			b.WriteString(extractFunc(t, f) + "\n")
		}
		b.WriteString(call + "\n")
		file := filepath.Join(t.TempDir(), "h.sh")
		if err := os.WriteFile(file, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		cmd := execCommand(bash, file)
		cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
		out = append(out, cmd.Run() == nil)
	}
	return out
}

func allEqual(bs []bool, want bool) bool {
	for _, b := range bs {
		if b != want {
			return false
		}
	}
	return true
}

// TestK8JudgesTargetsAndRequests: curl is a stub that answers by the query; every deviation from a healthy system fails K8's
// target check.
func TestK8JudgesTargetsAndRequests(t *testing.T) {
	// answers: "<query fragment>=<value>" lines; an absent fragment answers the empty result (count of nothing).
	run := func(gwPods, mwPods int, answers map[string]string) bool {
		var rows []string
		for q, val := range answers {
			rows = append(rows, fmt.Sprintf("%s) printf '%%s' '{\"status\":\"success\",\"data\":{\"result\":[{\"metric\":{},\"value\":[1.7,\"%s\"]}]}}';;", q, val))
		}
		sort.Strings(rows)
		stub := "curl() {\n  case \"$*\" in\n" + strings.ReplaceAll(strings.Join(rows, "\n"), ") printf", ") printf") +
			"\n  *) printf '%s' '{\"status\":\"success\",\"data\":{\"result\":[]}}';;\n  esac\n}\n" +
			fmt.Sprintf("gateway_pods() { for _ in $(seq 1 %d); do echo g; done; }\nmock_worker_pods() { for _ in $(seq 1 %d); do echo w; done; }\n", gwPods, mwPods)
		res := runK(t, []string{"prom_count", "prom_targets_up"}, stub, "prom_targets_up")
		if !allEqual(res, res[0]) {
			t.Fatalf("bash versions disagree: %v", res)
		}
		return res[0]
	}
	healthy := map[string]string{
		`*serverflow-gateway*==\ 1*`:       "3",
		`*serverflow-control-plane*==\ 1*`: "1",
		`*serverflow-mock-worker*==\ 1*`:   "3",
	}
	if !run(3, 3, healthy) {
		t.Fatal("K8 failed on a healthy system")
	}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for a, b := range healthy {
			m[a] = b
		}
		if v == "" {
			delete(m, k)
		} else {
			m[k] = v
		}
		return m
	}
	for name, c := range map[string]struct {
		gw, mw  int
		answers map[string]string
	}{
		"one gateway not scraped":    {3, 3, with(`*serverflow-gateway*==\ 1*`, "2")},
		"control plane missing":      {3, 3, with(`*serverflow-control-plane*==\ 1*`, "")},
		"a worker not scraped":       {3, 3, with(`*serverflow-mock-worker*==\ 1*`, "2")},
		"a target is down":           {3, 3, func() map[string]string { m := with("", ""); m[`*==\ 0*`] = "1"; return m }()},
		"more pods than targets":     {4, 3, healthy},
		"single gateway is no proof": {1, 3, with(`*serverflow-gateway*==\ 1*`, "1")},
	} {
		if run(c.gw, c.mw, c.answers) {
			t.Errorf("%s: K8's target check passed", name)
		}
	}
	// requests_counted: zero and absent fail, positive passes.
	req := func(val string) bool {
		stub := "curl() { printf '%s' '{\"status\":\"success\",\"data\":{\"result\":[{\"metric\":{},\"value\":[1,\"" + val + "\"]}]}}'; }\n"
		if val == "" {
			stub = "curl() { printf '%s' '{\"status\":\"success\",\"data\":{\"result\":[]}}'; }\n"
		}
		return runK(t, []string{"prom_count", "requests_counted"}, stub, "requests_counted")[0]
	}
	if !req("24") || !req("1.2e+01") {
		t.Error("requests_counted rejected a positive count")
	}
	if req("0") || req("") {
		t.Error("requests_counted accepted a zero or empty count")
	}
	// An error answer is never read as zero targets down.
	errStub := "curl() { printf '%s' '{\"status\":\"error\"}'; }\n"
	if runK(t, []string{"prom_count"}, errStub, "prom_count x")[0] {
		t.Error("prom_count accepted an error response")
	}
}

// TestK9JudgesDashboardsAndDatasource: Grafana's search must list the five uids and exactly five dashboards, and the datasource
// health must be OK.
func TestK9JudgesDashboardsAndDatasource(t *testing.T) {
	uids := []string{"sf-cluster-overview", "sf-scheduler", "sf-resilience", "sf-model", "sf-worker"}
	search := func(us ...string) string {
		var items []string
		for _, u := range us {
			items = append(items, fmt.Sprintf(`{"uid":"%s","title":"t","type":"dash-db"}`, u))
		}
		return "[" + strings.Join(items, ",") + "]"
	}
	run := func(searchBody, healthBody string) bool {
		stub := "grafana_curl() {\n  case \"$1\" in\n  /api/search*) printf '%s' '" + searchBody + "';;\n  /api/datasources/uid/serverflow-prometheus/health) printf '%s' '" + healthBody + "';;\n  *) return 1;;\n  esac\n}\n"
		return runK(t, []string{"grafana_provisioned"}, stub, "grafana_provisioned")[0]
	}
	okHealth := `{"message":"Successfully queried the Prometheus API.","status":"OK"}`
	if !run(search(uids...), okHealth) {
		t.Fatal("K9 failed on a healthy Grafana")
	}
	for name, c := range map[string][2]string{
		"a dashboard missing":  {search(uids[:4]...), okHealth},
		"a sixth dashboard":    {search(append(append([]string{}, uids...), "other")...), okHealth},
		"wrong uid":            {search("a", "b", "c", "d", "e"), okHealth},
		"datasource unhealthy": {search(uids...), `{"message":"connection refused","status":"ERROR"}`},
		"datasource not found": {search(uids...), `{"message":"Data source not found"}`},
	} {
		if run(c[0], c[1]) {
			t.Errorf("%s: K9 passed", name)
		}
	}
	healthy := func(body string) bool {
		return runK(t, []string{"grafana_database_ok"}, "curl() { printf '%s' '"+body+"'; }\n", "grafana_database_ok")[0]
	}
	if !healthy("{\n  \"database\": \"ok\",\n  \"version\": \"11.6.16\"\n}") {
		t.Error("grafana_database_ok rejected a healthy answer")
	}
	if healthy(`{"database":"failing"}`) {
		t.Error("grafana_database_ok accepted a failing database")
	}
}

// TestK11JudgesTheTrace: the trace found by request ID must carry spans of both services; the query asks for the request ID, in a
// window around now, and never puts the API key anywhere.
func TestK11JudgesTheTrace(t *testing.T) {
	span := func(svc string) string {
		return `{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"` + svc + `"}}]},"scopeSpans":[]}`
	}
	run := func(body string) bool {
		stub := "iso_utc() { echo 2026-01-01T00:00:00Z; }\nK11_RID=abc\ncurl() { printf '%s' '" + body + "'; }\n"
		return runK(t, []string{"trace_has_gateway_and_worker"}, stub, "trace_has_gateway_and_worker")[0]
	}
	both := `{"result":{"resourceSpans":[` + span("serverflow-gateway") + `,` + span("serverflow-mock-worker") + `]}}`
	if !run(both) {
		t.Fatal("K11 failed on a trace with both services")
	}
	if !run(strings.ReplaceAll(both, `"key":"service.name","value":`, "\"key\": \"service.name\",\n \"value\": ")) {
		t.Error("K11 depends on the JSON whitespace")
	}
	for name, body := range map[string]string{
		"gateway only":                           `{"result":{"resourceSpans":[` + span("serverflow-gateway") + `]}}`,
		"worker only":                            `{"result":{"resourceSpans":[` + span("serverflow-mock-worker") + `]}}`,
		"worker name as an attribute value only": `{"result":{"resourceSpans":[` + span("serverflow-gateway") + `],"x":"serverflow-mock-worker"}}`,
		"no trace":                               `{"result":{}}`,
		"empty":                                  ``,
	} {
		if run(body) {
			t.Errorf("%s: K11 passed", name)
		}
	}
	fn := extractFunc(t, "trace_has_gateway_and_worker") + extractFunc(t, "k11_trace_in_jaeger")
	for _, want := range []string{"query.service_name=serverflow-gateway", "serverflow.request_id", "query.start_time_min", "query.start_time_max", "/api/v3/traces", "poll 90"} {
		if !strings.Contains(fn, want) {
			t.Errorf("K11 lost %q", want)
		}
	}
	if strings.Contains(fn, "Authorization") {
		t.Error("K11 must hand the key to curl on standard input (gw_curl) and never to Jaeger")
	}
}

// TestObservabilityIsPartOfUp: the script applies the phase after the jobs, waits for Jaeger before the core system and for
// Prometheus and Grafana after it, creates the Grafana Secret, and publishes the three URLs kind maps.
func TestObservabilityIsPartOfUp(t *testing.T) {
	up := extractFunc(t, "cmd_up")
	pos := -1
	for _, o := range []string{"apply_phase jobs", "apply_phase observability", "wait_deployment jaeger", "apply_phase core", "wait_deployment gateway", "wait_deployment prometheus", "wait_deployment grafana", "bootstrap_tenants"} {
		i := strings.Index(up, o)
		if i <= pos {
			t.Errorf("cmd_up: %q is missing or out of order", o)
		}
		pos = i
	}
	if !contains(createdSecrets(t), "grafana") {
		t.Error("SECRETS does not include grafana (the Grafana pod reads it)")
	}
}

// TestGrafanaIsLockedDown: no anonymous access, no sign-up, the admin password from the Secret and not from the manifest.
func TestGrafanaIsLockedDown(t *testing.T) {
	p := overlayManifests(t).pod(t, "Deployment/grafana")
	c := p.Spec.Containers[0]
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	if env["GF_AUTH_ANONYMOUS_ENABLED"] != "false" || env["GF_USERS_ALLOW_SIGN_UP"] != "false" {
		t.Error("Grafana must disable anonymous access and sign-up")
	}
	if _, ok := env["GF_SECURITY_ADMIN_PASSWORD"]; ok {
		t.Error("the admin password must come from the Secret grafana, not from the manifest")
	}
	reads := false
	for _, ef := range c.EnvFrom {
		reads = reads || (ef.SecretRef != nil && ef.SecretRef.Name == "grafana")
	}
	if !reads {
		t.Error("Grafana does not read the Secret grafana")
	}
	if env["PROMETHEUS_URL"] != "http://prometheus:9090" {
		t.Errorf("PROMETHEUS_URL is %q; the datasource file reads it and the Service prometheus listens on 9090", env["PROMETHEUS_URL"])
	}
}
