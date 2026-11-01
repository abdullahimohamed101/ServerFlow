package k8s

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// The tests of the core system (Phase 17 PR 3): the gateway's drain numbers, rollout and PDB, the control plane's single
// replica, the worker pod's shape, the entry points, the names that internal/config and the mock worker honour. As elsewhere,
// each rule is a function from a (possibly broken) object to violations, so the real render and its mutations are judged by the
// same code.

// ---- navigation of the raw documents ----

func at(v any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k < 0 || k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

func asStr(v any) string { s, _ := v.(string); return s }

// cloneRaw deep-copies documents so a mutation never leaks into the next one.
func cloneRaw(t testing.TB, docs []map[string]any) []map[string]any {
	t.Helper()
	b, err := yaml.Marshal(docs)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	if err := yaml.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func findDoc(docs []map[string]any, kind, name string) map[string]any {
	for _, d := range docs {
		if d["kind"] == kind && asStr(at(d, "metadata", "name")) == name {
			return d
		}
	}
	return nil
}

// findConfigMap returns the generated ConfigMap `base-<hash>`.
func findConfigMap(docs []map[string]any, base string) map[string]any {
	for _, d := range docs {
		if d["kind"] == "ConfigMap" && strings.HasPrefix(asStr(at(d, "metadata", "name")), base+"-") {
			return d
		}
	}
	return nil
}

func seconds(s string) int {
	d, err := time.ParseDuration(s)
	if err != nil {
		return -1
	}
	return int(d / time.Second)
}

// ---- the gateway ----

// gatewayDrain reads the drain numbers of the rendered gateway: the settings are in the generated ConfigMap the Deployment
// references, the termination grace and the rollout in the Deployment.
func gatewayDrain(docs []map[string]any) (drain, []string) {
	var v []string
	var d drain
	dep := findDoc(docs, "Deployment", "gateway")
	if dep == nil {
		return d, []string{"no Deployment gateway"}
	}
	d.Replicas, _ = asInt(at(dep, "spec", "replicas"))
	d.TerminationGrace, _ = asInt(at(dep, "spec", "template", "spec", "terminationGracePeriodSeconds"))
	d.MaxUnavailable = -1
	if n, ok := asInt(at(dep, "spec", "strategy", "rollingUpdate", "maxUnavailable")); ok {
		d.MaxUnavailable = n
	}
	// The ConfigMap the container really reads.
	cm := ""
	if ef, ok := at(dep, "spec", "template", "spec", "containers", 0, "envFrom").([]any); ok {
		for _, e := range ef {
			if n := asStr(at(e, "configMapRef", "name")); n != "" {
				cm = n
			}
		}
	}
	cmDoc := findDoc(docs, "ConfigMap", cm)
	if cm == "" || cmDoc == nil {
		return d, append(v, "the gateway reads no ConfigMap of the render (envFrom configMapRef)")
	}
	data := at(cmDoc, "data")
	d.ResilienceMode = asStr(at(data, "SERVERFLOW_RESILIENCE_MODE"))
	d.ReadinessGrace = seconds(asStr(at(data, "SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE")))
	d.ShutdownTimeout = seconds(asStr(at(data, "SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT")))
	if d.ShutdownTimeout < 0 {
		v = append(v, "SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT is not set or not a duration")
	}
	return d, v
}

// gatewayRolloutViolations: a rolling update that never removes a ready pod before its replacement is ready, a PDB, and
// scheduling hints that cannot leave a replica Pending on a single node.
func gatewayRolloutViolations(docs []map[string]any) []string {
	var v []string
	dep := findDoc(docs, "Deployment", "gateway")
	if dep == nil {
		return []string{"no Deployment gateway"}
	}
	if asStr(at(dep, "spec", "strategy", "type")) != "RollingUpdate" {
		v = append(v, "the gateway strategy must be RollingUpdate")
	}
	if n, ok := asInt(at(dep, "spec", "strategy", "rollingUpdate", "maxSurge")); !ok || n < 1 {
		v = append(v, "the gateway needs maxSurge >= 1 (the replacement starts before the old pod stops)")
	}
	if n, ok := asInt(at(dep, "spec", "minReadySeconds")); !ok || n < 5 {
		v = append(v, "minReadySeconds must be at least 5 (a pod that flaps right after Ready does not count)")
	}
	if n, ok := asInt(at(dep, "spec", "progressDeadlineSeconds")); !ok || n < 300 || n > 600 {
		v = append(v, "progressDeadlineSeconds must be 300-600: long enough for a cold start (a gateway is not Ready until a worker registers), short enough to report a stuck rollout")
	}
	if at(dep, "spec", "template", "spec", "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution") != nil {
		v = append(v, "pod anti-affinity must be preferred only: on one node a required rule leaves replicas Pending")
	}
	if tsc, ok := at(dep, "spec", "template", "spec", "topologySpreadConstraints").([]any); ok {
		for _, c := range tsc {
			if asStr(at(c, "whenUnsatisfiable")) != "ScheduleAnyway" {
				v = append(v, "topologySpreadConstraints must be ScheduleAnyway (single node)")
			}
		}
	}
	pdb := findDoc(docs, "PodDisruptionBudget", "gateway")
	if pdb == nil {
		v = append(v, "no PodDisruptionBudget gateway")
	} else {
		if n, ok := asInt(at(pdb, "spec", "maxUnavailable")); !ok || n != 1 {
			v = append(v, "the PodDisruptionBudget must allow exactly one unavailable gateway")
		}
		if at(pdb, "spec", "minAvailable") != nil {
			v = append(v, "the PodDisruptionBudget must use maxUnavailable only")
		}
		sel, _ := at(pdb, "spec", "selector", "matchLabels").(map[string]any)
		lbl, _ := at(dep, "spec", "template", "metadata", "labels").(map[string]any)
		if len(sel) == 0 {
			v = append(v, "the PodDisruptionBudget has no selector")
		}
		for k, val := range sel {
			if lbl[k] != val {
				v = append(v, "the PodDisruptionBudget selector does not match the gateway pods ("+k+")")
			}
		}
	}
	return v
}

func TestGatewayDrainSettingsInTheManifest(t *testing.T) {
	docs := overlayManifests(t).Raw
	d, v := gatewayDrain(docs)
	v = append(v, drainViolations(d)...)
	for _, x := range v {
		t.Error(x)
	}
	if d.Replicas != 3 {
		t.Errorf("the kind overlay runs %d gateways, want 3 (D11)", d.Replicas)
	}
	t.Logf("gateway drain: %+v", d)
	// The base alone already satisfies the relations with its two replicas.
	baseDocs := parse(t, render(t, "deploy/kubernetes/base")).Raw
	bd, bv := gatewayDrain(baseDocs)
	for _, x := range append(bv, drainViolations(bd)...) {
		t.Errorf("base: %s", x)
	}
	if bd.Replicas != 2 {
		t.Errorf("the base gateway has %d replicas, want 2", bd.Replicas)
	}
}

func TestGatewayDrainMutationsOnTheManifest(t *testing.T) {
	docs := overlayManifests(t).Raw
	setCM := func(d []map[string]any, key, val string) {
		data := at(findConfigMap(d, "gateway-config"), "data").(map[string]any)
		if val == "" {
			delete(data, key)
		} else {
			data[key] = val
		}
	}
	dep := func(d []map[string]any) map[string]any { return findDoc(d, "Deployment", "gateway") }
	cases := []struct {
		name  string
		apply func(d []map[string]any)
		want  string
	}{
		{"resilience mode removed", func(d []map[string]any) { setCM(d, "SERVERFLOW_RESILIENCE_MODE", "") }, "RESILIENCE_MODE must be on"},
		{"resilience mode off", func(d []map[string]any) { setCM(d, "SERVERFLOW_RESILIENCE_MODE", "off") }, "RESILIENCE_MODE must be on"},
		{"grace 0", func(d []map[string]any) { setCM(d, "SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE", "0s") }, "grace must be positive"},
		{"grace above shutdown timeout", func(d []map[string]any) { setCM(d, "SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE", "31s") }, "shorter than the shutdown timeout"},
		{"grace unparsable", func(d []map[string]any) { setCM(d, "SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE", "five") }, "grace must be positive"},
		{"shutdown timeout removed", func(d []map[string]any) { setCM(d, "SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT", "") }, "SHUTDOWN_TIMEOUT is not set"},
		{"termination grace 39", func(d []map[string]any) {
			at(dep(d), "spec", "template", "spec").(map[string]any)["terminationGracePeriodSeconds"] = 39
		}, "terminationGracePeriodSeconds"},
		{"maxUnavailable 1", func(d []map[string]any) {
			at(dep(d), "spec", "strategy", "rollingUpdate").(map[string]any)["maxUnavailable"] = 1
		}, "maxUnavailable must be 0"},
		{"single replica", func(d []map[string]any) { dep(d)["spec"].(map[string]any)["replicas"] = 1 }, "single gateway replica"},
		{"the ConfigMap is not read", func(d []map[string]any) {
			at(dep(d), "spec", "template", "spec", "containers", 0, "envFrom", 0, "configMapRef").(map[string]any)["name"] = "elsewhere"
		}, "reads no ConfigMap"},
	}
	for _, c := range cases {
		d := cloneRaw(t, docs)
		c.apply(d)
		gd, v := gatewayDrain(d)
		v = append(v, drainViolations(gd)...)
		if !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", c.name, c.want, v)
		}
	}
}

func TestGatewayRolloutAndDisruptionBudget(t *testing.T) {
	docs := overlayManifests(t).Raw
	for _, v := range gatewayRolloutViolations(docs) {
		t.Error(v)
	}
	dep := func(d []map[string]any) map[string]any { return findDoc(d, "Deployment", "gateway") }
	mut := []struct {
		name  string
		apply func(d []map[string]any)
		want  string
	}{
		{"recreate", func(d []map[string]any) { at(dep(d), "spec", "strategy").(map[string]any)["type"] = "Recreate" }, "must be RollingUpdate"},
		{"no surge", func(d []map[string]any) {
			at(dep(d), "spec", "strategy", "rollingUpdate").(map[string]any)["maxSurge"] = 0
		}, "maxSurge"},
		{"no minReadySeconds", func(d []map[string]any) { delete(dep(d)["spec"].(map[string]any), "minReadySeconds") }, "minReadySeconds"},
		{"no progress deadline", func(d []map[string]any) { delete(dep(d)["spec"].(map[string]any), "progressDeadlineSeconds") }, "progressDeadlineSeconds"},
		{"progress deadline 120", func(d []map[string]any) { dep(d)["spec"].(map[string]any)["progressDeadlineSeconds"] = 120 }, "progressDeadlineSeconds must be 300-600"},
		{"required anti-affinity", func(d []map[string]any) {
			at(dep(d), "spec", "template", "spec", "affinity", "podAntiAffinity").(map[string]any)["requiredDuringSchedulingIgnoredDuringExecution"] = []any{map[string]any{"topologyKey": "kubernetes.io/hostname"}}
		}, "preferred only"},
		{"DoNotSchedule spread", func(d []map[string]any) {
			at(dep(d), "spec", "template", "spec", "topologySpreadConstraints", 0).(map[string]any)["whenUnsatisfiable"] = "DoNotSchedule"
		}, "ScheduleAnyway"},
		{"PDB removed", func(d []map[string]any) {
			for i, x := range d {
				if x["kind"] == "PodDisruptionBudget" {
					d[i] = map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "gone"}}
				}
			}
		}, "no PodDisruptionBudget"},
		{"PDB allows two", func(d []map[string]any) {
			at(findDoc(d, "PodDisruptionBudget", "gateway"), "spec").(map[string]any)["maxUnavailable"] = 2
		}, "exactly one"},
		{"PDB selects nothing", func(d []map[string]any) {
			at(findDoc(d, "PodDisruptionBudget", "gateway"), "spec", "selector", "matchLabels").(map[string]any)["app.kubernetes.io/component"] = "nothing"
		}, "does not match the gateway pods"},
	}
	for _, c := range mut {
		d := cloneRaw(t, docs)
		c.apply(d)
		if v := gatewayRolloutViolations(d); !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", c.name, c.want, v)
		}
	}
}

// ---- the control plane ----

func controlPlaneViolations(docs []map[string]any) []string {
	var v []string
	dep := findDoc(docs, "Deployment", "control-plane")
	if dep == nil {
		return []string{"no Deployment control-plane"}
	}
	if n, ok := asInt(at(dep, "spec", "replicas")); !ok || n != 1 {
		v = append(v, "the control plane must have exactly one replica (its registry is in memory: two would diverge)")
	}
	if asStr(at(dep, "spec", "strategy", "type")) != "Recreate" {
		v = append(v, "the control plane strategy must be Recreate (a surge pod would hold a second, empty registry)")
	}
	return v
}

func TestControlPlaneIsOneRecreatedReplica(t *testing.T) {
	docs := overlayManifests(t).Raw
	for _, v := range controlPlaneViolations(docs) {
		t.Error(v)
	}
	dep := func(d []map[string]any) map[string]any { return findDoc(d, "Deployment", "control-plane") }
	for name, c := range map[string]struct {
		apply func(d []map[string]any)
		want  string
	}{
		"two replicas":  {func(d []map[string]any) { dep(d)["spec"].(map[string]any)["replicas"] = 2 }, "exactly one replica"},
		"rolling":       {func(d []map[string]any) { at(dep(d), "spec", "strategy").(map[string]any)["type"] = "RollingUpdate" }, "Recreate"},
		"no strategy":   {func(d []map[string]any) { delete(dep(d)["spec"].(map[string]any), "strategy") }, "Recreate"},
		"zero replicas": {func(d []map[string]any) { dep(d)["spec"].(map[string]any)["replicas"] = 0 }, "exactly one replica"},
	} {
		d := cloneRaw(t, docs)
		c.apply(d)
		if v := controlPlaneViolations(d); !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", name, c.want, v)
		}
	}
}

// ---- the worker pod ----

func envOf(c container, name string) (int, envVar) {
	for i, e := range c.Env {
		if e.Name == name {
			return i, e
		}
	}
	return -1, envVar{}
}

func argOf(c container, prefix string) string {
	for _, a := range c.Args {
		if strings.HasPrefix(a, prefix) {
			return strings.TrimPrefix(a, prefix)
		}
	}
	return ""
}

// workerViolations: the pod shape of D7. The agent registers the pod IP, under the pod name, and is probed on loopback; the
// mock worker drains for longer than the registry needs to notice and for less than the pod's grace period.
func workerViolations(p pod) []string {
	var v []string
	add := func(format string, a ...any) { v = append(v, p.id()+": "+fmtv(format, a...)) }
	var mw, ag *container
	for i := range p.Spec.Containers {
		switch p.Spec.Containers[i].Name {
		case "mock-worker":
			mw = &p.Spec.Containers[i]
		case "worker-agent":
			ag = &p.Spec.Containers[i]
		}
	}
	if len(p.Spec.Containers) != 2 || mw == nil || ag == nil {
		return append(v, p.id()+": a worker pod is exactly the containers mock-worker and worker-agent")
	}
	// Identity.
	if got := argOf(*mw, "--worker-id="); got != "$(POD_NAME)" {
		add("the mock worker's --worker-id must be $(POD_NAME), is %q", got)
	}
	if _, e := envOf(*ag, "SERVERFLOW_WORKER_ID"); e.Value != "$(POD_NAME)" {
		add("the agent's SERVERFLOW_WORKER_ID must be $(POD_NAME), is %q", e.Value)
	}
	// Downward API variables, defined before they are expanded.
	for _, c := range []container{*mw, *ag} {
		i, e := envOf(c, "POD_NAME")
		if i < 0 || e.ValueFrom == nil || e.ValueFrom.FieldRef == nil || e.ValueFrom.FieldRef.FieldPath != "metadata.name" {
			add("%s needs POD_NAME from the downward API (metadata.name)", c.Name)
		}
	}
	ipAt, ip := envOf(*ag, "POD_IP")
	if ipAt < 0 || ip.ValueFrom == nil || ip.ValueFrom.FieldRef == nil || ip.ValueFrom.FieldRef.FieldPath != "status.podIP" {
		add("the agent needs POD_IP from the downward API (status.podIP)")
	}
	for _, name := range []string{"SERVERFLOW_WORKER_ID", "SERVERFLOW_WORKER_ADVERTISE_URL"} {
		i, _ := envOf(*ag, name)
		nameAt, _ := envOf(*ag, "POD_NAME")
		if i >= 0 && (nameAt < 0 || nameAt > i || (name == "SERVERFLOW_WORKER_ADVERTISE_URL" && (ipAt < 0 || ipAt > i))) {
			add("%s uses a variable that is defined after it ($(VAR) expands only earlier ones)", name)
		}
	}
	// Addresses.
	want := map[string]string{
		"SERVERFLOW_WORKER_ADVERTISE_URL":     "http://$(POD_IP):9000",
		"SERVERFLOW_WORKER_BACKEND_URL":       "http://127.0.0.1:9000",
		"SERVERFLOW_WORKER_CONTROL_PLANE_URL": "http://control-plane:9090",
		"SERVERFLOW_WORKER_STATUS_LISTEN":     "127.0.0.1:9102",
		"SERVERFLOW_WORKER_MODEL":             "mock-model",
	}
	for name, val := range want {
		if _, e := envOf(*ag, name); e.Value != val {
			add("the agent's %s must be %q, is %q", name, val, e.Value)
		}
	}
	if got := argOf(*mw, "--addr="); got != "0.0.0.0:9000" {
		add("the mock worker must listen on 0.0.0.0:9000 (the gateway dials the pod IP), is %q", got)
	}
	if len(ag.Ports) != 0 {
		add("the agent declares ports; its status endpoint is on loopback and nothing else is exposed")
	}
	// Probes.
	for _, c := range []struct {
		name  string
		pr    *probe
		wants string
	}{
		{"agent startup", ag.StartupProbe, "http://127.0.0.1:9102/healthz"},
		{"agent readiness", ag.ReadinessProbe, "http://127.0.0.1:9102/readyz"},
		{"agent liveness", ag.LivenessProbe, "http://127.0.0.1:9102/healthz"},
	} {
		if c.pr == nil || c.pr.Exec == nil || len(c.pr.Exec.Command) != 2 || c.pr.Exec.Command[0] != "/usr/local/bin/healthcheck" || c.pr.Exec.Command[1] != c.wants {
			add("the %s probe must be exec /usr/local/bin/healthcheck %s", c.name, c.wants)
		}
	}
	if rp := mw.ReadinessProbe; rp == nil || rp.HTTPGet == nil || rp.HTTPGet.Path != "/readyz" {
		add("the mock worker's readiness probe must be GET /readyz")
	}
	if lp := mw.LivenessProbe; lp == nil || lp.HTTPGet == nil || lp.HTTPGet.Path != "/health" {
		add("the mock worker's liveness probe must be GET /health")
	}
	// Drain: --drain-announce above the heartbeat (2 s) + registry refresh (1 s) + margin, below --drain-timeout, which ends
	// with 10 s to spare before the kubelet kills the pod.
	announce, timeout := seconds(argOf(*mw, "--drain-announce=")), seconds(argOf(*mw, "--drain-timeout="))
	tgps := int64(-1)
	if p.Spec.TerminationGracePeriodSeconds != nil {
		tgps = *p.Spec.TerminationGracePeriodSeconds
	}
	if announce < 5 {
		add("--drain-announce must be at least 5s (heartbeat 2s + registry refresh 1s + margin), is %ds", announce)
	}
	if timeout <= announce {
		add("--drain-timeout (%ds) must exceed --drain-announce (%ds)", timeout, announce)
	}
	if tgps < int64(timeout)+10 {
		add("terminationGracePeriodSeconds (%d) must be at least --drain-timeout + 10s", tgps)
	}
	if p.W.Spec.ProgressDeadlineSeconds < 300 {
		add("progressDeadlineSeconds (%d) must be at least 300 (a worker is Ready only after it registers)", p.W.Spec.ProgressDeadlineSeconds)
	}
	if p.Replicas < 2 {
		add("a worker Deployment with %d replica cannot be rolled without losing capacity", p.Replicas)
	}
	return v
}

func TestWorkerPodShape(t *testing.T) {
	m := overlayManifests(t)
	p := m.pod(t, "Deployment/mock-worker")
	for _, v := range workerViolations(p) {
		t.Error(v)
	}
	if p.Replicas != 3 {
		t.Errorf("%d mock workers, want 3 (D8)", p.Replicas)
	}
	// The workers' Deployment must not be selected by a Service that would route client traffic around the scheduler.
	for _, s := range m.Services {
		if s.Spec.Selector["app.kubernetes.io/component"] == "mock-worker" {
			t.Errorf("service %s selects the worker pods; the gateway dials pod IPs from the registry, a Service would bypass the scheduler", s.Metadata.Name)
		}
	}
}

func TestWorkerPodMutations(t *testing.T) {
	type mutation struct {
		name  string
		want  string
		apply func(p *pod)
	}
	ag := func(p *pod) *container { return &p.Spec.Containers[1] }
	mw := func(p *pod) *container { return &p.Spec.Containers[0] }
	setEnv := func(c *container, name, val string) {
		for i := range c.Env {
			if c.Env[i].Name == name {
				c.Env[i].Value = val
			}
		}
	}
	dropEnv := func(c *container, name string) {
		var out []envVar
		for _, e := range c.Env {
			if e.Name != name {
				out = append(out, e)
			}
		}
		c.Env = out
	}
	swapFirstTwo := func(c *container) { c.Env[0], c.Env[1] = c.Env[1], c.Env[0] }
	muts := []mutation{
		{"the worker ID is a fixed name", "--worker-id", func(p *pod) {
			mw(p).Args = []string{"--addr=0.0.0.0:9000", "--worker-id=mock-1", "--drain-announce=6s", "--drain-timeout=30s"}
		}},
		{"the agent registers another ID", "SERVERFLOW_WORKER_ID", func(p *pod) { setEnv(ag(p), "SERVERFLOW_WORKER_ID", "mock-1") }},
		{"the agent advertises localhost", "SERVERFLOW_WORKER_ADVERTISE_URL", func(p *pod) { setEnv(ag(p), "SERVERFLOW_WORKER_ADVERTISE_URL", "http://127.0.0.1:9000") }},
		{"the agent advertises a Service name", "SERVERFLOW_WORKER_ADVERTISE_URL", func(p *pod) { setEnv(ag(p), "SERVERFLOW_WORKER_ADVERTISE_URL", "http://mock-worker:9000") }},
		{"POD_IP missing", "POD_IP", func(p *pod) { dropEnv(ag(p), "POD_IP") }},
		{"POD_NAME is a literal", "POD_NAME from the downward API", func(p *pod) {
			for i := range ag(p).Env {
				if ag(p).Env[i].Name == "POD_NAME" {
					ag(p).Env[i].ValueFrom = nil
					ag(p).Env[i].Value = "x"
				}
			}
		}},
		{"POD_NAME defined after its use", "defined after it", func(p *pod) { swapFirstTwo(ag(p)); ag(p).Env = append(ag(p).Env[1:], ag(p).Env[0]) }},
		{"status endpoint on the pod network", "SERVERFLOW_WORKER_STATUS_LISTEN", func(p *pod) { setEnv(ag(p), "SERVERFLOW_WORKER_STATUS_LISTEN", "0.0.0.0:9102") }},
		{"agent readiness is liveness", "agent readiness probe", func(p *pod) { ag(p).ReadinessProbe.Exec.Command[1] = "http://127.0.0.1:9102/healthz" }},
		{"agent without readiness", "agent readiness probe", func(p *pod) { ag(p).ReadinessProbe = nil }},
		{"agent probe on the pod IP", "agent liveness probe", func(p *pod) { ag(p).LivenessProbe.Exec.Command[1] = "http://10.0.0.1:9102/healthz" }},
		{"agent exposes a port", "declares ports", func(p *pod) {
			ag(p).Ports = append(ag(p).Ports, struct {
				Name          string `yaml:"name"`
				ContainerPort int    `yaml:"containerPort"`
				HostPort      int    `yaml:"hostPort"`
			}{"status", 9102, 0})
		}},
		{"worker listens on loopback", "must listen on", func(p *pod) { mw(p).Args[0] = "--addr=127.0.0.1:9000" }},
		{"no drain announce", "--drain-announce must be at least 5s", func(p *pod) { mw(p).Args = append(mw(p).Args[:6:6], "--drain-timeout=30s") }},
		{"announce 2s", "--drain-announce must be at least 5s", func(p *pod) {
			for i, a := range mw(p).Args {
				if strings.HasPrefix(a, "--drain-announce=") {
					mw(p).Args[i] = "--drain-announce=2s"
				}
			}
		}},
		{"termination grace 35", "terminationGracePeriodSeconds", func(p *pod) { g := int64(35); p.Spec.TerminationGracePeriodSeconds = &g }},
		{"worker deadline 120", "progressDeadlineSeconds (120)", func(p *pod) { p.W.Spec.ProgressDeadlineSeconds = 120 }},
		{"one worker", "cannot be rolled", func(p *pod) { p.Replicas = 1 }},
		{"third container", "exactly the containers", func(p *pod) { p.Spec.Containers = append(p.Spec.Containers, container{Name: "extra"}) }},
	}
	for _, mu := range muts {
		p := overlayManifests(t).pod(t, "Deployment/mock-worker")
		mu.apply(&p)
		if v := workerViolations(p); !contains(v, mu.want) {
			t.Errorf("%s: no violation containing %q; got %v", mu.name, mu.want, v)
		}
	}
}

// ---- the entry points and the pod network ----

// entryViolations: the NodePorts of the overlay are the container ports kind maps to 127.0.0.1, and the script's URLs use the
// host ports of those mappings.
func entryViolations(services []service, kc kindConfig, script string) []string {
	var v []string
	hostFor := map[int]int{}
	for _, m := range kc.Nodes[0].ExtraPortMappings {
		hostFor[m.ContainerPort] = m.HostPort
	}
	urlPort := func(name string) int {
		m := regexp.MustCompile(`(?m)^` + name + `=http://127\.0\.0\.1:([0-9]+)$`).FindStringSubmatch(script)
		if m == nil {
			return -1
		}
		n, _ := strconv.Atoi(m[1])
		return n
	}
	for name, urlVar := range map[string]string{"gateway": "GATEWAY_URL", "control-plane": "CONTROL_PLANE_URL", "prometheus": "PROMETHEUS_URL", "grafana": "GRAFANA_URL", "jaeger-ui": "JAEGER_URL"} {
		found := false
		for _, s := range services {
			if s.Metadata.Name != name {
				continue
			}
			found = true
			if s.Spec.Type != "NodePort" || len(s.Spec.Ports) != 1 || s.Spec.Ports[0].NodePort == 0 {
				v = append(v, "service "+name+" must be a NodePort with one fixed nodePort")
				continue
			}
			host, mapped := hostFor[s.Spec.Ports[0].NodePort]
			if !mapped {
				v = append(v, fmtv("service %s: nodePort %d is not mapped by deploy/kubernetes/kind/cluster.yaml", name, s.Spec.Ports[0].NodePort))
			} else if got := urlPort(urlVar); got != host {
				v = append(v, fmtv("scripts/k8s-up.sh %s uses port %d, kind maps nodePort %d to host port %d", urlVar, got, s.Spec.Ports[0].NodePort, host))
			}
		}
		if !found {
			v = append(v, "no Service "+name)
		}
	}
	return v
}

func TestNodePortsMatchKind(t *testing.T) {
	m := overlayManifests(t)
	for _, v := range entryViolations(m.Services, loadKind(t), read(t, "scripts/k8s-up.sh")) {
		t.Error(v)
	}
	// Mutations.
	script := read(t, "scripts/k8s-up.sh")
	clone := func(ss []service) []service {
		out := make([]service, len(ss))
		copy(out, ss)
		for i := range out {
			out[i].Spec.Ports = append(out[i].Spec.Ports[:0:0], ss[i].Spec.Ports...)
		}
		return out
	}
	setNodePort := func(ss []service, name string, port int) {
		for i := range ss {
			if ss[i].Metadata.Name == name {
				ss[i].Spec.Ports[0].NodePort = port
			}
		}
	}
	for name, c := range map[string]struct {
		svc    func([]service)
		script string
		want   string
	}{
		"unmapped nodePort":       {func(ss []service) { setNodePort(ss, "gateway", 30081) }, script, "not mapped"},
		"gateway not a nodePort":  {func(ss []service) { setNodePort(ss, "gateway", 0) }, script, "fixed nodePort"},
		"swapped ports":           {func(ss []service) { setNodePort(ss, "gateway", 30090); setNodePort(ss, "control-plane", 30080) }, script, "kind maps nodePort"},
		"script on the wrong URL": {func([]service) {}, strings.Replace(script, "GATEWAY_URL=http://127.0.0.1:28080", "GATEWAY_URL=http://127.0.0.1:18080", 1), "GATEWAY_URL uses port 18080"},
		"script lost the URL":     {func([]service) {}, strings.Replace(script, "CONTROL_PLANE_URL=http://127.0.0.1:29090", "", 1), "CONTROL_PLANE_URL uses port -1"},
	} {
		ss := clone(m.Services)
		c.svc(ss)
		if v := entryViolations(ss, loadKind(t), c.script); !contains(v, c.want) {
			t.Errorf("%s: no violation containing %q; got %v", name, c.want, v)
		}
	}
}

func TestWorkerNetworksIsThePodSubnet(t *testing.T) {
	docs := overlayManifests(t).Raw
	cm := findConfigMap(docs, "gateway-config")
	if cm == nil {
		t.Fatal("no gateway-config")
	}
	got := asStr(at(cm, "data", "SERVERFLOW_GATEWAY_WORKER_NETWORKS"))
	if want := loadKind(t).Networking.PodSubnet; got != want {
		t.Errorf("SERVERFLOW_GATEWAY_WORKER_NETWORKS is %q, the kind pod subnet is %q (D11: the gateway dials only the pod network)", got, want)
	}
	if got == "" || got == "0.0.0.0/0" {
		t.Errorf("worker_networks %q allows everything", got)
	}
}

// ---- names honoured by the binaries ----

var upperToken = regexp.MustCompile(`"([A-Z][A-Z0-9_]*)"`)

// configTokens are the upper-case quoted names of internal/config's non-test sources (the suffixes after SERVERFLOW_).
func configTokens(t testing.TB) map[string]bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root(t), "internal/config/*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("internal/config: %v", err)
	}
	out := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range upperToken.FindAllStringSubmatch(string(b), -1) {
			out[m[1]] = true
		}
	}
	return out
}

// honoured: is SERVERFLOW_<suffix> read by internal/config? Some groups read their keys under a common prefix
// (SERVERFLOW_RESILIENCE_ + MODE), so the suffix after the group is accepted too.
func honoured(tokens map[string]bool, name string) bool {
	rest, ok := strings.CutPrefix(name, "SERVERFLOW_")
	if !ok {
		return true // not ours (GOMEMLIMIT, POD_NAME, ...)
	}
	if tokens[rest] {
		return true
	}
	for _, g := range []string{"RESILIENCE_", "TRACING_"} {
		if r, ok := strings.CutPrefix(rest, g); ok && tokens[r] {
			return true
		}
	}
	return false
}

// flagNames are the flags of the mock worker (internal/mockworker/flags.go).
func mockWorkerFlags(t testing.TB) map[string]bool {
	out := map[string]bool{}
	for _, m := range regexp.MustCompile(`fs\.\w+Var\(&[\w.]+, "([a-z-]+)"`).FindAllStringSubmatch(read(t, "internal/mockworker/flags.go"), -1) {
		out[m[1]] = true
	}
	if len(out) < 10 {
		t.Fatalf("found %d mock-worker flags, the parser is broken", len(out))
	}
	return out
}

// nameViolations: every SERVERFLOW_* variable of the render (container env and generated ConfigMaps) is one internal/config
// reads, and every mock-worker flag exists. A misspelled name would be ignored silently by the binary.
func nameViolations(t testing.TB, docs []map[string]any, pods []pod) []string {
	var v []string
	tokens := configTokens(t)
	for _, d := range docs {
		if d["kind"] != "ConfigMap" {
			continue
		}
		if data, ok := at(d, "data").(map[string]any); ok {
			for k := range data {
				if !honoured(tokens, k) {
					v = append(v, fmtv("ConfigMap %s: %s is not read by internal/config", asStr(at(d, "metadata", "name")), k))
				}
			}
		}
	}
	flags := mockWorkerFlags(t)
	for _, p := range pods {
		for _, c := range p.all() {
			for _, e := range c.Env {
				if !honoured(tokens, e.Name) {
					v = append(v, fmtv("%s container %s: env %s is not read by internal/config", p.id(), c.Name, e.Name))
				}
			}
			if c.Name == "mock-worker" {
				for _, a := range c.Args {
					name, _, _ := strings.Cut(strings.TrimPrefix(a, "--"), "=")
					if !strings.HasPrefix(a, "--") || !flags[name] {
						v = append(v, fmtv("%s: the argument %q is not a mock-worker flag", p.id(), a))
					}
				}
			}
		}
	}
	return v
}

func TestEnvironmentNamesAreHonoured(t *testing.T) {
	m := overlayManifests(t)
	for _, v := range nameViolations(t, m.Raw, m.Pods) {
		t.Error(v)
	}
	// Mutations: a misspelled name, in a ConfigMap, in a container, and a misspelled flag.
	docs := cloneRaw(t, m.Raw)
	data := at(findConfigMap(docs, "gateway-config"), "data").(map[string]any)
	data["SERVERFLOW_RESILIENCE_MOD"] = "on"
	data["SERVERFLOW_GATEWAY_WORKER_SORCE"] = "registry"
	if v := nameViolations(t, docs, nil); !contains(v, "SERVERFLOW_RESILIENCE_MOD ") || !contains(v, "SERVERFLOW_GATEWAY_WORKER_SORCE") {
		t.Errorf("misspelled ConfigMap names not caught: %v", v)
	}
	p := m.pod(t, "Deployment/mock-worker")
	p.Spec.Containers[1].Env = append(p.Spec.Containers[1].Env, envVar{Name: "SERVERFLOW_WORKER_ADVERTISE_ULR", Value: "x"})
	p.Spec.Containers[0].Args = append(p.Spec.Containers[0].Args, "--drain-anounce=6s")
	v := nameViolations(t, nil, []pod{p})
	if !contains(v, "SERVERFLOW_WORKER_ADVERTISE_ULR") || !contains(v, "--drain-anounce=6s") {
		t.Errorf("misspelled env and flag not caught: %v", v)
	}
}

// ---- tools/admin-pod.yaml ----

// adminPod returns the one-off pod template with the placeholders filled the way scripts/k8s-up.sh fills them.
func adminPod(t testing.TB, name, args string) pod {
	t.Helper()
	text := read(t, "deploy/kubernetes/tools/admin-pod.yaml")
	text = strings.ReplaceAll(text, "__ADMIN_POD_NAME__", name)
	text = strings.ReplaceAll(text, "__ADMIN_ARGS__", args)
	var w struct {
		Kind     string  `yaml:"kind"`
		Metadata objMeta `yaml:"metadata"`
		Spec     podSpec `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(text), &w); err != nil {
		t.Fatalf("admin-pod.yaml with %s: %v", args, err)
	}
	p := pod{Kind: w.Kind, Name: w.Metadata.Name, Replicas: 1, Labels: w.Metadata.Labels, Spec: w.Spec}
	p.W.Metadata = w.Metadata
	return p
}

func TestAdminPodTemplateIsHardened(t *testing.T) {
	p := adminPod(t, "admin-1-2", `["tenant","list"]`)
	if p.Kind != "Pod" {
		t.Fatalf("the template is a %s, want Pod", p.Kind)
	}
	created := createdSecrets(t)
	var v []string
	for _, f := range []func(pod) []string{checkSecurity, checkResources, checkLabels, checkVolumes} {
		v = append(v, f(p)...)
	}
	v = append(v, checkEnvAndSecrets(p, created)...)
	v = append(v, checkImages(t, p)...)
	for _, x := range v {
		t.Error(x)
	}
	if p.Spec.RestartPolicy != "Never" || p.Spec.ActiveDeadlineSeconds == 0 {
		t.Error("the admin pod must be restartPolicy Never with an activeDeadlineSeconds")
	}
	// Its image tag is the overlay's.
	if !strings.HasSuffix(p.Spec.Containers[0].Image, ":dev") {
		t.Errorf("the admin pod's image %q must use the tag the script loads (dev)", p.Spec.Containers[0].Image)
	}
	// The template is not part of any kustomization (it is created per use, with a unique name).
	if strings.Contains(read(t, "deploy/kubernetes/base/kustomization.yaml"), "admin-pod") || strings.Contains(string(render(t, overlay)), "__ADMIN_") {
		t.Error("the admin pod template must not be rendered by kustomize")
	}
	// The mutated template fails the same checks (a template that cannot fail proves nothing).
	bad := adminPod(t, "admin-1-2", `["tenant","list"]`)
	bad.Spec.Containers[0].SecurityContext.RunAsUser = nil
	bad.Spec.SecurityContext.RunAsUser = nil
	if !contains(checkSecurity(bad), "non-zero runAsUser") {
		t.Error("an admin pod without runAsUser is not caught")
	}
}

// ---- script behaviour, run against stubs ----

// extractFunc returns the text of a top-level shell function of scripts/k8s-up.sh.
func extractFunc(t testing.TB, name string) string {
	t.Helper()
	script := read(t, "scripts/k8s-up.sh")
	start := strings.Index(script, "\n"+name+"() {")
	if start < 0 {
		t.Fatalf("scripts/k8s-up.sh has no function %s", name)
	}
	end := strings.Index(script[start+1:], "\n}\n")
	if end < 0 {
		t.Fatalf("function %s is not closed", name)
	}
	return script[start+1 : start+1+end+3]
}

// TestAdminRunBuildsAHardenedPodAndRefusesOddArguments runs admin_run against a stub kc: the pod it would create is judged by
// the same policies as the workloads, an argument outside [A-Za-z0-9._:-] is refused before anything is created, and a failed
// pod's log is not printed if it looks like a key.
func TestAdminRunBuildsAHardenedPodAndRefusesOddArguments(t *testing.T) {
	for _, bash := range bashes(t) {
		dir := t.TempDir()
		created := filepath.Join(dir, "created.yaml")
		calls := filepath.Join(dir, "calls")
		harness := `set -u
NAMESPACE=serverflow
ADMIN_POD="$ROOT/deploy/kubernetes/tools/admin-pod.yaml"
die() { printf 'DIE: %s\n' "$*" >&2; exit 1; }
kc() {
  printf '%s\n' "$*" >>"$CALLS"
  case "$*" in
    "create -f -") cat >"$CREATED" ;;
    *"get pod"*) printf '%s' "${PHASE:-Succeeded}" ;;
    *"logs"*) printf '%s\n' "${LOG:-}" ;;
  esac
  return 0
}
` + extractFunc(t, "admin_run") + `
admin_run "$@"
`
		file := filepath.Join(dir, "harness.sh")
		if err := os.WriteFile(file, []byte(harness), 0o600); err != nil {
			t.Fatal(err)
		}
		run := func(env []string, args ...string) (string, error) {
			_ = os.Remove(created)
			_ = os.Remove(calls)
			cmd := execCommand(bash, append([]string{file}, args...)...)
			cmd.Dir = root(t)
			cmd.Env = append([]string{"PATH=/usr/bin:/bin", "ROOT=" + root(t), "CREATED=" + created, "CALLS=" + calls}, env...)
			out, err := cmd.CombinedOutput()
			return string(out), err
		}
		out, err := run([]string{"LOG=tenant-line"}, "key", "create", "--tenant", "dev", "--label", "k8s-up")
		if err != nil || !strings.Contains(out, "tenant-line") {
			t.Fatalf("%s: a good call failed: %v %q", bash, err, out)
		}
		b, _ := os.ReadFile(created)
		var w struct {
			Metadata objMeta `yaml:"metadata"`
			Spec     struct {
				Containers []struct {
					Args []string `yaml:"args"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal(b, &w); err != nil {
			t.Fatalf("%s: the pod it created is not YAML: %v\n%s", bash, err, b)
		}
		if got := strings.Join(w.Spec.Containers[0].Args, " "); got != "key create --tenant dev --label k8s-up" {
			t.Errorf("%s: the pod's args are %q", bash, got)
		}
		if !regexp.MustCompile(`^admin-[0-9]+-[0-9]+$`).MatchString(w.Metadata.Name) {
			t.Errorf("%s: the pod name is %q", bash, w.Metadata.Name)
		}
		c, _ := os.ReadFile(calls)
		if !strings.Contains(string(c), "delete pod "+w.Metadata.Name) {
			t.Errorf("%s: the pod is not deleted afterwards: %s", bash, c)
		}
		// The created pod passes the policies.
		p := adminPod(t, w.Metadata.Name, `["x"]`)
		if v := checkSecurity(p); len(v) != 0 {
			t.Errorf("%s: %v", bash, v)
		}
		for _, bad := range []string{"a b", "x;y", "$(id)", "a|b", "`id`", "a\"b", "a&b", "a\\b", "", "a'b", "a\nb", "a>b", "*", "~", "a=b"} {
			out, err := run(nil, "tenant", "create", bad)
			if err == nil || !strings.Contains(out, "refusing the argument") {
				t.Errorf("%s: the argument %q was accepted: %q", bash, bad, out)
			}
			if _, e := os.Stat(created); e == nil {
				t.Errorf("%s: a pod was created for the argument %q", bash, bad)
			}
		}
		// A failed pod: nonzero, and a line that looks like an API key is not echoed.
		out, err = run([]string{"PHASE=Failed", "LOG=sf_live_abcdefghijklmnopqrstuvwxyz0123456789"}, "key", "create", "--tenant", "dev")
		if err == nil {
			t.Errorf("%s: a failed pod was reported as success", bash)
		}
		if strings.Contains(out, "sf_live_") {
			t.Errorf("%s: a key-looking log line was printed", bash)
		}
	}
}

// auditStub answers the two config reads of guard_context and the pod list of the audit.
func runAudit(t *testing.T, bash, rows string) (string, error) {
	t.Helper()
	dir, _ := fakeKubectl(t)
	// Replace the stub with one that also prints FAKE_PODS for `get pods`.
	stub := `#!/bin/sh
case "$*" in
  *"config current-context"*) printf 'kind-serverflow\n' ;;
  *"config view --minify"*) printf 'https://127.0.0.1:41234\n' ;;
  *"get pods"*) printf '%s' "$FAKE_PODS" ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	state := stateDir(t)
	if err := os.WriteFile(filepath.Join(state, "kubeconfig"), []byte("# not read\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := execCommand(bash, "-u", filepath.Join(root(t), "scripts/k8s-up.sh"), "audit")
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin", "HOME=" + t.TempDir(), "K8S_UP_DIR=" + state, "FAKE_PODS=" + rows}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestAuditJudgesWhatTheAPIReports(t *testing.T) {
	const good = `gateway-abc|false|65532|true|RuntimeDefault|tmp,|gateway,,,true,false,,["ALL"],,,100m,64Mi,128Mi,;` + "\n" +
		`mock-worker-abc|false|65532|true|RuntimeDefault||mock-worker,,,true,false,,["ALL"],,,50m,32Mi,64Mi,;worker-agent,65532,,true,false,,["ALL"],,RuntimeDefault,25m,16Mi,32Mi,;` + "\n"
	for _, bash := range bashes(t) {
		out, err := runAudit(t, bash, good)
		if err != nil || !strings.Contains(out, "audit: 2 pods, 3 containers, 0 exceptions") || !strings.Contains(out, "sum of the memory limits of the running containers: 224 MiB") {
			t.Fatalf("%s: the good rows were not accepted: %v\n%s", bash, err, out)
		}
		mutate := map[string]struct{ from, to, want string }{
			"root user":                {"|65532|true|RuntimeDefault|tmp,|gateway,,,", "|0|true|RuntimeDefault|tmp,|gateway,0,,", "runs as root"},
			"no user at all":           {"gateway-abc|false|65532|true|RuntimeDefault|tmp,|", "gateway-abc|false||true|RuntimeDefault|tmp,|", "runs as root or has no explicit"},
			"not runAsNonRoot":         {"gateway-abc|false|65532|true|", "gateway-abc|false|65532||", "runAsNonRoot"},
			"writable root":            {"gateway,,,true,false", "gateway,,,false,false", "readOnlyRootFilesystem"},
			"privilege escalation":     {"gateway,,,true,false", "gateway,,,true,true", "allowPrivilegeEscalation"},
			"privileged":               {"gateway,,,true,false,,", "gateway,,,true,false,true,", "privileged"},
			"capability not dropped":   {`gateway,,,true,false,,["ALL"]`, `gateway,,,true,false,,["NET_RAW"]`, "does not drop exactly ALL"},
			"capability added":         {`gateway,,,true,false,,["ALL"],,`, `gateway,,,true,false,,["ALL"],NET_ADMIN,`, "adds capabilities"},
			"no seccomp":               {"gateway-abc|false|65532|true|RuntimeDefault", "gateway-abc|false|65532|true|", "seccomp is not RuntimeDefault"},
			"no requests":              {"100m,64Mi,128Mi", ",,128Mi", "no cpu or memory request"},
			"no memory limit":          {"100m,64Mi,128Mi", "100m,64Mi,", "no memory limit"},
			"cpu limit":                {"100m,64Mi,128Mi,;", "100m,64Mi,128Mi,500m;", "CPU limit"},
			"token mounted":            {"gateway-abc|false|65532|true|RuntimeDefault|tmp,|", "gateway-abc|false|65532|true|RuntimeDefault|kube-api-access-x,|", "API token volume"},
			"automount on":             {"gateway-abc|false|", "gateway-abc|true|", "automountServiceAccountToken"},
			"automount unset":          {"gateway-abc|false|", "gateway-abc||", "automountServiceAccountToken"},
			"container seccomp weaker": {"worker-agent,65532,,true,false,,[\"ALL\"],,RuntimeDefault", "worker-agent,65532,,true,false,,[\"ALL\"],,Unconfined", "seccomp is not RuntimeDefault"},
		}
		for name, m := range mutate {
			if !strings.Contains(good, m.from) {
				t.Fatalf("test bug: %q is not in the good rows (%s)", m.from, name)
			}
			out, err := runAudit(t, bash, strings.Replace(good, m.from, m.to, 1))
			if err == nil || !strings.Contains(out, "EXCEPTION") || !strings.Contains(out, m.want) {
				t.Errorf("%s: %s was not caught (%v):\n%s", bash, name, err, out)
			}
		}
		if out, err := runAudit(t, bash, ""); err == nil {
			t.Errorf("%s: an empty namespace passed the audit: %s", bash, out)
		}
	}
}

// ---- the shape of the smoke and the bootstrap ----

func TestSmokeChecksAndScriptHygiene(t *testing.T) {
	script := read(t, "scripts/k8s-up.sh")
	smoke := extractFunc(t, "cmd_smoke")
	var ids []string
	for _, m := range regexp.MustCompile(`(?m)^  check ([KD][0-9]+) "`).FindAllStringSubmatch(smoke, -1) {
		ids = append(ids, m[1])
	}
	want := []string{"K1", "D2", "D3", "D4", "K2", "K3", "K4", "K5", "K6", "K7", "K8", "K9", "K10", "K11", "K12", "K14", "K15", "D5", "D6", "D7", "K13"}
	if strings.Join(ids, " ") != strings.Join(want, " ") {
		t.Errorf("the smoke runs %v, want %v (K13 last: it searches the logs everything else produced; K8 and K9 follow K7 and K11 follows K10 because they need requests; K12 comes after them because it deletes a worker; K15 follows the requests of K14 and precedes the restarts of D5-D7)", ids, want)
	}
	if !strings.Contains(smoke, `smoke: %d of %d checks passed, %d failed, 0 skipped`) {
		t.Error("the smoke's last line is no longer the contract `smoke: N of N checks passed, 0 failed, 0 skipped`")
	}
	for i, line := range strings.Split(script, "\n") {
		code := strings.TrimSpace(line)
		if strings.HasPrefix(code, "#") {
			continue
		}
		where := "k8s-up.sh:" + itoa(i+1) + ": "
		// No `A && B || C`: C would also run when B fails. (`A || B` and `A && B` alone are fine.)
		if strings.Contains(code, " && ") && strings.Contains(code, " || ") && !strings.HasPrefix(code, "if ") && !strings.HasPrefix(code, "while ") && !strings.HasPrefix(code, "elif ") {
			t.Errorf("%sA && B || C: %s", where, code)
		}
		// A key or token is never an argument of a command: curl reads its header from standard input.
		if regexp.MustCompile(`(-H|--header)\s+["']?Authorization`).MatchString(code) || regexp.MustCompile(`curl[^|]*Bearer`).MatchString(code) {
			t.Errorf("%sa bearer credential on a command line: %s", where, code)
		}
		// Every wait is bounded: no loop without a deadline check.
		if strings.Contains(code, "sleep ") && !strings.Contains(code, "sleep 1") && !strings.Contains(code, "sleep 2") {
			t.Errorf("%sunexpected sleep: %s", where, code)
		}
	}
	for _, fn := range []string{"admin_run", "poll", "wait_job", "restart_pod"} {
		body := extractFunc(t, fn)
		if !strings.Contains(body, "deadline") {
			t.Errorf("%s waits without a deadline", fn)
		}
	}
	// bootstrap: both tenants and both keys.
	b := extractFunc(t, "bootstrap_tenants")
	for _, want := range []string{"tenant create dev --rpm 6000", "tenant create limited --rpm 3", `"$K8S_DIR/api_key"`, `"$K8S_DIR/api_key_limited"`} {
		if !strings.Contains(b, want) {
			t.Errorf("bootstrap_tenants lost %q", want)
		}
	}
	// The secrets the core system reads are the ones the script creates (the policy test checks the manifests; this checks the list).
	for _, s := range []string{"controlplane", "metrics"} {
		found := false
		for _, n := range createdSecrets(t) {
			found = found || n == s
		}
		if !found {
			t.Errorf("SECRETS does not include %s", s)
		}
	}
	// The up sequence applies the core phase after the jobs and bootstraps last.
	up := extractFunc(t, "cmd_up")
	order := []string{"apply_phase foundation", "apply_phase data", "apply_phase jobs", "apply_phase core", "wait_deployment gateway", "bootstrap_tenants"}
	pos := -1
	for _, o := range order {
		i := strings.Index(up, o)
		if i <= pos {
			t.Errorf("cmd_up: %q is missing or out of order", o)
		}
		pos = i
	}
}

func execCommand(name string, args ...string) *exec.Cmd { return exec.Command(name, args...) }

// TestK1JudgesPhaseReadinessAndRestarts runs pods_ready_no_restarts against stubbed pod rows: a Pending pod (no container
// statuses yet) must fail, as must an unready container or a restart; Succeeded Job pods are skipped.
func TestK1JudgesPhaseReadinessAndRestarts(t *testing.T) {
	for _, bash := range bashes(t) {
		run := func(rows string) bool {
			dir := t.TempDir()
			harness := "set -u\nNAMESPACE=serverflow\nkc() { printf '%s' \"$ROWS\"; }\n" + extractFunc(t, "pods_ready_no_restarts") + "\npods_ready_no_restarts\n"
			file := filepath.Join(dir, "h.sh")
			if err := os.WriteFile(file, []byte(harness), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := execCommand(bash, file)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "ROWS=" + rows}
			return cmd.Run() == nil
		}
		good := "gateway-a Running true/0 \nmock-worker-a Running true/0 true/0 \nmigrate-x Succeeded false/0 \n"
		if !run(good) {
			t.Errorf("%s: a healthy namespace failed", bash)
		}
		for name, rows := range map[string]string{
			"pending pod":      good + "gateway-b Pending \n",
			"pending, no data": good + "gateway-b Pending\n",
			"unready":          good + "gateway-b Running false/0 \n",
			"restarted":        good + "gateway-b Running true/1 \n",
			"failed pod":       good + "gateway-b Failed true/0 \n",
			"empty":            "",
		} {
			if run(rows) {
				t.Errorf("%s: %s passed K1", bash, name)
			}
		}
	}
}

// TestK13AndK2Hygiene pins the shape of the node-log leg and of the gateway readiness check.
func TestK13AndK2Hygiene(t *testing.T) {
	k13 := extractFunc(t, "k13_no_secrets_leaked")
	for _, want := range []string{"deadline", "serverflow_admin-", `cat /var/log/pods/*/*/*.log`, "node_hits"} {
		if !strings.Contains(k13, want) {
			t.Errorf("k13 lost %q: the node-log leg polls until the admin pods' logs are collected and fails if their directories remain", want)
		}
	}
	if regexp.MustCompile(`(?m)^[^#]*(grep -v|--exclude|-not -path|! -path)[^#]*pods`).MatchString(k13) {
		t.Error("k13 must not exclude any log directory from the search")
	}
	for _, fn := range []string{"gateway_lists_model", "k2_gateway_ready"} {
		if regexp.MustCompile(`curl[^\n]*\|\s*grep -q`).MatchString(extractFunc(t, fn)) {
			t.Errorf("%s pipes into grep -q under pipefail", fn)
		}
	}
	if regexp.MustCompile(`(?m)^\s*(ready|models)\(\) \{`).MatchString(read(t, "scripts/k8s-up.sh")) {
		t.Error("nested helper functions passed by name trip shellcheck SC2329; define them at top level")
	}
	if regexp.MustCompile(`for i in \$\(seq`).MatchString(extractFunc(t, "k14_shared_quota")) {
		t.Error("k14 loop variable is unused (SC2034): use `for _ in`")
	}
}
