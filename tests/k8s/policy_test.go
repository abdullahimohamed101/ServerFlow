package k8s

import (
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The checks are functions from a parsed pod to a list of violations, so the same code judges the real render
// (TestRenderedPolicy*) and the deliberately broken copies of it (TestPolicyCatchesMutations).

func nonRootUser(p pod, c container) (int64, bool) {
	if c.SecurityContext != nil && c.SecurityContext.RunAsUser != nil {
		return *c.SecurityContext.RunAsUser, true
	}
	if p.Spec.SecurityContext != nil && p.Spec.SecurityContext.RunAsUser != nil {
		return *p.Spec.SecurityContext.RunAsUser, true
	}
	return 0, false
}

// checkSecurity: what Pod Security `restricted` demands, stated explicitly so the manifests are hardened on their own
// terms and not only because the admission controller happens to be on (D17).
func checkSecurity(p pod) []string {
	var v []string
	add := func(format string, a ...any) { v = append(v, p.id()+": "+fmtv(format, a...)) }
	s := p.Spec
	if !boolIs(s.AutomountServiceAccountToken, false) {
		add("automountServiceAccountToken must be false")
	}
	if s.ServiceAccountName == "" || s.ServiceAccountName == "default" {
		add("needs its own ServiceAccount, not the default one")
	}
	if s.HostNetwork || s.HostPID || s.HostIPC {
		add("must not use the host network, PID or IPC namespace")
	}
	if s.ShareProcessNamespace != nil {
		add("shareProcessNamespace must be unset")
	}
	for _, vol := range s.Volumes {
		if vol.HostPath != nil {
			add("volume %s is a hostPath", vol.Name)
		}
	}
	podSeccomp := s.SecurityContext != nil && s.SecurityContext.SeccompProfile != nil && s.SecurityContext.SeccompProfile.Type == "RuntimeDefault"
	podNonRoot := s.SecurityContext != nil && boolIs(s.SecurityContext.RunAsNonRoot, true)
	for _, c := range p.all() {
		who := "container " + c.Name
		sc := c.SecurityContext
		if sc == nil {
			add("%s has no securityContext", who)
			continue
		}
		if boolIs(sc.Privileged, true) {
			add("%s is privileged", who)
		}
		if !boolIs(sc.AllowPrivilegeEscalation, false) {
			add("%s must set allowPrivilegeEscalation: false", who)
		}
		if !boolIs(sc.ReadOnlyRootFilesystem, true) {
			add("%s must set readOnlyRootFilesystem: true", who)
		}
		if sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
			add("%s must drop exactly [ALL] capabilities", who)
		}
		if sc.Capabilities != nil && len(sc.Capabilities.Add) > 0 {
			add("%s adds capabilities %v", who, sc.Capabilities.Add)
		}
		if !(podNonRoot || boolIs(sc.RunAsNonRoot, true)) {
			add("%s must run as non-root (runAsNonRoot: true)", who)
		}
		if uid, ok := nonRootUser(p, c); !ok || uid == 0 {
			add("%s needs an explicit non-zero runAsUser", who)
		}
		if !(podSeccomp || (sc.SeccompProfile != nil && sc.SeccompProfile.Type == "RuntimeDefault")) {
			add("%s must use the RuntimeDefault seccomp profile", who)
		}
		if strings.HasSuffix(c.Image, ":latest") {
			add("%s uses :latest", who)
		}
	}
	return v
}

// checkResources: every container has requests and a memory limit; no CPU limit (CFS throttling is latency, D18).
func checkResources(p pod) []string {
	var v []string
	for _, c := range p.all() {
		who := p.id() + ": container " + c.Name
		for _, r := range []string{"cpu", "memory"} {
			if c.Resources.Requests[r] == "" {
				v = append(v, who+" has no "+r+" request")
			}
		}
		lim, ok := memMiB(c.Resources.Limits["memory"])
		if !ok {
			v = append(v, who+" needs a memory limit in Mi or Gi")
			continue
		}
		if req, ok := memMiB(c.Resources.Requests["memory"]); ok && req > lim {
			v = append(v, who+" requests more memory than its limit")
		}
		if c.Resources.Limits["cpu"] != "" {
			v = append(v, who+" has a CPU limit")
		}
	}
	return v
}

func (c container) probeNamedPortsExist() []string {
	var v []string
	named := map[string]bool{}
	for _, p := range c.Ports {
		named[p.Name] = true
	}
	for name, pr := range map[string]*probe{"startup": c.StartupProbe, "readiness": c.ReadinessProbe, "liveness": c.LivenessProbe} {
		if pr == nil {
			continue
		}
		var port any
		if pr.HTTPGet != nil {
			port = pr.HTTPGet.Port
		}
		if pr.TCPSocket != nil {
			port = pr.TCPSocket.Port
		}
		if s, ok := port.(string); ok && !named[s] {
			v = append(v, "container "+c.Name+": the "+name+" probe names port "+s+", which the container does not declare")
		}
	}
	return v
}

// serviceNames are the in-cluster hostnames a liveness probe must not depend on (D13: liveness never depends on another service).
var serviceNames = []string{"postgres", "redis", "redpanda", "control-plane", "gateway", "jaeger", "prometheus", "grafana", "mock-worker"}

// checkProbes: a long-running container has startup, readiness and liveness probes; the liveness probe asks only about the
// container itself (loopback, or no network at all, and never /readyz); the probe periods are positive and bounded.
func checkProbes(p pod) []string {
	var v []string
	if !p.longRunning() {
		return v
	}
	for _, c := range p.Spec.Containers {
		who := p.id() + ": container " + c.Name
		if c.ReadinessProbe == nil {
			v = append(v, who+" has no readinessProbe")
		}
		if c.LivenessProbe == nil {
			v = append(v, who+" has no livenessProbe")
		}
		if c.StartupProbe == nil {
			v = append(v, who+" has no startupProbe (it absorbs slow starts so liveness can stay short)")
		}
		if lp := c.LivenessProbe; lp != nil {
			if lp.HTTPGet != nil && strings.Contains(lp.HTTPGet.Path, "readyz") {
				v = append(v, who+": liveness must not use /readyz (it depends on other services)")
			}
			if lp.Exec != nil {
				cmd := strings.Join(lp.Exec.Command, " ")
				for _, n := range serviceNames {
					if n != p.Labels["app.kubernetes.io/component"] && regexp.MustCompile(`(^|[^A-Za-z0-9-])`+regexp.QuoteMeta(n)+`(:[0-9]+|\b)`).MatchString(cmd) && !strings.Contains(cmd, "127.0.0.1") {
						v = append(v, who+": the liveness command mentions the service "+n)
					}
				}
				if strings.Contains(cmd, "redpanda:") || strings.Contains(cmd, "postgres:") {
					v = append(v, who+": the liveness command dials a Service name")
				}
			}
			if lp.FailureThreshold < 3 {
				v = append(v, who+": liveness failureThreshold below 3 restarts on a blip")
			}
		}
		for name, pr := range map[string]*probe{"startup": c.StartupProbe, "readiness": c.ReadinessProbe, "liveness": c.LivenessProbe} {
			if pr == nil {
				continue
			}
			if pr.PeriodSeconds < 1 || pr.TimeoutSeconds < 1 || pr.FailureThreshold < 1 {
				v = append(v, who+": the "+name+" probe needs explicit periodSeconds, timeoutSeconds and failureThreshold")
			}
		}
		// The startup probe must allow at least as long as one liveness period: otherwise liveness would restart a slow start.
		if c.StartupProbe != nil && c.LivenessProbe != nil && c.StartupProbe.PeriodSeconds*c.StartupProbe.FailureThreshold < c.LivenessProbe.PeriodSeconds {
			v = append(v, who+": the startup window is shorter than one liveness period")
		}
		v = append(v, c.probeNamedPortsExist()...)
	}
	return v
}

// checkLifecycle: termination grace and rollout settings. A StatefulSet is a single instance; Jobs end.
func checkLifecycle(p pod) []string {
	var v []string
	id := p.id()
	if p.longRunning() {
		if p.Spec.TerminationGracePeriodSeconds == nil || *p.Spec.TerminationGracePeriodSeconds < 30 {
			v = append(v, id+": terminationGracePeriodSeconds must be set and at least 30 (a data store needs time to flush)")
		}
	}
	switch p.Kind {
	case "StatefulSet":
		if p.Replicas != 1 {
			v = append(v, id+": the data tier is a single instance (no HA claim); replicas must be 1")
		}
		if p.W.Spec.ServiceName == "" {
			v = append(v, id+": needs serviceName")
		}
		for _, c := range p.W.Spec.VolumeClaimTemplates {
			q, ok := memMiB(c.Spec.Resources.Requests["storage"])
			if !ok || q > 2048 {
				v = append(v, id+": the volume claim "+c.Metadata.Name+" must request at most 2Gi")
			}
			if len(c.Spec.AccessModes) != 1 || c.Spec.AccessModes[0] != "ReadWriteOnce" {
				v = append(v, id+": the volume claim "+c.Metadata.Name+" must be ReadWriteOnce")
			}
		}
	case "Job":
		if p.Spec.RestartPolicy != "Never" && p.Spec.RestartPolicy != "OnFailure" {
			v = append(v, id+": a Job needs restartPolicy Never or OnFailure")
		}
		if p.W.Spec.BackoffLimit == nil || p.W.Spec.ActiveDeadlineSeconds == nil {
			v = append(v, id+": a Job needs backoffLimit and activeDeadlineSeconds so it cannot run forever")
		}
	}
	return v
}

// checkVolumes: every emptyDir is size-limited; every mount names a volume or claim of the pod.
func checkVolumes(p pod) []string {
	var v []string
	have := map[string]bool{}
	for _, c := range p.W.Spec.VolumeClaimTemplates {
		have[c.Metadata.Name] = true
	}
	for _, vol := range p.Spec.Volumes {
		have[vol.Name] = true
		if vol.EmptyDir != nil && vol.EmptyDir.SizeLimit == "" {
			v = append(v, p.id()+": emptyDir "+vol.Name+" has no sizeLimit")
		}
	}
	for _, c := range p.all() {
		for _, m := range c.VolumeMounts {
			if !have[m.Name] {
				v = append(v, p.id()+": container "+c.Name+" mounts "+m.Name+", which the pod does not define")
			}
		}
	}
	return v
}

var secretName = regexp.MustCompile(`(?i)pass|token|secret|key|credential|dsn`)

// checkEnvAndSecrets: no literal secret in env; every Secret a pod reads is one scripts/k8s-up.sh creates.
func checkEnvAndSecrets(p pod, created []string) []string {
	var v []string
	known := map[string]bool{}
	for _, n := range created {
		known[n] = true
	}
	for _, c := range p.all() {
		who := p.id() + ": container " + c.Name
		for _, e := range c.Env {
			if secretName.MatchString(e.Name) && e.Value != "" && e.Value != "true" && e.Value != "false" {
				v = append(v, who+": env "+e.Name+" has a literal value; secrets come from a Secret")
			}
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && !known[e.ValueFrom.SecretKeyRef.Name] {
				v = append(v, who+": env "+e.Name+" reads the Secret "+e.ValueFrom.SecretKeyRef.Name+", which k8s-up.sh does not create")
			}
		}
		for _, ef := range c.EnvFrom {
			if ef.SecretRef != nil && !known[ef.SecretRef.Name] {
				v = append(v, who+": envFrom reads the Secret "+ef.SecretRef.Name+", which k8s-up.sh does not create")
			}
		}
		for _, a := range append(append([]string{}, c.Command...), c.Args...) {
			if secretLiteral.MatchString(a) {
				v = append(v, who+": a command argument looks like a secret literal")
			}
		}
	}
	return v
}

var secretLiteral = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|credential)[A-Za-z_]*\s*[:=]\s*["']?[A-Za-z0-9+/_.-]{12,}["']?\s*$`)

// checkLabels: the recommended labels on the object and its pod template, and a selector that the template satisfies.
func checkLabels(p pod) []string {
	var v []string
	for _, l := range []string{"app.kubernetes.io/name", "app.kubernetes.io/component", "app.kubernetes.io/part-of"} {
		if p.Labels[l] == "" {
			v = append(v, p.id()+": pod template lacks the label "+l)
		}
		if p.W.Metadata.Labels[l] == "" {
			v = append(v, p.id()+": object lacks the label "+l)
		}
	}
	if p.Kind != "Job" {
		if len(p.W.Spec.Selector.MatchLabels) == 0 {
			v = append(v, p.id()+": no selector")
		}
		for k, val := range p.W.Spec.Selector.MatchLabels {
			if p.Labels[k] != val {
				v = append(v, p.id()+": the selector label "+k+" does not match the pod template")
			}
		}
	}
	return v
}

var allChecks = map[string]func(pod) []string{
	"security":  checkSecurity,
	"resources": checkResources,
	"probes":    checkProbes,
	"lifecycle": checkLifecycle,
	"volumes":   checkVolumes,
	"labels":    checkLabels,
}

func runChecks(t testing.TB, m manifests) []string {
	var v []string
	created := createdSecrets(t)
	for _, p := range m.Pods {
		for _, f := range allChecks {
			v = append(v, f(p)...)
		}
		v = append(v, checkEnvAndSecrets(p, created)...)
		v = append(v, checkImages(t, p)...)
	}
	sort.Strings(v)
	return v
}

func TestRenderedPodsAreHardenedAndComplete(t *testing.T) {
	m := overlayManifests(t)
	if len(m.Pods) < 5 {
		t.Fatalf("expected at least PostgreSQL, Redis, Redpanda and the two jobs, found %d pod templates", len(m.Pods))
	}
	for _, v := range runChecks(t, m) {
		t.Error(v)
	}
}

func TestPodSecurityPolicy(t *testing.T) {
	for _, p := range overlayManifests(t).Pods {
		for _, v := range checkSecurity(p) {
			t.Error(v)
		}
	}
}

func TestResourcesPolicy(t *testing.T) {
	for _, p := range overlayManifests(t).Pods {
		for _, v := range checkResources(p) {
			t.Error(v)
		}
	}
}

func TestProbePolicy(t *testing.T) {
	m := overlayManifests(t)
	for _, p := range m.Pods {
		for _, v := range checkProbes(p) {
			t.Error(v)
		}
	}
	// The three data stores are long-running and must be probed (a Job is not).
	for _, id := range []string{"StatefulSet/postgres", "StatefulSet/redis", "StatefulSet/redpanda"} {
		p := m.pod(t, id)
		c := p.Spec.Containers[0]
		if c.StartupProbe == nil || c.ReadinessProbe == nil || c.LivenessProbe == nil {
			t.Errorf("%s: probes missing", id)
		}
	}
}

func TestLifecycleAndVolumes(t *testing.T) {
	for _, p := range overlayManifests(t).Pods {
		for _, v := range append(checkLifecycle(p), append(checkVolumes(p), checkLabels(p)...)...) {
			t.Error(v)
		}
	}
}

// TestMemoryFitsTheEngineMinimum: the sum of memory limits plus about 1 GiB for the kind node must fit in the engine memory
// that scripts/k8s-up.sh demands (D18, D3). It grows with each pull request; the number is logged.
func TestMemoryFitsTheEngineMinimum(t *testing.T) {
	total := 0
	for _, p := range overlayManifests(t).Pods {
		for _, c := range p.all() {
			lim, _ := memMiB(c.Resources.Limits["memory"])
			total += lim * p.Replicas
		}
	}
	min := scriptInt(t, "MIN_MEMORY_MB")
	t.Logf("sum of memory limits %d MiB (every pod counted, jobs included) + 1024 MiB node = %d MiB; the engine must have %d MiB", total, total+1024, min)
	if total+1024 > min {
		t.Errorf("limits (%d MiB) plus the node (1024 MiB) exceed the engine memory k8s-up.sh demands (%d MiB)", total, min)
	}
}

func scriptInt(t testing.TB, name string) int {
	t.Helper()
	m := regexp.MustCompile(`(?m)^` + name + `=([0-9]+)$`).FindStringSubmatch(read(t, "scripts/k8s-up.sh"))
	if m == nil {
		t.Fatalf("scripts/k8s-up.sh does not define %s", name)
	}
	n := 0
	for _, ch := range m[1] {
		n = n*10 + int(ch-'0')
	}
	return n
}

func scriptWords(t testing.TB, name string) []string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^` + name + `="([^"]*)"`).FindStringSubmatch(read(t, "scripts/k8s-up.sh"))
	if m == nil {
		t.Fatalf("scripts/k8s-up.sh does not define %s", name)
	}
	return strings.Fields(m[1])
}

func createdSecrets(t testing.TB) []string { return scriptWords(t, "SECRETS") }
