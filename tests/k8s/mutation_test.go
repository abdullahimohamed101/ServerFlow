package k8s

import (
	"strings"
	"testing"
)

// TestPolicyCatchesMutations breaks a fresh parse of the real render in one way at a time and asserts that the policy names
// it. A policy that cannot fail would let the deployment drift unnoticed.
func TestPolicyCatchesMutations(t *testing.T) {
	type mutation struct {
		name  string
		want  string // a substring of a violation
		apply func(p *pod)
	}
	first := func(p *pod) *container { return &p.Spec.Containers[0] }
	i64 := func(n int64) *int64 { return &n }
	bl := func(b bool) *bool { return &b }
	postgres := "StatefulSet/postgres"
	mutations := []struct {
		pod string
		mutation
	}{
		{postgres, mutation{"run as root", "non-zero runAsUser", func(p *pod) { first(p).SecurityContext.RunAsUser = i64(0); p.Spec.SecurityContext.RunAsUser = i64(0) }}},
		{postgres, mutation{"no runAsUser at all", "non-zero runAsUser", func(p *pod) { first(p).SecurityContext.RunAsUser = nil; p.Spec.SecurityContext.RunAsUser = nil }}},
		{postgres, mutation{"not runAsNonRoot", "runAsNonRoot", func(p *pod) { p.Spec.SecurityContext.RunAsNonRoot = bl(false) }}},
		{postgres, mutation{"privileged", "privileged", func(p *pod) { first(p).SecurityContext.Privileged = bl(true) }}},
		{postgres, mutation{"privilege escalation allowed", "allowPrivilegeEscalation", func(p *pod) { first(p).SecurityContext.AllowPrivilegeEscalation = bl(true) }}},
		{postgres, mutation{"writable root file system", "readOnlyRootFilesystem", func(p *pod) { first(p).SecurityContext.ReadOnlyRootFilesystem = bl(false) }}},
		{postgres, mutation{"capability not dropped", "drop exactly [ALL]", func(p *pod) { first(p).SecurityContext.Capabilities.Drop = []string{"NET_RAW"} }}},
		{postgres, mutation{"capability added", "adds capabilities", func(p *pod) { first(p).SecurityContext.Capabilities.Add = []string{"NET_ADMIN"} }}},
		{postgres, mutation{"no seccomp", "seccomp", func(p *pod) { p.Spec.SecurityContext.SeccompProfile = nil }}},
		{postgres, mutation{"token mounted", "automountServiceAccountToken", func(p *pod) { p.Spec.AutomountServiceAccountToken = bl(true) }}},
		{postgres, mutation{"token default unset", "automountServiceAccountToken", func(p *pod) { p.Spec.AutomountServiceAccountToken = nil }}},
		{postgres, mutation{"default service account", "ServiceAccount", func(p *pod) { p.Spec.ServiceAccountName = "default" }}},
		{postgres, mutation{"host network", "host network", func(p *pod) { p.Spec.HostNetwork = true }}},
		{postgres, mutation{"hostPath", "hostPath", func(p *pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, volume{Name: "h", HostPath: &struct{ Path string }{"/"}})
		}}},
		{postgres, mutation{"latest tag", ":latest", func(p *pod) { first(p).Image = "mirror.gcr.io/library/postgres:latest" }}},
		{postgres, mutation{"tag without digest", "pinned as host/repo:tag@sha256", func(p *pod) { first(p).Image = "mirror.gcr.io/library/postgres:16-alpine" }}},
		{postgres, mutation{"digest differs from compose", "differs from docker-compose.yml", func(p *pod) {
			// Flip the last hex digit of whatever digest the manifest carries now (a Dependabot bump changes it).
			img := first(p).Image
			last := img[len(img)-1]
			flipped := byte('0')
			if last == '0' {
				flipped = '1'
			}
			first(p).Image = img[:len(img)-1] + string(flipped)
		}}},
		{postgres, mutation{"image compose does not pin", "does not pin this repository", func(p *pod) {
			first(p).Image = "mirror.gcr.io/library/mariadb:11@sha256:" + strings.Repeat("a", 64)
		}}},
		{postgres, mutation{"Docker Hub", "Docker Hub", func(p *pod) {
			// The same digest as the manifest's, on Docker Hub.
			_, digest, _ := strings.Cut(first(p).Image, "@")
			first(p).Image = "docker.io/library/postgres:16-alpine@" + digest
		}}},
		{postgres, mutation{"pull always", "imagePullPolicy", func(p *pod) { first(p).ImagePullPolicy = "Always" }}},
		{postgres, mutation{"no memory limit", "memory limit", func(p *pod) { delete(first(p).Resources.Limits, "memory") }}},
		{postgres, mutation{"no cpu request", "no cpu request", func(p *pod) { delete(first(p).Resources.Requests, "cpu") }}},
		{postgres, mutation{"cpu limit", "CPU limit", func(p *pod) { first(p).Resources.Limits["cpu"] = "500m" }}},
		{postgres, mutation{"request above limit", "requests more memory", func(p *pod) { first(p).Resources.Requests["memory"] = "512Mi" }}},
		{postgres, mutation{"no readiness", "no readinessProbe", func(p *pod) { first(p).ReadinessProbe = nil }}},
		{postgres, mutation{"no liveness", "no livenessProbe", func(p *pod) { first(p).LivenessProbe = nil }}},
		{postgres, mutation{"no startup", "no startupProbe", func(p *pod) { first(p).StartupProbe = nil }}},
		{postgres, mutation{"liveness on readyz", "must not use /readyz", func(p *pod) {
			first(p).LivenessProbe.Exec = nil
			first(p).LivenessProbe.HTTPGet = &httpGet{Path: "/readyz", Port: "postgres"}
		}}},
		{postgres, mutation{"liveness dials a service", "mentions the service", func(p *pod) {
			first(p).LivenessProbe.Exec.Command = []string{"redis-cli", "-h", "redis", "ping"}
		}}},
		{postgres, mutation{"probe names a missing port", "does not declare", func(p *pod) {
			first(p).ReadinessProbe.Exec = nil
			first(p).ReadinessProbe.HTTPGet = &httpGet{Path: "/", Port: "nope"}
		}}},
		{postgres, mutation{"hair-trigger liveness", "failureThreshold below 3", func(p *pod) { first(p).LivenessProbe.FailureThreshold = 1 }}},
		{postgres, mutation{"startup shorter than liveness period", "startup window", func(p *pod) {
			first(p).StartupProbe.FailureThreshold = 1
			first(p).StartupProbe.PeriodSeconds = 1
		}}},
		{postgres, mutation{"unbounded emptyDir", "no sizeLimit", func(p *pod) {
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].EmptyDir != nil {
					p.Spec.Volumes[i].EmptyDir.SizeLimit = ""
					return
				}
			}
		}}},
		{postgres, mutation{"mount of an unknown volume", "does not define", func(p *pod) {
			first(p).VolumeMounts = append(first(p).VolumeMounts, struct {
				Name      string `yaml:"name"`
				MountPath string `yaml:"mountPath"`
			}{"ghost", "/ghost"})
		}}},
		{postgres, mutation{"no termination grace", "terminationGracePeriodSeconds", func(p *pod) { p.Spec.TerminationGracePeriodSeconds = nil }}},
		{postgres, mutation{"two replicas", "single instance", func(p *pod) { p.Replicas = 2 }}},
		{postgres, mutation{"huge claim", "at most 2Gi", func(p *pod) { p.W.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests["storage"] = "50Gi" }}},
		{postgres, mutation{"secret literal in env", "literal value", func(p *pod) {
			first(p).Env = append(first(p).Env, envVar{Name: "POSTGRES_PASSWORD", Value: "hunter2hunter2hunter2"})
		}}},
		{postgres, mutation{"unknown secret", "does not create", func(p *pod) {
			first(p).EnvFrom[0].SecretRef.Name = "no-such-secret"
		}}},
		{postgres, mutation{"password in an argument", "secret literal", func(p *pod) {
			first(p).Args = []string{"--password=abcdefghijklmnop"}
		}}},
		{postgres, mutation{"label missing", "lacks the label", func(p *pod) { delete(p.Labels, "app.kubernetes.io/component") }}},
		{postgres, mutation{"selector does not match", "selector label", func(p *pod) { p.W.Spec.Selector.MatchLabels["app.kubernetes.io/component"] = "other" }}},
		{postgres, mutation{"container seccomp Unconfined overrides the pod's RuntimeDefault", "seccomp profile", func(p *pod) {
			first(p).SecurityContext.SeccompProfile = &struct{ Type string }{"Unconfined"}
		}}},
		{postgres, mutation{"container runAsNonRoot false overrides the pod's true", "effective runAsNonRoot", func(p *pod) { first(p).SecurityContext.RunAsNonRoot = bl(false) }}},
		{postgres, mutation{"hostPort", "hostPort", func(p *pod) { first(p).Ports[0].HostPort = 5432 }}},
		{postgres, mutation{"container seLinuxOptions spc_t", "seLinuxOptions", func(p *pod) { first(p).SecurityContext.SELinuxOptions = map[string]any{"type": "spc_t"} }}},
		{postgres, mutation{"pod seLinuxOptions", "seLinuxOptions", func(p *pod) { p.Spec.SecurityContext.SELinuxOptions = map[string]any{"level": "s0:c1"} }}},
		{postgres, mutation{"nfs volume", "of the type nfs", func(p *pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, volume{Name: "n", Rest: map[string]any{"nfs": map[string]any{"server": "x", "path": "/"}}})
		}}},
		{postgres, mutation{"flexVolume", "of the type flexVolume", func(p *pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, volume{Name: "f", Rest: map[string]any{"flexVolume": map[string]any{"driver": "x"}}})
		}}},
		{postgres, mutation{"volume without a source", "has no source", func(p *pod) { p.Spec.Volumes = append(p.Spec.Volumes, volume{Name: "empty"}) }}},
		{postgres, mutation{"unsafe sysctl", "not on the safe list", func(p *pod) {
			p.Spec.SecurityContext.Sysctls = append(p.Spec.SecurityContext.Sysctls, struct {
				Name  string `yaml:"name"`
				Value string `yaml:"value"`
			}{"kernel.core_pattern", "|/x"})
		}}},
		{"Job/migrate", mutation{"serverflow image latest", "other than latest", func(p *pod) { first(p).Image = "serverflow/admin:latest" }}},
		{"Job/migrate", mutation{"unknown serverflow image", "not one of the six", func(p *pod) { first(p).Image = "serverflow/evil:dev" }}},
		{"Job/migrate", mutation{"image the script does not build", "does not build and load", func(p *pod) { first(p).Image = "serverflow/gateway:dev" }}},
		{"Job/migrate", mutation{"job runs forever", "backoffLimit and activeDeadlineSeconds", func(p *pod) { p.W.Spec.ActiveDeadlineSeconds = nil }}},
		{"Job/migrate", mutation{"job restarts in place forever", "restartPolicy", func(p *pod) { p.Spec.RestartPolicy = "Always" }}},
	}
	m := overlayManifests(t)
	created := createdSecrets(t)
	for _, mu := range mutations {
		fresh := overlayManifests(t) // a fresh parse, so one mutation never leaks into the next
		p := fresh.pod(t, mu.pod)
		mu.apply(&p)
		var v []string
		for _, f := range allChecks {
			v = append(v, f(p)...)
		}
		v = append(v, checkEnvAndSecrets(p, created)...)
		v = append(v, checkImages(t, p)...)
		if !contains(v, mu.want) {
			t.Errorf("%s: no violation containing %q; got %v", mu.name, mu.want, v)
		}
	}
	// And the unmutated render is clean (the baseline the mutations are measured from).
	if v := runChecks(t, m); len(v) != 0 {
		t.Errorf("the real render has violations: %v", v)
	}
	t.Logf("%d mutations caught", len(mutations))
}

func contains(vs []string, sub string) bool {
	for _, v := range vs {
		if strings.Contains(v, sub) {
			return true
		}
	}
	return false
}

// drain is what a gateway Deployment must satisfy once PR 3 adds it (D12, D19): the numbers are read from the manifest.
type drain struct {
	ResilienceMode                  string
	ReadinessGrace, ShutdownTimeout int
	TerminationGrace                int
	MaxUnavailable                  int
	Replicas                        int
}

// drainViolations are the timing relations of the rolling-update claim.
func drainViolations(d drain) []string {
	var v []string
	if d.ResilienceMode != "on" {
		v = append(v, "SERVERFLOW_RESILIENCE_MODE must be on (the default is off, and without it there is no drain)")
	}
	if d.ReadinessGrace <= 0 {
		v = append(v, "the readiness grace must be positive")
	}
	if d.ReadinessGrace >= d.ShutdownTimeout {
		v = append(v, "the readiness grace must be shorter than the shutdown timeout")
	}
	if d.TerminationGrace < d.ShutdownTimeout+10 {
		v = append(v, "terminationGracePeriodSeconds must be at least shutdown_timeout + 10 s")
	}
	if d.MaxUnavailable != 0 {
		v = append(v, "maxUnavailable must be 0")
	}
	if d.Replicas < 2 {
		v = append(v, "a single gateway replica cannot roll without an outage")
	}
	return v
}

// TestGatewayDrainRelations tests the checker that PR 3 applies to the gateway Deployment: the plan's numbers satisfy it and
// each way of breaking a relation (the plan's mutation list) is named. No gateway exists in this pull request, so the render is
// asserted to hold none; PR 3 replaces that assertion with the checker applied to the manifest.
func TestGatewayDrainRelations(t *testing.T) {
	good := drain{ResilienceMode: "on", ReadinessGrace: 5, ShutdownTimeout: 30, TerminationGrace: 45, MaxUnavailable: 0, Replicas: 3}
	if v := drainViolations(good); len(v) != 0 {
		t.Fatalf("the plan's numbers violate their own rules: %v", v)
	}
	bad := map[string]func(*drain){
		"resilience off":        func(d *drain) { d.ResilienceMode = "off" },
		"resilience unset":      func(d *drain) { d.ResilienceMode = "" },
		"grace zero":            func(d *drain) { d.ReadinessGrace = 0 },
		"grace above shutdown":  func(d *drain) { d.ReadinessGrace = 31 },
		"grace equals shutdown": func(d *drain) { d.ReadinessGrace = 30 },
		"termination 39":        func(d *drain) { d.TerminationGrace = 39 },
		"termination 30":        func(d *drain) { d.TerminationGrace = 30 },
		"maxUnavailable 1":      func(d *drain) { d.MaxUnavailable = 1 },
		"one replica":           func(d *drain) { d.Replicas = 1 },
	}
	for name, f := range bad {
		d := good
		f(&d)
		if len(drainViolations(d)) == 0 {
			t.Errorf("%s: not caught", name)
		}
	}
	for _, p := range overlayManifests(t).Pods {
		if p.Labels["app.kubernetes.io/component"] == "gateway" {
			t.Errorf("%s exists: replace this assertion with drainViolations applied to its settings", p.id())
		}
	}
}
