package k8s

import (
	"bytes"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// The rolling-update test (Phase 17 PR 6, D20) needs a cluster, so what is checked here is everything around it that does not:
// the objects the script applies to roll the gateway (the original with exactly one edit each), the shape of the procedure in the
// script, and the CI wiring. The load generator has its own tests in tests/k8s/e2e.

// rollRender runs `scripts/k8s-up.sh roll-render PROFILE [TAG]` (it renders; it touches no cluster).
func rollRender(t *testing.T, profile, tag string) []map[string]any {
	t.Helper()
	render(t, overlay) // skips when neither kubectl nor Docker can render
	args := []string{"-u", "scripts/k8s-up.sh", "roll-render", profile}
	if tag != "" {
		args = append(args, tag)
	}
	cmd := exec.Command("bash", args...)
	cmd.Dir = root(t)
	cmd.Env = append(os.Environ(), "K8S_UP_DIR="+stateDir(t))
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("roll-render %s %s: %v\n%s", profile, tag, err, errb.String())
	}
	return decodeAll(t, out.Bytes())
}

// gatewayObjects returns the rendered gateway ConfigMap and Deployment.
func gatewayObjects(t *testing.T, docs []map[string]any) (cm, dep map[string]any) {
	t.Helper()
	cm, dep = findConfigMap(docs, "gateway-config"), findDoc(docs, "Deployment", "gateway")
	if cm == nil || dep == nil {
		t.Fatalf("the gateway ConfigMap or Deployment is missing (%d documents)", len(docs))
	}
	return cm, dep
}

func configMapRefOf(dep map[string]any) string {
	for _, e := range at(dep, "spec", "template", "spec", "containers", 0, "envFrom").([]any) {
		if n := asStr(at(e, "configMapRef", "name")); n != "" {
			return n
		}
	}
	return ""
}

// TestRollVariantsAreTheGatewayWithOneEditEach: what roll-test applies is the real gateway ConfigMap and Deployment with a new
// ConfigMap name (a new hash, which rolls the pods), the Deployment pointing at it, GOMEMLIMIT changed, and nothing else; the
// control additionally switches resilience off and the readiness grace to 0; the annotation appears on the pod template only
// when asked for; and a stream of exactly the two objects comes out.
func TestRollVariantsAreTheGatewayWithOneEditEach(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash scripts")
	}
	orig := decodeAll(t, render(t, overlay))
	origCM, origDep := gatewayObjects(t, orig)
	origData := at(origCM, "data").(map[string]any)

	check := func(name, profile, tag string, wantData map[string]any) {
		docs := rollRender(t, profile, tag)
		if len(docs) != 2 {
			t.Fatalf("%s: %d documents, want the ConfigMap and the Deployment", name, len(docs))
		}
		cm, dep := gatewayObjects(t, docs)
		cmName := asStr(at(cm, "metadata", "name"))
		if cmName == asStr(at(origCM, "metadata", "name")) || cmName != "gateway-config-rolldemo" {
			t.Errorf("%s: the ConfigMap is %q: it must have a new name, so that the pod template changes", name, cmName)
		}
		if configMapRefOf(dep) != cmName {
			t.Errorf("%s: the Deployment references %q, not %q", name, configMapRefOf(dep), cmName)
		}
		if got := at(cm, "data").(map[string]any); !reflect.DeepEqual(got, wantData) {
			t.Errorf("%s: ConfigMap data differs from the expectation:\n got %v\nwant %v", name, got, wantData)
		}
		// Everything else of both objects is the original's.
		strip := func(d map[string]any, isDep bool) map[string]any {
			c := cloneRaw(t, []map[string]any{d})[0]
			delete(at(c, "metadata").(map[string]any), "name")
			delete(c, "data")
			if isDep {
				tpl := at(c, "spec", "template", "metadata").(map[string]any)
				delete(tpl, "annotations")
				ef := at(c, "spec", "template", "spec", "containers", 0, "envFrom").([]any)
				for _, e := range ef {
					if m, ok := e.(map[string]any); ok && m["configMapRef"] != nil {
						m["configMapRef"] = nil
					}
				}
			}
			return c
		}
		if !reflect.DeepEqual(strip(cm, false), strip(origCM, false)) {
			t.Errorf("%s: the ConfigMap changed beyond its name and data", name)
		}
		if !reflect.DeepEqual(strip(dep, true), strip(origDep, true)) {
			t.Errorf("%s: the Deployment changed beyond the ConfigMap reference and the annotation", name)
		}
		ann := at(dep, "spec", "template", "metadata", "annotations")
		if tag == "" && ann != nil {
			t.Errorf("%s: unexpected annotations %v", name, ann)
		}
		if tag != "" {
			if got := asStr(at(ann, "serverflow.dev/image-tag")); got != tag {
				t.Errorf("%s: annotation %q, want %q", name, got, tag)
			}
		}
	}
	with := func(edits map[string]any) map[string]any {
		m := map[string]any{}
		for k, v := range origData {
			m[k] = v
		}
		for k, v := range edits {
			m[k] = v
		}
		return m
	}
	if origData["GOMEMLIMIT"] != "100MiB" || origData["SERVERFLOW_RESILIENCE_MODE"] != "on" || origData["SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE"] != "5s" {
		t.Fatalf("the gateway ConfigMap no longer has the values roll-test edits: %v", origData)
	}
	check("normal", "normal", "", with(map[string]any{"GOMEMLIMIT": "99MiB"}))
	check("normal with the annotation", "normal", "rollmain-1700000000", with(map[string]any{"GOMEMLIMIT": "99MiB"}))
	control := with(map[string]any{"GOMEMLIMIT": "99MiB", "SERVERFLOW_RESILIENCE_MODE": "off", "SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE": "0s"})
	check("control", "control", "", control)
	check("control with the annotation", "control", "rollcontrol-1700000000", control)
}

// scriptRegion returns the roll-test section of the script.
func scriptRegion(t *testing.T) string {
	t.Helper()
	s := read(t, "scripts/k8s-up.sh")
	i, j := strings.Index(s, "\nROLL_DIR="), strings.Index(s, "\n# check: the static checks")
	if i < 0 || j < i {
		t.Fatal("the roll-test section of scripts/k8s-up.sh is missing")
	}
	return s[i:j]
}

// TestRollTestProcedure pins the procedure of D20 in the script: the load, the two rollouts, the control, the restore, and that
// the key never leaves its file.
func TestRollTestProcedure(t *testing.T) {
	r := scriptRegion(t)
	for _, want := range []string{
		`--clients 24`, `ROLL_MIN_REQUESTS=3000`, `--url "$GATEWAY_URL"`, `--key-file "$K8S_DIR/api_key"`, `ROLL_PHASE_SECONDS:-20`,
		`roll_step roll1`, `roll_step roll2`, `printf 'mark after\n'`, `--expect "$expect"`,
		`roll_scenario main normal none`, `roll_scenario control control failures`,
		`rollout status deployment/gateway --timeout=300s`, `trap roll_cleanup EXIT INT TERM`, `ROLL_DIRTY=1`,
		`roll_restore`, `roll-test: PASS`, `the test cannot see the outage`,
	} {
		if !strings.Contains(r, want) {
			t.Errorf("the roll-test section lacks %q", want)
		}
	}
	// The control switches exactly the two settings the plan names.
	if !regexp.MustCompile(`RESILIENCE_MODE: "off"`).MatchString(r) || !regexp.MustCompile(`DRAIN_READINESS_GRACE: 0s`).MatchString(r) {
		t.Error("the control must switch resilience off and the readiness grace to 0")
	}
	// The restore comes from the repository's own render, and runs from the exit trap only when something was changed.
	if !regexp.MustCompile(`(?s)roll_cleanup\(\) \{.*ROLL_DIRTY.*roll_restore`).MatchString(r) {
		t.Error("the cleanup must restore the gateway when it was changed")
	}
	// The key is only ever a path argument: the section never reads, prints or expands the file's content.
	for i, line := range strings.Split(r, "\n") {
		code := strings.TrimSpace(line)
		if strings.HasPrefix(code, "#") || !strings.Contains(code, "api_key") {
			continue
		}
		if !strings.Contains(code, `--key-file "$K8S_DIR/api_key"`) && !strings.Contains(code, `-s "$K8S_DIR/api_key"`) {
			t.Errorf("roll-test section line %d touches the key file other than by path: %s", i+1, code)
		}
	}
	for _, bad := range []string{"cat \"$K8S_DIR/api_key\"", "$(<", "read -r key", "Authorization"} {
		if strings.Contains(r, bad) {
			t.Errorf("the roll-test section must not handle the key itself (%q)", bad)
		}
	}
	// No pkill/killall, no stash: only the processes it started, by PID.
	for _, bad := range []string{"pkill", "killall", "kill -9", "kill -KILL"} {
		if strings.Contains(r, bad) {
			t.Errorf("the roll-test section uses %q", bad)
		}
	}
	// Every state the test creates is under the script's own state directory.
	if !strings.Contains(r, `ROLL_DIR="$K8S_DIR/roll"`) {
		t.Error("the roll-test files must live under $K8S_DIR")
	}
}

// TestRollTestCIWiring (D24): the step runs in the nightly and manual runs and on pull requests that touch the gateway manifests
// or the drain code, only when the cluster is up, and its log goes through the secret filter before it can be uploaded.
func TestRollTestCIWiring(t *testing.T) {
	k := read(t, ".github/workflows/kubernetes.yml")
	for _, want := range []string{
		"scripts/k8s-up.sh roll-test", "schedule | workflow_dispatch) run=true", "deploy/kubernetes/base/gateway", "internal/gateway/resilience",
		"steps.roll.outputs.run == 'true'", "grep -Eq '^roll-test: PASS'", "fetch-depth: 2", `- "tests/k8s/e2e/**"`, "actions/setup-go@",
	} {
		if !strings.Contains(k, want) {
			t.Errorf("kubernetes.yml lacks %q", want)
		}
	}
	roll, smoke, audit := strings.Index(k, "scripts/k8s-up.sh roll-test"), strings.Index(k, "scripts/k8s-up.sh smoke"), strings.Index(k, "scripts/k8s-up.sh audit")
	if roll < audit || audit < smoke || smoke < 0 {
		t.Error("the rolling-update test must run after the smoke checks and the audit, on the cluster they left")
	}
	if !regexp.MustCompile(`(?m)^\s*(- )?(uses: actions/setup-go@[0-9a-f]{40}|if: steps\.roll\.outputs\.run == 'true')`).MatchString(k) {
		t.Error("setup-go must be pinned to a SHA and run only with the roll test")
	}
	for _, f := range []string{"docs/development/ci.md", "docs/operations/kubernetes-deployment.md"} {
		if !strings.Contains(read(t, f), "roll-test") {
			t.Errorf("%s does not document roll-test", f)
		}
	}
}
