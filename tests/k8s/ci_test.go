package k8s

import (
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflow struct {
	Permissions map[string]string `yaml:"permissions"`
	Jobs        map[string]struct {
		Name        string            `yaml:"name"`
		Permissions map[string]string `yaml:"permissions"`
		Steps       []struct {
			Name string         `yaml:"name"`
			Uses string         `yaml:"uses"`
			Run  string         `yaml:"run"`
			If   string         `yaml:"if"`
			With map[string]any `yaml:"with"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
	On any `yaml:"on"`
}

func loadWorkflow(t testing.TB, rel string) workflow {
	t.Helper()
	var w workflow
	if err := yaml.Unmarshal([]byte(read(t, rel)), &w); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return w
}

// TestWorkflowsPinTheTools: every tool the Kubernetes jobs download is verified against a SHA-256 taken from tools.env, the
// versions come from that file (one source), every action is pinned to a commit, and no registry image is pulled
// without scripts/ci-docker-pull.sh (internal/ciguard enforces the same for all workflows).
func TestWorkflowsPinTheTools(t *testing.T) {
	p := pins(t)
	for _, f := range []string{".github/workflows/ci.yml", ".github/workflows/kubernetes.yml"} {
		text := read(t, f)
		w := loadWorkflow(t, f)
		for name, j := range w.Jobs {
			if !strings.Contains(name, "k8s") && !strings.Contains(name, "kind") {
				continue
			}
			var all strings.Builder
			for _, s := range j.Steps {
				if s.Uses != "" && !regexp.MustCompile(`@[0-9a-f]{40}$`).MatchString(strings.Fields(s.Uses)[0]) {
					t.Errorf("%s/%s: %s is not pinned to a commit SHA", f, name, s.Uses)
				}
				all.WriteString(s.Run + "\n")
			}
			run := all.String()
			if !strings.Contains(run, "deploy/kubernetes/tools.env") {
				t.Errorf("%s/%s: versions and checksums must come from deploy/kubernetes/tools.env", f, name)
			}
			curls := regexp.MustCompile(`curl [^\n]*-o "\$RUNNER_TEMP/([^"]+)"`).FindAllStringSubmatch(run, -1)
			if len(curls) == 0 {
				t.Errorf("%s/%s: no tool download found", f, name)
			}
			for _, m := range curls {
				checked := false
				for _, l := range strings.Split(run, "\n") {
					checked = checked || (strings.Contains(l, "sha256sum --check -") && strings.Contains(l, `$RUNNER_TEMP/`+m[1]))
				}
				if !checked {
					t.Errorf("%s/%s: %s is downloaded without a `sha256sum --check` of that very file", f, name, m[1])
				}
			}
			if strings.Count(run, "sha256sum --check -") < len(curls) {
				t.Errorf("%s/%s: %d downloads but %d checksum checks", f, name, len(curls), strings.Count(run, "sha256sum --check -"))
			}
			if regexp.MustCompile(`(?m)^\s*(docker (run|pull)|kubectl (apply|create|delete))\b`).MatchString(run) {
				t.Errorf("%s/%s: the job drives docker or kubectl directly; it must go through scripts/", f, name)
			}
		}
		if !regexp.MustCompile(`\$\{KUBECTL_LINUX_AMD64_SHA256\}`).MatchString(text) {
			t.Errorf("%s: kubectl's checksum variable is not used", f)
		}
		for _, k := range []string{"KUBERNETES_VERSION", "KUBECTL_LINUX_AMD64_SHA256"} {
			if !regexp.MustCompile(`(?m)grep -E '\^\([^']*\b` + k + `\b`).MatchString(text) {
				t.Errorf("%s: the pins step does not export %s from tools.env", f, k)
			}
		}
	}
	// The download URLs name the pinned versions through the variables, and the pins exist.
	ci := read(t, ".github/workflows/ci.yml") + read(t, ".github/workflows/kubernetes.yml")
	for _, want := range []string{"https://dl.k8s.io/release/${KUBERNETES_VERSION}/bin/linux/amd64/kubectl",
		"https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64",
		"https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/"} {
		if !strings.Contains(ci, want) {
			t.Errorf("the workflows never download %s", want)
		}
	}
	for _, k := range []string{"KIND_LINUX_AMD64_SHA256", "KUBECONFORM_LINUX_AMD64_SHA256", "KUBECTL_LINUX_AMD64_SHA256"} {
		if p[k] == "" {
			t.Errorf("tools.env has no %s", k)
		}
	}
}

// TestStaticJobAndKindJob: the static job runs `quality.sh k8s` on every pull request; the kind job is advisory, runs the
// script commands, deletes the cluster even on failure and checks the smoke's last line.
func TestStaticJobAndKindJob(t *testing.T) {
	ci := loadWorkflow(t, ".github/workflows/ci.yml")
	j, ok := ci.Jobs["k8s-static"]
	if !ok {
		t.Fatal("ci.yml has no k8s-static job")
	}
	found := false
	for _, s := range j.Steps {
		found = found || strings.TrimSpace(s.Run) == "scripts/quality.sh k8s"
	}
	if !found {
		t.Error("k8s-static must run scripts/quality.sh k8s")
	}
	if !regexp.MustCompile(`(?m)^on:\n  push:\n    branches: \[master, main\]\n  pull_request:\n`).MatchString(read(t, ".github/workflows/ci.yml")) {
		t.Error("ci.yml must run on every pull request (no path filter), so the static job always reports")
	}

	text := read(t, ".github/workflows/kubernetes.yml")
	kw := loadWorkflow(t, ".github/workflows/kubernetes.yml")
	k, ok := kw.Jobs["kind-data-tier"]
	if !ok || !strings.Contains(k.Name, "advisory") {
		t.Fatal("kubernetes.yml needs the advisory job kind-data-tier")
	}
	var runs []string
	cleanup := false
	for _, s := range k.Steps {
		runs = append(runs, s.Run)
		if strings.Contains(s.Run, "scripts/k8s-up.sh down") && s.If == "always()" {
			cleanup = true
		}
	}
	joined := strings.Join(runs, "\n")
	for _, want := range []string{"scripts/k8s-up.sh up", "scripts/k8s-up.sh smoke", "scripts/k8s-up.sh diagnose", "0 failed, 0 skipped"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the kind job never runs %q", want)
		}
	}
	if !cleanup {
		t.Error("the kind job must run `scripts/k8s-up.sh down` with if: always()")
	}
	if strings.Contains(text, "services:") || strings.Contains(text, "continue-on-error") {
		t.Error("kubernetes.yml: no services: container and no continue-on-error (advisory means not required, not hidden)")
	}
	if !strings.Contains(text, "secret appeared in the diagnostics") {
		t.Error("the diagnostics artifact must be withheld if a generated secret appears in it")
	}
}

// TestWorkflowTokenHygiene: the new jobs run with a read-only token, no persisted credentials, no pull_request_target, and a
// diagnostics artifact that is withheld when a generated secret appears in it.
func TestWorkflowTokenHygiene(t *testing.T) {
	for _, f := range []string{".github/workflows/ci.yml", ".github/workflows/kubernetes.yml"} {
		text := read(t, f)
		w := loadWorkflow(t, f)
		if strings.Contains(text, "pull_request_target") {
			t.Errorf("%s uses pull_request_target (it runs with secrets on code from forks)", f)
		}
		if len(w.Permissions) != 1 || w.Permissions["contents"] != "read" {
			t.Errorf("%s: the workflow permissions must be exactly contents: read, are %v", f, w.Permissions)
		}
		for name, j := range w.Jobs {
			for perm, val := range j.Permissions {
				if perm != "contents" || val != "read" {
					t.Errorf("%s/%s: job permission %s: %s is broader than contents: read", f, name, perm, val)
				}
			}
			for _, st := range j.Steps {
				if strings.HasPrefix(st.Uses, "actions/checkout@") {
					if v, ok := st.With["persist-credentials"]; !ok || v != false {
						t.Errorf("%s/%s: checkout must set persist-credentials: false (is %v)", f, name, v)
					}
				}
			}
		}
		if regexp.MustCompile(`(?i)persist-credentials:\s*true`).MatchString(text) {
			t.Errorf("%s persists credentials", f)
		}
	}
	k := read(t, ".github/workflows/kubernetes.yml")
	filter := `if grep -qF -f "$RUNNER_TEMP/needles" "$RUNNER_TEMP/k8s-diagnose.txt"; then`
	i, j, u := strings.Index(k, filter), strings.Index(k, `rm -f "$RUNNER_TEMP/k8s-diagnose.txt"`), strings.Index(k, "actions/upload-artifact@")
	if i < 0 || j < i || u < j {
		t.Error("the diagnostics must be filtered (grep for the generated values, remove the file on a hit) before the artifact upload")
	}
	if !strings.Contains(k, `sed -n 's/^[A-Z_]*=//p' "$f"`) || !strings.Contains(k, "grep -E '.{12,}'") {
		t.Error("the needles of the secret filter are no longer built from .dev/*.env")
	}
	if regexp.MustCompile(`(?m)^\s*(grep -qF[^\n]*\|\||if \[ -z)`).MatchString(k) {
		t.Error("the secret filter must not be disabled with || true")
	}
}
