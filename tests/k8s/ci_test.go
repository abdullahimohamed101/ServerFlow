package k8s

import (
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflow struct {
	Jobs map[string]struct {
		Name  string `yaml:"name"`
		Steps []struct {
			Name string `yaml:"name"`
			Uses string `yaml:"uses"`
			Run  string `yaml:"run"`
			If   string `yaml:"if"`
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
			curls := regexp.MustCompile(`curl [^\n]*-o "\$RUNNER_TEMP/([a-z.-]+)"`).FindAllStringSubmatch(run, -1)
			if len(curls) == 0 {
				t.Errorf("%s/%s: no tool download found", f, name)
			}
			for _, m := range curls {
				if !regexp.MustCompile(`sha256sum --check -`).MatchString(run) || !strings.Contains(run, `$RUNNER_TEMP/`+m[1]) {
					t.Errorf("%s/%s: %s is downloaded without a checksum", f, name, m[1])
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
