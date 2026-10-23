// Package k8s holds the static tests of the Kubernetes deployment (Phase 17, docs/operations/kubernetes-deployment.md).
// They need no cluster: they render the kustomize overlay with the pinned kubectl (scripts/k8s-tool.sh: a binary of exactly
// that version, else its container image), parse the manifests and fail when a property the deployment promises (Pod
// Security `restricted` hardening, probes, resources, pinned images equal to Compose's, no secret in git, a context
// guard in the scripts) is lost. quality.sh k8s requires the named tests to run, so a missing tool is a failure there.
package k8s

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"
)

func root(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the repository")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func read(t testing.TB, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// pins parses deploy/kubernetes/tools.env (NAME=VALUE lines; the file is data).
func pins(t testing.TB) map[string]string {
	t.Helper()
	out := map[string]string{}
	for i, line := range strings.Split(read(t, "deploy/kubernetes/tools.env"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || !regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`).MatchString(k) || v == "" {
			t.Fatalf("tools.env:%d is not NAME=VALUE", i+1)
		}
		if _, dup := out[k]; dup {
			t.Fatalf("tools.env:%d repeats %s", i+1, k)
		}
		out[k] = v
	}
	return out
}

var renderCache sync.Map // overlay dir -> rendered bytes

// render runs `kubectl kustomize --load-restrictor=LoadRestrictionsNone <dir>` through scripts/k8s-tool.sh. Without the
// pinned kubectl and without Docker it skips (the gate, quality.sh k8s, fails if a named test skipped) except in CI.
func render(t testing.TB, dir string) []byte {
	t.Helper()
	if v, ok := renderCache.Load(dir); ok {
		return v.([]byte)
	}
	if runtime.GOOS == "windows" {
		t.Skip("the Kubernetes scripts are bash scripts (WSL2 is the supported way on Windows)")
	}
	cmd := exec.Command("bash", filepath.Join(root(t), "scripts/k8s-tool.sh"), "kubectl", "kustomize", "--load-restrictor=LoadRestrictionsNone", dir)
	cmd.Dir = root(t)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		msg := stderr.String()
		if strings.Contains(msg, "neither the pinned binary nor docker is available") && os.Getenv("CI") != "true" {
			t.Skip("neither kubectl " + pins(t)["KUBERNETES_VERSION"] + " nor Docker is available to render the manifests")
		}
		t.Fatalf("kustomize %s: %v\n%s", dir, err, msg)
	}
	renderCache.Store(dir, stdout.Bytes())
	return stdout.Bytes()
}

const overlay = "deploy/kubernetes/overlays/kind"

// decodeAll splits a multi-document YAML stream.
func decodeAll(t testing.TB, b []byte) []map[string]any {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	var out []map[string]any
	for {
		var d map[string]any
		err := dec.Decode(&d)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if d != nil {
			out = append(out, d)
		}
	}
}

// bashes returns the distinct bash interpreters available (a Mac's /bin/bash is 3.2, the CI runner's is 5.2).
func bashes(t *testing.T) []string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("bash scripts")
	}
	seen := map[string]bool{}
	var out []string
	for _, c := range []string{"bash", "/bin/bash", "/opt/homebrew/bin/bash", "/usr/local/bin/bash"} {
		p, err := exec.LookPath(c)
		if err != nil {
			continue
		}
		if r, err := filepath.EvalSymlinks(p); err == nil {
			p = r
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		t.Fatal("bash is required")
	}
	return out
}

// execOut runs a command in the repository root with a minimal environment and returns its combined output.
func execOut(t testing.TB, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = root(t)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// stateDir makes a throwaway state directory for scripts/k8s-up.sh inside the repository's gitignored .dev/ (the script
// refuses any K8S_UP_DIR outside it) and removes it at the end of the test.
func stateDir(t testing.TB) string {
	t.Helper()
	dev := filepath.Join(root(t), ".dev")
	if err := os.MkdirAll(dev, 0o700); err != nil {
		t.Fatal(err)
	}
	d, err := os.MkdirTemp(dev, "k8s-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}
