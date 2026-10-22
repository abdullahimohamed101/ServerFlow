package k8s

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestContextCheckUnderStrictShell: only kind-serverflow is accepted, under every bash available and `bash -u`.
func TestContextCheckUnderStrictShell(t *testing.T) {
	script := filepath.Join(root(t), "scripts/k8s-up.sh")
	for _, bash := range bashes(t) {
		run := func(name string) (string, error) {
			cmd := exec.Command(bash, "-u", script, "--check-context", name)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
			out, err := cmd.CombinedOutput()
			return string(out), err
		}
		if out, err := run("kind-serverflow"); err != nil {
			t.Errorf("%s: the allowed context was refused: %v\n%s", bash, err, out)
		}
		for _, bad := range []string{"", "prod", "docker-desktop", "minikube", "kind-kind", "kind-serverflow2", "kind-serverflow ", " kind-serverflow", "Kind-Serverflow", "kind-serverflow\n", "arn:aws:eks:eu-west-1:1:cluster/x", "serverflow"} {
			if out, err := run(bad); err == nil {
				t.Errorf("%s: the context %q was accepted: %s", bash, bad, out)
			}
		}
	}
}

// fakeKubectl writes a kubectl stub that logs each call (arguments and whether KUBECONFIG was set) and answers the two
// `config` queries from FAKE_CONTEXT and FAKE_SERVER. Nothing touches a cluster.
func fakeKubectl(t *testing.T) (dir, log string) {
	t.Helper()
	dir = t.TempDir()
	log = filepath.Join(dir, "calls.log")
	stub := `#!/bin/sh
printf '%s|KUBECONFIG=%s\n' "$*" "${KUBECONFIG-unset}" >> "$FAKE_LOG"
case "$*" in
  *"config current-context"*) printf '%s\n' "$FAKE_CONTEXT" ;;
  *"config view --minify"*) printf '%s\n' "$FAKE_SERVER" ;;
  *"version --client"*) printf 'Client Version: v1.36.4\n' ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, log
}

func runWithFake(t *testing.T, bash, context, server string, args ...string) (out string, err error, calls string, kubeconfig string) {
	t.Helper()
	stubDir, log := fakeKubectl(t)
	state := t.TempDir()
	kubeconfig = filepath.Join(state, "kubeconfig")
	if e := os.WriteFile(kubeconfig, []byte("# not read: the stub answers\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(bash, append([]string{"-u", filepath.Join(root(t), "scripts/k8s-up.sh")}, args...)...)
	cmd.Env = []string{
		"PATH=" + stubDir + ":/usr/bin:/bin", "HOME=" + t.TempDir(), "K8S_UP_DIR=" + state,
		"FAKE_LOG=" + log, "FAKE_CONTEXT=" + context, "FAKE_SERVER=" + server,
		"KUBECONFIG=/nonexistent/a-foreign-kubeconfig", // must never steer the script
	}
	b, err := cmd.CombinedOutput()
	c, _ := os.ReadFile(log)
	return string(b), err, string(c), kubeconfig
}

// TestForeignContextIsRefused (AC 12): with another context current, or a server that is not on loopback, the cluster commands
// refuse before they run anything but the two `config` reads; with the right one they pass the dedicated kubeconfig and
// context explicitly, and the foreign KUBECONFIG of the environment is gone.
func TestForeignContextIsRefused(t *testing.T) {
	for _, bash := range bashes(t) {
		for _, cmdName := range []string{"status", "smoke"} {
			for _, bad := range []struct{ context, server string }{
				{"prod", "https://127.0.0.1:6443"},
				{"docker-desktop", "https://127.0.0.1:6443"},
				{"kind-other", "https://127.0.0.1:6443"},
				{"", "https://127.0.0.1:6443"},
				{"kind-serverflow", "https://203.0.113.7:6443"},
				{"kind-serverflow", "https://10.0.0.1:6443"},
				{"kind-serverflow", "https://127.0.0.1.evil.example:6443"},
				{"kind-serverflow", "http://127.0.0.1:6443"},
				{"kind-serverflow", ""},
			} {
				out, err, calls, _ := runWithFake(t, bash, bad.context, bad.server, cmdName)
				if err == nil {
					t.Errorf("%s %s: context %q server %q was accepted\n%s", bash, cmdName, bad.context, bad.server, out)
				}
				if !strings.Contains(out, "refusing") && !strings.Contains(out, "cannot read") {
					t.Errorf("%s %s: no refusal message for context %q server %q: %s", bash, cmdName, bad.context, bad.server, out)
				}
				for _, line := range strings.Split(strings.TrimSpace(calls), "\n") {
					if line != "" && !strings.Contains(line, " config ") {
						t.Errorf("%s %s: a refused run still called kubectl %q", bash, cmdName, line)
					}
				}
			}
		}
		out, err, calls, kubeconfig := runWithFake(t, bash, "kind-serverflow", "https://127.0.0.1:41234", "status")
		if err != nil {
			t.Fatalf("%s: the allowed context failed: %v\n%s", bash, err, out)
		}
		if !strings.Contains(calls, "--kubeconfig "+kubeconfig+" --context kind-serverflow") || !strings.Contains(calls, " -n serverflow get ") {
			t.Errorf("%s: the status call did not pass the dedicated kubeconfig and context: %q", bash, calls)
		}
		for _, line := range strings.Split(strings.TrimSpace(calls), "\n") {
			if !strings.Contains(line, "--kubeconfig "+kubeconfig) {
				t.Errorf("%s: a kubectl call without the dedicated kubeconfig: %q", bash, line)
			}
			if !strings.HasSuffix(line, "|KUBECONFIG=unset") {
				t.Errorf("%s: KUBECONFIG leaked into kubectl: %q", bash, line)
			}
		}
	}
}

// TestEveryKubectlCallIsGuarded reads the script: kubectl is only ever called through kc(), with the explicit kubeconfig, or
// for `version --client`; nothing writes a context, nothing reads ~/.kube, `kind` always names its kubeconfig and cluster.
func TestEveryKubectlCallIsGuarded(t *testing.T) {
	script := read(t, "scripts/k8s-up.sh")
	for i, line := range strings.Split(script, "\n") {
		code := strings.TrimSpace(line)
		if strings.HasPrefix(code, "#") {
			continue
		}
		where := "k8s-up.sh:" + itoa(i+1) + ": "
		if regexp.MustCompile(`(^|[|;&(]\s*|\$\()\s*kubectl\s`).MatchString(code) {
			ok := strings.Contains(code, `kubectl --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" "$@"`) || // kc itself
				strings.Contains(code, `kubectl --kubeconfig "$KUBECONFIG_FILE" config `) ||
				strings.Contains(code, "kubectl version --client")
			if !ok {
				t.Errorf("%skubectl called outside kc(): %s", where, code)
			}
		}
		for _, bad := range []string{"use-context", "set-context", "delete-context", "config set", "current-context=", "~/.kube", "$HOME/.kube", "kubectl apply -k", "apply -k", "--all-namespaces", "-A "} {
			if strings.Contains(code, bad) && !strings.Contains(code, `die "`) && !strings.HasPrefix(code, "say") {
				t.Errorf("%sforbidden: %s", where, code)
			}
		}
		if regexp.MustCompile(`\bkind (create|delete|export|get)\b`).MatchString(code) && !strings.HasPrefix(code, "say") {
			isGet := strings.Contains(code, "kind get clusters")
			if !isGet && !(strings.Contains(code, "--kubeconfig") && strings.Contains(code, `--name "$CLUSTER"`)) {
				t.Errorf("%skind must be given --name and --kubeconfig: %s", where, code)
			}
		}
	}
	if !regexp.MustCompile(`(?m)^unset KUBECONFIG`).MatchString(script) {
		t.Error("the script must unset KUBECONFIG before anything else")
	}
	// guard_context runs first in every command that reaches the cluster.
	for _, fn := range []string{"cmd_status", "cmd_smoke"} {
		m := regexp.MustCompile(`(?s)\n` + fn + `\(\) \{\n  guard_context\n`).MatchString(script)
		if !m {
			t.Errorf("%s must start with guard_context", fn)
		}
	}
	if !regexp.MustCompile(`(?s)\nguard_context\(\).*require_context "\$cur"`).MatchString(script) {
		t.Error("guard_context must call require_context on the current context")
	}
	if !regexp.MustCompile(`(?m)^  create_cluster\n  guard_context\n`).MatchString(script) {
		t.Error("cmd_up must guard the context right after creating the cluster, before any kc call")
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// TestScriptsUnderStrictShell runs the Docker-free paths with `bash -u` and a minimal environment under every bash.
func TestScriptsUnderStrictShell(t *testing.T) {
	for _, bash := range bashes(t) {
		for _, args := range [][]string{
			{"scripts/k8s-up.sh", "--check-context", "kind-serverflow"},
			{"scripts/k8s-tool.sh", "pin", "KUBERNETES_VERSION"},
		} {
			if out, err := execOut(t, bash, append([]string{"-u"}, args...)...); err != nil {
				t.Errorf("%s -u %v: %v\n%s", bash, args, err, out)
			}
		}
		// Unknown commands and a missing kubectl fail cleanly, not with an unbound variable.
		for _, args := range [][]string{{"scripts/k8s-up.sh", "bogus"}, {"scripts/k8s-up.sh"}, {"scripts/k8s-up.sh", "audit"}, {"scripts/k8s-up.sh", "status"}} {
			out, err := execOut(t, bash, append([]string{"-u"}, args...)...)
			if err == nil {
				t.Errorf("%s -u %v: expected a failure", bash, args)
			}
			if strings.Contains(out, "unbound variable") {
				t.Errorf("%s -u %v: %s", bash, args, out)
			}
		}
	}
}

// TestMakefileTargets: the commands the plan names exist, call the script, and are marked phony.
func TestMakefileTargets(t *testing.T) {
	mk := read(t, "Makefile")
	want := map[string]string{
		"k8s-up": "scripts/k8s-up.sh up", "k8s-down": "scripts/k8s-up.sh down", "k8s-reset": "scripts/k8s-up.sh reset",
		"k8s-smoke": "scripts/k8s-up.sh smoke", "k8s-roll-test": "scripts/k8s-up.sh roll-test", "k8s-audit": "scripts/k8s-up.sh audit",
		"k8s-render": "scripts/k8s-up.sh render", "k8s-check": "scripts/quality.sh k8s",
	}
	for target, cmd := range want {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(target) + `:\n\t` + regexp.QuoteMeta(cmd) + `$`).MatchString(mk) {
			t.Errorf("Makefile target %s must run %s", target, cmd)
		}
		if !regexp.MustCompile(`(?m)^\.PHONY:.*\b` + regexp.QuoteMeta(target) + `\b`).MatchString(mk) {
			t.Errorf("%s is not .PHONY", target)
		}
	}
	for _, s := range []string{"scripts/k8s-up.sh", "scripts/k8s-tool.sh"} {
		fi, err := os.Stat(filepath.Join(root(t), s))
		if err != nil {
			t.Errorf("%s: %v", s, err)
			continue
		}
		if runtime.GOOS != "windows" && fi.Mode()&0o111 == 0 {
			t.Errorf("%s is not executable", s)
		}
	}
}

// TestKustomizeCommandIsPinned (D1): kustomize's default load restrictor forbids reading observability/ from deploy/kubernetes/,
// and `kubectl apply -k` has no flag to relax it, so everything renders with --load-restrictor=LoadRestrictionsNone and is
// applied from a pipe. No document, script or workflow may tell anyone to `apply -k`.
func TestKustomizeCommandIsPinned(t *testing.T) {
	if !strings.Contains(read(t, "scripts/k8s-up.sh"), "--load-restrictor=LoadRestrictionsNone") {
		t.Error("scripts/k8s-up.sh must render with --load-restrictor=LoadRestrictionsNone")
	}
	if !strings.Contains(read(t, "docs/operations/kubernetes-deployment.md"), "kubectl kustomize --load-restrictor=LoadRestrictionsNone") {
		t.Error("the documentation must show the render command")
	}
	for _, f := range []string{"scripts/k8s-up.sh", "scripts/k8s-tool.sh", "scripts/quality.sh", "Makefile", ".github/workflows/ci.yml", ".github/workflows/kubernetes.yml", "docs/operations/kubernetes-deployment.md"} {
		for i, line := range strings.Split(read(t, f), "\n") {
			if regexp.MustCompile(`apply\s+(-\S+\s+)*-k\b`).MatchString(line) && !strings.Contains(line, "not") && !strings.Contains(line, "never") && !strings.Contains(line, "no ") {
				t.Errorf("%s:%d uses kubectl apply -k: %s", f, i+1, strings.TrimSpace(line))
			}
			if strings.Contains(line, "kustomize build") && !strings.HasPrefix(strings.TrimSpace(line), "#") {
				t.Errorf("%s:%d uses a separate kustomize binary: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
	// The script applies a phase at a time from the render, never a directory.
	if !regexp.MustCompile(`render \| kc apply -f - -l "serverflow.dev/phase=\$1"`).MatchString(read(t, "scripts/k8s-up.sh")) {
		t.Error("apply_phase must pipe the render into kc apply with the phase selector")
	}
}

// TestSecretsNeverTouchACommandLine: the Secrets are created from a file by a pipe, and no script prints a .dev file.
func TestSecretsNeverTouchACommandLine(t *testing.T) {
	script := read(t, "scripts/k8s-up.sh")
	if !strings.Contains(script, `--from-env-file="$f" --dry-run=client -o yaml | kc apply -f -`) {
		t.Error("Secrets must be created with --from-env-file and --dry-run=client piped into kubectl apply")
	}
	for i, line := range strings.Split(script, "\n") {
		code := strings.TrimSpace(line)
		if strings.HasPrefix(code, "#") {
			continue
		}
		if regexp.MustCompile(`--from-literal|\bcat\b[^|]*\.env|\$\(<\s*"?\$DEV_DIR|echo[^\n]*PASSWORD`).MatchString(code) {
			t.Errorf("k8s-up.sh:%d could expose a secret: %s", i+1, code)
		}
		if regexp.MustCompile(`\bset -x\b|\bset -o xtrace\b|\bbash -x\b`).MatchString(code) {
			t.Errorf("k8s-up.sh:%d traces commands, which would print secrets", i+1)
		}
	}
	if !strings.Contains(script, `"$ROOT/scripts/dev-up.sh" secrets`) {
		t.Error("secrets must come from scripts/dev-up.sh secrets (one generator for both stacks)")
	}
}
