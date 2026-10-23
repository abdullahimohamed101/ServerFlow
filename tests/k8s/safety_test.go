package k8s

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func runScript(t *testing.T, bash string, env []string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(bash, append([]string{"-u", filepath.Join(root(t), "scripts/k8s-up.sh")}, args...)...)
	cmd.Dir = root(t)
	cmd.Env = append([]string{"HOME=" + t.TempDir()}, env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestStateDirIsConfinedToDotDev: K8S_UP_DIR names what `reset` deletes, so it must be a directory strictly inside
// <repo>/.dev, without '..', '.', empty components or symlinks.
func TestStateDirIsConfinedToDotDev(t *testing.T) {
	good := stateDir(t)
	link := filepath.Join(root(t), ".dev", "k8s-test-link")
	outside := t.TempDir()
	_ = os.Remove(link)
	if err := os.Symlink(outside, link); err != nil {
		t.Skip("symlinks unavailable: " + err.Error())
	}
	t.Cleanup(func() { _ = os.Remove(link) })
	bad := []string{
		outside, "/tmp", "/", root(t), ".dev", "./.dev", filepath.Join(root(t), ".dev"), ".dev/", ".dev/..", ".dev/../x", ".dev/a/../b", ".dev/./k8s",
		"../outside", "deploy", "k8s", filepath.Join(root(t), ".dev") + "/", filepath.Join(root(t), ".dev") + "//x",
		link, filepath.Join(link, "sub"), ".dev/k8s-test-link", ".dev/k8s-test-link/deeper/still",
	}
	for _, bash := range bashes(t) {
		env := []string{"PATH=/usr/bin:/bin"}
		if out, err := runScript(t, bash, append(env, "K8S_UP_DIR="+good), "--check-context", "kind-serverflow"); err != nil {
			t.Errorf("%s: a directory inside .dev was refused: %v\n%s", bash, err, out)
		}
		for _, b := range bad {
			out, err := runScript(t, bash, append(env, "K8S_UP_DIR="+b), "--check-context", "kind-serverflow")
			if err == nil || !strings.Contains(out, "refusing") {
				t.Errorf("%s: K8S_UP_DIR=%q was accepted: %s", bash, b, out)
			}
		}
	}
}

// fakeTools writes kind and docker stubs that log every call. FAKE_CLUSTER=1 makes `kind get clusters` list serverflow.
func fakeTools(t *testing.T) (dir, log string) {
	t.Helper()
	dir = t.TempDir()
	log = filepath.Join(dir, "calls.log")
	kind := `#!/bin/sh
printf 'kind %s\n' "$*" >> "$FAKE_LOG"
case "$*" in
  "get clusters") [ "$FAKE_CLUSTER" = 1 ] && echo serverflow ;;
esac
exit 0
`
	docker := `#!/bin/sh
printf 'docker %s\n' "$*" >> "$FAKE_LOG"
exit 0
`
	for n, body := range map[string]string{"kind": kind, "docker": docker} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir, log
}

// TestDownAndResetScope (AC 12): `down` deletes only a cluster this checkout created (the marker or the dedicated kubeconfig),
// only by `kind delete cluster --name serverflow --kubeconfig <state>/kubeconfig`; `reset` removes only the state
// directory; neither touches anything else, whatever K8S_UP_DIR says.
func TestDownAndResetScope(t *testing.T) {
	for _, bash := range bashes(t) {
		run := func(cluster string, state string, cmd string) (string, string, error) {
			tools, log := fakeTools(t)
			out, err := runScript(t, bash, []string{"PATH=" + tools + ":/usr/bin:/bin", "FAKE_LOG=" + log, "FAKE_CLUSTER=" + cluster, "K8S_UP_DIR=" + state}, cmd)
			calls, _ := os.ReadFile(log)
			return out, string(calls), err
		}
		// A cluster named serverflow that this checkout did not create is never deleted.
		st := stateDir(t)
		out, calls, err := run("1", st, "down")
		if err == nil || !strings.Contains(out, "refusing to delete") || strings.Contains(calls, "kind delete") {
			t.Errorf("%s: down deleted or ignored a foreign cluster: err=%v\n%s\n%s", bash, err, out, calls)
		}
		out, calls, err = run("1", st, "reset")
		if err == nil || strings.Contains(calls, "kind delete") {
			t.Errorf("%s: reset deleted a foreign cluster: %v\n%s", bash, err, out)
		}
		if _, e := os.Stat(st); e != nil {
			t.Errorf("%s: reset removed the state directory although it refused", bash)
		}
		// Ours (marker): one precise delete, then the marker is gone.
		for _, marker := range []string{"owns-cluster", "kubeconfig"} {
			st = stateDir(t)
			if err := os.WriteFile(filepath.Join(st, marker), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			out, calls, err = run("1", st, "down")
			if err != nil {
				t.Fatalf("%s: down of our own cluster (%s) failed: %v\n%s", bash, marker, err, out)
			}
			want := "kind delete cluster --name serverflow --kubeconfig " + filepath.Join(st, "kubeconfig")
			if strings.Count(calls, "kind delete") != 1 || !strings.Contains(calls, want+"\n") {
				t.Errorf("%s: down must run exactly %q, calls:\n%s", bash, want, calls)
			}
			for _, line := range strings.Split(strings.TrimSpace(calls), "\n") {
				if !regexp.MustCompile(`^(kind (get clusters|delete cluster --name serverflow --kubeconfig \S+)|docker ps -aq --filter label=io\.x-k8s\.kind\.cluster=serverflow)$`).MatchString(line) {
					t.Errorf("%s: down ran something outside its whitelist: %q", bash, line)
				}
			}
			if _, e := os.Stat(filepath.Join(st, "kubeconfig")); e == nil {
				t.Errorf("%s: down left the kubeconfig", bash)
			}
		}
		// reset removes the state directory and nothing beside it.
		st = stateDir(t)
		sibling := stateDir(t)
		for _, f := range []string{filepath.Join(st, "owns-cluster"), filepath.Join(st, "extra"), filepath.Join(sibling, "keep")} {
			if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		out, _, err = run("1", st, "reset")
		if err != nil {
			t.Fatalf("%s: reset failed: %v\n%s", bash, err, out)
		}
		if _, e := os.Stat(st); e == nil {
			t.Errorf("%s: reset left the state directory", bash)
		}
		if _, e := os.Stat(filepath.Join(sibling, "keep")); e != nil {
			t.Errorf("%s: reset removed a neighbour in .dev", bash)
		}
		if _, e := os.Stat(filepath.Join(root(t), ".dev")); e != nil {
			t.Errorf("%s: reset removed .dev", bash)
		}
		// Hostile K8S_UP_DIR values delete nothing.
		victim := t.TempDir()
		if err := os.WriteFile(filepath.Join(victim, "precious"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, d := range []string{victim, ".dev", filepath.Join(root(t), ".dev"), ".dev/..", "."} {
			if _, _, err := run("0", d, "reset"); err == nil {
				t.Errorf("%s: reset accepted K8S_UP_DIR=%q", bash, d)
			}
		}
		if _, e := os.Stat(filepath.Join(victim, "precious")); e != nil {
			t.Errorf("%s: reset deleted a file outside .dev", bash)
		}
		if _, e := os.Stat(filepath.Join(root(t), "deploy/kubernetes/tools.env")); e != nil {
			t.Errorf("%s: the repository lost a file", bash)
		}
	}
}

// TestUpRefusesAForeignCluster: `up` finds a cluster named serverflow this checkout did not create and stops before touching it.
func TestUpRefusesAForeignCluster(t *testing.T) {
	// `up` needs a real engine for its preflight, so the function is checked by reading it.
	script := read(t, "scripts/k8s-up.sh")
	if !regexp.MustCompile(`(?s)cluster_exists; then\n\s+\[ -f "\$OWNER_MARK" \] \|\| die`).MatchString(script) {
		t.Error("create_cluster must refuse an existing cluster without the ownership marker")
	}
	if !strings.Contains(script, `: >"$OWNER_MARK"`) {
		t.Error("create_cluster must write the marker before creating the cluster")
	}
}

// TestDestructiveCommandsAreWhitelisted reads the script: the only docker subcommands are the ones listed, kind deletes one named
// cluster, and the only recursive removal is the validated state directory.
func TestDestructiveCommandsAreWhitelisted(t *testing.T) {
	allowedDocker := regexp.MustCompile(`docker (info|run --rm --pull never --network none --entrypoint df|context show|build -q -f deploy/docker/Dockerfile|save |ps -aq --filter "label=io\.x-k8s\.kind\.cluster=\$CLUSTER")`)
	for i, line := range strings.Split(read(t, "scripts/k8s-up.sh"), "\n") {
		code := strings.TrimSpace(line)
		if hasAnyPrefix(code, "#", "say ", "die ") || strings.Contains(code, `die "`) {
			continue
		}
		where := "k8s-up.sh:" + itoa(i+1) + ": "
		for _, m := range regexp.MustCompile(`\bdocker [a-z]+[^|;&)]*`).FindAllString(code, -1) {
			if strings.Contains(code, "command -v docker") || strings.Contains(code, "docker is not installed") {
				continue
			}
			if !allowedDocker.MatchString(m) && !strings.HasPrefix(m, "docker ps") {
				t.Errorf("%sdocker command outside the whitelist: %s", where, code)
			}
		}
		for _, bad := range []string{"volume prune", "system prune", "image prune", "docker rm", "docker rmi", "docker kill", "docker stop", "network rm", "network prune", "kind delete clusters", "xargs"} {
			if strings.Contains(code, bad) {
				t.Errorf("%sforbidden: %s", where, code)
			}
		}
		if regexp.MustCompile(`--all([^-a-z]|$)`).MatchString(code) || (strings.Contains(code, "$(docker ps") && !strings.HasPrefix(code, "left=")) {
			t.Errorf("%sforbidden: %s", where, code)
		}
		if strings.Contains(code, "kind delete") && !strings.Contains(code, `kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE"`) {
			t.Errorf("%skind delete must name the cluster and the kubeconfig: %s", where, code)
		}
		if regexp.MustCompile(`\brm\s+-[a-zA-Z]*r`).MatchString(code) {
			if !strings.Contains(code, `rm -rf "$K8S_DIR"`) || !strings.Contains(code, `"$K8S_DIR" != "$ROOT/.dev"`) {
				t.Errorf("%sthe only recursive removal is the validated state directory: %s", where, code)
			}
		}
	}
	// Modes of what holds credentials.
	script := read(t, "scripts/k8s-up.sh")
	for _, want := range []string{"umask 077", `chmod 700 "$K8S_DIR"`, `chmod 600 "$KUBECONFIG_FILE"`} {
		if !strings.Contains(script, want) {
			t.Errorf("k8s-up.sh no longer has %q (the kubeconfig must be 0600 in a 0700 directory)", want)
		}
	}
	// Secrets are applied server-side: client-side apply stores the Secret's data in the last-applied-configuration annotation.
	if !strings.Contains(script, "kc apply --server-side --field-manager=serverflow-k8s-up -f -") {
		t.Error("Secrets must be applied with --server-side (no last-applied annotation holding their data)")
	}
}

// TestSecretsGeneratorIgnoresTheEnvironmentsShellOptions: with SHELLOPTS=xtrace in the environment the generator, started
// by hand, prints the passwords it generates; through k8s-up.sh it must not.
func TestSecretsGeneratorIgnoresTheEnvironmentsShellOptions(t *testing.T) {
	for _, bash := range bashes(t) {
		leaks := func(viaScript bool) (string, []string) {
			dev := filepath.Join(stateDir(t), "dev")
			rel, _ := filepath.Rel(root(t), dev)
			env := []string{"PATH=/usr/bin:/bin", "SHELLOPTS=xtrace", "DEV_UP_DIR=" + rel}
			var out string
			if viaScript {
				out, _ = runScript(t, bash, env, "secrets")
			} else {
				cmd := exec.Command(bash, filepath.Join(root(t), "scripts/dev-up.sh"), "secrets")
				cmd.Dir = root(t)
				cmd.Env = append([]string{"HOME=" + t.TempDir()}, env...)
				b, _ := cmd.CombinedOutput()
				out = string(b)
			}
			var values []string
			for _, n := range []string{"postgres.env", "redis.env", "kafka.env", "controlplane.env", "metrics.env", "grafana.env"} {
				b, err := os.ReadFile(filepath.Join(dev, n))
				if err != nil {
					t.Fatalf("%s: %s was not generated: %v\n%s", bash, n, err, out)
				}
				_, v, _ := strings.Cut(strings.TrimSpace(string(b)), "=")
				if len(v) >= 16 && strings.Contains(out, v) {
					values = append(values, n)
				}
			}
			return out, values
		}
		if _, l := leaks(false); len(l) == 0 {
			t.Errorf("%s: the positive control failed: dev-up.sh under SHELLOPTS=xtrace no longer leaks, so this test proves nothing", bash)
		}
		if out, l := leaks(true); len(l) != 0 {
			t.Errorf("%s: k8s-up.sh secrets leaked %v under SHELLOPTS=xtrace (output withheld, %d bytes)", bash, l, len(out))
		}
	}
	if !regexp.MustCompile(`env -u SHELLOPTS .*"\$ROOT/scripts/dev-up\.sh" secrets`).MatchString(read(t, "scripts/k8s-up.sh")) {
		t.Error("the generator must be started with env -u SHELLOPTS")
	}
}

// TestToolRunnerIsConfined: the container fallbacks of scripts/k8s-tool.sh drop capabilities, run kubectl without a network,
// mount nothing but deploy/ and observability/ read-only for kubectl and nothing for kubeconform (the repository root holds
// .dev/, where the secrets are), and use a PATH binary only at exactly the pinned version.
func TestToolRunnerIsConfined(t *testing.T) {
	tool := read(t, "scripts/k8s-tool.sh")
	for _, want := range []string{
		"--cap-drop ALL", "--security-opt no-new-privileges",
		`-v "$ROOT/deploy":/work/deploy:ro`, `-v "$ROOT/observability":/work/observability:ro`,
		`in_container "$(pin KUBECTL_IMAGE)" none repo`, `in_container "$(pin KUBECONFORM_IMAGE)" bridge none`,
		`grep -q "Client Version: $want\$"`, `[ "$(kubeconform -v 2>/dev/null)" = "$want" ]`,
	} {
		if !strings.Contains(tool, want) {
			t.Errorf("k8s-tool.sh lost %q", want)
		}
	}
	for i, line := range strings.Split(tool, "\n") {
		if strings.Contains(line, "-v ") && strings.Contains(line, "/work") && !strings.Contains(line, ":ro") {
			t.Errorf("k8s-tool.sh:%d mounts read-write: %s", i+1, strings.TrimSpace(line))
		}
		if hasAnyPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if regexp.MustCompile(`-v "?\$ROOT"?:`).MatchString(line) || strings.Contains(line, ".dev") || strings.Contains(line, "--privileged") || strings.Contains(line, "--network host") || strings.Contains(line, "docker.sock") {
			t.Errorf("k8s-tool.sh:%d is too permissive: %s", i+1, strings.TrimSpace(line))
		}
	}
	// quality.sh hands the render to kubeconform on stdin, so that container needs no mount.
	if !regexp.MustCompile(`kubectl kustomize [^\n]*\\\n\s+\| scripts/k8s-tool.sh kubeconform -strict [^\n]* -\s*\\`).MatchString(read(t, "scripts/quality.sh")) {
		t.Error("quality.sh must pipe the render into `kubeconform -strict ... -` (standard input)")
	}
}

// TestQualityGateListsEveryTest: quality.sh k8s names the tests that must run; every top-level test of this package is in
// the list (so a new test cannot be silently skipped), the gate fails on a skip, and kubeconform runs strict.
func TestQualityGateListsEveryTest(t *testing.T) {
	q := read(t, "scripts/quality.sh")
	i := strings.Index(q, "\nk8s() {")
	if i < 0 {
		t.Fatal("quality.sh has no k8s stage")
	}
	stage := q[i:]
	stage = stage[:strings.Index(stage, "\n}\n")]
	list := regexp.MustCompile(`(?s)for t in (.*?); do`).FindStringSubmatch(stage)
	if list == nil {
		t.Fatal("the k8s stage has no must-run list")
	}
	named := map[string]bool{}
	for _, n := range strings.Fields(strings.ReplaceAll(list[1], "\\", " ")) {
		named[n] = true
	}
	files, _ := filepath.Glob(filepath.Join(root(t), "tests/k8s/*_test.go"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, m := range regexp.MustCompile(`(?m)^func (Test\w+)\(t \*testing\.T\)`).FindAllStringSubmatch(string(b), -1) {
			if !named[m[1]] {
				t.Errorf("%s is not in the must-run list of quality.sh k8s", m[1])
			}
			delete(named, m[1])
		}
	}
	for n := range named {
		t.Errorf("quality.sh k8s names %s, which does not exist", n)
	}
	for _, want := range []string{`grep -q -- "--- PASS: $t "`, `grep -q -- "--- SKIP"`, "kubeconform -strict -summary", "go test -race -count=1 -v ./tests/k8s/...", "--load-restrictor=LoadRestrictionsNone"} {
		if !strings.Contains(stage, want) {
			t.Errorf("quality.sh k8s lost %q", want)
		}
	}
	if !regexp.MustCompile(`CI:-\}" = "true" \]; then\n\s+fail`).MatchString(stage) {
		t.Error("in CI a missing tool must fail the k8s stage, not skip it")
	}
}
