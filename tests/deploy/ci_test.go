package deploy

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

var (
	stepOutputRef = regexp.MustCompile(`steps\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z0-9_-]+)`)
	dockerImgCmd  = regexp.MustCompile(`(?m)docker\s+(save|load|pull|run|rmi|image)\b[^\n]*`)
)

// TestWorkflowOutputsAndDockerCommands catches the failure classes of the first Phase 16 CI run: a step output that the
// producing step never wrote (so `docker save ""` ran with an empty name), an output used before its step, and a
// workflow expression pasted straight into a docker command where an empty value cannot be noticed.
func TestWorkflowOutputsAndDockerCommands(t *testing.T) {
	for _, wf := range []string{".github/workflows/ci.yml", ".github/workflows/nightly.yml"} {
		doc := readYAML(t, wf)
		for jobName, j := range asMap(t, doc["jobs"], wf+" jobs") {
			steps := asList(asMap(t, j, jobName)["steps"])
			producers := map[string]map[string]any{} // id -> step, for steps seen so far
			for i, sv := range steps {
				st := asMap(t, sv, fmt.Sprintf("%s step %d", jobName, i))
				text := fmt.Sprint(st["run"]) + " " + fmt.Sprint(st["with"]) + " " + fmt.Sprint(st["env"]) + " " + fmt.Sprint(st["if"])
				for _, m := range stepOutputRef.FindAllStringSubmatch(text, -1) {
					p, ok := producers[m[1]]
					if !ok {
						t.Errorf("%s/%s: step %d uses steps.%s.outputs.%s, but no earlier step has id %s", wf, jobName, i, m[1], m[2], m[1])
						continue
					}
					if _, isAction := p["uses"]; isAction {
						continue // an action's outputs are its own business
					}
					run := fmt.Sprint(p["run"])
					if !regexp.MustCompile(`\b` + regexp.QuoteMeta(m[2]) + `=[^\n]*GITHUB_OUTPUT`).MatchString(run) {
						t.Errorf("%s/%s: steps.%s.outputs.%s is never written (the step must append %s=... to $GITHUB_OUTPUT)", wf, jobName, m[1], m[2], m[2])
					}
				}
				if run, ok := st["run"].(string); ok {
					for _, c := range dockerImgCmd.FindAllString(run, -1) {
						if strings.Contains(c, "${{") {
							t.Errorf("%s/%s: %q pastes a workflow expression into a docker command; pass it through env: and check it is non-empty first", wf, jobName, strings.TrimSpace(c))
						}
						if c2 := strings.ReplaceAll(c, `--save ""`, ""); strings.Contains(c2, `""`) || strings.Contains(c2, "''") { // redis' --save "" is a real empty value
							t.Errorf("%s/%s: %q has an empty argument", wf, jobName, strings.TrimSpace(c))
						}
					}
					// docker save/load of a variable must be guarded by a non-empty check in the same step.
					if regexp.MustCompile(`docker\s+save\s+"\$[A-Za-z_{]`).MatchString(run) && !regexp.MustCompile(`\[ -z "\$[A-Za-z_]+" \]`).MatchString(run) {
						t.Errorf("%s/%s: docker save of a variable without a [ -z \"$VAR\" ] guard in the same step", wf, jobName)
					}
				}
				if id, ok := st["id"].(string); ok {
					producers[id] = st
				}
			}
		}
	}
}

func dockerComposeAvailable() bool {
	if runtime.GOOS == "windows" {
		return false
	}
	return exec.Command("docker", "compose", "version").Run() == nil
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root(t), from))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(to, from)), 0o755); err != nil {
		t.Fatal(err)
	}
	mode := os.FileMode(0o644)
	if strings.HasSuffix(from, ".sh") {
		mode = 0o755
	}
	if err := os.WriteFile(filepath.Join(to, from), b, mode); err != nil {
		t.Fatal(err)
	}
}

// TestComposeConfigFromACleanTree runs `docker compose config -q` and `scripts/dev-pull.sh --list` the way CI does: in a tree with no
// .dev/ and no .env (nothing generated yet). With Compose 2.38 (the runner) a missing env_file fails `config -q`; the
// placeholder helper must make it pass and leave the tree as it found it. Needs the docker CLI with the compose plugin
// (no daemon): `quality.sh deploy` requires this test whenever the plugin exists.
func TestComposeConfigFromACleanTree(t *testing.T) {
	if !dockerComposeAvailable() {
		t.Skip("docker compose is not installed")
	}
	dir := t.TempDir()
	for _, f := range []string{"docker-compose.yml", "docker-compose.persist.yml", "docker-compose.scale.yml", "docker-compose.private-control-plane.yml",
		"deploy/compose/.env.example", "deploy/compose/nginx.conf", "deploy/docker/Dockerfile", "scripts/dev-placeholders.sh", "scripts/dev-pull.sh",
		"observability/prometheus/compose/prometheus.yml", "observability/prometheus/compose/targets.yml"} {
		copyFile(t, f, dir)
	}
	run := func(name string, args ...string) (string, error) {
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if _, err := os.Stat(filepath.Join(dir, ".dev")); err == nil {
		t.Fatal("the clean tree must not have .dev")
	}
	// What CI's deploy stage does.
	if out, err := run("bash", "scripts/dev-placeholders.sh", "create"); err != nil {
		t.Fatalf("placeholders: %v\n%s", err, out)
	}
	for _, files := range [][]string{{"-f", "docker-compose.yml"}, {"-f", "docker-compose.yml", "-f", "docker-compose.scale.yml", "-f", "docker-compose.persist.yml", "-f", "docker-compose.private-control-plane.yml"}} {
		args := append([]string{"compose", "-p", "serverflow-ci", "--env-file", "deploy/compose/.env.example"}, files...)
		args = append(args, "--profile", "*", "config", "--no-env-resolution", "-q")
		if out, err := run("docker", args...); err != nil {
			t.Fatalf("compose config -q with placeholders failed: %v\n%s", err, out)
		}
	}
	for _, n := range []string{"controlplane", "dsn", "grafana", "kafka", "metrics", "postgres", "redis"} {
		if fi, err := os.Stat(filepath.Join(dir, ".dev", n+".env")); err != nil || fi.Size() != 0 {
			t.Errorf("placeholder %s.env must exist and be empty", n)
		}
	}
	if out, err := run("bash", "scripts/dev-placeholders.sh", "remove"); err != nil {
		t.Fatalf("remove: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dir, ".dev")); err == nil {
		t.Error("the placeholders and .dev/ must be gone after remove")
	}
	// The Redpanda reference for the cache step comes from here, with no .dev in the tree.
	out, err := run("bash", "scripts/dev-pull.sh", "--list")
	if err != nil {
		t.Fatalf("dev-pull --list: %v\n%s", err, out)
	}
	if !regexp.MustCompile(`(?m)^mirror\.gcr\.io/redpandadata/redpanda:[^@\s]+@sha256:[0-9a-f]{64}$`).MatchString(out) {
		t.Errorf("dev-pull --list did not print the pinned Redpanda reference:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(dir, ".dev")); err == nil {
		t.Error("dev-pull --list left .dev/ behind")
	}
	// A real file is never removed or emptied.
	if err := os.MkdirAll(filepath.Join(dir, ".dev"), 0o700); err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(dir, ".dev", "redis.env")
	if err := os.WriteFile(real, []byte("X=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("bash", "scripts/dev-placeholders.sh", "create"); err != nil {
		t.Fatal(err)
	}
	if _, err := run("bash", "scripts/dev-placeholders.sh", "remove"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(real); string(b) != "X=1\n" {
		t.Error("an existing secret file was touched")
	}
}
