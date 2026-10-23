package deploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestMakefileTargets: every command ADR-021 documents exists in the Makefile and calls a script that is
// executable in git.
func TestMakefileTargets(t *testing.T) {
	mk := read(t, "Makefile")
	for _, target := range []string{"dev-up", "dev-down", "dev-reset", "dev-smoke", "dev-audit", "dev-ps", "dev-logs", "dev-config", "dev-pull", "dev-psql", "dev-redis-cli", "dev-rpk", "dev-key", "deploy-check"} {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(target) + `:`).MatchString(mk) {
			t.Errorf("Makefile has no target %s", target)
		}
	}
	if !regexp.MustCompile(`(?m)^dev-up:\n\tscripts/dev-up\.sh up$`).MatchString(mk) {
		t.Error("make dev-up must run scripts/dev-up.sh up")
	}
	// The host-process workflow stays: nothing of Phase 16 replaced these targets.
	for _, target := range []string{"dev-cluster", "dev-postgres", "dev-redis", "dev-kafka", "dev-tracing"} {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(target) + `:`).MatchString(mk) {
			t.Errorf("the host-process target %s was removed", target)
		}
	}
	for _, s := range []string{"scripts/dev-up.sh", "scripts/dev-smoke.sh", "scripts/dev-pull.sh", "scripts/dev-postgres.sh", "scripts/dev-redis.sh", "scripts/dev-kafka.sh"} {
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

func bashOrSkip(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the dev scripts are bash scripts (WSL2 is the supported way on Windows)")
	}
	p, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal("bash is required to test scripts/dev-up.sh")
	}
	return p
}

func checkEnv(t *testing.T, content string) (int, string) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bashOrSkip(t), filepath.Join(root(t), "scripts/dev-up.sh"), "--check-env", f)
	out, err := cmd.CombinedOutput()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return code, string(out)
}

// TestDevUpCheckEnv: .env is hostile input. Every value that could add a flag, a variable, a command or a second
// line is rejected before Docker is touched, with the variable named and the value never echoed (AC 12).
func TestDevUpCheckEnv(t *testing.T) {
	if code, out := checkEnv(t, read(t, "deploy/compose/.env.example")); code != 0 {
		t.Fatalf("the committed .env.example must be valid: %s", out)
	}
	// Valid forms: comments, blank lines, CRLF, a byte order mark, an unpublished control plane.
	if code, out := checkEnv(t, "\xef\xbb\xbf# c\r\n\r\nPROFILES=core\r\nGATEWAYS=3\r\nCONTROL_PLANE_PORT=\r\n"); code != 0 {
		t.Errorf("CRLF/BOM file rejected: %s", out)
	}

	const canary = "CANARYVALUE"
	hostile := []struct{ name, line string }{
		{"dollar", "MOCK_3_TTFT=$" + canary},
		{"command substitution", "MOCK_3_TTFT=$(" + canary + ")"},
		{"backticks", "MOCK_3_TTFT=`" + canary + "`"},
		{"single quote", "MOCK_3_TTFT='" + canary + "'"},
		{"double quote", `MOCK_3_TTFT="` + canary + `"`},
		{"space", "MOCK_3_TTFT=1s " + canary},
		{"semicolon", "MOCK_3_TTFT=1s;" + canary},
		{"equals", "MOCK_3_TTFT=1s=" + canary},
		{"flag injection", "MOCK_3_FAILURE_RATE=0.5 --failure-mode=" + canary},
		{"flag injection without space", "MOCK_3_FAILURE_RATE=0.5,--failure-mode=drop"},
		{"newline injection via CR", "MOCK_3_TTFT=1s\rMOCK_3_SEED=" + canary},
		{"backslash", `MOCK_3_TTFT=1s\n` + canary},
		{"pipe", "MOCK_3_TTFT=1s|" + canary},
		{"ampersand", "MOCK_3_TTFT=1s&" + canary},
		{"redirect", "MOCK_3_TTFT=1s>" + canary},
		{"port zero", "GATEWAY_PORT=0"},
		{"port too low", "GATEWAY_PORT=80"},
		{"port too high", "GATEWAY_PORT=70000"},
		{"port not a number", "GATEWAY_PORT=" + canary},
		{"negative", "WORKER_QUEUE=-1"},
		{"unknown profile", "PROFILES=core," + canary},
		{"profiles without core", "PROFILES=data"},
		{"events without data", "PROFILES=core,events"},
		{"empty required value", "GATEWAYS="},
		{"empty profiles", "PROFILES="},
		{"gateways too many", "GATEWAYS=99"},
		{"unknown variable", "NOT_A_VARIABLE=1"},
		{"lowercase variable", "gateways=3"},
		{"not a pair", canary},
		{"strategy", "SF_STRATEGY=" + canary},
		{"resilience mode", "SF_RESILIENCE_MODE=" + canary},
		{"failure mode", "MOCK_1_FAILURE_MODE=" + canary},
		{"failure rate above one", "MOCK_1_FAILURE_RATE=1.5"},
		{"tps zero", "MOCK_1_TPS=0"},
		{"image from docker hub", "REDPANDA_IMAGE=docker.io/redpandadata/redpanda:v25.1.1"},
		{"image latest", "REDPANDA_IMAGE=quay.io/x/y:latest"},
		{"image without host", "REDPANDA_IMAGE=redpandadata/redpanda:v25.1.1"},
		{"project name upper case", "COMPOSE_PROJECT_NAME=Bad"},
		{"project name path", "COMPOSE_PROJECT_NAME=../x"},
		{"persist", "PERSIST=yes"},
		{"sample ratio", "TRACING_SAMPLE_RATIO=2"},
	}
	for _, h := range hostile {
		t.Run(h.name, func(t *testing.T) {
			code, out := checkEnv(t, h.line+"\n")
			if code == 0 {
				t.Fatalf("%q was accepted", h.line)
			}
			if strings.Contains(out, canary) {
				t.Errorf("the output echoes the value: %s", out)
			}
			if name, _, ok := strings.Cut(h.line, "="); ok && regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`).MatchString(name) && !strings.Contains(out, name) && name != canary {
				t.Errorf("the output does not name %s: %s", name, out)
			}
		})
	}

	// Two variables on one port, and duplicates, are refused as well.
	if code, _ := checkEnv(t, "GATEWAY_PORT=18080\nGRAFANA_PORT=18080\n"); code == 0 {
		t.Error("duplicate ports were accepted")
	}
	if code, _ := checkEnv(t, "GATEWAYS=1\nGATEWAYS=2\n"); code == 0 {
		t.Error("a variable set twice was accepted")
	}
}

var secretLiteral = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|credential)[A-Za-z_]*\s*[:=]\s*["']?[A-Za-z0-9+/_.-]{12,}["']?\s*$`)

// Files that ship the container deployment, scanned for secret-looking literals. (scripts/dev-{postgres,redis,kafka}.sh
// keep their documented, fixed, public passwords for the throwaway test servers; they are not part of the stack.)
func deploymentFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	for _, pat := range []string{"deploy/*/*", "deploy/*/.env.example", "docker-compose*.yml", "scripts/dev-up.sh", "scripts/dev-smoke.sh", "scripts/dev-pull.sh", "observability/prometheus/compose/prometheus.yml", "observability/prometheus/compose/targets.yml", ".dockerignore"} {
		m, err := filepath.Glob(filepath.Join(root(t), pat))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, m...)
	}
	if len(files) < 8 {
		t.Fatalf("expected the deployment files, found %d", len(files))
	}
	return files
}

// TestNoSecretsCommitted: no committed deployment file contains a password, token or key literal. Generated
// secrets live in .dev/ (gitignored); everything here refers to variables.
func TestNoSecretsCommitted(t *testing.T) {
	allowed := regexp.MustCompile(`\$|<|\bTOKEN_FILE\b|_file\b|credentials_file|/etc/prometheus/secrets/metrics_token`)
	for _, f := range deploymentFiles(t) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if secretLiteral.MatchString(line) && !allowed.MatchString(line) {
				rel, _ := filepath.Rel(root(t), f)
				t.Errorf("%s:%d looks like a secret literal", rel, i+1) // the line itself is not printed
			}
		}
	}
	// The retired skeleton's well-known passwords must not come back.
	for _, f := range deploymentFiles(t) {
		b, _ := os.ReadFile(f)
		for _, bad := range []string{"POSTGRES_PASSWORD: postgres", "POSTGRES_PASSWORD=postgres", "GF_SECURITY_ADMIN_PASSWORD: admin", "GF_SECURITY_ADMIN_PASSWORD=admin"} {
			if strings.Contains(string(b), bad) {
				t.Errorf("%s contains a default password", f)
			}
		}
	}
}

// TestComposeLeavesNoDefaultSecrets: no password or token has a default value in an interpolation.
func TestComposeLeavesNoDefaultSecrets(t *testing.T) {
	for _, f := range composeFiles {
		for _, m := range interp.FindAllStringSubmatch(read(t, f), -1) {
			if secretName.MatchString(m[1]) && m[2] != "" {
				t.Errorf("%s: ${%s} has a default value", f, m[1])
			}
		}
	}
}

// TestDevUpDoesNotUseComposeWait: `docker compose up --wait` fails on Compose 2.38 (the CI runner) for any service without a
// healthcheck (the one-shot jobs and the admin tool have none); dev-up.sh waits itself.
func TestDevUpDoesNotUseComposeWait(t *testing.T) {
	for _, f := range []string{"scripts/dev-up.sh", "scripts/dev-smoke.sh", "Makefile", ".github/workflows/ci.yml", ".github/workflows/nightly.yml"} {
		for i, line := range strings.Split(read(t, f), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if strings.Contains(line, "--wait") {
				t.Errorf("%s:%d uses --wait: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
	if !strings.Contains(read(t, "scripts/dev-up.sh"), "wait_stack") {
		t.Error("dev-up.sh must wait with wait_stack")
	}
}

// bashes returns the distinct bash interpreters available (the CI runner has 5.2, macOS /bin/bash is 3.2; they differ:
// `local x` leaves x unset in bash 5 and set-but-empty in 3.2, which is how an unbound variable once passed on a Mac and
// aborted `make dev-up` on the runner).
func bashes(t *testing.T) []string {
	t.Helper()
	bashOrSkip(t)
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
	return out
}

// TestDevUpSecretsUnderStrictShell runs the first-run secret generation (the part of `make dev-up` that only ever runs on a
// pristine tree) with `bash -u`, an empty environment and a temporary directory, under every bash available.
func TestDevUpSecretsUnderStrictShell(t *testing.T) {
	script := filepath.Join(root(t), "scripts/dev-up.sh")
	for _, bash := range bashes(t) {
		t.Run(filepath.Base(filepath.Dir(bash))+"/"+filepath.Base(bash), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "dev")
			run := func() string {
				cmd := exec.Command(bash, "-u", script, "secrets")
				cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "DEV_UP_DIR=" + dir}
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%s -u dev-up.sh secrets: %v\n%s", bash, err, out)
				}
				return string(out)
			}
			if out := run(); strings.TrimSpace(out) != "" {
				t.Errorf("secret generation must print nothing, got %q", out)
			}
			vals := map[string]string{}
			for _, n := range []string{"postgres.env", "dsn.env", "redis.env", "kafka.env", "controlplane.env", "metrics.env", "grafana.env"} {
				b, err := os.ReadFile(filepath.Join(dir, n))
				if err != nil || len(b) < 20 {
					t.Fatalf("%s missing or empty: %v", n, err)
				}
				fi, _ := os.Stat(filepath.Join(dir, n))
				if fi.Mode().Perm() != 0o600 {
					t.Errorf("%s mode %v, want 0600", n, fi.Mode().Perm())
				}
				_, v, _ := strings.Cut(strings.TrimSpace(string(b)), "=")
				vals[n] = v
			}
			if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
				t.Errorf("directory mode %v, want 0700", fi.Mode().Perm())
			}
			if !strings.Contains(vals["dsn.env"], ":"+vals["postgres.env"]+"@") {
				t.Error("the DSN does not carry the generated PostgreSQL password")
			}
			if b, _ := os.ReadFile(filepath.Join(dir, "metrics_token")); string(b) != vals["metrics.env"] {
				t.Error("metrics_token must equal the metrics token")
			}
			if fi, _ := os.Stat(filepath.Join(dir, "metrics_token")); fi.Mode().Perm() != 0o644 {
				t.Errorf("metrics_token mode %v, want 0644", fi.Mode().Perm())
			}
			seen := map[string]bool{}
			for n, v := range vals {
				if n != "dsn.env" && seen[v] {
					t.Errorf("%s repeats another secret", n)
				}
				seen[v] = true
			}
			// Idempotent: a second run keeps every value.
			before, _ := os.ReadFile(filepath.Join(dir, "redis.env"))
			run()
			after, _ := os.ReadFile(filepath.Join(dir, "redis.env"))
			if string(before) != string(after) {
				t.Error("a second run regenerated an existing secret")
			}
			// An empty placeholder (scripts/dev-placeholders.sh) counts as absent and is filled.
			if err := os.WriteFile(filepath.Join(dir, "kafka.env"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			run()
			if b, _ := os.ReadFile(filepath.Join(dir, "kafka.env")); len(b) < 20 {
				t.Error("an empty placeholder was not replaced by a generated secret")
			}
		})
	}
}

// TestScriptsUnderStrictShell runs the Docker-free paths of the scripts with `bash -u` and an empty environment.
func TestScriptsUnderStrictShell(t *testing.T) {
	for _, bash := range bashes(t) {
		for _, args := range [][]string{
			{"scripts/dev-up.sh", "--check-env", "deploy/compose/.env.example"},
			{"scripts/dev-placeholders.sh", "remove"},
		} {
			cmd := exec.Command(bash, append([]string{"-u"}, args...)...)
			cmd.Dir = root(t)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%s -u %v: %v\n%s", bash, args, err, out)
			}
		}
	}
}
