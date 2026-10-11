package deploy

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	fromLine   = regexp.MustCompile(`(?m)^FROM\s+(\S+)(?:\s+AS\s+(\S+))?\s*$`)
	pinnedFrom = regexp.MustCompile(`^[a-z0-9.\-]+(:[0-9]+)?/[a-z0-9._/\-]+:[A-Za-z0-9._\-]+@sha256:[0-9a-f]{64}$`)
)

// TestDockerfile pins the properties ADR-021 promises: literal digest-pinned FROM lines on non-Docker-Hub hosts,
// a non-root runtime, a static build, one target per shipped command, and nothing that bakes a secret in.
func TestDockerfile(t *testing.T) {
	raw := read(t, "deploy/docker/Dockerfile")
	if strings.HasPrefix(strings.TrimSpace(raw), "# syntax") || strings.Contains(raw, "\n# syntax=") {
		t.Error("a # syntax line would require BuildKit; the Dockerfile must build with the classic builder")
	}
	df := stripComments(raw)

	froms := fromLine.FindAllStringSubmatch(df, -1)
	if len(froms) < 7 {
		t.Fatalf("expected a build stage, a runtime stage and six targets, found %d FROM lines", len(froms))
	}
	stages := map[string]bool{}
	for _, f := range froms {
		ref, name := f[1], f[2]
		if name != "" {
			stages[name] = true
		}
		if stages[ref] { // FROM runtime AS gateway
			continue
		}
		if !pinnedFrom.MatchString(ref) {
			t.Errorf("FROM %s must be host/path:tag@sha256:digest, literal", ref)
		}
		for _, hub := range []string{"docker.io/", "index.docker.io/"} {
			if strings.HasPrefix(ref, hub) {
				t.Errorf("FROM %s comes from Docker Hub, which rate-limits CI", ref)
			}
		}
		if strings.Contains(ref, ":latest") || !strings.Contains(ref, "@sha256:") {
			t.Errorf("FROM %s is not pinned", ref)
		}
	}

	if strings.Contains(df, "FROM ${") || strings.Contains(df, "FROM $") {
		t.Error("FROM lines must be literal (hadolint DL3006, Dependabot)")
	}
	for _, want := range []string{"CGO_ENABLED=0", "-trimpath", "-buildvcs=false", "-buildid=", "internal/version.Version", "internal/version.Commit", "USER 65532:65532"} {
		if !strings.Contains(df, want) {
			t.Errorf("Dockerfile lacks %q", want)
		}
	}
	for _, banned := range []string{"\nADD ", "\nVOLUME ", "--mount=", "<<EOF", "BUILDPLATFORM", "COPY . ", "COPY .env", "sudo"} {
		if strings.Contains(df, banned) {
			t.Errorf("Dockerfile contains %q (classic-builder compatibility, secrets, or reproducibility)", banned)
		}
	}
	secretish := regexp.MustCompile(`(?mi)^\s*(ENV|ARG)\s+\S*(PASSWORD|TOKEN|SECRET|KEY|DSN|CREDENTIAL)`)
	if m := secretish.FindString(df); m != "" {
		t.Errorf("a secret-looking ENV or ARG is baked into the image build: %q", strings.TrimSpace(m))
	}
	for _, line := range strings.Split(df, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "ENTRYPOINT") && !strings.Contains(line, `["`) {
			t.Errorf("ENTRYPOINT must be exec form (the process must be PID 1 to get SIGTERM): %s", line)
		}
	}

	// One target per command under cmd/, except the host tools.
	entries, err := os.ReadDir(filepath.Join(root(t), "cmd"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "benchmark" || e.Name() == "healthcheck" {
			continue
		}
		if !stages[e.Name()] {
			t.Errorf("cmd/%s has no Dockerfile target", e.Name())
		}
		if !strings.Contains(df, "./cmd/"+e.Name()) {
			t.Errorf("cmd/%s is not built by the build stage", e.Name())
		}
	}
	if !strings.Contains(df, "./cmd/healthcheck") {
		t.Error("the healthcheck binary must be built; the images have no shell, curl or wget")
	}
}

// TestDockerignore keeps secrets and bulk out of the build context.
func TestDockerignore(t *testing.T) {
	di := read(t, ".dockerignore")
	have := map[string]bool{}
	for _, l := range strings.Split(di, "\n") {
		have[strings.TrimSpace(l)] = true
	}
	for _, must := range []string{".git", ".dev", ".env", ".env.*", ".worktrees", "docs", "**/*_test.go"} {
		if !have[must] {
			t.Errorf(".dockerignore must list %q", must)
		}
	}
}

// TestGitignoreCoversGeneratedFiles: the generated secrets directory and environment files never reach git.
func TestGitignoreCoversGeneratedFiles(t *testing.T) {
	gi := read(t, ".gitignore")
	for _, must := range []string{".dev/", ".env", ".env.*", "!.env.example"} {
		found := false
		for _, l := range strings.Split(gi, "\n") {
			if strings.TrimSpace(l) == must {
				found = true
			}
		}
		if !found {
			t.Errorf(".gitignore must list %q", must)
		}
	}
}

// stripComments drops whole-line comments so prose about forbidden constructs is not mistaken for them.
func stripComments(s string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
