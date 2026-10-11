package tracing_test

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"serverflow/internal/config"
	"serverflow/internal/mockworker"
)

// The commands, files, settings and flags the tracing documentation names must exist, and its YAML example
// must load and validate, so the docs cannot drift from the code.

var tracingDocs = []string{"docs/operations/tracing.md", "docs/benchmarks/phase-11-tracing.md", "docs/decisions/ADR-018-opentelemetry-tracing.md", "README.md"}

func readRepo(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDocumentedMakeTargetsScriptsAndFilesExist(t *testing.T) {
	makefile := readRepo(t, "Makefile")
	targets := regexp.MustCompile(`make (dev-tracing[a-z-]*)`)
	paths := regexp.MustCompile("`?((?:scripts|observability|internal|docs)/[A-Za-z0-9_./-]+\\.(?:sh|yml|go|md))`?")
	seen := 0
	for _, doc := range tracingDocs {
		text := readRepo(t, doc)
		for _, m := range targets.FindAllStringSubmatch(text, -1) {
			seen++
			if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(m[1]) + `:`).MatchString(makefile) {
				t.Errorf("%s documents `make %s`, which the Makefile does not define", doc, m[1])
			}
		}
		for _, m := range paths.FindAllStringSubmatch(text, -1) {
			p := m[1]
			if strings.Contains(p, "*") {
				continue
			}
			if _, err := os.Stat(filepath.Join("..", "..", p)); err != nil {
				t.Errorf("%s names %s, which does not exist", doc, p)
			}
		}
	}
	if seen == 0 {
		t.Fatal("the documentation names no make target: the check would silently stop covering it")
	}
	info, err := os.Stat(filepath.Join("..", "..", "scripts", "trace-demo.sh"))
	if err != nil || info.Mode()&0o111 == 0 {
		t.Errorf("scripts/trace-demo.sh must exist and be executable: %v", err)
	}
}

func TestDocumentedEnvironmentVariablesAreRead(t *testing.T) {
	src := readRepo(t, "internal/config/tracing.go")
	env := regexp.MustCompile(`SERVERFLOW_TRACING_([A-Z_]+)`)
	n := 0
	for _, doc := range tracingDocs {
		for _, m := range env.FindAllStringSubmatch(readRepo(t, doc), -1) {
			if m[1] == "OVERHEAD" {
				continue // the opt-in switch of TestTracingOverhead, a test setting and not a gateway one
			}
			n++
			if !strings.Contains(src, `"`+m[1]+`"`) {
				t.Errorf("%s documents SERVERFLOW_TRACING_%s, which internal/config/tracing.go does not read", doc, m[1])
			}
		}
	}
	if n == 0 {
		t.Fatal("no environment variable documented")
	}
}

func TestDocumentedYAMLExampleLoadsAndValidates(t *testing.T) {
	text := readRepo(t, "docs/operations/tracing.md")
	m := regexp.MustCompile("(?s)```yaml\n(.*?)```").FindStringSubmatch(text)
	if m == nil {
		t.Fatal("docs/operations/tracing.md has no yaml example")
	}
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(m[1]), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("the documented example does not load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the documented example is invalid: %v", err)
	}
	want := config.Default().Tracing
	want.Enabled = true
	if cfg.Tracing != want {
		t.Errorf("the documented example differs from the defaults it claims to show:\n got %v\nwant %v", cfg.Tracing, want)
	}
}

func TestDocumentedMockWorkerFlagsAreAccepted(t *testing.T) {
	flags := regexp.MustCompile(`(--otlp-endpoint=\S+?|--trace-insecure-ok)[\x60 ]`)
	n := 0
	for _, doc := range append(tracingDocs, "docs/development/mock-worker.md") {
		for _, m := range flags.FindAllStringSubmatch(readRepo(t, doc)+" ", -1) {
			n++
			arg := strings.TrimRight(m[1], "`.,)")
			args := []string{arg}
			if strings.HasPrefix(arg, "--trace-insecure-ok") {
				args = []string{"--otlp-endpoint=http://collector.example:4318", arg}
			}
			if _, _, err := mockworker.ParseFlags(args, io.Discard); err != nil {
				t.Errorf("%s documents %q, which mock-worker refuses: %v", doc, arg, err)
			}
		}
	}
	if n == 0 {
		t.Fatal("no mock-worker tracing flag documented")
	}
}
