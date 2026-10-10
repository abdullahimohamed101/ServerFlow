package observability

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// promtool runs scripts/promtool.sh (a local promtool, else a container). It skips the test when neither exists,
// except where SERVERFLOW_REQUIRE_PROMTOOL is set (the CI observability job and `scripts/quality.sh
// observability`): there a missing promtool is a failure, never a silent pass.
func promtool(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command(filepath.Join(root(t), "scripts/promtool.sh"), args...)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.ExitCode() == 3 {
		if os.Getenv("SERVERFLOW_REQUIRE_PROMTOOL") != "" {
			t.Fatalf("promtool is required here but not available:\n%s", out)
		}
		t.Skipf("promtool is not available (set up promtool or Docker; CI requires it): %s", strings.TrimSpace(string(out)))
	}
	if err != nil {
		t.Fatalf("promtool %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestPromtoolAcceptsTheConfigurationRulesAndRuleTests(t *testing.T) {
	promtool(t, "check", "config", "observability/prometheus/prometheus.yml")
	promtool(t, "check", "rules", "observability/prometheus/rules/recording.yml", "observability/prometheus/rules/alerts.yml")
	promtool(t, "test", "rules", "observability/prometheus/tests/recording.test.yml", "observability/prometheus/tests/alerts.test.yml")
}

// Every dashboard expression, with its variables substituted, must be valid PromQL. Each goes into a temporary
// rule file as a recording rule and promtool checks the file. The directory is inside the repository because a
// container runtime may share only part of the filesystem.
func TestEveryDashboardExpressionIsValidPromQL(t *testing.T) {
	exprs := dashboardExprs(t)
	keys := make([]string, 0, len(exprs))
	for k := range exprs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("groups:\n  - name: dashboard-expressions\n    rules:\n")
	for i, k := range keys {
		fmt.Fprintf(&b, "      # %s\n      - record: dashboard:expr_%d:value\n        expr: |\n          %s\n", k, i, SubstituteVariables(exprs[k]))
	}
	dir, err := os.MkdirTemp(root(t), ".promtool-dashboards-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	file := filepath.Join(dir, "dashboards.yml")
	if err := os.WriteFile(file, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	rel, _ := filepath.Rel(root(t), file)
	out := promtool(t, "check", "rules", rel)
	if !strings.Contains(out, fmt.Sprintf("%d rules found", len(keys))) {
		t.Fatalf("promtool did not see all %d expressions:\n%s", len(keys), out)
	}
}

// The mutation this guards against: a panel pointed at a misspelt metric would still be valid PromQL, so syntax
// alone is not enough. TestEveryMetricUsedIsExportedOrRecorded (exported_test.go) is the check that catches it.
