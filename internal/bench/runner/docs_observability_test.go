package runner

import (
	"strings"
	"testing"
)

// The benchmark commands in the Phase 10 observability notes are parsed with the real option parser, like the ones
// covered by docs_test.go, so a documented live-run command cannot drift from the harness.
func TestPhase10DocumentedBenchmarkCommandsParse(t *testing.T) {
	for _, file := range []string{"docs/operations/observability.md", "docs/benchmarks/phase-10-observability.md"} {
		t.Run(file, func(t *testing.T) {
			runs, _ := docExamples(t, file)
			if len(runs) == 0 {
				t.Fatalf("%s documents no benchmark command: the check would silently stop covering it", file)
			}
			for _, args := range runs {
				rf, err := ParseRun(args)
				if err != nil {
					t.Fatalf("the documented command %q is refused: %v", strings.Join(args, " "), err)
				}
				if w := rf.Options.LoadWarning(); w != "" {
					t.Errorf("the documented command %q would warn that it overloads its target: %s", strings.Join(args, " "), w)
				}
			}
		})
	}
}
