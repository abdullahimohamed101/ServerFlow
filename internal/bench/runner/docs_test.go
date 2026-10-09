package runner

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The benchmark commands printed in the README, the Makefile, the usage text and the benchmark
// notes are parsed with the real option parser. A documented command that the tool refuses, or
// that would overload its own cluster and fail its own validity gate, fails here, so the docs
// cannot drift from the harness.

var (
	benchArgs   = regexp.MustCompile("BENCH_ARGS=\"([^\"]*)\"")
	benchRunCmd = regexp.MustCompile("(?:go run \\./cmd/benchmark|benchmark) run\\s+(--[^`\\n#]*)")
	benchCmpCmd = regexp.MustCompile(`(?:make bench-compare A=(run_\d+) B=(run_\d+)|benchmark compare (?:--dir \S+ )?(run_\d+) (run_\d+))`)
)

func docExamples(t *testing.T, rel string) (runs [][]string, compares [][2]string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ReplaceAll(string(b), "\\\n", " ") // shell line continuations
	for _, m := range benchArgs.FindAllStringSubmatch(text, -1) {
		runs = append(runs, strings.Fields(m[1]))
	}
	for _, m := range benchRunCmd.FindAllStringSubmatch(text, -1) {
		runs = append(runs, strings.Fields(m[1]))
	}
	for _, m := range benchCmpCmd.FindAllStringSubmatch(text, -1) {
		a, b := m[1], m[2]
		if a == "" {
			a, b = m[3], m[4]
		}
		compares = append(compares, [2]string{a, b})
	}
	return runs, compares
}

func TestEveryDocumentedBenchmarkCommandParsesAndStaysWithinTheSafeLoad(t *testing.T) {
	for _, file := range []string{"README.md", "Makefile", "cmd/benchmark/main.go", "docs/benchmarks/phase-7-harness.md", "docs/plans/completed/phase-7-benchmark-harness.md"} {
		t.Run(file, func(t *testing.T) {
			runs, compares := docExamples(t, file)
			if len(runs) == 0 {
				t.Fatalf("%s documents no benchmark command: the check would silently stop covering it", file)
			}
			repeat := 1
			for _, args := range runs {
				if strings.Contains(strings.Join(args, " "), "...") {
					continue // a shortened repeat of an example above it
				}
				rf, err := ParseRun(args)
				if err != nil {
					t.Fatalf("the documented command %q is refused: %v", strings.Join(args, " "), err)
				}
				if w := rf.Options.LoadWarning(); w != "" {
					t.Errorf("the documented command %q would warn that it overloads its cluster: %s", strings.Join(args, " "), w)
				}
				repeat = max(repeat, rf.Options.Repeat)
			}
			for _, c := range compares {
				a, _ := strconv.Atoi(strings.TrimPrefix(c[0], "run_"))
				b, _ := strconv.Atoi(strings.TrimPrefix(c[1], "run_"))
				if d := a - b; c[0] == c[1] || (repeat > 1 && d > -repeat && d < repeat) {
					t.Errorf("compare %s %s would compare two runs of one repeat group (--repeat %d)", c[0], c[1], repeat)
				}
			}
		})
	}
}

func TestTheDocumentedExamplesAreFoundByTheCheck(t *testing.T) {
	runs, compares := docExamples(t, "README.md")
	if len(runs) < 2 || len(compares) < 1 {
		t.Fatalf("the README examples were not found: %v %v", runs, compares)
	}
}
