package chaos

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var chaosCmd = regexp.MustCompile("(?:go run \\./cmd/benchmark|benchmark) chaos ([^`\\n#]*)")

// Every `benchmark chaos` command in the benchmark note is parsed with the real flag parser, so the note
// cannot document a flag or a scenario the tool refuses.
func TestEveryDocumentedChaosCommandParses(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "benchmarks", "phase-15-chaos-harness.md"))
	if err != nil {
		t.Fatal(err)
	}
	cmds := chaosCmd.FindAllStringSubmatch(string(b), -1)
	if len(cmds) < 4 {
		t.Fatalf("found %d documented commands: the check would silently stop covering the note", len(cmds))
	}
	for _, m := range cmds {
		args := strings.Fields(m[1])
		if _, _, err := ParseFlags(args); err != nil {
			t.Errorf("the documented command %q is refused: %v", m[0], err)
		}
	}
}

// Every scenario is described in the note by name.
func TestTheNoteNamesEveryScenario(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "benchmarks", "phase-15-chaos-harness.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range Names() {
		if !strings.Contains(string(b), "`"+n+"`") {
			t.Errorf("the note does not describe %s", n)
		}
	}
}
