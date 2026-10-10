// Package gofloor holds a documentation guard: the Go floor declared in go.mod, go.work and the
// documents must be one and the same version.
package gofloor

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func read(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// match returns the single capture group of the one required match of re in file rel.
func match(t *testing.T, rel string, re *regexp.Regexp) string {
	t.Helper()
	m := re.FindStringSubmatch(read(t, rel))
	if m == nil {
		t.Fatalf("%s does not state the Go version (pattern %s): the check would silently stop covering it", rel, re)
	}
	return m[1]
}

func TestGoFloorAgreesEverywhere(t *testing.T) {
	directive := regexp.MustCompile(`(?m)^go (\d+\.\d+)(?:\.\d+)?$`)
	floor := match(t, "go.mod", directive)
	if got := match(t, "go.work", directive); got != floor {
		t.Errorf("go.work says go %s but go.mod says go %s", got, floor)
	}
	docs := map[string]*regexp.Regexp{
		"README.md":                 regexp.MustCompile(`requires only Go (\d+\.\d+)\+`),
		"docs/development/setup.md": regexp.MustCompile(`(?m)^\| Go \| (\d+\.\d+)\+ \|`),
		"docs/development/ci.md":    regexp.MustCompile(`currently (\d+\.\d+)\)`),
	}
	for rel, re := range docs {
		if got := match(t, rel, re); got != floor {
			t.Errorf("%s documents Go %s+ but go.mod says go %s", rel, got, floor)
		}
	}
}
