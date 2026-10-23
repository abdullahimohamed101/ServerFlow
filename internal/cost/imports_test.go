package cost

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// internal/cost is the estimator shared by the rate limiter and the scheduler, so it must stay pure: only the
// standard library and pkg/protocol (no Redis, no gateway, no scheduler, no other internal package).
func TestCostImportsOnlyTheStandardLibraryAndProtocol(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range af.Imports {
			path := strings.Trim(im.Path.Value, `"`)
			first := strings.SplitN(path, "/", 2)[0]
			if strings.Contains(first, ".") || (strings.HasPrefix(path, "serverflow/") && path != "serverflow/pkg/protocol") {
				if strings.HasSuffix(f, "_test.go") && path == "serverflow/pkg/protocol" {
					continue
				}
				t.Errorf("%s imports %q: internal/cost may use only the standard library and serverflow/pkg/protocol", f, path)
			}
		}
	}
}
