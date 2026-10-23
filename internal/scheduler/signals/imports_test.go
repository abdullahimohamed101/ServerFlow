package signals

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The signals package must stay pure (ADR-022): standard library and pkg/protocol only, no wall clock, no
// goroutines, no randomness source of its own. The clock is always passed in.
func TestSignalsIsPure(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range af.Imports {
			path := strings.Trim(im.Path.Value, `"`)
			first := strings.SplitN(path, "/", 2)[0]
			if (strings.Contains(first, ".") || strings.HasPrefix(path, "serverflow/")) && path != "serverflow/pkg/protocol" {
				t.Errorf("%s imports %q: signals may use only the standard library and serverflow/pkg/protocol", f, path)
			}
			if path == "math/rand" || path == "math/rand/v2" || path == "os" || path == "net/http" {
				t.Errorf("%s imports %q: signals is pure arithmetic", f, path)
			}
		}
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.GoStmt:
				t.Errorf("%s: signals starts no goroutines", fset.Position(x.Pos()))
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && id.Name == "time" && (x.Sel.Name == "Now" || x.Sel.Name == "Since" || x.Sel.Name == "Until" || x.Sel.Name == "After" || x.Sel.Name == "Sleep" || x.Sel.Name == "NewTimer" || x.Sel.Name == "NewTicker" || x.Sel.Name == "AfterFunc" || x.Sel.Name == "Tick") {
					t.Errorf("%s: signals reads no clock (time.%s): the caller passes the time in", fset.Position(x.Pos()), x.Sel.Name)
				}
			}
			return true
		})
	}
}
