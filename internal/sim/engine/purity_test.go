package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The engine must stay usable from a virtual-clock driver: no wall clock, no timers, no goroutines, no
// locks, no I/O. This test reads the package's own non-test sources and rejects them.
func TestEngineIsPure(t *testing.T) {
	allowedImports := map[string]bool{
		"bytes": true, "crypto/sha256": true, "encoding/hex": true, "encoding/json": true, "errors": true,
		"fmt": true, "io": true, "math": true, "math/rand/v2": true, "sort": true, "time": true,
	}
	banned := map[string]bool{
		"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "AfterFunc": true,
		"NewTimer": true, "NewTicker": true, "Tick": true,
	}
	files, _ := filepath.Glob("*.go")
	n := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		n++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if !allowedImports[path] {
				t.Errorf("%s imports %q: not allowed in the pure engine", name, path)
			}
		}
		ast.Inspect(f, func(node ast.Node) bool {
			switch x := node.(type) {
			case *ast.GoStmt:
				t.Errorf("%s: go statement at %v", name, fset.Position(x.Pos()))
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && id.Name == "time" && banned[x.Sel.Name] {
					t.Errorf("%s: time.%s at %v", name, x.Sel.Name, fset.Position(x.Pos()))
				}
			}
			return true
		})
	}
	if n == 0 {
		t.Fatal("no sources found")
	}
	if _, err := os.Stat("engine.go"); err != nil {
		t.Fatal(err)
	}
}
