package scheduler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// The work-aware strategies are pure arithmetic over the snapshot (ADR-011, ADR-022): they read no clock, start no
// goroutine and do no I/O. The only state is the shared rotor. The check is on the source, so a later edit cannot
// quietly add a clock read.
func TestWorkAwareStrategiesAreSyntacticallyPure(t *testing.T) {
	allowed := map[string]bool{
		"context": true, "math": true, "sort": true,
		"serverflow/internal/cost": true, "serverflow/pkg/protocol": true,
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "workaware.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, im := range f.Imports {
		path, _ := strconv.Unquote(im.Path.Value)
		if !allowed[path] {
			t.Errorf("workaware.go imports %q: only %v are allowed", path, allowed)
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			t.Errorf("%s: go statement", fset.Position(x.Pos()))
		case *ast.SelectStmt, *ast.SendStmt, *ast.ChanType:
			t.Errorf("%s: channel use", fset.Position(n.Pos()))
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" {
					t.Errorf("%s: time.%s", fset.Position(x.Pos()), sel.Sel.Name)
				}
			}
		}
		return true
	})
}
