package engine

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The engine must stay usable from a virtual-clock driver: no wall clock, no timers, no goroutines, no
// channels, no locks, no global randomness, no I/O. The checker resolves import aliases through the AST,
// so renaming a package does not hide a call.
var (
	allowedImports = map[string]bool{
		"bytes": true, "crypto/sha256": true, "encoding/hex": true, "encoding/json": true, "errors": true,
		"fmt": true, "io": true, "math": true, "math/rand/v2": true, "reflect": true, "sort": true,
		"strings": true, "time": true, "unicode/utf8": true,
	}
	// bannedTime are the time package functions that read or wait on the wall clock. Everything else
	// in time (Duration, constants, arithmetic) is allowed.
	bannedTime = map[string]bool{
		"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "AfterFunc": true,
		"NewTimer": true, "NewTicker": true, "Tick": true,
	}
	// allowedRand are the only math/rand/v2 names the engine may use: constructors of its own seeded
	// generator. Package-level functions (IntN, Float64, ...) draw from a global, unseeded source.
	allowedRand = map[string]bool{"New": true, "NewPCG": true, "Rand": true, "PCG": true}
)

// purityViolations returns what is wrong with one source file.
func purityViolations(name string, src any) []string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		return []string{err.Error()}
	}
	var bad []string
	add := func(pos token.Pos, format string, args ...any) {
		bad = append(bad, fset.Position(pos).String()+": "+fmt.Sprintf(format, args...))
	}
	local := map[string]string{} // identifier -> import path
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if !allowedImports[path] {
			add(imp.Pos(), "import %q is not allowed in the pure engine", path)
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if path == "math/rand/v2" {
			name = "rand"
		}
		if imp.Name != nil {
			name = imp.Name.Name
		}
		switch name {
		case ".":
			add(imp.Pos(), "dot-import of %q hides what is called", path)
		case "_":
		default:
			local[name] = path
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			add(x.Pos(), "go statement")
		case *ast.SelectStmt:
			add(x.Pos(), "select statement")
		case *ast.SendStmt:
			add(x.Pos(), "channel send")
		case *ast.ChanType:
			add(x.Pos(), "channel type")
		case *ast.UnaryExpr:
			if x.Op == token.ARROW {
				add(x.Pos(), "channel receive")
			}
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			if !ok || id.Obj != nil { // a local variable or field, not a package
				return true
			}
			switch local[id.Name] {
			case "time":
				if bannedTime[x.Sel.Name] {
					add(x.Pos(), "time.%s reads or waits on the wall clock", x.Sel.Name)
				}
			case "math/rand/v2":
				if !allowedRand[x.Sel.Name] {
					add(x.Pos(), "rand.%s uses the global random source", x.Sel.Name)
				}
			}
		}
		return true
	})
	return bad
}

func TestEngineIsPure(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	n := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		n++
		for _, v := range purityViolations(name, nil) {
			t.Errorf("%s", v)
		}
	}
	if n < 3 {
		t.Fatalf("found only %d sources", n)
	}
}

// Each snippet is a way to sneak wall-clock time, global randomness or concurrency into the engine; the
// checker must reject every one (and accept the clean control).
func TestPurityCheckerRejectsBypasses(t *testing.T) {
	head := "package engine\n"
	cases := map[string]string{
		"time.Now":            head + `import "time"; var _ = time.Now()`,
		"aliased time":        head + `import tm "time"; var _ = tm.Now()`,
		"aliased Since":       head + `import clk "time"; func f(t clk.Time) { _ = clk.Since(t) }`,
		"time.After":          head + `import "time"; var _ = time.After(1)`,
		"dot-import":          head + `import . "time"; var _ = Now()`,
		"global rand":         head + `import "math/rand/v2"; var _ = rand.IntN(3)`,
		"aliased global rand": head + `import r "math/rand/v2"; var _ = r.Float64()`,
		"math/rand v1":        head + `import "math/rand"; var _ = rand.Intn(3)`,
		"make chan":           head + `var _ = make(chan int)`,
		"select":              head + `func f(c chan int) { select { default: } }`,
		"go statement":        head + `func f() { go f() }`,
		"send":                head + `func f(c chan int) { c <- 1 }`,
		"receive":             head + `func f(c chan int) int { return <-c }`,
		"sync":                head + `import "sync"; var _ sync.Mutex`,
		"os":                  head + `import "os"; var _ = os.Args`,
	}
	for name, src := range cases {
		if len(purityViolations("x.go", src)) == 0 {
			t.Errorf("%s: not detected", name)
		}
	}
	clean := head + `import ("time"; "math/rand/v2"); func f(d time.Duration) *rand.Rand { _ = d + time.Second; return rand.New(rand.NewPCG(1, 2)) }`
	if v := purityViolations("x.go", clean); len(v) != 0 {
		t.Errorf("clean control rejected: %v", v)
	}
}
