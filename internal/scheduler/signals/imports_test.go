package signals

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

// The signals package must stay usable from a virtual-clock driver and from the router under its lock: no wall clock,
// no timers, no goroutines, no channels, no randomness, no I/O. The only import beyond the allowlist below is refused. The checker resolves import aliases through the AST,
// so renaming a package does not hide a call.
var (
	allowedImports = map[string]bool{
		"sort": true, "sync": true, "time": true, "serverflow/pkg/protocol": true,
	}
	// bannedTime are the time package functions that read or wait on the wall clock. Everything else
	// in time (Duration, constants, arithmetic) is allowed.
	bannedTime = map[string]bool{
		"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "AfterFunc": true,
		"NewTimer": true, "NewTicker": true, "Tick": true,
	}
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
			add(imp.Pos(), "import %q is not allowed in the pure signals package", path)
		}
		name := path[strings.LastIndex(path, "/")+1:]
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
			}
		}
		return true
	})
	return bad
}

func TestSignalsIsPure(t *testing.T) {
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
	if n < 2 {
		t.Fatalf("found only %d sources", n)
	}
}

// Each snippet is a way to sneak wall-clock time, global randomness or concurrency into the engine; the
// checker must reject every one (and accept the clean control).
func TestPurityCheckerRejectsBypasses(t *testing.T) {
	head := "package signals\n"
	cases := map[string]string{
		"time.Now":      head + `import "time"; var _ = time.Now()`,
		"aliased time":  head + `import tm "time"; var _ = tm.Now()`,
		"aliased Since": head + `import clk "time"; func f(t clk.Time) { _ = clk.Since(t) }`,
		"time.After":    head + `import "time"; var _ = time.After(1)`,
		"time.Tick":     head + `import "time"; var _ = time.Tick(1)`,
		"dot-import":    head + `import . "time"; var _ = Now()`,
		"math/rand":     head + `import "math/rand"; var _ = rand.Intn(3)`,
		"math/rand/v2":  head + `import "math/rand/v2"; var _ = rand.IntN(3)`,
		"crypto/rand":   head + `import "crypto/rand"; var _ = rand.Reader`,
		"syscall":       head + `import "syscall"; var _ = syscall.Getpid()`,
		"runtime":       head + `import "runtime"; var _ = runtime.NumCPU()`,
		"os":            head + `import "os"; var _ = os.Args`,
		"net/http":      head + `import "net/http"; var _ = http.StatusOK`,
		"sync/atomic":   head + `import "sync/atomic"; var _ atomic.Int64`,
		"internal pkg":  head + `import "serverflow/internal/api"; var _ = api.CodeNoCapacity`,
		"make chan":     head + `var _ = make(chan int)`,
		"select":        head + `func f(c chan int) { select { default: } }`,
		"go statement":  head + `func f() { go f() }`,
		"send":          head + `func f(c chan int) { c <- 1 }`,
		"receive":       head + `func f(c chan int) int { return <-c }`,
		"blank import":  head + `import _ "os"`,
	}
	for name, src := range cases {
		if len(purityViolations("x.go", src)) == 0 {
			t.Errorf("%s: not detected", name)
		}
	}
	clean := head + `import ("time"; "sort"; "sync"); var mu sync.Mutex; func f(d time.Duration, a []int) time.Duration { sort.Ints(a); mu.Lock(); defer mu.Unlock(); return d + time.Second }`
	if v := purityViolations("x.go", clean); len(v) != 0 {
		t.Errorf("clean control rejected: %v", v)
	}
}
