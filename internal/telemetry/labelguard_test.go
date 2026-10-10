package telemetry

import (
	"fmt"
	"sync"
	"testing"
)

func TestLabelGuardCapsDistinctValues(t *testing.T) {
	g := NewLabelGuard(3, "other")
	for _, v := range []string{"a", "b", "c"} {
		if got := g.Value(v); got != v {
			t.Fatalf("%s -> %s", v, got)
		}
	}
	if got := g.Value("d"); got != "other" {
		t.Fatalf("the fourth value must overflow, got %q", got)
	}
	if got := g.Value("a"); got != "a" {
		t.Fatal("an admitted value stays admitted")
	}
	if g.Len() != 3 {
		t.Fatalf("len %d", g.Len())
	}
}

func TestLabelGuardHostileInputStaysBounded(t *testing.T) {
	g := NewLabelGuard(10, "other")
	distinct := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				v := g.Value(fmt.Sprintf("m-%d-%d", w, i))
				mu.Lock()
				distinct[v] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(distinct) != 11 || g.Len() != 10 {
		t.Fatalf("distinct outputs %d, admitted %d; want 11 (10 + other) and 10", len(distinct), g.Len())
	}
}

func TestLabelGuardSeenValueDoesNotAllocate(t *testing.T) {
	g := NewLabelGuard(4, "other")
	g.Value("x")
	if n := testing.AllocsPerRun(100, func() { g.Value("x"); g.Value("never-admitted-after-full") }); n != 0 {
		t.Fatalf("%v allocations", n)
	}
}
