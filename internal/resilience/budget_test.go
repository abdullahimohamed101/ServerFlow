package resilience

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestBudget(p BudgetParams) (*Budget, *fakeClock) {
	clk := newFakeClock()
	return NewBudget(p, clk.Now), clk
}

func std() BudgetParams { return BudgetParams{Ratio: 0.2, Min: 10, Window: 10 * time.Second} }

func reserveUpTo(b *Budget, model string, max int) int {
	n := 0
	for ; n < max; n++ {
		if _, ok := b.Reserve(model); !ok {
			break
		}
	}
	return n
}

func TestBudgetMinimumAtLowVolume(t *testing.T) {
	b, _ := newTestBudget(std())
	for i := 0; i < 5; i++ {
		b.NoteFirst("m")
	}
	// 20% of 5 is 1, the minimum 10 dominates: exactly 10 retries, the 11th is denied.
	if got := reserveUpTo(b, "m", 100); got != 10 {
		t.Fatalf("allowed %d retries, want exactly 10", got)
	}
	if _, ok := b.Reserve("m"); ok {
		t.Fatal("the next one must be denied")
	}
}

func TestBudgetRatioAtHighVolume(t *testing.T) {
	for _, tc := range []struct{ firsts, want int }{{50, 10}, {51, 10}, {54, 10}, {55, 11}, {100, 20}, {1000, 200}, {14, 10}} {
		b, _ := newTestBudget(std())
		for i := 0; i < tc.firsts; i++ {
			b.NoteFirst("m")
		}
		if got := reserveUpTo(b, "m", 10000); got != tc.want {
			t.Errorf("%d first attempts: %d retries, want %d", tc.firsts, got, tc.want)
		}
	}
}

func TestBudgetDeniedReserveConsumesNothing(t *testing.T) {
	b, _ := newTestBudget(std())
	reserveUpTo(b, "m", 100)
	for i := 0; i < 5; i++ {
		b.Reserve("m")
	}
	_, retries := b.Usage("m")
	if retries != 10 {
		t.Fatalf("retries in window = %d, denials must not count", retries)
	}
}

func TestBudgetRefundGivesTheRetryBack(t *testing.T) {
	b, _ := newTestBudget(std())
	res := make([]Reservation, 0, 10)
	for i := 0; i < 10; i++ {
		r, ok := b.Reserve("m")
		if !ok {
			t.Fatal("within the minimum")
		}
		res = append(res, r)
	}
	if _, ok := b.Reserve("m"); ok {
		t.Fatal("exhausted")
	}
	b.Refund(res[3]) // no other worker was found for this retry
	if _, ok := b.Reserve("m"); !ok {
		t.Fatal("a refunded retry frees its place")
	}
	if _, ok := b.Reserve("m"); ok {
		t.Fatal("exactly one place was freed")
	}
	b.Refund(Reservation{}) // zero value is harmless
}

func TestBudgetRefundAfterTheWindowMovedOnChangesNothing(t *testing.T) {
	b, clk := newTestBudget(std())
	r, _ := b.Reserve("m")
	clk.Advance(11 * time.Second)
	r2, _ := b.Reserve("m") // a retry in the current bucket
	b.Refund(r)             // its bucket is gone: must not take the new retry's count
	if _, retries := b.Usage("m"); retries != 1 {
		t.Fatalf("retries = %d, want 1", retries)
	}
	b.Refund(r2)
	if _, retries := b.Usage("m"); retries != 0 {
		t.Fatalf("retries = %d, want 0", retries)
	}
}

func TestBudgetWindowEdges(t *testing.T) {
	b, clk := newTestBudget(std())
	reserveUpTo(b, "m", 100) // 10 retries at t0
	clk.Advance(9*time.Second + 900*time.Millisecond)
	if _, ok := b.Reserve("m"); ok {
		t.Fatal("still inside the window at t0+9.9s")
	}
	clk.Advance(100 * time.Millisecond) // t0+10s: the bucket left the window
	if got := reserveUpTo(b, "m", 100); got != 10 {
		t.Fatalf("after the window rolled: %d retries, want a fresh 10", got)
	}
}

func TestBudgetWindowSlidesRatherThanResets(t *testing.T) {
	p := std()
	p.Min = 0
	p.Ratio = 1
	b, clk := newTestBudget(p)
	for i := 0; i < 4; i++ {
		b.NoteFirst("m") // 4 firsts at t0
	}
	reserveUpTo(b, "m", 2) // 2 retries at t0
	clk.Advance(5 * time.Second)
	for i := 0; i < 4; i++ {
		b.NoteFirst("m") // 4 more at t0+5
	}
	// 8 firsts, 2 retries: 6 more allowed.
	if got := reserveUpTo(b, "m", 100); got != 6 {
		t.Fatalf("allowed %d, want 6", got)
	}
	clk.Advance(5 * time.Second) // t0+10: the first 4 firsts and 2 retries leave; 4 firsts and 6 retries remain
	if _, ok := b.Reserve("m"); ok {
		t.Fatal("window is sliding: 6 retries still count against 4 firsts")
	}
	clk.Advance(5 * time.Second) // everything left
	if f, r := b.Usage("m"); f != 0 || r != 0 {
		t.Fatalf("usage = %d,%d", f, r)
	}
}

func TestBudgetModelsAreIndependent(t *testing.T) {
	b, _ := newTestBudget(std())
	reserveUpTo(b, "a", 100)
	if _, ok := b.Reserve("a"); ok {
		t.Fatal("a exhausted")
	}
	if got := reserveUpTo(b, "b", 100); got != 10 {
		t.Fatalf("b has its own budget: %d", got)
	}
}

func TestBudgetOverflowModelsShareOther(t *testing.T) {
	p := std()
	p.MaxModels = 2
	b, _ := newTestBudget(p)
	b.NoteFirst("a")
	b.NoteFirst("b")
	// c, d, e are beyond the cap: they share one budget.
	n := 0
	for _, m := range []string{"c", "d", "e", "c", "d", "e", "c", "d", "e", "c", "d", "e"} {
		if _, ok := b.Reserve(m); ok {
			n++
		}
	}
	if n != 10 {
		t.Fatalf("overflow models got %d retries together, want one shared budget of 10", n)
	}
	if got := reserveUpTo(b, "a", 100); got != 10 {
		t.Fatalf("a tracked model keeps its own budget: %d", got)
	}
}

func TestBudgetFirstAttemptsCountOnlyWhatIsNoted(t *testing.T) {
	b, _ := newTestBudget(BudgetParams{Ratio: 0.5, Min: 0, Window: 10 * time.Second})
	if _, ok := b.Reserve("m"); ok {
		t.Fatal("no first attempts and no minimum: no retries")
	}
	b.NoteFirst("m")
	b.NoteFirst("m")
	if got := reserveUpTo(b, "m", 10); got != 1 {
		t.Fatalf("2 firsts at 0.5 -> %d", got)
	}
}

func TestBudgetPruneForgetsUnservedModels(t *testing.T) {
	b, _ := newTestBudget(std())
	b.NoteFirst("keep")
	b.NoteFirst("gone")
	reserveUpTo(b, "gone", 3)
	b.Prune(func(m string) bool { return m == "keep" })
	if f, r := b.Usage("gone"); f != 0 || r != 0 {
		t.Fatalf("pruned model still has usage %d,%d", f, r)
	}
	if f, _ := b.Usage("keep"); f != 1 {
		t.Fatal("kept model lost its counts")
	}
}

// Concurrent reservations never overshoot the limit by even one.
func TestBudgetAtomicReserveUnderRace(t *testing.T) {
	b, _ := newTestBudget(BudgetParams{Ratio: 0, Min: 100, Window: 10 * time.Second})
	var granted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, ok := b.Reserve("m"); ok {
					granted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 100 {
		t.Fatalf("granted %d, want exactly 100", granted.Load())
	}
}

// Amplification against a worker that always fails: every first attempt retries once, but the budget caps the
// total at the ratio plus the minimum allowance.
func TestBudgetBoundsAmplification(t *testing.T) {
	b, clk := newTestBudget(std())
	const requests = 1000
	attempts := 0
	for i := 0; i < requests; i++ {
		b.NoteFirst("m")
		attempts++
		if _, ok := b.Reserve("m"); ok {
			attempts++
		}
		clk.Advance(5 * time.Millisecond) // 200 req/s for 5 s
	}
	max := float64(requests)*(1+0.2) + 10
	if float64(attempts) > max {
		t.Fatalf("%d attempts for %d requests exceeds 1 + budget_ratio plus the minimum (%v)", attempts, requests, max)
	}
	t.Logf("attempts per request: %.3f", float64(attempts)/requests)
}
