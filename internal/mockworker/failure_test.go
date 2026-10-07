package mockworker

import "testing"

func rolls(inj *Injector, n int) []bool {
	out := make([]bool, n)
	for i := range out {
		_, out[i] = inj.Roll()
	}
	return out
}

func TestInjectorRateZeroNeverFailsRateOneAlwaysFails(t *testing.T) {
	for _, f := range rolls(NewInjector(1, 0, ModeError), 5000) {
		if f {
			t.Fatal("rate 0 must never fail")
		}
	}
	for _, f := range rolls(NewInjector(1, 1, ModeError), 5000) {
		if !f {
			t.Fatal("rate 1 must always fail")
		}
	}
}

func TestInjectorIsReproducibleForASeed(t *testing.T) {
	a := rolls(NewInjector(42, 0.5, ModeError), 500)
	b := rolls(NewInjector(42, 0.5, ModeError), 500)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at roll %d", i)
		}
	}
	c := rolls(NewInjector(43, 0.5, ModeError), 500)
	same := true
	for i := range a {
		if a[i] != c[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different seeds produced an identical sequence")
	}
}

func TestInjectorRateIsApproximatelyHonored(t *testing.T) {
	const n = 20000
	fails := 0
	for _, f := range rolls(NewInjector(7, 0.25, ModeError), n) {
		if f {
			fails++
		}
	}
	if got := float64(fails) / n; got < 0.22 || got > 0.28 {
		t.Fatalf("observed failure rate %.3f, want about 0.25", got)
	}
}

func TestInjectorSequenceDoesNotDependOnRate(t *testing.T) {
	// Rolling consumes one number regardless of rate, so raising the rate only
	// adds failures; it never reshuffles which requests were already failing.
	low := rolls(NewInjector(9, 0.2, ModeError), 1000)
	high := rolls(NewInjector(9, 0.6, ModeError), 1000)
	for i := range low {
		if low[i] && !high[i] {
			t.Fatalf("request %d failed at rate 0.2 but not at 0.6", i)
		}
	}
}

func TestInjectorReturnsConfiguredMode(t *testing.T) {
	mode, _ := NewInjector(1, 1, ModeMidstream).Roll()
	if mode != ModeMidstream {
		t.Fatalf("got mode %q", mode)
	}
}
