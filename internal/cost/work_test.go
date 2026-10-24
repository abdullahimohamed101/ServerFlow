package cost

import (
	"math"
	"math/rand/v2"
	"testing"
)

// Work is the scheduler's token accounting (ADR-022, D3): input_weight * input + ceil(output_fraction * output).

func TestWorkHandComputed(t *testing.T) {
	tests := []struct {
		name    string
		in, out int
		w       Weights
		want    int64
	}{
		{"defaults are the plain sum", 120, 4096, DefaultWeights(), 4216},
		{"the zero Weights value weighs nothing, but a request still costs 1", 7, 9, Weights{}, 1},
		{"input weight halves the input", 1000, 500, Weights{InputWeight: 0.5, OutputFraction: 1}, 500 + 500},
		{"output fraction scales the budget", 1000, 500, Weights{InputWeight: 1, OutputFraction: 0.3}, 1000 + 150},
		{"input weight zero ignores the prompt", 1000, 500, Weights{InputWeight: 0, OutputFraction: 1}, 500},
		{"each part rounds up", 3, 3, Weights{InputWeight: 0.5, OutputFraction: 0.5}, 2 + 2},
		{"a request with any estimate costs at least 1", 1, 0, Weights{InputWeight: 0, OutputFraction: 0.5}, 1},
		{"no estimate costs nothing", 0, 0, DefaultWeights(), 0},
		{"negative estimates count as zero", -5, 10, DefaultWeights(), 10},
		{"the largest estimate does not overflow", MaxCost, MaxCost, Weights{InputWeight: 100, OutputFraction: 1}, 100*MaxCost + MaxCost},
	}
	for _, tt := range tests {
		got := Work(tt.in, tt.out, tt.w)
		if got != tt.want {
			t.Errorf("%s: Work(%d, %d, %+v) = %d, want %d", tt.name, tt.in, tt.out, tt.w, got, tt.want)
		}
	}
}

func TestWorkRepairsInvalidWeights(t *testing.T) {
	nan, inf := math.NaN(), math.Inf(1)
	for _, w := range []Weights{{nan, 1}, {1, nan}, {inf, 1}, {1, inf}, {-1, 1}, {1, -1}, {math.Inf(-1), math.Inf(-1)}} {
		got := Work(100, 200, w)
		if got < 1 || got > 100*1e4+200*1e4 {
			t.Errorf("Work with %+v = %d: out of the sane range", w, got)
		}
	}
	// an invalid weight reads as the default 1, so the others still apply
	if got := Work(100, 200, Weights{InputWeight: math.NaN(), OutputFraction: 0.5}); got != 100+100 {
		t.Errorf("NaN input weight: got %d, want 200", got)
	}
	// a weight above the bound is clamped, not trusted
	if got := Work(1, 0, Weights{InputWeight: 1e300, OutputFraction: 1}); got != 1e4 {
		t.Errorf("huge weight: got %d, want the clamp 10000", got)
	}
}

// With the default weights Work is exactly the sum Estimate returns, which is what the ledger held before the
// weights existed (PR B): turning the weights on cannot change a default deployment.
func TestWorkWithDefaultWeightsIsTheEstimateSum(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for i := 0; i < 20000; i++ {
		in, out := rng.IntN(1<<20), rng.IntN(1<<14)
		if rng.IntN(7) == 0 {
			in, out = rng.IntN(MaxCost), rng.IntN(MaxCost)
		}
		if got, want := Work(in, out, DefaultWeights()), int64(in)+int64(out); got != want {
			t.Fatalf("Work(%d, %d) = %d, want %d", in, out, got, want)
		}
	}
}

// Raising the estimate, or either weight, never lowers the work.
func TestWorkIsMonotonic(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	for i := 0; i < 20000; i++ {
		in, out := rng.IntN(5000), rng.IntN(5000)
		w := Weights{InputWeight: rng.Float64() * 3, OutputFraction: rng.Float64()}
		base := Work(in, out, w)
		if Work(in+1+rng.IntN(100), out, w) < base || Work(in, out+1+rng.IntN(100), w) < base {
			t.Fatalf("not monotonic in the estimate at %d %d %+v", in, out, w)
		}
		w2 := w
		w2.InputWeight += rng.Float64()
		w2.OutputFraction = math.Min(1, w2.OutputFraction+rng.Float64())
		if Work(in, out, w2) < base {
			t.Fatalf("not monotonic in the weights at %d %d %+v -> %+v", in, out, w, w2)
		}
	}
}
