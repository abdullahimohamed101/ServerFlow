package collect

import (
	"math"
	"testing"
)

func seq(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = float64(i + 1)
	}
	return out
}

func TestPercentileIsNearestRank(t *testing.T) {
	tests := []struct {
		name string
		in   []float64
		p    float64
		want float64
		ok   bool
	}{
		// 1..20: p95 -> rank ceil(19) = 19; p99 -> ceil(19.8) = 20; p50 -> 10; p5 -> 1.
		{"p95 of 20", seq(20), 95, 19, true},
		{"p99 of 20", seq(20), 99, 20, true},
		{"p50 of 20", seq(20), 50, 10, true},
		{"p5 of 20", seq(20), 5, 1, true},
		{"p100 is the max", seq(20), 100, 20, true},
		// 1..100: the percentile equals its rank exactly.
		{"p95 of 100", seq(100), 95, 95, true},
		{"p99 of 100", seq(100), 99, 99, true},
		{"p50 of 100", seq(100), 50, 50, true},
		// 1..10: p50 -> rank 5; p95 -> ceil(9.5) = 10.
		{"p50 of 10", seq(10), 50, 5, true},
		{"p95 of 10", seq(10), 95, 10, true},
		{"single sample, any p", []float64{7}, 1, 7, true},
		{"single sample p99", []float64{7}, 99, 7, true},
		{"ties", []float64{3, 3, 3, 3}, 95, 3, true},
		{"two values p50", []float64{1, 9}, 50, 1, true},
		{"two values p51", []float64{1, 9}, 51, 9, true},
		{"empty", nil, 50, 0, false},
		{"p zero", seq(3), 0, 0, false},
		{"p over 100", seq(3), 100.5, 0, false},
		{"p NaN", seq(3), math.NaN(), 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Percentile(tc.in, tc.p)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("Percentile(p=%v) = %v, %v; want %v, %v", tc.p, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestSummarizeDoesNotModifyInputAndSortsACopy(t *testing.T) {
	in := []float64{5, 1, 4, 2, 3}
	d := Summarize(in)
	if in[0] != 5 || in[4] != 3 {
		t.Fatalf("input was reordered: %v", in)
	}
	// sorted 1..5: mean 3, p50 -> rank ceil(2.5) = 3 -> 3, p95 and p99 -> rank 5.
	want := Dist{Count: 5, Min: 1, Mean: 3, P50: 3, P95: 5, P99: 5, Max: 5}
	if *d != want {
		t.Fatalf("got %+v, want %+v", *d, want)
	}
}

func TestSummarizeOfNothingIsNilNotZero(t *testing.T) {
	if Summarize(nil) != nil || Summarize([]float64{}) != nil {
		t.Fatal("no samples must yield nil so the report can say 'not measured'")
	}
}

func TestJainIndex(t *testing.T) {
	tests := []struct {
		name string
		in   []float64
		want float64
		ok   bool
	}{
		{"equal", []float64{5, 5, 5, 5}, 1, true},
		{"one holds all of four", []float64{8, 0, 0, 0}, 0.25, true},
		// (1+3)^2 / (2 * (1+9)) = 16/20.
		{"one and three", []float64{1, 3}, 0.8, true},
		// (1+2+3)^2 / (3 * 14) = 36/42.
		{"one two three", []float64{1, 2, 3}, 36.0 / 42.0, true},
		{"single value", []float64{9}, 1, true},
		{"single zero", []float64{0}, 0, false},
		{"all zero", []float64{0, 0, 0}, 0, false},
		{"empty", nil, 0, false},
		{"negative", []float64{1, -1}, 0, false},
		{"NaN", []float64{1, math.NaN()}, 0, false},
		{"Inf", []float64{1, math.Inf(1)}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Jain(tc.in)
			if ok != tc.ok || math.Abs(got-tc.want) > 1e-12 {
				t.Fatalf("Jain(%v) = %v, %v; want %v, %v", tc.in, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestJainScalesInvariantly(t *testing.T) {
	a, _ := Jain([]float64{1, 2, 3})
	b, _ := Jain([]float64{100, 200, 300})
	if math.Abs(a-b) > 1e-12 {
		t.Fatalf("Jain must not depend on scale: %v vs %v", a, b)
	}
}

func TestMean(t *testing.T) {
	if v, ok := Mean([]float64{1, 2, 6}); !ok || v != 3 {
		t.Fatalf("got %v %v", v, ok)
	}
	if _, ok := Mean(nil); ok {
		t.Fatal("mean of nothing is undefined")
	}
}
