package report

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"serverflow/internal/bench/collect"
)

func result(id string, rps, p95Lat, ttftP95, jain, errRate float64) Result {
	j := jain
	r := Result{RunID: id, Metadata: baseMeta(), Imbalance: Imbalance{RequestJain: &j}}
	r.Metadata.Scheduler = "round-robin"
	r.Summary.RequestsPerSecond, r.Summary.ErrorRate = rps, errRate
	r.Summary.Latency = &collect.Dist{P95: p95Lat}
	if ttftP95 > 0 {
		r.Summary.TTFT = &collect.Dist{P95: ttftP95}
	}
	return r
}

func find(t *testing.T, c Comparison, key string) Delta {
	t.Helper()
	for _, d := range c.Deltas {
		if d.Key == key {
			return d
		}
	}
	t.Fatalf("no delta %s", key)
	return Delta{}
}

func TestDeltasAreBRelativeToAWithTheRightSign(t *testing.T) {
	a := result("run_001", 100, 200, 50, 0.8, 0.01)
	b := result("run_002", 125, 150, 60, 0.9, 0.03)
	c := Compare(a, b)
	check := func(key string, abs, pct float64) {
		t.Helper()
		d := find(t, c, key)
		if d.Abs == nil || math.Abs(*d.Abs-abs) > 1e-9 {
			t.Errorf("%s abs = %v, want %v", key, d.Abs, abs)
		}
		if pct != 0 {
			if d.Pct == nil || math.Abs(*d.Pct-pct) > 1e-9 {
				t.Errorf("%s pct = %v, want %v", key, d.Pct, pct)
			}
		}
	}
	check("throughput", 25, 25)      // (125-100)/100
	check("latency_p95", -50, -25)   // (150-200)/200: B is lower
	check("ttft_p95", 10, 20)        // (60-50)/50
	check("request_jain", 0.1, 12.5) // (0.9-0.8)/0.8
	check("error_rate", 2, 0)        // 1% -> 3% is +2 points
	if d := find(t, c, "error_rate"); d.Pct != nil {
		t.Error("error rate is compared in points, not percent")
	}
}

func TestComparingARunWithItselfGivesZeroDeltas(t *testing.T) {
	a := result("run_001", 100, 200, 50, 0.8, 0.01)
	c := Compare(a, a)
	for _, d := range c.Deltas {
		if d.Abs != nil && *d.Abs != 0 {
			t.Errorf("%s: abs %v", d.Key, *d.Abs)
		}
		if d.Pct != nil && *d.Pct != 0 {
			t.Errorf("%s: pct %v", d.Key, *d.Pct)
		}
	}
	if len(c.Differences) != 0 || c.Unfair() {
		t.Fatalf("%+v", c.Differences)
	}
}

func TestZeroAndMissingBaselinesHaveNoPercent(t *testing.T) {
	a := result("run_001", 0, 200, 0, 0.8, 0)
	b := result("run_002", 10, 200, 40, 0.8, 0)
	c := Compare(a, b)
	if d := find(t, c, "throughput"); d.Pct != nil || d.Abs == nil || *d.Abs != 10 {
		t.Errorf("a zero baseline has an absolute delta only: %+v", d)
	}
	if d := find(t, c, "ttft_p95"); d.A != nil || d.Abs != nil || d.Pct != nil {
		t.Errorf("one side not measured: no delta at all: %+v", d)
	}
	if _, ok := PercentDelta(0, 5); ok {
		t.Error("percent of zero")
	}
	if p, ok := PercentDelta(4, 3); !ok || p != -25 {
		t.Errorf("got %v", p)
	}
}

func TestDifferencesFlagUnfairComparisonsButExpectTheScheduler(t *testing.T) {
	a := result("run_001", 1, 1, 1, 1, 0)
	b := result("run_002", 1, 1, 1, 1, 0)
	b.Metadata.Scheduler = "least-active"
	c := Compare(a, b)
	if len(c.Differences) != 1 || !c.Differences[0].Expected || c.Unfair() {
		t.Fatalf("a scheduler difference alone is the point of the comparison: %+v", c.Differences)
	}

	b.Metadata.Seed = 2
	b.Metadata.Workload = "burst"
	n := 5
	b.Metadata.WorkerCount = &n
	b.Metadata.GitCommit = "def"
	c = Compare(a, b)
	if !c.Unfair() {
		t.Fatal("a different seed, workload, worker count and commit is unfair")
	}
	var out bytes.Buffer
	if err := c.Write(&out); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	for _, want := range []string{"WARNING", "! seed: 1 -> 2", "! workload: mixed -> burst", "! workers: 3 -> 5", "! git commit: abc -> def", "  scheduler: round-robin -> least-active", "throughput", "positive means B is higher, not better"} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
	for _, banned := range []string{"winner", "best"} {
		if strings.Contains(strings.ToLower(s), banned) {
			t.Errorf("must not declare a winner: contains %q", banned)
		}
	}
}

func TestSpreadSaysWhetherADifferenceExceedsRunToRunNoise(t *testing.T) {
	mk := func(rps ...float64) []Result {
		var g []Result
		for _, v := range rps {
			g = append(g, result("r", v, 100, 10, 1, 0))
		}
		return g
	}
	ga, gb := mk(100, 104, 98), mk(103, 99, 101)
	a, b := ga[1], gb[0]
	c := CompareGroups(a, b, ga, gb)
	if d := find(t, c, "throughput"); d.Spread != "within spread" {
		t.Errorf("ranges 98-104 and 99-103 overlap: %q", d.Spread)
	}
	gc := mk(120, 125, 118)
	c = CompareGroups(a, gc[0], ga, gc)
	if d := find(t, c, "throughput"); d.Spread != "outside spread" {
		t.Errorf("ranges 98-104 and 118-125 do not overlap: %q", d.Spread)
	}
	c = CompareGroups(a, gc[0], ga, gc)
	if d := find(t, c, "latency_p95"); d.Spread != "within spread" {
		t.Errorf("identical values lie within spread: %q", d.Spread)
	}
	if d := find(t, Compare(a, b), "throughput"); d.Spread != "no repeat data" {
		t.Errorf("single runs have no spread: %q", d.Spread)
	}
}

func TestLoadGroupFindsRunsOfTheSameRepeatGroup(t *testing.T) {
	base := t.TempDir()
	var first Result
	for i := 1; i <= 3; i++ {
		id, dir, _ := CreateRun(base)
		r := Build(Input{RunID: id, Metadata: baseMeta(), Window: collect.Window{End: sec(1)}})
		r.Metadata.Repeat = Repeat{Index: i, Of: 3, Group: "run_001"}
		if i == 3 {
			r.Metadata.Repeat.Group = "run_other"
		}
		if i == 1 {
			first = r
		}
		if err := Write(dir, r, nil); err != nil {
			t.Fatal(err)
		}
	}
	g, err := LoadGroup(base, first)
	if err != nil || len(g) != 2 {
		t.Fatalf("%d %v", len(g), err)
	}
	single := Build(Input{RunID: "run_x", Metadata: baseMeta()})
	if g, _ := LoadGroup(base, single); len(g) != 1 {
		t.Fatal("a run without repeats is its own group")
	}
}
