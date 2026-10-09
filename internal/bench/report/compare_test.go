package report

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"serverflow/internal/bench/collect"
)

func result(id string, rps, p95Lat, ttftP95, jain, errRate float64) Result {
	j := jain
	r := Result{RunID: id, Valid: true, Metadata: baseMeta(), Imbalance: Imbalance{RequestJain: &j}}
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

func group(rps ...float64) []Result {
	var g []Result
	for i, v := range rps {
		g = append(g, result("run_"+string(rune('a'+i)), v, 100, 10, 1, 0))
	}
	return g
}

func TestSpreadNeedsThreeRunsOnBothSides(t *testing.T) {
	for name, tc := range map[string]struct{ a, b []Result }{
		"1 vs 3": {group(100), group(103, 99, 101)},
		"3 vs 1": {group(100, 104, 98), group(120)},
		"2 vs 2": {group(100, 104), group(120, 125)},
		"2 vs 3": {group(100, 104), group(120, 125, 119)},
	} {
		c := CompareGroups(tc.a[0], tc.b[0], tc.a, tc.b)
		if c.Grouped {
			t.Errorf("%s: must not be grouped", name)
		}
		for _, d := range c.Deltas {
			if d.Spread != "insufficient repeats" {
				t.Errorf("%s: %s spread %q, want insufficient repeats", name, d.Key, d.Spread)
			}
		}
		var out bytes.Buffer
		_ = c.Write(&out)
		if !strings.Contains(out.String(), "insufficient repeats") || strings.Contains(out.String(), "ranges overlap") {
			t.Errorf("%s:\n%s", name, out.String())
		}
	}
}

func TestGroupedComparisonUsesMediansAndRangesAndNeverSaysSignificant(t *testing.T) {
	ga, gb := group(100, 104, 98), group(103, 99, 101)
	c := CompareGroups(ga[0], gb[0], ga, gb)
	d := find(t, c, "throughput")
	// medians 100 and 101; the delta is between medians, not between the two runs named (100, 103).
	if !c.Grouped || *d.A != 100 || *d.B != 101 || *d.Abs != 1 || *d.AMin != 98 || *d.AMax != 104 || *d.BMin != 99 || *d.BMax != 103 {
		t.Fatalf("%+v", d)
	}
	if d.Spread != "ranges overlap" {
		t.Errorf("98-104 and 99-103 overlap: %q", d.Spread)
	}
	gc := group(120, 125, 118)
	c = CompareGroups(ga[0], gc[0], ga, gc)
	if d := find(t, c, "throughput"); d.Spread != "ranges do not overlap" || *d.B != 120 {
		t.Errorf("%+v", d)
	}
	// identical values (latency is 100 in every run): nothing to judge, so n/a rather than a vacuous verdict.
	if d := find(t, c, "latency_p95"); d.Spread != "n/a" {
		t.Errorf("equal values: %q", d.Spread)
	}
	var out bytes.Buffer
	_ = c.Write(&out)
	s := out.String()
	if !strings.Contains(s, "medians of 3 and 3") || !strings.Contains(s, "[98-104]") || !strings.Contains(s, "not a significance test") || !strings.Contains(s, "10%") {
		t.Errorf("%s", s)
	}
	for _, banned := range []string{"significant at", "outside spread", "within spread"} {
		if strings.Contains(s, banned) {
			t.Errorf("must not imply significance: %q", banned)
		}
	}
}

func TestZeroVersusZeroIsNotAVerdict(t *testing.T) {
	ga, gb := group(0, 0, 0), group(0, 0, 0)
	if d := find(t, CompareGroups(ga[0], gb[0], ga, gb), "throughput"); d.Spread != "n/a" {
		t.Fatalf("%q", d.Spread)
	}
}

func TestInvalidOrErroringRunsAreWarnedAboutAboveTheTable(t *testing.T) {
	a := result("run_001", 100, 100, 10, 1, 0)
	b := result("run_002", 100, 100, 10, 1, 0.5)
	b.Valid, b.InvalidReasons = false, []string{"error rate 50.00% exceeds 5.00%"}
	c := result("run_003", 100, 100, 10, 1, 0.08)
	var out bytes.Buffer
	_ = Compare(a, b).Write(&out)
	s := out.String()
	if !strings.HasPrefix(s, "WARNING: run_002 is INVALID: error rate 50.00%") || strings.Index(s, "WARNING") > strings.Index(s, "METRIC") {
		t.Fatalf("the warning must come before the table:\n%s", s)
	}
	out.Reset()
	_ = Compare(a, c).Write(&out)
	if !strings.Contains(out.String(), "run_003 has an error rate of 8.00%") {
		t.Fatalf("%s", out.String())
	}
	if w := Compare(a, a).Warnings; len(w) != 0 {
		t.Fatalf("%v", w)
	}
}

func TestLoadGroupFindsRunsOfTheSameRepeatGroupInTheGivenDirectory(t *testing.T) {
	base := t.TempDir()
	var first Result
	for i := 1; i <= 3; i++ {
		id, dir, _ := CreateRun(base)
		r := Build(Input{RunID: id, Metadata: baseMeta(), Window: collect.Window{End: sec(1)}})
		r.Metadata.Repeat = Repeat{Index: i, Of: 3, Group: "g1"}
		if i == 3 {
			r.Metadata.Repeat.Group = "other"
		}
		if i == 1 {
			first = r
		}
		if err := Write(dir, r, nil); err != nil {
			t.Fatal(err)
		}
	}
	g, warns, err := LoadGroup(base, first)
	if err != nil || len(g) != 2 || len(warns) != 0 {
		t.Fatalf("%d %v %v", len(g), warns, err)
	}
	single := Build(Input{RunID: "run_x", Metadata: baseMeta()})
	if g, _, _ := LoadGroup(base, single); len(g) != 1 {
		t.Fatal("a run without repeats is its own group")
	}
	// The same group name in another directory is another group.
	other := t.TempDir()
	if g, _, _ := LoadGroup(other, first); len(g) != 1 {
		t.Fatalf("group names must not leak across directories: %d", len(g))
	}
}

func TestOneBadSiblingCannotBlockALoadGroup(t *testing.T) {
	base := t.TempDir()
	var first Result
	for i := 1; i <= 3; i++ {
		id, dir, _ := CreateRun(base)
		r := Build(Input{RunID: id, Metadata: baseMeta(), Window: collect.Window{End: sec(1)}})
		r.Metadata.Repeat = Repeat{Index: i, Of: 3, Group: "g"}
		if i == 1 {
			first = r
		}
		if err := Write(dir, r, nil); err != nil {
			t.Fatal(err)
		}
	}
	// A corrupt run, a run of another schema, a directory that is not a run, and an oversized file.
	for id, content := range map[string]string{"run_004": "{not json", "run_005": `{"schema_version": 99}`} {
		_ = os.Mkdir(filepath.Join(base, id), 0o755)
		_ = os.WriteFile(filepath.Join(base, id, ResultFile), []byte(content), 0o644)
	}
	_ = os.Mkdir(filepath.Join(base, "scratch"), 0o755)
	_ = os.Mkdir(filepath.Join(base, "run_006"), 0o755) // no result file at all
	g, warns, err := LoadGroup(base, first)
	if err != nil || len(g) != 3 {
		t.Fatalf("%d %v", len(g), err)
	}
	if len(warns) != 3 {
		t.Fatalf("one warning per unreadable run directory: %v", warns)
	}
	for _, w := range warns {
		if strings.Contains(w, "scratch") {
			t.Fatalf("non-run directories are not even looked at: %v", warns)
		}
	}
}
