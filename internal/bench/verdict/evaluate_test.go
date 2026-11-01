package verdict

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/report"
)

const hashW1 = "w1-hash"

type runSpec struct {
	id, strategy, workload string
	seed                   int64
	rep                    int
	p95                    float64
	hash                   string
	inputWeight            float64
	invalid                bool
	noExperiment           bool
	noLatency              bool
}

func mkRun(s runSpec) report.Result {
	if s.rep == 0 {
		s.rep = 1
	}
	if s.hash == "" {
		s.hash = hashW1
	}
	if s.inputWeight == 0 {
		s.inputWeight = 1
	}
	r := report.Result{SchemaVersion: 1, RunID: s.id, Valid: !s.invalid}
	if s.invalid {
		r.InvalidReasons = []string{"error rate 20%"}
	}
	r.Metadata = report.Metadata{Scheduler: s.strategy, Workload: s.workload, Seed: s.seed, Repeat: report.Repeat{Index: s.rep, Of: 5, Group: "g"}, Concurrency: 24}
	if !s.noExperiment {
		r.Metadata.Experiment = &report.Experiment{Strategy: s.strategy, World: "sim:fast", ParameterHash: s.hash,
			Options: &report.SchedulerOptions{LoadOverlay: "max", OutputFraction: 1, InputWeight: s.inputWeight, CapacityHalflifeMs: 60000, CapacityFloor: 0.25}}
	}
	if !s.noLatency {
		r.Summary.Latency = &collect.Dist{P95: s.p95}
	}
	return r
}

// pairs builds runs for seeds 1..len(effects): baseline 100 ms, treatment 100 * (1 - effect) ms.
func pairs(workload, a, b string, effects []float64) []report.Result {
	var rs []report.Result
	for i, e := range effects {
		seed := int64(i + 1)
		rs = append(rs,
			mkRun(runSpec{id: fmt.Sprintf("%s-%s-%d", a, workload, seed), strategy: a, workload: workload, seed: seed, p95: 100}),
			mkRun(runSpec{id: fmt.Sprintf("%s-%s-%d", b, workload, seed), strategy: b, workload: workload, seed: seed, p95: 100 * (1 - e)}))
	}
	return rs
}

func seedList(n int) string {
	var ss []string
	for i := 1; i <= n; i++ {
		ss = append(ss, fmt.Sprint(i))
	}
	return "[" + strings.Join(ss, ",") + "]"
}

func block(json string) string { return "```verdict\n" + json + "\n```\n" }

func sup(id, workload, b string, n int, extra string) string {
	return block(fmt.Sprintf(`{"id":%q,"kind":"superiority","metric":"latency_p95","a":{"scheduler":"least-active","workload":%q},"b":{"scheduler":%q,"workload":%q},"seeds":%s,"direction":"lower",%s}`,
		id, workload, b, workload, seedList(n), extra))
}

func freezeFor(pred string) *Freeze {
	return &Freeze{PredictionsSHA256: FrozenHash([]byte(pred)), FrozenAt: "2026-11-02T10:00:00Z"}
}

func eval(t *testing.T, pred string, runs []report.Result) Report {
	t.Helper()
	r, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: freezeFor(pred), Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func status(t *testing.T, r Report, id string) Verdict {
	t.Helper()
	for _, v := range r.Verdicts {
		if v.ID == id {
			return v
		}
	}
	t.Fatalf("no verdict %s", id)
	return Verdict{}
}

func reasons(v Verdict) string { return strings.Join(v.Reasons, " | ") }

var tightGood = []float64{0.14, 0.15, 0.16, 0.15, 0.17, 0.13, 0.15, 0.16, 0.14, 0.15}

const cmpFields = `"confirmatory":true,"confirm_min_effect":0.10,"refute_max_effect":0.05`

func TestSuperiorityConfirmedRefutedInconclusive(t *testing.T) {
	cases := []struct {
		name    string
		effects []float64
		want    Status
		reason  string
	}{
		{"confirmed", tightGood, Confirmed, "reaches the threshold"},
		{"refuted: the whole interval below the refutation threshold", []float64{0.01, 0.02, 0.015, 0.02, 0.01, 0.025, 0.02, 0.015}, Refuted, "below the refutation threshold"},
		{"inconclusive: interval includes zero", []float64{-0.2, 0.3, -0.1, 0.25, -0.3, 0.2, 0.1, -0.15}, Inconclusive, "includes zero"},
		{"inconclusive: excludes zero but between the thresholds", []float64{0.07, 0.075, 0.08, 0.07, 0.072, 0.078, 0.07, 0.074}, Inconclusive, "below the confirmation threshold"},
		{"a large effect in the wrong direction is not a confirmation", []float64{-0.3, -0.31, -0.29, -0.3, -0.32, -0.3, -0.3, -0.29}, Refuted, "below the refutation threshold"},
	}
	for _, c := range cases {
		pred := sup("H3", "heavy-tail", "least-tokens", len(c.effects), cmpFields)
		v := status(t, eval(t, pred, pairs("heavy-tail", "least-active", "least-tokens", c.effects)), "H3")
		if v.Status != c.want || !strings.Contains(reasons(v), c.reason) {
			t.Errorf("%s: %s (%s), want %s containing %q", c.name, v.Status, reasons(v), c.want, c.reason)
		}
		if v.Stats == nil || v.Stats.Pairs != len(c.effects) {
			t.Errorf("%s: stats %+v", c.name, v.Stats)
		}
	}
}

func TestDirectionHigherAndTheSignOfTheEffect(t *testing.T) {
	// Treatment 20% higher; the claim is "higher by at least 10%": confirmed. The same data with the claim
	// "lower" is refuted.
	up := []float64{-0.2, -0.21, -0.19, -0.2, -0.22, -0.2, -0.2, -0.19}
	runs := pairs("mixed", "least-active", "least-tokens", up)
	hi := block(fmt.Sprintf(`{"id":"H1","kind":"superiority","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":%s,"direction":"higher","confirm_min_effect":0.1,"refute_max_effect":0.05}`, seedList(8)))
	if v := status(t, eval(t, hi, runs), "H1"); v.Status != Confirmed || v.Stats.Effect < 0.19 || v.Stats.Effect > 0.21 {
		t.Fatalf("%+v %s", v.Stats, reasons(v))
	}
	lo := sup("H1", "mixed", "least-tokens", 8, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	if v := status(t, eval(t, lo, runs), "H1"); v.Status != Refuted || v.Stats.Effect > -0.19 {
		t.Fatalf("%+v %s", v.Stats, reasons(v))
	}
}

func TestThresholdBoundaries(t *testing.T) {
	// Every pair has exactly the same effect, so the median and the interval are that value.
	// Nearly the same effect in every pair (zero spread is degenerate and never confirmed): five at e and one
	// pair 0.00005 either way, so the median is e and the interval is within 0.00005 of it.
	same := func(e float64, n int) []float64 {
		out := make([]float64, n)
		for i := range out {
			out[i] = e
		}
		out[0], out[n-1] = e-0.00005, e+0.00005
		return out
	}
	run := func(e float64) Verdict {
		pred := sup("H1", "mixed", "least-tokens", 6, `"confirm_min_effect":0.10,"refute_max_effect":0.05`)
		return status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", same(e, 6))), "H1")
	}
	if v := run(0.10); v.Status != Confirmed {
		t.Errorf("effect exactly at the confirmation threshold is confirmed: %s", v.Status)
	}
	if v := run(0.0999); v.Status != Inconclusive {
		t.Errorf("just below: %s", v.Status)
	}
	if v := run(0.0499); v.Status != Refuted {
		t.Errorf("an interval entirely below the refutation threshold: %s %s", v.Status, reasons(v))
	}
	if v := run(0.05); v.Status != Inconclusive {
		t.Errorf("the whole interval AT the refutation threshold is not below it: %s", v.Status)
	}
}

func TestHolmDowngradesBorderlineConfirmatoryClaims(t *testing.T) {
	// 14 pairs: 3 with a big loss, 11 with a ~15% gain. The raw bootstrap p-value is about 0.036: below 0.05,
	// so alone the claim is confirmed. Two such claims in one family have Holm-adjusted values 2 * 0.036 =
	// 0.072, so neither survives. A strong claim next to a borderline one does not hurt it: the step-down
	// multiplier of the larger p-value is 1.
	effects := []float64{-0.5, -0.5, -0.5, 0.12, 0.13, 0.14, 0.15, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, 0.22}
	good := make([]float64, 14)
	for i := range good {
		good[i] = 0.15 + float64(i)/1000
	}
	runs := append(append(pairs("heavy-tail", "least-active", "least-tokens", effects), pairs("mixed", "least-active", "least-work", effects)...),
		pairs("burst", "least-active", "least-work", good)...)
	h3 := sup("H3", "heavy-tail", "least-tokens", 14, cmpFields)
	h4 := sup("H4", "mixed", "least-work", 14, cmpFields)
	h5 := sup("H5", "burst", "least-work", 14, cmpFields)
	v := status(t, eval(t, h3, runs), "H3")
	if v.Stats.P < 0.025 || v.Stats.P >= 0.05 {
		t.Fatalf("the fixture must have a p-value in [0.025, 0.05), got %v", v.Stats.P)
	}
	if v.Status != Confirmed || v.Stats.Adjusted == nil || *v.Stats.Adjusted != v.Stats.P || v.Stats.FamilySize != 1 {
		t.Fatalf("a family of one is the plain test: %s %+v", v.Status, v.Stats)
	}
	r := eval(t, h3+h4, runs)
	for _, id := range []string{"H3", "H4"} {
		got := status(t, r, id)
		if got.Status != Inconclusive || !strings.Contains(reasons(got), "Holm-adjusted") || *got.Stats.Adjusted < 0.07 || got.Stats.FamilySize != 2 {
			t.Fatalf("%s must not survive Holm: %s %s %+v", id, got.Status, reasons(got), got.Stats)
		}
	}
	if !reflect.DeepEqual(r.HolmFamily, []string{"H3", "H4"}) {
		t.Fatalf("%v", r.HolmFamily)
	}
	r = eval(t, h3+h5, runs)
	if a, b := status(t, r, "H3"), status(t, r, "H5"); a.Status != Confirmed || b.Status != Confirmed || *a.Stats.Adjusted != a.Stats.P {
		t.Fatalf("%s %s %+v", a.Status, b.Status, a.Stats)
	}
	// An exploratory claim (not confirmatory) is not corrected and not in the family.
	noC := func(s string) string { return strings.Replace(s, `"confirmatory":true,`, "", 1) }
	r = eval(t, noC(h3)+noC(h4), runs)
	if got := status(t, r, "H3"); got.Status != Confirmed || got.Stats.Adjusted != nil || len(r.HolmFamily) != 0 {
		t.Fatalf("%s %+v", got.Status, got.Stats)
	}
}

func TestAMissingConfirmatoryClaimCountsAgainstTheFamily(t *testing.T) {
	effects := []float64{-0.5, -0.5, -0.5, 0.12, 0.13, 0.14, 0.15, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, 0.22}
	runs := pairs("heavy-tail", "least-active", "least-tokens", effects)
	pred := sup("H3", "heavy-tail", "least-tokens", 14, cmpFields) + sup("H4", "mixed", "least-work", 14, cmpFields) // H4 has no runs
	r := eval(t, pred, runs)
	h4 := status(t, r, "H4")
	if h4.Status != Invalid || !strings.Contains(reasons(h4), "matches no run") {
		t.Fatalf("%s %s", h4.Status, reasons(h4))
	}
	if h3 := status(t, r, "H3"); h3.Status != Inconclusive || h3.Stats.FamilySize != 2 {
		t.Fatalf("missing data must not make the other claim easier: %s %+v", h3.Status, h3.Stats)
	}
}

func TestInvalidInputsAreInvalidNotPassed(t *testing.T) {
	good := pairs("mixed", "least-active", "least-tokens", tightGood)
	pred := sup("H1", "mixed", "least-tokens", 10, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	cut := func(f func(rs []report.Result) []report.Result) []report.Result {
		cp := make([]report.Result, len(good))
		for i, r := range good {
			e := *r.Metadata.Experiment // do not share the record between cases
			r.Metadata.Experiment = &e
			if r.Summary.Latency != nil {
				l := *r.Summary.Latency
				r.Summary.Latency = &l
			}
			cp[i] = r
		}
		return f(cp)
	}
	cases := []struct {
		name string
		runs []report.Result
		want string
	}{
		{"no treatment group", cut(func(rs []report.Result) []report.Result {
			var o []report.Result
			for _, r := range rs {
				if r.Metadata.Scheduler == "least-active" {
					o = append(o, r)
				}
			}
			return o
		}), "arm b matches no run"},
		{"one pair missing its treatment", good[:len(good)-1], "has a baseline run but no treatment run"},
		{"a registered seed has no runs at all", pairs("mixed", "least-active", "least-tokens", tightGood[:9]), "no runs for registered seed 10"},
		{"too few pairs", pairs("mixed", "least-active", "least-tokens", tightGood[:4]), "no runs for registered seed 5"},
		{"an invalid run", cut(func(rs []report.Result) []report.Result {
			rs[3].Valid = false
			rs[3].InvalidReasons = []string{"too many errors"}
			return rs
		}), "is INVALID (too many errors)"},
		{"no experiment record", cut(func(rs []report.Result) []report.Result { rs[0].Metadata.Experiment = nil; return rs }), "no experiment record"},
		{"worlds differ between arms", cut(func(rs []report.Result) []report.Result { rs[1].Metadata.Experiment.ParameterHash = "other"; return rs }), "different parameter hashes"},
		{"a duplicate seed and repeat", append(append([]report.Result(nil), good...), mkRun(runSpec{id: "dup", strategy: "least-active", workload: "mixed", seed: 1, p95: 90})), "pairing is ambiguous"},
		{"metric not measured", cut(func(rs []report.Result) []report.Result { rs[2].Summary.Latency = nil; return rs }), "latency_p95 is not measured in seed 2"},
		{"a zero baseline", cut(func(rs []report.Result) []report.Result { rs[4].Summary.Latency.P95 = 0; return rs }), "not positive"},
	}
	for _, c := range cases {
		v := status(t, eval(t, pred, c.runs), "H1")
		if v.Status != Invalid || !strings.Contains(reasons(v), c.want) {
			t.Errorf("%s: %s (%s), want INVALID containing %q", c.name, v.Status, reasons(v), c.want)
		}
		if v.Stats != nil {
			t.Errorf("%s: an invalid verdict carries no statistics", c.name)
		}
	}
	// Runs of other seeds or other workloads are not an arm.
	other := pairs("burst", "least-active", "least-tokens", tightGood)
	if v := status(t, eval(t, pred, other), "H1"); v.Status != Invalid {
		t.Errorf("%s", v.Status)
	}
}

func TestRepeatsOfOneSeedPairByRepeatIndex(t *testing.T) {
	var runs []report.Result
	for rep := 1; rep <= 7; rep++ {
		runs = append(runs,
			mkRun(runSpec{id: fmt.Sprintf("a%d", rep), strategy: "least-active", workload: "mixed", seed: 7, rep: rep, p95: 100}),
			mkRun(runSpec{id: fmt.Sprintf("b%d", rep), strategy: "least-tokens", workload: "mixed", seed: 7, rep: rep, p95: 80 + float64(rep)/10}))
	}
	pred := block(`{"id":"H1","kind":"superiority","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":[7],"direction":"lower","confirm_min_effect":0.1,"refute_max_effect":0.05}`)
	v := status(t, eval(t, pred, runs), "H1")
	if v.Status != Confirmed || v.Stats.Pairs != 7 {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	if len(v.Warnings) != 1 || !strings.Contains(v.Warnings[0], "only 1 distinct seed") {
		t.Fatalf("repeats of one seed are not independent samples: %v", v.Warnings)
	}
}

func TestSelectorsPickTheOptionsOfAnArm(t *testing.T) {
	var runs []report.Result
	for seed := int64(1); seed <= 6; seed++ {
		runs = append(runs,
			mkRun(runSpec{id: fmt.Sprintf("a%d", seed), strategy: "least-work", workload: "prefill-heavy", seed: seed, p95: 100, inputWeight: 1}),
			mkRun(runSpec{id: fmt.Sprintf("b%d", seed), strategy: "least-work", workload: "prefill-heavy", seed: seed, p95: 90 + float64(seed)/100, inputWeight: 0.3}),
			mkRun(runSpec{id: fmt.Sprintf("c%d", seed), strategy: "least-work", workload: "prefill-heavy", seed: seed, p95: 50, inputWeight: 0.7}))
	}
	pred := block(`{"id":"H5b","kind":"superiority","metric":"latency_p95","a":{"scheduler":"least-work","workload":"prefill-heavy","input_weight":1},"b":{"scheduler":"least-work","workload":"prefill-heavy","input_weight_tuned":"input_weight"},"seeds":[1,2,3,4,5,6],"direction":"lower","confirm_min_effect":0.05,"refute_max_effect":0.03}`)
	tuned := []byte(`{"tuned_at":"2026-11-04T10:00:00Z","values":{"input_weight":0.3}}`)
	f := freezeFor(pred)
	f.TunedValuesSHA256, f.TunedValuesFrozenAt = shaHex(tuned), "2026-11-05T10:00:00Z"
	r, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: f, Tuned: tuned, Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	if v := status(t, r, "H5b"); v.Status != Confirmed || v.Stats.Effect < 0.0990 || v.Stats.Effect > 0.1001 {
		t.Fatalf("the 0.3 arm (90 ms) against 1.0 (100 ms) is a 10%% effect: %s %+v %s", v.Status, v.Stats, reasons(v))
	}
	// Without the tuned file the arm cannot be selected.
	r, _ = Evaluate(Inputs{Predictions: []byte(pred), Freeze: freezeFor(pred), Runs: runs})
	if v := status(t, r, "H5b"); v.Status != Invalid || !strings.Contains(reasons(v), `needs the tuned value "input_weight"`) {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
}

func shaHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func TestEquivalence(t *testing.T) {
	eq := func(extra string) string {
		return block(fmt.Sprintf(`{"id":"H2","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"least-active","workload":"uniform-short"},"b":{"scheduler":"least-tokens","workload":"uniform-short"},"seeds":%s,%s}`, seedList(8), extra))
	}
	run := func(effects []float64, extra string) Verdict {
		return status(t, eval(t, eq(extra), pairs("uniform-short", "least-active", "least-tokens", effects)), "H2")
	}
	inside := []float64{0.01, -0.01, 0.02, 0, -0.02, 0.01, -0.005, 0.005}
	if v := run(inside, `"margin":0.05`); v.Status != Confirmed || v.Stats.TOST == nil || !v.Stats.TOST.Equivalent {
		t.Fatalf("%s %s %+v", v.Status, reasons(v), v.Stats)
	}
	// An interval that merely includes zero is not equivalence: it straddles the margin.
	wide := []float64{0.2, -0.2, 0.15, -0.15, 0.25, -0.1, 0.1, 0}
	if v := run(wide, `"margin":0.05`); v.Status != Inconclusive {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// Entirely outside the margin on either side: refuted.
	for _, e := range []float64{0.2, -0.2} {
		out := make([]float64, 8)
		for i := range out {
			out[i] = e + float64(i)/1000
		}
		if v := run(out, `"margin":0.05`); v.Status != Refuted {
			t.Fatalf("effect %v: %s %s", e, v.Status, reasons(v))
		}
	}
	// A difference outside the invalid margin where none was predicted makes the harness suspect.
	out := []float64{0.2, 0.21, 0.2, 0.19, 0.2, 0.2, 0.21, 0.19}
	if v := run(out, `"margin":0.03,"refute_margin":0.05,"invalid_margin":0.05`); v.Status != Invalid || !strings.Contains(reasons(v), "harness is suspect") {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// Between the confirmation margin and a wider refutation margin: inconclusive.
	mid := []float64{0.04, 0.041, 0.04, 0.039, 0.04, 0.04, 0.041, 0.039}
	if v := run(mid, `"margin":0.03,"refute_margin":0.05`); v.Status != Inconclusive {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// Equivalence is symmetric: negating every difference gives the same verdict class and mirrored interval.
	neg := make([]float64, len(inside))
	for i, x := range inside {
		neg[i] = -x
	}
	a, b := run(inside, `"margin":0.05`), run(neg, `"margin":0.05`)
	if a.Status != b.Status || math.Abs(a.Stats.Effect+b.Stats.Effect) > 1e-12 || math.Abs(a.Stats.Lo+b.Stats.Hi) > 1e-12 {
		t.Fatalf("%+v %+v", a.Stats, b.Stats)
	}
}

func TestTunedRangeAndAllOf(t *testing.T) {
	pred := block(`{"id":"H5a","kind":"tuned_range","value_key":"input_weight","min":0.1,"max":0.5}`) +
		block(`{"id":"H5c","kind":"tuned_range","value_key":"input_weight","min":0.6,"max":0.9}`) +
		block(`{"id":"H5d","kind":"tuned_range","value_key":"missing","min":0,"max":1}`) +
		block(`{"id":"ALL","kind":"all_of","parts":["H5a"]}`) +
		block(`{"id":"ANY","kind":"all_of","parts":["H5a","H5c"]}`) +
		block(`{"id":"BAD","kind":"all_of","parts":["H5a","H5d"]}`) +
		block(`{"id":"BADR","kind":"all_of","parts":["H5d","H5d2"]}`)
	if _, err := ParsePredictions([]byte(pred)); err == nil || !strings.Contains(err.Error(), "H5d2") {
		t.Fatalf("a part that does not exist: %v", err)
	}
	pred = strings.Replace(pred, block(`{"id":"BADR","kind":"all_of","parts":["H5d","H5d2"]}`), "", 1)
	tuned := []byte(`{"tuned_at":"2026-11-04T10:00:00Z","values":{"input_weight":0.3}}`)
	f := freezeFor(pred)
	f.TunedValuesSHA256, f.TunedValuesFrozenAt = shaHex(tuned), "2026-11-05T10:00:00Z"
	r, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: f, Tuned: tuned})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Status{"H5a": Confirmed, "H5c": Refuted, "H5d": Invalid, "ALL": Confirmed, "ANY": Refuted, "BAD": Invalid}
	for id, w := range want {
		if got := status(t, r, id).Status; got != w {
			t.Errorf("%s = %s, want %s (%s)", id, got, w, reasons(status(t, r, id)))
		}
	}
	// The boundaries of the range are inside it.
	for _, v := range []string{"0.1", "0.5"} {
		tt := []byte(`{"tuned_at":"2026-11-04T10:00:00Z","values":{"input_weight":` + v + `}}`)
		ff := freezeFor(pred)
		ff.TunedValuesSHA256, ff.TunedValuesFrozenAt = shaHex(tt), "2026-11-05T10:00:00Z"
		rr, _ := Evaluate(Inputs{Predictions: []byte(pred), Freeze: ff, Tuned: tt})
		if status(t, rr, "H5a").Status != Confirmed {
			t.Errorf("tuned %s is inside [0.1, 0.5]", v)
		}
	}
	// With no tuned file the range hypotheses are invalid, and all_of follows.
	r, _ = Evaluate(Inputs{Predictions: []byte(pred), Freeze: freezeFor(pred)})
	if status(t, r, "H5a").Status != Invalid || status(t, r, "ALL").Status != Invalid {
		t.Error("no tuned values, no verdict")
	}
	// all_of: confirmed needs every part; a part inconclusive keeps it inconclusive; a refuted part refutes it.
	incl := []float64{-0.2, 0.3, -0.1, 0.25, -0.3, 0.2, 0.1, -0.15}
	runs := append(pairs("mixed", "least-active", "least-tokens", tightGood[:8]), pairs("burst", "least-active", "least-tokens", incl)...)
	p2 := sup("P1", "mixed", "least-tokens", 8, `"confirm_min_effect":0.1,"refute_max_effect":0.05`) +
		sup("P2", "burst", "least-tokens", 8, `"confirm_min_effect":0.1,"refute_max_effect":0.05`) +
		block(`{"id":"BOTH","kind":"all_of","parts":["P1","P2"]}`) + block(`{"id":"ONE","kind":"all_of","parts":["P1"]}`)
	rr := eval(t, p2, runs)
	if status(t, rr, "BOTH").Status != Inconclusive || status(t, rr, "ONE").Status != Confirmed {
		t.Errorf("%s %s", status(t, rr, "BOTH").Status, status(t, rr, "ONE").Status)
	}
}

func TestFreezeRules(t *testing.T) {
	pred := sup("H3", "heavy-tail", "least-tokens", 10, cmpFields)
	runs := pairs("heavy-tail", "least-active", "least-tokens", tightGood)
	// Matching freeze: confirmed, noted.
	r := eval(t, pred, runs)
	if !r.Frozen || r.Count(Confirmed) != 1 || !strings.Contains(strings.Join(r.Notes, "\n"), "match the freeze") {
		t.Fatalf("%+v", r)
	}
	// Edited after the freeze (a threshold changed): every verdict is invalid, nothing is computed.
	edited := strings.Replace(pred, "0.10", "0.05", 1)
	r, err := Evaluate(Inputs{Predictions: []byte(edited), Freeze: freezeFor(pred), Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	v := status(t, r, "H3")
	if r.Frozen || v.Status != Invalid || v.Stats != nil || !strings.Contains(reasons(v), "edited after the freeze") || !strings.Contains(strings.Join(r.Notes, "\n"), "CHANGED AFTER THE FREEZE") {
		t.Fatalf("%+v %s", r, reasons(v))
	}
	// Amendments after the heading are not part of the frozen text: appending them keeps the freeze valid and
	// never changes a verdict, even a verdict block inside them is ignored.
	// The file is frozen with its (empty) amendments heading in place, as the documentation says.
	base := pred + "\n" + AmendmentsHeading + "\n\nNone yet.\n"
	amended := base + "\n2026-12-01: the threshold of H3 is changed to 0.9.\n" + sup("H3", "heavy-tail", "least-tokens", 10, `"confirm_min_effect":0.9,"refute_max_effect":0.05`)
	r, err = Evaluate(Inputs{Predictions: []byte(amended), Freeze: freezeFor(base), Runs: runs})
	if err != nil || !r.Frozen || status(t, r, "H3").Status != Confirmed {
		t.Fatalf("%v %+v", err, r)
	}
	if FrozenHash([]byte(amended)) != FrozenHash([]byte(base)) {
		t.Fatal("amendments must not change the hash")
	}
	// No freeze record: refused, unless allowed, and then marked.
	if _, err := Evaluate(Inputs{Predictions: []byte(pred), Runs: runs}); err == nil {
		t.Fatal("verdicts need a freeze")
	}
	r, err = Evaluate(Inputs{Predictions: []byte(pred), Runs: runs, AllowUnfrozen: true})
	if err != nil || r.Frozen || !strings.Contains(strings.Join(r.Notes, "\n"), "NO FREEZE RECORD") {
		t.Fatalf("%v %+v", err, r)
	}
	if _, err := Evaluate(Inputs{Predictions: []byte(pred), AllowUnfrozen: true, Tuned: []byte("{}")}); err == nil {
		t.Fatal("tuned values need a freeze")
	}
	if _, err := Evaluate(Inputs{Predictions: []byte("no blocks"), Freeze: freezeFor("no blocks")}); err == nil {
		t.Fatal("unreadable predictions are an error")
	}
}

func TestFrozenTextBoundary(t *testing.T) {
	for _, c := range []struct{ in, frozen string }{
		{"a\nb\n", "a\nb\n"},
		{"a\n## Amendments\nx\n", "a\n"},
		{"a\n## Amendments", "a\n"},
		{"## Amendments\nx", ""},
		{"a\n## Amendments extra\nx", "a\n## Amendments extra\nx"},
		{"a\n### Amendments\nx", "a\n### Amendments\nx"},
		{"a\n## Amendments\n## Amendments\n", "a\n"},
	} {
		if got := string(FrozenText([]byte(c.in))); got != c.frozen {
			t.Errorf("FrozenText(%q) = %q, want %q", c.in, got, c.frozen)
		}
	}
}

func TestTunedValuesAreRefusedWhenNewerThanTheFreeze(t *testing.T) {
	pred := sup("H3", "heavy-tail", "least-tokens", 10, cmpFields)
	tunedJSON := func(at string) []byte {
		return []byte(`{"tuned_at":"` + at + `","values":{"input_weight":0.3}}`)
	}
	frozen := func(tuned []byte, at string) *Freeze {
		f := freezeFor(pred)
		f.TunedValuesSHA256, f.TunedValuesFrozenAt = shaHex(tuned), at
		return f
	}
	ok := tunedJSON("2026-11-04T10:00:00Z")
	for _, c := range []struct {
		name   string
		tuned  []byte
		freeze *Freeze
		want   string
	}{
		{"tuned after the tuned-values freeze", tunedJSON("2026-11-06T10:00:00Z"), frozen(tunedJSON("2026-11-06T10:00:00Z"), "2026-11-05T10:00:00Z"), "later than the tuned-values freeze"},
		{"tuned before the predictions freeze", tunedJSON("2026-11-01T10:00:00Z"), frozen(tunedJSON("2026-11-01T10:00:00Z"), "2026-11-05T10:00:00Z"), "before the predictions freeze"},
		{"file changed after it was frozen", tunedJSON("2026-11-04T11:00:00Z"), frozen(ok, "2026-11-05T10:00:00Z"), "hashes to"},
		{"freeze records no tuned values", ok, freezeFor(pred), "no tuned-values freeze"},
		{"not JSON", []byte("0.3"), frozen(ok, "2026-11-05T10:00:00Z"), "tuned values"},
		{"unknown field", []byte(`{"tuned_at":"2026-11-04T10:00:00Z","values":{},"extra":1}`), frozen(ok, "2026-11-05T10:00:00Z"), "tuned values"},
		{"bad time", []byte(`{"tuned_at":"yesterday","values":{}}`), frozen([]byte(`{"tuned_at":"yesterday","values":{}}`), "2026-11-05T10:00:00Z"), "RFC 3339"},
	} {
		_, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: c.freeze, Tuned: c.tuned})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want error containing %q", c.name, err, c.want)
		}
	}
	// Exactly at the freeze instant is allowed.
	at := tunedJSON("2026-11-05T10:00:00Z")
	if _, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: frozen(at, "2026-11-05T10:00:00Z"), Tuned: at}); err != nil {
		t.Errorf("tuned_at equal to the freeze instant: %v", err)
	}
	if _, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: frozen(ok, "2026-11-05T10:00:00Z"), Tuned: ok}); err != nil {
		t.Errorf("a proper tuned file: %v", err)
	}
}

func TestParseFreeze(t *testing.T) {
	h := strings.Repeat("a", 64)
	for _, c := range []struct{ in, want string }{
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z"}`, ""},
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z","tuned_values_sha256":"` + h + `","tuned_values_frozen_at":"2026-11-03T10:00:00Z"}`, ""},
		{`{"predictions_sha256":"short","frozen_at":"2026-11-02T10:00:00Z"}`, "64 lowercase hex"},
		{`{"predictions_sha256":"` + strings.ToUpper(h) + `","frozen_at":"2026-11-02T10:00:00Z"}`, "64 lowercase hex"},
		{`{"predictions_sha256":"` + h + `","frozen_at":"nope"}`, "RFC 3339"},
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z","tuned_values_sha256":"` + h + `"}`, "go together"},
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z","tuned_values_sha256":"x","tuned_values_frozen_at":"2026-11-03T10:00:00Z"}`, "64 lowercase hex"},
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z","tuned_values_sha256":"` + h + `","tuned_values_frozen_at":"2026-11-01T10:00:00Z"}`, "before frozen_at"},
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z","other":1}`, "unknown field"},
		{`{"predictions_sha256":"` + h + `","frozen_at":"2026-11-02T10:00:00Z"} {}`, "more than one"},
		{`not json`, "freeze record"},
	} {
		_, err := ParseFreeze([]byte(c.in))
		if (c.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.in, err, c.want)
		}
	}
}

func TestEvaluationIsPureAndNeverEditsAPrediction(t *testing.T) {
	pred := sup("H3", "heavy-tail", "least-tokens", 10, cmpFields) + sup("H4", "mixed", "least-work", 10, cmpFields)
	runs := append(pairs("heavy-tail", "least-active", "least-tokens", tightGood), pairs("mixed", "least-active", "least-work", tightGood)...)
	before := []byte(pred)
	snapshot := append([]byte(nil), before...)
	f := freezeFor(pred)
	a, err := Evaluate(Inputs{Predictions: before, Freeze: f, Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Evaluate(Inputs{Predictions: before, Freeze: f, Runs: runs})
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same inputs, different reports")
	}
	var oa, ob bytes.Buffer
	_ = a.Write(&oa)
	_ = b.Write(&ob)
	if oa.String() != ob.String() || !bytes.Equal(before, snapshot) {
		t.Fatal("output must be a pure function of the inputs, and the predictions untouched")
	}
	// The order of the runs does not matter.
	rev := make([]report.Result, len(runs))
	for i := range runs {
		rev[len(runs)-1-i] = runs[i]
	}
	c, _ := Evaluate(Inputs{Predictions: before, Freeze: f, Runs: rev})
	if !reflect.DeepEqual(a.Verdicts, c.Verdicts) {
		t.Fatal("run order changed a verdict")
	}
	if !strings.Contains(oa.String(), Banner) || !strings.Contains(oa.String(), "not hardware-validated") {
		t.Fatal("the output carries the scope label")
	}
}

func TestPredictionParsing(t *testing.T) {
	good := `{"id":"H3","kind":"superiority","metric":"latency_p95","a":{"scheduler":"x","workload":"mixed"},"b":{"scheduler":"y","workload":"mixed"},"seeds":[1],"direction":"lower","confirm_min_effect":0.1,"refute_max_effect":0.05}`
	mut := func(from, to string) string { return strings.Replace(good, from, to, 1) }
	for _, c := range []struct{ name, in, want string }{
		{"ok", block(good), ""},
		{"prose and other fences are ignored", "# Title\n\n```go\nx := 1\n```\n\n| table |\n" + block(good), ""},
		{"crlf fence", "```verdict\r\n" + good + "\r\n```\r\n", ""},
		{"no blocks", "just prose", "no ```verdict block"},
		{"unclosed", "```verdict\n" + good, "never closed"},
		{"unknown field", block(mut(`"seeds"`, `"sedes"`)), "unknown field"},
		{"two values", block(good + "\n" + good), "more than one"},
		{"not json", block("{"), "not valid"},
		{"duplicate id", block(good) + block(good), "appears twice"},
		{"bad id", block(mut(`"H3"`, `"3 H"`)), "id must match"},
		{"unknown kind", block(mut(`"superiority"`, `"better"`)), "unknown kind"},
		{"unknown metric", block(mut("latency_p95", "vibes")), "unknown metric"},
		{"no seeds", block(mut(`"seeds":[1]`, `"seeds":[]`)), "seeds must list"},
		{"duplicate seed", block(mut(`"seeds":[1]`, `"seeds":[1,1]`)), "listed twice"},
		{"bad direction", block(mut("lower", "sideways")), "direction"},
		{"threshold zero", block(mut(`"confirm_min_effect":0.1`, `"confirm_min_effect":0`)), "confirm_min_effect"},
		{"refute above confirm", block(mut(`"refute_max_effect":0.05`, `"refute_max_effect":0.5`)), "refute_max_effect"},
		{"percent given as a fraction over 10", block(mut(`"confirm_min_effect":0.1`, `"confirm_min_effect":11`)), "confirm_min_effect"},
		{"margin on a superiority", block(mut(`"seeds":[1]`, `"seeds":[1],"margin":0.1`)), "margin does not apply"},
		{"missing b", block(mut(`"b":{"scheduler":"y","workload":"mixed"},`, "")), "a and b are required"},
		{"selector overlay", block(mut(`"scheduler":"x"`, `"scheduler":"x","load_overlay":"both"`)), "load_overlay"},
		{"selector weight both", block(mut(`"scheduler":"x"`, `"scheduler":"x","input_weight":1,"input_weight_tuned":"k"`)), "exclusive"},
		{"selector weight range", block(mut(`"scheduler":"x"`, `"scheduler":"x","input_weight":101`)), "input_weight"},
		{"selector output fraction", block(mut(`"scheduler":"x"`, `"scheduler":"x","output_fraction":0`)), "output_fraction"},
		{"resamples low", block(mut(`"seeds":[1]`, `"seeds":[1],"resamples":10`)), "resamples"},
		{"equivalence margin zero", block(`{"id":"E","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"x","workload":"w"},"b":{"scheduler":"y","workload":"w"},"seeds":[1],"margin":0}`), "margin must be"},
		{"equivalence refute below margin", block(`{"id":"E","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"x","workload":"w"},"b":{"scheduler":"y","workload":"w"},"seeds":[1],"margin":0.05,"refute_margin":0.03}`), "refute_margin"},
		{"tuned range inverted", block(`{"id":"T","kind":"tuned_range","value_key":"k","min":2,"max":1}`), "min < max"},
		{"tuned range of one point", block(`{"id":"T","kind":"tuned_range","value_key":"k","min":1,"max":1}`), "not a range"},
		{"all_of nested", block(`{"id":"T","kind":"tuned_range","value_key":"k","min":0,"max":1}`) + block(`{"id":"A","kind":"all_of","parts":["T"]}`) + block(`{"id":"B","kind":"all_of","parts":["A"]}`), "itself an all_of"},
		{"all_of self", block(`{"id":"A","kind":"all_of","parts":["A"]}`), "itself"},
	} {
		_, err := ParsePredictions([]byte(c.in))
		if (c.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
}

func TestOnlyTheVerdictToolReadsNoClock(t *testing.T) {
	// The package is pure: no wall clock, no environment, no randomness except the seeded stream.
	for _, f := range []string{"stats.go", "evaluate.go", "predictions.go", "freeze.go", "format.go", "metrics.go"} {
		b, err := readFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"time.Now", "time.Since", "time.Until", "os.Getenv", "rand.Seed", "math/rand\"", "crypto/rand"} {
			if strings.Contains(b, bad) {
				t.Errorf("%s uses %s", f, bad)
			}
		}
	}
}

func TestFewerThanMinPairsIsInvalid(t *testing.T) {
	pred := sup("H1", "mixed", "least-tokens", MinPairs-1, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	v := status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", tightGood[:MinPairs-1])), "H1")
	if v.Status != Invalid || !strings.Contains(reasons(v), "fewer than the minimum of 5") {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// Exactly the minimum is enough to compute, but not to confirm: with 5 non-zero pairs an exact sign test
	// cannot give a p-value below 0.0625.
	pred = sup("H1", "mixed", "least-tokens", MinPairs, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	v = status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", tightGood[:MinPairs])), "H1")
	if v.Status != Inconclusive || !strings.Contains(reasons(v), "too few pairs to confirm") || v.Stats.P != 0.0625 {
		t.Fatalf("%s %s %+v", v.Status, reasons(v), v.Stats)
	}
	pred = sup("H1", "mixed", "least-tokens", 6, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	if v := status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", tightGood[:6])), "H1"); v.Status != Confirmed || v.Stats.P != 0.03125 {
		t.Fatalf("six non-zero pairs can confirm: %s %s", v.Status, reasons(v))
	}
}

func TestBootstrapSeedAndResamplesCanBePreRegistered(t *testing.T) {
	runs := pairs("mixed", "least-active", "least-tokens", tightGood)
	base := `"confirm_min_effect":0.1,"refute_max_effect":0.05`
	a := status(t, eval(t, sup("H1", "mixed", "least-tokens", 10, base), runs), "H1")
	b := status(t, eval(t, sup("H1", "mixed", "least-tokens", 10, base+`,"bootstrap_seed":5,"resamples":2000`), runs), "H1")
	if a.Stats.BootstrapSed != DefaultBootstrapSeed || a.Stats.Resamples != DefaultResamples || b.Stats.BootstrapSed != 5 || b.Stats.Resamples != 2000 {
		t.Fatalf("%+v %+v", a.Stats, b.Stats)
	}
}

func TestSignedNeverShowsNegativeZero(t *testing.T) {
	for x, want := range map[float64]string{0: "+0.00%", -1e-9: "+0.00%", 0.1234: "+12.34%", -0.1234: "-12.34%", -0.0005: "-0.05%"} {
		if got := signed(x); got != want {
			t.Errorf("signed(%v) = %s, want %s", x, got, want)
		}
	}
}

func TestRunsOfUnregisteredSeedsAreIgnored(t *testing.T) {
	runs := pairs("mixed", "least-active", "least-tokens", tightGood)
	// A stray, unpaired baseline run of a seed nobody registered must neither invalidate nor change the verdict.
	runs = append(runs, mkRun(runSpec{id: "stray", strategy: "least-active", workload: "mixed", seed: 99, p95: 1}))
	pred := sup("H1", "mixed", "least-tokens", 10, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	if v := status(t, eval(t, pred, runs), "H1"); v.Status != Confirmed || v.Stats.Pairs != 10 {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
}

func TestResultsWithoutARepeatIndexPairWithRepeatOne(t *testing.T) {
	var runs []report.Result
	for seed := int64(1); seed <= 6; seed++ {
		a := mkRun(runSpec{id: fmt.Sprintf("a%d", seed), strategy: "least-active", workload: "mixed", seed: seed, p95: 100})
		a.Metadata.Repeat.Index = 0 // a result that never recorded one
		b := mkRun(runSpec{id: fmt.Sprintf("b%d", seed), strategy: "least-tokens", workload: "mixed", seed: seed, rep: 1, p95: 80 + float64(seed)/10})
		runs = append(runs, a, b)
	}
	pred := sup("H1", "mixed", "least-tokens", 6, `"confirm_min_effect":0.1,"refute_max_effect":0.05`)
	if v := status(t, eval(t, pred, runs), "H1"); v.Status != Confirmed {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
}

func TestInvalidMarginIsSeparateFromTheRefuteMargin(t *testing.T) {
	pred := block(fmt.Sprintf(`{"id":"H11","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"least-active","workload":"uniform-short"},"b":{"scheduler":"least-tokens","workload":"uniform-short"},"seeds":%s,"margin":0.03,"refute_margin":0.05,"invalid_margin":0.07}`, seedList(8)))
	// 6% apart: outside the 5% refute margin (refuted) but not outside the 7% invalid margin.
	six := []float64{0.06, 0.061, 0.06, 0.059, 0.06, 0.06, 0.061, 0.059}
	if v := status(t, eval(t, pred, pairs("uniform-short", "least-active", "least-tokens", six)), "H11"); v.Status != Refuted {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// 4% apart: outside the confirmation margin, inside the refute margin: inconclusive, not invalid.
	four := []float64{0.04, 0.041, 0.04, 0.039, 0.04, 0.04, 0.041, 0.039}
	if v := status(t, eval(t, pred, pairs("uniform-short", "least-active", "least-tokens", four)), "H11"); v.Status != Inconclusive {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// 8% apart: outside the invalid margin: the harness is suspect.
	eight := []float64{0.08, 0.081, 0.08, 0.079, 0.08, 0.08, 0.081, 0.079}
	if v := status(t, eval(t, pred, pairs("uniform-short", "least-active", "least-tokens", eight)), "H11"); v.Status != Invalid {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// 4% with only the invalid margin at 3%: the same data is now invalid, so the margin used is the invalid one.
	tight := strings.Replace(pred, `"invalid_margin":0.07`, `"invalid_margin":0.03`, 1)
	if v := status(t, eval(t, tight, pairs("uniform-short", "least-active", "least-tokens", four)), "H11"); v.Status != Invalid {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
}

func TestSignTestFloor(t *testing.T) {
	for n, want := range map[int]float64{0: 1, 1: 1, 2: 0.5, 3: 0.25, 4: 0.125, 5: 0.0625, 6: 0.03125, 7: 0.015625, 8: 0.0078125, 10: 2.0 / 1024, 59: 2.0 / (1 << 59), 60: 0, 500: 0} {
		if got := SignTestFloor(n); got != want {
			t.Errorf("SignTestFloor(%d) = %v, want %v", n, got, want)
		}
	}
	if NonZero([]float64{0, 1, -1, 0, 2}) != 3 || NonZero(nil) != 0 {
		t.Error("NonZero")
	}
	if !ZeroSpread([]float64{3, 3, 3}) || !ZeroSpread([]float64{0}) || ZeroSpread([]float64{3, 3, 4}) || ZeroSpread(nil) {
		t.Error("ZeroSpread")
	}
}

// With n non-zero pairs the smallest p-value an exact sign test can give is 2/2^n, so at alpha = 0.05 five pairs
// can never confirm a superiority claim, six can (0.03125), and with two confirmatory claims in the family
// (Holm multiplies the smaller p by 2) seven are needed (0.0156 * 2 = 0.031; six give 0.0625).
func TestTooFewPairsCannotConfirmWhateverTheEffect(t *testing.T) {
	huge := func(n int) []float64 {
		out := make([]float64, n)
		for i := range out {
			out[i] = 0.4 + float64(i)/1000 // every pair improves by about 40%
		}
		return out
	}
	conf := `"confirm_min_effect":0.1,"refute_max_effect":0.05`
	for n, want := range map[int]Status{5: Inconclusive, 6: Confirmed, 7: Confirmed} {
		for _, extra := range []string{conf, cmpFields} { // exploratory and confirmatory (family of one)
			pred := sup("H1", "mixed", "least-tokens", n, extra)
			v := status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", huge(n))), "H1")
			if v.Status != want {
				t.Errorf("n=%d %q: %s (%s), want %s", n, extra, v.Status, reasons(v), want)
			}
			if v.Stats.PBootstrap > 0.001 || v.Stats.P != SignTestFloor(n) || v.Stats.NonZero != n {
				t.Errorf("n=%d: bootstrap p is tiny but the p used is the floor: %+v", n, v.Stats)
			}
		}
	}
	// Two confirmatory claims: six pairs each is not enough, seven is.
	for n, want := range map[int]Status{6: Inconclusive, 7: Confirmed} {
		runs := append(pairs("mixed", "least-active", "least-tokens", huge(n)), pairs("burst", "least-active", "least-work", huge(n))...)
		pred := sup("H1", "mixed", "least-tokens", n, cmpFields) + sup("H2", "burst", "least-work", n, cmpFields)
		r := eval(t, pred, runs)
		for _, id := range []string{"H1", "H2"} {
			if v := status(t, r, id); v.Status != want {
				t.Errorf("family of two, n=%d %s: %s (%s), want %s", n, id, v.Status, reasons(v), want)
			}
		}
	}
	// A pair with no change does not count: six pairs with one zero are five non-zero pairs.
	z := huge(6)
	z[0] = 0
	v := status(t, eval(t, sup("H1", "mixed", "least-tokens", 6, conf), pairs("mixed", "least-active", "least-tokens", z)), "H1")
	if v.Stats.NonZero != 5 || v.Status == Confirmed {
		t.Errorf("%d non-zero, %s", v.Stats.NonZero, v.Status)
	}
	// The floor never lowers a larger bootstrap p-value.
	effects := []float64{-0.5, -0.5, -0.5, 0.12, 0.13, 0.14, 0.15, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, 0.22}
	b := status(t, eval(t, sup("H1", "mixed", "least-tokens", 14, conf), pairs("mixed", "least-active", "least-tokens", effects)), "H1")
	if b.Stats.P != b.Stats.PBootstrap || b.Stats.P < 0.025 {
		t.Errorf("%+v", b.Stats)
	}
}

func TestZeroSpreadIsNeverConfirmedOrRefuted(t *testing.T) {
	same := func(e float64, n int) []float64 {
		out := make([]float64, n)
		for i := range out {
			out[i] = e
		}
		return out
	}
	conf := `"confirm_min_effect":0.1,"refute_max_effect":0.05`
	for name, c := range map[string]struct {
		pred    string
		effects []float64
	}{
		"superiority, constant big gain":     {sup("H1", "mixed", "least-tokens", 10, conf), same(0.3, 10)},
		"superiority, constant tiny gain":    {sup("H1", "mixed", "least-tokens", 10, conf), same(0.01, 10)},
		"confirmatory, constant gain":        {sup("H1", "mixed", "least-tokens", 10, cmpFields), same(0.3, 10)},
		"equivalence, identical (no change)": {block(fmt.Sprintf(`{"id":"H1","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":%s,"margin":0.05}`, seedList(10))), same(0, 10)},
		"equivalence, constant large change": {block(fmt.Sprintf(`{"id":"H1","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":%s,"margin":0.05}`, seedList(10))), same(0.3, 10)},
	} {
		v := status(t, eval(t, c.pred, pairs("mixed", "least-active", "least-tokens", c.effects)), "H1")
		if v.Status != Inconclusive || !strings.Contains(reasons(v), "degenerate: zero spread") {
			t.Errorf("%s: %s (%s)", name, v.Status, reasons(v))
		}
	}
	// One pair off by a hair is spread again, and the claim can be confirmed.
	e := same(0.3, 10)
	e[0] = 0.301
	if v := status(t, eval(t, sup("H1", "mixed", "least-tokens", 10, conf), pairs("mixed", "least-active", "least-tokens", e)), "H1"); v.Status != Confirmed {
		t.Errorf("%s %s", v.Status, reasons(v))
	}
	// A degenerate member keeps an all_of from being confirmed.
	runs := pairs("mixed", "least-active", "least-tokens", same(0.3, 10))
	pred := sup("P", "mixed", "least-tokens", 10, conf) + block(`{"id":"ALL","kind":"all_of","parts":["P"]}`)
	if v := status(t, eval(t, pred, runs), "ALL"); v.Status != Inconclusive {
		t.Errorf("%s", v.Status)
	}
}

func TestFewDistinctSeedsWarnWithoutChangingTheVerdict(t *testing.T) {
	// Three seeds, three repeats each (9 pairs): no warning. Two seeds: a warning, same kind of verdict.
	build := func(seeds int) ([]report.Result, string) {
		var runs []report.Result
		var ss []string
		for s := 1; s <= seeds; s++ {
			ss = append(ss, fmt.Sprint(s))
			for rep := 1; rep <= 9/seeds+1; rep++ {
				runs = append(runs,
					mkRun(runSpec{id: fmt.Sprintf("a%d-%d", s, rep), strategy: "least-active", workload: "mixed", seed: int64(s), rep: rep, p95: 100}),
					mkRun(runSpec{id: fmt.Sprintf("b%d-%d", s, rep), strategy: "least-tokens", workload: "mixed", seed: int64(s), rep: rep, p95: 70 + float64(s*10+rep)/50}))
			}
		}
		return runs, block(fmt.Sprintf(`{"id":"H1","kind":"superiority","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":[%s],"direction":"lower","confirm_min_effect":0.1,"refute_max_effect":0.05}`, strings.Join(ss, ",")))
	}
	for seeds, warn := range map[int]bool{1: true, 2: true, 3: false, 4: false} {
		runs, pred := build(seeds)
		r := eval(t, pred, runs)
		v := status(t, r, "H1")
		if v.Status != Confirmed {
			t.Fatalf("%d seeds: %s %s", seeds, v.Status, reasons(v))
		}
		if (len(v.Warnings) == 1) != warn {
			t.Errorf("%d seeds: warnings %v, want warning=%v", seeds, v.Warnings, warn)
		}
		var out bytes.Buffer
		_ = r.Write(&out)
		if strings.Contains(out.String(), "! warning:") != warn {
			t.Errorf("%d seeds: the warning must be printed exactly when raised", seeds)
		}
	}
}

func TestAConfidenceIntervalWithLowerBoundExactlyZeroDoesNotExcludeZero(t *testing.T) {
	// 4 pairs without change and 6 with a ~20% gain: the median is 20% but a resample can have a zero median
	// with probability about 17%, so the lower end of the interval is exactly 0.
	effects := []float64{0, 0, 0, 0, 0.2, 0.201, 0.202, 0.203, 0.204, 0.205}
	v := status(t, eval(t, sup("H1", "mixed", "least-tokens", 10, `"confirm_min_effect":0.1,"refute_max_effect":0.05`), pairs("mixed", "least-active", "least-tokens", effects)), "H1")
	if v.Stats.Lo != 0 || v.Stats.Effect < 0.1 {
		t.Fatalf("fixture: %+v", v.Stats)
	}
	if v.Status != Inconclusive || !strings.Contains(reasons(v), "includes zero") {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
}

func TestEquivalenceRefuteMarginDefaultsToTheMargin(t *testing.T) {
	pred := block(fmt.Sprintf(`{"id":"H2","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":%s,"margin":0.05}`, seedList(8)))
	// An interval straddling the margin (about 4% to 6%) is neither inside it nor entirely outside it. With the
	// default refute margin of 5% that is inconclusive; a refute margin of 0 would call it refuted.
	straddle := []float64{0.04, 0.06, 0.045, 0.055, 0.05, 0.04, 0.06, 0.05}
	if v := status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", straddle)), "H2"); v.Status != Inconclusive {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	// An explicit larger refute margin changes the outcome for entirely-outside data only if it is exceeded.
	out := []float64{0.07, 0.08, 0.075, 0.07, 0.08, 0.075, 0.07, 0.08}
	if v := status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", out)), "H2"); v.Status != Refuted {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
	wide := strings.Replace(pred, `"margin":0.05`, `"margin":0.05,"refute_margin":0.09`, 1)
	if v := status(t, eval(t, wide, pairs("mixed", "least-active", "least-tokens", out)), "H2"); v.Status != Inconclusive {
		t.Fatalf("%s %s", v.Status, reasons(v))
	}
}

func TestTheDescriptiveTOSTUsesAlphaOverTwoAndEqualityIsNotEquivalence(t *testing.T) {
	// Differences whose t-based TOST p-value lies between 0.025 and 0.05: equivalent at 0.05, not at 0.025, which
	// is the level matching a 95% interval. The verdict's cross-check must use 0.025.
	effects := []float64{0.01, -0.01, -0.02, 0, -0.03}
	var margin float64
	for m := 0.0240; m <= 0.0299; m += 0.0001 {
		d := []float64{-0.01, 0.01, 0.02, 0, 0.03}
		r, _ := TOST(d, m, 0.025)
		if r.P > 0.03 && r.P < 0.045 {
			margin = m
			break
		}
	}
	if margin == 0 {
		t.Fatal("no margin found for the fixture")
	}
	pred := block(fmt.Sprintf(`{"id":"H2","kind":"equivalence","metric":"latency_p95","a":{"scheduler":"least-active","workload":"mixed"},"b":{"scheduler":"least-tokens","workload":"mixed"},"seeds":%s,"margin":%v}`, seedList(5), margin))
	v := status(t, eval(t, pred, pairs("mixed", "least-active", "least-tokens", effects)), "H2")
	if v.Stats.TOST == nil || v.Stats.TOST.P <= 0.025 || v.Stats.TOST.P >= 0.05 || v.Stats.TOST.Equivalent {
		t.Fatalf("alpha 0.025 must be used: %+v", v.Stats.TOST)
	}
	// Equality with alpha is not equivalence (P < alpha, strictly).
	r, _ := TOST([]float64{-0.01, 0.01, 0.02, 0, 0.03}, 0.03, 0.025)
	if eq, _ := TOST([]float64{-0.01, 0.01, 0.02, 0, 0.03}, 0.03, r.P); eq.Equivalent {
		t.Error("P equal to alpha must not be equivalent")
	}
}

func TestTunedValueWindowIsInclusiveToTheNanosecond(t *testing.T) {
	pred := sup("H3", "heavy-tail", "least-tokens", 10, cmpFields)
	const predictionsFrozen, tunedFrozen = "2026-11-02T10:00:00Z", "2026-11-05T10:00:00Z"
	try := func(tunedAt string) error {
		b := []byte(`{"tuned_at":"` + tunedAt + `","values":{"input_weight":0.3}}`)
		f := freezeFor(pred)
		f.FrozenAt = predictionsFrozen
		f.TunedValuesSHA256, f.TunedValuesFrozenAt = shaHex(b), tunedFrozen
		_, err := Evaluate(Inputs{Predictions: []byte(pred), Freeze: f, Tuned: b})
		return err
	}
	for at, ok := range map[string]bool{
		"2026-11-02T09:59:59.999999999Z": false, // 1 ns before the predictions freeze
		"2026-11-02T10:00:00Z":           true,
		"2026-11-02T10:00:00.000000001Z": true,
		"2026-11-05T09:59:59.999999999Z": true,
		"2026-11-05T10:00:00Z":           true,  // exactly at the tuned-values freeze
		"2026-11-05T10:00:00.000000001Z": false, // 1 ns after it
	} {
		err := try(at)
		if ok && err != nil {
			t.Errorf("tuned_at %s must be accepted: %v", at, err)
		}
		if !ok && err == nil {
			t.Errorf("tuned_at %s must be refused", at)
		}
	}
}
