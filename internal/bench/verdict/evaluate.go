package verdict

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"serverflow/internal/bench/report"
)

// Status is a verdict.
type Status string

// The four verdicts. INVALID means the evidence cannot answer (missing or inconsistent input), which is
// different from INCONCLUSIVE (the evidence was valid and too weak).
const (
	Confirmed    Status = "CONFIRMED"
	Refuted      Status = "REFUTED"
	Inconclusive Status = "INCONCLUSIVE"
	Invalid      Status = "INVALID"
)

// Inputs is everything a verdict is computed from. Evaluate does no I/O.
type Inputs struct {
	// Predictions are the bytes of the predictions file.
	Predictions []byte
	// Freeze pins the predictions (and tuned values). It may be nil only with AllowUnfrozen.
	Freeze *Freeze
	// AllowUnfrozen evaluates without a freeze record; the report then says the verdicts are not
	// pre-registered evidence. For smoke checks of the pipeline.
	AllowUnfrozen bool
	// Tuned are the bytes of the tuned-values file, or nil.
	Tuned []byte
	// Runs are the loaded results.
	Runs []report.Result
}

// Verdict is the outcome for one hypothesis.
type Verdict struct {
	ID, Kind, Claim string
	Status          Status
	// Reasons say why, in sentences; for INVALID they are the problems found.
	Reasons []string
	// Stats, when set, are the numbers the verdict rests on.
	Stats *Stats
}

// Stats are the numbers of one comparison. Effect, Lo and Hi are in fractions of the baseline: for a
// superiority claim the improvement in the claimed direction, for an equivalence claim the signed relative
// difference B against A.
type Stats struct {
	Metric       string
	Pairs        int
	Effect       float64
	Lo, Hi       float64
	Level        float64
	Resamples    int
	BootstrapSed uint64
	P            float64
	// Adjusted and FamilySize are set for a confirmatory hypothesis (Holm).
	Adjusted   *float64
	FamilySize int
	// TOST is the descriptive parametric cross-check of an equivalence claim.
	TOST *TOSTResult
}

// Report is the result of Evaluate.
type Report struct {
	PredictionsHash string
	// Frozen is true when a freeze record was given and matched.
	Frozen bool
	// Notes are lines about the evaluation as a whole (freeze status, runs that could not be read).
	Notes    []string
	Verdicts []Verdict
	// HolmFamily lists the ids of the confirmatory hypotheses, in file order.
	HolmFamily []string
}

// Count returns how many verdicts have status s.
func (r Report) Count(s Status) int {
	n := 0
	for _, v := range r.Verdicts {
		if v.Status == s {
			n++
		}
	}
	return n
}

type pairKey struct {
	seed int64
	rep  int
}

// Evaluate computes the verdicts. It returns an error, and no verdicts, when the predictions cannot be read,
// when there is no freeze record and AllowUnfrozen is not set, or when the tuned values are refused. A
// predictions file that differs from the freeze is not an error: every hypothesis is then INVALID and says so.
func Evaluate(in Inputs) (Report, error) {
	hs, err := ParsePredictions(in.Predictions)
	if err != nil {
		return Report{}, err
	}
	rep := Report{PredictionsHash: FrozenHash(in.Predictions)}
	for _, h := range hs {
		if h.Kind == KindSuperiority && h.Confirmatory {
			rep.HolmFamily = append(rep.HolmFamily, h.ID)
		}
	}
	var tuned *Tuned
	switch {
	case in.Freeze == nil && !in.AllowUnfrozen:
		return Report{}, errors.New("no freeze record: verdicts are only pre-registered evidence against a freeze (pass one, or allow unfrozen for a smoke check)")
	case in.Freeze == nil:
		if in.Tuned != nil {
			return Report{}, errors.New("tuned values need a freeze record")
		}
		rep.Notes = append(rep.Notes, "NO FREEZE RECORD: these verdicts are not pre-registered evidence")
	default:
		if in.Tuned != nil {
			t, err := CheckTuned(*in.Freeze, in.Tuned)
			if err != nil {
				return Report{}, err
			}
			tuned = &t
		}
		if rep.PredictionsHash == in.Freeze.PredictionsSHA256 {
			rep.Frozen = true
			rep.Notes = append(rep.Notes, fmt.Sprintf("predictions match the freeze recorded at %s", in.Freeze.FrozenAt))
		} else {
			msg := fmt.Sprintf("the predictions file (frozen text sha256 %s) differs from the freeze record (%s): it was edited after the freeze", rep.PredictionsHash, in.Freeze.PredictionsSHA256)
			rep.Notes = append(rep.Notes, "PREDICTIONS CHANGED AFTER THE FREEZE: no verdict below is valid evidence")
			for _, h := range hs {
				rep.Verdicts = append(rep.Verdicts, Verdict{ID: h.ID, Kind: h.Kind, Claim: h.Claim, Status: Invalid, Reasons: []string{msg}})
			}
			return rep, nil
		}
	}

	byID := map[string]int{}
	rep.Verdicts = make([]Verdict, len(hs))
	for i, h := range hs {
		byID[h.ID] = i
		switch h.Kind {
		case KindSuperiority, KindEquivalence:
			rep.Verdicts[i] = evaluateComparison(h, in.Runs, tuned)
		case KindTunedRange:
			rep.Verdicts[i] = evaluateTunedRange(h, tuned)
		}
	}

	// Holm over the confirmatory family. A hypothesis with no computed p-value counts with p = 1.
	if len(rep.HolmFamily) > 0 {
		ps := make([]float64, len(rep.HolmFamily))
		for k, id := range rep.HolmFamily {
			ps[k] = 1
			if s := rep.Verdicts[byID[id]].Stats; s != nil {
				ps[k] = s.P
			}
		}
		holm, err := Holm(ps, Alpha)
		if err != nil {
			return Report{}, err
		}
		for k, id := range rep.HolmFamily {
			v := &rep.Verdicts[byID[id]]
			if v.Stats == nil {
				continue
			}
			adj := holm[k].Adjusted
			v.Stats.Adjusted, v.Stats.FamilySize = &adj, len(ps)
			if v.Status == Confirmed && !holm[k].Reject {
				v.Status = Inconclusive
				v.Reasons = []string{fmt.Sprintf("the effect reaches the threshold and its interval excludes zero, but the Holm-adjusted p-value %.4g (family of %d) is not below %g", adj, len(ps), Alpha)}
			} else if v.Status == Confirmed {
				v.Reasons = append(v.Reasons, fmt.Sprintf("Holm-adjusted p-value %.4g (family of %d) is below %g", adj, len(ps), Alpha))
			}
		}
	}

	for i, h := range hs {
		if h.Kind == KindAllOf {
			rep.Verdicts[i] = evaluateAllOf(h, hs, rep.Verdicts, byID)
		}
	}
	return rep, nil
}

func evaluateAllOf(h Hypothesis, hs []Hypothesis, vs []Verdict, byID map[string]int) Verdict {
	v := Verdict{ID: h.ID, Kind: h.Kind, Claim: h.Claim}
	var confirmed, refuted, invalid int
	for _, p := range h.Parts {
		s := vs[byID[p]].Status
		v.Reasons = append(v.Reasons, fmt.Sprintf("%s: %s", p, s))
		switch s {
		case Confirmed:
			confirmed++
		case Refuted:
			refuted++
		case Invalid:
			invalid++
		}
	}
	switch {
	case refuted > 0:
		v.Status = Refuted
	case invalid > 0:
		v.Status = Invalid
	case confirmed == len(h.Parts):
		v.Status = Confirmed
	default:
		v.Status = Inconclusive
	}
	return v
}

func evaluateTunedRange(h Hypothesis, tuned *Tuned) Verdict {
	v := Verdict{ID: h.ID, Kind: h.Kind, Claim: h.Claim}
	if tuned == nil {
		v.Status, v.Reasons = Invalid, []string{"needs a tuned-values file (and a freeze that records it)"}
		return v
	}
	val, ok := tuned.Values[h.ValueKey]
	switch {
	case !ok:
		v.Status, v.Reasons = Invalid, []string{fmt.Sprintf("the tuned values have no %q", h.ValueKey)}
	case val >= *h.Min && val <= *h.Max:
		v.Status, v.Reasons = Confirmed, []string{fmt.Sprintf("tuned %s = %g lies in [%g, %g]", h.ValueKey, val, *h.Min, *h.Max)}
	default:
		v.Status, v.Reasons = Refuted, []string{fmt.Sprintf("tuned %s = %g lies outside [%g, %g]", h.ValueKey, val, *h.Min, *h.Max)}
	}
	return v
}

func evaluateComparison(h Hypothesis, runs []report.Result, tuned *Tuned) Verdict {
	v := Verdict{ID: h.ID, Kind: h.Kind, Claim: h.Claim}
	invalid := func(format string, a ...any) Verdict {
		v.Status = Invalid
		v.Reasons = append(v.Reasons, fmt.Sprintf(format, a...))
		return v
	}
	armA, problems := selectArm(h, "a", *h.A, runs, tuned)
	armB, more := selectArm(h, "b", *h.B, runs, tuned)
	problems = append(problems, more...)
	if len(problems) > 0 {
		v.Status, v.Reasons = Invalid, problems
		return v
	}
	// The same world on both sides.
	hashes := map[string]bool{}
	for _, arm := range []map[pairKey]report.Result{armA, armB} {
		for _, r := range arm {
			hashes[r.Metadata.Experiment.ParameterHash] = true
		}
	}
	if len(hashes) > 1 {
		return invalid("the selected runs have %d different parameter hashes: the arms (or the runs of an arm) did not run in the same world", len(hashes))
	}
	// Same set of pairs.
	var keys []pairKey
	for k := range armA {
		if _, ok := armB[k]; !ok {
			return invalid("seed %d repeat %d has a baseline run but no treatment run", k.seed, k.rep)
		}
		keys = append(keys, k)
	}
	for k := range armB {
		if _, ok := armA[k]; !ok {
			return invalid("seed %d repeat %d has a treatment run but no baseline run", k.seed, k.rep)
		}
	}
	for _, s := range h.Seeds {
		found := false
		for _, k := range keys {
			found = found || k.seed == s
		}
		if !found {
			return invalid("no runs for registered seed %d", s)
		}
	}
	if len(keys) < MinPairs {
		return invalid("%d pairs is fewer than the minimum of %d", len(keys), MinPairs)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].seed != keys[j].seed {
			return keys[i].seed < keys[j].seed
		}
		return keys[i].rep < keys[j].rep
	})
	get := metrics[h.Metric]
	a, b := make([]float64, len(keys)), make([]float64, len(keys))
	for i, k := range keys {
		var okA, okB bool
		a[i], okA = get(armA[k])
		b[i], okB = get(armB[k])
		if !okA || !okB {
			return invalid("metric %s is not measured in seed %d repeat %d (%s)", h.Metric, k.seed, k.rep, missingWhich(okA, okB))
		}
	}
	rel, err := PairedRelative(a, b)
	if err != nil {
		return invalid("%v", err)
	}
	signed := rel
	if h.Kind == KindSuperiority && h.Direction == DirectionLower {
		signed = make([]float64, len(rel))
		for i, x := range rel {
			signed[i] = -x
		}
	}
	resamples, seed := h.Resamples, uint64(DefaultBootstrapSeed)
	if resamples == 0 {
		resamples = DefaultResamples
	}
	if h.BootstrapSeed != nil {
		seed = *h.BootstrapSeed
	}
	bs, err := BootstrapMedian(signed, Level, resamples, seed)
	if err != nil {
		return invalid("%v", err)
	}
	v.Stats = &Stats{Metric: h.Metric, Pairs: len(keys), Effect: bs.Estimate, Lo: bs.Interval.Lo, Hi: bs.Interval.Hi, Level: Level,
		Resamples: resamples, BootstrapSed: seed, P: bs.P}

	if h.Kind == KindEquivalence {
		if t, err := TOST(rel, h.Margin, Alpha/2); err == nil {
			v.Stats.TOST = &t
		}
		return classifyEquivalence(h, v, bs.Interval)
	}
	return classifySuperiority(h, v, bs)
}

func missingWhich(okA, okB bool) string {
	switch {
	case !okA && !okB:
		return "neither arm"
	case !okA:
		return "baseline"
	default:
		return "treatment"
	}
}

func classifySuperiority(h Hypothesis, v Verdict, bs Bootstrap) Verdict {
	reaches := bs.Estimate >= h.ConfirmMinEffect
	excludesZero := bs.Interval.Lo > 0
	switch {
	case reaches && excludesZero:
		v.Status = Confirmed
		v.Reasons = []string{fmt.Sprintf("median effect %s reaches the threshold %s and its %.0f%% interval excludes zero", pct(bs.Estimate), pct(h.ConfirmMinEffect), 100*Level)}
	case bs.Interval.Hi < h.RefuteMaxEffect:
		v.Status = Refuted
		v.Reasons = []string{fmt.Sprintf("the whole %.0f%% interval lies below the refutation threshold %s", 100*Level, pct(h.RefuteMaxEffect))}
	default:
		v.Status = Inconclusive
		switch {
		case !excludesZero:
			v.Reasons = []string{fmt.Sprintf("the %.0f%% interval includes zero", 100*Level)}
		case !reaches:
			v.Reasons = []string{fmt.Sprintf("the interval excludes zero but the median effect %s is below the confirmation threshold %s and the interval is not entirely below the refutation threshold %s", pct(bs.Estimate), pct(h.ConfirmMinEffect), pct(h.RefuteMaxEffect))}
		}
	}
	return v
}

func classifyEquivalence(h Hypothesis, v Verdict, iv Interval) Verdict {
	refute := h.RefuteMargin
	if refute == 0 {
		refute = h.Margin
	}
	switch {
	case h.InvalidMargin > 0 && OutsideMargin(iv, h.InvalidMargin):
		v.Status = Invalid
		v.Reasons = []string{fmt.Sprintf("the whole interval lies outside +/-%s: a difference this large where none was predicted means the harness is suspect", pct(h.InvalidMargin))}
	case InsideMargin(iv, h.Margin):
		v.Status = Confirmed
		v.Reasons = []string{fmt.Sprintf("the %.0f%% interval lies entirely inside +/-%s", 100*Level, pct(h.Margin))}
	case OutsideMargin(iv, refute):
		v.Status = Refuted
		v.Reasons = []string{fmt.Sprintf("the %.0f%% interval lies entirely outside +/-%s", 100*Level, pct(refute))}
	default:
		v.Status = Inconclusive
		v.Reasons = []string{fmt.Sprintf("the %.0f%% interval is neither inside +/-%s nor outside +/-%s", 100*Level, pct(h.Margin), pct(refute))}
	}
	return v
}

func pct(x float64) string { return fmt.Sprintf("%.2f%%", 100*x) }

// selectArm returns the runs of one arm keyed by (seed, repeat index) and the problems found with them.
func selectArm(h Hypothesis, arm string, sel Selector, runs []report.Result, tuned *Tuned) (map[pairKey]report.Result, []string) {
	var problems []string
	want := map[int64]bool{}
	for _, s := range h.Seeds {
		want[s] = true
	}
	var tunedWeight *float64
	if sel.InputWeightTuned != "" {
		var x float64
		ok := false
		if tuned != nil {
			x, ok = tuned.Values[sel.InputWeightTuned]
		}
		if !ok {
			return nil, []string{fmt.Sprintf("arm %s needs the tuned value %q, which is not available", arm, sel.InputWeightTuned)}
		}
		tunedWeight = &x
	}
	out := map[pairKey]report.Result{}
	for _, r := range runs {
		if !want[r.Metadata.Seed] || !matches(sel, tunedWeight, r) {
			continue
		}
		label := report.Clean(r.RunID)
		if r.Metadata.Experiment == nil {
			problems = append(problems, fmt.Sprintf("run %s has no experiment record (written before PR G-lite): its world cannot be verified", label))
			continue
		}
		if !r.Valid {
			why := strings.Join(r.InvalidReasons, "; ")
			problems = append(problems, fmt.Sprintf("run %s is INVALID (%s); invalid runs are reported, not dropped", label, report.Clean(why)))
			continue
		}
		rep := r.Metadata.Repeat.Index
		if rep < 1 {
			rep = 1
		}
		k := pairKey{r.Metadata.Seed, rep}
		if prev, dup := out[k]; dup {
			problems = append(problems, fmt.Sprintf("arm %s: runs %s and %s are both seed %d repeat %d: the pairing is ambiguous", arm, label, report.Clean(prev.RunID), k.seed, k.rep))
			continue
		}
		out[k] = r
	}
	if len(out) == 0 && len(problems) == 0 {
		problems = append(problems, fmt.Sprintf("arm %s matches no run (scheduler %s, workload %s, seeds %d registered)", arm, sel.Scheduler, sel.Workload, len(h.Seeds)))
	}
	sort.Strings(problems)
	return out, problems
}

func matches(sel Selector, tunedWeight *float64, r report.Result) bool {
	m := r.Metadata
	strategy := m.Scheduler
	if m.Experiment != nil {
		strategy = m.Experiment.Strategy
	}
	if strategy != sel.Scheduler || m.Workload != sel.Workload {
		return false
	}
	if sel.Concurrency != nil && m.Concurrency != *sel.Concurrency {
		return false
	}
	if sel.Rate != nil && m.Rate != *sel.Rate {
		return false
	}
	e := m.Experiment
	needsRecord := sel.World != "" || sel.LoadOverlay != "" || sel.InputWeight != nil || sel.OutputFraction != nil || tunedWeight != nil
	if needsRecord && e == nil {
		return false
	}
	if sel.World != "" && e.World != sel.World {
		return false
	}
	needsOptions := sel.LoadOverlay != "" || sel.InputWeight != nil || sel.OutputFraction != nil || tunedWeight != nil
	if needsOptions && e.Options == nil {
		return false
	}
	if sel.LoadOverlay != "" && e.Options.LoadOverlay != sel.LoadOverlay {
		return false
	}
	if sel.InputWeight != nil && e.Options.InputWeight != *sel.InputWeight {
		return false
	}
	if tunedWeight != nil && e.Options.InputWeight != *tunedWeight {
		return false
	}
	if sel.OutputFraction != nil && e.Options.OutputFraction != *sel.OutputFraction {
		return false
	}
	return true
}
