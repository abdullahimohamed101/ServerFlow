package verdict

import (
	"fmt"
	"io"
	"strings"

	"serverflow/internal/bench/report"
)

// Banner heads every verdict output.
const Banner = "benchmark verdict (simulator-validated, not hardware-validated)"

// Write prints the report. The output is a pure function of the report: no clock, no paths, no randomness.
func (r Report) Write(w io.Writer) error {
	var b strings.Builder
	b.WriteString(Banner + "\n")
	fmt.Fprintf(&b, "frozen predictions text sha256 %s\n", r.PredictionsHash)
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "%s\n", report.Clean(n))
	}
	if len(r.HolmFamily) > 0 {
		fmt.Fprintf(&b, "confirmatory family (Holm, alpha %g): %s\n", Alpha, strings.Join(r.HolmFamily, ", "))
	} else {
		b.WriteString("confirmatory family: none declared\n")
	}
	b.WriteString("Statistics: percentile bootstrap interval and p-value of the median paired relative difference (see internal/bench/verdict/doc.go). A verdict describes the simulated worlds, workloads and seeds in the result files; it is not a claim about hardware.\n")
	for _, v := range r.Verdicts {
		fmt.Fprintf(&b, "\n%s  %s  %s\n", report.Clean(v.ID), v.Kind, v.Status)
		if v.Claim != "" {
			fmt.Fprintf(&b, "    claim: %s\n", report.Clean(v.Claim))
		}
		if s := v.Stats; s != nil {
			what := "median relative difference B against A"
			if v.Kind == KindSuperiority {
				what = "median effect (improvement as a share of A)"
			}
			fmt.Fprintf(&b, "    %s, %s: %s [%s, %s] at %.0f%%, %d pairs, %d resamples, seed %d\n", report.Clean(s.Metric), what,
				signed(s.Effect), signed(s.Lo), signed(s.Hi), 100*s.Level, s.Pairs, s.Resamples, s.BootstrapSed)
			fmt.Fprintf(&b, "    p-value %.4g", s.P)
			if s.PBootstrap != s.P {
				fmt.Fprintf(&b, " (bootstrap %.4g raised to the exact sign-test floor for %d non-zero pairs)", s.PBootstrap, s.NonZero)
			}
			if s.Adjusted != nil {
				fmt.Fprintf(&b, "; Holm-adjusted %.4g (family of %d)", *s.Adjusted, s.FamilySize)
			}
			b.WriteString("\n")
			if t := s.TOST; t != nil {
				fmt.Fprintf(&b, "    t-TOST (descriptive, never decides): mean %s, p %.4g (lower %.4g, upper %.4g)\n", signed(t.Mean), t.P, t.PLower, t.PUpper)
			}
		}
		for _, w := range v.Warnings {
			fmt.Fprintf(&b, "    ! warning: %s\n", report.Clean(w))
		}
		for _, why := range v.Reasons {
			fmt.Fprintf(&b, "    - %s\n", report.Clean(why))
		}
	}
	fmt.Fprintf(&b, "\nSummary: %d confirmed, %d refuted, %d inconclusive, %d invalid of %d.\n",
		r.Count(Confirmed), r.Count(Refuted), r.Count(Inconclusive), r.Count(Invalid), len(r.Verdicts))
	_, err := io.WriteString(w, b.String())
	return err
}

func signed(x float64) string {
	s := fmt.Sprintf("%+.2f%%", 100*x)
	if s == "-0.00%" { // a tiny negative number is not a sign worth showing
		return "+0.00%"
	}
	return s
}
