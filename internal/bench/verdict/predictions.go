package verdict

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
)

// A predictions file is Markdown for people with, for each hypothesis, one fenced block for the tool:
//
//	```verdict
//	{"id": "H3", "kind": "superiority", ...}
//	```
//
// The block holds one JSON object (Hypothesis). Prose, tables and every other fence are ignored. The frozen
// text is everything before a line that reads exactly "## Amendments"; dated amendments after the freeze go
// under that heading, are prose, are never parsed, and are not part of the hash: an amendment can explain, it
// cannot change a verdict.

// AmendmentsHeading ends the frozen text of a predictions file.
const AmendmentsHeading = "## Amendments"

// Hypothesis kinds.
const (
	KindSuperiority = "superiority"
	KindEquivalence = "equivalence"
	KindTunedRange  = "tuned_range"
	KindAllOf       = "all_of"
)

// Directions of a superiority claim: which way the treatment arm must move against the baseline.
const (
	DirectionLower  = "lower"
	DirectionHigher = "higher"
)

// Selector picks the runs of one arm. Scheduler and Workload are required; every other field, when set, must
// equal what the run recorded. A selector that names a world, an overlay or an option only matches runs that
// carry the experiment record.
type Selector struct {
	Scheduler   string `json:"scheduler"`
	Workload    string `json:"workload"`
	World       string `json:"world,omitempty"`
	LoadOverlay string `json:"load_overlay,omitempty"`
	// InputWeight and OutputFraction are the recorded scheduler.cost.* values. InputWeightTuned names a key of
	// the tuned-values file whose value must equal the recorded input weight (H5's held-out sweep).
	InputWeight      *float64 `json:"input_weight,omitempty"`
	InputWeightTuned string   `json:"input_weight_tuned,omitempty"`
	OutputFraction   *float64 `json:"output_fraction,omitempty"`
	Concurrency      *int     `json:"concurrency,omitempty"`
	Rate             *float64 `json:"rate_per_second,omitempty"`
}

// Hypothesis is one pre-registered prediction.
type Hypothesis struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Claim string `json:"claim,omitempty"`

	// Comparison (superiority and equivalence): the metric, the two arms and the seeds that were
	// registered. Runs are paired by (seed, repeat index).
	Metric string    `json:"metric,omitempty"`
	A      *Selector `json:"a,omitempty"`
	B      *Selector `json:"b,omitempty"`
	Seeds  []int64   `json:"seeds,omitempty"`

	// Superiority: B must move Direction against A. Confirmed when the median effect (the improvement as a
	// fraction of A) reaches ConfirmMinEffect and its 95% interval excludes zero (and, if Confirmatory, the
	// Holm-adjusted p-value is below 0.05); refuted when the whole interval lies below RefuteMaxEffect.
	Direction        string  `json:"direction,omitempty"`
	Confirmatory     bool    `json:"confirmatory,omitempty"`
	ConfirmMinEffect float64 `json:"confirm_min_effect,omitempty"`
	RefuteMaxEffect  float64 `json:"refute_max_effect,omitempty"`

	// Equivalence: confirmed when the 95% interval of the median relative difference lies entirely inside
	// (-Margin, +Margin); refuted when entirely outside [-RefuteMargin, +RefuteMargin] (default Margin);
	// invalid (the harness is suspect) when entirely outside [-InvalidMargin, +InvalidMargin] if that is set.
	Margin        float64 `json:"margin,omitempty"`
	RefuteMargin  float64 `json:"refute_margin,omitempty"`
	InvalidMargin float64 `json:"invalid_margin,omitempty"`

	// TunedRange: the tuned value ValueKey must lie in [Min, Max]: confirmed inside, refuted outside.
	ValueKey string   `json:"value_key,omitempty"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`

	// AllOf rolls up other hypotheses: confirmed when all parts are, refuted when any is, invalid when any is
	// and none is refuted, else inconclusive.
	Parts []string `json:"parts,omitempty"`

	// Resamples and BootstrapSeed override the bootstrap defaults.
	Resamples     int     `json:"resamples,omitempty"`
	BootstrapSeed *uint64 `json:"bootstrap_seed,omitempty"`
}

var (
	idPattern       = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,31}$`)
	selectorPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:,/-]{0,127}$`)
)

// FrozenText returns the part of a predictions file that the freeze covers.
func FrozenText(content []byte) []byte {
	if bytes.HasPrefix(content, []byte(AmendmentsHeading+"\n")) || bytes.Equal(content, []byte(AmendmentsHeading)) {
		return nil
	}
	if i := bytes.Index(content, []byte("\n"+AmendmentsHeading+"\n")); i >= 0 {
		return content[:i+1]
	}
	if bytes.HasSuffix(content, []byte("\n"+AmendmentsHeading)) {
		return content[:len(content)-len(AmendmentsHeading)]
	}
	return content
}

// FrozenHash is the SHA-256 (hex) of the frozen text: what a freeze record pins.
func FrozenHash(content []byte) string {
	sum := sha256.Sum256(FrozenText(content))
	return hex.EncodeToString(sum[:])
}

// ParsePredictions reads the hypotheses of the frozen text of a predictions file, in file order, and
// validates each. Anything wrong is an error naming the hypothesis: a prediction the tool cannot read
// exactly is not silently skipped.
func ParsePredictions(content []byte) ([]Hypothesis, error) {
	var out []Hypothesis
	lines := strings.Split(string(FrozenText(content)), "\n")
	for i := 0; i < len(lines); i++ {
		if strings.TrimRight(lines[i], " \t\r") != "```verdict" {
			continue
		}
		start := i + 1
		end := -1
		for j := start; j < len(lines); j++ {
			if strings.TrimRight(lines[j], " \t\r") == "```" {
				end = j
				break
			}
		}
		if end < 0 {
			return nil, fmt.Errorf("predictions: the verdict block at line %d is never closed", i+1)
		}
		var h Hypothesis
		dec := json.NewDecoder(strings.NewReader(strings.Join(lines[start:end], "\n")))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&h); err != nil {
			return nil, fmt.Errorf("predictions: the verdict block at line %d is not valid: %w", i+1, err)
		}
		if _, err := dec.Token(); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("predictions: the verdict block at line %d holds more than one JSON value", i+1)
		}
		out = append(out, h)
		i = end
	}
	if len(out) == 0 {
		return nil, errors.New("predictions: no ```verdict block found before the amendments")
	}
	ids := map[string]int{}
	for _, h := range out {
		if err := h.validate(); err != nil {
			return nil, fmt.Errorf("predictions: hypothesis %q: %w", h.ID, err)
		}
		if ids[h.ID]++; ids[h.ID] > 1 {
			return nil, fmt.Errorf("predictions: hypothesis id %q appears twice", h.ID)
		}
	}
	for _, h := range out {
		for _, p := range h.Parts {
			part := -1
			for k, o := range out {
				if o.ID == p {
					part = k
				}
			}
			switch {
			case part < 0:
				return nil, fmt.Errorf("predictions: hypothesis %q: part %q does not exist", h.ID, p)
			case out[part].Kind == KindAllOf:
				return nil, fmt.Errorf("predictions: hypothesis %q: part %q is itself an all_of", h.ID, p)
			}
		}
	}
	return out, nil
}

func fin(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func (h Hypothesis) validate() error {
	if !idPattern.MatchString(h.ID) {
		return fmt.Errorf("the id must match %s", idPattern)
	}
	switch h.Kind {
	case KindSuperiority, KindEquivalence, KindTunedRange, KindAllOf:
	default:
		return fmt.Errorf("unknown kind %q (choose %s, %s, %s or %s)", h.Kind, KindSuperiority, KindEquivalence, KindTunedRange, KindAllOf)
	}
	set := func(name string, isSet bool, allowed ...string) error {
		if !isSet {
			return nil
		}
		for _, k := range allowed {
			if h.Kind == k {
				return nil
			}
		}
		return fmt.Errorf("%s does not apply to kind %q", name, h.Kind)
	}
	for _, c := range []error{
		set("metric", h.Metric != "", KindSuperiority, KindEquivalence),
		set("a", h.A != nil, KindSuperiority, KindEquivalence),
		set("b", h.B != nil, KindSuperiority, KindEquivalence),
		set("seeds", len(h.Seeds) > 0, KindSuperiority, KindEquivalence),
		set("direction", h.Direction != "", KindSuperiority),
		set("confirmatory", h.Confirmatory, KindSuperiority),
		set("confirm_min_effect", h.ConfirmMinEffect != 0, KindSuperiority),
		set("refute_max_effect", h.RefuteMaxEffect != 0, KindSuperiority),
		set("margin", h.Margin != 0, KindEquivalence),
		set("refute_margin", h.RefuteMargin != 0, KindEquivalence),
		set("invalid_margin", h.InvalidMargin != 0, KindEquivalence),
		set("value_key", h.ValueKey != "", KindTunedRange),
		set("min", h.Min != nil, KindTunedRange),
		set("max", h.Max != nil, KindTunedRange),
		set("parts", len(h.Parts) > 0, KindAllOf),
		set("resamples", h.Resamples != 0, KindSuperiority, KindEquivalence),
		set("bootstrap_seed", h.BootstrapSeed != nil, KindSuperiority, KindEquivalence),
	} {
		if c != nil {
			return c
		}
	}
	switch h.Kind {
	case KindSuperiority, KindEquivalence:
		if err := h.validateComparison(); err != nil {
			return err
		}
		if h.Kind == KindSuperiority {
			if h.Direction != DirectionLower && h.Direction != DirectionHigher {
				return fmt.Errorf("direction must be %q or %q", DirectionLower, DirectionHigher)
			}
			if !(h.ConfirmMinEffect > 0 && h.ConfirmMinEffect <= 10) || !fin(h.ConfirmMinEffect) {
				return errors.New("confirm_min_effect must be in (0, 10] (a fraction: 0.10 is 10%)")
			}
			if !(h.RefuteMaxEffect >= 0 && h.RefuteMaxEffect <= h.ConfirmMinEffect) {
				return errors.New("refute_max_effect must be in [0, confirm_min_effect]")
			}
			return nil
		}
		if !(h.Margin > 0 && h.Margin <= 10) {
			return errors.New("margin must be in (0, 10] (a fraction: 0.05 is 5%)")
		}
		if h.RefuteMargin != 0 && !(h.RefuteMargin >= h.Margin && h.RefuteMargin <= 10) {
			return errors.New("refute_margin must be in [margin, 10]")
		}
		if h.InvalidMargin != 0 && !(h.InvalidMargin >= h.Margin && h.InvalidMargin <= 10) {
			return errors.New("invalid_margin must be in [margin, 10]")
		}
	case KindTunedRange:
		if h.ValueKey == "" || h.Min == nil || h.Max == nil || !fin(*h.Min) || !fin(*h.Max) || *h.Min >= *h.Max {
			return errors.New("a tuned_range needs value_key and finite min < max (a point is not a range)")
		}
	case KindAllOf:
		if len(h.Parts) == 0 {
			return errors.New("an all_of needs parts")
		}
		seen := map[string]bool{}
		for _, p := range h.Parts {
			if p == h.ID || seen[p] {
				return fmt.Errorf("part %q is repeated or is the hypothesis itself", p)
			}
			seen[p] = true
		}
	default:
		return fmt.Errorf("unknown kind %q (choose %s, %s, %s or %s)", h.Kind, KindSuperiority, KindEquivalence, KindTunedRange, KindAllOf)
	}
	return nil
}

func (h Hypothesis) validateComparison() error {
	if _, ok := metrics[h.Metric]; !ok {
		return fmt.Errorf("unknown metric %q (choose one of: %s)", h.Metric, strings.Join(MetricNames(), ", "))
	}
	if h.A == nil || h.B == nil {
		return errors.New("a and b are required")
	}
	if err := h.A.validate(); err != nil {
		return fmt.Errorf("a: %w", err)
	}
	if err := h.B.validate(); err != nil {
		return fmt.Errorf("b: %w", err)
	}
	if len(h.Seeds) == 0 || len(h.Seeds) > 1000 {
		return errors.New("seeds must list between 1 and 1000 seeds")
	}
	seen := map[int64]bool{}
	for _, s := range h.Seeds {
		if seen[s] {
			return fmt.Errorf("seed %d is listed twice", s)
		}
		seen[s] = true
	}
	if h.Resamples != 0 && (h.Resamples < minResamples || h.Resamples > MaxResamples) {
		return fmt.Errorf("resamples must be between %d and %d", minResamples, MaxResamples)
	}
	return nil
}

func (s Selector) validate() error {
	for name, v := range map[string]string{"scheduler": s.Scheduler, "workload": s.Workload} {
		if !selectorPattern.MatchString(v) {
			return fmt.Errorf("%s %q is not valid", name, v)
		}
	}
	if s.World != "" && !selectorPattern.MatchString(s.World) {
		return fmt.Errorf("world %q is not valid", s.World)
	}
	switch s.LoadOverlay {
	case "", "max", "ledger":
	default:
		return fmt.Errorf("load_overlay %q is not max or ledger", s.LoadOverlay)
	}
	if s.InputWeight != nil && s.InputWeightTuned != "" {
		return errors.New("input_weight and input_weight_tuned are exclusive")
	}
	if s.InputWeightTuned != "" && !selectorPattern.MatchString(s.InputWeightTuned) {
		return fmt.Errorf("input_weight_tuned %q is not valid", s.InputWeightTuned)
	}
	if s.InputWeight != nil && !(fin(*s.InputWeight) && *s.InputWeight >= 0 && *s.InputWeight <= 100) {
		return errors.New("input_weight must be in [0, 100]")
	}
	if s.OutputFraction != nil && !(*s.OutputFraction > 0 && *s.OutputFraction <= 1) {
		return errors.New("output_fraction must be in (0, 1]")
	}
	if s.Concurrency != nil && *s.Concurrency < 1 {
		return errors.New("concurrency must be positive")
	}
	if s.Rate != nil && !(fin(*s.Rate) && *s.Rate > 0) {
		return errors.New("rate_per_second must be positive")
	}
	return nil
}
