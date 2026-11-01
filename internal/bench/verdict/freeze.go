package verdict

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

// Freeze is the record that pins a predictions file (and optionally the tuned values) at a moment. It is
// written by a person in the freeze PR; the tool only reads it. Times are RFC 3339.
//
//	{"predictions_sha256": "...", "frozen_at": "2026-11-02T10:00:00Z",
//	 "tuned_values_sha256": "...", "tuned_values_frozen_at": "2026-11-05T10:00:00Z"}
//
// The last two are present only once a tuning pass has been done and its values frozen (Phase 14 D28 steps 4
// and 5).
type Freeze struct {
	PredictionsSHA256   string `json:"predictions_sha256"`
	FrozenAt            string `json:"frozen_at"`
	TunedValuesSHA256   string `json:"tuned_values_sha256,omitempty"`
	TunedValuesFrozenAt string `json:"tuned_values_frozen_at,omitempty"`
}

// Tuned is the tuned-values file: what the single tuning pass chose, and when.
//
//	{"tuned_at": "2026-11-04T18:00:00Z", "values": {"input_weight": 0.3}}
type Tuned struct {
	TunedAt string             `json:"tuned_at"`
	Values  map[string]float64 `json:"values"`
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func strictJSON(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("more than one JSON value")
	}
	return nil
}

func parseTime(name, s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q is not an RFC 3339 time", name, s)
	}
	return t, nil
}

// ParseFreeze reads and validates a freeze record.
func ParseFreeze(b []byte) (Freeze, error) {
	var f Freeze
	if err := strictJSON(b, &f); err != nil {
		return Freeze{}, fmt.Errorf("freeze record: %w", err)
	}
	if !hex64.MatchString(f.PredictionsSHA256) {
		return Freeze{}, errors.New("freeze record: predictions_sha256 must be 64 lowercase hex digits")
	}
	if _, err := parseTime("frozen_at", f.FrozenAt); err != nil {
		return Freeze{}, fmt.Errorf("freeze record: %w", err)
	}
	if (f.TunedValuesSHA256 == "") != (f.TunedValuesFrozenAt == "") {
		return Freeze{}, errors.New("freeze record: tuned_values_sha256 and tuned_values_frozen_at go together")
	}
	if f.TunedValuesSHA256 != "" {
		if !hex64.MatchString(f.TunedValuesSHA256) {
			return Freeze{}, errors.New("freeze record: tuned_values_sha256 must be 64 lowercase hex digits")
		}
		tf, err := parseTime("tuned_values_frozen_at", f.TunedValuesFrozenAt)
		if err != nil {
			return Freeze{}, fmt.Errorf("freeze record: %w", err)
		}
		pf, _ := parseTime("frozen_at", f.FrozenAt)
		if tf.Before(pf) {
			return Freeze{}, errors.New("freeze record: tuned_values_frozen_at is before frozen_at (tuning comes after the predictions freeze)")
		}
	}
	return f, nil
}

// CheckTuned validates a tuned-values file against the freeze record and returns it. It refuses (an error,
// no verdicts) when the freeze records no tuned-values freeze, when the file's bytes do not hash to the
// recorded value, when it says it was tuned after the tuned values were frozen, or when it was tuned before the
// predictions were frozen. The check cannot know which data the tuning used; that is the verifier's audit.
func CheckTuned(f Freeze, b []byte) (Tuned, error) {
	var t Tuned
	if err := strictJSON(b, &t); err != nil {
		return Tuned{}, fmt.Errorf("tuned values: %w", err)
	}
	if f.TunedValuesSHA256 == "" {
		return Tuned{}, errors.New("tuned values refused: the freeze record has no tuned-values freeze (tuned_values_sha256 and tuned_values_frozen_at)")
	}
	sum := sha256.Sum256(b)
	if got := hex.EncodeToString(sum[:]); got != f.TunedValuesSHA256 {
		return Tuned{}, fmt.Errorf("tuned values refused: the file hashes to %s but the freeze recorded %s", got, f.TunedValuesSHA256)
	}
	at, err := parseTime("tuned_at", t.TunedAt)
	if err != nil {
		return Tuned{}, fmt.Errorf("tuned values: %w", err)
	}
	frozen, _ := parseTime("tuned_values_frozen_at", f.TunedValuesFrozenAt)
	if at.After(frozen) {
		return Tuned{}, fmt.Errorf("tuned values refused: tuned_at %s is later than the tuned-values freeze %s", t.TunedAt, f.TunedValuesFrozenAt)
	}
	predictions, _ := parseTime("frozen_at", f.FrozenAt)
	if at.Before(predictions) {
		return Tuned{}, fmt.Errorf("tuned values refused: tuned_at %s is before the predictions freeze %s", t.TunedAt, f.FrozenAt)
	}
	for k, v := range t.Values {
		if !fin(v) {
			return Tuned{}, fmt.Errorf("tuned values: %q is not finite", k)
		}
	}
	return t, nil
}
