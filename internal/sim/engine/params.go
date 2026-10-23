package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// SchemaVersion is the parameter-file format version. It is part of the hashed bytes.
const SchemaVersion = 1

// maxParamsFileBytes bounds a parameter file read by LoadParams.
const maxParamsFileBytes = 64 << 10

// Params is the physics of one simulated worker: how long a decode step and a prefill take, how large
// the KV-cache budget is, how noisy the timings are and how long outputs run. It is a plain value with a
// stable JSON form (a "parameter file"); Hash identifies it so a result can say which world it came from.
//
// Every number is an assumption about an engine, not a measurement of one, unless Provenance says it was
// fitted to data. See docs/development/mock-worker.md for what the model can and cannot show.
type Params struct {
	Name       string `json:"name"`
	Provenance string `json:"provenance"`

	// Decode: a continuous-batching step with b sequences takes StepBaseMs + StepPerSeqMs*b.
	StepBaseMs   float64 `json:"step_base_ms"`
	StepPerSeqMs float64 `json:"step_per_seq_ms"`
	StepNoiseCV  float64 `json:"step_noise_cv"`

	// Prefill: processing n prompt tokens takes PrefillFixedMs (once per prefill) + PrefillPerTokenMs*n.
	// PrefillChunkTokens 0 runs a prompt in one blocking step (the batch's decoding stalls meanwhile);
	// N > 0 splits it into chunks of N tokens that share a step with the batch's decode.
	PrefillFixedMs     float64 `json:"prefill_fixed_ms"`
	PrefillPerTokenMs  float64 `json:"prefill_per_token_ms"`
	PrefillNoiseCV     float64 `json:"prefill_noise_cv"`
	PrefillChunkTokens int     `json:"prefill_chunk_tokens"`

	// KV cache: a token budget shared by all running sequences, and what one token costs in memory
	// (used only to report gpu_memory_used_mb together with WeightsMB).
	KVBudgetTokens  int   `json:"kv_budget_tokens"`
	KVBytesPerToken int64 `json:"kv_bytes_per_token"`
	WeightsMB       int64 `json:"weights_mb"`

	// Output length. With OutputFraction 0 a request's natural length is lognormal around the engine's
	// mean (Config.OutputMeanTokens) with coefficient of variation OutputCV, independent of max_tokens;
	// with OutputFraction f > 0 and a max_tokens given it is f*max_tokens times the same noise factor,
	// so the estimate "max_tokens" can be made wrong by a known amount.
	OutputCV       float64 `json:"output_cv"`
	OutputFraction float64 `json:"output_fraction"`
}

// Limits that keep a typo from producing an engine that cannot make progress or exhausts memory.
const (
	maxStepMs       = 60_000.0
	maxKVBudget     = 1 << 26
	maxKVBytesToken = 1 << 30
	maxWeightsMB    = 1 << 30
	maxCV           = 2.0
	maxChunk        = 1 << 20
)

func okMs(v float64) bool { return !math.IsNaN(v) && v >= 0 && v <= maxStepMs }

// Validate returns an error describing the first invalid value.
func (p Params) Validate() error {
	switch {
	case !okText(p.Name, false):
		return fmt.Errorf("name must be 1-%d printable characters", maxTextLen)
	case !okText(p.Provenance, false):
		return fmt.Errorf("provenance must be 1-%d printable characters (say where the numbers come from, or \"assumed\")", maxTextLen)
	case !okMs(p.StepBaseMs) || !okMs(p.StepPerSeqMs):
		return fmt.Errorf("step_base_ms and step_per_seq_ms must be in [0, %v], got %v and %v", maxStepMs, p.StepBaseMs, p.StepPerSeqMs)
	case p.StepBaseMs+p.StepPerSeqMs <= 0:
		return errors.New("a decode step must take time: step_base_ms + step_per_seq_ms must be > 0")
	case !okMs(p.PrefillFixedMs) || !okMs(p.PrefillPerTokenMs):
		return fmt.Errorf("prefill_fixed_ms and prefill_per_token_ms must be in [0, %v], got %v and %v", maxStepMs, p.PrefillFixedMs, p.PrefillPerTokenMs)
	case !okCV(p.StepNoiseCV) || !okCV(p.PrefillNoiseCV) || !okCV(p.OutputCV):
		return fmt.Errorf("noise coefficients of variation must be in [0, %v]", maxCV)
	case p.PrefillChunkTokens < 0 || p.PrefillChunkTokens > maxChunk:
		return fmt.Errorf("prefill_chunk_tokens must be in 0-%d, got %d", maxChunk, p.PrefillChunkTokens)
	case p.KVBudgetTokens < 2 || p.KVBudgetTokens > maxKVBudget:
		return fmt.Errorf("kv_budget_tokens must be in 2-%d, got %d", maxKVBudget, p.KVBudgetTokens)
	case p.KVBytesPerToken < 0 || p.KVBytesPerToken > maxKVBytesToken:
		return fmt.Errorf("kv_bytes_per_token must be in 0-%d, got %d", maxKVBytesToken, p.KVBytesPerToken)
	case p.WeightsMB < 0 || p.WeightsMB > maxWeightsMB:
		return fmt.Errorf("weights_mb must be in 0-%d, got %d", maxWeightsMB, p.WeightsMB)
	case math.IsNaN(p.OutputFraction) || p.OutputFraction < 0 || p.OutputFraction > 1:
		return fmt.Errorf("output_fraction must be in [0,1], got %v", p.OutputFraction)
	}
	return nil
}

// maxTextLen bounds name and provenance, which are echoed into every /stats response and log line.
const maxTextLen = 256

func okText(s string, allowEmpty bool) bool {
	if (s == "" && !allowEmpty) || len(s) > maxTextLen {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f || r == utf8.RuneError {
			return false
		}
	}
	return true
}

func okCV(v float64) bool { return !math.IsNaN(v) && v >= 0 && v <= maxCV }

// paramsFile is the on-disk form: the parameters plus the schema version.
type paramsFile struct {
	Version int `json:"version"`
	Params
}

// MarshalFile returns the canonical parameter-file bytes (field order fixed by the struct).
func (p Params) MarshalFile() ([]byte, error) {
	b, err := json.MarshalIndent(paramsFile{Version: SchemaVersion, Params: p}, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Hash is the SHA-256 (hex) of the canonical compact JSON of the validated parameters and the schema
// version. Two parameter sets have the same hash exactly when every field is equal, whatever the
// whitespace or key order of the file they came from.
func (p Params) Hash() string {
	b, err := json.Marshal(paramsFile{Version: SchemaVersion, Params: p.normalized()})
	if err != nil { // unreachable: only numbers and strings
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// normalized folds negative zero into zero, the one case where two equal numbers marshal differently.
func (p Params) normalized() Params {
	for _, f := range []*float64{&p.StepBaseMs, &p.StepPerSeqMs, &p.StepNoiseCV, &p.PrefillFixedMs, &p.PrefillPerTokenMs,
		&p.PrefillNoiseCV, &p.OutputCV, &p.OutputFraction} {
		*f += 0 // -0 + +0 is +0
	}
	return p
}

// fileKeys are the exact keys a parameter file must contain, no more and no fewer.
var fileKeys = func() map[string]bool {
	keys := map[string]bool{}
	t := reflect.TypeOf(paramsFile{})
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous {
			for j := range f.Type.NumField() {
				keys[strings.Split(f.Type.Field(j).Tag.Get("json"), ",")[0]] = true
			}
			continue
		}
		keys[strings.Split(f.Tag.Get("json"), ",")[0]] = true
	}
	return keys
}()

// checkKeys walks the top-level object and rejects what encoding/json would let through: duplicate
// keys (last wins), keys that match a field only case-insensitively, unknown keys, null values and
// missing keys.
func checkKeys(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return errors.New("must be a JSON object")
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key, _ := tok.(string)
		switch {
		case !fileKeys[key]:
			return fmt.Errorf("unknown key %q (keys are exact and case-sensitive)", key)
		case seen[key]:
			return fmt.Errorf("duplicate key %q", key)
		}
		seen[key] = true
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fmt.Errorf("key %q is null", key)
		}
	}
	for k := range fileKeys {
		if !seen[k] {
			return fmt.Errorf("missing key %q (every field must be given)", k)
		}
	}
	return nil
}

// ParseParams decodes and validates a parameter file. Unknown fields, a wrong version, trailing data and
// invalid values are errors, so a typo cannot silently fall back to a default.
func ParseParams(r io.Reader) (Params, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxParamsFileBytes+1))
	if err != nil {
		return Params{}, err
	}
	if len(raw) > maxParamsFileBytes {
		return Params{}, fmt.Errorf("parameter file is larger than %d bytes", maxParamsFileBytes)
	}
	if err := checkKeys(raw); err != nil {
		return Params{}, fmt.Errorf("parameter file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var f paramsFile
	if err := dec.Decode(&f); err != nil {
		return Params{}, fmt.Errorf("parameter file: %w", err)
	}
	if dec.More() {
		return Params{}, errors.New("parameter file: unexpected data after the JSON object")
	}
	if f.Version != SchemaVersion {
		return Params{}, fmt.Errorf("parameter file: version %d is not supported (want %d)", f.Version, SchemaVersion)
	}
	if err := f.Validate(); err != nil {
		return Params{}, fmt.Errorf("parameter file: %w", err)
	}
	return f.normalized(), nil
}

const assumed = "assumed: order-of-magnitude shape of a 7-8B model on one datacenter GPU; not fitted to any measurement"

// profiles are the built-in worlds. All are "assumed" until a calibration run replaces them.
var profiles = map[string]Params{
	"default": {
		Name: "default", Provenance: assumed,
		StepBaseMs: 20, StepPerSeqMs: 1.0,
		PrefillFixedMs: 5, PrefillPerTokenMs: 0.1,
		KVBudgetTokens: 32768, KVBytesPerToken: 524288, WeightsMB: 14000,
	},
	// fast, medium and slow are three hardware generations of the same model: the heterogeneous fleet.
	"fast": {
		Name: "fast", Provenance: assumed,
		StepBaseMs: 10, StepPerSeqMs: 0.5,
		PrefillFixedMs: 3, PrefillPerTokenMs: 0.05,
		KVBudgetTokens: 65536, KVBytesPerToken: 524288, WeightsMB: 14000,
	},
	"medium": {
		Name: "medium", Provenance: assumed,
		StepBaseMs: 20, StepPerSeqMs: 1.0,
		PrefillFixedMs: 5, PrefillPerTokenMs: 0.1,
		KVBudgetTokens: 32768, KVBytesPerToken: 524288, WeightsMB: 14000,
	},
	"slow": {
		Name: "slow", Provenance: assumed,
		StepBaseMs: 40, StepPerSeqMs: 2.0,
		PrefillFixedMs: 10, PrefillPerTokenMs: 0.2,
		KVBudgetTokens: 16384, KVBytesPerToken: 524288, WeightsMB: 14000,
	},
	// kv-tight is "default" with a KV budget small enough that long requests queue before the
	// concurrency cap is reached.
	"kv-tight": {
		Name: "kv-tight", Provenance: assumed,
		StepBaseMs: 20, StepPerSeqMs: 1.0,
		PrefillFixedMs: 5, PrefillPerTokenMs: 0.1,
		KVBudgetTokens: 6144, KVBytesPerToken: 524288, WeightsMB: 14000,
	},
	// noisy is "default" with timing noise (coefficient of variation 0.3) on decode steps and prefill.
	"noisy": {
		Name: "noisy", Provenance: assumed,
		StepBaseMs: 20, StepPerSeqMs: 1.0, StepNoiseCV: 0.3,
		PrefillFixedMs: 5, PrefillPerTokenMs: 0.1, PrefillNoiseCV: 0.3,
		KVBudgetTokens: 32768, KVBytesPerToken: 524288, WeightsMB: 14000,
	},
}

// Profile returns the built-in parameter set called name.
func Profile(name string) (Params, bool) {
	p, ok := profiles[name]
	return p, ok
}

// ProfileNames lists the built-in profiles in alphabetical order.
func ProfileNames() []string {
	names := make([]string, 0, len(profiles))
	for n := range profiles {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ms converts milliseconds to a Duration, saturating instead of overflowing.
func ms(v float64) time.Duration {
	ns := math.Round(v * float64(time.Millisecond))
	if ns >= float64(maxIteration) {
		return maxIteration
	}
	return time.Duration(ns)
}
