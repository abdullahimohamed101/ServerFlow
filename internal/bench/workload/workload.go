// Package workload generates the benchmark request streams of spec section 33. Every
// request is a pure function of the workload, the seed and the request's index, so two
// runs with the same seed offer the same load regardless of timing, concurrency or
// machine. Nothing here touches the network.
package workload

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Workload names (spec section 33).
const (
	UniformShort = "uniform-short"
	UniformLong  = "uniform-long"
	Mixed        = "mixed"
	Burst        = "burst"
	HotModel     = "hot-model"
	MultiTenant  = "multi-tenant"
)

// Names lists the workloads.
func Names() []string {
	return []string{UniformShort, UniformLong, Mixed, Burst, HotModel, MultiTenant}
}

// Class is one slice of a workload's prompt distribution: input and max_tokens are
// uniform within their ranges, inclusive.
type Class struct {
	Name           string
	Weight         float64
	InMin, InMax   int
	OutMin, OutMax int
}

// The size classes. Short and long are the spec's; the spec gives no medium, so it sits
// between them.
var (
	short  = Class{Name: "short", InMin: 100, InMax: 300, OutMin: 50, OutMax: 150}
	medium = Class{Name: "medium", InMin: 500, InMax: 1500, OutMin: 200, OutMax: 500}
	long   = Class{Name: "long", InMin: 2000, InMax: 8000, OutMin: 500, OutMax: 1500}
)

func weighted(c Class, w float64) Class { c.Weight = w; return c }

// Models names the models a workload asks for. Secondary is used by hot-model only.
type Models struct{ Primary, Secondary string }

// Spec configures a workload.
type Spec struct {
	Name   string
	Seed   int64
	Models Models
	// StreamRatio is the fraction of requests that stream, in [0, 1].
	StreamRatio float64
}

// Request is one planned request.
type Request struct {
	Seq         int
	Model       string
	Tenant      string
	Class       string
	InputTokens int
	MaxTokens   int
	Stream      bool
}

// Tenant weights for multi-tenant: one aggressive client sends ten times what each
// of the four normal clients does (10 of 14, about 71%, of all requests).
const (
	aggressiveTenant = "tenant-aggressive"
	aggressiveWeight = 10.0
	normalTenants    = 4
	// HotModelShare is the fraction of hot-model requests that ask for the primary model.
	HotModelShare = 0.9
)

// Workload generates requests for a Spec.
type Workload struct {
	spec    Spec
	classes []Class
	tenants []string
	tenantW []float64
}

// New validates spec and returns its workload.
func New(spec Spec) (*Workload, error) {
	w := &Workload{spec: spec, tenants: []string{"default"}, tenantW: []float64{1}}
	switch spec.Name {
	case UniformShort, Burst, HotModel, MultiTenant:
		w.classes = []Class{weighted(short, 1)}
	case UniformLong:
		w.classes = []Class{weighted(long, 1)}
	case Mixed:
		w.classes = []Class{weighted(short, 0.6), weighted(medium, 0.3), weighted(long, 0.1)}
	default:
		return nil, fmt.Errorf("unknown workload %q (choose one of: %s)", spec.Name, strings.Join(Names(), ", "))
	}
	if spec.Models.Primary == "" {
		return nil, fmt.Errorf("a model name is required")
	}
	if spec.Name == HotModel && (spec.Models.Secondary == "" || spec.Models.Secondary == spec.Models.Primary) {
		return nil, fmt.Errorf("hot-model needs a second, different model")
	}
	if !(spec.StreamRatio >= 0 && spec.StreamRatio <= 1) {
		return nil, fmt.Errorf("stream ratio must be in [0, 1], got %v", spec.StreamRatio)
	}
	if spec.Name == MultiTenant {
		w.tenants, w.tenantW = []string{aggressiveTenant}, []float64{aggressiveWeight}
		for i := 1; i <= normalTenants; i++ {
			w.tenants = append(w.tenants, fmt.Sprintf("tenant-%d", i))
			w.tenantW = append(w.tenantW, 1)
		}
	}
	return w, nil
}

// Spec returns the workload's configuration.
func (w *Workload) Spec() Spec { return w.spec }

// Classes returns the prompt distribution.
func (w *Workload) Classes() []Class { return append([]Class(nil), w.classes...) }

// Tenants returns the tenant names the workload uses.
func (w *Workload) Tenants() []string { return append([]string(nil), w.tenants...) }

// ModelNames returns the models the workload asks for.
func (w *Workload) ModelNames() []string {
	if w.spec.Name == HotModel {
		return []string{w.spec.Models.Primary, w.spec.Models.Secondary}
	}
	return []string{w.spec.Models.Primary}
}

// APIKey is the fake API key sent for a tenant. It is derived from the tenant name, is
// not a secret, and is never written to logs or results.
func APIKey(tenant string) string { return "sk-bench-" + tenant }

// Request returns request i (from 0). It is a pure function of the spec and i.
func (w *Workload) Request(i int) Request {
	// A separate stream per request index: the value drawn for request i does not depend on
	// how many requests were generated before it or on which client asks.
	rng := rand.New(rand.NewPCG(uint64(w.spec.Seed), uint64(i)^0x9e3779b97f4a7c15))
	c := w.classes[pick(rng.Float64(), classWeights(w.classes))]
	model := w.spec.Models.Primary
	if w.spec.Name == HotModel && rng.Float64() >= HotModelShare {
		model = w.spec.Models.Secondary
	}
	tenant := w.tenants[pick(rng.Float64(), w.tenantW)]
	return Request{
		Seq: i, Model: model, Tenant: tenant, Class: c.Name,
		InputTokens: between(rng, c.InMin, c.InMax),
		MaxTokens:   between(rng, c.OutMin, c.OutMax),
		Stream:      rng.Float64() < w.spec.StreamRatio,
	}
}

func classWeights(cs []Class) []float64 {
	ws := make([]float64, len(cs))
	for i, c := range cs {
		ws[i] = c.Weight
	}
	return ws
}

// pick maps u in [0, 1) to an index with probability proportional to weights.
func pick(u float64, weights []float64) int {
	var total float64
	for _, w := range weights {
		total += w
	}
	target := u * total
	var cum float64
	for i, w := range weights {
		cum += w
		if target < cum {
			return i
		}
	}
	return len(weights) - 1
}

func between(rng *rand.Rand, lo, hi int) int { return lo + rng.IntN(hi-lo+1) }

// Digest fingerprints the first n requests, so a result can record which load it offered
// and two runs can be checked to have offered the same.
func (w *Workload) Digest(n int) string {
	h := sha256.New()
	for i := range n {
		r := w.Request(i)
		_, _ = fmt.Fprintf(h, "%d|%s|%s|%s|%d|%d|%t\n", r.Seq, r.Model, r.Tenant, r.Class, r.InputTokens, r.MaxTokens, r.Stream)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Segment is a stretch of an open-loop schedule with a constant arrival rate.
type Segment struct {
	Start, End time.Duration
	Rate       float64
}

// BurstFactor is how much faster than normal the burst workload's middle third arrives.
const BurstFactor = 10

// Segments returns the arrival schedule of an open-loop run: the warm-up and the
// measurement window at rate per second, except that the burst workload runs the middle
// third of the window at BurstFactor times that rate (normal, then a sudden 10x, then
// normal again).
func Segments(name string, rate float64, warmup, duration time.Duration) []Segment {
	if name != Burst {
		return []Segment{{Start: 0, End: warmup + duration, Rate: rate}}
	}
	third := duration / 3
	segs := []Segment{
		{Start: warmup, End: warmup + third, Rate: rate},
		{Start: warmup + third, End: warmup + 2*third, Rate: rate * BurstFactor},
		{Start: warmup + 2*third, End: warmup + duration, Rate: rate},
	}
	if warmup > 0 {
		segs = append([]Segment{{Start: 0, End: warmup, Rate: rate}}, segs...)
	}
	return segs
}

// TotalArrivals is the number of requests a schedule offers, as a float.
func TotalArrivals(segs []Segment) float64 {
	var n float64
	for _, s := range segs {
		n += s.Rate * (s.End - s.Start).Seconds()
	}
	return n
}

// ArrivalTime returns when request i is due, as an offset from the start of the run, and
// false when the schedule offers fewer than i+1 requests. Arrivals are evenly spaced within
// a segment (deterministic, not Poisson).
func ArrivalTime(segs []Segment, i int) (time.Duration, bool) {
	if i < 0 {
		return 0, false
	}
	before := 0.0
	for _, s := range segs {
		n := s.Rate * (s.End - s.Start).Seconds()
		if float64(i) < before+n {
			off := (float64(i) - before) / s.Rate
			return s.Start + time.Duration(off*float64(time.Second)), true
		}
		before += n
	}
	return 0, false
}

// Prompt returns synthetic text of tokens words, each three letters and a space (four
// characters, so about one token each by the usual rule of thumb, and exactly one by the
// mock worker's whitespace count). It varies with seq so requests are not identical.
func Prompt(tokens, seq int) string {
	var b strings.Builder
	b.Grow(tokens * 4)
	for j := range tokens {
		x := (seq*7919 + j*31 + j/7) % 17576
		b.WriteByte(byte('a' + x%26))
		b.WriteByte(byte('a' + x/26%26))
		b.WriteByte(byte('a' + x/676))
		b.WriteByte(' ')
	}
	return b.String()
}

// ChatBody returns the JSON body of the chat completion request for r.
func ChatBody(r Request) ([]byte, error) {
	return json.Marshal(map[string]any{
		"model":      r.Model,
		"messages":   []map[string]string{{"role": "user", "content": Prompt(r.InputTokens, r.Seq)}},
		"max_tokens": r.MaxTokens,
		"stream":     r.Stream,
	})
}
