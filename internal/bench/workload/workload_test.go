package workload

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

var models = Models{Primary: "qwen-7b", Secondary: "llama-8b"}

func mk(t *testing.T, name string, seed int64) *Workload {
	t.Helper()
	w, err := New(Spec{Name: name, Seed: seed, Models: models, StreamRatio: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func plan(w *Workload, n int) []Request {
	out := make([]Request, n)
	for i := range out {
		out[i] = w.Request(i)
	}
	return out
}

func TestTheSameWorkloadAndSeedPlanTheSameRequests(t *testing.T) {
	for _, name := range Names() {
		a, b := plan(mk(t, name, 7), 500), plan(mk(t, name, 7), 500)
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("%s: request %d differs between identical workloads: %+v vs %+v", name, i, a[i], b[i])
			}
		}
		// Asking out of order, or again, changes nothing: each request depends only on its index.
		w := mk(t, name, 7)
		if w.Request(300) != a[300] || w.Request(3) != a[3] || w.Request(300) != a[300] {
			t.Fatalf("%s: a request must be a pure function of its index", name)
		}
		d1, d2 := mk(t, name, 7).Digest(500), mk(t, name, 7).Digest(500)
		if d1 != d2 {
			t.Fatalf("%s: digest unstable", name)
		}
	}
}

func TestDifferentSeedsPlanDifferentRequests(t *testing.T) {
	for _, name := range Names() {
		a, b := mk(t, name, 1), mk(t, name, 2)
		if a.Digest(200) == b.Digest(200) {
			t.Errorf("%s: seeds 1 and 2 planned the same load", name)
		}
	}
	if mk(t, Mixed, 1).Digest(100) == mk(t, UniformShort, 1).Digest(100) {
		t.Error("different workloads must have different digests")
	}
}

// A golden value pins the generator across Go versions and machines (math/rand/v2's PCG
// is specified to be stable), so a change to the planned load is a deliberate decision.
func TestPlannedLoadIsPinned(t *testing.T) {
	got := mk(t, Mixed, 1).Request(0)
	want := Request{Seq: 0, Model: "qwen-7b", Tenant: "default", Class: "short", InputTokens: 281, MaxTokens: 139, Stream: true}
	if got != want {
		t.Fatalf("request 0 of mixed seed 1 = %+v, pinned %+v", got, want)
	}
	if d := mk(t, Mixed, 1).Digest(1000); d != goldenMixedSeed1 {
		t.Fatalf("digest of mixed seed 1, 1000 requests = %s, pinned %s: the planned load changed", d, goldenMixedSeed1)
	}
}

const goldenMixedSeed1 = "d065b561cbd74cf2"

func TestRangesHoldForEveryWorkload(t *testing.T) {
	ranges := map[string][4]int{ // in min, in max, out min, out max
		"short": {100, 300, 50, 150}, "medium": {500, 1500, 200, 500}, "long": {2000, 8000, 500, 1500},
	}
	for _, name := range Names() {
		w := mk(t, name, 3)
		for _, r := range plan(w, 3000) {
			rg := ranges[r.Class]
			if r.InputTokens < rg[0] || r.InputTokens > rg[1] || r.MaxTokens < rg[2] || r.MaxTokens > rg[3] {
				t.Fatalf("%s: %+v outside the %s class %v", name, r, r.Class, rg)
			}
		}
	}
	// The spec's uniform workloads: short 100-300 in / 50-150 out, long 2k-8k in / 500-1500 out.
	for _, r := range plan(mk(t, UniformShort, 1), 500) {
		if r.Class != "short" {
			t.Fatal("uniform-short is all short")
		}
	}
	for _, r := range plan(mk(t, UniformLong, 1), 500) {
		if r.Class != "long" {
			t.Fatal("uniform-long is all long")
		}
	}
	// Both ends of a range are reachable.
	sawLo, sawHi := false, false
	for _, r := range plan(mk(t, UniformShort, 1), 20000) {
		sawLo = sawLo || r.MaxTokens == 50
		sawHi = sawHi || r.MaxTokens == 150
	}
	if !sawLo || !sawHi {
		t.Error("range ends must be inclusive and reachable")
	}
}

func share(rs []Request, pred func(Request) bool) float64 {
	n := 0
	for _, r := range rs {
		if pred(r) {
			n++
		}
	}
	return float64(n) / float64(len(rs))
}

func near(t *testing.T, what string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %.4f, want %.2f +/- %.2f", what, got, want, tol)
	}
}

func TestMixedSplitsSixtyThirtyTen(t *testing.T) {
	rs := plan(mk(t, Mixed, 1), 30000) // sd of a share is at most 0.003; the tolerance is 0.015
	near(t, "short", share(rs, func(r Request) bool { return r.Class == "short" }), 0.6, 0.015)
	near(t, "medium", share(rs, func(r Request) bool { return r.Class == "medium" }), 0.3, 0.015)
	near(t, "long", share(rs, func(r Request) bool { return r.Class == "long" }), 0.1, 0.015)
}

func TestHotModelSplitsNinetyTen(t *testing.T) {
	rs := plan(mk(t, HotModel, 1), 30000)
	near(t, "primary", share(rs, func(r Request) bool { return r.Model == "qwen-7b" }), 0.9, 0.015)
	near(t, "secondary", share(rs, func(r Request) bool { return r.Model == "llama-8b" }), 0.1, 0.015)
	for _, name := range []string{Mixed, UniformShort, Burst, MultiTenant} {
		for _, r := range plan(mk(t, name, 1), 500) {
			if r.Model != "qwen-7b" {
				t.Fatalf("%s uses the primary model only", name)
			}
		}
	}
}

func TestMultiTenantHasOneAggressiveClientAndFourNormalOnes(t *testing.T) {
	w := mk(t, MultiTenant, 1)
	if len(w.Tenants()) != 5 || w.Tenants()[0] != "tenant-aggressive" {
		t.Fatalf("%v", w.Tenants())
	}
	rs := plan(w, 30000)
	near(t, "aggressive share", share(rs, func(r Request) bool { return r.Tenant == "tenant-aggressive" }), 10.0/14, 0.015)
	for _, n := range []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4"} {
		near(t, n+" share", share(rs, func(r Request) bool { return r.Tenant == n }), 1.0/14, 0.015)
	}
	for _, r := range plan(mk(t, Mixed, 1), 100) {
		if r.Tenant != "default" {
			t.Fatal("single-tenant workloads use the default tenant")
		}
	}
	if APIKey("tenant-1") == APIKey("tenant-2") || !strings.HasPrefix(APIKey("x"), "sk-bench-") {
		t.Fatal("each tenant has its own key")
	}
}

func TestStreamRatio(t *testing.T) {
	for _, ratio := range []float64{0, 0.25, 1} {
		w, _ := New(Spec{Name: Mixed, Seed: 1, Models: models, StreamRatio: ratio})
		near(t, "stream share", share(plan(w, 20000), func(r Request) bool { return r.Stream }), ratio, 0.015)
	}
}

func TestBurstScheduleIsNormalThenTenTimesThenNormal(t *testing.T) {
	segs := Segments(Burst, 10, 3*time.Second, 30*time.Second)
	if len(segs) != 4 {
		t.Fatalf("%+v", segs)
	}
	want := []Segment{
		{0, 3 * time.Second, 10}, {3 * time.Second, 13 * time.Second, 10},
		{13 * time.Second, 23 * time.Second, 100}, {23 * time.Second, 33 * time.Second, 10},
	}
	for i, s := range segs {
		if s != want[i] {
			t.Errorf("segment %d = %+v, want %+v", i, s, want[i])
		}
	}
	// Arrivals per segment: 30, 100, 1000, 100.
	counts := make([]int, len(segs))
	total := int(TotalArrivals(segs))
	if total != 1230 {
		t.Fatalf("total %d", total)
	}
	for i := 0; i < total; i++ {
		at, ok := ArrivalTime(segs, i)
		if !ok {
			t.Fatalf("arrival %d missing", i)
		}
		for k, s := range segs {
			if at >= s.Start && at < s.End {
				counts[k]++
			}
		}
	}
	if counts[0] != 30 || counts[1] != 100 || counts[2] != 1000 || counts[3] != 100 {
		t.Fatalf("arrivals per segment: %v", counts)
	}
	if counts[2] != 10*counts[1] {
		t.Fatal("the middle third must arrive at ten times the normal rate")
	}
	if _, ok := ArrivalTime(segs, total); ok {
		t.Fatal("the schedule has exactly 1230 arrivals")
	}
}

func TestBurstWithoutWarmupHasThreeSegmentsAndOtherWorkloadsAreConstant(t *testing.T) {
	if got := Segments(Burst, 5, 0, 9*time.Second); len(got) != 3 || got[1].Rate != 50 || got[1].Start != 3*time.Second {
		t.Fatalf("%+v", got)
	}
	got := Segments(Mixed, 4, time.Second, 9*time.Second)
	if len(got) != 1 || got[0].Rate != 4 || got[0].End != 10*time.Second {
		t.Fatalf("%+v", got)
	}
}

func TestArrivalTimesAreEvenlySpacedMonotonicAndExact(t *testing.T) {
	segs := Segments(Mixed, 4, 0, 2*time.Second) // 8 requests, 250 ms apart
	for i := 0; i < 8; i++ {
		at, ok := ArrivalTime(segs, i)
		if !ok || at != time.Duration(i)*250*time.Millisecond {
			t.Fatalf("arrival %d = %v %v", i, at, ok)
		}
	}
	if _, ok := ArrivalTime(segs, 8); ok {
		t.Fatal("only 8")
	}
	if _, ok := ArrivalTime(segs, -1); ok {
		t.Fatal("negative index")
	}
	b := Segments(Burst, 7, 2*time.Second, 12*time.Second)
	prev := time.Duration(-1)
	for i := 0; ; i++ {
		at, ok := ArrivalTime(b, i)
		if !ok {
			break
		}
		if at < prev {
			t.Fatalf("arrival %d went backwards: %v < %v", i, at, prev)
		}
		prev = at
	}
}

func TestPromptSizeAndBody(t *testing.T) {
	p := Prompt(250, 9)
	if len(p) != 1000 || len(strings.Fields(p)) != 250 {
		t.Fatalf("len %d words %d", len(p), len(strings.Fields(p)))
	}
	if Prompt(10, 1) == Prompt(10, 2) {
		t.Fatal("prompts should differ between requests")
	}
	p1, p2 := Prompt(10, 5), Prompt(10, 5)
	if p1 != p2 {
		t.Fatal("prompt must be deterministic")
	}
	body, err := ChatBody(Request{Seq: 4, Model: "m", InputTokens: 5, MaxTokens: 77, Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Model     string `json:"model"`
		MaxTokens int    `json:"max_tokens"`
		Stream    bool   `json:"stream"`
		Messages  []struct{ Role, Content string }
	}
	if err := json.Unmarshal(body, &got); err != nil || got.Model != "m" || got.MaxTokens != 77 || !got.Stream || len(got.Messages) != 1 || got.Messages[0].Role != "user" || len(strings.Fields(got.Messages[0].Content)) != 5 {
		t.Fatalf("%s %v", body, err)
	}
}

func TestNewRejectsBadSpecs(t *testing.T) {
	for name, spec := range map[string]Spec{
		"unknown workload": {Name: "nope", Models: models},
		"no model":         {Name: Mixed},
		"hot without 2nd":  {Name: HotModel, Models: Models{Primary: "a"}},
		"hot same model":   {Name: HotModel, Models: Models{Primary: "a", Secondary: "a"}},
		"ratio above one":  {Name: Mixed, Models: models, StreamRatio: 1.5},
		"ratio negative":   {Name: Mixed, Models: models, StreamRatio: -0.1},
		"ratio NaN":        {Name: Mixed, Models: models, StreamRatio: math.NaN()},
	} {
		if _, err := New(spec); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := New(Spec{Name: "nope", Models: models}); err == nil || !strings.Contains(err.Error(), "uniform-short") {
		t.Errorf("the error should list the valid workloads: %v", err)
	}
}
