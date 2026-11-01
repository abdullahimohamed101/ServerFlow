package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func simCluster() *Cluster {
	return &Cluster{Profile: "identical", HeartbeatMillis: 1000, RegistryRefreshMs: 100, Workers: []ClusterWorker{
		{ID: "w-1", Model: "m", TokensPerSecond: 1000, TTFTMillis: 20, MaxConcurrency: 8, QueueSize: 128, SimProfile: "fast", SimParamsHash: "aaaa"},
		{ID: "w-2", Model: "m", TokensPerSecond: 1000, TTFTMillis: 20, MaxConcurrency: 8, QueueSize: 128, SimProfile: "slow", SimParamsHash: "bbbb"},
	}}
}

func opts() *SchedulerOptions {
	return &SchedulerOptions{LoadOverlay: "ledger", OutputFraction: 1, InputWeight: 1, CapacityHalflifeMs: 60000, CapacityFloor: 0.25}
}

func expResult(id, strategy string, c *Cluster, o *SchedulerOptions) Result {
	r := result(id, 10, 100, 0, 0.9, 0)
	r.Metadata.Scheduler = strategy
	r.Metadata.Cluster = c
	r.Metadata.Experiment = &Experiment{Strategy: strategy, World: WorldLabel(c), ParameterHash: ParameterHash(c), Options: o}
	return r
}

func TestExperimentIsAdditiveInTheResultFile(t *testing.T) {
	// A result without the record serialises without the key, byte for byte as before, and parses back to nil.
	r := result("run_001", 10, 100, 0, 0.9, 0)
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte(`"experiment"`)) || bytes.Contains(b, []byte(`"sim_profile"`)) || bytes.Contains(b, []byte(`"sim_params_hash"`)) {
		t.Fatalf("a result without an experiment record gained keys: %s", b)
	}
	var back Result
	if err := json.Unmarshal(b, &back); err != nil || back.Metadata.Experiment != nil {
		t.Fatalf("%v %+v", err, back.Metadata.Experiment)
	}
	// A result written before the field existed (here: the old JSON with no such key) parses.
	var old Result
	if err := json.Unmarshal([]byte(`{"schema_version":1,"run_id":"run_009","metadata":{"scheduler":"round-robin","workload":"mixed","cluster":{"worker_profile":"identical","workers":[{"id":"w","model":"m","tokens_per_second":1,"ttft_ms":1,"max_concurrency":1,"queue_size":1}]}},"valid":true}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Metadata.Experiment != nil || old.Metadata.Cluster.Workers[0].SimProfile != "" {
		t.Fatalf("%+v", old.Metadata)
	}
	// New fields round-trip.
	n := expResult("run_002", "least-work", simCluster(), opts())
	b, _ = json.Marshal(n)
	var got Result
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	e := got.Metadata.Experiment
	if e == nil || e.Strategy != "least-work" || e.World != "sim:fast,slow" || e.ParameterHash != ParameterHash(simCluster()) || *e.Options != *opts() {
		t.Fatalf("%+v", e)
	}
	if w := got.Metadata.Cluster.Workers[1]; w.SimProfile != "slow" || w.SimParamsHash != "bbbb" {
		t.Fatalf("%+v", w)
	}
	for _, k := range []string{`"strategy"`, `"world"`, `"parameter_hash"`, `"options"`, `"load_overlay"`, `"output_fraction"`, `"input_weight"`, `"capacity_halflife_ms"`, `"capacity_floor"`} {
		if !strings.Contains(string(b), k) {
			t.Errorf("stored key %s missing", k)
		}
	}
}

func TestParameterHashIdentifiesTheWorldNotTheStrategyOrWorkerIDs(t *testing.T) {
	base := ParameterHash(simCluster())
	if len(base) != 64 || base != ParameterHash(simCluster()) {
		t.Fatalf("hash must be a stable sha256: %q", base)
	}
	if ParameterHash(nil) != "" {
		t.Error("no cluster, no hash")
	}
	renamed := simCluster()
	renamed.Workers[0].ID = "other"
	if ParameterHash(renamed) != base {
		t.Error("worker IDs are not part of the world")
	}
	each := map[string]func(c *Cluster){
		"profile":   func(c *Cluster) { c.Profile = "heterogeneous" },
		"heartbeat": func(c *Cluster) { c.HeartbeatMillis++ },
		"refresh":   func(c *Cluster) { c.RegistryRefreshMs++ },
		"model":     func(c *Cluster) { c.Workers[0].Model = "n" },
		"tps":       func(c *Cluster) { c.Workers[1].TokensPerSecond++ },
		"ttft":      func(c *Cluster) { c.Workers[0].TTFTMillis++ },
		"conc":      func(c *Cluster) { c.Workers[0].MaxConcurrency++ },
		"queue":     func(c *Cluster) { c.Workers[1].QueueSize++ },
		"simname":   func(c *Cluster) { c.Workers[0].SimProfile = "medium" },
		"simhash":   func(c *Cluster) { c.Workers[1].SimParamsHash = "cccc" },
		"count":     func(c *Cluster) { c.Workers = c.Workers[:1] },
		"order":     func(c *Cluster) { c.Workers[0], c.Workers[1] = c.Workers[1], c.Workers[0] },
	}
	for name, f := range each {
		c := simCluster()
		f(c)
		if ParameterHash(c) == base {
			t.Errorf("changing %s left the hash unchanged", name)
		}
	}
}

func TestWorldLabel(t *testing.T) {
	if WorldLabel(nil) != "remote" {
		t.Error("remote")
	}
	if got := WorldLabel(&Cluster{Profile: "heterogeneous"}); got != "mock:heterogeneous" {
		t.Error(got)
	}
	c := simCluster()
	c.Workers = append(c.Workers, ClusterWorker{SimProfile: "fast"}, ClusterWorker{SimProfile: "slow"})
	if got := WorldLabel(c); got != "sim:fast,slow" {
		t.Errorf("distinct profiles in first-use order: %s", got)
	}
}

func warned(c Comparison, sub string) bool {
	for _, w := range c.Warnings {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}

func hasDiff(c Comparison, field string) (Difference, bool) {
	for _, d := range c.Differences {
		if d.Field == field {
			return d, true
		}
	}
	return Difference{}, false
}

func TestCompareWarnsWhenWorldParametersOrOptionsDiffer(t *testing.T) {
	a := expResult("run_001", "least-active", simCluster(), opts())
	// Same world, same options, different strategy: nothing to warn about.
	b := expResult("run_002", "least-work", simCluster(), opts())
	c := Compare(a, b)
	if len(c.Warnings) != 0 || c.Unfair() {
		t.Fatalf("a strategy comparison in one world is fair: %v %v", c.Warnings, c.Differences)
	}
	// Different world and hash.
	other := simCluster()
	other.Workers[1].SimProfile, other.Workers[1].SimParamsHash = "medium", "dddd"
	c = Compare(a, expResult("run_003", "least-work", other, opts()))
	if !warned(c, "different worlds") || !warned(c, "different parameter hashes") || !c.Unfair() {
		t.Fatalf("%v", c.Warnings)
	}
	// Same world label, different parameters only.
	tweak := simCluster()
	tweak.Workers[1].SimParamsHash = "eeee"
	c = Compare(a, expResult("run_004", "least-work", tweak, opts()))
	if warned(c, "different worlds") || !warned(c, "different parameter hashes") {
		t.Fatalf("%v", c.Warnings)
	}
	// Different options.
	o2 := opts()
	o2.InputWeight = 0.3
	c = Compare(a, expResult("run_005", "least-work", simCluster(), o2))
	if !warned(c, "different scheduler options") || !c.Unfair() {
		t.Fatalf("%v", c.Warnings)
	}
	if d, ok := hasDiff(c, "scheduler options"); !ok || d.Expected {
		t.Fatalf("%+v", d)
	}
	// The overlay differs because the strategies need different ones: expected. With the same strategy: a warning.
	o3 := opts()
	o3.LoadOverlay = "max"
	c = Compare(a, expResult("run_006", "least-work", simCluster(), o3))
	if d, ok := hasDiff(c, "load overlay"); !ok || !d.Expected || warned(c, "load overlays") {
		t.Fatalf("%+v %v", d, c.Warnings)
	}
	c = Compare(a, expResult("run_007", "least-active", simCluster(), o3))
	if d, ok := hasDiff(c, "load overlay"); !ok || d.Expected || !warned(c, "different load overlays") || !c.Unfair() {
		t.Fatalf("%+v %v", d, c.Warnings)
	}
}

func TestCompareIgnoresExperimentRecordsOfOldResults(t *testing.T) {
	a := expResult("run_001", "least-active", simCluster(), opts())
	old := result("run_002", 10, 100, 0, 0.9, 0)
	old.Metadata.Scheduler = "least-work"
	c := Compare(a, old)
	if warned(c, "world") || warned(c, "parameter") {
		t.Fatalf("%v", c.Warnings)
	}
	for _, f := range []string{"world", "parameter hash", "load overlay", "scheduler options"} {
		if _, ok := hasDiff(c, f); ok {
			t.Errorf("an old result has no %s to differ in", f)
		}
	}
	// A remote result (options unknown) against an embedded one: worlds differ, options are not compared.
	remote := expResult("run_003", "least-work", nil, nil)
	remote.Metadata.Experiment.World = "remote"
	c = Compare(a, remote)
	if !warned(c, "different worlds") || warned(c, "scheduler options") {
		t.Fatalf("%v", c.Warnings)
	}
}

func TestCompareWarnsWhenARepeatGroupMixesParameterHashes(t *testing.T) {
	a := expResult("run_001", "least-active", simCluster(), opts())
	odd := simCluster()
	odd.Workers[0].SimParamsHash = "zzzz"
	ga := []Result{a, expResult("run_002", "least-active", odd, opts()), expResult("run_003", "least-active", simCluster(), opts())}
	b := expResult("run_004", "least-work", simCluster(), opts())
	c := CompareGroups(a, b, ga, []Result{b, b, b})
	if !warned(c, "repeat group of side A mixes 2 different parameter hashes") || warned(c, "side B") {
		t.Fatalf("%v", c.Warnings)
	}
}

func TestComparisonTextShowsTheExperimentWarnings(t *testing.T) {
	a := expResult("run_001", "least-active", simCluster(), opts())
	other := simCluster()
	other.Workers[0].SimProfile = "medium"
	var out bytes.Buffer
	if err := Compare(a, expResult("run_002", "least-work", other, opts())).Write(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"WARNING: run_001 and run_002 ran in different worlds", "! world: sim:fast,slow -> sim:medium,slow", "! parameter hash:"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}

func TestMarkdownReportShowsTheWorldOnlyWhenRecorded(t *testing.T) {
	r := expResult("run_001", "least-work", simCluster(), opts())
	md := Markdown(r)
	for _, want := range []string{"| World | sim:fast,slow (parameters `" + ParameterHash(simCluster())[:12] + "`) |", "| Scheduler options | overlay=ledger output_fraction=1 input_weight=1"} {
		if !strings.Contains(md, want) {
			t.Errorf("missing %q", want)
		}
	}
	if old := Markdown(result("run_002", 10, 100, 0, 0.9, 0)); strings.Contains(old, "| World |") {
		t.Error("an old result has no world row")
	}
}

// The parameter hash is a stored format: results written today are compared with results written later. Changing
// what it covers or how it is encoded must be a decision, so one value is pinned.
func TestParameterHashIsAStoredFormat(t *testing.T) {
	const want = "0704596c8db0ccb5b11a620f4fa47145bb1e1ad183546b649c479248c3083067"
	if got := ParameterHash(simCluster()); got != want {
		t.Fatalf("ParameterHash(simCluster()) = %s, pinned %s", got, want)
	}
}

func TestUpstreamHeaderTimeoutIsAdditiveAndCompared(t *testing.T) {
	// Unset: not serialised, and the label is unchanged.
	b, _ := json.Marshal(opts())
	if strings.Contains(string(b), "upstream_header_timeout") || strings.Contains(opts().Label(), "upstream_header_timeout") {
		t.Fatalf("default must be invisible: %s %s", b, opts().Label())
	}
	long := opts()
	long.UpstreamHeaderTimeoutMs = 120000
	b, _ = json.Marshal(long)
	if !strings.Contains(string(b), `"upstream_header_timeout_ms":120000`) || !strings.Contains(long.Label(), "upstream_header_timeout=120000ms") {
		t.Fatalf("%s %s", b, long.Label())
	}
	a := expResult("run_001", "least-work", simCluster(), opts())
	c := Compare(a, expResult("run_002", "least-work", simCluster(), long))
	if !warned(c, "different scheduler options") || !c.Unfair() {
		t.Fatalf("%v", c.Warnings)
	}
	if _, ok := hasDiff(c, "scheduler options"); !ok {
		t.Fatal("difference not listed")
	}
	c = Compare(expResult("run_003", "least-work", simCluster(), long), expResult("run_004", "least-work", simCluster(), long))
	if warned(c, "different scheduler options") {
		t.Fatalf("%v", c.Warnings)
	}
}
