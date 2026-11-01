package report

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Experiment records what the Phase 14 experiments need to know about a run beyond what Metadata has
// always held: which scheduling strategy and settings the gateway ran with, in which world, and a hash of
// that world's parameters, so two results can be checked to be comparable without trusting a label
// (ADR-022, PR G-lite). It is additive: results written before it existed have no "experiment" key and
// parse exactly as before, and a result without one is never reported as differing from one with it.
//
// A result of a simulated world is a simulator result: simulator-validated, not hardware-validated.
type Experiment struct {
	// Strategy is the scheduling strategy; for an embedded cluster it is the one the gateway ran with,
	// for a remote target the one the user declared.
	Strategy string `json:"strategy"`
	// World names where the workers live: "mock:identical" or "mock:heterogeneous" for the constant-speed
	// mock, "sim:fast,medium,slow" for the simulator engine with those profiles in turn, "remote" for a
	// target the harness did not build.
	World string `json:"world"`
	// ParameterHash is ParameterHash(Metadata.Cluster): the identity of the world's parameters. Empty
	// for a remote target, whose workers the harness cannot see.
	ParameterHash string `json:"parameter_hash,omitempty"`
	// Options are the gateway's scheduler settings that change what a strategy chooses. Nil for a remote
	// target (unknown).
	Options *SchedulerOptions `json:"options,omitempty"`
}

// SchedulerOptions are the gateway settings of scheduler.* that change scheduling decisions, as the
// gateway ran with them (not as a flag defaulted them).
type SchedulerOptions struct {
	// LoadOverlay is "max" or "ledger" (scheduler.load_overlay).
	LoadOverlay string `json:"load_overlay"`
	// OutputFraction and InputWeight are scheduler.cost.output_fraction and input_weight.
	OutputFraction float64 `json:"output_fraction"`
	InputWeight    float64 `json:"input_weight"`
	// CapacityHalflifeMs and CapacityFloor are scheduler.signals.capacity_halflife (milliseconds) and
	// capacity_floor.
	CapacityHalflifeMs int64   `json:"capacity_halflife_ms"`
	CapacityFloor      float64 `json:"capacity_floor"`
	// UpstreamHeaderTimeoutMs is gateway.upstream_header_timeout in milliseconds, recorded only when it is not the
	// harness default (30s); 0 means the default. A short one fails long non-streaming requests with 504.
	UpstreamHeaderTimeoutMs int64 `json:"upstream_header_timeout_ms,omitempty"`
}

// Label is the options on one line, for messages: equal options give equal labels.
func (o *SchedulerOptions) Label() string {
	if o == nil {
		return Unknown
	}
	s := fmt.Sprintf("overlay=%s output_fraction=%g input_weight=%g capacity_halflife=%dms capacity_floor=%g",
		o.LoadOverlay, o.OutputFraction, o.InputWeight, o.CapacityHalflifeMs, o.CapacityFloor)
	if o.UpstreamHeaderTimeoutMs != 0 {
		s += fmt.Sprintf(" upstream_header_timeout=%dms", o.UpstreamHeaderTimeoutMs)
	}
	return s
}

// ParameterHash identifies the parameters of an embedded cluster: its profile, heartbeat and registry
// refresh, and for every worker its model, speed, time to first token, concurrency, queue size and (when it
// runs the simulator engine) the simulator profile and that profile's parameter hash. Worker IDs and URLs are
// left out, so the same world booted twice has the same hash. The strategy and the scheduler options are not
// part of it: comparing strategies in one world is the point, and the options are compared on their own. It
// returns "" for a nil cluster.
func ParameterHash(c *Cluster) string {
	if c == nil {
		return ""
	}
	type worker struct {
		Model           string  `json:"model"`
		TokensPerSecond float64 `json:"tps"`
		TTFTMillis      int64   `json:"ttft_ms"`
		MaxConcurrency  int     `json:"max_concurrency"`
		QueueSize       int     `json:"queue_size"`
		SimProfile      string  `json:"sim_profile"`
		SimParamsHash   string  `json:"sim_params_hash"`
	}
	doc := struct {
		Version   int      `json:"version"`
		Profile   string   `json:"profile"`
		Heartbeat int64    `json:"heartbeat_ms"`
		Refresh   int64    `json:"refresh_ms"`
		Workers   []worker `json:"workers"`
	}{Version: 1, Profile: c.Profile, Heartbeat: c.HeartbeatMillis, Refresh: c.RegistryRefreshMs, Workers: []worker{}}
	for _, w := range c.Workers {
		doc.Workers = append(doc.Workers, worker{w.Model, w.TokensPerSecond, w.TTFTMillis, w.MaxConcurrency, w.QueueSize, w.SimProfile, w.SimParamsHash})
	}
	b, err := json.Marshal(doc)
	if err != nil { // unreachable: numbers and strings
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// WorldLabel names the world of a cluster: see Experiment.World.
func WorldLabel(c *Cluster) string {
	if c == nil {
		return "remote"
	}
	var sims []string
	for _, w := range c.Workers {
		if w.SimProfile != "" {
			sims = append(sims, w.SimProfile)
		}
	}
	if len(sims) == 0 {
		return "mock:" + c.Profile
	}
	// The distinct profiles in the order the workers first use them.
	seen := map[string]bool{}
	var order []string
	for _, s := range sims {
		if !seen[s] {
			seen[s] = true
			order = append(order, s)
		}
	}
	return "sim:" + strings.Join(order, ",")
}

// experimentDifferences are the Experiment fields that differ between two results, when both have one. A
// result without the record (written before Phase 14 G-lite) is never flagged against one with it.
func experimentDifferences(a, b Metadata) []Difference {
	ea, eb := a.Experiment, b.Experiment
	if ea == nil || eb == nil {
		return nil
	}
	var out []Difference
	add := func(field string, expected bool, av, bv string) {
		if av != bv {
			out = append(out, Difference{Field: field, A: av, B: bv, Expected: expected})
		}
	}
	add("world", false, ea.World, eb.World)
	add("parameter hash", false, hashLabel(ea.ParameterHash), hashLabel(eb.ParameterHash))
	if ea.Options != nil && eb.Options != nil {
		oa, ob := *ea.Options, *eb.Options
		// A work-aware strategy needs the ledger overlay and the harness sets it, so a difference in the overlay is
		// expected exactly when the strategies differ; with the same strategy it is a different configuration.
		add("load overlay", ea.Strategy != eb.Strategy, oa.LoadOverlay, ob.LoadOverlay)
		oa.LoadOverlay, ob.LoadOverlay = "", ""
		add("scheduler options", false, oa.Label(), ob.Label())
	}
	return out
}

func hashLabel(h string) string {
	if h == "" {
		return Unknown
	}
	return h[:min(len(h), 12)]
}

// experimentWarnings explains, in words, the experiment fields that make two groups incomparable: the
// worlds or their parameters differ between the sides, or a repeat group mixes parameter hashes.
func experimentWarnings(a, b Result, ga, gb []Result) []string {
	var out []string
	ea, eb := a.Metadata.Experiment, b.Metadata.Experiment
	if ea != nil && eb != nil {
		if ea.World != eb.World {
			out = append(out, fmt.Sprintf("%s and %s ran in different worlds (%s, %s): their deltas compare worlds, not just schedulers", Clean(a.RunID), Clean(b.RunID), Clean(ea.World), Clean(eb.World)))
		}
		if ea.ParameterHash != eb.ParameterHash {
			out = append(out, fmt.Sprintf("%s and %s have different parameter hashes (%s, %s): the worlds' parameters are not the same", Clean(a.RunID), Clean(b.RunID), hashLabel(ea.ParameterHash), hashLabel(eb.ParameterHash)))
		}
		if ea.Options != nil && eb.Options != nil {
			oa, ob := *ea.Options, *eb.Options
			if ea.Strategy == eb.Strategy && oa.LoadOverlay != ob.LoadOverlay {
				out = append(out, fmt.Sprintf("%s and %s ran strategy %s with different load overlays (%s, %s)", Clean(a.RunID), Clean(b.RunID), Clean(ea.Strategy), Clean(oa.LoadOverlay), Clean(ob.LoadOverlay)))
			}
			oa.LoadOverlay, ob.LoadOverlay = "", ""
			if oa != ob {
				out = append(out, fmt.Sprintf("%s and %s ran with different scheduler options (%s; %s)", Clean(a.RunID), Clean(b.RunID), Clean(ea.Options.Label()), Clean(eb.Options.Label())))
			}
		}
	}
	for _, side := range []struct {
		name string
		g    []Result
	}{{"A", ga}, {"B", gb}} {
		hashes := map[string]bool{}
		for _, r := range side.g {
			if e := r.Metadata.Experiment; e != nil {
				hashes[e.ParameterHash] = true
			}
		}
		if len(hashes) > 1 {
			out = append(out, fmt.Sprintf("the repeat group of side %s mixes %d different parameter hashes: its runs are not repeats of one configuration", side.name, len(hashes)))
		}
	}
	return out
}
