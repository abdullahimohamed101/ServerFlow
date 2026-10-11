package report

import (
	"fmt"
	"sort"
	"strings"
)

// Resilience values recorded in Metadata.Resilience.
const (
	// ResilienceAbsent means the build under test has no resilience feature at all (master before
	// Phase 15). It is not "off": off is the feature present and switched off.
	ResilienceAbsent = "absent"
	ResilienceOn     = "on"
	ResilienceOff    = "off"
)

// Chaos is the additive result of a fault-injection run (Phase 15): the run is split into a
// baseline, a fault and a recovery window, and the two pre-registered measures of the scenario are
// computed from them. A run without faults has no Chaos.
type Chaos struct {
	Scenario    string `json:"scenario"`
	Description string `json:"description"`
	// Variant tells runs of one scenario apart (the slow worker run with the header timeout below and
	// above the delay, the error burst in two modes).
	Variant string `json:"variant"`
	// TimeScale is the factor the scenario's default durations were multiplied by.
	TimeScale     float64 `json:"time_scale"`
	BucketSeconds float64 `json:"bucket_seconds"`
	// Params are the scenario's parameters as run: header timeout, extra delay, error rate, clients.
	Params map[string]string `json:"params"`
	// FaultWorker is the worker the fault was applied to ("" when the fault is not on one worker).
	FaultWorker string `json:"fault_worker"`
	// FaultAppliedSeconds and FaultClearedSeconds are when the harness applied and cleared the
	// fault, as offsets from the start of the run. The windows follow them, not the plan.
	FaultAppliedSeconds float64 `json:"fault_applied_seconds"`
	FaultClearedSeconds float64 `json:"fault_cleared_seconds"`

	Windows  []ChaosWindow `json:"windows"`
	Evidence ChaosEvidence `json:"evidence"`
	Measures ChaosMeasures `json:"measures"`
	Hard     ChaosHard     `json:"hard_bounds"`
	Gate     []ChaosCheck  `json:"validity_checks"`
	Series   []ChaosBucket `json:"series"`
}

// ChaosWindow summarizes the requests whose completion (or, for the counts, intended send) falls in
// one window.
type ChaosWindow struct {
	Name         string  `json:"name"`
	StartSeconds float64 `json:"start_seconds"`
	EndSeconds   float64 `json:"end_seconds"`
	Sent         int     `json:"sent"`
	Succeeded    int     `json:"succeeded"`
	Failed       int     `json:"failed"`
	// SuccessRate is Succeeded/Sent, 0 when nothing was sent.
	SuccessRate float64 `json:"success_rate"`
	// Retried counts requests the gateway reported more than one attempt for.
	Retried int `json:"retried"`
	// AttemptsDelta, RetriesDelta, NoCapacityDelta and IneligibleDelta are the changes in the gateway's
	// own counters over the window (inference_attempts_total, inference_retries_total,
	// scheduler_no_capacity_total, scheduler_ineligible_selections_total).
	AttemptsDelta   float64 `json:"gateway_attempts"`
	RetriesDelta    float64 `json:"gateway_retries"`
	NoCapacityDelta float64 `json:"gateway_no_capacity"`
	IneligibleDelta float64 `json:"gateway_ineligible_selections"`
	// AttemptsPerRequest is AttemptsDelta divided by Sent; nil when nothing was sent. Requests the
	// gateway refused without any attempt (no capacity) pull it down.
	AttemptsPerRequest *float64 `json:"attempts_per_request"`
	// Latency of successful requests and of failed ones, milliseconds, from the intended send.
	LatencyP50    *float64       `json:"latency_p50_ms"`
	LatencyP95    *float64       `json:"latency_p95_ms"`
	LatencyP99    *float64       `json:"latency_p99_ms"`
	FailureP50    *float64       `json:"failure_latency_p50_ms"`
	ErrorClasses  map[string]int `json:"error_classes"`
	StatusClasses map[string]int `json:"status_classes"`
}

// ChaosEvidence shows the fault really happened: the mock worker's own counters, not the harness's
// intention.
type ChaosEvidence struct {
	Applied bool `json:"applied"`
	// Touched is the requests a request-level fault affected (errored, delayed or held).
	Touched    int64 `json:"requests_touched"`
	Errored    int64 `json:"errored"`
	Delayed    int64 `json:"delayed"`
	Blackholed int64 `json:"blackholed"`
	Crashes    int64 `json:"crashes"`
	// Drains is how many workers the harness drained; ServedWhileAnnouncing the requests they served
	// after announcing the drain.
	Drains                int   `json:"drains"`
	ServedWhileAnnouncing int64 `json:"served_while_announcing"`
	// ControlPlane is the faulted worker's state as the control plane saw it at the end of the fault.
	ControlPlane string `json:"control_plane_state"`
	// Clients is the closed-loop client count in the fault window; SurvivingSlots the request slots of
	// the workers the fault leaves serving.
	Clients        int `json:"clients_in_fault"`
	SurvivingSlots int `json:"surviving_slots"`
}

// ChaosMeasures are the pre-registered primary measures and the descriptive ones. Only the primary
// two support a claim; the others describe.
type ChaosMeasures struct {
	// Primary: success rate in the fault window, and attempts per request in it.
	FaultSuccessRate        float64  `json:"primary_fault_success_rate"`
	FaultAttemptsPerRequest *float64 `json:"primary_fault_attempts_per_request"`

	// TimeToIsolate is the seconds from the fault's start until the faulted worker stopped receiving
	// non-probe first attempts; nil when it never did inside the fault window.
	TimeToIsolate     *float64 `json:"time_to_isolate_seconds"`
	TimeToIsolateNote string   `json:"time_to_isolate_note"`
	// TimeToRecover is the seconds from the fault's end (start for a permanent fault) until five
	// consecutive buckets with enough requests had a success ratio at least the baseline's.
	TimeToRecover     *float64 `json:"time_to_recover_seconds"`
	TimeToRecoverNote string   `json:"time_to_recover_note"`
	// FaultSuccessLatencyP95 and P99 are the tail of successes in the fault window (ms).
	FaultSuccessLatencyP95 *float64 `json:"fault_success_latency_p95_ms"`
	FaultSuccessLatencyP99 *float64 `json:"fault_success_latency_p99_ms"`
}

// ChaosHard are the bounds that must hold whatever the scenario.
type ChaosHard struct {
	// IneligibleSelections is scheduler_ineligible_selections_total over the run; it must be 0.
	IneligibleSelections float64 `json:"ineligible_selections"`
	GoroutinesStart      int     `json:"goroutines_start"`
	GoroutinesEnd        int     `json:"goroutines_end"`
	HeapStartMiB         float64 `json:"heap_start_mib"`
	HeapEndMiB           float64 `json:"heap_end_mib"`
}

// ChaosCheck is one clause of the validity gate and whether it held.
type ChaosCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// ChaosBucket is one slice of the run's time series (BucketSeconds long), by completion time.
type ChaosBucket struct {
	StartSeconds float64 `json:"t"`
	Succeeded    int     `json:"ok"`
	Failed       int     `json:"failed"`
	// FaultySelections is the scheduler's choices of the faulted worker in the bucket and Probes the
	// breaker probes of any worker (0 when the build has no breaker).
	FaultySelections float64 `json:"faulty_selections"`
	Probes           float64 `json:"probes"`
}

// ResilienceOf returns the resilience value a result records: "" for results written before the
// field existed.
func ResilienceOf(m Metadata) string { return m.Resilience }

func resilienceLabel(m Metadata) string {
	if m.Resilience == "" {
		return "not recorded"
	}
	return m.Resilience
}

// resilienceWarnings says when the compared results were not run with the same resilience setting,
// so the difference may be the feature rather than the thing being compared.
func resilienceWarnings(a, b Result, ga, gb []Result) []string {
	var out []string
	for _, g := range [][]Result{ga, gb} {
		seen := map[string]bool{}
		for _, r := range g {
			seen[resilienceLabel(r.Metadata)] = true
		}
		if len(seen) > 1 {
			var vs []string
			for v := range seen {
				vs = append(vs, v)
			}
			sort.Strings(vs)
			out = append(out, fmt.Sprintf("a repeat group mixes resilience settings (%s): its range is not one configuration", strings.Join(vs, ", ")))
		}
	}
	if x, y := resilienceLabel(a.Metadata), resilienceLabel(b.Metadata); x != y {
		out = append(out, fmt.Sprintf("the groups differ in resilience (%s against %s): a difference below may be the resilience feature itself, not the change you meant to compare. \"absent\" is a build without the feature, \"off\" is the feature present and switched off", x, y))
	}
	return out
}

// chaosMarkdown renders the chaos section of a report.
func chaosMarkdown(c *Chaos) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## Chaos: %s", Clean(c.Scenario))
	if c.Variant != "" {
		fmt.Fprintf(&b, " (%s)", Clean(c.Variant))
	}
	b.WriteString("\n\n")
	fmt.Fprintf(&b, "%s\n\nTime scale %g, fault applied at %.1fs and cleared at %.1fs after the start of the run, on %s.", CleanN(c.Description, 800), c.TimeScale, c.FaultAppliedSeconds, c.FaultClearedSeconds, orDash(c.FaultWorker))
	if len(c.Params) > 0 {
		keys := make([]string, 0, len(c.Params))
		for k := range c.Params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = Clean(k) + "=" + Clean(c.Params[k])
		}
		fmt.Fprintf(&b, " Parameters: %s.", strings.Join(parts, ", "))
	}
	b.WriteString("\n\n| Window | Seconds | Sent | Succeeded | Failed | Success | Attempts/request | Retried | p95 ms | p99 ms |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, w := range c.Windows {
		fmt.Fprintf(&b, "| %s | %.1f-%.1f | %d | %d | %d | %s | %s | %d | %s | %s |\n", Clean(w.Name), w.StartSeconds, w.EndSeconds, w.Sent, w.Succeeded, w.Failed,
			pct(w.SuccessRate), opt(w.AttemptsPerRequest, "%.3f"), w.Retried, opt(w.LatencyP95, "%.0f"), opt(w.LatencyP99, "%.0f"))
	}
	m := c.Measures
	fmt.Fprintf(&b, "\nPre-registered primary measures: success rate in the fault window %s, attempts per request in it %s.\n", pct(m.FaultSuccessRate), opt(m.FaultAttemptsPerRequest, "%.3f"))
	fmt.Fprintf(&b, "Descriptive: time to isolate %s%s; time to recover %s%s.\n", optSeconds(m.TimeToIsolate), noteSuffix(m.TimeToIsolateNote), optSeconds(m.TimeToRecover), noteSuffix(m.TimeToRecoverNote))
	e := c.Evidence
	fmt.Fprintf(&b, "\nFault evidence (the mock worker's own counters): applied %v, %d requests touched (%d errored, %d delayed, %d held), %d crashes, %d drains (%d requests served while announcing); control plane saw the worker as %s; %d clients on %d surviving slots.\n",
		e.Applied, e.Touched, e.Errored, e.Delayed, e.Blackholed, e.Crashes, e.Drains, e.ServedWhileAnnouncing, orDash(e.ControlPlane), e.Clients, e.SurvivingSlots)
	h := c.Hard
	fmt.Fprintf(&b, "Hard bounds: ineligible selections %.0f (must be 0); goroutines %d to %d; heap %.1f to %.1f MiB.\n\n", h.IneligibleSelections, h.GoroutinesStart, h.GoroutinesEnd, h.HeapStartMiB, h.HeapEndMiB)
	b.WriteString("Validity gate:\n\n")
	for _, ck := range c.Gate {
		mark := "ok"
		if !ck.OK {
			mark = "FAILED"
		}
		fmt.Fprintf(&b, "- %s: %s (%s)\n", Clean(ck.Name), mark, CleanN(ck.Detail, 600))
	}
	b.WriteString("\nOnly the two primary measures support a claim, and only when the ranges of repeat groups do not overlap; with ten or so descriptive measures, one range in ten is disjoint by chance.\n\n")
	return b.String()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return Clean(s)
}

func optSeconds(p *float64) string {
	if p == nil {
		return "not reached"
	}
	return fmt.Sprintf("%.1fs", *p)
}

func noteSuffix(n string) string {
	if n == "" {
		return ""
	}
	return " (" + Clean(n) + ")"
}
