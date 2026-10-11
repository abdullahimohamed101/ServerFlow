package chaos

import (
	"fmt"
	"strings"
	"time"

	"serverflow/internal/bench/embedded"
	"serverflow/internal/bench/report"
)

// GateInput is everything the validity gate looks at.
type GateInput struct {
	Scenario Scenario
	Options  Options
	Plan     ClientPlan
	Params   Params
	Timeline Timeline
	Windows  []report.ChaosWindow // baseline, fault, recovery
	Evidence Evidence
	Settled  time.Duration
	GitTree  string
	Commit   string
	// DrivenOK is false when the load driver itself failed.
	DrivenOK bool
}

// Gate evaluates the chaos validity gate (Phase 15 D19). A run is valid only when every check holds; the
// checks say what the run, rather than the system under test, could explain. Invalid runs are listed and
// never compared.
func Gate(in GateInput) []report.ChaosCheck {
	var out []report.ChaosCheck
	add := func(name string, ok bool, format string, a ...any) {
		out = append(out, report.ChaosCheck{Name: name, OK: ok, Detail: fmt.Sprintf(format, a...)})
	}
	win := func(i int) report.ChaosWindow {
		if i < len(in.Windows) {
			return in.Windows[i]
		}
		return report.ChaosWindow{}
	}
	tl := in.Timeline

	add("load driver", in.DrivenOK, "the load generator ran to the end of the run")
	add("fault applied and cleared", tl.Applied && tl.Cleared && tl.ApplyErr == nil && tl.ClearErr == nil,
		"applied %v (err %v), cleared %v (err %v)", tl.Applied, tl.ApplyErr, tl.Cleared, tl.ClearErr)

	base := win(0)
	add("baseline error rate", base.Sent > 0 && 1-base.SuccessRate <= in.Options.MaxErrorRate,
		"%.2f%% of %d baseline requests failed, limit %.2f%%", 100*(1-base.SuccessRate), base.Sent, 100*in.Options.MaxErrorRate)

	ok, detail := in.Scenario.evidenced(in.Evidence)
	add("fault evidenced", ok, "%s (the mock workers' counters, not the harness's intention)", detail)

	// Closed-loop clients against the slots that are left serving.
	p := in.Plan
	if in.Scenario.Overload {
		add("clients exceed slots (overload)", p.Base+p.Extra > p.Slots,
			"%d clients in the fault window against %d slots: an overload needs more clients than slots", p.Base+p.Extra, p.Slots)
	} else {
		limit := SafeSlotFraction * float64(p.Surviving)
		add("clients within the surviving slots", float64(p.Base) <= limit,
			"%d clients against %d surviving slots (%d slots less the faulted worker's): at most %.0f%% of them = %.0f, or the run measures rejections",
			p.Base, p.Surviving, p.Slots, 100*SafeSlotFraction, limit)
	}

	minReq := in.Options.minRequests()
	var short []string
	for _, w := range in.Windows {
		if w.Sent < minReq {
			short = append(short, fmt.Sprintf("%s %d", w.Name, w.Sent))
		}
	}
	add("minimum requests per window", len(short) == 0 && len(in.Windows) == 3,
		"every window needs at least %d requests; short: %s", minReq, orNone(short))

	add("cluster settled", in.Settled >= 5*embedded.RegistryRefresh,
		"Start waited %v after the control plane listed every worker (five registry refreshes = %v)", in.Settled.Round(time.Millisecond), 5*embedded.RegistryRefresh)

	clean := in.GitTree == "clean" && in.Commit != "" && in.Commit != report.Unknown
	if in.Options.AllowDirty && !clean {
		add("clean working tree", true, "WAIVED by --allow-dirty (tree %s, commit %s): a development run, not for the record", in.GitTree, shortCommit(in.Commit))
	} else {
		add("clean working tree", clean, "tree %s, commit %s: the commit must be recorded and the tree clean for a result to be reproducible", in.GitTree, shortCommit(in.Commit))
	}

	add("no sleep or clock step", tl.ClockStep <= MaxClockStep,
		"the wall clock moved %v against the monotonic clock during the run (limit %v): a larger move means the machine slept or its clock was stepped", tl.ClockStep.Round(time.Millisecond), MaxClockStep)
	return out
}

// FailedChecks returns the explanation of each check that did not hold.
func FailedChecks(cs []report.ChaosCheck) []string {
	var out []string
	for _, c := range cs {
		if !c.OK {
			out = append(out, "validity gate: "+c.Name+": "+c.Detail)
		}
	}
	return out
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "none"
	}
	return strings.Join(s, ", ")
}

func shortCommit(c string) string {
	if len(c) > 10 {
		return c[:10]
	}
	if c == "" {
		return report.Unknown
	}
	return c
}
