package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Phase is a named window of the run. A request belongs to the phase that was current when it started. Phases whose name
// starts with "roll" are the rollouts: the requests that must number at least the minimum of D20.
type Phase struct {
	Name  string
	Start time.Time
}

// PhaseSummary is the tally of one phase.
type PhaseSummary struct {
	Name     string         `json:"name"`
	Seconds  float64        `json:"seconds"`
	Requests int            `json:"requests"`
	Failures int            `json:"failures"`
	Classes  map[string]int `json:"failures_by_class"`
}

// Summary is the result of a run.
type Summary struct {
	Phases           []PhaseSummary `json:"phases"`
	Seconds          float64        `json:"seconds"`
	Requests         int            `json:"requests"`
	Failures         int            `json:"failures"`
	Classes          map[string]int `json:"failures_by_class"`
	RolloutSeconds   float64        `json:"rollout_seconds"`
	RolloutRequests  int            `json:"requests_during_rollouts"`
	RolloutFailures  int            `json:"failures_during_rollouts"`
	StreamRequests   int            `json:"stream_requests"`
	KeepAliveRequest int            `json:"keep_alive_requests"`
	// GeneratorErrors are failures of the generator itself (a request it could not build), not observations of the server.
	GeneratorErrors int `json:"generator_errors"`
	// CapacityRejections are 503 no_capacity answers: the gateway or its workers were full. That is load, not the drain, so the
	// negative control does not count them (they are reported separately, and in the baseline they show the load alone does it).
	CapacityRejections         int `json:"capacity_rejections"`
	BaselineCapacityRejections int `json:"capacity_rejections_in_baseline"`
	// DrainFailures are the failures a missing drain explains: connections refused, reset or closed, timeouts, cut streams,
	// malformed or truncated streams and bodies, 502 and 504. RolloutDrainFailures are those inside the rollout phases.
	DrainFailures        int `json:"drain_failures"`
	RolloutDrainFailures int `json:"drain_failures_during_rollouts"`
}

// IsCapacityRejection reports whether a failure class is a capacity 503 (http_503_no_capacity).
func IsCapacityRejection(class string) bool { return class == "http_503_no_capacity" }

// IsDrainAttributable reports whether a failure class is one that a rollout without a drain explains. Statuses other than 502
// and 504 (a 503 of any code, a 429) say the gateway or its workers were overloaded or unready, which the load alone can cause;
// transport_other and stream error events are not specific enough; request_not_built is the generator's own.
func IsDrainAttributable(class string) bool {
	switch class {
	case ClassRefused, ClassReset, ClassClosed, ClassTimeout, ClassStreamCut, ClassSSEMalformed, ClassBodyCut, ClassBodyMalformed:
		return true
	}
	return strings.HasPrefix(class, "http_502") || strings.HasPrefix(class, "http_504")
}

func isRollout(name string) bool { return strings.HasPrefix(name, "roll") }

// Summarize tallies outcomes by phase. end closes the last phase. Phases must be in time order.
func Summarize(phases []Phase, outcomes []Outcome, end time.Time) Summary {
	s := Summary{Classes: map[string]int{}}
	for i, p := range phases {
		stop := end
		if i+1 < len(phases) {
			stop = phases[i+1].Start
		}
		d := stop.Sub(p.Start).Seconds()
		s.Phases = append(s.Phases, PhaseSummary{Name: p.Name, Seconds: d, Classes: map[string]int{}})
		s.Seconds += d
		if isRollout(p.Name) {
			s.RolloutSeconds += d
		}
	}
	for _, o := range outcomes {
		i := sort.Search(len(phases), func(i int) bool { return phases[i].Start.After(o.Start) }) - 1
		if i < 0 {
			i = 0
		}
		ps := &s.Phases[i]
		ps.Requests++
		s.Requests++
		if o.Stream {
			s.StreamRequests++
		}
		if o.KeepAlive {
			s.KeepAliveRequest++
		}
		roll := isRollout(ps.Name)
		if roll {
			s.RolloutRequests++
		}
		if !o.OK() {
			ps.Failures++
			ps.Classes[o.Class]++
			s.Failures++
			s.Classes[o.Class]++
			if o.Class == ClassBadRequest {
				s.GeneratorErrors++
			}
			if IsCapacityRejection(o.Class) {
				s.CapacityRejections++
				if ps.Name == "baseline" {
					s.BaselineCapacityRejections++
				}
			}
			if IsDrainAttributable(o.Class) {
				s.DrainFailures++
				if roll {
					s.RolloutDrainFailures++
				}
			}
			if roll {
				s.RolloutFailures++
			}
		}
	}
	return s
}

// RuleOfThree is the 95% upper bound of the true failure rate when n requests produced no failure (3/n).
func RuleOfThree(n int) float64 {
	if n <= 0 {
		return math.NaN()
	}
	return 3 / float64(n)
}

// Verdict applies D20. expectFailures is the negative control: it passes only when the outage was seen.
func Verdict(s Summary, minRollout int, expectFailures bool) (pass bool, why string) {
	if expectFailures {
		// The outage must show during the rollouts, as a failure a missing drain explains. Capacity rejections, failures in the
		// baseline or after the rollouts, and requests the generator could not build are not the outage.
		if s.RolloutDrainFailures > 0 {
			return true, fmt.Sprintf("the negative control produced %d drain-attributable failures during the rollouts: the test can see the outage", s.RolloutDrainFailures)
		}
		return false, fmt.Sprintf("the negative control produced no drain-attributable failure during the rollouts (%d capacity rejections do not count): the test cannot see the outage", s.CapacityRejections)
	}
	switch {
	case s.GeneratorErrors > 0:
		return false, fmt.Sprintf("the load generator failed to build %d requests", s.GeneratorErrors)
	case s.Failures > 0:
		return false, fmt.Sprintf("%d of %d requests failed", s.Failures, s.Requests)
	case s.RolloutRequests < minRollout:
		return false, fmt.Sprintf("only %d requests were made during the rollouts; at least %d are needed", s.RolloutRequests, minRollout)
	}
	return true, "zero failures"
}

func sortedKeys(m map[string]int) []string {
	k := make([]string, 0, len(m))
	for n := range m {
		k = append(k, n)
	}
	sort.Strings(k)
	return k
}

// WriteText prints the report a person reads.
func WriteText(w io.Writer, s Summary, minRollout int, expectFailures bool) {
	p := func(f string, a ...any) { _, _ = fmt.Fprintf(w, f, a...) }
	p("requests: %d total (%d streaming, %d on keep-alive connections), %d failed\n", s.Requests, s.StreamRequests, s.KeepAliveRequest, s.Failures)
	for _, ph := range s.Phases {
		p("  %-10s %6.1f s %6d requests %4d failed\n", ph.Name, ph.Seconds, ph.Requests, ph.Failures)
		for _, c := range sortedKeys(ph.Classes) {
			p("    %-40s %d\n", c, ph.Classes[c])
		}
	}
	if len(s.Classes) == 0 {
		p("failures by class: none\n")
	} else {
		p("failures by class:\n")
		for _, c := range sortedKeys(s.Classes) {
			p("  %-40s %d\n", c, s.Classes[c])
		}
	}
	if expectFailures {
		p("requests during the rollouts: %d, failures among them: %d\n", s.RolloutRequests, s.RolloutFailures)
	} else {
		p("requests during the rollouts: %d (at least %d are needed), failures among them: %d\n", s.RolloutRequests, minRollout, s.RolloutFailures)
	}
	if s.Seconds > 0 {
		p("throughput of the load generator: %.1f requests/s overall", float64(s.Requests)/s.Seconds)
		if s.RolloutSeconds > 0 {
			p(", %.1f requests/s during the rollouts (%.0f s)", float64(s.RolloutRequests)/s.RolloutSeconds, s.RolloutSeconds)
		}
		p("\n")
	}
	if s.CapacityRejections > 0 {
		p("capacity rejections (503 no_capacity): %d, of them %d in the baseline, before any rollout: the gateway or its workers were full, a load effect and not the drain\n", s.CapacityRejections, s.BaselineCapacityRejections)
	}
	if expectFailures {
		p("drain-attributable failures (refused, reset, closed, timeout, cut or malformed streams and bodies, 502, 504): %d, of them %d during the rollouts\n", s.DrainFailures, s.RolloutDrainFailures)
	}
	if s.Failures == 0 && s.Requests > 0 {
		p("rule of three: 0 failures in %d requests bounds the true failure rate below %.3f%% at 95%% confidence (not a claim of no outage)\n", s.Requests, 100*RuleOfThree(s.Requests))
	} else if s.Requests > 0 {
		p("observed failure rate: %.2f%% (%d of %d)\n", 100*float64(s.Failures)/float64(s.Requests), s.Failures, s.Requests)
	}
	pass, why := Verdict(s, minRollout, expectFailures)
	res := "FAIL"
	if pass {
		res = "PASS"
	}
	p("result: %s (%s)\n", res, why)
}

// PodTiming is one gateway pod that was seen terminating.
type PodTiming struct {
	Name     string
	SIGTERM  time.Time // deletionTimestamp minus the grace period: when the pod was asked to stop
	LastSeen time.Time // the last poll that still found the pod object
}

// ParsePodObservations reads the poller's lines: "<unix seconds of the poll> <pod> <deletionTimestamp> <grace seconds>".
// Lines without a deletion timestamp (a pod that is not terminating) and malformed lines are ignored.
func ParsePodObservations(r io.Reader) ([]PodTiming, error) {
	by := map[string]*PodTiming{}
	var order []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 4 {
			continue
		}
		at, err1 := strconv.ParseInt(f[0], 10, 64)
		del, err2 := time.Parse(time.RFC3339, f[2])
		grace, err3 := strconv.Atoi(f[3])
		if err1 != nil || err2 != nil || err3 != nil || grace < 0 {
			continue
		}
		seen := time.Unix(at, 0).UTC()
		t, ok := by[f[1]]
		if !ok {
			t = &PodTiming{Name: f[1], SIGTERM: del.Add(-time.Duration(grace) * time.Second).UTC()}
			by[f[1]] = t
			order = append(order, f[1])
		}
		if seen.After(t.LastSeen) {
			t.LastSeen = seen
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	out := make([]PodTiming, 0, len(order))
	for _, n := range order {
		out = append(out, *by[n])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SIGTERM.Before(out[j].SIGTERM) })
	return out, nil
}

// WritePodTimings prints, per replaced pod, how long it took to go from SIGTERM to leaving the API. The clients cannot say
// which pod served a request (the gateway puts no pod identity in a response), so "SIGTERM to the last request that pod
// accepted" is not obtainable from outside; the time until the pod object is gone bounds it from above, to the poll period.
func WritePodTimings(w io.Writer, pods []PodTiming) {
	if len(pods) == 0 {
		_, _ = fmt.Fprintln(w, "replaced pods: none observed terminating")
		return
	}
	_, _ = fmt.Fprintf(w, "replaced pods: %d (SIGTERM = deletionTimestamp minus grace period; seconds resolution; the time to the last request a pod accepted is not obtainable from the client side, the SIGTERM-to-gone time below is its upper bound)\n", len(pods))
	var max time.Duration
	for _, p := range pods {
		d := p.LastSeen.Sub(p.SIGTERM)
		if d > max {
			max = d
		}
		_, _ = fmt.Fprintf(w, "  %-40s SIGTERM %s, gone about %.0f s later\n", p.Name, p.SIGTERM.Format("15:04:05Z"), d.Seconds())
	}
	_, _ = fmt.Fprintf(w, "longest SIGTERM-to-gone: about %.0f s\n", max.Seconds())
}
