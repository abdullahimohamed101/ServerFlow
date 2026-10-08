package report

import (
	"fmt"
	"sort"
	"strings"

	"serverflow/internal/bench/collect"
)

// Markdown renders the human-readable report. It states measurements and what they
// do not show; it never ranks anything.
func Markdown(r Result) string {
	var b strings.Builder
	m, s := r.Metadata, r.Summary
	fmt.Fprintf(&b, "# Benchmark %s\n\n", r.RunID)
	fmt.Fprintf(&b, "%s, workload `%s`, scheduler `%s`, %s. Seed %d, repeat %d of %d.\n\n",
		m.Date, m.Workload, m.Scheduler, loadLine(m), m.Seed, max(m.Repeat.Index, 1), max(m.Repeat.Of, 1))

	b.WriteString("## Setup\n\n| | |\n| --- | --- |\n")
	row := func(k, v string) { fmt.Fprintf(&b, "| %s | %s |\n", k, v) }
	row("Target", targetLine(m))
	row("Commit", m.GitCommit+" ("+m.GitTree+")")
	row("Models", strings.Join(m.Models, ", "))
	row("Workers", workersLine(m))
	row("GPU", m.GPUType)
	row("Duration / warm-up", fmt.Sprintf("%gs / %gs", m.DurationSecs, m.WarmupSecs))
	row("Streaming share", fmt.Sprintf("%g", m.StreamRatio))
	row("Plan digest", "`"+m.PlanDigest+"`")
	row("Go / machine", m.GoVersion+", "+m.Machine)
	b.WriteString("\nPrompt distribution (input tokens are synthetic text, about four characters each):\n\n")
	b.WriteString("| Class | Weight | Input tokens | max_tokens |\n| --- | --- | --- | --- |\n")
	for _, c := range m.Prompt {
		fmt.Fprintf(&b, "| %s | %g | %d-%d | %d-%d |\n", c.Name, c.Weight, c.InputMin, c.InputMax, c.OutputMin, c.OutputMax)
	}

	b.WriteString("\n## Requests\n\n")
	fmt.Fprintf(&b, "Sent %d, succeeded %d, failed %d (error rate %s). %d warm-up requests were not counted; %d needed a second attempt.\n\n",
		s.Sent, s.Succeeded, s.Failed, pct(s.ErrorRate), s.Warmup, s.Retried)
	if len(s.ErrorClasses) > 0 {
		fmt.Fprintf(&b, "Failures by class: %s. Responses by status class: %s.\n\n", countMap(s.ErrorClasses), countMap(s.StatusClasses))
	}

	b.WriteString("## Throughput\n\n")
	fmt.Fprintf(&b, "Measured over %.2fs (the %.0fs window plus the drain of requests still in flight at its end).\n\n", s.SpanSeconds, s.WindowSeconds)
	fmt.Fprintf(&b, "| Requests/s | Input tokens/s | Output tokens/s |\n| --- | --- | --- |\n| %.2f | %.1f | %.1f |\n\n",
		s.RequestsPerSecond, s.InputTokensPerSecond, s.OutputTokensPerSecond)
	fmt.Fprintf(&b, "Tokens: %d requests reported usage, %d were estimated (request size for input, stream chunks for streamed output).\n\n",
		s.UsageRequests, s.EstimatedRequests)

	b.WriteString("## Latency and time to first token (ms)\n\n")
	b.WriteString("Over successful requests, measured from the intended send time")
	if m.Mode == "closed" {
		b.WriteString(" (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013)")
	}
	b.WriteString(".\n\n| | n | p50 | p95 | p99 | max |\n| --- | --- | --- | --- | --- | --- |\n")
	dist := func(name string, d *collect.Dist) {
		if d == nil {
			fmt.Fprintf(&b, "| %s | not measured | | | | |\n", name)
			return
		}
		fmt.Fprintf(&b, "| %s | %d | %.1f | %.1f | %.1f | %.1f |\n", name, d.Count, d.P50, d.P95, d.P99, d.Max)
	}
	dist("Latency", s.Latency)
	dist("TTFT (streaming)", s.TTFT)
	b.WriteString("\n")

	b.WriteString("## Queues and worker balance\n\n")
	if q := r.Queue; q != nil {
		fmt.Fprintf(&b, "%d samples. Total queue depth across workers: average %.2f, p95 %.1f, max %.0f. Average requests being served: %.2f.\n\n",
			q.Samples, q.AvgTotalQueue, q.P95TotalQueue, q.MaxTotalQueue, q.AvgActive)
	} else {
		fmt.Fprintf(&b, "Queue depth: not measured (%s).\n\n", r.NotMeasured["queue"])
	}
	if len(r.Workers) > 0 {
		b.WriteString("| Worker | Model | Completed | Share | Mean queue | Mean active |\n| --- | --- | --- | --- | --- | --- |\n")
		var total int64
		for _, w := range r.Workers {
			if w.Completed != nil {
				total += *w.Completed
			}
		}
		for _, w := range r.Workers {
			share := "n/a"
			if w.Completed != nil && total > 0 {
				share = pct(float64(*w.Completed) / float64(total))
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", w.ID, w.Model, optInt(w.Completed), share, opt(w.MeanQueue, "%.2f"), opt(w.MeanActive, "%.2f"))
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "Request balance (Jain index, 1 is perfectly even): %s", optNotMeasured(r.Imbalance.RequestJain, "%.4f", r.NotMeasured["request_imbalance"]))
	if len(r.Imbalance.RequestJainByModel) > 1 {
		fmt.Fprintf(&b, " (lowest of %s)", floatMap(r.Imbalance.RequestJainByModel))
	}
	fmt.Fprintf(&b, ".\nQueue balance (Jain index of mean queue depth): %s.\n", optNotMeasured(r.Imbalance.QueueJain, "%.4f", r.NotMeasured["queue_imbalance"]))
	fmt.Fprintf(&b, "GPU utilization: %s.\n\n", optNotMeasured(r.GPUUtilization, "%.2f", r.NotMeasured["gpu_utilization"]))

	if len(r.NotMeasured) > 0 {
		b.WriteString("## Not measured\n\n")
		keys := make([]string, 0, len(r.NotMeasured))
		for k := range r.NotMeasured {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "- %s: %s\n", k, r.NotMeasured[k])
		}
		b.WriteString("\n")
	}

	b.WriteString("## How to read this\n\n")
	b.WriteString("- These are measurements of one run, not a ranking. A different seed, machine, or load can change them; use `--repeat` to see run-to-run spread.\n")
	if m.Target == "embedded" {
		b.WriteString("- The load generator, gateway, control plane and mock workers share one machine, so absolute numbers say little about capacity; compare runs made on the same machine.\n")
	}
	b.WriteString("- A balanced distribution (Jain near 1) is not automatically good when workers differ in speed.\n")
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "- %s\n", n)
	}
	return b.String()
}

func loadLine(m Metadata) string {
	if m.Mode == "open" {
		return fmt.Sprintf("open-loop at %g requests/s", m.Rate)
	}
	return fmt.Sprintf("closed-loop with %d clients", m.Concurrency)
}

func targetLine(m Metadata) string {
	if m.Target == "embedded" {
		return "embedded simulated cluster (mock workers)"
	}
	return m.Target + " " + m.TargetHost
}

func workersLine(m Metadata) string {
	n := "unknown"
	if m.WorkerCount != nil {
		n = fmt.Sprint(*m.WorkerCount)
	}
	if m.Cluster != nil {
		return n + ", profile " + m.Cluster.Profile
	}
	return n
}

func pct(f float64) string { return fmt.Sprintf("%.2f%%", f*100) }

func opt(p *float64, format string) string {
	if p == nil {
		return "n/a"
	}
	return fmt.Sprintf(format, *p)
}

func optInt(p *int64) string {
	if p == nil {
		return "n/a"
	}
	return fmt.Sprint(*p)
}

func optNotMeasured(p *float64, format, why string) string {
	if p == nil {
		if why == "" {
			why = "no reason recorded"
		}
		return "not measured (" + why + ")"
	}
	return fmt.Sprintf(format, *p)
}

func countMap(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, m[k])
	}
	return strings.Join(parts, ", ")
}

func floatMap(m map[string]float64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %.4f", k, m[k])
	}
	return strings.Join(parts, ", ")
}
