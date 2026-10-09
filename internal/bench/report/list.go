package report

import (
	"fmt"
	"io"
	"path/filepath"
	"text/tabwriter"
)

// List writes one line per run under base, oldest first. A run that cannot be read is
// shown with its error rather than hiding the rest.
func List(w io.Writer, base string) error {
	ids, err := RunIDs(base)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		_, err := fmt.Fprintf(w, "no runs in %s\n", base)
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "RUN\tDATE\tWORKLOAD\tSCHEDULER\tLOAD\tREQ/S\tP95 LATENCY\tFAILED\tREPEAT")
	for _, id := range ids {
		r, err := Load(filepath.Join(base, id))
		if err != nil {
			_, _ = fmt.Fprintf(tw, "%s\tunreadable: %s\n", id, Clean(err.Error()))
			continue
		}
		m, s := r.Metadata, r.Summary
		load := fmt.Sprintf("%d clients", m.Concurrency)
		if m.Mode == "open" {
			load = fmt.Sprintf("%g/s", m.Rate)
		}
		p95 := "n/a"
		if s.Latency != nil {
			p95 = fmt.Sprintf("%.0f ms", s.Latency.P95)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%.2f\t%s\t%d/%d\t%d/%d\n", id, Clean(m.Date), Clean(m.Workload), Clean(m.Scheduler), load,
			s.RequestsPerSecond, p95, s.Failed, s.Sent, max(m.Repeat.Index, 1), max(m.Repeat.Of, 1))
	}
	return tw.Flush()
}
