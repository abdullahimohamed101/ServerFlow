package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"serverflow/internal/bench/collect"
)

// Result is the machine-readable outcome of one run (result.json). Every spec section 34
// metric is either present or named in NotMeasured with the reason. Latency and TTFT
// are in milliseconds.
type Result struct {
	SchemaVersion int                   `json:"schema_version"`
	RunID         string                `json:"run_id"`
	Metadata      Metadata              `json:"metadata"`
	Summary       collect.Summary       `json:"summary"`
	Queue         *collect.QueueSummary `json:"queue"`
	Workers       []WorkerResult        `json:"workers"`
	Imbalance     Imbalance             `json:"imbalance"`
	// GPUUtilization is the mean utilization workers reported, nil when none reported any.
	GPUUtilization *float64 `json:"gpu_utilization"`
	// Valid is false when the run does not support the conclusions its numbers invite: too many
	// requests failed (latency, TTFT and balance then describe only the survivors) or nothing was
	// measured. InvalidReasons say why. A result with valid=false should not be compared as is.
	Valid          bool     `json:"valid"`
	InvalidReasons []string `json:"invalid_reasons"`
	// Warnings are conditions that leave the result valid but change what its numbers mean (for
	// example a window cut short by --max-requests). They are printed at the top of the report.
	Warnings []string `json:"warnings"`
	// StatsMissing names the workers whose completed count could not be used, with the reason;
	// the request balance is then partial.
	StatsMissing map[string]string `json:"stats_missing"`
	// Timings say where the wall-clock time of the run went.
	Timings Timings `json:"timings"`
	// NotMeasured maps a metric to why it is absent.
	NotMeasured map[string]string `json:"not_measured"`
	// SamplePollsFailed and SamplesDropped say how complete the queue samples are.
	SamplePollsFailed int      `json:"sample_polls_failed"`
	SamplesDropped    int      `json:"samples_dropped"`
	Notes             []string `json:"notes"`
}

// Timings are the phases of a run, in seconds. Wall and Monotonic are the same interval
// measured by the wall clock and by the monotonic clock; they differ when the machine slept
// or the clock was changed, which stalls otherwise unexplained.
type Timings struct {
	// PreRun is the time spent before the clock started: listing the workers and reading their
	// first completed counts. It is not part of the window.
	PreRun           float64 `json:"prerun_seconds"`
	Boot             float64 `json:"boot_seconds"`
	Warmup           float64 `json:"warmup_seconds"`
	Window           float64 `json:"window_seconds"`
	Drain            float64 `json:"drain_seconds"`
	Stats            float64 `json:"stats_seconds"`
	Close            float64 `json:"close_seconds"`
	WallClock        float64 `json:"wall_clock_seconds"`
	MonotonicElapsed float64 `json:"monotonic_seconds"`
}

// WorkerResult is one worker's share of the run. Pointers are nil when not measured.
type WorkerResult struct {
	ID         string   `json:"id"`
	Model      string   `json:"model"`
	Completed  *int64   `json:"completed"`
	MeanQueue  *float64 `json:"mean_queue_depth"`
	MeanActive *float64 `json:"mean_active_requests"`
}

// Imbalance holds the Jain fairness indexes (1 is perfectly even, 1/n is one worker
// holding everything). Evenness is not goodness: with workers of different speeds the
// fastest should serve more.
type Imbalance struct {
	// RequestJain is the index of per-worker completed requests among workers serving the
	// same model; with several models it is the lowest of the per-model indexes.
	RequestJain *float64 `json:"request_jain"`
	// RequestJainPartial is true when some worker's count is missing, so the index describes
	// only the workers that were read.
	RequestJainPartial bool               `json:"request_jain_partial"`
	RequestJainByModel map[string]float64 `json:"request_jain_by_model"`
	// QueueJain is the index of per-worker mean queue depth.
	QueueJain *float64 `json:"queue_jain"`
}

// Input is everything Build turns into a Result.
type Input struct {
	RunID    string
	Metadata Metadata
	Records  []collect.Record
	Window   collect.Window

	Samples           []collect.Sample
	SamplePollsFailed int
	SamplesDropped    int
	// Targets are the workers behind the gateway, when they could be listed.
	Targets []collect.WorkerTarget
	// StartCounts and EndCounts are the workers' completed counts at the start and end of the
	// measurement; StatsMissing names workers whose count could not be read.
	StartCounts, EndCounts collect.WorkerCounts
	StatsMissing           map[string]string
	// Sampled is whether queue sampling was attempted at all.
	Sampled bool
	Notes   []string

	// MaxErrorRate is the error rate above which the run is invalid; negative disables the check.
	MaxErrorRate float64
	Timings      Timings
	// Truncated says the request cap ended the run early; Window is then the cut window and
	// PlannedWindow the one that was asked for.
	Truncated     bool
	PlannedWindow time.Duration
}

// Build computes the result of a run. It is pure.
func Build(in Input) Result {
	r := Result{
		SchemaVersion: SchemaVersion, RunID: in.RunID, Metadata: in.Metadata,
		Summary:           collect.SummarizeRecords(in.Records, in.Window),
		SamplePollsFailed: in.SamplePollsFailed, SamplesDropped: in.SamplesDropped,
		NotMeasured: map[string]string{}, Notes: append([]string{}, in.Notes...), Timings: in.Timings,
		InvalidReasons: []string{}, Warnings: []string{}, StatsMissing: map[string]string{},
		Imbalance: Imbalance{RequestJainByModel: map[string]float64{}},
	}
	if r.Summary.Latency == nil {
		r.NotMeasured["latency"] = "no request succeeded"
	}
	if r.Summary.TTFT == nil {
		r.NotMeasured["ttft"] = "no streaming request succeeded; non-streaming requests have latency only"
	}

	r.Queue = collect.SummarizeQueue(in.Samples, in.Window)
	switch {
	case r.Queue != nil:
		if r.Queue.GPUMean != nil {
			r.GPUUtilization = r.Queue.GPUMean
		}
	case !in.Sampled:
		r.NotMeasured["queue"] = "no control plane to sample (pass --control-plane with --target)"
	default:
		r.NotMeasured["queue"] = "no worker sample fell inside the measurement window"
	}
	if r.GPUUtilization == nil {
		r.NotMeasured["gpu_utilization"] = "workers did not report GPU utilization (mock workers have no GPU)"
	}
	if j, ok := r.Queue.QueueJain(); ok {
		r.Imbalance.QueueJain = &j
	} else if r.Queue != nil {
		r.NotMeasured["queue_imbalance"] = "every worker's queue stayed empty, so the index is undefined"
	} else {
		r.NotMeasured["queue_imbalance"] = r.NotMeasured["queue"]
	}

	queueOf := map[string]collect.WorkerQueue{}
	if r.Queue != nil {
		for _, w := range r.Queue.Workers {
			queueOf[w.ID] = w
		}
	}
	delta := in.EndCounts.Delta(in.StartCounts)
	targets := append([]collect.WorkerTarget(nil), in.Targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	byModel := map[string][]float64{}
	for _, t := range targets {
		w := WorkerResult{ID: t.ID, Model: t.Model}
		if n, ok := delta[t.ID]; ok {
			c := n
			w.Completed = &c
			byModel[t.Model] = append(byModel[t.Model], float64(n))
		}
		if q, ok := queueOf[t.ID]; ok {
			mq, ma := q.MeanQueue, q.MeanActive
			w.MeanQueue, w.MeanActive = &mq, &ma
		}
		r.Workers = append(r.Workers, w)
	}
	r.requestImbalance(byModel, len(targets), in.StatsMissing)
	for _, t := range targets {
		if _, ok := delta[t.ID]; !ok {
			why := in.StatsMissing[t.ID]
			if why == "" {
				why = "no start or end count"
			}
			r.StatsMissing[t.ID] = why
		}
	}
	if len(r.StatsMissing) > 0 {
		r.Imbalance.RequestJainPartial = r.Imbalance.RequestJain != nil
		ids := make([]string, 0, len(r.StatsMissing))
		for id := range r.StatsMissing {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		r.Notes = append(r.Notes, fmt.Sprintf("Request balance is partial: %d of %d workers have no usable completed count (%s).",
			len(ids), len(targets), strings.Join(ids, ", ")))
	}
	r.validate(in.MaxErrorRate, in)
	return r
}

// lateStart is how long after the run began the first request may be sent before the window
// is considered shortened: the load should start at once, and pre-run work happens before the
// clock starts.
func lateStart(in Input) time.Duration {
	return max(time.Second, (in.Window.End)/20)
}

// validate sets Valid, InvalidReasons and Warnings. A maxErrorRate of 0 tolerates no failure at
// all; a negative one disables the check.
func (r *Result) validate(maxErrorRate float64, in Input) {
	s := r.Summary
	if s.Sent == 0 {
		r.InvalidReasons = append(r.InvalidReasons, "no request was measured (sent 0): none was due inside the measurement window")
	}
	if maxErrorRate >= 0 && s.Sent > 0 && s.ErrorRate > maxErrorRate {
		r.InvalidReasons = append(r.InvalidReasons, fmt.Sprintf(
			"error rate %.2f%% (%d of %d requests failed) exceeds %.2f%%: latency, TTFT and balance describe only the %d survivors; the load probably exceeded what the target can take",
			s.ErrorRate*100, s.Failed, s.Sent, maxErrorRate*100, s.Succeeded))
	}
	if first, ok := firstStart(in.Records); ok && first > lateStart(in) {
		r.InvalidReasons = append(r.InvalidReasons, fmt.Sprintf(
			"the first request was sent %s after the run began, so the %s window was shortened by about that much and the throughput (divided by the full window) is understated",
			FormatDuration(first), FormatDuration(in.Window.End-in.Window.Start)))
	}
	if in.Truncated {
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"the window was cut from %s to %s by --max-requests: throughput divides by the cut window, and %d of the %d requests counted in it were sent during warm-up",
			FormatDuration(in.PlannedWindow), FormatDuration(in.Window.End-in.Window.Start), s.WarmupCompletedInWindow, s.CompletedInWindow))
	}
	r.Valid = len(r.InvalidReasons) == 0
}

// firstStart is when the earliest request was actually sent.
func firstStart(recs []collect.Record) (time.Duration, bool) {
	if len(recs) == 0 {
		return 0, false
	}
	first := recs[0].Started
	for _, r := range recs[1:] {
		first = min(first, r.Started)
	}
	return first, true
}

// FormatDuration prints a duration with a sensible precision: milliseconds below ten seconds,
// otherwise seconds with one decimal.
func FormatDuration(d time.Duration) string {
	if d < 10*time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1f s", d.Seconds())
}

func (r *Result) requestImbalance(byModel map[string][]float64, targets int, missing map[string]string) {
	var headline *float64
	for m, xs := range byModel {
		if len(xs) < 2 {
			continue
		}
		if j, ok := collect.Jain(xs); ok {
			r.Imbalance.RequestJainByModel[m] = j
			if headline == nil || j < *headline {
				v := j
				headline = &v
			}
		}
	}
	r.Imbalance.RequestJain = headline
	if headline != nil {
		return
	}
	switch {
	case targets == 0:
		r.NotMeasured["request_imbalance"] = "the workers behind the gateway could not be listed (pass --control-plane with --target)"
	case len(missing) > 0 && len(byModel) == 0:
		r.NotMeasured["request_imbalance"] = "the workers' /stats could not be read"
	default:
		r.NotMeasured["request_imbalance"] = "needs at least two workers serving one model with at least one completed request"
	}
}

// Dir files.
const (
	ResultFile   = "result.json"
	ReportFile   = "report.md"
	RequestsFile = "requests.jsonl"
)

// Write persists the result as result.json and report.md in dir, and requests.jsonl when
// records is non-nil.
func Write(dir string, r Result, records []collect.Record) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, ResultFile), append(b, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, ReportFile), []byte(Markdown(r)), 0o644); err != nil {
		return err
	}
	if records == nil {
		return nil
	}
	f, err := os.Create(filepath.Join(dir, RequestsFile))
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, rec := range records {
		if err := enc.Encode(rec); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

// maxResultBytes bounds a result file that is read.
const maxResultBytes = 64 << 20

// Load reads a result from a run directory, or from a result.json path. It refuses symbolic
// links and anything that is not a regular file, and no error message contains file content.
func Load(path string) (Result, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return Result{}, err
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return Result{}, fmt.Errorf("%s is a symbolic link: refusing to follow it", path)
	}
	if st.IsDir() {
		path = filepath.Join(path, ResultFile)
		if st, err = os.Lstat(path); err != nil {
			return Result{}, err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return Result{}, fmt.Errorf("%s is a symbolic link: refusing to follow it", path)
		}
	}
	if !st.Mode().IsRegular() {
		return Result{}, fmt.Errorf("%s is not a regular file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, maxResultBytes+1))
	if err != nil {
		return Result{}, err
	}
	if len(b) > maxResultBytes {
		return Result{}, fmt.Errorf("%s: larger than %d MiB", path, maxResultBytes>>20)
	}
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		// Decoder errors can quote the offending bytes, so only positions and field names are kept.
		var syn *json.SyntaxError
		var typ *json.UnmarshalTypeError
		switch {
		case errors.As(err, &syn):
			return Result{}, fmt.Errorf("%s: not valid JSON (error at byte %d)", path, syn.Offset)
		case errors.As(err, &typ):
			return Result{}, fmt.Errorf("%s: does not match the result schema (field %s)", path, Clean(typ.Field))
		default:
			return Result{}, fmt.Errorf("%s: could not be decoded", path)
		}
	}
	if r.SchemaVersion != SchemaVersion {
		return Result{}, fmt.Errorf("%s: schema version %d, this harness reads %d", path, r.SchemaVersion, SchemaVersion)
	}
	return r, nil
}

// Resolve finds a run under base by ID, or accepts a path to a run directory or file.
func Resolve(base, ref string) (string, error) {
	if _, ok := ParseRunID(ref); ok {
		p := filepath.Join(base, ref)
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("run %s not found in %s", ref, base)
		}
		return p, nil
	}
	if _, err := os.Stat(ref); err != nil {
		return "", fmt.Errorf("%q is neither a run ID (run_001) nor a path: %w", ref, err)
	}
	return ref, nil
}

// RunDir returns the run directory a resolved path (a run directory or its result.json)
// belongs to.
func RunDir(path string) string {
	if st, err := os.Stat(path); err == nil && !st.IsDir() {
		return filepath.Dir(path)
	}
	return path
}

// LoadGroup returns the runs in the same repeat group as r, r included, in run order. The
// group is looked for in base, the directory the run itself was found in (never a default
// one). A run without repeats is its own group. Sibling directories that are not runs, and
// siblings that cannot be read (corrupt, another schema, too large) are skipped; the
// warnings name them, so one bad directory cannot block a comparison.
func LoadGroup(base string, r Result) (group []Result, warnings []string, err error) {
	if r.Metadata.Repeat.Of <= 1 || r.Metadata.Repeat.Group == "" {
		return []Result{r}, nil, nil
	}
	ids, err := RunIDs(base) // only directories named run_NNN
	if err != nil {
		return nil, nil, err
	}
	for _, id := range ids {
		g, err := Load(filepath.Join(base, id))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipped %s: %v", id, err))
			continue
		}
		if g.Metadata.Repeat.Group == r.Metadata.Repeat.Group {
			group = append(group, g)
		}
	}
	if len(group) == 0 {
		return []Result{r}, warnings, nil
	}
	return group, warnings, nil
}
