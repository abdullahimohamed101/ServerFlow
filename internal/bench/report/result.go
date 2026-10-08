package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

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
	// NotMeasured maps a metric to why it is absent.
	NotMeasured map[string]string `json:"not_measured"`
	// SamplePollsFailed and SamplesDropped say how complete the queue samples are.
	SamplePollsFailed int      `json:"sample_polls_failed"`
	SamplesDropped    int      `json:"samples_dropped"`
	Notes             []string `json:"notes"`
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
	RequestJain        *float64           `json:"request_jain"`
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
}

// Build computes the result of a run. It is pure.
func Build(in Input) Result {
	r := Result{
		SchemaVersion: SchemaVersion, RunID: in.RunID, Metadata: in.Metadata,
		Summary:           collect.SummarizeRecords(in.Records, in.Window),
		SamplePollsFailed: in.SamplePollsFailed, SamplesDropped: in.SamplesDropped,
		NotMeasured: map[string]string{}, Notes: append([]string{}, in.Notes...),
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
	return r
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

// Load reads a result from a run directory, or from a result.json path.
func Load(path string) (Result, error) {
	if st, err := os.Stat(path); err == nil && st.IsDir() {
		path = filepath.Join(path, ResultFile)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Result{}, err
	}
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return Result{}, fmt.Errorf("%s: %w", path, err)
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

// LoadGroup returns every run under base in the same repeat group as r, r included, in
// run order. A run without repeats is its own group.
func LoadGroup(base string, r Result) ([]Result, error) {
	if r.Metadata.Repeat.Of <= 1 || r.Metadata.Repeat.Group == "" {
		return []Result{r}, nil
	}
	ids, err := RunIDs(base)
	if err != nil {
		return nil, err
	}
	var out []Result
	for _, id := range ids {
		g, err := Load(filepath.Join(base, id))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		if g.Metadata.Repeat.Group == r.Metadata.Repeat.Group {
			out = append(out, g)
		}
	}
	if len(out) == 0 {
		return []Result{r}, nil
	}
	return out, nil
}
