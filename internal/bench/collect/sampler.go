package collect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"serverflow/internal/registry/client"
	"serverflow/pkg/protocol"
)

// maxEntries bounds the memory the sampler may use, counted in worker entries (a sample of
// 64 workers is 64 entries); a run that outlasts it keeps the first samples and counts the
// rest as dropped.
const maxEntries = 2000000

// WorkerSample is one worker's load at one instant, as the control plane reports it.
type WorkerSample struct {
	ID     string   `json:"id"`
	Model  string   `json:"model"`
	Active int      `json:"active"`
	Queue  int      `json:"queue"`
	GPU    *float64 `json:"gpu_utilization,omitempty"`
}

// Sample is every worker's load at one instant, as an offset from T0.
type Sample struct {
	At      time.Duration  `json:"at_ns"`
	Workers []WorkerSample `json:"workers"`
}

// FetchFunc reads the current worker load.
type FetchFunc func(ctx context.Context) ([]WorkerSample, error)

// ControlPlaneFetch reads worker load from a control plane's GET /v1/workers.
func ControlPlaneFetch(cp *client.Client) FetchFunc {
	return func(ctx context.Context) ([]WorkerSample, error) {
		ws, err := cp.Workers(ctx, client.Query{})
		if err != nil {
			return nil, err
		}
		out := make([]WorkerSample, 0, len(ws))
		for _, w := range ws {
			if w.State != protocol.StateReady {
				continue
			}
			out = append(out, WorkerSample{ID: w.WorkerID, Model: w.Model, Active: w.Metrics.ActiveRequests,
				Queue: w.Metrics.QueueDepth, GPU: w.Metrics.GPUUtilization})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return out, nil
	}
}

// Sampler polls worker load while a run is going.
type Sampler struct {
	fetch    FetchFunc
	interval time.Duration
	t0       time.Time

	mu      sync.Mutex
	samples []Sample
	entries int
	limit   int
	errs    int
	dropped int
}

// NewSampler returns a sampler that calls fetch every interval, with times measured
// from t0.
func NewSampler(fetch FetchFunc, interval time.Duration, t0 time.Time) *Sampler {
	return &Sampler{fetch: fetch, interval: interval, t0: t0, limit: maxEntries}
}

// Run samples until ctx is done. A failed poll is counted, never fatal: a control plane
// that stops answering must not stop the load test.
func (s *Sampler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		s.once(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Sampler) once(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, max(s.interval, time.Second))
	defer cancel()
	ws, err := s.fetch(cctx)
	at := time.Since(s.t0)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		s.errs++
	case s.entries+len(ws)+1 > s.limit:
		s.dropped++
	default:
		s.entries += len(ws) + 1
		s.samples = append(s.samples, Sample{At: at, Workers: ws})
	}
}

// Samples returns what was collected, in time order, and how many polls failed or were
// dropped for lack of room.
func (s *Sampler) Samples() (samples []Sample, failed, dropped int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Sample(nil), s.samples...), s.errs, s.dropped
}

// WorkerQueue is one worker's time-averaged load over the window.
type WorkerQueue struct {
	ID         string  `json:"id"`
	Model      string  `json:"model"`
	MeanQueue  float64 `json:"mean_queue_depth"`
	MeanActive float64 `json:"mean_active_requests"`
	Samples    int     `json:"samples"`
}

// QueueSummary describes queue behaviour over the window.
type QueueSummary struct {
	Samples int `json:"samples"`
	// TotalQueue is the queue depth summed over workers, per sample.
	AvgTotalQueue float64 `json:"avg_total_queue_depth"`
	P95TotalQueue float64 `json:"p95_total_queue_depth"`
	MaxTotalQueue float64 `json:"max_total_queue_depth"`
	AvgActive     float64 `json:"avg_total_active_requests"`
	// GPU is the mean and maximum GPU utilization workers reported, nil when none did.
	GPUMean *float64      `json:"gpu_utilization_mean,omitempty"`
	GPUMax  *float64      `json:"gpu_utilization_max,omitempty"`
	Workers []WorkerQueue `json:"workers"`
}

// SummarizeQueue summarizes samples taken inside the window. It returns nil when none
// fall inside it.
func SummarizeQueue(samples []Sample, w Window) *QueueSummary {
	var totals, actives []float64
	type acc struct {
		model         string
		queue, active float64
		n             int
	}
	per := map[string]*acc{}
	var gpus []float64
	for _, s := range samples {
		if !w.Contains(s.At) {
			continue
		}
		var tq, ta float64
		for _, ws := range s.Workers {
			tq += float64(ws.Queue)
			ta += float64(ws.Active)
			a := per[ws.ID]
			if a == nil {
				a = &acc{model: ws.Model}
				per[ws.ID] = a
			}
			a.queue += float64(ws.Queue)
			a.active += float64(ws.Active)
			a.n++
			if ws.GPU != nil {
				gpus = append(gpus, *ws.GPU)
			}
		}
		totals = append(totals, tq)
		actives = append(actives, ta)
	}
	if len(totals) == 0 {
		return nil
	}
	d := Summarize(totals)
	q := &QueueSummary{Samples: len(totals), AvgTotalQueue: d.Mean, P95TotalQueue: d.P95, MaxTotalQueue: d.Max}
	q.AvgActive, _ = Mean(actives)
	ids := make([]string, 0, len(per))
	for id := range per {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		a := per[id]
		q.Workers = append(q.Workers, WorkerQueue{ID: id, Model: a.model, MeanQueue: a.queue / float64(a.n),
			MeanActive: a.active / float64(a.n), Samples: a.n})
	}
	if len(gpus) > 0 {
		m, _ := Mean(gpus)
		mx := Summarize(gpus).Max
		q.GPUMean, q.GPUMax = &m, &mx
	}
	return q
}

// QueueJain is the Jain index of the workers' mean queue depths (queue imbalance). ok is
// false when it is undefined, which includes every queue having stayed empty.
func (q *QueueSummary) QueueJain() (float64, bool) {
	if q == nil {
		return 0, false
	}
	xs := make([]float64, 0, len(q.Workers))
	for _, w := range q.Workers {
		xs = append(xs, w.MeanQueue)
	}
	return Jain(xs)
}

// WorkerCounts maps worker ID to the number of requests it has completed.
type WorkerCounts map[string]int64

// WorkerTarget names a worker whose /stats can be read.
type WorkerTarget struct {
	ID, Model, Address string
	// State is the worker's registry state when known ("" for an embedded worker).
	State string
}

// maxStatsBytes bounds a /stats response.
const maxStatsBytes = 1 << 20

// statsDeadline bounds reading every worker's /stats together.
const statsDeadline = 10 * time.Second

// FetchCompleted reads each worker's completed-request count from its /stats endpoint (the
// mock worker serves one), all workers in parallel under one overall deadline. Workers that
// cannot be read are returned in missing with the reason, and left out of the counts.
func FetchCompleted(ctx context.Context, hc *http.Client, targets []WorkerTarget) (counts WorkerCounts, missing map[string]string) {
	counts, missing = WorkerCounts{}, map[string]string{}
	ctx, cancel := context.WithTimeout(ctx, statsDeadline)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := fetchCompleted(ctx, hc, t.Address)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				missing[t.ID] = err.Error()
				return
			}
			counts[t.ID] = n
		}()
	}
	wg.Wait()
	return counts, missing
}

func fetchCompleted(ctx context.Context, hc *http.Client, address string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+"/stats", nil)
	if err != nil {
		return 0, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("/stats answered %d", resp.StatusCode)
	}
	var st struct {
		Completed *int64 `json:"completed"`
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxStatsBytes))
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return 0, fmt.Errorf("/stats is not JSON: %w", err)
	}
	if st.Completed == nil {
		return 0, errors.New("/stats has no completed count")
	}
	return *st.Completed, nil
}

// Delta returns end minus start for every worker present in both.
func (c WorkerCounts) Delta(start WorkerCounts) WorkerCounts {
	out := WorkerCounts{}
	for id, e := range c {
		if s, ok := start[id]; ok {
			out[id] = e - s
		}
	}
	return out
}
