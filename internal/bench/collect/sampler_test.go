package collect

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/registry/client"
	"serverflow/pkg/protocol"
)

func TestSummarizeQueueIsExactAndHonoursTheWindow(t *testing.T) {
	gpu := 0.5
	samples := []Sample{
		{At: sec(0), Workers: []WorkerSample{{ID: "a", Queue: 100}}}, // before the window
		{At: sec(1), Workers: []WorkerSample{{ID: "a", Queue: 2, Active: 1, GPU: &gpu}, {ID: "b", Queue: 0, Active: 3}}},
		{At: sec(2), Workers: []WorkerSample{{ID: "a", Queue: 4, Active: 1}, {ID: "b", Queue: 0, Active: 3}}},
		{At: sec(3), Workers: []WorkerSample{{ID: "a", Queue: 0}, {ID: "b", Queue: 6}}},
		{At: sec(4), Workers: []WorkerSample{{ID: "a", Queue: 100}}}, // at the window end: excluded
	}
	q := SummarizeQueue(samples, Window{Start: sec(1), End: sec(4)})
	if q == nil || q.Samples != 3 {
		t.Fatalf("%+v", q)
	}
	// Totals 2, 4, 6: mean 4, p95 -> rank 3 -> 6, max 6. Active totals 4, 4, 0: mean 8/3.
	if q.AvgTotalQueue != 4 || q.P95TotalQueue != 6 || q.MaxTotalQueue != 6 || math.Abs(q.AvgActive-8.0/3) > 1e-12 {
		t.Errorf("%+v", q)
	}
	if len(q.Workers) != 2 || q.Workers[0].ID != "a" || q.Workers[0].MeanQueue != 2 || q.Workers[1].MeanQueue != 2 {
		t.Errorf("per worker: %+v", q.Workers)
	}
	if q.GPUMean == nil || *q.GPUMean != 0.5 {
		t.Errorf("gpu: %v", q.GPUMean)
	}
	if j, ok := q.QueueJain(); !ok || j != 1 {
		t.Errorf("both workers averaged a queue of 2: %v %v", j, ok)
	}
}

func TestSummarizeQueueWithNoSamplesIsNilAndAllEmptyQueuesHaveNoJain(t *testing.T) {
	if SummarizeQueue(nil, Window{End: sec(1)}) != nil {
		t.Fatal("no samples in the window must be nil")
	}
	q := SummarizeQueue([]Sample{{At: 0, Workers: []WorkerSample{{ID: "a"}, {ID: "b"}}}}, Window{End: sec(1)})
	if _, ok := q.QueueJain(); ok {
		t.Fatal("an index of all-zero queues is undefined, not 1")
	}
	if q.GPUMean != nil {
		t.Fatal("no worker reported GPU utilization")
	}
	var none *QueueSummary
	if _, ok := none.QueueJain(); ok {
		t.Fatal("nil summary has no index")
	}
}

func TestSamplerCountsFailedPollsAndKeepsGoing(t *testing.T) {
	var n atomic.Int64
	fetch := func(context.Context) ([]WorkerSample, error) {
		if n.Add(1)%2 == 0 {
			return nil, errors.New("down")
		}
		return []WorkerSample{{ID: "a", Queue: 1}}, nil
	}
	s := NewSampler(fetch, 5*time.Millisecond, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, failed, _ := s.Samples()
		if len(got) >= 3 && failed >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sampler stalled: %d samples %d failures", len(got), failed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	got, _, _ := s.Samples()
	for i := 1; i < len(got); i++ {
		if got[i].At < got[i-1].At {
			t.Fatal("samples must be in time order")
		}
	}
}

func TestControlPlaneFetchReadsLoadAndSkipsWorkersThatAreNotReady(t *testing.T) {
	gpu := 0.25
	body := map[string]any{"workers": []protocol.WorkerSnapshot{
		{WorkerID: "b", Model: "m", State: protocol.StateReady, Metrics: protocol.Metrics{ActiveRequests: 2, QueueDepth: 5, GPUUtilization: &gpu}},
		{WorkerID: "a", Model: "m", State: protocol.StateReady},
		{WorkerID: "c", Model: "m", State: protocol.StateLoadingModel},
	}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(body) }))
	defer ts.Close()
	got, err := ControlPlaneFetch(client.New(ts.URL, "", nil))(context.Background())
	if err != nil || len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" || got[1].Queue != 5 || got[1].Active != 2 || *got[1].GPU != 0.25 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestFetchCompletedReadsStatsAndReportsWhatItCannotRead(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stats" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"completed": 42}`))
	}))
	defer good.Close()
	noStats := httptest.NewServer(http.NotFoundHandler())
	defer noStats.Close()
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"x":1}`)) }))
	defer garbage.Close()

	counts, missing := FetchCompleted(context.Background(), http.DefaultClient, []WorkerTarget{
		{ID: "a", Address: good.URL + "/"}, {ID: "b", Address: noStats.URL}, {ID: "c", Address: garbage.URL}, {ID: "d", Address: "http://127.0.0.1:1"},
	})
	if len(counts) != 1 || counts["a"] != 42 {
		t.Fatalf("counts %v", counts)
	}
	if len(missing) != 3 || missing["b"] == "" || missing["c"] == "" || missing["d"] == "" {
		t.Fatalf("missing %v", missing)
	}
}

func TestDeltaIsEndMinusStartForWorkersInBoth(t *testing.T) {
	got := WorkerCounts{"a": 10, "b": 7, "c": 3}.Delta(WorkerCounts{"a": 4, "b": 7})
	if len(got) != 2 || got["a"] != 6 || got["b"] != 0 {
		t.Fatalf("%v", got)
	}
}

func TestSamplerBoundsMemoryByEntriesNotSamples(t *testing.T) {
	ws := make([]WorkerSample, 10)
	s := NewSampler(func(context.Context) ([]WorkerSample, error) { return ws, nil }, time.Second, time.Now())
	s.limit = 35 // room for three samples of 10 workers (11 entries each)
	for range 5 {
		s.once(context.Background())
	}
	got, _, dropped := s.Samples()
	if len(got) != 3 || dropped != 2 {
		t.Fatalf("kept %d dropped %d", len(got), dropped)
	}
}
