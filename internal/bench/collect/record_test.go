package collect

import (
	"math"
	"testing"
	"time"
)

func ok(seq int, intended, done time.Duration) Record {
	return Record{Seq: seq, Intended: intended, Started: intended, Done: done, Status: 200, InputTokens: 10, OutputTokens: 20}
}

func sec(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }

func TestSummaryCountsAndThroughputAreExact(t *testing.T) {
	// Window [2s, 12s). Two warm-up requests (due 0s and 1.9s) are excluded.
	recs := []Record{
		ok(0, 0, sec(1)),
		ok(1, sec(1.9), sec(2.5)),
		ok(2, sec(2), sec(3)),   // due exactly at the window start: counted
		ok(3, sec(5), sec(5.5)), // 500 ms
		ok(4, sec(11.9), sec(14)),
		{Seq: 5, Intended: sec(6), Started: sec(6), Done: sec(6.1), Status: 503, ErrClass: ""},
		{Seq: 6, Intended: sec(7), Started: sec(7), Done: sec(7.1), ErrClass: "connect"},
		ok(7, sec(12), sec(12.5)), // due exactly at the window end: not counted
	}
	s := SummarizeRecords(recs, Window{Start: sec(2), End: sec(12)})
	if s.Warmup != 2 {
		t.Errorf("warm-up excluded = %d, want 2", s.Warmup)
	}
	if s.Sent != 5 || s.Succeeded != 3 || s.Failed != 2 || s.Sent != s.Succeeded+s.Failed {
		t.Fatalf("counts: %+v", s)
	}
	if s.StatusClasses["2xx"] != 3 || s.StatusClasses["5xx"] != 1 || s.StatusClasses["none"] != 1 {
		t.Errorf("status classes: %v", s.StatusClasses)
	}
	if s.ErrorClasses["connect"] != 1 || s.ErrorClasses["status_5xx"] != 1 {
		t.Errorf("error classes: %v", s.ErrorClasses)
	}
	// The drain: the last measured request ended at 14s, so the span is 12s, not the 10s window.
	if s.WindowSeconds != 10 || s.SpanSeconds != 12 {
		t.Errorf("window %v span %v", s.WindowSeconds, s.SpanSeconds)
	}
	// Headline throughput counts successes that finished inside [2s, 12s) over the 10 s window:
	// request 1 (warm-up, done 2.5), 2 (done 3), 3 (done 5.5) finish inside it; request 4 (done 14)
	// is in the drain; 7 is outside. The warm-up request that finishes in the window counts.
	if s.CompletedInWindow != 3 || math.Abs(s.RequestsPerSecond-3.0/10) > 1e-12 ||
		math.Abs(s.OutputTokensPerSecond-60.0/10) > 1e-12 || math.Abs(s.InputTokensPerSecond-30.0/10) > 1e-12 {
		t.Errorf("windowed throughput: %+v", s)
	}
	// With the tail: the 3 measured successes (2, 3, 4) over the 12 s span.
	if math.Abs(s.RequestsPerSecondWithTail-3.0/12) > 1e-12 || math.Abs(s.OutputTokensPerSecondWithTail-60.0/12) > 1e-12 {
		t.Errorf("throughput with tail: %+v", s)
	}
	if math.Abs(s.ErrorRate-0.4) > 1e-12 {
		t.Errorf("error rate = %v", s.ErrorRate)
	}
	// Latencies of the successes: 1000, 500, 2100 ms -> sorted 500, 1000, 2100.
	if s.Latency.Count != 3 || s.Latency.P50 != 1000 || s.Latency.P95 != 2100 || s.Latency.Min != 500 {
		t.Errorf("latency: %+v", s.Latency)
	}
	if s.TTFT != nil {
		t.Errorf("no streaming requests, so no TTFT: %+v", s.TTFT)
	}
}

func TestSpanIsTheWindowWhenEverythingFinishesInside(t *testing.T) {
	s := SummarizeRecords([]Record{ok(0, sec(0), sec(1))}, Window{Start: 0, End: sec(10)})
	if s.SpanSeconds != 10 || s.RequestsPerSecond != 0.1 || s.RequestsPerSecondWithTail != 0.1 {
		t.Fatalf("%+v", s)
	}
}

func TestEmptyWindowHasNoDistributionsAndNoDivisionByZero(t *testing.T) {
	s := SummarizeRecords(nil, Window{Start: 0, End: sec(5)})
	if s.Sent != 0 || s.Latency != nil || s.TTFT != nil || s.ErrorRate != 0 || s.RequestsPerSecond != 0 {
		t.Fatalf("%+v", s)
	}
	z := SummarizeRecords(nil, Window{})
	if z.RequestsPerSecond != 0 || math.IsNaN(z.ErrorRate) {
		t.Fatalf("zero window: %+v", z)
	}
}

func TestLatencyIsMeasuredFromTheIntendedTime(t *testing.T) {
	// Due at 1s, but the harness only managed to send at 4s; the response ended at 5s.
	r := Record{Intended: sec(1), Started: sec(4), Done: sec(5), Status: 200}
	if r.Latency() != 4*time.Second {
		t.Fatalf("latency = %v, want 4s (from intended, not from start)", r.Latency())
	}
}

func TestTTFTOnlyForSuccessfulStreamsWithContent(t *testing.T) {
	good := Record{Stream: true, Intended: sec(1), FirstByte: sec(1.25), Done: sec(2), Status: 200}
	if d, ok := good.TTFT(); !ok || d != 250*time.Millisecond {
		t.Fatalf("got %v %v", d, ok)
	}
	for name, r := range map[string]Record{
		"not a stream": {Intended: sec(1), FirstByte: sec(1.25), Status: 200},
		"no content":   {Stream: true, Intended: sec(1), Status: 200},
		"failed":       {Stream: true, Intended: sec(1), FirstByte: sec(1.25), Status: 200, ErrClass: "stream_incomplete"},
	} {
		if _, ok := r.TTFT(); ok {
			t.Errorf("%s: must have no TTFT", name)
		}
	}
	s := SummarizeRecords([]Record{good, {Seq: 1, Intended: sec(1), Done: sec(2), Status: 200}}, Window{End: sec(10)})
	if s.TTFT == nil || s.TTFT.Count != 1 || s.Latency.Count != 2 {
		t.Fatalf("ttft counts only streams: %+v %+v", s.TTFT, s.Latency)
	}
}

func TestTokenSourcesAreCounted(t *testing.T) {
	a, b := ok(0, 0, sec(1)), ok(1, 0, sec(1))
	a.UsageReported = true
	s := SummarizeRecords([]Record{a, b}, Window{End: sec(10)})
	if s.UsageRequests != 1 || s.EstimatedRequests != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestRetriedCountsRequestsWithMoreThanOneAttempt(t *testing.T) {
	a, b, c := ok(0, 0, sec(1)), ok(1, 0, sec(1)), ok(2, 0, sec(1))
	a.Attempts, b.Attempts = 2, 1
	s := SummarizeRecords([]Record{a, b, c}, Window{End: sec(10)})
	if s.Retried != 1 {
		t.Fatalf("retried = %d", s.Retried)
	}
}

func TestStatusClass(t *testing.T) {
	for in, want := range map[int]string{200: "2xx", 204: "2xx", 404: "4xx", 503: "5xx", 0: "none", 99: "none", 600: "none", -1: "none"} {
		if got := StatusClass(in); got != want {
			t.Errorf("StatusClass(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestOneSlowTailRequestDoesNotMoveTheHeadlineThroughput(t *testing.T) {
	fast := func(tail time.Duration) Summary {
		recs := []Record{ok(0, sec(1), sec(2)), ok(1, sec(3), sec(4)), ok(2, sec(9), sec(9)+tail)}
		return SummarizeRecords(recs, Window{End: sec(10)})
	}
	a, b := fast(sec(0.5)), fast(sec(60))
	// The third request finishes at 9.5 s (inside the window) or at 69 s (in the drain): the
	// headline changes by that one request only, not by the length of the tail.
	if a.RequestsPerSecond != 0.3 || b.RequestsPerSecond != 0.2 {
		t.Fatalf("headline %v and %v", a.RequestsPerSecond, b.RequestsPerSecond)
	}
	if b.RequestsPerSecondWithTail >= a.RequestsPerSecondWithTail/3 {
		t.Fatalf("the with-tail figure is the one that collapses: %v vs %v", a.RequestsPerSecondWithTail, b.RequestsPerSecondWithTail)
	}
}
