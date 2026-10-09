package collect

import (
	"net/http"
	"time"
)

// Record is what the driver observed for one request. Times are offsets from the
// run's start (T0), so records compare across machines and clocks.
type Record struct {
	Seq    int    `json:"seq"`
	Tenant string `json:"tenant"`
	Model  string `json:"model"`
	Stream bool   `json:"stream"`

	// PlannedInputTokens and MaxTokens are what the workload asked for.
	PlannedInputTokens int `json:"planned_input_tokens"`
	MaxTokens          int `json:"max_tokens"`

	// Intended is when the request was due to be sent. Closed-loop clients send as soon as
	// the last request ends, so Intended equals Started; open-loop requests are due on a
	// schedule and Started can be later when the harness itself falls behind. Latency is
	// always measured from Intended, so a slow server cannot hide its queueing.
	Intended time.Duration `json:"intended_ns"`
	Started  time.Duration `json:"started_ns"`
	// FirstByte is when the first content chunk of a streaming response arrived; zero when
	// none did or the request is not streaming.
	FirstByte time.Duration `json:"first_byte_ns"`
	Done      time.Duration `json:"done_ns"`

	// Status is the HTTP status, 0 when no response arrived.
	Status int `json:"status"`
	// ErrClass is empty on success, otherwise one of the classes in the driver.
	ErrClass string `json:"error_class,omitempty"`
	// Attempts is the gateway's X-ServerFlow-Attempts, 0 when it did not say.
	Attempts int `json:"attempts,omitempty"`

	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	// UsageReported is true when the tokens come from the response's usage object, false when
	// they are estimated from the request size and the number of stream chunks.
	UsageReported bool `json:"usage_reported"`
}

// OK reports whether the request succeeded.
func (r Record) OK() bool {
	return r.ErrClass == "" && r.Status >= http.StatusOK && r.Status < http.StatusMultipleChoices
}

// Latency is the time from the intended send to the end of the response.
func (r Record) Latency() time.Duration { return r.Done - r.Intended }

// TTFT is the time from the intended send to the first content chunk, and whether the
// request has one (a successful streaming request that produced content).
func (r Record) TTFT() (time.Duration, bool) {
	if !r.Stream || !r.OK() || r.FirstByte == 0 {
		return 0, false
	}
	return r.FirstByte - r.Intended, true
}

// Window is the measurement window, as offsets from T0.
type Window struct{ Start, End time.Duration }

// Contains reports whether a request due at t counts: Start inclusive, End exclusive.
func (w Window) Contains(t time.Duration) bool { return t >= w.Start && t < w.End }

// Summary is the aggregate of the measured requests.
type Summary struct {
	Sent      int `json:"sent"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	// Warmup is how many requests were excluded because they were due before the window.
	Warmup int `json:"warmup_excluded"`
	// Retried counts measured requests the gateway needed a second attempt for.
	Retried int `json:"retried"`

	StatusClasses map[string]int `json:"status_classes"`
	ErrorClasses  map[string]int `json:"error_classes"`

	// Latency and TTFT are in milliseconds over successful requests; TTFT only over
	// streaming ones. Nil when there is nothing to summarize.
	Latency *Dist `json:"latency_ms"`
	TTFT    *Dist `json:"ttft_ms"`

	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	// UsageRequests and EstimatedRequests count successful requests whose tokens came from
	// the response's usage object versus from request size and chunk counts.
	UsageRequests     int `json:"usage_requests"`
	EstimatedRequests int `json:"estimated_requests"`

	// WindowSeconds is the nominal window length. SpanSeconds is the longer of it and the time
	// until the last measured request finished (the drain).
	WindowSeconds float64 `json:"window_seconds"`
	SpanSeconds   float64 `json:"span_seconds"`

	// CompletedInWindow counts successful requests that finished inside the window, whenever
	// they were sent: warm-up requests that finish inside it count, requests sent inside it
	// that finish in the drain do not. It is the numerator of the headline throughput, which
	// divides by the nominal window, so one slow tail request cannot move it.
	CompletedInWindow int `json:"completed_in_window"`
	// WarmupCompletedInWindow is how many of those were sent during warm-up. They are counted
	// because the throughput is a steady-state rate: work in flight when the window opens is
	// matched by work in flight when it closes. It is shown so a reader can see how much of the
	// figure it is (it matters most when the window is cut short).
	WarmupCompletedInWindow int     `json:"warmup_completed_in_window"`
	RequestsPerSecond       float64 `json:"requests_per_second"`
	InputTokensPerSecond    float64 `json:"input_tokens_per_second"`
	OutputTokensPerSecond   float64 `json:"output_tokens_per_second"`
	// The WithTail figures divide the measured (sent inside the window) successes and their
	// tokens by SpanSeconds, so the drain counts against them. They move with the slowest
	// request and are reported next to the headline, never instead of it.
	RequestsPerSecondWithTail     float64 `json:"requests_per_second_with_tail"`
	InputTokensPerSecondWithTail  float64 `json:"input_tokens_per_second_with_tail"`
	OutputTokensPerSecondWithTail float64 `json:"output_tokens_per_second_with_tail"`
	// ErrorRate is Failed/Sent, 0 when nothing was sent.
	ErrorRate float64 `json:"error_rate"`
}

// StatusClass names the class of an HTTP status for the summary: "2xx", "4xx", ...,
// or "none" when no response arrived.
func StatusClass(status int) string {
	switch {
	case status >= 100 && status < 600:
		return string(rune('0'+status/100)) + "xx"
	default:
		return "none"
	}
}

// SummarizeRecords aggregates records whose Intended time falls in the window. Requests due
// before it are warm-up and only counted; requests due after it are ignored (the driver
// does not issue any). Requests still in flight at the end of the window are included
// because their Intended time is inside it.
func SummarizeRecords(records []Record, w Window) Summary {
	s := Summary{StatusClasses: map[string]int{}, ErrorClasses: map[string]int{}}
	var lat, ttft []float64
	last := w.End
	var winIn, winOut int64
	for _, r := range records {
		if r.OK() && w.Contains(r.Done) {
			s.CompletedInWindow++
			if r.Intended < w.Start {
				s.WarmupCompletedInWindow++
			}
			winIn += int64(r.InputTokens)
			winOut += int64(r.OutputTokens)
		}
		if r.Intended < w.Start {
			s.Warmup++
			continue
		}
		if !w.Contains(r.Intended) {
			continue
		}
		s.Sent++
		s.StatusClasses[StatusClass(r.Status)]++
		if r.Attempts > 1 {
			s.Retried++
		}
		if r.Done > last {
			last = r.Done
		}
		if !r.OK() {
			s.Failed++
			class := r.ErrClass
			if class == "" {
				class = "status_" + StatusClass(r.Status)
			}
			s.ErrorClasses[class]++
			continue
		}
		s.Succeeded++
		lat = append(lat, ms(r.Latency()))
		if t, ok := r.TTFT(); ok {
			ttft = append(ttft, ms(t))
		}
		s.InputTokens += int64(r.InputTokens)
		s.OutputTokens += int64(r.OutputTokens)
		if r.UsageReported {
			s.UsageRequests++
		} else {
			s.EstimatedRequests++
		}
	}
	s.Latency, s.TTFT = Summarize(lat), Summarize(ttft)
	s.WindowSeconds = (w.End - w.Start).Seconds()
	s.SpanSeconds = (last - w.Start).Seconds()
	if s.WindowSeconds > 0 {
		s.RequestsPerSecond = float64(s.CompletedInWindow) / s.WindowSeconds
		s.InputTokensPerSecond = float64(winIn) / s.WindowSeconds
		s.OutputTokensPerSecond = float64(winOut) / s.WindowSeconds
	}
	if s.SpanSeconds > 0 {
		s.RequestsPerSecondWithTail = float64(s.Succeeded) / s.SpanSeconds
		s.InputTokensPerSecondWithTail = float64(s.InputTokens) / s.SpanSeconds
		s.OutputTokensPerSecondWithTail = float64(s.OutputTokens) / s.SpanSeconds
	}
	if s.Sent > 0 {
		s.ErrorRate = float64(s.Failed) / float64(s.Sent)
	}
	return s
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
