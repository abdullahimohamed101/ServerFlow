// Package driver sends a workload to a gateway and records what happened to each request.
//
// Closed-loop mode runs a fixed number of clients, each sending its next request when its
// last one ends: offered load falls when the server slows down. Open-loop mode sends
// requests on a schedule whatever the responses do. In both, a request's latency is measured
// from its intended send time, so a slow server (or a slow load generator) cannot hide
// its own queueing by delaying the next request. See ADR-013.
package driver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/workload"
)

// Mode is how load is offered.
type Mode string

// Modes.
const (
	Closed Mode = "closed"
	Open   Mode = "open"
)

// Error classes recorded for a request that did not succeed. A response with a non-2xx
// status has no class here; the summary classes it by status.
const (
	ErrConnect          = "connect"
	ErrTimeout          = "timeout"
	ErrTransport        = "transport"
	ErrRead             = "read_error"
	ErrStreamIncomplete = "stream_incomplete"
	ErrStreamError      = "stream_error"
	ErrBadBody          = "bad_body"
	ErrDrainTimeout     = "drain_timeout"
)

// Defaults.
const (
	DefaultRequestTimeout = 120 * time.Second
	DefaultDrainTimeout   = 60 * time.Second
	DefaultMaxRequests    = 500000

	maxBodyBytes   = 16 << 20
	headerAttempts = "X-ServerFlow-Attempts"
)

// Config configures a run.
type Config struct {
	// BaseURL is the gateway, for example http://127.0.0.1:8080.
	BaseURL  string
	Workload *workload.Workload
	Mode     Mode
	// Concurrency is the number of clients in closed-loop mode.
	Concurrency int
	// Segments is the arrival schedule in open-loop mode; MaxInFlight bounds requests in
	// flight (the scheduler waits for a slot, and the wait counts toward latency).
	Segments    []workload.Segment
	MaxInFlight int

	Warmup, Duration time.Duration
	// RequestTimeout bounds one request. DrainTimeout bounds the wait, after the window ends,
	// for requests still in flight; those still running are cancelled and recorded as failed.
	RequestTimeout, DrainTimeout time.Duration
	// MaxRequests caps the requests issued, bounding memory; the run ends early when it is reached.
	MaxRequests int

	// T0 is the run's time zero; the zero value means now. Samplers share it.
	T0 time.Time
	// Client is the HTTP client; nil gets a default without proxies and with room for Concurrency
	// idle connections.
	Client *http.Client
}

// Output is what a run produced.
type Output struct {
	Records []collect.Record
	T0      time.Time
	// Window is the measurement window (warm-up excluded). When the request cap ended the run
	// early, its End is cut back to the last request issued and Truncated is set.
	Window    collect.Window
	Truncated bool
	// MaxStartLag is the longest an open-loop request waited between its intended and actual
	// send, that is, how far the load generator itself fell behind.
	MaxStartLag time.Duration
}

// Validate reports the first problem with c.
func (c *Config) Validate() error {
	switch {
	case c.BaseURL == "":
		return errors.New("driver: no target URL")
	case c.Workload == nil:
		return errors.New("driver: no workload")
	case c.Duration <= 0:
		return errors.New("driver: duration must be positive")
	case c.Warmup < 0:
		return errors.New("driver: warm-up must not be negative")
	}
	switch c.Mode {
	case Closed:
		if c.Concurrency < 1 {
			return errors.New("driver: closed-loop mode needs concurrency of at least 1")
		}
	case Open:
		if len(c.Segments) == 0 {
			return errors.New("driver: open-loop mode needs an arrival schedule")
		}
		if c.MaxInFlight < 1 {
			return errors.New("driver: open-loop mode needs a positive in-flight limit")
		}
	default:
		return fmt.Errorf("driver: unknown mode %q", c.Mode)
	}
	return nil
}

type runner struct {
	cfg    Config
	client *http.Client
	t0     time.Time
	end    time.Duration // when no new request may start: warm-up + duration

	reqCtx    context.Context // cancelled to abandon requests still in flight after the drain timeout
	cancelReq context.CancelFunc

	mu      sync.Mutex
	records []collect.Record
	lag     time.Duration
	issued  atomic.Int64
	stopAt  atomic.Int64 // offset (ns) at which the request cap was hit, 0 if not
}

// Run drives the workload and returns every request's record. It returns early with
// ctx's error if ctx is cancelled; the records so far are still returned.
func Run(ctx context.Context, cfg Config) (*Output, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = DefaultMaxRequests
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := &runner{cfg: cfg, client: cfg.Client, t0: cfg.T0, end: cfg.Warmup + cfg.Duration}
	if r.t0.IsZero() {
		r.t0 = time.Now()
	}
	if r.client == nil {
		r.client = DefaultClient(max(cfg.Concurrency, cfg.MaxInFlight))
	}
	r.reqCtx, r.cancelReq = context.WithCancel(context.WithoutCancel(ctx))
	defer r.cancelReq()
	// Parent cancellation abandons everything at once; the end of the window starts the drain clock.
	stopWatch := context.AfterFunc(ctx, r.cancelReq)
	defer stopWatch()
	drain := time.AfterFunc(time.Until(r.t0.Add(r.end))+cfg.DrainTimeout, r.cancelReq)
	defer drain.Stop()

	if cfg.Mode == Closed {
		r.runClosed(ctx)
	} else {
		r.runOpen(ctx)
	}
	out := &Output{Records: r.records, T0: r.t0, MaxStartLag: r.lag,
		Window: collect.Window{Start: cfg.Warmup, End: r.end}}
	if at := time.Duration(r.stopAt.Load()); at > 0 {
		out.Truncated, out.Window.End = true, max(at, cfg.Warmup)
	}
	return out, ctx.Err()
}

// DefaultClient is an HTTP client for load generation: no proxy (a proxy named in the
// environment would sit in the measurement), no compression, many idle connections.
func DefaultClient(conns int) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableCompression = true
	tr.MaxIdleConns = conns + 16
	tr.MaxIdleConnsPerHost = conns + 16
	tr.MaxConnsPerHost = 0
	return &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// take reserves the next request index, or reports that the cap is reached.
func (r *runner) take() (int, bool) {
	n := int(r.issued.Add(1)) - 1
	if n >= r.cfg.MaxRequests {
		r.stopAt.CompareAndSwap(0, max(1, int64(time.Since(r.t0))))
		return 0, false
	}
	return n, true
}

func (r *runner) runClosed(ctx context.Context) {
	var wg sync.WaitGroup
	for range r.cfg.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if time.Since(r.t0) >= r.end {
					return
				}
				i, ok := r.take()
				if !ok {
					return
				}
				r.add(r.send(r.cfg.Workload.Request(i), -1))
			}
		}()
	}
	wg.Wait()
}

func (r *runner) runOpen(ctx context.Context) {
	sem := make(chan struct{}, r.cfg.MaxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; ctx.Err() == nil; i++ {
		due, ok := workload.ArrivalTime(r.cfg.Segments, i)
		if !ok || due >= r.end {
			return
		}
		if wait := time.Until(r.t0.Add(due)); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if _, ok := r.take(); !ok {
			<-sem
			return
		}
		wg.Add(1)
		go func(i int, due time.Duration) {
			defer wg.Done()
			defer func() { <-sem }()
			rec := r.send(r.cfg.Workload.Request(i), due)
			r.add(rec)
		}(i, due)
	}
}

func (r *runner) add(rec collect.Record) {
	r.mu.Lock()
	r.records = append(r.records, rec)
	if lag := rec.Started - rec.Intended; lag > r.lag {
		r.lag = lag
	}
	r.mu.Unlock()
}

// send performs one request that was due at intended and records it. A negative intended
// means due now (closed loop): the intended time is the moment of sending.
func (r *runner) send(req workload.Request, intended time.Duration) collect.Record {
	started := time.Since(r.t0)
	if intended < 0 {
		intended = started
	}
	rec := collect.Record{
		Seq: req.Seq, Tenant: req.Tenant, Model: req.Model, Stream: req.Stream,
		PlannedInputTokens: req.InputTokens, MaxTokens: req.MaxTokens,
		Intended: intended, Started: started,
		InputTokens: req.InputTokens,
	}
	body, err := workload.ChatBody(req)
	if err != nil {
		rec.ErrClass, rec.Done = ErrTransport, time.Since(r.t0)
		return rec
	}
	ctx, cancel := context.WithTimeout(r.reqCtx, r.cfg.RequestTimeout)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.cfg.BaseURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		rec.ErrClass, rec.Done = ErrTransport, time.Since(r.t0)
		return rec
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Authorization", "Bearer "+workload.APIKey(req.Tenant))
	if req.Stream {
		hreq.Header.Set("Accept", "text/event-stream")
	}

	resp, err := r.client.Do(hreq)
	if err != nil {
		rec.ErrClass = r.classify(ctx, err)
		rec.Done = time.Since(r.t0)
		return rec
	}
	defer func() { _ = resp.Body.Close() }()
	rec.Status = resp.StatusCode
	if v := resp.Header.Get(headerAttempts); v != "" {
		rec.Attempts, _ = strconv.Atoi(v)
	}
	switch {
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
	case req.Stream:
		r.readStream(ctx, resp.Body, &rec)
	default:
		r.readBody(ctx, resp.Body, &rec)
	}
	rec.Done = time.Since(r.t0)
	return rec
}

// classify names why a transport-level call failed.
func (r *runner) classify(ctx context.Context, err error) string {
	switch {
	case r.reqCtx.Err() != nil && time.Since(r.t0) > r.end:
		return ErrDrainTimeout
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return ErrTimeout
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return ErrConnect
	}
	return ErrTransport
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

func (u *usage) apply(rec *collect.Record) {
	if u == nil || (u.PromptTokens == 0 && u.CompletionTokens == 0) {
		return
	}
	rec.InputTokens, rec.OutputTokens, rec.UsageReported = u.PromptTokens, u.CompletionTokens, true
}

func (r *runner) readBody(ctx context.Context, body io.Reader, rec *collect.Record) {
	b, err := io.ReadAll(io.LimitReader(body, maxBodyBytes))
	if err != nil {
		rec.ErrClass = r.classify(ctx, err)
		if rec.ErrClass == ErrTransport {
			rec.ErrClass = ErrRead
		}
		return
	}
	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage *usage `json:"usage"`
	}
	if json.Unmarshal(b, &resp) != nil || len(resp.Choices) == 0 {
		rec.ErrClass = ErrBadBody
		return
	}
	rec.OutputTokens = (len(resp.Choices[0].Message.Content) + 3) / 4 // about four characters per token
	resp.Usage.apply(rec)
}

// readStream reads server-sent events. It notes when the first content chunk arrives and
// requires the stream to end with [DONE] and without an error event.
func (r *runner) readStream(ctx context.Context, body io.Reader, rec *collect.Record) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	chunks, done := 0, false
	var u *usage
	for sc.Scan() {
		line := sc.Text()
		payload, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		payload = strings.TrimSpace(payload)
		if payload == "[DONE]" {
			done = true
			continue
		}
		var ev struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *usage          `json:"usage"`
			Error json.RawMessage `json:"error"`
		}
		if json.Unmarshal([]byte(payload), &ev) != nil {
			continue
		}
		if len(ev.Error) > 0 && string(ev.Error) != "null" {
			rec.ErrClass = ErrStreamError
			continue
		}
		if len(ev.Choices) > 0 && ev.Choices[0].Delta.Content != "" {
			chunks++
			if rec.FirstByte == 0 {
				rec.FirstByte = time.Since(r.t0)
			}
		}
		if ev.Usage != nil {
			u = ev.Usage
		}
	}
	if err := sc.Err(); err != nil && rec.ErrClass == "" {
		rec.ErrClass = r.classify(ctx, err)
		if rec.ErrClass == ErrTransport {
			rec.ErrClass = ErrRead
		}
		return
	}
	if rec.ErrClass == "" && !done {
		rec.ErrClass = ErrStreamIncomplete
	}
	rec.OutputTokens = chunks // one chunk per token, as vLLM and the mock worker stream
	u.apply(rec)
}
