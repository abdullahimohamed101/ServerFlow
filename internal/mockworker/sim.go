package mockworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"serverflow/internal/api"
	"serverflow/internal/sim/engine"
	"serverflow/pkg/protocol"
)

// This file is the real-time driver of the simulator engine (--model-engine sim). The engine is a pure
// state machine over virtual time; here virtual time is the wall-clock offset since the server was built.
// There is no goroutine of its own: every request handler that is waiting advances the engine to "now"
// (under one lock) and wakes at the engine's next event, so an idle worker costs nothing and a Server
// needs no Close. Handlers consume the events addressed to them through a per-request stream.

// simDriver serialises access to the engine.
type simDriver struct {
	params  engine.Params
	hash    string
	maxSeqs int

	mu      sync.Mutex
	eng     *engine.Engine
	epoch   time.Time
	streams map[int64]*simStream
}

// simStream queues the events of one request until its handler takes them.
type simStream struct {
	evs    []engine.Event
	notify chan struct{}
	// startAt is the engine's virtual time of the request's admission (its EvStart event). The time to the first token
	// is measured on the same clock, so a handler that wakes late on a slow machine reports the same value.
	startAt time.Duration
}

func newSimDriver(cfg Config) *simDriver {
	p := cfg.SimParams()
	eng, err := engine.New(engine.Config{Params: p, Seed: cfg.Seed, MaxSeqs: cfg.MaxConcurrency, MaxQueue: cfg.QueueSize, OutputMeanTokens: cfg.OutputTokens})
	if err != nil { // Config.Validate checks the same values
		panic(fmt.Sprintf("mockworker: simulator engine: %v", err))
	}
	return &simDriver{params: p, hash: p.Hash(), maxSeqs: cfg.MaxConcurrency, eng: eng, epoch: time.Now(), streams: map[int64]*simStream{}}
}

// advanceLocked moves the engine to the present and hands events to their streams.
func (d *simDriver) advanceLocked() {
	evs, err := d.eng.Advance(time.Since(d.epoch))
	if err != nil { // time is read under the lock from a monotonic clock, so it cannot go backwards
		panic(err)
	}
	d.dispatchLocked(evs)
}

func (d *simDriver) dispatchLocked(evs []engine.Event) {
	for _, ev := range evs {
		if st := d.streams[ev.ID]; st != nil {
			st.evs = append(st.evs, ev)
			// The wake-up is only a latency optimisation: a handler that misses it still wakes at its
			// own timer for the engine's next event. The channel holds one pending signal.
			select {
			case st.notify <- struct{}{}:
			default:
			}
		}
	}
}

// submit enqueues a request and returns its stream.
func (d *simDriver) submit(r engine.Request) (engine.Info, *simStream, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, evs, err := d.eng.Submit(time.Since(d.epoch), r)
	if err != nil {
		d.dispatchLocked(evs)
		return engine.Info{}, nil, err
	}
	st := &simStream{notify: make(chan struct{}, 1)}
	d.streams[info.ID] = st
	d.dispatchLocked(evs)
	return info, st, nil
}

// release cancels the request if it is still live and forgets its stream. Safe to call twice.
func (d *simDriver) release(id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, evs, err := d.eng.Cancel(time.Since(d.epoch), id); err == nil {
		d.dispatchLocked(evs)
	}
	delete(d.streams, id)
}

// next returns the next event of the stream, waiting for the engine (driving it) or ctx.
func (d *simDriver) next(ctx context.Context, st *simStream) (engine.Event, error) {
	for {
		d.mu.Lock()
		d.advanceLocked()
		if len(st.evs) > 0 {
			ev := st.evs[0]
			st.evs = st.evs[1:]
			d.mu.Unlock()
			return ev, nil
		}
		at, busy := d.eng.NextEvent()
		wait := 50 * time.Millisecond // an idle engine with a live request cannot happen; do not hang if it does
		if busy {
			wait = max(time.Until(d.epoch.Add(at)), 0)
		}
		d.mu.Unlock()

		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-st.notify:
			t.Stop()
		case <-ctx.Done():
			t.Stop()
			return engine.Event{}, ctx.Err()
		}
	}
}

// stats advances the engine and returns its statistics.
func (d *simDriver) stats() engine.Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.advanceLocked()
	return d.eng.Stats()
}

// SimStats is the simulator section of /stats; present only with --model-engine sim.
type SimStats struct {
	Engine         string `json:"engine"`
	Profile        string `json:"profile"`
	Provenance     string `json:"provenance"`
	ParamsHash     string `json:"params_hash"`
	Prefilling     int    `json:"prefilling"`
	Running        int    `json:"running"`
	KVUsedTokens   int    `json:"kv_used_tokens"`
	KVBudgetTokens int    `json:"kv_budget_tokens"`
	Preemptions    int64  `json:"preemptions"`
	KVExhausted    int64  `json:"kv_exhausted"`
}

// fillSimStats overlays the simulator's view on the worker statistics.
func (s *Server) fillSimStats(st *Stats) {
	e := s.sim.stats()
	st.ActiveRequests, st.QueueDepth, st.QueuedInputTokens = e.Active, e.QueueDepth, e.QueuedInputTokens
	st.RecentTokensPerSecond = e.RecentTokensPerSecond
	st.ConfiguredTTFTMillis, st.ConfiguredTokensPerSecond = 0, 0 // no constant TTFT or speed exists in this mode
	util, mem := e.GPUUtilization, e.GPUMemoryUsedMB
	st.GPUUtilization, st.GPUMemoryUsedMB = &util, &mem
	st.Sim = &SimStats{
		Engine: ModelEngineSim, Profile: s.sim.params.Name, Provenance: s.sim.params.Provenance, ParamsHash: s.sim.hash,
		Prefilling: e.Prefilling, Running: e.Running, KVUsedTokens: e.KVUsedTokens, KVBudgetTokens: e.KVBudgetTokens,
		Preemptions: e.Preemptions, KVExhausted: e.KVExhausted,
	}
}

// chatSim serves a request that has passed validation and failure injection, timing it with the
// simulator instead of constant TTFT and speed. The response shapes, header timing (headers go out when
// the request gets a slot) and bookkeeping match the default path.
func (s *Server) chatSim(w http.ResponseWriter, r *http.Request, ireq *protocol.InferenceRequest, rl *chatLog, ts *traceState, mode FailureMode, fail bool) {
	queued := time.Now()
	ts.queueStart(queued)
	info, st, err := s.sim.submit(engine.Request{InputTokens: rl.promptTokens, MaxTokens: ireq.MaxTokens})
	if err != nil {
		rl.queueMillis = time.Since(queued).Milliseconds()
		ts.queueEnd()
		rl.queued, rl.queueDur = true, time.Since(queued)
		switch {
		case errors.Is(err, engine.ErrQueueFull):
			s.engine.recordRejected()
			writeAPIError(w, queueFullError())
		case errors.Is(err, engine.ErrTooLarge):
			writeAPIError(w, api.ErrInvalidRequest("the prompt does not fit in the simulated KV cache"))
		default:
			writeAPIError(w, api.ErrInternal())
		}
		return
	}
	defer s.sim.release(info.ID)

	// Wait in the queue until the engine admits the request.
	for {
		ev, err := s.sim.next(r.Context(), st)
		if err != nil {
			rl.queueMillis = time.Since(queued).Milliseconds()
			ts.queueEnd()
			rl.queued, rl.queueDur = true, time.Since(queued)
			rl.outcome = "cancelled" // the client left while queued
			return
		}
		if ev.Kind == engine.EvStart {
			st.startAt = ev.At
			break
		}
	}
	rl.queueMillis = time.Since(queued).Milliseconds()
	ts.queueEnd()
	rl.queued, rl.queueDur = true, time.Since(queued)
	if !s.faults.delay(r.Context()) {
		rl.outcome = "cancelled"
		return
	}
	s.metrics.inputTokens.Add(int64(rl.promptTokens))

	n := info.OutputTokens
	rl.outputTokens = n
	abortAt := -1
	if fail && mode == ModeMidstream {
		abortAt = min(midstreamTokens, n-1)
	}
	id := "chatcmpl-" + strings.TrimPrefix(protocol.NewRequestID(), "req_")
	created := time.Now()
	var genErr error
	if ireq.Stream {
		genErr = s.simStreamResponse(w, r.Context(), st, rl, id, created, abortAt)
	} else {
		genErr = s.simFullResponse(w, r.Context(), st, rl, id, created, abortAt)
	}
	switch {
	case genErr == nil:
		s.engine.RecordOutcome(true)
		rl.outcome = "ok"
	case errors.Is(genErr, errMidstream):
		s.engine.RecordOutcome(false)
		rl.outcome = "failed"
		panic(http.ErrAbortHandler)
	default:
		s.engine.RecordCancelled()
		rl.outcome = "cancelled"
	}
}

// simTokens feeds emit with each token of the request as the engine produces it and returns the
// finish reason and token count of the end of the request. A preemption only delays tokens.
func (s *Server) simTokens(ctx context.Context, st *simStream, abortAt int, emit func(i int, at time.Duration) error) (finish string, tokens int, err error) {
	for {
		ev, err := s.sim.next(ctx, st)
		if err != nil {
			return "", 0, err
		}
		switch ev.Kind {
		case engine.EvToken:
			if ev.Index == abortAt {
				return "", 0, errMidstream
			}
			s.engine.RecordTokens(1)
			if err := emit(ev.Index, ev.At); err != nil {
				return "", 0, err
			}
		case engine.EvDone:
			return ev.Finish, ev.Index, nil
		}
	}
}

func (s *Server) simStreamResponse(w http.ResponseWriter, ctx context.Context, st *simStream, rl *chatLog, id string, created time.Time, abortAt int) error {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return err
	}
	send := func(c api.ChatCompletion) error {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", c.JSON()); err != nil {
			return err
		}
		return rc.Flush()
	}
	finish, count, err := s.simTokens(ctx, st, abortAt, func(i int, at time.Duration) error {
		if i == 0 {
			// Virtual time from admission to the first token (prefill and the first step), not the wall time since
			// the headers went out: a handler that wakes late would otherwise find the first token already due.
			rl.ttftDur = max(at-st.startAt, 0)
			rl.ttftMillis = rl.ttftDur.Milliseconds()
			rl.firstTokenAt = time.Now()
			if err := send(api.NewChunk(id, s.cfg.Model, created, api.Delta{Role: "assistant", Content: api.Str("")}, "")); err != nil {
				return err
			}
		}
		return send(api.NewChunk(id, s.cfg.Model, created, api.Delta{Content: api.Str(tokenText(i))}, ""))
	})
	if err != nil {
		return err
	}
	rl.outputTokens = count
	if err := send(api.NewChunk(id, s.cfg.Model, created, api.Delta{}, finish)); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	return rc.Flush()
}

func (s *Server) simFullResponse(w http.ResponseWriter, ctx context.Context, st *simStream, rl *chatLog, id string, created time.Time, abortAt int) error {
	var content strings.Builder
	finish, count, err := s.simTokens(ctx, st, abortAt, func(i int, _ time.Duration) error { content.WriteString(tokenText(i)); return nil })
	if errors.Is(err, errMidstream) {
		// Promise a body, deliver half of it, then drop the connection.
		body := api.NewCompletion(id, s.cfg.Model, created, content.String(), "stop",
			api.Usage{PromptTokens: rl.promptTokens, CompletionTokens: abortAt, TotalTokens: rl.promptTokens + abortAt}).JSON()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write(body[:len(body)/2])
		_ = http.NewResponseController(w).Flush()
		return errMidstream
	}
	if err != nil {
		return err
	}
	rl.outputTokens = count
	body := api.NewCompletion(id, s.cfg.Model, created, content.String(), finish,
		api.Usage{PromptTokens: rl.promptTokens, CompletionTokens: count, TotalTokens: rl.promptTokens + count}).JSON()
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(body)
	return err
}
