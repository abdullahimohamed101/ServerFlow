// Package engine is a deterministic model of an LLM inference engine with continuous batching, used by
// the mock worker's --model-engine sim mode and meant to be reused by an offline simulator.
//
// It is a pure state machine over virtual time: every call carries the current time as a time.Duration
// since an arbitrary epoch, the engine never reads a clock, sleeps, starts a goroutine or takes a lock,
// and all randomness comes from the seed. Given the same Config and the same sequence of calls it
// produces the same events, bit for bit. A real-time driver passes wall-clock offsets; a discrete-event
// driver passes event times. Callers must serialise calls themselves.
//
// The model, in one paragraph. Work proceeds in iterations, one at a time. At the start of each
// iteration waiting requests are admitted in strict arrival order while a sequence slot (Config.MaxSeqs)
// and enough KV-cache tokens (Params.KVBudgetTokens) are free. If an admitted request still has prompt
// tokens to process, the iteration is a prefill of one request (the whole prompt, or one chunk of it),
// costing PrefillFixedMs + PrefillPerTokenMs*tokens; unless chunking is on, the running batch decodes
// nothing meanwhile. Otherwise the iteration is a decode step for the running batch of b sequences,
// costing StepBaseMs + StepPerSeqMs*b, in which every sequence emits one token: the per-request token
// rate falls as the batch grows while the aggregate rate b/(t0+t1*b) saturates at 1/t1. Finishing a
// prefill emits the request's first token. KV use grows by one token per generated token; when a decode
// step cannot get its tokens, the most recently started sequence is preempted (its KV is freed and it
// re-runs its prefill later, like recompute preemption), or, if it is alone, ends with finish "length".
// Timings are multiplied by seeded lognormal noise with mean 1.
package engine

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Errors returned by Submit.
var (
	// ErrQueueFull means the wait queue is full (the mock worker answers 503 queue_full).
	ErrQueueFull = errors.New("engine: queue full")
	// ErrTooLarge means the prompt can never fit in the KV budget.
	ErrTooLarge = errors.New("engine: prompt does not fit in the KV cache")
	// ErrInvalidRequest means the request is malformed (no input tokens, or a negative max_tokens).
	ErrInvalidRequest = errors.New("engine: invalid request")
	// ErrTimeRegression means a call carried a time earlier than the engine's clock.
	ErrTimeRegression = errors.New("engine: time went backwards")
)

// Config is the engine instance: its physics, seed and limits.
type Config struct {
	Params Params
	Seed   int64
	// MaxSeqs is how many sequences may hold a slot (be prefilling or running) at once.
	MaxSeqs int
	// MaxQueue is how many requests may wait beyond the free slots; more get ErrQueueFull.
	MaxQueue int
	// OutputMeanTokens is the mean natural output length (see Params.OutputFraction).
	OutputMeanTokens int
}

const (
	maxSeqsCap   = 1 << 14
	maxQueueCap  = 1 << 20
	maxOutputCap = 1 << 20
	// minIteration keeps every iteration strictly positive so virtual time always advances.
	minIteration = time.Microsecond
	// maxIteration bounds one iteration (a day); iteration end times saturate at the largest Duration
	// instead of wrapping, so event times never go backwards however extreme the time or parameters.
	maxIteration = 24 * time.Hour
	// maxOutput bounds a request's planned output length, so no float-to-int conversion can overflow.
	maxOutput = 1 << 30
	// noiseClamp bounds the lognormal multiplier so one draw cannot stall the engine for hours.
	noiseClamp = 20.0
)

// Validate returns an error describing the first invalid setting.
func (c Config) Validate() error {
	if err := c.Params.Validate(); err != nil {
		return err
	}
	switch {
	case c.MaxSeqs < 1 || c.MaxSeqs > maxSeqsCap:
		return fmt.Errorf("max seqs must be in 1-%d, got %d", maxSeqsCap, c.MaxSeqs)
	case c.MaxQueue < 0 || c.MaxQueue > maxQueueCap:
		return fmt.Errorf("max queue must be in 0-%d, got %d", maxQueueCap, c.MaxQueue)
	case c.OutputMeanTokens < 1 || c.OutputMeanTokens > maxOutputCap:
		return fmt.Errorf("output mean tokens must be in 1-%d, got %d", maxOutputCap, c.OutputMeanTokens)
	}
	return nil
}

// Request is what Submit needs to know about a request. InputTokens counts prompt tokens (at least 1);
// MaxTokens is the client's limit, 0 meaning none.
type Request struct {
	InputTokens int
	MaxTokens   int
}

// Info is what Submit decides about an accepted request. OutputTokens is how many tokens it will
// generate unless it is cancelled or the KV cache runs out first; Finish is "length" when max_tokens
// ends it (including when it equals the natural length) and "stop" otherwise.
type Info struct {
	ID           int64
	OutputTokens int
	Finish       string
}

// EventKind identifies an Event.
type EventKind uint8

const (
	// EvStart: the request left the wait queue and holds a slot (prefill is next).
	EvStart EventKind = iota + 1
	// EvToken: token number Index (0-based) is available.
	EvToken
	// EvPreempt: the request lost its KV cache and went back to the front of the queue.
	EvPreempt
	// EvDone: the request ended after Index tokens, with Finish "stop" or "length".
	EvDone
)

// Event is something that happened at virtual time At. Events are returned in non-decreasing At order.
type Event struct {
	At     time.Duration
	ID     int64
	Kind   EventKind
	Index  int
	Finish string
}

type seqState uint8

const (
	stWaiting seqState = iota
	stPrefilling
	stRunning
	stGone // done or cancelled
)

type seq struct {
	id            int64
	input, target int
	finish        string
	generated     int
	kvHeld        int
	prefillTarget int // tokens to (re)process: the prompt, plus tokens already generated after a preemption
	prefilled     int
	prefillBegun  bool
	started       bool // EvStart emitted
	state         seqState
}

type iteration struct {
	prefill *seq
	chunk   int
	decode  []*seq
}

// Engine is the state machine. The zero value is not usable; call New.
type Engine struct {
	cfg  Config
	p    Params
	now  time.Duration
	next int64
	rng  *rand.Rand // iteration noise, drawn in iteration order

	waiting    []*seq
	prefilling []*seq
	running    []*seq
	byID       map[int64]*seq
	kvUsed     int

	busy    bool
	iterEnd time.Duration
	iter    iteration

	rate rateWindow

	submitted, completed, rejected, cancelled, preemptions, kvEnded, tokens int64
}

// New returns an engine at virtual time 0.
func New(cfg Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Engine{
		cfg: cfg, p: cfg.Params,
		rng:  rand.New(rand.NewPCG(uint64(cfg.Seed), 0x9e3779b97f4a7c15)),
		byID: map[int64]*seq{},
	}, nil
}

// Now is the engine's clock: the latest time any call carried.
func (e *Engine) Now() time.Duration { return e.now }

// NextEvent is the time at which the engine next has something to report or do. ok is false when it is
// idle (nothing is running, prefilling or waiting for the next iteration).
func (e *Engine) NextEvent() (at time.Duration, ok bool) {
	if !e.busy {
		return 0, false
	}
	return e.iterEnd, true
}

// Advance moves the clock to now, completing every iteration that ends by then, and returns the events.
func (e *Engine) Advance(now time.Duration) ([]Event, error) {
	var evs []Event
	if err := e.advanceTo(now, &evs); err != nil {
		return nil, err
	}
	return evs, nil
}

// Submit advances to now and enqueues a request. The returned events are those produced while advancing.
func (e *Engine) Submit(now time.Duration, r Request) (Info, []Event, error) {
	var evs []Event
	if err := e.advanceTo(now, &evs); err != nil {
		return Info{}, nil, err
	}
	if r.InputTokens < 1 || r.MaxTokens < 0 {
		return Info{}, evs, fmt.Errorf("%w (input %d, max_tokens %d)", ErrInvalidRequest, r.InputTokens, r.MaxTokens)
	}
	if r.InputTokens > e.p.KVBudgetTokens-1 { // not InputTokens+1 > budget: that overflows near MaxInt
		e.rejected++
		return Info{}, evs, ErrTooLarge
	}
	free := max(e.cfg.MaxSeqs-len(e.prefilling)-len(e.running), 0)
	if len(e.waiting) >= e.cfg.MaxQueue+free {
		e.rejected++
		return Info{}, evs, ErrQueueFull
	}
	e.next++
	s := &seq{id: e.next, input: r.InputTokens, prefillTarget: r.InputTokens}
	s.target, s.finish = e.drawOutput(s.id, r.MaxTokens)
	e.byID[s.id] = s
	e.waiting = append(e.waiting, s)
	e.submitted++
	if !e.busy {
		e.startIteration(now, &evs)
	}
	return Info{ID: s.id, OutputTokens: s.target, Finish: s.finish}, evs, nil
}

// Cancel advances to now and removes a request wherever it is, freeing its KV tokens at once. A
// computation already launched in the current iteration still takes its time. It reports whether the
// request was live.
func (e *Engine) Cancel(now time.Duration, id int64) (bool, []Event, error) {
	var evs []Event
	if err := e.advanceTo(now, &evs); err != nil {
		return false, nil, err
	}
	s := e.byID[id]
	if s == nil {
		return false, evs, nil
	}
	e.remove(s)
	e.cancelled++
	return true, evs, nil
}

// Stats is a point-in-time view of the engine, as of the last call.
type Stats struct {
	// Active counts sequences holding a slot (prefilling or running); Prefilling and Running split it.
	Active, Prefilling, Running int
	QueueDepth                  int
	QueuedInputTokens           int
	RecentTokensPerSecond       float64
	KVUsedTokens                int
	KVBudgetTokens              int
	// GPUUtilization is the batch fill, Active/MaxSeqs in percent: a modelled quantity, not a
	// measurement of anything.
	GPUUtilization  float64
	GPUMemoryUsedMB int64
	Submitted       int64
	Completed       int64
	Rejected        int64
	Cancelled       int64
	Preemptions     int64
	KVExhausted     int64
	Tokens          int64
}

// Stats returns the current statistics. It does not advance the clock.
func (e *Engine) Stats() Stats {
	queued := 0
	for _, s := range e.waiting {
		queued += s.input
	}
	active := len(e.prefilling) + len(e.running)
	mem := e.p.WeightsMB + int64(e.kvUsed)*e.p.KVBytesPerToken/(1<<20)
	return Stats{
		Active: active, Prefilling: len(e.prefilling), Running: len(e.running),
		QueueDepth: len(e.waiting), QueuedInputTokens: queued,
		RecentTokensPerSecond: e.rate.rate(e.now),
		KVUsedTokens:          e.kvUsed, KVBudgetTokens: e.p.KVBudgetTokens,
		GPUUtilization:  100 * float64(active) / float64(e.cfg.MaxSeqs),
		GPUMemoryUsedMB: mem,
		Submitted:       e.submitted, Completed: e.completed, Rejected: e.rejected, Cancelled: e.cancelled,
		Preemptions: e.preemptions, KVExhausted: e.kvEnded, Tokens: e.tokens,
	}
}

// CheckInvariants verifies the engine's internal consistency and returns the first violation. It is for
// tests and costs O(live requests).
func (e *Engine) CheckInvariants() error {
	kv, live := 0, 0
	seen := map[int64]bool{}
	for name, list := range map[string][]*seq{"waiting": e.waiting, "prefilling": e.prefilling, "running": e.running} {
		for _, s := range list {
			if seen[s.id] {
				return fmt.Errorf("request %d is in two lists (second: %s)", s.id, name)
			}
			seen[s.id] = true
			live++
			if e.byID[s.id] != s {
				return fmt.Errorf("request %d in %s is not indexed", s.id, name)
			}
			if name == "waiting" && s.kvHeld != 0 {
				return fmt.Errorf("waiting request %d holds %d KV tokens", s.id, s.kvHeld)
			}
			kv += s.kvHeld
		}
	}
	switch {
	case live != len(e.byID):
		return fmt.Errorf("%d live requests but %d indexed", live, len(e.byID))
	case kv != e.kvUsed:
		return fmt.Errorf("KV held sums to %d but kvUsed is %d", kv, e.kvUsed)
	case e.kvUsed < 0 || e.kvUsed > e.p.KVBudgetTokens:
		return fmt.Errorf("kvUsed %d outside 0-%d", e.kvUsed, e.p.KVBudgetTokens)
	case len(e.prefilling)+len(e.running) > e.cfg.MaxSeqs:
		return fmt.Errorf("%d sequences exceed MaxSeqs %d", len(e.prefilling)+len(e.running), e.cfg.MaxSeqs)
	}
	stepping := map[*seq]bool{} // sequences in the decode step under way hold one extra token for its output
	for _, s := range e.iter.decode {
		stepping[s] = true
	}
	for _, s := range e.running {
		want := s.input + s.generated
		if stepping[s] {
			want++
		}
		if s.kvHeld != want {
			return fmt.Errorf("running request %d holds %d KV tokens, want %d", s.id, s.kvHeld, want)
		}
	}
	return nil
}

// --- internals -----------------------------------------------------------------

func (e *Engine) advanceTo(now time.Duration, evs *[]Event) error {
	if now < e.now {
		return fmt.Errorf("%w: %v < %v", ErrTimeRegression, now, e.now)
	}
	for e.busy && e.iterEnd <= now {
		t := e.iterEnd
		e.finishIteration(t, evs)
		e.startIteration(t, evs)
	}
	e.now = now
	return nil
}

// drawOutput decides a request's output length and finish reason from a stream private to the request,
// so it depends only on the seed and the request's sequence number, not on what else happened.
func (e *Engine) drawOutput(id int64, maxTokens int) (target int, finish string) {
	r := rand.New(rand.NewPCG(uint64(e.cfg.Seed), splitmix(uint64(id))))
	f := lognormal(r, e.p.OutputCV)
	var natural float64
	if e.p.OutputFraction > 0 && maxTokens > 0 {
		natural = math.Min(e.p.OutputFraction*f, 1) * float64(maxTokens)
	} else {
		natural = float64(e.cfg.OutputMeanTokens) * f
	}
	n := int(math.Round(math.Min(natural, maxOutput)))
	n = max(n, 1)
	if maxTokens > 0 && maxTokens <= n {
		return maxTokens, "length"
	}
	return n, "stop"
}

// addDuration returns t+d, saturating at the largest Duration.
func addDuration(t, d time.Duration) time.Duration {
	if t > math.MaxInt64-d {
		return math.MaxInt64
	}
	return t + d
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// lognormal returns a multiplier with mean 1 and coefficient of variation cv, clamped to
// [1/noiseClamp, noiseClamp]. cv 0 returns 1 without consuming randomness.
func lognormal(r *rand.Rand, cv float64) float64 {
	if cv <= 0 {
		return 1
	}
	s2 := math.Log1p(cv * cv)
	f := math.Exp(-s2/2 + math.Sqrt(s2)*r.NormFloat64())
	return math.Min(math.Max(f, 1/noiseClamp), noiseClamp)
}

func (e *Engine) decodeDuration(b int) time.Duration {
	d := ms((e.p.StepBaseMs + e.p.StepPerSeqMs*float64(b)) * lognormal(e.rng, e.p.StepNoiseCV))
	return min(max(d, minIteration), maxIteration)
}

func (e *Engine) prefillDuration(chunk int, first bool) time.Duration {
	v := e.p.PrefillPerTokenMs * float64(chunk)
	if first {
		v += e.p.PrefillFixedMs
	}
	return min(max(ms(v*lognormal(e.rng, e.p.PrefillNoiseCV)), minIteration), maxIteration)
}

// startIteration admits waiting requests and launches the next iteration at time t, or goes idle.
func (e *Engine) startIteration(t time.Duration, evs *[]Event) {
	e.busy = false
	for guard := 0; guard < 4*e.cfg.MaxSeqs+8; guard++ {
		e.admit(t, evs)
		if len(e.prefilling) == 0 && len(e.running) == 0 {
			return
		}
		decoding := len(e.running) > 0 && (len(e.prefilling) == 0 || e.p.PrefillChunkTokens > 0)
		if decoding && e.reserveDecodeKV(t, evs) {
			continue // preempting or ending a sequence freed KV: admission may now differ
		}
		break
	}
	if len(e.prefilling) == 0 && len(e.running) == 0 {
		return
	}
	decoding := len(e.running) > 0 && (len(e.prefilling) == 0 || e.p.PrefillChunkTokens > 0)
	var it iteration
	var dur time.Duration
	if len(e.prefilling) > 0 {
		p := e.prefilling[0]
		chunk := p.prefillTarget - p.prefilled
		if c := e.p.PrefillChunkTokens; c > 0 && chunk > c {
			chunk = c
		}
		it.prefill, it.chunk = p, chunk
		dur += e.prefillDuration(chunk, !p.prefillBegun)
		p.prefillBegun = true
	}
	if decoding {
		it.decode = append([]*seq(nil), e.running...)
		dur += e.decodeDuration(len(it.decode))
		for _, s := range it.decode {
			s.kvHeld++ // the slot for the token this step produces
			e.kvUsed++
		}
	}
	e.iter, e.busy, e.iterEnd = it, true, addDuration(t, dur)
}

// admit moves requests from the head of the wait queue into prefilling while a slot and KV are free.
// Strict order: if the head does not fit, nothing behind it is tried.
func (e *Engine) admit(t time.Duration, evs *[]Event) {
	for len(e.waiting) > 0 && len(e.prefilling)+len(e.running) < e.cfg.MaxSeqs {
		s := e.waiting[0]
		need := s.prefillTarget + 1 // the prompt, plus the slot for the token its prefill produces
		if e.kvUsed+need > e.p.KVBudgetTokens {
			return
		}
		e.waiting = e.waiting[1:]
		s.kvHeld, s.state, s.prefilled, s.prefillBegun = need, stPrefilling, 0, false
		e.kvUsed += need
		e.prefilling = append(e.prefilling, s)
		if !s.started {
			s.started = true
			*evs = append(*evs, Event{At: t, ID: s.id, Kind: EvStart})
		}
	}
}

// reserveDecodeKV makes sure the coming decode step can allocate one token per running sequence. It
// preempts the most recently started sequences until it can; a sequence that is alone ends with finish
// "length" instead. It reports whether it changed the batch.
func (e *Engine) reserveDecodeKV(t time.Duration, evs *[]Event) bool {
	changed := false
	for len(e.running) > 0 && e.kvUsed+len(e.running) > e.p.KVBudgetTokens {
		victim := e.running[len(e.running)-1]
		changed = true
		if len(e.running) == 1 && len(e.prefilling) == 0 {
			e.kvEnded++
			e.finish(victim, t, "length", evs)
			continue
		}
		e.running = e.running[:len(e.running)-1]
		e.kvUsed -= victim.kvHeld
		victim.kvHeld, victim.state = 0, stWaiting
		victim.prefillTarget, victim.prefilled, victim.prefillBegun = victim.input+victim.generated, 0, false
		e.waiting = append([]*seq{victim}, e.waiting...)
		e.preemptions++
		*evs = append(*evs, Event{At: t, ID: victim.id, Kind: EvPreempt, Index: victim.generated})
	}
	return changed
}

// finishIteration applies the effects of the iteration that ends at t.
func (e *Engine) finishIteration(t time.Duration, evs *[]Event) {
	it := e.iter
	e.iter, e.busy = iteration{}, false
	for _, s := range it.decode {
		if s.state != stRunning {
			continue // cancelled meanwhile
		}
		e.emitToken(s, t, evs)
	}
	if p := it.prefill; p != nil && p.state == stPrefilling {
		p.prefilled += it.chunk
		if p.prefilled >= p.prefillTarget {
			e.prefilling = removeSeq(e.prefilling, p)
			p.state = stRunning
			e.running = append(e.running, p)
			e.emitToken(p, t, evs)
		}
	}
}

func (e *Engine) emitToken(s *seq, t time.Duration, evs *[]Event) {
	*evs = append(*evs, Event{At: t, ID: s.id, Kind: EvToken, Index: s.generated})
	s.generated++
	e.tokens++
	e.rate.record(t, 1)
	if s.generated >= s.target {
		e.completed++
		e.finish(s, t, s.finish, evs)
	}
}

// finish ends a live request: frees its KV, drops it from the lists and reports it done.
func (e *Engine) finish(s *seq, t time.Duration, reason string, evs *[]Event) {
	e.remove(s)
	*evs = append(*evs, Event{At: t, ID: s.id, Kind: EvDone, Index: s.generated, Finish: reason})
}

func (e *Engine) remove(s *seq) {
	switch s.state {
	case stWaiting:
		e.waiting = removeSeq(e.waiting, s)
	case stPrefilling:
		e.prefilling = removeSeq(e.prefilling, s)
	case stRunning:
		e.running = removeSeq(e.running, s)
	}
	e.kvUsed -= s.kvHeld
	s.kvHeld, s.state = 0, stGone
	delete(e.byID, s.id)
}

func removeSeq(list []*seq, s *seq) []*seq {
	for i, x := range list {
		if x == s {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
