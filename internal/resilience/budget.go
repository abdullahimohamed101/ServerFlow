package resilience

import (
	"math"
	"sync"
	"time"

	"serverflow/internal/config"
)

// OtherModel is the key shared by every model beyond the tracked set (the same cap as the metric labels).
const OtherModel = "other"

// BudgetParams configure the retry budget. A retry is allowed while the retries in the window are fewer than
// max(Min, floor(Ratio x first attempts in the window)).
type BudgetParams struct {
	Ratio  float64
	Min    int
	Window time.Duration
	// MaxModels bounds the tracked models; further models share OtherModel. 0 means unbounded.
	MaxModels int
}

// BudgetParamsFrom takes the budget settings from the configuration (the model cap is metrics.max_models).
func BudgetParamsFrom(c config.Config) BudgetParams {
	r := c.Resilience.Retry
	return BudgetParams{Ratio: r.BudgetRatio, Min: r.BudgetMin, Window: r.BudgetWindow, MaxModels: c.Metrics.MaxModels}
}

// Budget limits retries per model so that they cannot multiply an outage. First attempts (routed attempts with
// Number 1, not admitted requests) are counted with NoteFirst; a retry is reserved with Reserve before its
// routing and refunded if no worker is found, so only a retry whose send begins stays consumed. Windows slide in
// one-second buckets; nothing runs in the background.
type Budget struct {
	p   BudgetParams
	now Clock

	mu     sync.Mutex
	models map[string]*slidingWindow // counter 0 = first attempts, counter 1 = retries
}

// Reservation is a retry taken from the budget; pass it to Refund if the retry does not happen.
type Reservation struct {
	model string
	at    time.Time
	valid bool
}

// NewBudget creates an empty budget.
func NewBudget(p BudgetParams, now Clock) *Budget {
	if p.Window < bucketWidth {
		p.Window = bucketWidth
	}
	return &Budget{p: p, now: now.orNow(), models: map[string]*slidingWindow{}}
}

// window returns the model's window, creating it, or the shared one for an untracked model beyond the cap.
func (b *Budget) window(model string) (*slidingWindow, string) {
	if w, ok := b.models[model]; ok {
		return w, model
	}
	if b.p.MaxModels > 0 && len(b.models) >= b.p.MaxModels && model != OtherModel {
		return b.window(OtherModel)
	}
	w := newSlidingWindow(b.p.Window)
	b.models[model] = &w
	return &w, model
}

// NoteFirst records a routed first attempt for the model.
func (b *Budget) NoteFirst(model string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	w, _ := b.window(model)
	w.add(now, now, 0, 1)
}

func (b *Budget) limit(firsts int) int {
	return max(b.p.Min, int(math.Floor(b.p.Ratio*float64(firsts)+1e-9)))
}

// Reserve takes one retry for the model if the budget allows it. The check and the increment are one atomic
// step, so concurrent requests cannot overshoot. A denied reserve consumes nothing.
func (b *Budget) Reserve(model string) (Reservation, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	w, key := b.window(model)
	firsts, retries := w.sum(now)
	if retries >= b.limit(firsts) {
		return Reservation{}, false
	}
	w.add(now, now, 1, 1)
	return Reservation{model: key, at: now, valid: true}, true
}

// Refund gives a reserved retry back (no other worker was found). It is idempotent only per reservation value:
// call it at most once. A reservation whose bucket already left the window changes nothing.
func (b *Budget) Refund(r Reservation) {
	if !r.valid {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if w, ok := b.models[r.model]; ok {
		w.add(r.at, b.now(), 1, -1)
	}
}

// Usage returns the first attempts and retries currently in the model's window (diagnostics and tests).
func (b *Budget) Usage(model string) (firsts, retries int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := model
	if _, ok := b.models[model]; !ok && b.p.MaxModels > 0 && len(b.models) >= b.p.MaxModels {
		key = OtherModel
	}
	if w, ok := b.models[key]; ok {
		return w.sum(b.now())
	}
	return 0, 0
}

// Prune drops the windows of models that are not in keep (the registry no longer serves them). The shared
// OtherModel window is kept while it has events.
func (b *Budget) Prune(keep func(model string) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	for m, w := range b.models {
		if keep(m) {
			continue
		}
		if m == OtherModel {
			if f, r := w.sum(now); f != 0 || r != 0 {
				continue
			}
		}
		delete(b.models, m)
	}
}
