package mockworker

import (
	"math/rand/v2"
	"sync"
)

// Injector decides, per request, whether to inject a failure. Decisions come
// from a seeded generator, so the same seed and the same request order give
// the same failure sequence (spec section 32: record the seed). Under
// concurrent requests the order in which they call Roll is not fixed, so
// reproducibility holds for a fixed request order only.
type Injector struct {
	mu   sync.Mutex
	rng  *rand.Rand
	rate float64
	mode FailureMode
}

// NewInjector returns an injector failing a fraction rate of requests.
func NewInjector(seed int64, rate float64, mode FailureMode) *Injector {
	s := uint64(seed)
	return &Injector{rng: rand.New(rand.NewPCG(s, s^0x9E3779B97F4A7C15)), rate: rate, mode: mode}
}

// Roll reports whether this request should fail. It always consumes one
// random number, so the sequence does not depend on the configured rate.
func (i *Injector) Roll() (FailureMode, bool) {
	i.mu.Lock()
	v := i.rng.Float64() // in [0,1): rate 0 never fails, rate 1 always does
	i.mu.Unlock()
	return i.mode, v < i.rate
}
