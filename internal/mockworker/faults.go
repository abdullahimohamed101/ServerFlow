package mockworker

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Faults injects faults into a running worker, in process (Phase 15). The settings are atomic and read
// by the chat handler on every request, so they can change under load. The zero state injects nothing.
// The handle is for the benchmark harness and tests; there is deliberately no HTTP endpoint for it.
//
// What each fault looks like to the gateway:
//   - error rate and mode: the existing --failure-mode behaviours, on top of --failure-rate;
//   - extra delay: the worker takes this much longer before it answers, holding its slot (a slow but
//     working worker; /stats stays healthy);
//   - blackhole: the request is read and never answered, without holding a slot; /stats keeps answering,
//     so the worker stays READY and only the caller's own timeout can detect it;
//   - crash: the listener closes and every connection is reset; restart listens again on the same port.
type Faults struct {
	srv *Server

	errRate atomic.Uint64 // math.Float64bits
	errMode atomic.Pointer[FailureMode]
	rngMu   sync.Mutex
	rng     *rand.Rand

	delayNs atomic.Int64

	mu   sync.Mutex
	hold chan struct{} // non-nil while blackholed; closed when it is cleared

	errored, delayed, blackholed, crashes atomic.Int64
}

func newFaults(s *Server, seed int64) *Faults {
	u := uint64(seed)
	return &Faults{srv: s, rng: rand.New(rand.NewPCG(u^0xA5A5A5A5, u+0x9E3779B97F4A7C15))}
}

// FaultStats counts the requests each fault touched, so a run can show the fault really happened.
type FaultStats struct {
	// Errored requests were failed by SetErrorRate, Delayed ones were slowed by SetExtraDelay,
	// Blackholed ones were held without an answer, Crashes is how many times Crash was called.
	Errored, Delayed, Blackholed, Crashes int64
}

// Touched is the number of requests any request-level fault affected.
func (s FaultStats) Touched() int64 { return s.Errored + s.Delayed + s.Blackholed }

// Stats returns the counters.
func (f *Faults) Stats() FaultStats {
	return FaultStats{Errored: f.errored.Load(), Delayed: f.delayed.Load(), Blackholed: f.blackholed.Load(), Crashes: f.crashes.Load()}
}

// SetErrorRate makes a fraction rate of requests fail with the given mode (error, unavailable, drop
// or midstream). A rate of 0 clears it.
func (f *Faults) SetErrorRate(rate float64, mode FailureMode) error {
	if math.IsNaN(rate) || rate < 0 || rate > 1 {
		return fmt.Errorf("error rate must be in [0,1], got %v", rate)
	}
	switch mode {
	case ModeError, ModeUnavailable, ModeDrop, ModeMidstream:
	default:
		return fmt.Errorf("failure mode %q is not supported (error, unavailable, drop, midstream)", mode)
	}
	f.errMode.Store(&mode)
	f.errRate.Store(math.Float64bits(rate))
	return nil
}

// SetExtraDelay adds d to every request before its response begins. 0 clears it.
func (f *Faults) SetExtraDelay(d time.Duration) {
	f.delayNs.Store(int64(max(d, 0)))
}

// SetBlackhole makes the worker read requests and never answer them. Clearing it releases the
// requests being held, which then proceed normally (their callers have usually given up).
func (f *Faults) SetBlackhole(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case on && f.hold == nil:
		f.hold = make(chan struct{})
	case !on && f.hold != nil:
		close(f.hold)
		f.hold = nil
	}
}

// Crash closes the listener and resets every connection, like a killed process. It fails when the
// worker is not being served by Serve or is already crashed.
func (f *Faults) Crash() error {
	if err := f.srv.crash(); err != nil {
		return err
	}
	f.crashes.Add(1)
	return nil
}

// Restart listens again on the address the worker had before Crash.
func (f *Faults) Restart() error { return f.srv.restart() }

// Clear removes every request-level fault (error rate, delay, blackhole). It does not restart a
// crashed worker.
func (f *Faults) Clear() {
	f.errRate.Store(0)
	f.delayNs.Store(0)
	f.SetBlackhole(false)
}

// rollError reports whether this request is failed by the error-rate fault, and how.
func (f *Faults) rollError() (FailureMode, bool) {
	rate := math.Float64frombits(f.errRate.Load())
	if rate <= 0 {
		return "", false
	}
	f.rngMu.Lock()
	v := f.rng.Float64()
	f.rngMu.Unlock()
	if v >= rate {
		return "", false
	}
	f.errored.Add(1)
	return *f.errMode.Load(), true
}

// blackhole holds the request while the blackhole is on. It returns false when ctx ended first.
func (f *Faults) blackhole(ctx context.Context) bool {
	f.mu.Lock()
	hold := f.hold
	f.mu.Unlock()
	if hold == nil {
		return true
	}
	f.blackholed.Add(1)
	select {
	case <-hold:
		return true
	case <-ctx.Done():
		return false
	}
}

// delay waits for the extra delay. It returns false when ctx ended first.
func (f *Faults) delay(ctx context.Context) bool {
	d := time.Duration(f.delayNs.Load())
	if d <= 0 {
		return true
	}
	f.delayed.Add(1)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
