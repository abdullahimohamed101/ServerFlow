package resilience

import (
	"sync/atomic"

	"serverflow/internal/config"
)

// Admission is the global request cap of one gateway (admission.max_global_requests): requests inside the chat
// handler from just after authentication until completion, queued plus running. Over the cap new arrivals are
// refused at once (reason global_limit). Per-tenant occupancy lives in Queue, as a share of a model's depth.
type Admission struct {
	max    int64
	active atomic.Int64
}

// NewAdmission creates a counter. max <= 0 means unlimited.
func NewAdmission(max int) *Admission { return &Admission{max: int64(max)} }

// AdmissionFrom builds the counter from the configuration.
func AdmissionFrom(c config.Config) *Admission { return NewAdmission(c.Admission.MaxGlobalRequests) }

// Slot is an admitted request's hold on the global cap. Release it exactly once per Acquire; extra calls are
// harmless.
type Slot struct {
	a        *Admission
	released atomic.Bool
}

// Acquire takes a place under the cap, or returns false when the cap is reached.
func (a *Admission) Acquire() (*Slot, bool) {
	if a.max > 0 {
		for {
			cur := a.active.Load()
			if cur >= a.max {
				return nil, false
			}
			if a.active.CompareAndSwap(cur, cur+1) {
				break
			}
		}
	} else {
		a.active.Add(1)
	}
	return &Slot{a: a}, true
}

// Release gives the place back. It is idempotent.
func (s *Slot) Release() {
	if s != nil && s.released.CompareAndSwap(false, true) {
		s.a.active.Add(-1)
	}
}

// Active is the number of requests currently holding a place.
func (a *Admission) Active() int { return int(a.active.Load()) }

// Limit is the cap (0 = unlimited).
func (a *Admission) Limit() int { return int(a.max) }
