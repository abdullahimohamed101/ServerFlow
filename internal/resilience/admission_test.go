package resilience

import (
	"sync"
	"sync/atomic"
	"testing"

	"serverflow/internal/config"
)

func TestAdmissionCap(t *testing.T) {
	a := NewAdmission(3)
	var slots []*Slot
	for i := 0; i < 3; i++ {
		s, ok := a.Acquire()
		if !ok {
			t.Fatalf("slot %d refused", i)
		}
		slots = append(slots, s)
	}
	if _, ok := a.Acquire(); ok {
		t.Fatal("over the cap: new arrivals are refused at once")
	}
	if a.Active() != 3 || a.Limit() != 3 {
		t.Fatal(a.Active())
	}
	slots[0].Release()
	slots[0].Release() // idempotent
	if a.Active() != 2 {
		t.Fatalf("active %d", a.Active())
	}
	if _, ok := a.Acquire(); !ok {
		t.Fatal("a released place can be taken")
	}
	if _, ok := a.Acquire(); ok {
		t.Fatal("exactly one place was freed")
	}
	var nilSlot *Slot
	nilSlot.Release() // harmless
}

func TestAdmissionUnlimited(t *testing.T) {
	a := NewAdmission(0)
	for i := 0; i < 100; i++ {
		if _, ok := a.Acquire(); !ok {
			t.Fatal("unlimited refused")
		}
	}
	if a.Active() != 100 {
		t.Fatal(a.Active())
	}
}

func TestAdmissionFromConfigReusesMaxGlobalRequests(t *testing.T) {
	c := config.Default()
	c.Admission.MaxGlobalRequests = 7
	if got := AdmissionFrom(c).Limit(); got != 7 {
		t.Fatalf("limit %d, want admission.max_global_requests", got)
	}
}

func TestAdmissionNeverExceedsCapUnderRace(t *testing.T) {
	const capN = 10
	a := NewAdmission(capN)
	var inside, peak atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s, ok := a.Acquire()
				if !ok {
					continue
				}
				n := inside.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				inside.Add(-1)
				s.Release()
			}
		}()
	}
	wg.Wait()
	if peak.Load() > capN || a.Active() != 0 {
		t.Fatalf("peak %d (cap %d), active %d", peak.Load(), capN, a.Active())
	}
}
