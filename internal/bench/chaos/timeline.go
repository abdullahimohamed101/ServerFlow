package chaos

import (
	"context"
	"sync"
	"time"
)

// Counters are the gateway's own series the harness reads each bucket (all summed over their labels,
// except the selections of the faulted worker). A series the build does not have reads as 0.
type Counters struct {
	Attempts   float64 // inference_attempts_total
	Requests   float64 // inference_requests_total
	Retries    float64 // inference_retries_total
	NoCapacity float64 // scheduler_no_capacity_total
	Ineligible float64 // scheduler_ineligible_selections_total
	// FaultySelections is scheduler_selections_total for the faulted worker, and Probes the breaker's
	// probes (circuit_breaker_probes_total; 0 on a build without a breaker).
	FaultySelections float64
	Probes           float64
}

// Sub returns c - o. A counter that went backwards (the gateway restarted) was reset to zero in between, so
// what it counted since is its current value; a delta is never negative.
func (c Counters) Sub(o Counters) Counters {
	d := func(a, b float64) float64 {
		if a < b {
			return a
		}
		return a - b
	}
	return Counters{d(c.Attempts, o.Attempts), d(c.Requests, o.Requests), d(c.Retries, o.Retries), d(c.NoCapacity, o.NoCapacity),
		d(c.Ineligible, o.Ineligible), d(c.FaultySelections, o.FaultySelections), d(c.Probes, o.Probes)}
}

// Snapshot is the counters at one tick of the timeline.
type Snapshot struct {
	At time.Duration
	Counters
}

// Timeline is what the conductor did, in offsets from the start of the run.
type Timeline struct {
	// Ticks are the snapshots, one per bucket from the start of the run (tick 0) to its end.
	Ticks []Snapshot
	// ApplyTick and ClearTick are the ticks at which the fault was applied and cleared (-1 if it was not).
	ApplyTick, ClearTick int
	// AppliedAt is when Apply was called; ClearedAt when Clear returned, so a restart that took time
	// counts as fault time.
	AppliedAt, ClearedAt time.Duration
	Applied, Cleared     bool
	ApplyErr, ClearErr   error
	// ClockStep is the largest move of the wall clock against the monotonic clock since the start.
	ClockStep time.Duration
	// ControlPlane is the control plane's state of the faulted worker just before the fault was cleared.
	ControlPlane string
}

// conductor drives the timeline of one run on a Clock: one goroutine takes a snapshot of the gateway's
// counters every bucket, applies the fault when the baseline is over, clears it when the fault window is
// over, and watches for the wall clock moving against the monotonic clock. A rolling fault's Apply runs in
// its own goroutine, since it takes seconds; the others are applied inline.
type conductor struct {
	clock Clock
	t0    time.Time
	p     Params
	env   *Env
	sc    Scenario
	// snap reads the gateway's counters; state reads the faulted worker's control plane state.
	snap  func() Counters
	state func(ctx context.Context) string
}

func (c *conductor) run(ctx context.Context) (tl Timeline) {
	tl = Timeline{ApplyTick: -1, ClearTick: -1}
	off0 := c.clock.WallOffset()
	ticks := int(c.p.Total() / c.p.Bucket)
	applyTick := int((c.p.Warmup + c.p.Baseline) / c.p.Bucket)
	clearTick := int((c.p.Warmup + c.p.Baseline + c.p.Fault) / c.p.Bucket)

	var applyDone chan struct{}
	var applyErr error // written by the apply goroutine, read after it has finished
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		tl.ApplyErr = applyErr
	}()
	// A roll that has not finished when the ticks end is abandoned, not waited for.
	actx, abandon := context.WithCancel(ctx)
	defer abandon()
	for i := 0; i <= ticks; i++ {
		if err := c.clock.SleepUntil(ctx, c.t0.Add(time.Duration(i)*c.p.Bucket)); err != nil {
			return tl
		}
		now := c.clock.Now().Sub(c.t0)
		if d := absDur(c.clock.WallOffset() - off0); d > tl.ClockStep {
			tl.ClockStep = d
		}
		tl.Ticks = append(tl.Ticks, Snapshot{At: now, Counters: c.snap()})

		if i == applyTick && !tl.Applied {
			tl.ApplyTick, tl.AppliedAt, tl.Applied = i, now, true
			applyDone = make(chan struct{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(applyDone)
				applyErr = c.sc.apply(actx, c.env)
			}()
			if !c.sc.Rolling {
				select {
				case <-applyDone:
				case <-ctx.Done():
					return tl
				}
				if applyErr != nil {
					return tl
				}
			}
		}
		if i >= clearTick && tl.Applied && !tl.Cleared {
			if c.sc.Rolling {
				select {
				case <-applyDone:
					if applyErr != nil {
						return tl
					}
				default:
					continue // the roll is still going: clear when it has finished
				}
			}
			if c.state != nil {
				tl.ControlPlane = c.state(ctx)
			}
			tl.ClearTick = i
			tl.ClearErr = c.sc.clear(ctx, c.env)
			tl.ClearedAt, tl.Cleared = c.clock.Now().Sub(c.t0), tl.ClearErr == nil
			if tl.ClearErr != nil {
				return tl
			}
		}
	}
	return tl
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
