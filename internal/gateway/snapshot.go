package gateway

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"serverflow/internal/registry/client"
	"serverflow/pkg/protocol"
)

// workerLister is the part of the registry client the cache needs.
type workerLister interface {
	Workers(ctx context.Context, q client.Query) ([]protocol.WorkerSnapshot, error)
}

// snapshotCache keeps the last good view of the registry so the request path
// never waits on the control plane (ARCHITECTURE.md: the routing hot path may
// depend on control plane state only through cached snapshots).
//
// Trust in a snapshot is bounded. A snapshot older than maxStale is not used at
// all (the gateway fails closed), and within that bound every worker's
// heartbeat age is advanced by the snapshot's age, so a worker that died after
// the snapshot was taken stops being eligible once it would have turned
// suspect, instead of staying "eligible" until the next refresh (spec 63 rule
// 10: never assume worker health).
type snapshotCache struct {
	src          workerLister
	refresh      time.Duration
	maxStale     time.Duration
	suspectAfter time.Duration
	now          func() time.Time
	log          *slog.Logger

	cur    atomic.Pointer[snapshot]
	failed atomic.Bool // the last refresh failed; logged once per change
	// refreshFailures counts failed refreshes (not ones cut short by shutdown); created is when the cache began,
	// the age reported before any snapshot exists. Both feed the Prometheus collector.
	refreshFailures atomic.Int64
	created         time.Time
}

type snapshot struct {
	byModel map[string][]protocol.WorkerSnapshot
	fetched time.Time // when the request that produced it was sent (the conservative end)
}

func newSnapshotCache(src workerLister, refresh, maxStale, suspectAfter time.Duration, log *slog.Logger) *snapshotCache {
	return &snapshotCache{src: src, refresh: refresh, maxStale: maxStale, suspectAfter: suspectAfter, now: time.Now, log: log, created: time.Now()}
}

// Run refreshes immediately and then every refresh interval until ctx ends.
func (c *snapshotCache) Run(ctx context.Context) {
	t := time.NewTicker(c.refresh)
	defer t.Stop()
	for {
		_ = c.Refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Refresh fetches the registry once. On failure the previous snapshot stays.
func (c *snapshotCache) Refresh(ctx context.Context) error {
	started := c.now()
	rctx, cancel := context.WithTimeout(ctx, max(c.maxStale/2, 100*time.Millisecond))
	defer cancel()
	ws, err := c.src.Workers(rctx, client.Query{})
	if err != nil {
		if ctx.Err() == nil {
			c.refreshFailures.Add(1)
		}
		if ctx.Err() == nil && !c.failed.Swap(true) {
			c.log.Warn("could not refresh the worker registry; any earlier snapshot keeps serving until it is too old", "error", errText(err))
		}
		return err
	}
	if c.failed.Swap(false) {
		c.log.Info("the worker registry is reachable again")
	}
	by := make(map[string][]protocol.WorkerSnapshot)
	for _, w := range ws {
		if !believable(&w) {
			w.Eligible = false
		}
		by[w.Model] = append(by[w.Model], w)
	}
	for _, list := range by {
		// Kept in ID order so the scheduler's sorted-input fast path always applies.
		sort.Slice(list, func(i, j int) bool { return list[i].WorkerID < list[j].WorkerID })
	}
	c.cur.Store(&snapshot{byModel: by, fetched: started})
	return nil
}

// believable reports whether a snapshot's numbers and address are sane. The
// control plane is trusted, but a bug or a hostile one must not be able to defeat
// the staleness checks with a negative age or a NaN, or steer a request to an address
// registration would have refused (credentials, a query, a metadata host).
func believable(w *protocol.WorkerSnapshot) bool {
	age := w.HeartbeatAgeSeconds
	if math.IsNaN(age) || math.IsInf(age, 0) || age < 0 {
		return false
	}
	if w.MaxConcurrency < 1 || w.Metrics.ActiveRequests < 0 || w.Metrics.QueueDepth < 0 {
		return false
	}
	return protocol.ValidateAddress(w.Address) == nil
}

// errText keeps log lines short. Errors from the registry client may name the
// control plane URL (never a token or credentials: config forbids userinfo).
func errText(err error) string {
	s := err.Error()
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// Fresh reports whether a snapshot exists and is within the staleness bound,
// and how old it is.
func (c *snapshotCache) Fresh() (age time.Duration, ok bool) {
	_, age, ok = c.fresh()
	return age, ok
}

// fresh returns the snapshot it judged, so callers use the same one it aged.
func (c *snapshotCache) fresh() (s *snapshot, age time.Duration, ok bool) {
	s = c.cur.Load()
	if s == nil {
		return nil, 0, false
	}
	age = c.now().Sub(s.fetched)
	return s, age, age <= c.maxStale
}

// Degraded reports that refreshes have been failing for more than two intervals, so the view is
// aging without being renewed. One missed refresh is not an outage: a genuine shortage of capacity
// must still be reported as one.
func (c *snapshotCache) Degraded() bool {
	if !c.failed.Load() {
		return false
	}
	_, age, ok := c.fresh()
	return !ok || age > 2*c.refresh
}

// View returns copies of the workers serving model, with heartbeat ages
// advanced and eligibility re-judged. ok is false when there is no usable
// snapshot; a model nobody serves yields ok with no workers.
func (c *snapshotCache) View(model string) (workers []protocol.WorkerSnapshot, ok bool) {
	s, age, ok := c.fresh()
	if !ok {
		return nil, false
	}
	return c.age(s.byModel[model], age), true
}

func (c *snapshotCache) age(in []protocol.WorkerSnapshot, age time.Duration) []protocol.WorkerSnapshot {
	out := make([]protocol.WorkerSnapshot, len(in))
	copy(out, in)
	for i := range out {
		w := &out[i]
		w.HeartbeatAgeSeconds += age.Seconds()
		// Compared as seconds: converting a huge float to a Duration overflows differently by platform.
		if w.Eligible && w.HeartbeatAgeSeconds >= c.suspectAfter.Seconds() {
			w.Eligible = false
			w.Health = protocol.HealthSuspect
		}
	}
	return out
}

// Models lists the models that have at least one eligible worker, sorted.
func (c *snapshotCache) Models() (models []string, ok bool) {
	s, age, ok := c.fresh()
	if !ok {
		return nil, false
	}
	for m, ws := range s.byModel {
		for _, w := range c.age(ws, age) {
			if w.Eligible {
				models = append(models, m)
				break
			}
		}
	}
	sort.Strings(models)
	return models, true
}
