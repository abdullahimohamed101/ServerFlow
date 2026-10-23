package gateway

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/internal/registry/client"
	"serverflow/internal/resilience"
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

	// onSync, when set (resilience is effective), receives the registry view after every successful refresh and
	// whenever aging turns an eligible worker suspect, as breaker members plus a worker-to-model map. syncMu
	// serializes those calls so a late one for a superseded snapshot cannot overwrite a newer view.
	onSync func(members []resilience.Member, models map[string]string)
	syncMu sync.Mutex
	// onRefreshed, when set, runs after every successful refresh, once the new view and the breakers' sync are in
	// place: the admission queue wakes the waiters that the corrected counts may now serve (Phase 15 PR E).
	onRefreshed func()
}

type snapshot struct {
	byModel map[string][]protocol.WorkerSnapshot
	fetched time.Time // when the request that produced it was sent (the conservative end)
	// nextAge is the snapshot age, in nanoseconds, at which the next worker that is eligible now turns suspect
	// (math.MaxInt64 when none will). Only used when onSync is set.
	nextAge atomic.Int64
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
	snap := &snapshot{byModel: by, fetched: started}
	snap.nextAge.Store(math.MaxInt64)
	c.cur.Store(snap)
	if c.onSync != nil {
		c.syncMu.Lock()
		c.runSync(snap, c.now().Sub(started))
		c.syncMu.Unlock()
	}
	if c.onRefreshed != nil {
		c.onRefreshed()
	}
	return nil
}

// runSync reports the view of snapshot s at the given age to onSync. The caller holds syncMu.
func (c *snapshotCache) runSync(s *snapshot, age time.Duration) {
	suspect := c.suspectAfter.Seconds()
	members := make([]resilience.Member, 0, len(s.byModel)*2)
	models := make(map[string]string, len(s.byModel)*2)
	next := int64(math.MaxInt64)
	for model, list := range s.byModel {
		for i := range list {
			w := &list[i]
			eligible := schedulable(w)
			if eligible && w.HeartbeatAgeSeconds+age.Seconds() >= suspect {
				eligible = false
			}
			if eligible {
				// The age at which this worker turns suspect; compared as seconds, like age().
				if secs := suspect - w.HeartbeatAgeSeconds; secs < float64(math.MaxInt64)/1e9 {
					next = min(next, int64(math.Ceil(secs*1e9)))
				}
			}
			members = append(members, resilience.Member{Key: keyOf(w), Model: model, Eligible: eligible})
			models[w.WorkerID] = model
		}
	}
	s.nextAge.Store(next)
	c.onSync(members, models)
}

// syncAged re-reports the view when aging has made a worker suspect since the last report, so the breakers'
// ejection cap sees the same eligible set the router does.
func (c *snapshotCache) syncAged(s *snapshot, age time.Duration) {
	if c.onSync == nil || int64(age) < s.nextAge.Load() {
		return
	}
	if !c.syncMu.TryLock() {
		return // someone is reporting; the next request checks again
	}
	defer c.syncMu.Unlock()
	if c.cur.Load() != s || int64(age) < s.nextAge.Load() {
		return
	}
	c.runSync(s, age)
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
	c.syncAged(s, age)
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

// Serves reports whether any worker in the snapshot serves model, eligible or not (a model whose workers are
// all lost is still a model the registry knows). ok is false when there is no usable snapshot, in which case
// nothing is known about the model. It does not copy the workers.
func (c *snapshotCache) Serves(model string) (serves, ok bool) {
	s, _, ok := c.fresh()
	if !ok {
		return false, false
	}
	return len(s.byModel[model]) > 0, true
}

// worker returns the current, aged view of one worker of a model (eligibility re-judged as View does), or false
// when there is no usable snapshot or the model has no such worker. Workers are kept in ID order.
func (c *snapshotCache) worker(model, id string) (protocol.WorkerSnapshot, bool) {
	s, age, ok := c.fresh()
	if !ok {
		return protocol.WorkerSnapshot{}, false
	}
	list := s.byModel[model]
	i := sort.Search(len(list), func(i int) bool { return list[i].WorkerID >= id })
	if i >= len(list) || list[i].WorkerID != id {
		return protocol.WorkerSnapshot{}, false
	}
	return c.age(list[i:i+1], age)[0], true
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
