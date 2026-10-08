package gateway

import (
	"context"
	"sync"
	"time"
)

const (
	probeTimeout  = time.Second
	probeCacheTTL = 2 * time.Second
)

// prober reports whether the thing the gateway forwards to can take requests.
type prober interface {
	Probe(ctx context.Context) error
}

// readiness caches the result of probing the upstream so /readyz traffic
// cannot turn into probe traffic. The lock is held during the probe so
// concurrent callers share one probe.
type readiness struct {
	upstream prober
	mu       sync.Mutex
	checked  time.Time
	err      error
}

func (r *readiness) check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.checked.IsZero() && time.Since(r.checked) < probeCacheTTL {
		return r.err
	}
	// Detach from the caller's cancellation: the result is shared and cached,
	// so one client disconnecting must not turn into a "not ready" for all.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), probeTimeout)
	defer cancel()
	r.err = r.upstream.Probe(pctx)
	r.checked = time.Now()
	return r.err
}
