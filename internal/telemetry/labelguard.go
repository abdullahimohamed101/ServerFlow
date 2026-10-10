package telemetry

import "sync"

// LabelGuard bounds how many distinct values a metric label can take (ADR-017). The first max distinct values
// pass through unchanged; any later value is reported as the overflow value. Values never expire: a guard
// bounds a process's series, it does not follow a fleet's churn, so first-come-first-kept is a documented
// limitation.
//
// Value is on the request path. For a value already seen it takes a read lock and does one map lookup, with no
// allocation. The write lock is taken only to admit a new value, at most max times in the guard's life, plus
// never again once the guard is full.
type LabelGuard struct {
	max      int
	overflow string

	mu   sync.RWMutex
	seen map[string]struct{}
}

// NewLabelGuard returns a guard admitting at most max distinct values (at least 1) and mapping the rest to
// overflow.
func NewLabelGuard(max int, overflow string) *LabelGuard {
	if max < 1 {
		max = 1
	}
	return &LabelGuard{max: max, overflow: overflow, seen: make(map[string]struct{}, min(max, 64))}
}

// Value returns raw if it is, or can still become, one of the admitted values, else the overflow value.
func (g *LabelGuard) Value(raw string) string {
	g.mu.RLock()
	_, ok := g.seen[raw]
	full := len(g.seen) >= g.max
	g.mu.RUnlock()
	if ok {
		return raw
	}
	if full {
		return g.overflow
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.seen[raw]; ok {
		return raw
	}
	if len(g.seen) >= g.max {
		return g.overflow
	}
	g.seen[raw] = struct{}{}
	return raw
}

// Len is how many distinct values have been admitted.
func (g *LabelGuard) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.seen)
}

// Overflow is the value that stands in for everything beyond the cap.
func (g *LabelGuard) Overflow() string { return g.overflow }
