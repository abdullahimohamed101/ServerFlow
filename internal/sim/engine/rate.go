package engine

import "time"

// The recent-throughput window mirrors the mock worker's (internal/mockworker/engine.go): the average
// over up to windowBuckets complete buckets of bucketWidth, divided by the time actually covered, so a
// young busy period is not diluted and an idle gap falls to 0. It works on virtual durations.
const (
	bucketWidth   = 100 * time.Millisecond
	windowBuckets = 50 // 5 seconds
	ringSize      = windowBuckets + 1
)

type rateWindow struct {
	slots   [ringSize]struct{ idx, n int64 }
	epoch   time.Duration // time of the first token ever
	first   int64         // bucket where the current busy period began
	last    int64         // bucket of the most recent token
	started bool
}

func (t *rateWindow) bucketOf(now time.Duration) int64 {
	if d := now - t.epoch; d > 0 {
		return int64(d / bucketWidth)
	}
	return 0
}

func (t *rateWindow) record(now time.Duration, n int64) {
	if !t.started {
		t.epoch = now
	}
	idx := t.bucketOf(now)
	if !t.started || idx-t.last > windowBuckets {
		t.slots = [ringSize]struct{ idx, n int64 }{}
		t.first, t.started = idx, true
	}
	s := &t.slots[idx%ringSize]
	if s.idx != idx {
		*s = struct{ idx, n int64 }{idx: idx}
	}
	s.n += n
	t.last = idx
}

func (t *rateWindow) rate(now time.Duration) float64 {
	if !t.started {
		return 0
	}
	cur := t.bucketOf(now)
	covered := cur - (t.first + 1)
	if covered > windowBuckets {
		covered = windowBuckets
	}
	if covered <= 0 {
		return 0
	}
	var total int64
	for _, s := range t.slots {
		if s.idx < cur && s.idx >= cur-covered {
			total += s.n
		}
	}
	return float64(total) / (float64(covered) * bucketWidth.Seconds())
}
