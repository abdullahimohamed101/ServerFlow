package engine

import (
	"testing"
	"time"
)

// The window counts only complete buckets: tokens in the still-filling current bucket must not be read.
func TestRateWindowIgnoresTheCurrentBucket(t *testing.T) {
	var w rateWindow
	w.record(0, 1)                      // bucket 0 starts the busy period and is skipped as partly filled
	w.record(150*time.Millisecond, 100) // bucket 1
	w.record(250*time.Millisecond, 100) // bucket 2, still filling at 270ms
	if got := w.rate(170 * time.Millisecond); got != 0 {
		t.Errorf("rate with no complete bucket after the first = %v, want 0", got)
	}
	if got, want := w.rate(270*time.Millisecond), 1000.0; got != want {
		t.Errorf("rate = %v, want %v (only bucket 1 is complete)", got, want)
	}
	// Idle for longer than the window: back to 0.
	if got := w.rate(30 * time.Second); got != 0 {
		t.Errorf("rate after idle = %v, want 0", got)
	}
}
