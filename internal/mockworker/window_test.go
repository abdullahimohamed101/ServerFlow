package mockworker

import (
	"testing"
	"time"
)

// These tests pin the throughput window to exact values. The window is 50
// buckets of 100ms (five seconds); the bucket in which a busy period began is
// skipped because it is only partly filled; the reset boundary is a gap of
// more than 50 buckets. A mutation of any of those constants must change a
// number asserted here.

var windowBase = time.Unix(1_000_000, 0)

// at returns the instant offset into bucket k, counting from windowBase, the
// time of the first token in every test below.
func at(k int64, offset time.Duration) time.Time {
	return windowBase.Add(time.Duration(k)*100*time.Millisecond + offset)
}

func exactly(t *testing.T, got, want float64, what string) {
	t.Helper()
	if got < want-0.001 || got > want+0.001 {
		t.Fatalf("%s: got %v, want exactly %v", what, got, want)
	}
}

func TestWindowConstantsAreFiveSecondsOfHundredMillisecondBuckets(t *testing.T) {
	if bucketWidth != 100*time.Millisecond || windowBuckets != 50 || bucketWidth*windowBuckets != 5*time.Second {
		t.Fatalf("documented window is 50 x 100ms, got %d x %v", windowBuckets, bucketWidth)
	}
	if ringSize <= windowBuckets {
		t.Fatalf("the ring (%d) needs a spare slot beyond the %d-bucket window", ringSize, windowBuckets)
	}
}

func TestWindowAgesOutAtExactlyFiveSeconds(t *testing.T) {
	tp := &throughput{}
	tp.record(at(0, 0), 1) // opens the busy period; its bucket is skipped
	tp.record(at(1, 20*time.Millisecond), 100)

	exactly(t, tp.rate(at(1, 50*time.Millisecond)), 0, "while its own bucket is still filling")
	exactly(t, tp.rate(at(2, 0)), 1000, "one complete bucket of 100 tokens covers 0.1s")
	exactly(t, tp.rate(at(51, 0)), 20, "50 complete buckets: the burst is the oldest one still in the window")
	exactly(t, tp.rate(at(52, 0)), 0, "one bucket later it has aged out")
}

func TestWindowRingNeverOverwritesTheOldestBucketInTheWindow(t *testing.T) {
	tp := &throughput{}
	tp.record(at(0, 0), 1)
	tp.record(at(1, 0), 100)
	// A token in bucket 51 lands in the ring slot after the window's newest
	// bucket. With too small a ring it would share a slot with bucket 1.
	tp.record(at(51, 0), 1)
	exactly(t, tp.rate(at(51, 50*time.Millisecond)), 20, "bucket 1 is still inside the window")
}

func TestWindowResetBoundary(t *testing.T) {
	t.Run("a gap of exactly the window length continues the busy period", func(t *testing.T) {
		tp := &throughput{}
		tp.record(at(0, 0), 1)
		tp.record(at(1, 0), 100)
		tp.record(at(51, 0), 100) // 50 buckets after the last token
		exactly(t, tp.rate(at(53, 0)), 20, "the earlier busy period is still being averaged over")
	})
	t.Run("a longer gap starts a new busy period", func(t *testing.T) {
		tp := &throughput{}
		tp.record(at(0, 0), 1)
		tp.record(at(1, 0), 100)
		tp.record(at(52, 0), 1) // 51 buckets after the last token
		tp.record(at(53, 0), 100)
		exactly(t, tp.rate(at(54, 0)), 1000, "averaged over the new period only, not diluted by the gap")
	})
}

func TestWindowIgnoresAClockThatStepsBackwards(t *testing.T) {
	tp := &throughput{}
	tp.record(at(10, 0), 5)
	tp.record(at(11, 0), 100)
	// A non-monotonic clock jumping back must not panic or index out of range.
	tp.record(at(-40, 0), 7)
	_ = tp.rate(at(-40, 0))
	_ = tp.rate(at(12, 0))
}
