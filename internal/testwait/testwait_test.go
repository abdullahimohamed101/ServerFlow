package testwait

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorder stands in for *testing.T so a failing call can be observed instead of failing this test. Fatalf ends the
// goroutine it runs in, like the real one.
type recorder struct {
	testing.TB
	mu  sync.Mutex
	msg string
}

func (r *recorder) Helper() {}
func (r *recorder) Fatalf(f string, a ...any) {
	r.mu.Lock()
	r.msg = fmt.Sprintf(f, a...)
	r.mu.Unlock()
	runtime.Goexit()
}
func (r *recorder) failure() string { r.mu.Lock(); defer r.mu.Unlock(); return r.msg }

// run calls f on its own goroutine (so Goexit ends only that) and returns what the recorder saw.
func run(f func(t testing.TB)) string {
	r := &recorder{}
	done := make(chan struct{})
	go func() { defer close(done); f(r) }()
	<-done
	return r.failure()
}

func TestEventuallyReturnsAsSoonAsTheConditionHolds(t *testing.T) {
	var ok atomic.Bool
	time.AfterFunc(30*time.Millisecond, func() { ok.Store(true) })
	start := time.Now()
	took := Eventually(t, 10*time.Second, ok.Load, "the flag")
	if took < 25*time.Millisecond || time.Since(start) > 5*time.Second {
		t.Fatalf("took %v", took)
	}
}

func TestEventuallyReportsWhatWasAwaitedAndTheLastState(t *testing.T) {
	var n atomic.Int64
	msg := run(func(r testing.TB) {
		Eventually(r, 40*time.Millisecond, func() bool { n.Add(1); return false }, "the thing", func() string { return fmt.Sprintf("seen %d times", n.Load()) })
	})
	if !strings.Contains(msg, "waiting for: the thing") || !strings.Contains(msg, "40ms") || !strings.Contains(msg, "last state: seen ") {
		t.Fatalf("message %q", msg)
	}
	if n.Load() < 3 {
		t.Fatalf("polled %d times in 40 ms", n.Load())
	}
}

func TestEventuallyChecksOnceMoreAtTheDeadline(t *testing.T) {
	if msg := run(func(r testing.TB) { Eventually(r, 0, func() bool { return true }, "already true") }); msg != "" {
		t.Fatalf("a condition that holds at once failed with a zero timeout: %q", msg)
	}
}

func TestNeverFailsAsSoonAsTheConditionIsTrue(t *testing.T) {
	var ok atomic.Bool
	time.AfterFunc(20*time.Millisecond, func() { ok.Store(true) })
	start := time.Now()
	msg := run(func(r testing.TB) { Never(r, 10*time.Second, ok.Load, "no event") })
	if !strings.Contains(msg, "no event: it happened") || time.Since(start) > 5*time.Second {
		t.Fatalf("message %q after %v", msg, time.Since(start))
	}
}

func TestNeverPassesWhenTheConditionStaysFalse(t *testing.T) {
	if msg := run(func(r testing.TB) { Never(r, 30*time.Millisecond, func() bool { return false }, "no event") }); msg != "" {
		t.Fatal(msg)
	}
}

func TestStaysFailsWhenTheStateIsLost(t *testing.T) {
	var lost atomic.Bool
	time.AfterFunc(20*time.Millisecond, func() { lost.Store(true) })
	msg := run(func(r testing.TB) {
		Stays(r, 10*time.Second, func() bool { return !lost.Load() }, "the lease", func() string { return "gone" })
	})
	if !strings.Contains(msg, "the lease: it was lost") || !strings.Contains(msg, "last state: gone") {
		t.Fatalf("message %q", msg)
	}
	if msg := run(func(r testing.TB) { Stays(r, 20*time.Millisecond, func() bool { return true }, "x") }); msg != "" {
		t.Fatal(msg)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// Run with -race: the buffer is written while WaitForSubstring reads it.
func TestWaitForSubstringReadsABufferThatIsBeingWritten(t *testing.T) {
	var buf syncBuf
	go func() {
		for i := 0; i < 50; i++ {
			_, _ = fmt.Fprintf(&buf, "line %d\n", i)
			time.Sleep(time.Millisecond)
		}
	}()
	got := WaitForSubstring(t, &buf, "line 40\n", 10*time.Second)
	if !strings.Contains(got, "line 40") {
		t.Fatalf("returned %q", got)
	}
	msg := run(func(r testing.TB) { WaitForSubstring(r, &buf, "never written", 30*time.Millisecond) })
	if !strings.Contains(msg, `"never written" in the output`) || !strings.Contains(msg, "output so far:") {
		t.Fatalf("message %q", msg)
	}
}
