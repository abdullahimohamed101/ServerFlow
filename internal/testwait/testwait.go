// Package testwait is the one place tests wait for a condition. A test that sleeps a fixed time and then asserts is a
// race with the machine: it fails when the machine is slow and wastes time when it is fast. A test that polls a
// condition against a generous deadline passes as soon as the condition holds and fails only if it never does, and the
// failure says what was awaited and what was last seen.
//
// Test support only: it imports the standard library and testing, so any package's tests may use it. Every function
// calls t.Fatalf or t.Fatal on failure and so, like any other use of them, must be called from the test's own goroutine.
// The conditions and buffers passed in are called from that goroutine too but while other goroutines (the code under
// test, a child process's output copier) are running, so they must be safe for concurrent use.
package testwait

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Poll is how often a condition is checked. It is short because a condition is normally a cheap read of memory or one
// local HTTP round trip.
const Poll = 5 * time.Millisecond

// Eventually polls cond every Poll until it returns true and returns how long that took. If cond is still false when
// timeout has passed (it is checked one last time at the deadline), the test fails with msg, the timeout and, when
// describe is given, the state describe reports at that moment. Give timeout generously (seconds): it only costs time
// when the test is about to fail anyway.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string, describe ...func() string) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		if cond() {
			return time.Since(start)
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(Poll)
	}
	t.Fatalf("timed out after %v waiting for: %s%s", timeout, msg, lastState(describe))
	return 0
}

// Never polls cond for d and fails the test as soon as it is true. It is for "this must not happen": a sleep followed
// by a check of absence passes whenever the event is merely late, so the length of d is the only evidence, and a late
// event is only ever caught by waiting for the event's cause. Use Never only where there is nothing to wait for (the
// absence of an event whose cause has already been observed), keep d as short as the cause allows, and say in a comment
// why waiting for something observable is not possible. cond is checked once immediately and once more at the end.
func Never(t testing.TB, d time.Duration, cond func() bool, msg string, describe ...func() string) {
	t.Helper()
	hold(t, d, func() bool { return !cond() }, "happened", msg, describe)
}

// Stays polls cond for d and fails the test as soon as it is false: a state that must be kept, not merely reached.
// The same advice as for Never applies.
func Stays(t testing.TB, d time.Duration, cond func() bool, msg string, describe ...func() string) {
	t.Helper()
	hold(t, d, cond, "was lost", msg, describe)
}

func hold(t testing.TB, d time.Duration, ok func() bool, what, msg string, describe []func() string) {
	t.Helper()
	start := time.Now()
	deadline := start.Add(d)
	for {
		if !ok() {
			t.Fatalf("%s: it %s %v into a %v window%s", msg, what, time.Since(start).Round(time.Millisecond), d, lastState(describe))
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(Poll)
	}
}

// WaitForSubstring waits until buf (a log or stderr buffer that another goroutine is still writing) contains substr
// and returns what it held then. buf's String must be safe for concurrent use with its writer. On failure the test
// reports the tail of what the buffer did hold.
func WaitForSubstring(t testing.TB, buf fmt.Stringer, substr string, timeout time.Duration) string {
	t.Helper()
	var last string
	Eventually(t, timeout, func() bool {
		last = buf.String()
		return strings.Contains(last, substr)
	}, fmt.Sprintf("%q in the output", substr), func() string { return "output so far: " + tail(buf.String(), 2000) })
	return last
}

func lastState(describe []func() string) string {
	if len(describe) == 0 || describe[0] == nil {
		return ""
	}
	return " [last state: " + describe[0]() + "]"
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
