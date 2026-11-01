package shutdown

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// raise delivers sig to this process. Tests use SIGUSR1/SIGUSR2, which nothing else here listens to.
func raise(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := syscall.Kill(os.Getpid(), sig); err != nil {
		t.Fatal(err)
	}
}

func forced() (func(os.Signal), chan os.Signal) {
	c := make(chan os.Signal, 4)
	return func(s os.Signal) { c <- s }, c
}

func TestTheFirstSignalCancelsAndForcesNothing(t *testing.T) {
	force, c := forced()
	ctx, stop := notifyContext(context.Background(), force, syscall.SIGUSR1)
	defer stop()
	raise(t, syscall.SIGUSR1)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the first signal did not cancel the context")
	}
	select {
	case s := <-c:
		t.Fatalf("one signal forced an exit (%v)", s)
	case <-time.After(100 * time.Millisecond):
	}
}

// The second signal is acted on when it arrives the instant the context is cancelled, before anything after the first signal
// has had time to run. With signal.NotifyContext and `go func() { <-ctx.Done(); stop() }()` that window is where it is
// swallowed. (Two signals sent before the first is read are coalesced by the kernel and the runtime into one; that is not
// a case a process can tell apart, so the test waits for the cancellation, which is the first point a second one can matter.)
func TestASecondSignalRightAfterTheContextIsCancelledForcesTheExit(t *testing.T) {
	for i := range 300 {
		force, c := forced()
		ctx, stop := notifyContext(context.Background(), force, syscall.SIGUSR1)
		raise(t, syscall.SIGUSR1)
		<-ctx.Done()
		raise(t, syscall.SIGUSR1)
		select {
		case <-c:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: the second signal was swallowed", i)
		}
		stop()
	}
}

func TestStopReleasesTheSignalsAndForcesNothing(t *testing.T) {
	var n atomic.Int32
	ctx, stop := notifyContext(context.Background(), func(os.Signal) { n.Add(1) }, syscall.SIGUSR2)
	stop()
	stop() // idempotent
	if ctx.Err() == nil {
		t.Fatal("stop must cancel the context")
	}
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("stop forced an exit")
	}
}

func TestAParentCancellationIsNotAForcedExit(t *testing.T) {
	var n atomic.Int32
	parent, cancel := context.WithCancel(context.Background())
	ctx, stop := notifyContext(parent, func(os.Signal) { n.Add(1) }, syscall.SIGUSR2)
	defer stop()
	cancel()
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("cancelling the parent forced an exit")
	}
}
