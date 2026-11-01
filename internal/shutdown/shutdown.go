// Package shutdown turns the first SIGINT or SIGTERM into a cancelled context and a second one into an immediate exit.
//
// signal.NotifyContext alone cannot promise the second part. It keeps the signals registered until its stop function
// runs, and the usual `go func() { <-ctx.Done(); stop() }()` runs stop some time AFTER the context is cancelled. A second
// signal that lands in that gap is delivered to the registered channel, which nobody reads any more: it is swallowed, not
// acted on, and a process whose shutdown is stuck cannot be forced out. Here the signals stay registered for the whole
// life of the context and the second one is read and acted on by the goroutine that saw the first, so there is no gap.
package shutdown

import (
	"context"
	"os"
	"os/signal"
	"sync"
)

// NotifyContext returns a context cancelled by the first of sigs (or by the parent, or by stop). A second signal, before
// stop is called, ends the process at once with exit status 1 and without running deferred functions. Call stop to release
// the signal registration; it is safe to call more than once.
func NotifyContext(parent context.Context, sigs ...os.Signal) (context.Context, context.CancelFunc) {
	return notifyContext(parent, func(os.Signal) { os.Exit(1) }, sigs...)
}

// notifyContext is NotifyContext with the forced exit injectable, so a test can observe it instead of dying.
func notifyContext(parent context.Context, force func(os.Signal), sigs ...os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, sigs...)
	done := make(chan struct{})
	go func() {
		// The first signal starts the shutdown. If the context ends some other way (the parent, or stop) the shutdown is
		// under way all the same, so a signal from then on is the "second" one.
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
		}
		select {
		case <-done: // stop was called: the registration is released and nothing is forced
			return
		default:
		}
		select {
		case sig := <-ch:
			force(sig)
		case <-done:
		}
	}()
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			close(done)
			signal.Stop(ch)
		})
		cancel()
	}
}
