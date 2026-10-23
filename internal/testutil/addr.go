// Package testutil holds helpers shared by the repository's tests. Nothing outside _test.go files imports it.
//
// The helpers in this file answer one question: "what address do I give a client that must see a dead server?".
// The wrong answer is to listen on port 0, read the port and close the listener. A port that was just closed is
// not an address nothing can listen on: the kernel gives it to the next listener that asks for any port (Linux
// does so readily), including a server the same test starts a moment later, and then the "dead" address answers.
package testutil

import (
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

// refused is port 1 (tcpmux). It is below every ephemeral range (Linux 32768+, macOS and the BSDs 49152+) and it
// is privileged, so no test server is ever handed it by listening on port 0, and nothing listens there on a
// developer machine or a CI runner.
const refused = "127.0.0.1:1"

var (
	refusedOnce sync.Once
	refusedErr  error
)

// RefusedAddr returns a loopback address on which every TCP connection is refused (ECONNREFUSED, at once, on
// macOS and Linux). No test can be given the address by the kernel, so it stays refused for as long as the test
// runs. Use it whenever a client needs a dead server and nothing ever has to start there.
//
// A bound socket that never listens is not a portable substitute: on Linux a connect to it is refused, on macOS
// the connect hangs until the dial timeout. HeldFreeAddr is the helper for an address that must be refused now
// and served later.
//
// The first call of a process checks that the address really refuses (a machine with a service on port 1 fails
// the test with a clear message instead of making it flaky).
func RefusedAddr(t testing.TB) string {
	t.Helper()
	refusedOnce.Do(func() { refusedErr = checkRefused(refused) })
	if refusedErr != nil {
		t.Fatalf("%s is not a refused address on this machine: %v", refused, refusedErr)
	}
	return refused
}

func checkRefused(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		_ = c.Close()
		return errors.New("a connection was accepted: something listens there")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return err
	}
	return nil
}

// HeldFreeAddr reserves a free loopback address for the whole test and returns it with open, which starts a real
// server on exactly that address when the test is ready. Until open is called every connection is accepted and
// reset at once, so a client sees a failed connection (ECONNRESET) and never a hang or a reply; nobody else can
// be given the port, because the listener is never closed before the test ends.
//
// open returns the listener to hand to http.Server.Serve, httptest.Server.Listener or a hand-written accept
// loop. Closing that listener (http.Server.Close does) does not release the port: the address goes back to
// resetting connections, and open may be called again to bring the server back on the same address. That makes a
// restart on the same address safe: down, up, down, up. Only one listener from open may be live at a time.
//
// The port is released when the test ends (t.Cleanup).
func HeldFreeAddr(t testing.TB) (addr string, open func() net.Listener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a loopback port: %v", err)
	}
	h := &held{ln: ln.(*net.TCPListener)}
	h.startIdle()
	t.Cleanup(h.shutdown)
	return ln.Addr().String(), func() net.Listener {
		t.Helper()
		l, err := h.open()
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
}

// held owns the socket. It is in one of two states: idle (a goroutine accepts and resets connections) or lent
// (a heldListener is live and its owner accepts). The socket is closed only by shutdown.
type held struct {
	ln *net.TCPListener

	mu   sync.Mutex
	shut bool
	cur  *heldListener
	idle chan struct{} // closed when the reset loop has ended; nil when no loop runs
}

// startIdle starts the accept-and-reset loop, unless the socket is already being shut down.
func (h *held) startIdle() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shut {
		return
	}
	done := make(chan struct{})
	h.idle = done
	go func() {
		defer close(done)
		for {
			c, err := h.ln.AcceptTCP()
			if err != nil {
				return // the deadline of a hand-over, or the final close
			}
			reset(c)
		}
	}()
}

// stopIdle ends the reset loop and waits for it, leaving the socket without a deadline.
func (h *held) stopIdle() {
	h.mu.Lock()
	done := h.idle
	h.idle = nil
	h.mu.Unlock()
	if done == nil {
		return
	}
	_ = h.ln.SetDeadline(time.Now()) // wakes the pending Accept
	<-done
	_ = h.ln.SetDeadline(time.Time{})
}

func (h *held) open() (net.Listener, error) {
	h.mu.Lock()
	if h.shut {
		h.mu.Unlock()
		return nil, errors.New("testutil: the held address was already released with the test")
	}
	if h.cur != nil {
		h.mu.Unlock()
		return nil, errors.New("testutil: the previous listener of the held address is still open")
	}
	l := &heldListener{h: h}
	h.cur = l
	h.mu.Unlock()
	h.stopIdle()
	return l, nil
}

func (h *held) shutdown() {
	h.mu.Lock()
	h.shut = true
	if h.cur != nil {
		h.cur.closed = true
	}
	done := h.idle
	h.mu.Unlock()
	_ = h.ln.Close()
	if done != nil {
		<-done
	}
}

// reset closes c with a RST instead of a FIN: the peer sees "connection reset by peer", as it would see a refusal.
func reset(c *net.TCPConn) {
	_ = c.SetLinger(0)
	_ = c.Close()
}

// heldListener is the net.Listener lent by open. Its Close hands the socket back to the reset loop.
type heldListener struct {
	h      *held
	wg     sync.WaitGroup // accepts in flight
	closed bool           // guarded by h.mu
}

func (l *heldListener) Addr() net.Addr { return l.h.ln.Addr() }

func (l *heldListener) Accept() (net.Conn, error) {
	h := l.h
	h.mu.Lock()
	if l.closed {
		h.mu.Unlock()
		return nil, net.ErrClosed
	}
	l.wg.Add(1)
	h.mu.Unlock()
	defer l.wg.Done()

	c, err := h.ln.AcceptTCP()
	h.mu.Lock()
	closed := l.closed
	h.mu.Unlock()
	if closed { // closed while waiting: a connection that slipped in is reset like any other
		if c != nil {
			reset(c)
		}
		return nil, net.ErrClosed
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (l *heldListener) Close() error {
	h := l.h
	h.mu.Lock()
	if l.closed {
		h.mu.Unlock()
		return nil
	}
	l.closed = true
	shut := h.shut
	h.mu.Unlock()
	if shut {
		return nil
	}
	_ = h.ln.SetDeadline(time.Now()) // wakes every pending Accept of this listener
	l.wg.Wait()
	_ = h.ln.SetDeadline(time.Time{})
	h.mu.Lock()
	h.cur = nil
	h.mu.Unlock()
	h.startIdle()
	return nil
}
