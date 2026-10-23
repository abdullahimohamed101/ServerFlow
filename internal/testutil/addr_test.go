package testutil

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"
)

func TestRefusedAddrRefuses(t *testing.T) {
	addr := RefusedAddr(t)
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		_ = c.Close()
		t.Fatalf("something listens on %s", addr)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("want connection refused, got %v", err)
	}
}

// The reason RefusedAddr exists: an address that was bound and closed is given to the next listener that asks.
func TestARecentlyClosedPortIsNotARefusedAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	released := ln.Addr().String()
	_ = ln.Close()
	if released == RefusedAddr(t) {
		t.Fatalf("an ephemeral listener was given the refused address %s", released)
	}
	// Take the released port ourselves, as another test's server would: the "closed" address now answers.
	taken, err := net.Listen("tcp", released)
	if err != nil {
		t.Skipf("the released port was taken by someone else already: %v", err)
	}
	defer func() { _ = taken.Close() }()
	c, err := net.DialTimeout("tcp", released, time.Second)
	if err != nil {
		t.Fatalf("expected the released port to answer once taken: %v", err)
	}
	_ = c.Close()
}

// connFailure dials addr and reports how the connection ended: the dial error, or the read error of a connection
// that was accepted and reset. Anything that returns data, closes cleanly or hangs fails the test.
func connFailure(t *testing.T, addr string) error {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(make([]byte, 1))
	if n != 0 {
		t.Fatalf("%s sent data while held", addr)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("%s hung instead of failing the connection", addr)
	}
	if err == nil {
		t.Fatalf("%s: read succeeded", addr)
	}
	return err
}

func TestHeldFreeAddrFailsConnectionsAndNobodyElseIsGivenThePort(t *testing.T) {
	addr, _ := HeldFreeAddr(t)
	for range 3 {
		if err := connFailure(t, addr); !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("want a reset connection, got %v", err)
		}
	}
	for i := 0; i < 2000; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		got := l.Addr().String()
		_ = l.Close()
		if got == addr {
			t.Fatalf("a new listener was given the held port %s", addr)
		}
	}
	if l, err := net.Listen("tcp", addr); err == nil {
		_ = l.Close()
		t.Fatalf("the held port %s could be bound by someone else", addr)
	}
}

func TestHeldFreeAddrServesOnItsAddressAndReturnsToRefusingEveryTime(t *testing.T) {
	addr, open := HeldFreeAddr(t)
	for round := 1; round <= 3; round++ {
		_ = connFailure(t, addr)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "up") }))
		_ = srv.Listener.Close()
		srv.Listener = open()
		srv.Start()
		if srv.Listener.Addr().String() != addr {
			t.Fatalf("round %d: served on %s, want %s", round, srv.Listener.Addr(), addr)
		}
		c := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
		resp, err := c.Get("http://" + addr)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(b) != "up" {
			t.Fatalf("round %d: body %q", round, b)
		}
		srv.Close() // closes the lent listener: the port must stay ours
		if _, err := c.Get("http://" + addr); err == nil {
			t.Fatalf("round %d: the address still answers after the server closed", round)
		}
		for i := 0; i < 200; i++ {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			got := l.Addr().String()
			_ = l.Close()
			if got == addr {
				t.Fatalf("round %d: a new listener was given the held port", round)
			}
		}
	}
}

func TestAHeldAddrCanBeReopenedAndClosedTwice(t *testing.T) {
	_, open := HeldFreeAddr(t)
	l := open()
	_ = l.Close()
	l2 := open()
	_ = l2.Close()
	_ = l2.Close() // closing twice is harmless
}

func TestAcceptOnAClosedHeldListenerReturnsErrClosed(t *testing.T) {
	_, open := HeldFreeAddr(t)
	l := open()
	done := make(chan error, 1)
	go func() { _, err := l.Accept(); done <- err }()
	time.Sleep(50 * time.Millisecond)
	_ = l.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("got %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the pending Accept")
	}
	if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("got %v, want net.ErrClosed", err)
	}
}
