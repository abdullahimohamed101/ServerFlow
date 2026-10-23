//go:build linux

package gateway

import (
	"net"
	"testing"
	"time"
)

// TestARefusedWorkerKeepsItsPortAndRefuses: the dead worker's connections are refused, and a listener asked for
// "any port" many times never receives that port while the test runs.
func TestARefusedWorkerKeepsItsPortAndRefuses(t *testing.T) {
	addr := holdRefusedPort(t)
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c.Close()
		t.Fatalf("%s accepted a connection", addr)
	}
	for i := 0; i < 2000; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		got := l.Addr().String()
		_ = l.Close()
		if got == addr {
			t.Fatalf("a new listener was given the port held by the refused worker (%s)", addr)
		}
	}
}
