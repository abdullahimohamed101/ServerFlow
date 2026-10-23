//go:build linux

package gateway

import (
	"net"
	"strconv"
	"syscall"
	"testing"
)

// holdRefusedPort (Linux) returns an address on which every connection is refused, and keeps the port reserved until the
// test ends. The socket is bound but never listens: connect() gets a reset (ECONNREFUSED) on Linux (on macOS a connect to a bound, non-listening socket hangs until the dial timeout, so the other file keeps the old behaviour there), and no other test's
// listener can be given the same ephemeral port, which is what happens when a closed httptest server's port is reused.
func holdRefusedPort(t *testing.T) string {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatalf("bind: %v", err)
	}
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatalf("getsockname: %v", err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(sa.(*syscall.SockaddrInet4).Port))
}
