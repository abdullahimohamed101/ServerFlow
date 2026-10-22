//go:build !unix

package gateway

import (
	"net"
	"testing"
)

// holdRefusedPort: where a bound-but-not-listening socket is not available, take a free port and release it. The
// port can in principle be reused by another test before the refused connection is made.
func holdRefusedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
