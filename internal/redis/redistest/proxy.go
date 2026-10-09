package redistest

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Mode is how a Proxy treats connections.
type Mode int32

// The proxy modes.
const (
	// Pass forwards traffic untouched.
	Pass Mode = iota
	// Cut closes every connection and refuses new ones (a stopped Redis: connection refused or reset).
	Cut
	// Blackhole accepts connections and then reads and forwards nothing, so every command hangs (packets dropped).
	Blackhole
	// Slow forwards traffic but delays every chunk by the proxy's delay.
	Slow
)

// Proxy is a TCP forwarder in front of a real server whose behaviour tests change at run time.
type Proxy struct {
	ln         net.Listener
	target     string
	mode       atomic.Int32
	delay      atomic.Int64
	mu         sync.Mutex
	conns      map[net.Conn]struct{}
	accepted   atomic.Int64
	fromClient atomic.Int64 // bytes forwarded from clients to the server
	done       chan struct{}
}

// NewProxy listens on a loopback port and forwards to target. It stops when the test ends.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}, done: make(chan struct{})}
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr is the address clients should use.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// BytesFromClient counts the bytes clients sent through the proxy to the server.
func (p *Proxy) BytesFromClient() int64 { return p.fromClient.Load() }

// Accepted counts connections the proxy accepted.
func (p *Proxy) Accepted() int64 { return p.accepted.Load() }

// SetMode switches behaviour. Cut also closes existing connections.
func (p *Proxy) SetMode(m Mode) {
	p.mode.Store(int32(m))
	if m == Cut {
		p.closeAll()
	}
}

// SetDelay sets the per-chunk delay used in Slow mode.
func (p *Proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }

// Close stops the proxy and closes its connections.
func (p *Proxy) Close() {
	select {
	case <-p.done:
		return
	default:
		close(p.done)
	}
	_ = p.ln.Close()
	p.closeAll()
}

func (p *Proxy) closeAll() {
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		return false
	default:
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.accepted.Add(1)
		if Mode(p.mode.Load()) == Cut || !p.track(c) {
			_ = c.Close()
			continue
		}
		go p.serve(c)
	}
}

func (p *Proxy) serve(client net.Conn) {
	defer func() {
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
		_ = client.Close()
	}()
	if Mode(p.mode.Load()) == Blackhole {
		_, _ = io.Copy(io.Discard, client) // read and drop until the client or the proxy closes
		return
	}
	up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		return
	}
	if !p.track(up) {
		_ = up.Close()
		return
	}
	defer func() {
		p.mu.Lock()
		delete(p.conns, up)
		p.mu.Unlock()
		_ = up.Close()
	}()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pipe(up, client, &p.fromClient); _ = up.Close() }()
	go func() { defer wg.Done(); p.pipe(client, up, nil); _ = client.Close() }()
	wg.Wait()
}

// pipe copies src to dst chunk by chunk, consulting the mode on every chunk so that a change takes effect
// on connections that are already open.
func (p *Proxy) pipe(dst, src net.Conn, count *atomic.Int64) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if count != nil {
				count.Add(int64(n))
			}
			switch Mode(p.mode.Load()) {
			case Blackhole:
				continue // swallow
			case Slow:
				select {
				case <-time.After(time.Duration(p.delay.Load())):
				case <-p.done:
					return
				}
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
