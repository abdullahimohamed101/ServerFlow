package redistest

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// FakeServer speaks just enough RESP to accept a client's handshake (HELLO is declined so the client falls back
// to RESP2; AUTH, SELECT, CLIENT and PING succeed) and answers every other command with a fixed error reply. It
// stands in for a Redis that is up but erroring (out of memory, loading, read-only, a broken script).
type FakeServer struct {
	ln    net.Listener
	reply atomic.Value // string, a full RESP line without CRLF, e.g. "-OOM command not allowed"
	cmds  atomic.Int64
}

// NewFakeServer starts a server that answers commands with reply (for example "-OOM command not allowed").
func NewFakeServer(t testing.TB, reply string) *FakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &FakeServer{ln: ln}
	f.reply.Store(reply)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

// Addr is the server's address.
func (f *FakeServer) Addr() string { return f.ln.Addr().String() }

// SetReply changes the error every non-handshake command gets.
func (f *FakeServer) SetReply(r string) { f.reply.Store(r) }

// Commands counts non-handshake commands received.
func (f *FakeServer) Commands() int64 { return f.cmds.Load() }

func (f *FakeServer) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		var out string
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			out = "-ERR unknown command 'HELLO'"
		case "AUTH", "SELECT", "CLIENT":
			out = "+OK"
		case "PING":
			out = "+PONG"
		default:
			f.cmds.Add(1)
			out = f.reply.Load().(string)
		}
		if _, err := c.Write([]byte(out + "\r\n")); err != nil {
			return
		}
	}
}

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[0] != '*' {
		return []string{"?"}, nil
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, _ := strconv.Atoi(strings.TrimSpace(hdr[1:]))
		buf := make([]byte, size+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	if len(args) == 0 {
		args = []string{"?"}
	}
	return args, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
