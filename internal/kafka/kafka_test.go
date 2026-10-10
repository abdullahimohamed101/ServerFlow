package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"serverflow/internal/events"
)

const canary = "CANARY-kafka-p4ss-91c2"

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	for d := wd; d != filepath.Dir(d); d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
	}
	t.Fatal("go.mod not found")
	return ""
}

// The Kafka client library may be imported by internal/kafka only (D1, ADR-019). A second importer would spread
// the dependency and its credential handling across the codebase.
func TestOnlyThisPackageImportsTheKafkaClient(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	found := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".worktrees", "node_modules", ".data":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		for _, imp := range f.Imports {
			if strings.Contains(imp.Path.Value, "twmb/franz-go") {
				found++
				if !strings.HasPrefix(filepath.ToSlash(rel), "internal/kafka/") {
					t.Errorf("%s imports %s; only internal/kafka may import the Kafka client", rel, imp.Path.Value)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Fatal("found no importer at all: the walk is broken")
	}
}

func TestConfigNeverPrintsThePassword(t *testing.T) {
	c := Config{Brokers: []string{"k:9092"}, SASLMechanism: "plain", SASLUsername: "svc", SASLPassword: canary, GroupID: "g"}
	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("c", "cfg", c)
	slog.New(slog.NewTextHandler(&logs, nil)).Info("c", "cfg", c)
	js, err := c.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for name, out := range map[string]string{
		"String": c.String(), "GoString": c.GoString(), "%v": fmt.Sprintf("%v", c), "%+v": fmt.Sprintf("%+v", c), "%#v": fmt.Sprintf("%#v", c),
		"ptr %+v": fmt.Sprintf("%+v", &c), "slog": logs.String(), "json": string(js),
	} {
		if strings.Contains(out, canary) {
			t.Errorf("%s leaks the password: %s", name, out)
		}
	}
}

func TestScrubRemovesThePasswordAndItsEncodings(t *testing.T) {
	c := Config{SASLPassword: canary}
	for _, in := range []string{canary, "x " + canary + " y", "AQ" + b64(canary), "pw=" + urlEsc(canary)} {
		if got := c.scrub(in); strings.Contains(got, canary) || strings.Contains(got, b64(canary)) {
			t.Errorf("scrub(%q) = %q", in, got)
		}
	}
	e := c.safe(fmt.Errorf("broker said: %w", errors.New("bad password "+canary)))
	if strings.Contains(e.Error(), canary) || strings.Contains(fmt.Sprintf("%+v %#v", e, e), canary) {
		t.Fatalf("safe error leaks: %v", e)
	}
	if !errors.Is(c.safe(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("a scrubbed error must still match errors.Is")
	}
	if errors.Unwrap(e) != nil {
		t.Fatal("the unscrubbed cause must not be reachable")
	}
}

func b64(s string) string { return fmtB64(s) }

// hostileBroker speaks just enough of the Kafka protocol to take a SASL PLAIN login and then answer it with an
// error message that echoes the password it was sent. A client must not repeat that text anywhere.
type hostileBroker struct {
	ln   net.Listener
	mu   sync.Mutex
	seen []string
}

func startHostileBroker(t *testing.T) *hostileBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &hostileBroker{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go h.serve(c)
		}
	}()
	return h
}

func (h *hostileBroker) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	for {
		var size [4]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(size[:]))
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		key, ver, corr := int16(binary.BigEndian.Uint16(body[0:2])), int16(binary.BigEndian.Uint16(body[2:4])), int32(binary.BigEndian.Uint32(body[4:8]))
		var resp []byte
		switch key {
		case 18: // ApiVersions: always answer in the oldest framing the client will accept
			r := kmsg.NewPtrApiVersionsResponse()
			r.Version = 0
			if ver > 2 {
				r.ErrorCode = 35 // UNSUPPORTED_VERSION: the client retries with an older one
			} else {
				r.Version = ver
			}
			for _, k := range []int16{18, 17, 36} {
				ak := kmsg.NewApiVersionsResponseApiKey()
				ak.ApiKey, ak.MinVersion, ak.MaxVersion = k, 0, map[int16]int16{18: 2, 17: 1, 36: 1}[k]
				r.ApiKeys = append(r.ApiKeys, ak)
			}
			resp = r.AppendTo(nil)
		case 17:
			r := kmsg.NewPtrSASLHandshakeResponse()
			r.Version = ver
			r.SupportedMechanisms = []string{"PLAIN"}
			resp = r.AppendTo(nil)
		case 36:
			// The client's auth bytes are "\x00user\x00password"; echo them back in the error message.
			h.mu.Lock()
			h.seen = append(h.seen, "sasl")
			h.mu.Unlock()
			r := kmsg.NewPtrSASLAuthenticateResponse()
			r.Version = ver
			r.ErrorCode = 58
			msg := "authentication failed for password " + canary + " (echoed by a hostile broker)"
			r.ErrorMessage = &msg
			resp = r.AppendTo(nil)
		default:
			return
		}
		out := binary.BigEndian.AppendUint32(nil, uint32(len(resp)+4))
		out = binary.BigEndian.AppendUint32(out, uint32(corr))
		out = append(out, resp...)
		if _, err := c.Write(out); err != nil {
			return
		}
	}
}

func TestHostileBrokerCannotMakeTheClientEchoThePassword(t *testing.T) {
	b := startHostileBroker(t)
	var logs syncBuffer
	cfg := Config{
		Brokers: []string{b.ln.Addr().String()}, ClientID: "t", Topic: "t", SASLMechanism: "plain", SASLUsername: "svc", SASLPassword: canary,
		MaxBufferedRecords: 10, DeliveryTimeout: 2 * time.Second, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	got := make(chan error, 1)
	if err := p.Produce(events.Record{Key: []byte("k"), Value: []byte("v")}, func(err error) { got <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if err == nil {
			t.Fatal("a record cannot be delivered through a hostile broker")
		}
		if s := fmt.Sprintf("%v|%+v|%#v|%s", err, err, err, err.Error()); strings.Contains(s, canary) {
			t.Fatalf("the delivery error echoes the password: %s", s)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no delivery result")
	}
	b.mu.Lock()
	reached := len(b.seen)
	b.mu.Unlock()
	if reached == 0 {
		t.Fatal("the fake broker never saw the SASL exchange, so this test proved nothing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil && strings.Contains(err.Error(), canary) {
		t.Fatalf("flush error echoes the password: %v", err)
	}
	if strings.Contains(logs.String(), canary) {
		t.Fatalf("the log echoes the password:\n%s", logs.String())
	}
}

// With nothing listening, the producer still starts, never blocks, and refuses at its bound with ErrFull.
func TestProducerStartsWithoutABrokerAndBoundsItsBuffer(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	_ = l.Close() // nothing listens here any more
	var logs syncBuffer
	p, err := NewProducer(Config{Brokers: []string{addr}, ClientID: "t", Topic: "t", MaxBufferedRecords: 50, DeliveryTimeout: time.Hour, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatalf("a broker that is down at start must not be an error: %v", err)
	}
	defer func() { _ = p.Close() }()
	start := time.Now()
	accepted, full := 0, 0
	for i := 0; i < 5000; i++ {
		switch err := p.Produce(events.Record{Key: []byte("k"), Value: []byte("v")}, func(error) {}); {
		case err == nil:
			accepted++
		case errors.Is(err, events.ErrFull):
			full++
		default:
			t.Fatal(err)
		}
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("Produce blocked: %v", time.Since(start))
	}
	if accepted != 50 || full != 4950 {
		t.Fatalf("accepted %d, refused %d; want 50 and 4950", accepted, full)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "kafka is unreachable") && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := strings.Count(logs.String(), "kafka is unreachable"); n != 1 {
		t.Fatalf("the outage must be logged exactly once, got %d:\n%s", n, logs.String())
	}
}

func TestProducerRejectsBadConfigurationWithoutEchoingIt(t *testing.T) {
	if _, err := NewProducer(Config{Topic: "t"}); err == nil {
		t.Error("no brokers must fail")
	}
	if _, err := NewProducer(Config{Brokers: []string{"a:1"}}); err == nil {
		t.Error("no topic must fail")
	}
	_, err := NewProducer(Config{Brokers: []string{"a:1"}, Topic: "t", SASLMechanism: "bogus", SASLPassword: canary})
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Errorf("bad mechanism: %v", err)
	}
	if _, err := NewProducer(Config{Brokers: []string{"a:1"}, Topic: "t", Compression: "rot13"}); err == nil {
		t.Error("bad compression must fail")
	}
	if _, err := NewProducer(Config{Brokers: []string{"a:1"}, Topic: "t", TLS: true, TLSCAFile: "/nonexistent/ca.pem"}); err == nil {
		t.Error("an unreadable CA file must fail")
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }
