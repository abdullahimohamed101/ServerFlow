// Package kafkatest helps tests reach a real broker: the shared one started by scripts/dev-kafka.sh (or the CI
// container), and, for outage tests, a private one that a test may stop, pause and restart without disturbing
// other tests.
package kafkatest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/kafka"
	"serverflow/internal/usage"
)

// SkipMessage is what a skipped Kafka test says. scripts/quality.sh greps for it so a skip cannot pass the gate.
const SkipMessage = "SERVERFLOW_TEST_KAFKA_BROKERS is not set"

var counter atomic.Int64

// Config returns a connection to the shared test broker, or skips the test. The test broker requires SASL, so a
// client that forgets its credentials fails here as it would in CI.
func Config(t testing.TB) kafka.Config {
	t.Helper()
	brokers := os.Getenv("SERVERFLOW_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip(SkipMessage)
	}
	// The shared test broker (scripts/dev-kafka.sh, CI) takes SCRAM-SHA-256. SERVERFLOW_TEST_KAFKA_SASL_MECHANISM=plain
	// is for the one-off compatibility run against Apache Kafka (docs/operations/kafka-and-usage.md).
	mech := config.SASLScram256
	if m := os.Getenv("SERVERFLOW_TEST_KAFKA_SASL_MECHANISM"); m != "" {
		mech = m
	}
	return kafka.Config{
		Brokers: strings.Split(brokers, ","), ClientID: "serverflow-test", Topic: "unset",
		SASLMechanism: mech, SASLUsername: os.Getenv("SERVERFLOW_TEST_KAFKA_USER"), SASLPassword: os.Getenv("SERVERFLOW_TEST_KAFKA_PASSWORD"),
		Compression: "snappy", MaxBufferedRecords: 10_000, Linger: 5 * time.Millisecond, BatchMaxBytes: 1 << 20, DeliveryTimeout: 30 * time.Second,
		StartOffset: "earliest",
	}
}

// Unique returns a name no other test, package or run uses.
func Unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d-%d", prefix, os.Getpid(), time.Now().UnixNano()%1_000_000_000, counter.Add(1))
}

// Admin returns an admin connection closed at the end of the test.
func Admin(t testing.TB, cfg kafka.Config) *kafka.Admin {
	t.Helper()
	a, err := kafka.NewAdmin(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	return a
}

// NewTopic creates a topic with a unique name and deletes it when the test ends.
func NewTopic(t testing.TB, cfg kafka.Config, partitions int) string {
	t.Helper()
	name := Unique("sf-test")
	a := Admin(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.CreateTopic(ctx, name, partitions); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.DeleteTopic(c, name)
	})
	return name
}

// ReadN reads n messages from a topic with a fresh consumer group and commits nothing of value; it fails the
// test if they do not arrive within the timeout.
func ReadN(t testing.TB, cfg kafka.Config, topic string, n int, timeout time.Duration) []usage.Message {
	t.Helper()
	cfg.Topic, cfg.GroupID, cfg.StartOffset = topic, Unique("sf-read"), "earliest"
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var out []usage.Message
	deadline := time.Now().Add(timeout)
	for len(out) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msgs, _ := c.Poll(ctx, 1000)
		cancel()
		out = append(out, msgs...)
	}
	if len(out) < n {
		t.Fatalf("read %d of %d messages from %s within %v", len(out), n, topic, timeout)
	}
	return out
}

// PrivateBroker is a Redpanda container owned by one test.
type PrivateBroker struct {
	t         testing.TB
	port      int
	container string
	root      string
}

func repoRoot(t testing.TB) string {
	t.Helper()
	dir, _ := os.Getwd()
	for d := dir; d != filepath.Dir(d); d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "scripts", "dev-kafka.sh")); err == nil {
			return d
		}
	}
	t.Fatal("repository root not found")
	return ""
}

// StartPrivateBroker starts a broker of its own with scripts/dev-kafka.sh on a free port (from
// SERVERFLOW_TEST_KAFKA_PRIVATE_PORT_BASE upward when that is set) and removes it when the test ends. It skips
// when the shared broker is not configured or Docker is unavailable.
func StartPrivateBroker(t testing.TB) *PrivateBroker {
	t.Helper()
	_ = Config(t) // skips with the standard message when Kafka tests are not configured
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not available for the outage test")
	}
	port := 0
	if base, err := strconv.Atoi(os.Getenv("SERVERFLOW_TEST_KAFKA_PRIVATE_PORT_BASE")); err == nil {
		for p := base; p < base+10; p++ {
			if l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p)); err == nil {
				_ = l.Close()
				port = p
				break
			}
		}
	}
	if port == 0 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port = l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
	}
	b := &PrivateBroker{t: t, port: port, container: fmt.Sprintf("serverflow-test-kafka-private-%d", port), root: repoRoot(t)}
	t.Cleanup(func() { _, _ = b.script("stop") })
	if out, err := b.script("start"); err != nil {
		t.Fatalf("start private broker: %v\n%s", err, out)
	}
	return b
}

func (b *PrivateBroker) script(args ...string) ([]byte, error) {
	cmd := exec.Command(filepath.Join(b.root, "scripts", "dev-kafka.sh"), args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DEV_KAFKA_PORT=%d", b.port), "DEV_KAFKA_CONTAINER="+b.container)
	return cmd.CombinedOutput()
}

func (b *PrivateBroker) docker(args ...string) {
	b.t.Helper()
	if out, err := exec.Command("docker", append(args, b.container)...).CombinedOutput(); err != nil {
		b.t.Fatalf("docker %s: %v\n%s", args[0], err, out)
	}
}

// Config returns a connection to this broker.
func (b *PrivateBroker) Config() kafka.Config {
	cfg := Config(b.t)
	cfg.Brokers = []string{fmt.Sprintf("127.0.0.1:%d", b.port)}
	// The private broker is always the Redpanda that scripts/dev-kafka.sh starts, whatever the shared one is.
	cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword = config.SASLScram256, "serverflow-test", "serverflow-test-kafka-password"
	return cfg
}

// Stop stops the broker process (a crash from the clients' point of view); Start brings it back with its data.
func (b *PrivateBroker) Stop() { b.docker("stop", "-t", "0") }
func (b *PrivateBroker) Start() {
	b.docker("start")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if out, err := exec.Command("docker", "exec", b.container, "rpk", "cluster", "health").CombinedOutput(); err == nil && strings.Contains(string(out), "Healthy:") && strings.Contains(string(out), "true") {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Pause freezes the broker: connections stay open and nothing is answered (a black hole). Unpause thaws it.
func (b *PrivateBroker) Pause()   { b.docker("pause") }
func (b *PrivateBroker) Unpause() { b.docker("unpause") }
