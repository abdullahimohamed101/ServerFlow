package integration

import (
	"context"
	"io"
	"net/http"
	"serverflow/internal/postgres/postgrestest"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"serverflow/internal/kafka/kafkatest"
	"serverflow/internal/mockworker"
	"serverflow/pkg/protocol"
)

func eventsGatewayEnv(gwAddr, upstream string, extra ...string) []string {
	_, port, _ := strings.Cut(gwAddr, ":")
	return append([]string{
		"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model, "SERVERFLOW_GATEWAY_UPSTREAM_URL=" + upstream,
		"SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT=15s", "SERVERFLOW_EVENTS_MODE=on", "SERVERFLOW_EVENTS_SHUTDOWN_FLUSH_TIMEOUT=2s",
		"SERVERFLOW_EVENTS_DELIVERY_TIMEOUT=2s", "SERVERFLOW_EVENTS_LINGER=5ms",
	}, extra...)
}

func metricValue(t *testing.T, gw, prefix string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + gw + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	var sum float64
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, prefix) {
			f := strings.Fields(line)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			sum += v
		}
	}
	return sum
}

// A gateway whose Kafka is unreachable starts, serves every request normally, counts the lost events, logs the
// outage once, and still shuts down on time on SIGTERM.
func TestProcessGatewayStartsWithKafkaDownAndStopsOnTime(t *testing.T) {
	mock, _ := startMock(t)
	dead := freePort(t) // nothing listens here
	gw := freePort(t)
	p := startProc(t, "gateway", "gateway starting", eventsGatewayEnv(gw, mock.URL, "SERVERFLOW_EVENTS_BROKERS="+dead))
	for i := 0; i < 20; i++ {
		if code, body := gatewayChat(gw); code != 200 {
			t.Fatalf("request %d with Kafka down: %d %s", i, code, body)
		}
	}
	eventually(t, 20*time.Second, func() bool {
		return metricValue(t, gw, `event_publish_failures_total{reason="delivery_failed"}`) > 0
	}, "dropped events are counted as delivery failures")
	if n := strings.Count(p.stderr.String(), "kafka is unreachable"); n != 1 {
		t.Fatalf("the outage must be logged once, got %d:\n%s", n, p.stderr.String())
	}
	began := time.Now()
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if err := p.wait(t, 15*time.Second); err != nil {
		t.Fatalf("exit: %v\n%s", err, p.stderr.String())
	}
	if d := time.Since(began); d > 8*time.Second {
		t.Fatalf("shutdown took %v with the broker down; the flush must give up on time", d)
	}
}

// SIGTERM with requests in flight: the HTTP server drains first, so every in-flight request still emits its
// terminal event, and the flush delivers them before the process exits.
func TestProcessGatewaySIGTERMFlushesTheTerminalEventsOfInflightRequests(t *testing.T) {
	kcfg := kafkatest.Config(t)
	topic := kafkatest.NewTopic(t, kcfg, 3)
	mock, _ := startMock(t, func(c *mockworker.Config) { c.OutputTokens, c.TokensPerSecond, c.TTFT = 60, 40, 20*time.Millisecond })
	gw := freePort(t)
	p := startProc(t, "gateway", "gateway starting", eventsGatewayEnv(gw, mock.URL,
		"SERVERFLOW_EVENTS_BROKERS="+strings.Join(kcfg.Brokers, ","), "SERVERFLOW_EVENTS_TOPIC="+topic,
		"SERVERFLOW_EVENTS_SASL_MECHANISM="+kcfg.SASLMechanism, "SERVERFLOW_EVENTS_SASL_USERNAME="+kcfg.SASLUsername,
		"SERVERFLOW_EVENTS_SASL_PASSWORD="+kcfg.SASLPassword,
		// A long linger holds the last events in the client until the flush: without Close they would be lost. The flush
		// gets the production default of 5s: it has to connect, authenticate and produce, and on a machine running the
		// whole suite at once 2s was not always enough (the first full-suite run of the fix round lost 18 of 20 events).
		"SERVERFLOW_EVENTS_LINGER=1500ms", "SERVERFLOW_EVENTS_SHUTDOWN_FLUSH_TIMEOUT=5s",
		// The base environment uses a 2s delivery timeout for the broker-down test; here the first connection and SASL
		// handshake may take longer than that on a busy machine (the failure that led here dropped the first events).
		"SERVERFLOW_EVENTS_DELIVERY_TIMEOUT=30s"))
	const inflight = 5
	var wg sync.WaitGroup
	started := make(chan struct{}, inflight)
	results := make(chan int, inflight)
	for i := 0; i < inflight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post("http://"+gw+"/v1/chat/completions", "application/json", strings.NewReader(chat(true, "hello")))
			if err != nil {
				results <- -1
				return
			}
			buf := make([]byte, 16)
			_, _ = resp.Body.Read(buf)
			started <- struct{}{}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			results <- resp.StatusCode
		}()
	}
	for i := 0; i < inflight; i++ {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("requests did not start streaming")
		}
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if err := p.wait(t, 30*time.Second); err != nil {
		t.Fatalf("exit: %v\n%s", err, p.stderr.String())
	}
	wg.Wait()
	close(results)
	for code := range results {
		if code != 200 {
			t.Fatalf("an in-flight stream ended with %d during shutdown", code)
		}
	}
	// received, routed, first_token and completed for each of the five requests
	if n, err := kafkatest.Admin(t, kcfg).Records(context.Background(), topic); err != nil || n != inflight*4 {
		t.Fatalf("the topic holds %d events (%v), want %d; the gateway log:\n%s", n, err, inflight*4, p.stderr.String())
	}
	msgs := kafkatest.ReadN(t, kcfg, topic, inflight*4, 30*time.Second)
	done := map[string]bool{}
	for _, m := range msgs {
		e, err := protocol.Decode(m.Value)
		if err != nil {
			t.Fatal(err)
		}
		if e.EventType == protocol.EventCompleted {
			done[e.RequestID] = true
		}
	}
	if len(done) != inflight {
		t.Fatalf("%d of %d in-flight requests delivered a completed event before exit", len(done), inflight)
	}
	if strings.Contains(p.stderr.String(), kcfg.SASLPassword) {
		t.Fatal("the gateway log contains the SASL password")
	}
}

// The usage-consumer binary end to end: it refuses to start on an unmigrated database, then consumes a request's
// events into a row, serves /healthz and /metrics, and exits cleanly on SIGTERM.
func TestProcessUsageConsumerEndToEnd(t *testing.T) {
	kcfg := kafkatest.Config(t)
	dsn := postgrestest.NewDSN(t)
	topic := kafkatest.NewTopic(t, kcfg, 3)
	metrics := freePort(t)
	env := []string{
		"SERVERFLOW_POSTGRES_DSN=" + dsn, "SERVERFLOW_EVENTS_BROKERS=" + strings.Join(kcfg.Brokers, ","), "SERVERFLOW_EVENTS_TOPIC=" + topic,
		"SERVERFLOW_EVENTS_SASL_MECHANISM=" + kcfg.SASLMechanism, "SERVERFLOW_EVENTS_SASL_USERNAME=" + kcfg.SASLUsername,
		"SERVERFLOW_EVENTS_SASL_PASSWORD=" + kcfg.SASLPassword, "SERVERFLOW_EVENTS_CONSUMER_GROUP_ID=" + kafkatest.Unique("sf-proc"),
		"SERVERFLOW_EVENTS_CONSUMER_METRICS_ADDR=" + metrics, "SERVERFLOW_EVENTS_CONSUMER_BATCH_TIMEOUT=50ms",
	}
	if code, out := runBin(t, "usage-consumer", env); code == 0 || !strings.Contains(out, "migration") {
		t.Fatalf("an unmigrated database must stop the consumer: exit %d, %s", code, out)
	}
	out := execAdmin(t, dsn, "migrate", "up")
	_ = out
	p := startProc(t, "usage-consumer", "usage-consumer starting", env)
	resp, err := http.Get("http://" + metrics + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v", err)
	}
	_ = resp.Body.Close()
	// one completed event for a request
	adm := kafkatest.Admin(t, kcfg)
	e := protocol.Event{EventType: protocol.EventCompleted, SchemaVersion: 1, Timestamp: time.Now(), Source: "t", RequestID: "req_00000000000000aa",
		AttemptID: "att_00000000000000bb", Model: model, Terminal: &protocol.TerminalData{HTTPStatus: 200, TokensSource: protocol.TokensFromEstimate}}
	e.EventID = protocol.NewEventID(e.RequestID, e.EventType, e.AttemptID)
	b, err := protocol.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := adm.Produce(ctx, topic, []byte(e.RequestID), b); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() bool {
		return metricValue(t, metrics, `usage_consumer_records_total{result="inserted"}`) == 1
	}, "the event is inserted")
	if v := metricValue(t, metrics, "consumer_lag{"); v != 0 {
		t.Logf("consumer_lag = %v right after insert (it drains with the commit)", v)
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if err := p.wait(t, 20*time.Second); err != nil {
		t.Fatalf("exit: %v\n%s", err, p.stderr.String())
	}
	if strings.Contains(p.stderr.String(), kcfg.SASLPassword) {
		t.Fatal("the consumer log contains the SASL password")
	}
}
