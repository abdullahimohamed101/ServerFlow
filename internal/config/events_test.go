package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const canaryPassword = "CANARY-sasl-p4ss-7f3a"

func eventsOn() Config {
	c := Default()
	c.Events.Mode = EventsOn
	c.Events.Brokers = []string{"127.0.0.1:9092"}
	return c
}

func TestEventsAreOffByDefaultAndDefaultsAreValid(t *testing.T) {
	c := Default()
	if c.Events.Mode != EventsOff {
		t.Fatalf("events must be off by default, got %q", c.Events.Mode)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	on := eventsOn()
	if err := on.Validate(); err != nil {
		t.Fatalf("on with a loopback broker should be valid: %v", err)
	}
}

func TestEventsOffIgnoresAMalformedSection(t *testing.T) {
	c := Default()
	c.Events.Brokers = []string{"not a broker"}
	c.Events.BufferSize = -5
	c.Events.SASLMechanism = "kerberos"
	c.Events.ShutdownFlushTimeout = time.Hour
	if err := c.Validate(); err != nil {
		t.Fatalf("mode off must ignore the section: %v", err)
	}
	c.Events.Mode = "maybe"
	if err := c.Validate(); err == nil {
		t.Fatal("an unknown mode must be refused even when off")
	}
}

func TestEventsValidation(t *testing.T) {
	cases := map[string]func(*Config){
		"no brokers":       func(c *Config) { c.Events.Brokers = nil },
		"url broker":       func(c *Config) { c.Events.Brokers = []string{"kafka://127.0.0.1:9092"} },
		"credentials":      func(c *Config) { c.Events.Brokers = []string{"user:pw@127.0.0.1:9092"} },
		"no port":          func(c *Config) { c.Events.Brokers = []string{"127.0.0.1"} },
		"bad port":         func(c *Config) { c.Events.Brokers = []string{"127.0.0.1:99999"} },
		"empty topic":      func(c *Config) { c.Events.Topic = "" },
		"slash topic":      func(c *Config) { c.Events.Topic = "a/b" },
		"bad sasl":         func(c *Config) { c.Events.SASLMechanism = "gssapi" },
		"sasl no user":     func(c *Config) { c.Events.SASLMechanism = SASLScram256; c.Events.SASLPassword = "x" },
		"sasl no password": func(c *Config) { c.Events.SASLMechanism = SASLScram256; c.Events.SASLUsername = "u" },
		"ca without tls":   func(c *Config) { c.Events.TLSCAFile = "/x" },
		"bad compression":  func(c *Config) { c.Events.Compression = "rot13" },
		"zero buffer":      func(c *Config) { c.Events.BufferSize = 0 },
		"huge buffer":      func(c *Config) { c.Events.BufferSize = maxEventsBuffer + 1 },
		"zero client buf":  func(c *Config) { c.Events.MaxBufferedRecords = 0 },
		"zero batch":       func(c *Config) { c.Events.BatchMaxRecords = 0 },
		"zero linger":      func(c *Config) { c.Events.Linger = 0 },
		"zero delivery":    func(c *Config) { c.Events.DeliveryTimeout = 0 },
		"flush > shutdown": func(c *Config) { c.Events.ShutdownFlushTimeout = c.Gateway.ShutdownTimeout + time.Second },
		"remote plaintext": func(c *Config) { c.Events.Brokers = []string{"kafka.example.com:9092"} },
		"remote plain sasl": func(c *Config) {
			c.Events.Brokers = []string{"kafka.example.com:9092"}
			c.Events.SASLMechanism = SASLPlain
			c.Events.SASLUsername = "u"
			c.Events.SASLPassword = "p"
		},
	}
	for name, mut := range cases {
		c := eventsOn()
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !strings.Contains(err.Error(), "events.") && !strings.Contains(err.Error(), "gateway") {
			t.Errorf("%s: error does not name the key: %v", name, err)
		}
	}
	// the safe remote shapes are accepted, and the opt-out works
	ok := map[string]func(*Config){
		"remote tls": func(c *Config) { c.Events.Brokers = []string{"kafka.example.com:9093"}; c.Events.TLS = true },
		"remote scram": func(c *Config) {
			c.Events.Brokers = []string{"kafka.example.com:9092"}
			c.Events.SASLMechanism = SASLScram512
			c.Events.SASLUsername = "u"
			c.Events.SASLPassword = "p"
		},
		"remote opt-out": func(c *Config) {
			c.Events.Brokers = []string{"kafka.example.com:9092"}
			c.Events.AllowInsecureTransport = true
		},
		"flush == shutdown": func(c *Config) { c.Events.ShutdownFlushTimeout = c.Gateway.ShutdownTimeout },
	}
	for name, mut := range ok {
		c := eventsOn()
		mut(&c)
		if err := c.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestEventsValidationNeverEchoesTheSecretOrBroker(t *testing.T) {
	c := eventsOn()
	c.Events.Brokers = []string{"user:" + canaryPassword + "@kafka.internal:9092"}
	c.Events.SASLMechanism, c.Events.SASLUsername, c.Events.SASLPassword = "bogus", "u", canaryPassword
	if err := c.Validate(); err == nil || strings.Contains(err.Error(), canaryPassword) {
		t.Fatalf("error must exist and hide the secret: %v", err)
	}
	c.Events.Brokers = []string{"127.0.0.1:9092"}
	c.Events.SASLMechanism = SASLScram256
	c.Events.SASLUsername = ""
	if err := c.Validate(); err == nil || strings.Contains(err.Error(), canaryPassword) {
		t.Fatalf("error must exist and hide the secret: %v", err)
	}
}

func TestEventsPasswordIsRedactedEverywhere(t *testing.T) {
	c := Default()
	c.Events.Mode = EventsOn
	c.Events.SASLMechanism, c.Events.SASLUsername, c.Events.SASLPassword = SASLScram256, "svc", canaryPassword
	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("cfg", "events", c.Events, "whole", c.Events.String())
	slog.New(slog.NewTextHandler(&logBuf, nil)).Info("cfg", "events", c.Events)
	js, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	pjs, _ := json.Marshal(&c.Events)
	outputs := map[string]string{
		"String": c.Events.String(), "GoString": c.Events.GoString(), "%v": fmt.Sprintf("%v", c.Events), "%+v": fmt.Sprintf("%+v", c.Events),
		"%#v": fmt.Sprintf("%#v", c.Events), "%s": fmt.Sprintf("%s", c.Events), "ptr %v": fmt.Sprintf("%v", &c.Events), "ptr %+v": fmt.Sprintf("%+v", &c.Events),
		"slog": logBuf.String(), "JSON whole config": string(js), "JSON events": string(pjs),
		"%+v whole config": fmt.Sprintf("%+v", c),
	}
	for name, out := range outputs {
		if strings.Contains(out, canaryPassword) {
			t.Errorf("%s leaks the password: %s", name, out)
		}
	}
	if !strings.Contains(string(pjs), "redacted") || !strings.Contains(c.Events.String(), "<redacted>") {
		t.Error("expected a redaction marker")
	}
}

func TestEventsEnvOverrides(t *testing.T) {
	t.Setenv("SERVERFLOW_EVENTS_MODE", " ON ")
	t.Setenv("SERVERFLOW_EVENTS_BROKERS", "127.0.0.1:1, localhost:2")
	t.Setenv("SERVERFLOW_EVENTS_TOPIC", "t.v1")
	t.Setenv("SERVERFLOW_EVENTS_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("SERVERFLOW_EVENTS_SASL_USERNAME", "u")
	t.Setenv("SERVERFLOW_EVENTS_SASL_PASSWORD", canaryPassword)
	t.Setenv("SERVERFLOW_EVENTS_BUFFER_SIZE", "77")
	t.Setenv("SERVERFLOW_EVENTS_LINGER", "7ms")
	t.Setenv("SERVERFLOW_EVENTS_TLS", "true")
	t.Setenv("SERVERFLOW_EVENTS_CONSUMER_GROUP_ID", "g")
	t.Setenv("SERVERFLOW_EVENTS_CONSUMER_BATCH_SIZE", "5")
	t.Setenv("SERVERFLOW_EVENTS_CONSUMER_START_OFFSET", "latest")
	t.Setenv("SERVERFLOW_EVENTS_BATCH_MAX_RECORDS", "nonsense") // ignored, like every unparsable value
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	e := c.Events
	if e.Mode != EventsOn || len(e.Brokers) != 2 || e.Topic != "t.v1" || e.SASLMechanism != SASLScram256 || e.SASLPassword != canaryPassword ||
		e.BufferSize != 77 || e.Linger != 7*time.Millisecond || !e.TLS || e.Consumer.GroupID != "g" || e.Consumer.BatchSize != 5 ||
		e.Consumer.StartOffset != "latest" || e.BatchMaxRecords != 500 {
		t.Fatalf("env not applied: %+v", e)
	}
}

func TestValidateConsumer(t *testing.T) {
	c := Default()
	c.Events.Brokers = []string{"127.0.0.1:9092"}
	if err := c.ValidateConsumer(); err != nil {
		t.Fatalf("defaults with a broker: %v", err)
	}
	for name, mut := range map[string]func(*Config){
		"no brokers":   func(c *Config) { c.Events.Brokers = nil },
		"no group":     func(c *Config) { c.Events.Consumer.GroupID = "" },
		"bad offset":   func(c *Config) { c.Events.Consumer.StartOffset = "middle" },
		"zero batch":   func(c *Config) { c.Events.Consumer.BatchSize = 0 },
		"zero timeout": func(c *Config) { c.Events.Consumer.BatchTimeout = 0 },
		"public addr":  func(c *Config) { c.Events.Consumer.MetricsAddr = ":9103" },
		"remote addr":  func(c *Config) { c.Events.Consumer.MetricsAddr = "10.0.0.5:9103" },
		"no dsn":       func(c *Config) { c.Postgres.DSN = "" },
	} {
		c := Default()
		c.Events.Brokers = []string{"127.0.0.1:9092"}
		mut(&c)
		if err := c.ValidateConsumer(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
