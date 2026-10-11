package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

// Event publishing modes (events.mode).
const (
	EventsOff = "off"
	EventsOn  = "on"
)

// Kafka SASL mechanisms (events.sasl_mechanism).
const (
	SASLNone            = "none"
	SASLPlain           = "plain"
	SASLScram256        = "scram-sha-256"
	SASLScram512        = "scram-sha-512"
	EventsStartEarliest = "earliest"
	EventsStartLatest   = "latest"
)

// EventsConfig configures the lifecycle event stream (Phase 12): the gateway's publisher and the usage
// consumer. Events are off by default. SASLPassword is a secret: it is never logged or echoed in an error,
// and formatting an EventsConfig (%v, %+v, %#v, slog, JSON) prints it redacted.
//
// Settings are only validated when mode is "on", so a malformed section cannot stop a gateway that does not
// publish. The usage consumer validates the same section for itself (ValidateConsumer).
type EventsConfig struct {
	Mode    string   `yaml:"mode"`
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	// ClientID names this process to the broker; Source names this gateway instance inside every event
	// (default: the host name).
	ClientID string `yaml:"client_id"`
	Source   string `yaml:"source"`

	TLS           bool   `yaml:"tls"`
	TLSCAFile     string `yaml:"tls_ca_file"`
	SASLMechanism string `yaml:"sasl_mechanism"`
	SASLUsername  string `yaml:"sasl_username"`
	SASLPassword  string `yaml:"sasl_password"`
	// AllowInsecureTransport permits a broker that is not on this machine without TLS or SASL.
	AllowInsecureTransport bool `yaml:"allow_insecure_transport"`

	Compression string `yaml:"compression"`
	// BufferSize bounds the in-memory queue between the request path and the producer; MaxBufferedRecords
	// bounds the Kafka client's own queue. When either is full the newest event is dropped and counted.
	BufferSize         int           `yaml:"buffer_size"`
	MaxBufferedRecords int           `yaml:"max_buffered_records"`
	Linger             time.Duration `yaml:"linger"`
	// BatchMaxBytes caps one produce batch (the client has no record-count cap; a batch is also sent every Linger).
	BatchMaxBytes int `yaml:"batch_max_bytes"`
	// DeliveryTimeout is how long an unacknowledged event is retried before it is dropped.
	DeliveryTimeout      time.Duration `yaml:"delivery_timeout"`
	ShutdownFlushTimeout time.Duration `yaml:"shutdown_flush_timeout"`

	Consumer EventsConsumerConfig `yaml:"consumer"`
}

// EventsConsumerConfig configures the usage consumer.
type EventsConsumerConfig struct {
	GroupID string `yaml:"group_id"`
	// StartOffset is where a group with no committed offset begins: "earliest" or "latest".
	StartOffset  string        `yaml:"start_offset"`
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
	MetricsAddr  string        `yaml:"metrics_addr"`
}

func defaultEvents() EventsConfig {
	return EventsConfig{
		Mode: EventsOff, Topic: "inference.lifecycle.v1", ClientID: "serverflow", SASLMechanism: SASLNone, Compression: "snappy",
		BufferSize: 10_000, MaxBufferedRecords: 10_000, Linger: 50 * time.Millisecond, BatchMaxBytes: 1 << 20,
		DeliveryTimeout: 30 * time.Second, ShutdownFlushTimeout: 5 * time.Second,
		Consumer: EventsConsumerConfig{GroupID: "serverflow-usage", StartOffset: EventsStartEarliest, BatchSize: 500,
			BatchTimeout: time.Second, MetricsAddr: "127.0.0.1:9103"},
	}
}

func (e EventsConfig) redactedSASL() string {
	if e.SASLPassword == "" {
		return "<unset>"
	}
	return "<redacted>"
}

// String formats the config without the password. GoString and LogValue do the same.
func (e EventsConfig) String() string {
	return fmt.Sprintf("{mode:%s brokers:%v topic:%s tls:%t sasl_mechanism:%s sasl_username:%s sasl_password:%s compression:%s buffer_size:%d delivery_timeout:%v consumer:%+v}",
		e.Mode, e.Brokers, e.Topic, e.TLS, e.SASLMechanism, e.SASLUsername, e.redactedSASL(), e.Compression, e.BufferSize, e.DeliveryTimeout, e.Consumer)
}

// GoString implements fmt.GoStringer.
func (e EventsConfig) GoString() string { return "config.EventsConfig" + e.String() }

// LogValue implements slog.LogValuer.
func (e EventsConfig) LogValue() slog.Value { return slog.StringValue(e.String()) }

// MarshalJSON redacts the password, so logging a whole Config as JSON cannot leak it.
func (e EventsConfig) MarshalJSON() ([]byte, error) {
	type plain EventsConfig // no methods, so no recursion
	p := plain(e)
	if p.SASLPassword != "" {
		p.SASLPassword = "<redacted>"
	}
	return json.Marshal(p)
}

// Bounds for the events settings.
const (
	maxEventsBuffer     = 1_000_000
	maxEventsBatch      = 100_000
	maxEventsBatchBytes = 8 << 20
	maxEventsDuration   = 10 * time.Minute
	maxEventsBrokers    = 32
)

// validateConnection checks what both the publisher and the consumer need. Errors name the key, never a value.
func (e *EventsConfig) validateConnection() error {
	if len(e.Brokers) == 0 {
		return fmt.Errorf("events.brokers must list at least one host:port")
	}
	if len(e.Brokers) > maxEventsBrokers {
		return fmt.Errorf("events.brokers lists more than %d brokers", maxEventsBrokers)
	}
	remote := false
	for _, b := range e.Brokers {
		host, port, err := net.SplitHostPort(b)
		if err != nil || host == "" || port == "" || strings.ContainsAny(b, "@/ ") || strings.Contains(b, "://") {
			return fmt.Errorf("events.brokers entries must be plain host:port (no scheme and no credentials)")
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("events.brokers entries must have a port in 1-65535")
		}
		if !isLoopbackHost(host) {
			remote = true
		}
	}
	if e.Topic == "" || len(e.Topic) > 249 || strings.ContainsAny(e.Topic, " /\\") {
		return fmt.Errorf("events.topic must be a valid Kafka topic name")
	}
	if e.ClientID == "" || len(e.ClientID) > 128 {
		return fmt.Errorf("events.client_id must be 1-128 characters")
	}
	switch e.SASLMechanism {
	case SASLNone:
	case SASLPlain, SASLScram256, SASLScram512:
		if e.SASLUsername == "" || e.SASLPassword == "" {
			return fmt.Errorf("events.sasl_username and events.sasl_password are required with events.sasl_mechanism %q", e.SASLMechanism)
		}
	default:
		return fmt.Errorf("events.sasl_mechanism must be none, plain, scram-sha-256 or scram-sha-512")
	}
	if e.TLSCAFile != "" && !e.TLS {
		return fmt.Errorf("events.tls_ca_file needs events.tls")
	}
	if remote && !e.AllowInsecureTransport {
		if !e.TLS && e.SASLMechanism == SASLNone {
			return fmt.Errorf("events.brokers is not on this machine: set events.tls and events.sasl_mechanism, or events.allow_insecure_transport on a trusted private network")
		}
		if !e.TLS && e.SASLMechanism == SASLPlain {
			return fmt.Errorf("events.sasl_mechanism plain would send the password unencrypted to a remote broker: set events.tls, use a scram mechanism, or set events.allow_insecure_transport")
		}
	}
	return nil
}

// validate checks the publisher's settings. gatewayShutdown is gateway.shutdown_timeout.
func (e *EventsConfig) validate(gatewayShutdown time.Duration) error {
	switch e.Mode {
	case EventsOff:
		return nil
	case EventsOn:
	default:
		return fmt.Errorf("events.mode must be %q or %q", EventsOff, EventsOn)
	}
	if err := e.validateConnection(); err != nil {
		return err
	}
	switch e.Compression {
	case "none", "snappy", "lz4", "zstd", "gzip":
	default:
		return fmt.Errorf("events.compression must be none, snappy, lz4, zstd or gzip")
	}
	if e.BufferSize < 1 || e.BufferSize > maxEventsBuffer {
		return fmt.Errorf("events.buffer_size must be in 1-%d", maxEventsBuffer)
	}
	if e.MaxBufferedRecords < 1 || e.MaxBufferedRecords > maxEventsBuffer {
		return fmt.Errorf("events.max_buffered_records must be in 1-%d", maxEventsBuffer)
	}
	if e.BatchMaxBytes < 1024 || e.BatchMaxBytes > maxEventsBatchBytes {
		return fmt.Errorf("events.batch_max_bytes must be in 1024-%d", maxEventsBatchBytes)
	}
	for _, d := range []struct {
		key string
		v   time.Duration
	}{{"events.linger", e.Linger}, {"events.delivery_timeout", e.DeliveryTimeout}, {"events.shutdown_flush_timeout", e.ShutdownFlushTimeout}} {
		if d.v <= 0 || d.v > maxEventsDuration {
			return fmt.Errorf("%s must be > 0 and at most %v", d.key, maxEventsDuration)
		}
	}
	if e.ShutdownFlushTimeout > gatewayShutdown {
		return fmt.Errorf("events.shutdown_flush_timeout must not exceed gateway.shutdown_timeout")
	}
	return nil
}

// ValidateConsumer checks the settings the usage consumer needs, whatever events.mode says (the mode switches
// the gateway's publisher only).
func (c *Config) ValidateConsumer() error {
	e := &c.Events
	if err := e.validateConnection(); err != nil {
		return err
	}
	k := e.Consumer
	if k.GroupID == "" || len(k.GroupID) > 249 || strings.ContainsAny(k.GroupID, " /\\") {
		return fmt.Errorf("events.consumer.group_id must be a valid consumer group name")
	}
	switch k.StartOffset {
	case EventsStartEarliest, EventsStartLatest:
	default:
		return fmt.Errorf("events.consumer.start_offset must be %q or %q", EventsStartEarliest, EventsStartLatest)
	}
	if k.BatchSize < 1 || k.BatchSize > maxEventsBatch {
		return fmt.Errorf("events.consumer.batch_size must be in 1-%d", maxEventsBatch)
	}
	if k.BatchTimeout <= 0 || k.BatchTimeout > maxEventsDuration {
		return fmt.Errorf("events.consumer.batch_timeout must be > 0 and at most %v", maxEventsDuration)
	}
	if host, port, err := net.SplitHostPort(k.MetricsAddr); err != nil || port == "" {
		return fmt.Errorf("events.consumer.metrics_addr must be host:port")
	} else if !isLoopbackHost(host) && !e.AllowInsecureTransport {
		// The consumer's /metrics and /healthz carry no secrets but are not meant for the open network.
		return fmt.Errorf("events.consumer.metrics_addr must be a loopback address (set events.allow_insecure_transport to expose it)")
	}
	return c.Postgres.validate()
}

// applyEventsEnv overlays SERVERFLOW_EVENTS_* variables. Like the rest of applyEnv it ignores unparsable values.
func applyEventsEnv(cfg *Config, env func(string) (string, bool)) {
	e := &cfg.Events
	str := func(key string, dst *string) {
		if v, ok := env(key); ok {
			*dst = v
		}
	}
	lower := func(key string, dst *string) {
		if v, ok := env(key); ok {
			*dst = strings.ToLower(strings.TrimSpace(v))
		}
	}
	lower("EVENTS_MODE", &e.Mode)
	if v, ok := env("EVENTS_BROKERS"); ok {
		e.Brokers = splitList(v)
	}
	for k, dst := range map[string]*string{
		"EVENTS_TOPIC": &e.Topic, "EVENTS_CLIENT_ID": &e.ClientID, "EVENTS_SOURCE": &e.Source, "EVENTS_TLS_CA_FILE": &e.TLSCAFile,
		"EVENTS_SASL_USERNAME": &e.SASLUsername, "EVENTS_SASL_PASSWORD": &e.SASLPassword,
		"EVENTS_CONSUMER_GROUP_ID": &e.Consumer.GroupID, "EVENTS_CONSUMER_METRICS_ADDR": &e.Consumer.MetricsAddr,
	} {
		str(k, dst)
	}
	lower("EVENTS_SASL_MECHANISM", &e.SASLMechanism)
	lower("EVENTS_COMPRESSION", &e.Compression)
	lower("EVENTS_CONSUMER_START_OFFSET", &e.Consumer.StartOffset)
	for k, dst := range map[string]*bool{"EVENTS_TLS": &e.TLS, "EVENTS_ALLOW_INSECURE_TRANSPORT": &e.AllowInsecureTransport} {
		if v, ok := env(k); ok {
			if b, err := strconv.ParseBool(v); err == nil {
				*dst = b
			}
		}
	}
	for k, dst := range map[string]*int{
		"EVENTS_BUFFER_SIZE": &e.BufferSize, "EVENTS_MAX_BUFFERED_RECORDS": &e.MaxBufferedRecords,
		"EVENTS_BATCH_MAX_BYTES": &e.BatchMaxBytes, "EVENTS_CONSUMER_BATCH_SIZE": &e.Consumer.BatchSize,
	} {
		if v, ok := env(k); ok {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	for k, dst := range map[string]*time.Duration{
		"EVENTS_LINGER": &e.Linger, "EVENTS_DELIVERY_TIMEOUT": &e.DeliveryTimeout, "EVENTS_SHUTDOWN_FLUSH_TIMEOUT": &e.ShutdownFlushTimeout,
		"EVENTS_CONSUMER_BATCH_TIMEOUT": &e.Consumer.BatchTimeout,
	} {
		if v, ok := env(k); ok {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}
}
