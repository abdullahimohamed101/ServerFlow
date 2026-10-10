// Package kafka is the only package that imports the Kafka client library (github.com/twmb/franz-go). It
// builds producers and consumers from configuration, applies TLS and SASL, keeps the SASL password out of
// logs, errors and formatted output, and logs a broker outage and its recovery once each. Everything else in
// ServerFlow sees only events.Sink and usage.Source (ADR-019); a test fails if another package imports the
// client library.
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"serverflow/internal/config"
)

// Config describes the connection and the producer and consumer settings. SASLPassword is a secret: it is
// never logged or put in an error, and formatting a Config prints it redacted under %v, %+v, %#v, slog and JSON.
type Config struct {
	Brokers  []string
	ClientID string
	Topic    string

	TLS       bool
	TLSCAFile string
	// TLSConfig, if set, replaces the default TLS settings (system roots, TLS 1.2 or later). For tests and
	// embedding; it cannot be set from the configuration file.
	TLSConfig *tls.Config

	SASLMechanism string // none, plain, scram-sha-256, scram-sha-512
	SASLUsername  string
	SASLPassword  string

	// Producer settings.
	Compression        string
	MaxBufferedRecords int
	Linger             time.Duration
	BatchMaxBytes      int
	DeliveryTimeout    time.Duration

	// Consumer settings.
	GroupID     string
	StartOffset string // earliest or latest, for a group with no committed offset
	// LagInterval is how often the consumer asks the broker for the group's lag (default 5s).
	LagInterval time.Duration

	// Logger receives one line when brokers become unreachable and one when they recover; nil discards them.
	Logger *slog.Logger
}

// ConfigFrom maps the application configuration to a kafka Config.
func ConfigFrom(e config.EventsConfig) Config {
	return Config{
		Brokers: e.Brokers, ClientID: e.ClientID, Topic: e.Topic, TLS: e.TLS, TLSCAFile: e.TLSCAFile,
		SASLMechanism: e.SASLMechanism, SASLUsername: e.SASLUsername, SASLPassword: e.SASLPassword,
		Compression: e.Compression, MaxBufferedRecords: e.MaxBufferedRecords, Linger: e.Linger, BatchMaxBytes: e.BatchMaxBytes,
		DeliveryTimeout: e.DeliveryTimeout, GroupID: e.Consumer.GroupID, StartOffset: e.Consumer.StartOffset,
	}
}

// String formats the config without the password.
func (c Config) String() string {
	pw := "<unset>"
	if c.SASLPassword != "" {
		pw = "<redacted>"
	}
	return fmt.Sprintf("{brokers:%v client_id:%s topic:%s tls:%t sasl_mechanism:%s sasl_username:%s sasl_password:%s compression:%s group_id:%s}",
		c.Brokers, c.ClientID, c.Topic, c.TLS, c.SASLMechanism, c.SASLUsername, pw, c.Compression, c.GroupID)
}

// GoString implements fmt.GoStringer.
func (c Config) GoString() string { return "kafka.Config" + c.String() }

// LogValue implements slog.LogValuer.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// MarshalJSON redacts the password.
func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Brokers       []string `json:"brokers"`
		ClientID      string   `json:"client_id"`
		Topic         string   `json:"topic"`
		TLS           bool     `json:"tls"`
		SASLMechanism string   `json:"sasl_mechanism"`
		SASLUsername  string   `json:"sasl_username"`
		SASLPassword  string   `json:"sasl_password"`
		GroupID       string   `json:"group_id"`
	}{c.Brokers, c.ClientID, c.Topic, c.TLS, c.SASLMechanism, c.SASLUsername, map[bool]string{true: "<redacted>", false: ""}[c.SASLPassword != ""], c.GroupID})
}

// scrub removes the password, and the common encodings of it, from text that came from a broker or the
// client library, which may echo what it was sent.
func (c Config) scrub(s string) string {
	if c.SASLPassword == "" {
		return s
	}
	for _, v := range []string{
		c.SASLPassword, base64.StdEncoding.EncodeToString([]byte(c.SASLPassword)), base64.RawStdEncoding.EncodeToString([]byte(c.SASLPassword)),
		url.QueryEscape(c.SASLPassword), url.PathEscape(c.SASLPassword),
	} {
		if v != "" {
			s = strings.ReplaceAll(s, v, "<redacted>")
		}
	}
	return s
}

// safeError carries an error's scrubbed text. It reports Is for what the original reported Is for, but does not
// expose the original through Unwrap, so no formatting or errors.As walk can reach text that was not scrubbed.
type safeError struct {
	msg   string
	cause error
}

func (e *safeError) Error() string        { return e.msg }
func (e *safeError) Is(target error) bool { return errors.Is(e.cause, target) }

// Format prints only the scrubbed message under every verb, so %+v and %#v cannot reach the cause's own text.
func (e *safeError) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, e.msg) }

func (c Config) safe(err error) error {
	if err == nil {
		return nil
	}
	return &safeError{msg: c.scrub(err.Error()), cause: err}
}

func (c Config) saslMechanism() (sasl.Mechanism, error) {
	switch c.SASLMechanism {
	case "", config.SASLNone:
		return nil, nil
	case config.SASLPlain:
		return plain.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsMechanism(), nil
	case config.SASLScram256:
		return scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha256Mechanism(), nil
	case config.SASLScram512:
		return scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha512Mechanism(), nil
	}
	return nil, errors.New("kafka: unknown sasl mechanism")
}

func (c Config) tlsConfig() (*tls.Config, error) {
	if c.TLSConfig != nil {
		return c.TLSConfig, nil
	}
	if !c.TLS {
		return nil, nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.TLSCAFile != "" {
		pem, err := os.ReadFile(c.TLSCAFile)
		if err != nil {
			return nil, errors.New("kafka: could not read events.tls_ca_file")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("kafka: events.tls_ca_file holds no certificate")
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

func compression(name string) (kgo.CompressionCodec, error) {
	switch name {
	case "", "snappy":
		return kgo.SnappyCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	}
	return kgo.NoCompression(), errors.New("kafka: unknown compression")
}

// baseOpts are the client options shared by producers, consumers and the admin client.
func (c Config) baseOpts(h *health) ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.DialTimeout(5 * time.Second),
	}
	tc, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	mech, err := c.saslMechanism()
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	if h != nil {
		opts = append(opts, kgo.WithHooks(h))
	}
	return opts, nil
}
