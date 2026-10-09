package redis

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const secretPW = "s3cr3t-Pa55w0rd-do-not-print"

func TestConfigNeverPrintsThePassword(t *testing.T) {
	c := Config{Address: "redis://user:" + secretPW + "@redis.example.com:6379/2", Password: secretPW, Timeout: time.Second}
	outs := []string{
		fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), c.String(),
		fmt.Sprintf("%v", &c), fmt.Sprintf("%+v", []Config{c}), fmt.Sprintf("%v", map[string]Config{"a": c}),
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("cfg", "redis", c, "group", slog.GroupValue(slog.Any("c", c)))
	slog.New(slog.NewTextHandler(&buf, nil)).Info("cfg", "redis", c)
	outs = append(outs, buf.String())
	for _, o := range outs {
		if strings.Contains(o, secretPW) {
			t.Fatalf("password leaked: %s", o)
		}
	}
	if !strings.Contains(c.String(), "<redacted>") {
		t.Fatal("expected a redaction marker")
	}
	if !strings.Contains(Config{Address: "h:1"}.String(), "password:<unset>") {
		t.Fatal("an unset password should say so")
	}
}

func TestOptionsFromAddress(t *testing.T) {
	o, err := Config{Address: "localhost:6379", Password: "p", DB: 3, Timeout: 40 * time.Millisecond}.options()
	if err != nil {
		t.Fatal(err)
	}
	if o.Addr != "localhost:6379" || o.Password != "p" || o.DB != 3 || o.TLSConfig != nil {
		t.Fatalf("unexpected options: %+v", o)
	}
	if o.ReadTimeout != 40*time.Millisecond || o.DialTimeout != 40*time.Millisecond || o.PoolTimeout != 40*time.Millisecond || !o.ContextTimeoutEnabled {
		t.Fatalf("timeouts not applied: %+v", o)
	}
	if o.MaxRetries != 1 {
		t.Fatalf("max retries %d", o.MaxRetries)
	}

	o, err = Config{Address: "rediss://u:pw@redis.example.com:6380/5", Timeout: time.Second}.options()
	if err != nil {
		t.Fatal(err)
	}
	if o.Addr != "redis.example.com:6380" || o.Password != "pw" || o.DB != 5 || o.TLSConfig == nil || o.TLSConfig.ServerName != "redis.example.com" {
		t.Fatalf("rediss URL not parsed by the driver: %+v", o)
	}
	// Explicit settings win over the URL's.
	o, _ = Config{Address: "rediss://u:pw@h:1/5", Password: "other", DB: 7}.options()
	if o.Password != "other" || o.DB != 7 {
		t.Fatalf("explicit settings must override the URL: %+v", o)
	}
}

func TestOptionsTLSSettings(t *testing.T) {
	o, _ := Config{Address: "redis.example.com:6379", TLS: true}.options()
	if o.TLSConfig == nil || o.TLSConfig.MinVersion != tls.VersionTLS12 || o.TLSConfig.ServerName != "redis.example.com" || o.TLSConfig.InsecureSkipVerify {
		t.Fatalf("redis.tls must verify the server on TLS 1.2+: %+v", o.TLSConfig)
	}
	// A supplied TLS config is never allowed below TLS 1.2.
	o, _ = Config{Address: "h:1", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS10}}.options()
	if o.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("TLS minimum not raised")
	}
}

func TestOptionsRejectBadAddressesWithoutEchoingThem(t *testing.T) {
	for _, a := range []string{"", "no-port", "http://h:1", "redis://%zz:" + secretPW + "@h:1", "redis://u:" + secretPW + "@h:notaport", "rediss://:" + secretPW + "@[::1"} {
		_, err := Config{Address: a}.options()
		if err == nil {
			t.Fatalf("%q was accepted", a)
		}
		if strings.Contains(err.Error(), secretPW) || (a != "" && strings.Contains(err.Error(), a)) {
			t.Fatalf("error echoes the address: %v", err)
		}
	}
}

func TestCheckTransport(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"loopback ip", Config{Address: "127.0.0.1:6379"}, true},
		{"localhost", Config{Address: "localhost:6379"}, true},
		{"LOCALHOST", Config{Address: "LocalHost:6379"}, true},
		{"ipv6 loopback", Config{Address: "[::1]:6379"}, true},
		{"loopback url", Config{Address: "redis://127.0.0.1:6379/0"}, true},
		{"ipv6 loopback url", Config{Address: "redis://[::1]:6379"}, true},
		{"unix socket", Config{Address: "unix:///var/run/redis.sock"}, true},
		{"remote plain", Config{Address: "redis.example.com:6379", Password: "p"}, false},
		{"remote plain no password", Config{Address: "10.0.0.5:6379"}, false},
		{"remote tls no password", Config{Address: "redis.example.com:6379", TLS: true}, false},
		{"remote tls and password", Config{Address: "redis.example.com:6379", TLS: true, Password: "p"}, true},
		{"rediss url with password", Config{Address: "rediss://:pw@redis.example.com:6379"}, true},
		{"rediss url without password", Config{Address: "rediss://redis.example.com:6379"}, false},
		{"redis url with password but no tls", Config{Address: "redis://:pw@redis.example.com:6379"}, false},
		{"userinfo trick", Config{Address: "redis://localhost@evil.example.com:6379"}, false},
		{"userinfo trick with tls flag only", Config{Address: "redis://127.0.0.1:x@evil.example.com:6379"}, false},
		{"hostname that starts like loopback", Config{Address: "127.0.0.1.evil.example.com:6379", Password: "p"}, false},
		{"localhost prefix", Config{Address: "localhost.evil.example.com:6379", Password: "p"}, false},
		{"allow insecure", Config{Address: "10.0.0.5:6379", AllowInsecureTransport: true}, true},
		{"allow insecure still validates the address", Config{Address: "nonsense", AllowInsecureTransport: true}, false},
	}
	for _, tc := range cases {
		err := CheckTransport(tc.cfg)
		if (err == nil) != tc.ok {
			t.Errorf("%s: got err=%v, want ok=%t", tc.name, err, tc.ok)
		}
		if err != nil && (strings.Contains(err.Error(), "pw") || strings.Contains(err.Error(), "evil") || strings.Contains(err.Error(), "10.0.0.5")) {
			t.Errorf("%s: error echoes configuration: %v", tc.name, err)
		}
	}
}
