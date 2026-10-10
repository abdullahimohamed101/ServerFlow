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

func TestTracingDefaults(t *testing.T) {
	c := Default().Tracing
	if c.Enabled {
		t.Error("tracing must be off by default")
	}
	if c.Endpoint != "http://127.0.0.1:4318" || c.SampleRatio != 1.0 || c.Incoming != TraceIncomingLink || !c.IncludeTenantID {
		t.Errorf("unexpected defaults: %v", c)
	}
	if c.QueueSize != 2048 || c.MaxExportBatch != 512 || c.BatchTimeout != 5*time.Second || c.ExportTimeout != 5*time.Second || c.AllowInsecureTransport {
		t.Errorf("unexpected pipeline defaults: %v", c)
	}
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

func TestTracingEnvOverrides(t *testing.T) {
	t.Setenv("SERVERFLOW_TRACING_ENABLED", "true")
	t.Setenv("SERVERFLOW_TRACING_ENDPOINT", "https://collector.example:4318")
	t.Setenv("SERVERFLOW_TRACING_SAMPLE_RATIO", "0.25")
	t.Setenv("SERVERFLOW_TRACING_INCOMING", " TRUST ")
	t.Setenv("SERVERFLOW_TRACING_INCLUDE_TENANT_ID", "false")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	c := cfg.Tracing
	if !c.Enabled || c.Endpoint != "https://collector.example:4318" || c.SampleRatio != 0.25 || c.Incoming != TraceIncomingTrust || c.IncludeTenantID {
		t.Errorf("env not applied: %v", c)
	}
}

func TestTracingEnvInvalidValuesAreIgnored(t *testing.T) {
	t.Setenv("SERVERFLOW_TRACING_ENABLED", "maybe")
	t.Setenv("SERVERFLOW_TRACING_SAMPLE_RATIO", "lots")
	t.Setenv("SERVERFLOW_TRACING_INCLUDE_TENANT_ID", "x")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tracing.Enabled || cfg.Tracing.SampleRatio != 1.0 || !cfg.Tracing.IncludeTenantID {
		t.Errorf("invalid env values must leave defaults: %v", cfg.Tracing)
	}
}

func TestTracingValidation(t *testing.T) {
	const canary = "canary-host-xyzzy"
	tests := []struct {
		name    string
		mutate  func(*TracingConfig)
		wantErr string
	}{
		{"valid", func(c *TracingConfig) {}, ""},
		{"https remote", func(c *TracingConfig) { c.Endpoint = "https://" + canary + ":4318" }, ""},
		{"http loopback", func(c *TracingConfig) { c.Endpoint = "http://localhost:4318/v1/traces" }, ""},
		{"http ipv6 loopback", func(c *TracingConfig) { c.Endpoint = "http://[::1]:4318" }, ""},
		{"http remote refused", func(c *TracingConfig) { c.Endpoint = "http://" + canary + ":4318" }, "non-loopback"},
		{"http remote allowed", func(c *TracingConfig) { c.Endpoint = "http://" + canary + ":4318"; c.AllowInsecureTransport = true }, ""},
		{"userinfo", func(c *TracingConfig) { c.Endpoint = "https://user:" + canary + "@collector:4318" }, "credentials"},
		{"query", func(c *TracingConfig) { c.Endpoint = "https://collector:4318/?token=" + canary }, "query"},
		{"fragment", func(c *TracingConfig) { c.Endpoint = "https://collector:4318/#" + canary }, "query or fragment"},
		{"bad scheme", func(c *TracingConfig) { c.Endpoint = "grpc://" + canary }, "http:// or https://"},
		{"empty", func(c *TracingConfig) { c.Endpoint = "" }, "http:// or https://"},
		{"no host", func(c *TracingConfig) { c.Endpoint = "http://" }, "http:// or https://"},
		{"ratio low", func(c *TracingConfig) { c.SampleRatio = -0.1 }, "sample_ratio"},
		{"ratio high", func(c *TracingConfig) { c.SampleRatio = 1.01 }, "sample_ratio"},
		{"ratio zero ok", func(c *TracingConfig) { c.SampleRatio = 0 }, ""},
		{"incoming", func(c *TracingConfig) { c.Incoming = "follow" }, "tracing.incoming"},
		{"queue", func(c *TracingConfig) { c.QueueSize = 0 }, "queue_size"},
		{"batch above queue", func(c *TracingConfig) { c.MaxExportBatch = c.QueueSize + 1 }, "max_export_batch"},
		{"batch timeout", func(c *TracingConfig) { c.BatchTimeout = 0 }, "batch_timeout"},
		{"export timeout", func(c *TracingConfig) { c.ExportTimeout = time.Hour }, "export_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Tracing.Enabled = true
			tt.mutate(&cfg.Tracing)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
			if strings.Contains(err.Error(), canary) {
				t.Errorf("error echoes the endpoint: %v", err)
			}
		})
	}
}

func TestTracingDisabledSkipsValidation(t *testing.T) {
	cfg := Default()
	cfg.Tracing = TracingConfig{Endpoint: "::bad::", SampleRatio: 7, Incoming: "x", QueueSize: -1}
	if err := cfg.Validate(); err != nil {
		t.Errorf("disabled tracing must not validate its other settings: %v", err)
	}
}

func TestTracingConfigIsRedacted(t *testing.T) {
	const secret = "canary-collector-token"
	c := Default().Tracing
	c.Endpoint = "https://user:" + secret + "@collector.internal:4318"

	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("cfg", "tracing", c)
	cfg := Default()
	cfg.Tracing = c
	js, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]string{
		"String": c.String(), "GoString": c.GoString(), "%v": fmt.Sprintf("%v", c), "%+v": fmt.Sprintf("%+v", c), "%#v": fmt.Sprintf("%#v", c),
		"LogValue": logBuf.String(), "JSON": string(js),
	} {
		if strings.Contains(got, secret) || strings.Contains(got, "collector.internal") {
			t.Errorf("%s leaks the endpoint: %s", name, got)
		}
	}
}
