package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const formatCanary = "CANARY-fmt-s3cret-5d1e"

// fmt consults String and GoString only for some verbs: %d, %t and others print the raw fields of a type that has no
// Formatter. Every config type that holds a secret must print redacted under every verb, as a value, through a pointer,
// inside a slice, a map or another struct, in JSON and through slog.
func TestSecretBearingConfigsAreRedactedUnderEveryVerb(t *testing.T) {
	full := Default()
	full.Redis.Password = formatCanary
	full.Postgres.DSN = "postgres://svc:" + formatCanary + "@db.example:5432/app"
	listen := "127.0.0.1:9100"
	full.Metrics.Listen = &listen
	full.Metrics.Token = formatCanary
	full.Tracing.Endpoint = "https://user:" + formatCanary + "@otel.example:4318"
	full.ControlPlane.Token = formatCanary
	full.Events.SASLPassword = formatCanary

	type wrapper struct{ C Config }
	bundles := map[string]any{
		"RedisConfig": full.Redis, "PostgresConfig": full.Postgres, "MetricsConfig": full.Metrics, "TracingConfig": full.Tracing,
		"ControlPlaneConfig": full.ControlPlane, "EventsConfig": full.Events,
		"Config": full, "wrapper": wrapper{full},
	}
	shapes := map[string]any{}
	for name, v := range bundles {
		shapes[name] = v
	}
	shapes["&Config"] = &full
	shapes["[]Config"] = []Config{full}
	shapes["[]*Config"] = []*Config{&full}
	shapes["map of Config"] = map[string]Config{"k": full}
	shapes["&wrapper"] = &wrapper{full}
	shapes["[]RedisConfig"] = []RedisConfig{full.Redis}
	shapes["[]PostgresConfig"] = []PostgresConfig{full.Postgres}
	shapes["[]MetricsConfig"] = []MetricsConfig{full.Metrics}
	shapes["&RedisConfig"] = &full.Redis
	shapes["&PostgresConfig"] = &full.Postgres
	shapes["&MetricsConfig"] = &full.Metrics
	shapes["&ControlPlaneConfig"] = &full.ControlPlane
	p := &full
	shapes["deref of a pointer"] = *p

	verbs := []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%t", "%e", "%b", "%o", "%c", "%U", "%10.3v", "%-20s"}
	for name, v := range shapes {
		var out strings.Builder
		for _, verb := range verbs {
			fmt.Fprintf(&out, verb+"|", v)
		}
		fmt.Fprint(&out, v)
		fmt.Fprintln(&out, v)
		if js, err := json.Marshal(v); err == nil {
			out.Write(js)
		} else {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		slog.New(slog.NewJSONHandler(&logs, nil)).Info("x", "v", v)
		slog.New(slog.NewTextHandler(&logs, nil)).Info("x", "v", v)
		out.WriteString(logs.String())
		got := out.String()
		for _, secret := range []string{formatCanary, hex.EncodeToString([]byte(formatCanary)), strings.ToUpper(hex.EncodeToString([]byte(formatCanary)))} {
			if strings.Contains(got, secret) {
				t.Errorf("%s leaks a secret: %s", name, got[max(0, strings.Index(got, formatCanary)-150):min(len(got), strings.Index(got, formatCanary)+80)])
			}
		}
	}
}

// %#v keeps the GoString shape, and the other verbs keep the String text, so nothing that already parses either changes.
func TestFormatKeepsTheStringAndGoStringShapes(t *testing.T) {
	r := RedisConfig{Address: "127.0.0.1:6379", Password: formatCanary}
	if got, want := fmt.Sprintf("%v", r), r.String(); got != want {
		t.Errorf("%%v = %q, want String %q", got, want)
	}
	if got, want := fmt.Sprintf("%#v", r), r.GoString(); got != want {
		t.Errorf("%%#v = %q, want GoString %q", got, want)
	}
	if got, want := fmt.Sprintf("%+v", PostgresConfig{DSN: "postgres://u:p@h/db"}), (PostgresConfig{DSN: "postgres://u:p@h/db"}).String(); got != want {
		t.Errorf("%%+v = %q, want %q", got, want)
	}
}

// The root Config keeps printing as before: its own fields, each section formatting itself, %#v under its own name.
func TestRootConfigFormatKeepsItsShape(t *testing.T) {
	c := Default()
	c.Redis.Password = formatCanary
	if got := fmt.Sprintf("%+v", c); !strings.Contains(got, "Gateway:{Port:8080") || !strings.Contains(got, "Redis:"+c.Redis.String()) {
		t.Errorf("%%+v changed shape: %.300s", got)
	}
	if got := fmt.Sprintf("%#v", c); !strings.HasPrefix(got, "config.Config{Gateway:config.GatewayConfig{") || !strings.Contains(got, c.Redis.GoString()) {
		t.Errorf("%%#v changed shape: %.300s", got)
	}
	if got := fmt.Sprintf("%v", &c); !strings.HasPrefix(got, "{{8080 ") && !strings.HasPrefix(got, "&{{8080 ") {
		t.Errorf("%%v of a pointer changed shape: %.200s", got)
	}
}
