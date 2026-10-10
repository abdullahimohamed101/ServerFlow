package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPostgresConfigJSONNeverContainsTheDSN(t *testing.T) {
	const secret = "s3cr3t-canary-password"
	cfg := Default()
	cfg.Postgres.DSN = "postgres://user:" + secret + "@db.internal:5432/app?sslmode=verify-full"

	for name, v := range map[string]any{"PostgresConfig": cfg.Postgres, "whole Config": cfg, "pointer to Config": &cfg} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(string(b), secret) || strings.Contains(string(b), "db.internal") {
			t.Errorf("%s JSON contains the DSN: %s", name, b)
		}
	}
	var got map[string]any
	b, _ := json.Marshal(cfg.Postgres)
	if err := json.Unmarshal(b, &got); err != nil || got["DSN"] != "<redacted>" {
		t.Errorf("expected the DSN to be shown as redacted, got %s (%v)", b, err)
	}
	var unset map[string]any
	empty, _ := json.Marshal(PostgresConfig{})
	if err := json.Unmarshal(empty, &unset); err != nil || unset["DSN"] != "" {
		t.Errorf("an unset DSN should stay empty, got %s (%v)", empty, err)
	}
}
