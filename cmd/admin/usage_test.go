package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	usagepkg "serverflow/internal/usage"
)

func TestUsageSummaryReadsTheView(t *testing.T) {
	setupDB(t)
	mustOK(t, "tenant", "create", "acme")
	st := openStore(t)
	ten, err := st.GetTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	var rows []usagepkg.Row
	for i := 0; i < 6; i++ {
		in := int64(10)
		r := usagepkg.Row{EventID: fmt.Sprintf("evt_%032x", i), RequestID: fmt.Sprintf("req_%016x", i), TenantID: ten.ID, Model: "m", Outcome: "completed",
			HTTPStatus: 200, InputTokens: &in, OutputTokens: &in, TokensSource: "usage", OccurredAt: time.Now().Add(-time.Hour)}
		if i%3 == 0 {
			r.Outcome, r.FailureClass, r.HTTPStatus, r.InputTokens, r.OutputTokens, r.TokensSource = "failed", "timeout", 504, nil, nil, "estimate"
		}
		rows = append(rows, r)
	}
	if _, err := st.InsertUsage(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	r := mustOK(t, "usage", "summary", "--since", "7d", "--tenant", "acme")
	// two groups, never summed together: usage (4 requests) and estimate (2 failures)
	if !strings.Contains(r.out, "tokens_source=usage\trequests=4\tfailures=0\tinput_tokens=40") || !strings.Contains(r.out, "tokens_source=estimate\trequests=2\tfailures=2") {
		t.Fatalf("unexpected summary:\n%s", r.out)
	}
	if r := mustOK(t, "usage", "summary", "--since", time.Now().Add(time.Hour).UTC().Format(time.RFC3339)); !strings.Contains(r.out, "no usage") {
		t.Fatalf("future window should be empty:\n%s", r.out)
	}
	if r := admin(t, "usage", "summary", "--since", "banana"); r.code != 2 {
		t.Fatalf("bad --since must be a usage error: %+v", r)
	}
	if r := admin(t, "usage", "nope"); r.code != 2 {
		t.Fatalf("unknown subcommand: %+v", r)
	}
}
