package main

import (
	"context"
	"fmt"
	"time"
)

// usageCmd implements `usage summary --since DURATION|RFC3339 [--tenant TENANT]`: it reads usage_records (exact to the instant).
// Rows with different tokens_source are listed separately, never summed together.
func (a *app) usageCmd(ctx context.Context, sub string, args []string) error {
	if sub != "summary" {
		return usagef("unknown usage subcommand %q", truncate(sub))
	}
	fs := newFlags("usage summary")
	since := fs.String("since", "24h", "")
	tenant := fs.String("tenant", "", "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return usagef("usage summary takes only --since and --tenant")
	}
	from, err := parseSince(*since, time.Now())
	if err != nil {
		return err
	}
	tenantID := ""
	if *tenant != "" {
		t, err := a.st.GetTenant(ctx, *tenant)
		if err != nil {
			return err
		}
		tenantID = t.ID
	}
	rows, err := a.st.UsageSummaries(ctx, from, tenantID)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.out, "usage since %s\n", from.UTC().Format(time.RFC3339))
	for _, r := range rows {
		_, _ = fmt.Fprintf(a.out, "tenant=%s\tmodel=%s\ttokens_source=%s\trequests=%d\tfailures=%d\tinput_tokens=%s\toutput_tokens=%s\testimated_cost_tokens=%s\n",
			orDash(r.TenantID), orDash(r.Model), r.TokensSource, r.Requests, r.Failures, r.InputTokens, r.OutputTokens, r.EstimatedCostTokens)
	}
	if len(rows) == 0 {
		_, _ = fmt.Fprintln(a.out, "no usage recorded in that window")
	}
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// parseSince accepts a duration back from now (24h, 7d) or an RFC 3339 time.
func parseSince(s string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	d, err := parseExpiry(s)
	if err != nil {
		return time.Time{}, usagef("--since %q is not a duration such as 24h or 7d, or an RFC 3339 time", truncate(s))
	}
	return now.Add(-d), nil
}
