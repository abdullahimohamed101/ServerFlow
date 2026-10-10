// Package observability checks the committed Prometheus rules and Grafana dashboards against what ServerFlow
// really exports (ADR-017). It has no runtime role: its tests keep dashboards, alerts and metric names from
// drifting apart. This file holds the small PromQL helpers the tests share.
package observability

import (
	"regexp"
	"sort"
	"strings"
)

// Grafana variables that appear in dashboard expressions, with the value a check substitutes for them.
var grafanaVars = []struct{ name, value string }{
	{"$__rate_interval", "1m"},
	{"$__range", "1h"},
	{"$__interval", "1m"},
	{"${model}", ".*"}, {"$model", ".*"},
	{"${worker_id}", ".*"}, {"$worker_id", ".*"},
	{"${datasource}", "serverflow-prometheus"}, {"$datasource", "serverflow-prometheus"},
}

// SubstituteVariables replaces the Grafana variables in a dashboard expression with concrete values, so the
// result is plain PromQL. Longer names are replaced first ($__rate_interval before $__range's prefix cases).
func SubstituteVariables(expr string) string {
	for _, v := range grafanaVars {
		expr = strings.ReplaceAll(expr, v.name, v.value)
	}
	return expr
}

// Variables returns the names of the Grafana variables an expression references ($model, ${worker_id},
// $__range, ...), sorted and without duplicates. A dashboard must define each one it uses, apart from Grafana's
// built-in $__range, $__rate_interval and $__interval.
func Variables(expr string) []string {
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?`).FindAllStringSubmatch(expr, -1) {
		seen[m[1]] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

var (
	reString   = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|` + "`[^`]*`")
	reBraces   = regexp.MustCompile(`\{[^{}]*\}`)
	reRange    = regexp.MustCompile(`\[[^\]]*\]`)
	reClause   = regexp.MustCompile(`\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`)
	reIdent    = regexp.MustCompile(`[A-Za-z_:][A-Za-z0-9_:]*`)
	reFuncCall = regexp.MustCompile(`^\s*\(`)
)

// words that are PromQL syntax, not metric names.
var keywords = map[string]bool{
	"and": true, "or": true, "unless": true, "by": true, "without": true, "on": true, "ignoring": true,
	"group_left": true, "group_right": true, "bool": true, "offset": true, "inf": true, "nan": true, "atan2": true,
}

// MetricNames returns the sorted metric (or recording rule) names an expression refers to. The tokenizer, in
// order: remove string literals; remove {label matchers}; remove [ranges]; remove by/without/on/ignoring/
// group_left/group_right clauses with their label lists; then every remaining identifier that is not directly
// followed by "(" (a function) and is not a keyword is a metric name. It does not understand subqueries'
// resolution steps or "@" modifiers specially, which is fine because both leave identifiers intact. Metrics
// written as {__name__="x"} are not seen; the rules do not use that form.
func MetricNames(expr string) []string {
	s := reString.ReplaceAllString(SubstituteVariables(expr), `""`)
	s = reBraces.ReplaceAllString(s, "")
	s = reRange.ReplaceAllString(s, "")
	s = reClause.ReplaceAllString(s, " ")
	seen := map[string]bool{}
	for _, loc := range reIdent.FindAllStringIndex(s, -1) {
		name := s[loc[0]:loc[1]]
		if keywords[name] || reFuncCall.MatchString(s[loc[1]:]) {
			continue
		}
		seen[name] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Family reduces a series name to the metric family that exports it: a histogram's _bucket, _sum and _count
// series belong to the histogram. A name that is itself a family (say, a counter ending in _count) is
// matched first by the caller.
func Family(name string, families map[string]bool) (string, bool) {
	if families[name] {
		return name, true
	}
	for _, suf := range []string{"_bucket", "_sum", "_count"} {
		if base, ok := strings.CutSuffix(name, suf); ok && families[base] {
			return base, true
		}
	}
	return "", false
}

var reLabelItem = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:=~|!~|!=|=)?`)

// LabelNames returns the label names an expression mentions: those in by/without/on/ignoring/group_left/group_right
// clauses and in {label matchers}, sorted and without duplicates. String literals are removed first, so a regular
// expression or a legend text is never read as a name. Like MetricNames it is a tokenizer, not a parser; it exists to
// catch a misspelt label (a "workerid" for "worker_id") that is still valid PromQL and matches nothing.
func LabelNames(expr string) []string {
	s := reString.ReplaceAllString(SubstituteVariables(expr), `""`)
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`\b(?:by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)`).FindAllStringSubmatch(s, -1) {
		for _, item := range strings.Split(m[1], ",") {
			if n := strings.TrimSpace(item); n != "" {
				seen[n] = true
			}
		}
	}
	for _, m := range reBraces.FindAllStringSubmatch(s, -1) {
		body := strings.TrimSuffix(strings.TrimPrefix(m[0], "{"), "}")
		for _, item := range strings.Split(body, ",") {
			if mm := reLabelItem.FindStringSubmatch(item); mm != nil {
				seen[mm[1]] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
