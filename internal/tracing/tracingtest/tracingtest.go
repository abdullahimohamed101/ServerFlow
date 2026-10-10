// Package tracingtest holds checks shared by the tests of every process that exports spans: the attribute
// allow-list and a canary search over everything a span can carry. Import it from tests only.
package tracingtest

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"serverflow/internal/tracing"
)

// Dump renders every piece of text a span can carry: name, attributes, events and their attributes, links
// and their attributes, status description, instrumentation scope and resource.
func Dump(spans tracetest.SpanStubs) string {
	var b strings.Builder
	kv := func(a []attribute.KeyValue) {
		for _, x := range a {
			fmt.Fprintf(&b, " %s=%s", x.Key, x.Value.String())
		}
	}
	for _, s := range spans {
		fmt.Fprintf(&b, "span %q kind=%v status=%v/%q scope=%s/%s", s.Name, s.SpanKind, s.Status.Code, s.Status.Description, s.InstrumentationScope.Name, s.InstrumentationScope.Version)
		fmt.Fprintf(&b, " trace=%s span=%s parent=%s state=%s", s.SpanContext.TraceID(), s.SpanContext.SpanID(), s.Parent.SpanID(), s.SpanContext.TraceState().String())
		kv(s.Attributes)
		for _, e := range s.Events {
			fmt.Fprintf(&b, " event %q", e.Name)
			kv(e.Attributes)
		}
		for _, l := range s.Links {
			fmt.Fprintf(&b, " link trace=%s span=%s state=%s", l.SpanContext.TraceID(), l.SpanContext.SpanID(), l.SpanContext.TraceState().String())
			kv(l.Attributes)
		}
		if s.Resource != nil {
			kv(s.Resource.Attributes())
		}
		b.WriteString("\n")
	}
	return b.String()
}

// RequireAllowListed fails the test for any attribute key (span, event, link or resource) that is not in
// tracing.AllowedKeys, and for any string value over tracing.MaxAttrValueBytes.
func RequireAllowListed(t *testing.T, spans tracetest.SpanStubs) {
	t.Helper()
	check := func(where string, a []attribute.KeyValue) {
		for _, x := range a {
			if _, ok := tracing.AllowedKeys[x.Key]; !ok {
				t.Errorf("%s uses the attribute %q, which is not in the allow-list (internal/tracing/attrs.go)", where, x.Key)
			}
			if x.Value.Type() == attribute.STRING && len(x.Value.AsString()) > tracing.MaxAttrValueBytes {
				t.Errorf("%s: %s is %d bytes", where, x.Key, len(x.Value.AsString()))
			}
		}
	}
	for _, s := range spans {
		check("span "+s.Name, s.Attributes)
		for _, e := range s.Events {
			check("event "+e.Name+" of "+s.Name, e.Attributes)
		}
		for _, l := range s.Links {
			check("a link of "+s.Name, l.Attributes)
		}
		if s.Resource != nil {
			check("the resource of "+s.Name, s.Resource.Attributes())
		}
	}
}

// RequireNoCanary fails if any canary appears anywhere in what the spans carry.
func RequireNoCanary(t *testing.T, spans tracetest.SpanStubs, canaries ...string) {
	t.Helper()
	dump := Dump(spans)
	for _, c := range canaries {
		if strings.Contains(dump, c) {
			t.Errorf("canary %q leaked into the spans:\n%s", c, dump)
		}
	}
}
