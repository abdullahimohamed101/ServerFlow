package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// On a build without the Phase 15 resilience feature the flag is a no-op and the result says "absent":
// recording "off" would make a before/after comparison claim the feature was switched off.
func TestResilienceFlagIsANoOpRecordedAsAbsentWithoutTheFeature(t *testing.T) {
	for _, v := range []string{"", "on", "off"} {
		args := []string{"--workload", "uniform-short"}
		if v != "" {
			args = append(args, "--resilience", v)
		}
		o := quickOptions(t, args...)
		var out bytes.Buffer
		rs, err := Run(context.Background(), o, &out)
		if err != nil || len(rs) != 1 {
			t.Fatalf("--resilience %q: %v", v, err)
		}
		if got := rs[0].Metadata.Resilience; got != "absent" {
			t.Fatalf("--resilience %q recorded %q, want absent", v, got)
		}
		if noted := strings.Contains(out.String(), "has no effect"); noted != (v != "") {
			t.Fatalf("--resilience %q: no-op note printed=%v\n%s", v, noted, out.String())
		}
	}
}

func TestResilienceFlagRejectsOtherValues(t *testing.T) {
	if _, err := ParseRun([]string{"--resilience", "maybe"}); err == nil || !strings.Contains(err.Error(), "--resilience") {
		t.Fatalf("%v", err)
	}
}
