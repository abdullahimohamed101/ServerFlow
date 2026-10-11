package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// Since Phase 15 PR C the configuration has a resilience section (default off), so the flag is applied and the
// result records the effective mode. (The "absent" case for a build without the section is covered in
// internal/bench/embedded.) The gateway does not read the section before PR D.
func TestResilienceFlagIsRecordedAsTheEffectiveMode(t *testing.T) {
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
		want := map[string]string{"": "off", "on": "on", "off": "off"}[v]
		if got := rs[0].Metadata.Resilience; got != want {
			t.Fatalf("--resilience %q recorded %q, want %q", v, got, want)
		}
		if strings.Contains(out.String(), "has no effect") {
			t.Fatalf("--resilience %q: the build has the section, so the flag is not a no-op\n%s", v, out.String())
		}
	}
}

func TestResilienceFlagRejectsOtherValues(t *testing.T) {
	if _, err := ParseRun([]string{"--resilience", "maybe"}); err == nil || !strings.Contains(err.Error(), "--resilience") {
		t.Fatalf("%v", err)
	}
}
