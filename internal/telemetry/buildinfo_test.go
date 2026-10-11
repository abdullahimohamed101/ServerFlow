package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"serverflow/internal/version"
)

func buildInfoLabels(t *testing.T) map[string]string {
	t.Helper()
	ch := make(chan prometheus.Metric, 1)
	BuildInfoCollector().Collect(ch)
	var m dto.Metric
	if err := (<-ch).Write(&m); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, l := range m.GetLabel() {
		out[l.GetName()] = l.GetValue()
	}
	return out
}

func TestBuildInfoPrefersLinkerStamp(t *testing.T) {
	oldV, oldC := version.Version, version.Commit
	t.Cleanup(func() { version.Version, version.Commit = oldV, oldC })

	version.Version, version.Commit = "", ""
	if l := buildInfoLabels(t); l["version"] == "" || l["commit"] == "" {
		t.Fatalf("unstamped labels must still be present: %v", l)
	}

	version.Version, version.Commit = "v1.2.3-4-gabc", "0123456789abcdef0123456789abcdef01234567"
	l := buildInfoLabels(t)
	if l["version"] != "v1.2.3-4-gabc" || l["commit"] != "0123456789ab" {
		t.Fatalf("stamp not used (commit is cut to 12 characters): %v", l)
	}
}
