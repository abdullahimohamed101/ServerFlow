package telemetry

import (
	"runtime"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"
)

// BuildInfoCollector exports serverflow_build_info{version,commit,go_version} 1 so every binary says what it
// is. The values come from the Go build information (module version and VCS revision when the binary was
// built inside a checkout), never from a file or the environment.
func BuildInfoCollector() prometheus.Collector {
	version, commit := "unknown", "unknown"
	if bi, ok := debug.ReadBuildInfo(); ok {
		if v := bi.Main.Version; v != "" && v != "(devel)" {
			version = v
		}
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				commit = s.Value
				if len(commit) > 12 {
					commit = commit[:12]
				}
			}
		}
	}
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "serverflow_build_info",
		Help:        "A constant 1, labelled with the version, VCS revision and Go version of this binary.",
		ConstLabels: prometheus.Labels{"version": version, "commit": commit, "go_version": runtime.Version()},
	})
	g.Set(1)
	return g
}
