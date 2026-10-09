// Package report defines the benchmark result schema and everything done with results
// after a run: metadata capture, run IDs, persistence, the readable report, and the
// comparison of two runs. It has no network and no load generation.
package report

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Unknown is what the harness records when it cannot determine a metadata value. A
// value is never left out.
const Unknown = "unknown"

// Versions of the result file and of the harness that wrote it.
const (
	SchemaVersion  = 1
	HarnessVersion = "1"
)

// Metadata is what a run must persist (spec section 32) plus what is needed to judge
// whether two runs are comparable. Fields that cannot be known are the string "unknown"
// or JSON null; none is omitted.
type Metadata struct {
	Date           string `json:"date"`
	HarnessVersion string `json:"harness_version"`
	GoVersion      string `json:"go_version"`
	Machine        string `json:"machine"`
	GitCommit      string `json:"git_commit"`
	// GitTree is "clean", "dirty" (uncommitted changes) or "unknown".
	GitTree string `json:"git_tree"`

	// Target is "embedded" (a simulated cluster booted in process) or "remote".
	Target string `json:"target"`
	// TargetHost is the host:port of a remote gateway, never a URL with credentials.
	TargetHost string `json:"target_host"`
	Scheduler  string `json:"scheduler"`
	// SchedulerSource is "embedded" when the harness started the gateway with this scheduler, or
	// "declared (unverified)" when it only records what the user said a remote gateway runs.
	SchedulerSource string   `json:"scheduler_source"`
	Models          []string `json:"models"`
	// WorkerCount is nil (JSON null) when unknown.
	WorkerCount *int   `json:"worker_count"`
	GPUType     string `json:"gpu_type"`

	// Mode is "closed" (fixed concurrency) or "open" (fixed arrival rate).
	Mode         string  `json:"mode"`
	Concurrency  int     `json:"concurrency"`
	Rate         float64 `json:"rate_per_second"`
	MaxInFlight  int     `json:"max_in_flight"`
	DurationSecs float64 `json:"duration_seconds"`
	WarmupSecs   float64 `json:"warmup_seconds"`
	Seed         int64   `json:"seed"`
	Workload     string  `json:"workload"`
	StreamRatio  float64 `json:"stream_ratio"`
	PlanDigest   string  `json:"plan_digest"`
	// Prompt describes the prompt and max_tokens distribution of the workload.
	Prompt []PromptClass `json:"prompt_distribution"`

	Repeat Repeat `json:"repeat"`
	// Cluster describes the embedded cluster; nil for a remote target.
	Cluster *Cluster `json:"cluster"`
}

// PromptClass is one slice of a workload's prompt distribution.
type PromptClass struct {
	Name      string  `json:"name"`
	Weight    float64 `json:"weight"`
	InputMin  int     `json:"input_tokens_min"`
	InputMax  int     `json:"input_tokens_max"`
	OutputMin int     `json:"max_tokens_min"`
	OutputMax int     `json:"max_tokens_max"`
}

// Repeat places a run within a --repeat group. A run on its own is index 1 of 1.
type Repeat struct {
	Index int    `json:"index"`
	Of    int    `json:"of"`
	Group string `json:"group"`
}

// Cluster describes a simulated cluster.
type Cluster struct {
	Profile           string          `json:"worker_profile"`
	HeartbeatMillis   int64           `json:"heartbeat_interval_ms"`
	RegistryRefreshMs int64           `json:"gateway_registry_refresh_ms"`
	Workers           []ClusterWorker `json:"workers"`
}

// ClusterWorker is one mock worker's configuration.
type ClusterWorker struct {
	ID              string  `json:"id"`
	Model           string  `json:"model"`
	TokensPerSecond float64 `json:"tokens_per_second"`
	TTFTMillis      int64   `json:"ttft_ms"`
	MaxConcurrency  int     `json:"max_concurrency"`
	QueueSize       int     `json:"queue_size"`
}

// GitState is the commit and tree state of a source directory.
type GitState struct{ Commit, Tree string }

// Git reports the commit and whether the tree had uncommitted changes for the code under
// test. The VCS stamp of the running binary (go build stamps it) comes first, because the
// binary may be run from any directory, including another repository whose commit would be
// recorded by mistake. Without a stamp (go run, go test) it asks git in dir, and failing
// that reports "unknown". Paths git ignores (such as benchmark/runs) do not make a tree dirty.
func Git(ctx context.Context, dir string) GitState {
	st := GitState{Commit: Unknown, Tree: Unknown}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if s.Value != "" {
					st.Commit = s.Value
				}
			case "vcs.modified":
				switch s.Value {
				case "true":
					st.Tree = "dirty"
				case "false":
					st.Tree = "clean"
				}
			}
		}
		if st.Commit != Unknown {
			return st
		}
		st.Tree = Unknown
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if out, err := runGit(ctx, dir, "rev-parse", "HEAD"); err == nil && out != "" {
		st.Commit = out
		if status, err := runGit(ctx, dir, "status", "--porcelain"); err == nil {
			st.Tree = "clean"
			if status != "" {
				st.Tree = "dirty"
			}
		}
	}
	return st
}

// commandWaitDelay bounds how long a command may keep its pipes open after its context ends.
const commandWaitDelay = 2 * time.Second

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.WaitDelay = commandWaitDelay
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

// Machine describes the host in one line without identifying it (no hostname).
func Machine() string {
	return runtime.GOOS + "/" + runtime.GOARCH + ", " + strconv.Itoa(runtime.NumCPU()) + " CPUs, " + cpuName()
}

func cpuName() string {
	switch runtime.GOOS {
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sysctl", "-n", "machdep.cpu.brand_string")
		cmd.WaitDelay = commandWaitDelay
		if out, err := cmd.Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return s
			}
		}
	case "linux":
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "model name") {
					if _, v, ok := strings.Cut(line, ":"); ok {
						return strings.TrimSpace(v)
					}
				}
			}
		}
	}
	return Unknown
}

// Environment fills the fields every run shares: date, versions, host and git state of
// srcDir.
func (m *Metadata) Environment(ctx context.Context, srcDir string, now time.Time) {
	g := Git(ctx, srcDir)
	m.Date = now.UTC().Format(time.RFC3339)
	m.HarnessVersion = HarnessVersion
	m.GoVersion = runtime.Version()
	m.Machine = Machine()
	m.GitCommit, m.GitTree = g.Commit, g.Tree
}

// Missing reports the names of metadata fields that are empty, which must not happen:
// an unknown value is written as "unknown".
func (m Metadata) Missing() []string {
	var out []string
	for name, v := range map[string]string{
		"date": m.Date, "harness_version": m.HarnessVersion, "go_version": m.GoVersion, "machine": m.Machine,
		"scheduler_source": m.SchedulerSource, "git_commit": m.GitCommit, "git_tree": m.GitTree, "target": m.Target, "scheduler": m.Scheduler,
		"gpu_type": m.GPUType, "mode": m.Mode, "workload": m.Workload, "plan_digest": m.PlanDigest,
	} {
		if v == "" {
			out = append(out, name)
		}
	}
	if len(m.Models) == 0 {
		out = append(out, "models")
	}
	if len(m.Prompt) == 0 {
		out = append(out, "prompt_distribution")
	}
	return out
}
