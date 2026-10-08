package report

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"serverflow/internal/bench/collect"
)

func baseMeta() Metadata {
	n := 3
	return Metadata{
		Date: "2026-10-08T00:00:00Z", HarnessVersion: HarnessVersion, GoVersion: "go1", Machine: "m", GitCommit: "abc", GitTree: "clean",
		Target: "embedded", Scheduler: "round-robin", Models: []string{"m1"}, WorkerCount: &n, GPUType: "none", Mode: "closed",
		Concurrency: 4, DurationSecs: 10, Workload: "mixed", Seed: 1, PlanDigest: "d", Prompt: []PromptClass{{Name: "x", Weight: 1}},
		Repeat: Repeat{Index: 1, Of: 1}, Cluster: &Cluster{Profile: "identical"},
	}
}

func sec(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }

func rec(seq int, at, done float64, stream bool, first float64) collect.Record {
	r := collect.Record{Seq: seq, Model: "m1", Stream: stream, Intended: sec(at), Started: sec(at), Done: sec(done), Status: 200, InputTokens: 10, OutputTokens: 20}
	if stream {
		r.FirstByte = sec(first)
	}
	return r
}

func TestBuildIsSelfConsistentAndExactOnAFixture(t *testing.T) {
	in := Input{
		RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{Start: sec(1), End: sec(11)}, Sampled: true,
		Records: []collect.Record{
			rec(0, 0.5, 0.9, false, 0), // warm-up
			rec(1, 1, 1.5, true, 1.1),
			rec(2, 2, 3, false, 0),
			{Seq: 3, Intended: sec(3), Started: sec(3), Done: sec(3.2), Status: 503},
		},
		Samples: []collect.Sample{
			{At: sec(2), Workers: []collect.WorkerSample{{ID: "a", Model: "m1", Queue: 4}, {ID: "b", Model: "m1", Queue: 0}}},
			{At: sec(3), Workers: []collect.WorkerSample{{ID: "a", Model: "m1", Queue: 2}, {ID: "b", Model: "m1", Queue: 2}}},
		},
		Targets:     []collect.WorkerTarget{{ID: "b", Model: "m1"}, {ID: "a", Model: "m1"}},
		StartCounts: collect.WorkerCounts{"a": 10, "b": 10}, EndCounts: collect.WorkerCounts{"a": 11, "b": 11},
	}
	r := Build(in)
	if r.Summary.Sent != 3 || r.Summary.Succeeded != 2 || r.Summary.Failed != 1 || r.Summary.Warmup != 1 {
		t.Fatalf("%+v", r.Summary)
	}
	if r.Summary.TTFT == nil || r.Summary.TTFT.P50 < 99.9 || r.Summary.TTFT.P50 > 100.1 {
		t.Fatalf("ttft %+v", r.Summary.TTFT)
	}
	if len(r.Workers) != 2 || r.Workers[0].ID != "a" || *r.Workers[0].Completed != 1 || *r.Workers[0].MeanQueue != 3 || *r.Workers[1].MeanQueue != 1 {
		t.Fatalf("workers %+v", r.Workers)
	}
	if r.Imbalance.RequestJain == nil || *r.Imbalance.RequestJain != 1 {
		t.Fatalf("request jain %v", r.Imbalance.RequestJain)
	}
	// Mean queues 3 and 1: (4^2)/(2*10) = 0.8.
	if r.Imbalance.QueueJain == nil || *r.Imbalance.QueueJain < 0.7999999 || *r.Imbalance.QueueJain > 0.8000001 {
		t.Fatalf("queue jain %v", r.Imbalance.QueueJain)
	}
	if _, ok := r.NotMeasured["gpu_utilization"]; !ok || len(r.NotMeasured) != 1 {
		t.Fatalf("only the GPU is unmeasured here: %v", r.NotMeasured)
	}
}

func TestEverythingUnmeasuredIsNamedNotZero(t *testing.T) {
	r := Build(Input{RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{End: sec(5)}})
	for _, k := range []string{"latency", "ttft", "queue", "gpu_utilization", "request_imbalance", "queue_imbalance"} {
		if r.NotMeasured[k] == "" {
			t.Errorf("%s must be listed as not measured with a reason", k)
		}
	}
	if r.Queue != nil || r.Imbalance.RequestJain != nil || r.Summary.Latency != nil {
		t.Fatal("unmeasured values must be nil")
	}
	md := Markdown(r)
	for _, want := range []string{"not measured", "## Not measured", "queue_imbalance"} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}

func TestRequestImbalanceUsesWorkersOfTheSameModelAndTheLowestIndex(t *testing.T) {
	in := Input{
		RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{End: sec(5)},
		Targets:     []collect.WorkerTarget{{ID: "a", Model: "q"}, {ID: "b", Model: "q"}, {ID: "c", Model: "q"}, {ID: "d", Model: "l"}, {ID: "e", Model: "l"}, {ID: "f", Model: "solo"}},
		StartCounts: collect.WorkerCounts{"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0},
		EndCounts:   collect.WorkerCounts{"a": 5, "b": 5, "c": 5, "d": 8, "e": 0, "f": 99},
	}
	r := Build(in)
	// q is perfectly even; l has one worker holding everything of two: 0.5; solo is excluded.
	if r.Imbalance.RequestJainByModel["q"] != 1 || r.Imbalance.RequestJainByModel["l"] != 0.5 || len(r.Imbalance.RequestJainByModel) != 2 {
		t.Fatalf("%v", r.Imbalance.RequestJainByModel)
	}
	if *r.Imbalance.RequestJain != 0.5 {
		t.Fatalf("headline is the lowest: %v", *r.Imbalance.RequestJain)
	}
}

func TestMetadataMissingFindsEmptyFieldsButAcceptsUnknown(t *testing.T) {
	m := baseMeta()
	if got := m.Missing(); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	m.GitCommit, m.GPUType = "", ""
	m.Models = nil
	if got := m.Missing(); len(got) != 3 {
		t.Fatalf("%v", got)
	}
	m.GitCommit, m.GPUType, m.Models = Unknown, Unknown, []string{"x"}
	if len(m.Missing()) != 0 {
		t.Fatal("unknown is a value")
	}
}

func TestRunIDsAreSequentialAndNeverReused(t *testing.T) {
	base := filepath.Join(t.TempDir(), "runs")
	var ids []string
	for range 3 {
		id, dir, err := CreateRun(base)
		if err != nil {
			t.Fatal(err)
		}
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			t.Fatal("directory must exist")
		}
		ids = append(ids, id)
	}
	if strings.Join(ids, " ") != "run_001 run_002 run_003" {
		t.Fatalf("%v", ids)
	}
	// Deleting the newest run must not free its ID.
	if err := os.RemoveAll(filepath.Join(base, "run_003")); err != nil {
		t.Fatal(err)
	}
	id, _, err := CreateRun(base)
	if err != nil || id != "run_004" {
		t.Fatalf("got %q %v, want run_004", id, err)
	}
	// Junk and files are ignored; a gap does not matter.
	_ = os.WriteFile(filepath.Join(base, "run_900"), nil, 0o644)
	_ = os.Mkdir(filepath.Join(base, "notes"), 0o755)
	_ = os.Mkdir(filepath.Join(base, "run_010"), 0o755)
	if id, _, _ := CreateRun(base); id != "run_011" {
		t.Fatalf("got %q", id)
	}
	list, _ := RunIDs(base)
	if strings.Join(list, " ") != "run_001 run_002 run_004 run_010 run_011" {
		t.Fatalf("%v", list)
	}
	if list, err := RunIDs(filepath.Join(base, "none")); err != nil || len(list) != 0 {
		t.Fatalf("missing dir: %v %v", list, err)
	}
}

func TestParseRunID(t *testing.T) {
	for in, want := range map[string]int{"run_001": 1, "run_042": 42, "run_1000": 1000} {
		if n, ok := ParseRunID(in); !ok || n != want {
			t.Errorf("%s: %d %v", in, n, ok)
		}
	}
	for _, bad := range []string{"", "run_", "run_1", "run_abc", "../run_001", "run_001/x", "x_001"} {
		if _, ok := ParseRunID(bad); ok {
			t.Errorf("%q must not parse", bad)
		}
	}
	if FormatRunID(7) != "run_007" || FormatRunID(1234) != "run_1234" {
		t.Fatal("format")
	}
}

func TestWriteLoadRoundTripAndResolve(t *testing.T) {
	base := t.TempDir()
	id, dir, _ := CreateRun(base)
	r := Build(Input{RunID: id, Metadata: baseMeta(), Window: collect.Window{End: sec(5)}, Records: []collect.Record{rec(0, 0, 1, false, 0)}})
	if err := Write(dir, r, []collect.Record{rec(0, 0, 1, false, 0)}); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{ResultFile, ReportFile, RequestsFile} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	p, err := Resolve(base, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Load(p)
	if err != nil || got.RunID != id || got.Summary.Sent != 1 || got.SchemaVersion != SchemaVersion {
		t.Fatalf("%+v %v", got, err)
	}
	// Records are optional.
	_, dir2, _ := CreateRun(base)
	if err := Write(dir2, r, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir2, RequestsFile)); err == nil {
		t.Fatal("requests.jsonl must be off unless asked for")
	}
	if _, err := Resolve(base, "run_099"); err == nil {
		t.Fatal("missing run must be an error")
	}
	if _, err := Resolve(base, "nonsense"); err == nil {
		t.Fatal("non-run must be an error")
	}
	// A different schema version is refused.
	var raw map[string]any
	b, _ := os.ReadFile(filepath.Join(dir, ResultFile))
	_ = json.Unmarshal(b, &raw)
	raw["schema_version"] = 99
	b, _ = json.Marshal(raw)
	_ = os.WriteFile(filepath.Join(dir, ResultFile), b, 0o644)
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "schema version") {
		t.Fatalf("got %v", err)
	}
}

func TestJSONCarriesEveryMetadataFieldIncludingNulls(t *testing.T) {
	m := baseMeta()
	m.WorkerCount, m.Cluster = nil, nil
	b, _ := json.Marshal(Build(Input{RunID: "run_001", Metadata: m, Window: collect.Window{End: sec(1)}}))
	for _, key := range []string{`"git_commit"`, `"git_tree"`, `"models"`, `"worker_count":null`, `"gpu_type"`, `"scheduler"`, `"concurrency"`,
		`"prompt_distribution"`, `"duration_seconds"`, `"seed"`, `"date"`, `"cluster":null`, `"not_measured"`, `"go_version"`, `"machine"`} {
		if !bytes.Contains(b, []byte(key)) {
			t.Errorf("result.json lacks %s", key)
		}
	}
}

func TestGitReportsCommitAndDirtyTree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q")
	_ = os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("benchmark/runs/\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644)
	run("add", ".")
	run("commit", "-q", "-m", "x")
	head := run("rev-parse", "HEAD")

	if g := Git(context.Background(), dir); g.Commit != head || g.Tree != "clean" {
		t.Fatalf("clean tree: %+v (head %s)", g, head)
	}
	_ = os.MkdirAll(filepath.Join(dir, "benchmark", "runs", "run_001"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "benchmark", "runs", "run_001", "result.json"), []byte("{}"), 0o644)
	if g := Git(context.Background(), dir); g.Tree != "clean" {
		t.Fatalf("results must not make the tree dirty: %+v", g)
	}
	_ = os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed"), 0o644)
	if g := Git(context.Background(), dir); g.Commit != head || g.Tree != "dirty" {
		t.Fatalf("dirty tree: %+v", g)
	}
	_ = os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "new.txt"), []byte("n"), 0o644)
	if g := Git(context.Background(), dir); g.Tree != "dirty" {
		t.Fatalf("an untracked file is a change: %+v", g)
	}
}

func TestGitOutsideARepositoryIsUnknownOrBuildStamped(t *testing.T) {
	g := Git(context.Background(), t.TempDir())
	if g.Commit == "" || g.Tree == "" {
		t.Fatalf("must never be empty: %+v", g)
	}
}

func TestEnvironmentFillsEveryCommonField(t *testing.T) {
	var m Metadata
	m.Environment(context.Background(), t.TempDir(), time.Date(2026, 10, 8, 12, 0, 0, 0, time.FixedZone("x", 3600)))
	if m.Date != "2026-10-08T11:00:00Z" || m.HarnessVersion == "" || m.GoVersion == "" || m.Machine == "" || m.GitCommit == "" || m.GitTree == "" {
		t.Fatalf("%+v", m)
	}
}
