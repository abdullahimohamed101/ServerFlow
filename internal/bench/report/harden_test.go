package report

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"serverflow/internal/bench/collect"
)

func TestCleanRemovesEverythingATerminalCouldObey(t *testing.T) {
	for in, want := range map[string]string{
		"plain text 1.5":            "plain text 1.5",
		"\x1b[31mred\x1b[0m":        "?[31mred?[0m",
		"line1\r\nline2\tx":         "line1??line2?x",
		"bad\xffutf8":               "bad?utf8",
		"\u202eevil\u202c":          "?evil?", // bidirectional override and pop
		"zero\u200bwidth":           "zero?width",
		"\x07bell\x00nul":           "?bell?nul",
		"snowman \u2603 and \u00e9": "snowman \u2603 and \u00e9",
		"\u2028sep":                 "?sep",
		"\x9b31m":                   "?31m", // the C1 control sequence introducer
		strings.Repeat("a", 500):    strings.Repeat("a", 200) + "...",
		"":                          "",
	} {
		if got := Clean(in); got != want {
			t.Errorf("Clean(%q) = %q, want %q", in, got, want)
		}
	}
}

func hostile() Result {
	r := Result{SchemaVersion: SchemaVersion, RunID: "run_001\x1b[2J", Valid: true, NotMeasured: map[string]string{}}
	r.Metadata = baseMeta()
	r.Metadata.Scheduler = "evil\x1b]0;pwned\x07\rovert\n"
	r.Metadata.Workload = "w\u202eorkload"
	r.Metadata.Date = "d\x1b[31m"
	return r
}

func TestHostileStringsInAResultNeverReachThePrintedOutput(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "run_001")
	_ = os.Mkdir(dir, 0o755)
	if err := Write(dir, hostile(), nil); err != nil {
		t.Fatal(err)
	}
	var list bytes.Buffer
	if err := List(&list, base); err != nil {
		t.Fatal(err)
	}
	other := hostile()
	other.RunID = "run_002"
	other.Metadata.Scheduler = "x"
	var cmp bytes.Buffer
	if err := Compare(hostile(), other).Write(&cmp); err != nil {
		t.Fatal(err)
	}
	for name, out := range map[string]string{"list": list.String(), "compare": cmp.String(), "markdown": Markdown(hostile())} {
		for _, bad := range []string{"\x1b", "\r", "\u202e", "\x07"} {
			if strings.Contains(out, bad) {
				t.Errorf("%s output contains %q:\n%q", name, bad, out)
			}
		}
	}
	// A hostile scheduler must not be able to start a new line either.
	if strings.Contains(list.String(), "overt\n") {
		t.Errorf("a newline in a field split the list: %q", list.String())
	}
}

func TestLoadRefusesSymlinksAndNeverQuotesFileContent(t *testing.T) {
	base := t.TempDir()
	secret := filepath.Join(base, "secret.txt")
	_ = os.WriteFile(secret, []byte("Xtop-secret-content"), 0o644)

	// A symlinked result.json inside a run directory.
	dir := filepath.Join(base, "run_001")
	_ = os.Mkdir(dir, 0o755)
	if err := os.Symlink(secret, filepath.Join(dir, ResultFile)); err != nil {
		t.Skip("symlinks unavailable")
	}
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "symbolic link") || strings.Contains(err.Error(), "secret-content") || strings.Contains(err.Error(), "'X'") {
		t.Fatalf("%v", err)
	}
	// A symlinked run directory, and a symlink given directly.
	link := filepath.Join(base, "run_002")
	_ = os.Symlink(dir, link)
	if _, err := Load(link); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("%v", err)
	}
	if _, err := Load(filepath.Join(link, ResultFile)); err == nil {
		t.Fatal("a path through a symlinked directory must not be a valid result either")
	}

	// Content that is not JSON must not appear in the error.
	bad := filepath.Join(base, "run_003")
	_ = os.Mkdir(bad, 0o755)
	_ = os.WriteFile(filepath.Join(bad, ResultFile), []byte("Xtop-secret-content"), 0o644)
	_, err := Load(bad)
	if err == nil || strings.Contains(err.Error(), "X") && strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "invalid character") {
		t.Fatalf("%v", err)
	}
	// Valid JSON of the wrong shape: the field name, never the value.
	wrong := filepath.Join(base, "run_004")
	_ = os.Mkdir(wrong, 0o755)
	_ = os.WriteFile(filepath.Join(wrong, ResultFile), []byte(`{"schema_version": "top-secret-content"}`), 0o644)
	if _, err := Load(wrong); err == nil || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("%v", err)
	}
	// A directory where a file is expected, and a FIFO-like non-regular file.
	if _, err := Load(filepath.Join(base, "nonexistent")); err == nil {
		t.Fatal("missing")
	}
	// A symlink among the siblings is skipped with a warning, not followed.
	_ = os.Remove(link)
	mk := func(i int) Result {
		return Result{SchemaVersion: SchemaVersion, RunID: FormatRunID(i), Valid: true, Metadata: Metadata{Repeat: Repeat{Index: i, Of: 3, Group: "g"}}}
	}
	for i := 10; i <= 12; i++ {
		d := filepath.Join(base, FormatRunID(i))
		_ = os.Mkdir(d, 0o755)
		if err := Write(d, mk(i), nil); err != nil {
			t.Fatal(err)
		}
	}
	g, warns, err := LoadGroup(base, mk(10))
	if err != nil || len(g) != 3 || len(warns) < 2 {
		t.Fatalf("%d %v %v", len(g), warns, err)
	}
}

func TestComparingTwoMembersOfOneRepeatGroupWarns(t *testing.T) {
	a := result("run_001", 100, 100, 10, 1, 0)
	b := result("run_002", 101, 100, 10, 1, 0)
	a.Metadata.Repeat = Repeat{Index: 1, Of: 3, Group: "g"}
	b.Metadata.Repeat = Repeat{Index: 2, Of: 3, Group: "g"}
	var out bytes.Buffer
	_ = Compare(a, b).Write(&out)
	if !strings.HasPrefix(out.String(), "WARNING: run_001 and run_002 belong to the same repeat group") {
		t.Fatalf("%s", out.String())
	}
	out.Reset()
	_ = Compare(a, a).Write(&out)
	if !strings.HasPrefix(out.String(), "WARNING: A and B are the same run") {
		t.Fatalf("%s", out.String())
	}
	// Different groups, and no groups: no such warning.
	b.Metadata.Repeat.Group = "h"
	if w := Compare(a, b).Warnings; len(w) != 0 {
		t.Fatalf("%v", w)
	}
}

func TestOneSideWithFewerThanThreeRunsDoesNotSayTwoSingleRuns(t *testing.T) {
	ga, gb := group(1, 2, 3, 4, 5), group(9)
	var out bytes.Buffer
	_ = CompareGroups(ga[0], gb[0], ga, gb).Write(&out)
	if strings.Contains(out.String(), "Two single runs") || !strings.Contains(out.String(), "A side with fewer than 3 runs") {
		t.Fatalf("%s", out.String())
	}
}

func TestAMetricMeasuredInFewerThanThreeGroupRunsGetsNoVerdict(t *testing.T) {
	ga, gb := group(10, 11, 12), group(20, 21, 22)
	for i := range ga {
		ga[i].Summary.TTFT = &collect.Dist{P95: float64(10 + i)}
		gb[i].Summary.TTFT = &collect.Dist{P95: float64(50 + i)}
	}
	ga[2].Summary.TTFT = nil // only two of the three runs of A have a TTFT
	c := CompareGroups(ga[0], gb[0], ga, gb)
	if d := find(t, c, "ttft_p95"); d.Spread != "insufficient repeats" || d.A != nil {
		t.Fatalf("%+v", d)
	}
	if d := find(t, c, "throughput"); d.Spread != "ranges do not overlap" {
		t.Fatalf("a complete metric still gets its verdict: %+v", d)
	}
}

func TestAMaxErrorRateOfZeroToleratesNoFailure(t *testing.T) {
	mk := func(ok, fail int, max float64) Result {
		var recs []collect.Record
		for i := 0; i < ok; i++ {
			recs = append(recs, rec(i, 1, 1.2, false, 0))
		}
		for i := 0; i < fail; i++ {
			recs = append(recs, collect.Record{Seq: 100 + i, Intended: sec(1), Started: sec(1), Done: sec(1.1), Status: 503})
		}
		return Build(Input{RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{End: sec(5)}, Records: recs, MaxErrorRate: max})
	}
	if r := mk(99, 1, 0); r.Valid {
		t.Fatal("zero tolerance: one failure in 100 makes the run invalid")
	}
	if r := mk(100, 0, 0); !r.Valid {
		t.Fatal("zero tolerance: no failure is valid")
	}
}

func TestFormatDurationUsesMillisecondsBelowTenSeconds(t *testing.T) {
	for in, want := range map[time.Duration]string{
		100 * time.Millisecond: "100 ms", 1700 * time.Millisecond: "1700 ms", 9999 * time.Millisecond: "9999 ms",
		10 * time.Second: "10.0 s", 90 * time.Second: "90.0 s", 0: "0 ms",
	} {
		if got := FormatDuration(in); got != want {
			t.Errorf("%v: %q, want %q", in, got, want)
		}
	}
	r := Build(Input{RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{End: 100 * time.Millisecond}, Records: []collect.Record{rec(0, 0.01, 0.05, false, 0)}})
	if md := Markdown(r); !strings.Contains(md, "100 ms measurement window") || strings.Contains(md, "0s measurement") {
		t.Fatalf("%s", md)
	}
}

func TestATruncatedWindowIsWarnedAboutAndALateStartIsInvalid(t *testing.T) {
	recs := []collect.Record{rec(0, 0.5, 1.1, false, 0), rec(1, 1.2, 1.5, false, 0)}
	in := Input{RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{Start: sec(1), End: sec(1.7)}, Records: recs,
		Truncated: true, PlannedWindow: sec(20), MaxErrorRate: 0.05}
	r := Build(in)
	if !r.Valid || len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "cut from 20.0 s to 700 ms by --max-requests") {
		t.Fatalf("a cut window is a warning, not an invalidity: %v %v", r.Valid, r.Warnings)
	}
	md := Markdown(r)
	if !strings.Contains(md, "WARNING: the numbers in this report need care") || !strings.Contains(md, "cut from 20.0 s") {
		t.Fatalf("%s", md)
	}
	// The first request left at 5 s: the window was shortened by pre-run work and the run is invalid.
	late := Build(Input{RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{Start: sec(2), End: sec(12)}, MaxErrorRate: 0.05,
		Records: []collect.Record{{Seq: 0, Intended: sec(5), Started: sec(5), Done: sec(5.1), Status: 200}}})
	if late.Valid || !strings.Contains(strings.Join(late.InvalidReasons, " "), "first request was sent 5000 ms after the run began") {
		t.Fatalf("%v %v", late.Valid, late.InvalidReasons)
	}
	// Starting on time is fine.
	if ok := Build(Input{RunID: "run_001", Metadata: baseMeta(), Window: collect.Window{Start: sec(2), End: sec(12)}, MaxErrorRate: 0.05,
		Records: []collect.Record{{Seq: 0, Intended: 0, Started: sec(0.2), Done: sec(0.3), Status: 200}, {Seq: 1, Intended: sec(3), Started: sec(3), Done: sec(3.1), Status: 200}}}); !ok.Valid {
		t.Fatalf("%v", ok.InvalidReasons)
	}
}
