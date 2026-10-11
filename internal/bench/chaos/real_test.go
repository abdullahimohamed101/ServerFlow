package chaos

import (
	"context"
	"strings"
	"testing"
	"time"

	"serverflow/internal/bench/report"
)

// TestRealScenarios runs every scenario once against a real embedded cluster at a small time scale, on
// whatever gateway this build has (master: no resilience feature, so the result says "absent"). It checks
// the plumbing and the evidence, not the numbers: those depend on the machine, and the validity gate (which
// may fail on a loaded machine) is not asserted, only the clauses that are about the harness itself.
func TestRealScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("boots one embedded cluster per scenario (about 2 minutes); run without -short")
	}
	for _, sc := range Scenarios() {
		t.Run(sc.Name, func(t *testing.T) {
			o := Options{Scenario: sc.Name, Scale: 0.15, Workers: 4, MockConcurrent: 8, MockTPS: 1000, MockTTFT: 20 * time.Millisecond, Scheduler: "round-robin",
				Workload: "uniform-short", Model: "qwen-7b", StreamRatio: 0.5, Seed: 1, Repeat: 1, MaxErrorRate: DefaultMaxErrorRate, MinRequests: 20,
				OutDir: t.TempDir(), AllowDirty: true}
			if err := o.Validate(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			res, recs, err := RunOnce(ctx, o, sc, RealDeps(false), "run_001", report.Repeat{Index: 1, Of: 1})
			if err != nil {
				t.Fatal(err)
			}
			ch := res.Chaos
			if ch == nil || len(ch.Windows) != 3 || len(recs) == 0 {
				t.Fatalf("no chaos result: %+v", ch)
			}
			// Since PR C the configuration section exists (default off) although the gateway does not read it yet.
			if res.Metadata.Resilience != "off" {
				t.Fatalf("resilience %q: the build has the configuration section, default off", res.Metadata.Resilience)
			}
			for _, c := range ch.Gate {
				switch c.Name {
				case "fault applied and cleared", "fault evidenced", "load driver", "no sleep or clock step", "clients within the surviving slots", "clients exceed slots (overload)":
					if !c.OK {
						t.Errorf("gate clause %q failed: %s", c.Name, c.Detail)
					}
				}
			}
			w := ch.Windows
			if w[0].Sent == 0 || w[1].Sent == 0 || w[2].Sent == 0 {
				t.Errorf("a window is empty: %d %d %d", w[0].Sent, w[1].Sent, w[2].Sent)
			}
			switch sc.Name {
			case "error-burst-unavailable", "error-burst-unavailable-partial", "conn-reset":
				// Master retries a 503 or a reset before output on another worker: the clients are spared,
				// and the retries show in the attempts.
				if w[1].Retried == 0 {
					t.Errorf("%s: nothing was retried in the fault window", sc.Name)
				}
			case "worker-freeze", "error-burst-500", "midstream", "overload":
				// Master has nothing that avoids these faults, so the clients see them.
				if w[1].Failed == 0 {
					t.Errorf("%s: no client saw the fault on master", sc.Name)
				}
			case "drain-roll":
				if ch.Evidence.Drains != 4 {
					t.Errorf("drains %d, want 4", ch.Evidence.Drains)
				}
			case "worker-crash":
				if ch.Evidence.Crashes != 1 {
					t.Errorf("crashes %d", ch.Evidence.Crashes)
				}
			}
			dir := o.OutDir + "/run_001"
			if err := writeAndReload(dir, res); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(report.Markdown(res), "## Chaos") {
				t.Error("the report has no chaos section")
			}
		})
	}
}

func writeAndReload(dir string, res report.Result) error {
	if err := mkdir(dir); err != nil {
		return err
	}
	if err := report.Write(dir, res, nil); err != nil {
		return err
	}
	got, err := report.Load(dir)
	if err != nil {
		return err
	}
	if got.Chaos == nil || got.Metadata.Resilience != res.Metadata.Resilience {
		return errReload
	}
	return nil
}
