package runner

import (
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

func parse(t *testing.T, args ...string) (RunFlags, error) {
	t.Helper()
	return ParseRun(args)
}

func TestDefaultsGiveAValidClosedLoopEmbeddedRun(t *testing.T) {
	rf, err := parse(t)
	if err != nil {
		t.Fatal(err)
	}
	o := rf.Options
	if !o.Embedded() || o.Concurrency != DefaultConcurrency || o.Rate != 0 || o.Mode() != "closed" || o.Workers != 4 ||
		o.Scheduler != "round-robin" || o.Workload != "mixed" || o.Seed != 1 || o.Warmup != 5*time.Second || o.Repeat != 1 {
		t.Fatalf("%+v", o)
	}
}

func TestTheSpecCommandParses(t *testing.T) {
	rf, err := parse(t, "--scheduler", "round-robin", "--workers", "4", "--mock-concurrency", "32", "--concurrency", "100", "--duration", "300s", "--workload", "mixed")
	if err != nil {
		t.Fatal(err)
	}
	if o := rf.Options; o.Concurrency != 100 || o.Duration != 300*time.Second {
		t.Fatalf("%+v", o)
	}
}

func TestOpenLoopOptionsParseAndDefaultTheInFlightLimit(t *testing.T) {
	rf, err := parse(t, "--rate", "50", "--workload", "burst", "--duration", "30s")
	if err != nil {
		t.Fatal(err)
	}
	if o := rf.Options; o.Mode() != "open" || o.Concurrency != 0 || o.MaxInFlight != DefaultMaxConcurrency {
		t.Fatalf("%+v", o)
	}
}

func TestUnsafeOrInconsistentInputIsRefusedWithAClearError(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"--bogus"}, "bogus"},
		{"positional", []string{"extra"}, "unexpected argument"},
		{"unknown workload", []string{"--workload", "nope"}, "uniform-short"},
		{"unknown scheduler", []string{"--scheduler", "magic"}, "known strategy"},
		{"scheduler injection", []string{"--scheduler", "a;b"}, "not a valid name"},
		{"both loads", []string{"--concurrency", "5", "--rate", "5"}, "mutually exclusive"},
		{"burst closed", []string{"--workload", "burst", "--concurrency", "5"}, "open-loop"},
		{"burst without load", []string{"--workload", "burst"}, "open-loop"},
		{"concurrency cap", []string{"--concurrency", "2001"}, "cap"},
		{"rate cap", []string{"--rate", "10001"}, "cap"},
		{"burst peak cap", []string{"--rate", "1500", "--workload", "burst"}, "peak rate"},
		{"negative concurrency", []string{"--concurrency", "-1"}, "negative"},
		{"negative rate", []string{"--rate", "-1"}, "negative"},
		{"NaN rate", []string{"--rate", "NaN"}, "negative"},
		{"zero duration", []string{"--duration", "0s"}, "duration"},
		{"huge duration", []string{"--duration", "3h"}, "duration"},
		{"negative warm-up", []string{"--warmup", "-1s"}, "warmup"},
		{"zero workers", []string{"--workers", "0"}, "workers"},
		{"too many workers", []string{"--workers", "65"}, "workers"},
		{"bad profile", []string{"--worker-profile", "odd"}, "worker-profile"},
		{"bad stream ratio", []string{"--stream-ratio", "2"}, "stream ratio"},
		{"repeat zero", []string{"--repeat", "0"}, "repeat"},
		{"repeat many", []string{"--repeat", "21"}, "repeat"},
		{"bad model", []string{"--model", "has space"}, "model"},
		{"one worker two models", []string{"--workload", "hot-model", "--workers", "1"}, "workers"},
		{"more clients than slots", []string{"--workers", "4", "--worker-profile", "heterogeneous", "--concurrency", "100"}, "request slots"},
		{"slots named", []string{"--workers", "3", "--concurrency", "25"}, "24 request slots"},
		{"too short", []string{"--duration", "1ms"}, "duration"},
		{"error rate range", []string{"--max-error-rate", "2"}, "max-error-rate"},
		{"mock tps", []string{"--mock-tps", "0"}, "must all be positive"},
		{"sample interval", []string{"--sample-interval", "1ms"}, "sample-interval"},
		{"too many planned requests", []string{"--rate", "1000", "--duration", "10m", "--max-requests", "1000"}, "max-requests"},
		{"inflight over cap", []string{"--rate", "10", "--max-inflight", "5000"}, "max-inflight"},
		{"embedded and target", []string{"--embedded", "--target", "http://127.0.0.1:1"}, "mutually exclusive"},
		{"control plane without target", []string{"--control-plane", "http://127.0.0.1:9090"}, "--target only"},
		{"remote target", []string{"--target", "http://gateway.example.com:8080"}, "--allow-remote"},
		{"remote private ip", []string{"--target", "http://10.1.2.3:8080"}, "--allow-remote"},
		{"remote control plane", []string{"--target", "http://127.0.0.1:8080", "--control-plane", "http://cp.example.com"}, "--allow-remote"},
		{"credentials in url", []string{"--target", "http://user:pw@127.0.0.1:8080"}, "credentials"},
		{"not a url", []string{"--target", "gateway:8080"}, "http://"},
		{"ftp", []string{"--target", "ftp://127.0.0.1"}, "http://"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parse(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestCredentialsInATargetURLNeverAppearInTheError(t *testing.T) {
	_, err := parse(t, "--target", "http://admin:s3cr3t@127.0.0.1:8080")
	if err == nil || strings.Contains(err.Error(), "s3cr3t") || strings.Contains(err.Error(), "admin") {
		t.Fatalf("%v", err)
	}
}

func TestLoopbackAndExplicitlyAllowedTargetsPass(t *testing.T) {
	for _, args := range [][]string{
		{"--target", "http://127.0.0.1:8080"},
		{"--target", "http://localhost:8080", "--control-plane", "http://[::1]:9090"},
		{"--target", "https://gateway.example.com", "--allow-remote", "--workers", "0"},
	} {
		if _, err := parse(t, args...); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}
}

func TestCapsCanBeRaisedOnPurpose(t *testing.T) {
	if _, err := parse(t, "--concurrency", "5000", "--max-concurrency", "5000", "--mock-concurrency", "1024", "--workers", "8"); err != nil {
		t.Fatal(err)
	}
	if _, err := parse(t, "--rate", "20000", "--max-rate", "30000", "--max-requests", "100000000", "--duration", "10s"); err != nil {
		t.Fatal(err)
	}
}

func TestHelpIsNotAFailure(t *testing.T) {
	_, err := parse(t, "-h")
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(err.Error(), "-concurrency") {
		t.Fatalf("%v", err)
	}
}

func TestNoSecretFlagExists(t *testing.T) {
	_, err := parse(t, "-h")
	for _, bad := range []string{"-token", "-api-key", "-password"} {
		if strings.Contains(err.Error(), bad) {
			t.Errorf("a secret must not be a flag (it would show in the process list): %s", bad)
		}
	}
}

func TestOverloadIsAllowedOnPurposeAndTheDefaultNeverOverloads(t *testing.T) {
	if _, err := parse(t, "--workers", "4", "--concurrency", "100", "--allow-overload"); err != nil {
		t.Fatal(err)
	}
	if _, err := parse(t, "--workers", "4", "--concurrency", "32"); err != nil {
		t.Fatalf("exactly the slots is allowed: %v", err)
	}
	rf, err := parse(t, "--workers", "1")
	if err != nil || rf.Options.Concurrency != 8 {
		t.Fatalf("the default shrinks to the 8 slots of one worker: %+v %v", rf.Options.Concurrency, err)
	}
	// An open loop has no client count to overload.
	if _, err := parse(t, "--rate", "500", "--workers", "1"); err != nil {
		t.Fatal(err)
	}
}

func TestErrorRateDefaultsAndFlags(t *testing.T) {
	rf, err := parse(t)
	if err != nil || rf.Options.MaxErrorRate != 0.05 || rf.Options.AllowErrors {
		t.Fatalf("%+v %v", rf.Options, err)
	}
	if rf, err := parse(t, "--max-error-rate", "0.2", "--allow-errors"); err != nil || rf.Options.MaxErrorRate != 0.2 || !rf.Options.AllowErrors {
		t.Fatalf("%+v %v", rf.Options, err)
	}
}
