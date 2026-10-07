package mockworker

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseFlagsSpecExample(t *testing.T) {
	// The exact example from spec section 53, plus a port.
	cfg, level, err := ParseFlags([]string{"--ttft=200ms", "--tokens-per-second=50", "--failure-rate=.01", "--model=qwen", "--addr=:9001"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TTFT != 200*time.Millisecond || cfg.TokensPerSecond != 50 || cfg.FailureRate != 0.01 || cfg.Model != "qwen" || cfg.Addr != ":9001" {
		t.Fatalf("unexpected config %+v", cfg)
	}
	if level != "info" {
		t.Fatalf("default log level %q", level)
	}
}

func TestParseFlagsDefaultsAndAllFlags(t *testing.T) {
	cfg, _, err := ParseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	def := DefaultConfig()
	cfg.Seed = 0
	if cfg != def {
		t.Fatalf("no flags must give the defaults:\n got %+v\nwant %+v", cfg, def)
	}

	cfg, level, err := ParseFlags([]string{
		"--model=m", "--addr=127.0.0.1:1", "--worker-id=w1", "--ttft=1s", "--tokens-per-second=7.5", "--output-tokens=9",
		"--max-concurrency=3", "--queue-size=0", "--failure-rate=1", "--failure-mode=midstream", "--seed=42",
		"--startup-delay=2s", "--drain-timeout=3s", "--log-level=debug",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := Config{Model: "m", Addr: "127.0.0.1:1", WorkerID: "w1", TTFT: time.Second, TokensPerSecond: 7.5, OutputTokens: 9,
		MaxConcurrency: 3, QueueSize: 0, FailureRate: 1, FailureMode: ModeMidstream, Seed: 42,
		StartupDelay: 2 * time.Second, DrainTimeout: 3 * time.Second}
	if cfg != want || level != "debug" {
		t.Fatalf("got %+v (%s), want %+v", cfg, level, want)
	}
}

func TestParseFlagsSeed(t *testing.T) {
	a, _, _ := ParseFlags(nil, io.Discard)
	time.Sleep(time.Millisecond)
	b, _, _ := ParseFlags(nil, io.Discard)
	if a.Seed == 0 || a.Seed == b.Seed {
		t.Fatalf("without --seed a fresh random seed is expected, got %d and %d", a.Seed, b.Seed)
	}
	fixed, _, _ := ParseFlags([]string{"--seed=0"}, io.Discard)
	if fixed.Seed != 0 {
		t.Fatalf("an explicit --seed=0 must be honored, got %d", fixed.Seed)
	}
}

func TestParseFlagsRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"--nope=1"}, "nope"},
		{"stray argument", []string{"extra"}, "unexpected argument"},
		{"not a number", []string{"--tokens-per-second=fast"}, "tokens-per-second"},
		{"not a duration", []string{"--ttft=soon"}, "ttft"},
		{"zero speed", []string{"--tokens-per-second=0"}, "tokens-per-second"},
		{"negative ttft", []string{"--ttft=-1s"}, "ttft"},
		{"rate above 1", []string{"--failure-rate=2"}, "failure-rate"},
		{"bad mode", []string{"--failure-mode=explode"}, "failure-mode"},
		{"zero concurrency", []string{"--max-concurrency=0"}, "max-concurrency"},
		{"empty model", []string{"--model="}, "model"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ParseFlags(tt.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want an error mentioning %q", err, tt.want)
			}
		})
	}
}
