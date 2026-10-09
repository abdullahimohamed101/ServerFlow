// Command benchmark is the scheduler benchmark harness: it drives a reproducible load
// test against ServerFlow, persists the result, and compares two results.
//
//	benchmark run --scheduler round-robin --workers 4 --concurrency 100 --duration 60s --workload mixed --seed 1
//	benchmark compare run_001 run_002
//	benchmark list
//
// See docs/benchmarks/phase-7-harness.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"serverflow/internal/bench/report"
	"serverflow/internal/bench/runner"
	"serverflow/internal/config"
)

const usage = `benchmark: scheduler benchmark harness

Usage:
  benchmark run [flags]             run a load test and write benchmark/runs/run_NNN
  benchmark compare [--dir D] A B   print the deltas between two runs (IDs or paths)
  benchmark list [--dir D]          list past runs
  benchmark run -h                  all flags of run

Without --target, run boots a simulated cluster (control plane, mock workers, gateway) in
this process. Choose a load with --concurrency (closed loop) or --rate (open loop).`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := execute(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "run":
		err = cmdRun(ctx, args[1:], stdout)
	case "compare":
		err = cmdCompare(args[1:], stdout)
	case "list":
		err = cmdList(args[1:], stdout)
	case "help", "-h", "--help":
		_, _ = fmt.Fprintln(stdout, usage)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "benchmark: unknown command %q\n\n%s\n", args[0], usage)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		_, _ = fmt.Fprintln(stderr, err)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "benchmark: %v\n", err)
		return 1
	}
}

func cmdRun(ctx context.Context, args []string, stdout io.Writer) error {
	rf, err := runner.ParseRun(args)
	if err != nil {
		return err
	}
	if rf.Options.ControlPlaneURL != "" {
		cfg, err := config.Load(rf.ConfigPath)
		if err != nil {
			return err
		}
		rf.Options.ControlPlaneToken = cfg.ControlPlane.Token
	}
	_, err = runner.Run(ctx, rf.Options, stdout)
	return err
}

func dirFlag(name string, args []string) (dir string, rest []string, err error) {
	fs := flag.NewFlagSet("benchmark "+name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&dir, "dir", "benchmark/runs", "directory holding the runs")
	if err := fs.Parse(args); err != nil {
		return "", nil, fmt.Errorf("%w (usage: benchmark %s [--dir DIR]%s)", err, name, map[string]string{"compare": " RUN_A RUN_B"}[name])
	}
	return dir, fs.Args(), nil
}

func cmdList(args []string, stdout io.Writer) error {
	dir, rest, err := dirFlag("list", args)
	if err != nil {
		return err
	}
	if len(rest) > 0 {
		return fmt.Errorf("list takes no arguments, got %q", rest[0])
	}
	return report.List(stdout, dir)
}

func cmdCompare(args []string, stdout io.Writer) error {
	dir, rest, err := dirFlag("compare", args)
	if err != nil {
		return err
	}
	if len(rest) != 2 {
		return errors.New("compare needs exactly two runs: benchmark compare [--dir DIR] RUN_A RUN_B (flags before the run IDs)")
	}
	var results [2]report.Result
	var groups [2][]report.Result
	for i, ref := range rest {
		p, err := report.Resolve(dir, ref)
		if err != nil {
			return err
		}
		if results[i], err = report.Load(p); err != nil {
			return err
		}
		base := dir
		if st, serr := os.Stat(p); serr == nil && !st.IsDir() {
			p = filepath.Dir(p)
		}
		if _, ok := report.ParseRunID(filepath.Base(p)); ok {
			base = filepath.Dir(p)
		}
		if groups[i], err = report.LoadGroup(base, results[i]); err != nil {
			return err
		}
	}
	return report.CompareGroups(results[0], results[1], groups[0], groups[1]).Write(stdout)
}
