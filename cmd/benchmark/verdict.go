package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"serverflow/internal/bench/verdict"
)

// File size limits for the inputs of `benchmark verdict`.
const (
	maxPredictionsBytes = 1 << 20
	maxSmallFileBytes   = 64 << 10
)

func readRegular(path string, limit int64) ([]byte, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file (symbolic links are not followed)", path)
	}
	if st.Size() > limit {
		return nil, fmt.Errorf("%s is %d bytes, more than the %d this tool reads", path, st.Size(), limit)
	}
	return os.ReadFile(path)
}

// cmdVerdict evaluates pre-registered predictions against result files (Phase 14 D25). It prints the report
// even when some verdicts are INVALID, and then fails, so a script cannot mistake invalid evidence for a
// completed experiment.
func cmdVerdict(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("benchmark verdict", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	predictions := fs.String("predictions", "", "predictions file (Markdown with ```verdict blocks)")
	freeze := fs.String("freeze", "", "freeze record (JSON) pinning the predictions")
	runs := fs.String("runs", "", "directory holding the run_NNN result directories")
	tuned := fs.String("tuned", "", "tuned-values file (JSON), when a hypothesis needs it")
	allowUnfrozen := fs.Bool("allow-unfrozen", false, "evaluate without a freeze record (a smoke check; the output says it is not pre-registered evidence)")
	printHash := fs.Bool("print-hash", false, "print the sha256 of the frozen text of --predictions (what a freeze record pins) and exit")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w (usage: benchmark verdict --predictions FILE --freeze FILE --runs DIR [--tuned FILE] | --predictions FILE --print-hash)", err)
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("verdict takes flags only, got %q", fs.Arg(0))
	}
	if *predictions == "" {
		return errors.New("verdict needs --predictions")
	}
	pb, err := readRegular(*predictions, maxPredictionsBytes)
	if err != nil {
		return err
	}
	if *printHash {
		_, err := fmt.Fprintln(stdout, verdict.FrozenHash(pb))
		return err
	}
	if *runs == "" {
		return errors.New("verdict needs --runs")
	}
	if *freeze == "" && !*allowUnfrozen {
		return errors.New("verdict needs --freeze (or --allow-unfrozen for a smoke check)")
	}
	in := verdict.Inputs{Predictions: pb, AllowUnfrozen: *allowUnfrozen && *freeze == ""}
	if *freeze != "" {
		fb, err := readRegular(*freeze, maxSmallFileBytes)
		if err != nil {
			return err
		}
		f, err := verdict.ParseFreeze(fb)
		if err != nil {
			return err
		}
		in.Freeze = &f
	}
	if *tuned != "" {
		if in.Tuned, err = readRegular(*tuned, maxSmallFileBytes); err != nil {
			return err
		}
	}
	rs, problems, err := verdict.LoadRuns(*runs)
	if err != nil {
		return err
	}
	in.Runs = rs
	rep, err := verdict.Evaluate(in)
	if err != nil {
		return err
	}
	rep.Notes = append(rep.Notes, fmt.Sprintf("%d result files loaded", len(rs)))
	for _, p := range problems {
		rep.Notes = append(rep.Notes, "unreadable result: "+p)
	}
	if err := rep.Write(stdout); err != nil {
		return err
	}
	if n := rep.Count(verdict.Invalid); n > 0 {
		return fmt.Errorf("%d of %d verdicts are INVALID: the evidence cannot answer them (see above)", n, len(rep.Verdicts))
	}
	return nil
}
