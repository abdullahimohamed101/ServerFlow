package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Usage (scripts/k8s-up.sh roll-test is the caller):
//
//	e2e run --key-file F [--url U] [--clients 24] [--log FILE] [--json FILE] [--min-rollout 3000] [--expect none|failures] < control
//	e2e pods FILE
//
// run starts the clients at once, in the phase "baseline". It reads commands from standard input, one per line:
// "mark NAME" starts the phase NAME (names of rollouts start with "roll"), "stop" (or end of input, or SIGINT/SIGTERM)
// stops sending, waits for the requests in flight, prints the report and exits 0 when the verdict is PASS, 1 otherwise.
// pods prints the SIGTERM-to-gone time of the gateway pods from the poller's observation file.
func main() { os.Exit(realMain(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func realMain(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "usage: e2e run --key-file FILE [flags] | e2e pods FILE")
		return 2
	}
	switch args[0] {
	case "run":
		return runCmd(args[1:], stdin, stdout, stderr)
	case "pods":
		if len(args) != 2 {
			_, _ = fmt.Fprintln(stderr, "usage: e2e pods FILE")
			return 2
		}
		f, err := os.Open(args[1])
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "e2e:", err)
			return 1
		}
		defer func() { _ = f.Close() }()
		pods, err := ParsePodObservations(f)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "e2e:", err)
			return 1
		}
		WritePodTimings(stdout, pods)
		return 0
	}
	_, _ = fmt.Fprintf(stderr, "e2e: unknown command %q\n", args[0])
	return 2
}

// Config is a load run.
type Config struct {
	URL            string
	Key            string
	Clients        int
	Timeout        time.Duration
	FailurePause   time.Duration
	MinRollout     int
	ExpectFailures bool
	Log            io.Writer // one line per request, nil for none
	Status         string    // a file rewritten twice a second with the running counts (see writeStatus), "" for none
}

var phaseName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

func runCmd(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	target := fs.String("url", "http://127.0.0.1:28080", "gateway base URL (loopback only)")
	keyFile := fs.String("key-file", "", "file holding the API key (never printed)")
	clients := fs.Int("clients", 24, "concurrent closed-loop clients")
	timeout := fs.Duration("timeout", 60*time.Second, "per-request timeout")
	minRollout := fs.Int("min-rollout", 3000, "minimum requests during the rollout phases")
	expect := fs.String("expect", "none", "none: pass needs zero failures; failures: the negative control, pass needs at least one")
	logFile := fs.String("log", "", "write one line per request (unix ms, client, stream, keep-alive, status, class, latency ms) to this file")
	statusFile := fs.String("status", "", "rewrite this file twice a second with requests=, rollout_requests= and drain_failures= (the caller decides when to stop rolling)")
	jsonFile := fs.String("json", "", "write the summary as JSON to this file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *expect != "none" && *expect != "failures" {
		_, _ = fmt.Fprintln(stderr, "e2e: --expect must be none or failures")
		return 2
	}
	if *clients < 2 || *clients > 256 {
		_, _ = fmt.Fprintln(stderr, "e2e: --clients must be 2-256")
		return 2
	}
	if err := checkLoopback(*target); err != nil {
		_, _ = fmt.Fprintln(stderr, "e2e:", err)
		return 2
	}
	kb, err := os.ReadFile(*keyFile)
	key := strings.TrimSpace(string(kb))
	if err != nil || key == "" {
		_, _ = fmt.Fprintln(stderr, "e2e: cannot read an API key from --key-file")
		return 2
	}
	cfg := Config{URL: *target, Key: key, Clients: *clients, Timeout: *timeout, FailurePause: 50 * time.Millisecond, MinRollout: *minRollout, ExpectFailures: *expect == "failures", Status: *statusFile}
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "e2e:", err)
			return 2
		}
		defer func() { _ = f.Close() }()
		cfg.Log = f
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	s := Run(ctx, cfg, stdin)
	WriteText(stdout, s, cfg.MinRollout, cfg.ExpectFailures)
	if *jsonFile != "" {
		b, _ := json.MarshalIndent(s, "", "  ")
		if err := os.WriteFile(*jsonFile, append(b, '\n'), 0o600); err != nil {
			_, _ = fmt.Fprintln(stderr, "e2e:", err)
			return 1
		}
	}
	if pass, _ := Verdict(s, cfg.MinRollout, cfg.ExpectFailures); !pass {
		return 1
	}
	return 0
}

// checkLoopback: the load goes to the kind entry point on this machine and nowhere else, whatever the arguments say.
func checkLoopback(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Hostname() == "" {
		return fmt.Errorf("--url must be an http URL")
	}
	h := u.Hostname()
	if ip := net.ParseIP(h); h != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("--url must point at a loopback address, not %q", h)
	}
	return nil
}

// Run drives the clients until control says stop, then returns the summary. Control lines: "mark NAME", "stop".
func Run(ctx context.Context, cfg Config, control io.Reader) Summary {
	var (
		mu       sync.Mutex
		outcomes []Outcome
		phases   = []Phase{{Name: "baseline", Start: time.Now()}}
	)
	stopCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		sc := bufio.NewScanner(control)
		for sc.Scan() {
			cmd, arg, _ := strings.Cut(strings.TrimSpace(sc.Text()), " ")
			switch {
			case cmd == "stop":
				stop()
				return
			case cmd == "mark" && phaseName.MatchString(arg):
				mu.Lock()
				phases = append(phases, Phase{Name: arg, Start: time.Now()})
				mu.Unlock()
			}
		}
		stop() // the controller went away: do not keep loading the cluster
	}()

	if cfg.Status != "" {
		go func() {
			t := time.NewTicker(500 * time.Millisecond)
			defer t.Stop()
			for {
				mu.Lock()
				snap := Summarize(phases, outcomes, time.Now())
				mu.Unlock()
				writeStatus(cfg.Status, snap)
				select {
				case <-stopCtx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			keepAlive := id%2 == 0
			client := newClient(keepAlive)
			defer client.CloseIdleConnections()
			for n := id; stopCtx.Err() == nil; n++ {
				// Half of each group streams; the other half does not; every client alternates, so each mixes both.
				stream := (n/2)%2 == 0
				start := time.Now()
				// The request itself is not tied to stopCtx: a request in flight at the stop is finished and counted.
				status, class := doRequest(context.Background(), client, cfg.URL, cfg.Key, stream, cfg.Timeout)
				o := Outcome{Client: id, Stream: stream, KeepAlive: keepAlive, Start: start, End: time.Now(), Status: status, Class: class}
				mu.Lock()
				outcomes = append(outcomes, o)
				if cfg.Log != nil {
					_, _ = fmt.Fprintf(cfg.Log, "%d,%d,%t,%t,%d,%s,%d\n", o.Start.UnixMilli(), o.Client, o.Stream, o.KeepAlive, o.Status, o.Class, o.End.Sub(o.Start).Milliseconds())
				}
				mu.Unlock()
				if !o.OK() && cfg.FailurePause > 0 {
					// A closed-loop client that was refused does not spin: it pauses, then asks again (a new request, not a retry).
					select {
					case <-stopCtx.Done():
					case <-time.After(cfg.FailurePause):
					}
				}
			}
		}(i)
	}
	wg.Wait()
	end := time.Now()
	mu.Lock()
	defer mu.Unlock()
	return Summarize(phases, outcomes, end)
}

// writeStatus replaces the file atomically, so a reader never sees half a line.
func writeStatus(path string, s Summary) {
	tmp := path + ".tmp"
	line := fmt.Sprintf("requests=%d rollout_requests=%d rollout_failures=%d drain_failures=%d rollout_drain_failures=%d\n",
		s.Requests, s.RolloutRequests, s.RolloutFailures, s.DrainFailures, s.RolloutDrainFailures)
	if os.WriteFile(tmp, []byte(line), 0o600) == nil {
		_ = os.Rename(tmp, path)
	}
}
