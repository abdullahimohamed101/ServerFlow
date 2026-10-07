// Command mock-worker runs a configurable fake inference worker: an
// OpenAI-compatible server with controllable latency, throughput, queueing,
// and failures, standing in for vLLM when no GPU is available. SIGINT or
// SIGTERM drains it gracefully.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"serverflow/internal/mockworker"
	"serverflow/internal/telemetry"
)

func main() {
	cfg, logLevel, err := mockworker.ParseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(os.Stderr, "mock-worker: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	logger, err := telemetry.NewDefaultLogger(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mock-worker: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal start the drain, and give signals back to the
	// default handler so a second Ctrl-C force-quits instead of being swallowed.
	go func() { <-ctx.Done(); stop() }()

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mock-worker: %v\n", err)
		os.Exit(1)
	}
	cfg.Addr = ln.Addr().String() // the real address, so ":0" still yields a unique worker ID
	logger.Info("mock-worker starting", "component", "mock-worker", "worker_id", cfg.EffectiveWorkerID(),
		"addr", ln.Addr().String(), "model", cfg.Model, "ttft", cfg.TTFT.String(), "tokens_per_second", cfg.TokensPerSecond,
		"output_tokens", cfg.OutputTokens, "max_concurrency", cfg.MaxConcurrency, "queue_size", cfg.QueueSize,
		"failure_rate", cfg.FailureRate, "failure_mode", string(cfg.FailureMode), "seed", cfg.Seed,
		"startup_delay", cfg.StartupDelay.String())

	if err := mockworker.New(cfg, logger).Serve(ctx, ln); err != nil {
		logger.Error("mock-worker stopped with error", "component", "mock-worker", "error", err)
		os.Exit(1)
	}
	logger.Info("mock-worker stopped", "component", "mock-worker")
}
