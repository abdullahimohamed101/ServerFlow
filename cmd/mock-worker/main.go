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
	"time"

	"serverflow/internal/mockworker"
	"serverflow/internal/telemetry"
	"serverflow/internal/tracing"
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

	var opts []mockworker.Option
	var tracer *tracing.Provider
	if cfg.OTLPEndpoint != "" {
		tracer, err = tracing.Setup(tracing.Config{
			Endpoint: cfg.OTLPEndpoint, Sampler: tracing.SamplerParentOnly, QueueSize: 2048, MaxExportBatch: 512,
			BatchTimeout: 5 * time.Second, ExportTimeout: 5 * time.Second,
		}, tracing.Service{Name: "serverflow-mock-worker", Version: tracing.BuildVersion(), InstanceID: cfg.EffectiveWorkerID() + "-" + tracing.NewInstanceID()}, logger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mock-worker: %v\n", err)
			os.Exit(1)
		}
		opts = append(opts, mockworker.WithTracing(tracer))
		logger.Info("tracing enabled: spans are recorded only for requests the gateway sampled", "component", "mock-worker")
	}

	serveErr := mockworker.New(cfg, logger, opts...).Serve(ctx, ln)
	if tracer != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := tracer.Shutdown(sctx); err != nil {
			logger.Warn("trace shutdown did not finish; some spans were lost", "component", "mock-worker")
		}
		cancel()
		st := tracer.Stats()
		logger.Info("tracing stopped", "component", "mock-worker", "spans_exported", st.Exported, "spans_dropped", st.Dropped, "spans_failed", st.Failed)
	}
	if serveErr != nil {
		logger.Error("mock-worker stopped with error", "component", "mock-worker", "error", serveErr)
		os.Exit(1)
	}
	logger.Info("mock-worker stopped", "component", "mock-worker")
}
