package main

import (
	"context"
	"log/slog"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
)

// shutdownTracingTimeout bounds the final span flush. A dead collector can delay exit by this long.
const shutdownTracingTimeout = 5 * time.Second

// setupTracing builds the tracing provider and the gateway options that use it when tracing.enabled. With
// tracing off it returns nothing and no OpenTelemetry code runs. Starting with the collector down is not an
// error: spans are queued, dropped when the queue is full, and counted.
func setupTracing(cfg config.Config, logger *slog.Logger) (*tracing.Provider, []gateway.Option, error) {
	t := cfg.Tracing
	if !t.Enabled {
		return nil, nil, nil
	}
	mode := tracing.SamplerRatio
	if t.Incoming == config.TraceIncomingTrust {
		mode = tracing.SamplerParentRatio
	}
	p, err := tracing.Setup(tracing.Config{
		Endpoint: t.Endpoint, Sampler: mode, SampleRatio: t.SampleRatio, QueueSize: t.QueueSize, MaxExportBatch: t.MaxExportBatch,
		BatchTimeout: t.BatchTimeout, ExportTimeout: t.ExportTimeout,
	}, tracing.Service{Name: "serverflow-gateway", Version: tracing.BuildVersion(), InstanceID: tracing.NewInstanceID()}, logger)
	if err != nil {
		return nil, nil, err
	}
	obs := gwtrace.New(p, gwtrace.Options{Incoming: t.Incoming, IncludeTenantID: t.IncludeTenantID})
	stats := func() gateway.TracingStats {
		s := p.Stats()
		return gateway.TracingStats{Exported: s.Exported, Dropped: s.Dropped, FailedSpans: s.Failed, Failures: s.Failures}
	}
	logger.Info("tracing enabled", "component", "gateway", "sample_ratio", t.SampleRatio, "incoming", t.Incoming,
		"include_tenant_id", t.IncludeTenantID, "queue_size", t.QueueSize)
	if t.Incoming == config.TraceIncomingTrust {
		logger.Warn("tracing.incoming=trust: clients can force sampling and graft spans into traces of their choosing; use it only behind a trusted proxy", "component", "gateway")
	}
	return p, []gateway.Option{gateway.WithObserver(obs), gateway.WithTracingStats(stats)}, nil
}

// shutdownTracing flushes and stops the pipeline after the server stopped. A nil provider (tracing off) is fine.
func shutdownTracing(p *tracing.Provider, logger *slog.Logger) {
	if p == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTracingTimeout)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		logger.Warn("trace shutdown did not finish; some spans were lost", "component", "gateway")
	}
	st := p.Stats()
	logger.Info("tracing stopped", "component", "gateway", "spans_exported", st.Exported, "spans_dropped", st.Dropped, "spans_failed", st.Failed)
}
