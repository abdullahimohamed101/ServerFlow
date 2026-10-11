package chaos

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/bench/embedded"
	"serverflow/internal/mockworker"
	"serverflow/internal/registry/client"
)

// FaultHandle is what a scenario does to one mock worker; *mockworker.Faults implements it.
type FaultHandle interface {
	SetErrorRate(rate float64, mode mockworker.FailureMode) error
	SetExtraDelay(d time.Duration)
	SetBlackhole(on bool)
	Crash() error
	Restart() error
	Clear()
	Stats() mockworker.FaultStats
}

// Cluster is the system under test as the harness uses it. *embedded.Cluster, through Adapt, is the
// real one; tests substitute a fake.
type Cluster interface {
	GatewayURL() string
	Specs() []embedded.WorkerSpec
	// Faults returns the fault handle of a worker, nil when unknown.
	Faults(id string) FaultHandle
	DrainWorker(ctx context.Context, id string) error
	ReplaceWorker(ctx context.Context, id string) error
	ServedWhileAnnouncing(id string) int64
	// WorkerState is the control plane's state of a worker, or "absent" when it lists none.
	WorkerState(ctx context.Context, id string) (string, error)
	Metrics() prometheus.Gatherer
	ControlPlane() *client.Client // nil when there is none (a fake)
	Settled() time.Duration
	Resilience() string
	Close()
}

// Adapt wraps an embedded cluster.
func Adapt(c *embedded.Cluster) Cluster { return adapter{c} }

type adapter struct{ c *embedded.Cluster }

func (a adapter) GatewayURL() string                               { return a.c.GatewayURL }
func (a adapter) Specs() []embedded.WorkerSpec                     { return a.c.Specs() }
func (a adapter) DrainWorker(ctx context.Context, id string) error { return a.c.DrainWorker(ctx, id) }
func (a adapter) ReplaceWorker(ctx context.Context, id string) error {
	return a.c.ReplaceWorker(ctx, id)
}
func (a adapter) ServedWhileAnnouncing(id string) int64 { return a.c.ServedWhileAnnouncing(id) }
func (a adapter) Metrics() prometheus.Gatherer          { return a.c.Metrics() }
func (a adapter) ControlPlane() *client.Client          { return a.c.ControlPlane() }
func (a adapter) Settled() time.Duration                { return a.c.Settled() }
func (a adapter) Resilience() string                    { return a.c.Resilience() }
func (a adapter) Close()                                { a.c.Close() }

// Faults avoids returning a typed nil inside the interface.
func (a adapter) Faults(id string) FaultHandle {
	if f := a.c.Faults(id); f != nil {
		return f
	}
	return nil
}

func (a adapter) WorkerState(ctx context.Context, id string) (string, error) {
	w, err := a.c.ControlPlane().Worker(ctx, id)
	if err != nil {
		if client.IsUnknownWorker(err) {
			return "absent", nil
		}
		return "absent", err
	}
	return string(w.State), nil
}
