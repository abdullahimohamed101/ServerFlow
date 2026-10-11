package chaos

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"serverflow/internal/bench/embedded"
	"serverflow/internal/config"
	"serverflow/internal/mockworker"
)

// Window lengths of a scenario, in buckets (one bucket is one second at time scale 1). A run is
// warm-up (unmeasured), baseline, fault and recovery.
const (
	warmupBuckets   = 3
	baselineBuckets = 10
	faultBuckets    = 20
	recoveryBuckets = 20

	// MinBucket is the shortest bucket the time scale may produce.
	MinBucket = 100 * time.Millisecond
	// FaultedWorker is the worker a single-worker scenario faults: the first.
	FaultedWorker = "worker-01"
)

// Params are a scenario's settings once the time scale is applied.
type Params struct {
	Scale  float64
	Bucket time.Duration

	Warmup, Baseline, Fault, Recovery time.Duration

	// HeaderTimeout is the gateway's upstream header timeout for this scenario (0: the harness default of 30 s).
	HeaderTimeout time.Duration
	// Delay is the extra delay of a slow worker.
	Delay time.Duration
	// Rate and Mode are the error burst's.
	Rate float64
	Mode mockworker.FailureMode
	// DrainAnnounce is the announce period of a drained mock worker: above the heartbeat plus the registry
	// refresh, not scaled (those are fixed in the embedded cluster).
	DrainAnnounce time.Duration
}

// Total is the length of the whole run after the warm-up.
func (p Params) Total() time.Duration { return p.Warmup + p.Baseline + p.Fault + p.Recovery }

func (p Params) buckets(n int) time.Duration { return time.Duration(n) * p.Bucket }

// newParams applies the time scale: a bucket is a second times scale (at least MinBucket), and every
// window is a fixed number of buckets, so the shape of a run does not change with the scale. The
// settings that are not time-scaled (heartbeat 1 s, registry refresh 100 ms) are what limits how far a
// scenario can be shrunk; the doc says so.
func newParams(scale float64) Params {
	b := max(time.Duration(scale*float64(time.Second)), MinBucket)
	b = b.Round(MinBucket / 2)
	p := Params{Scale: scale, Bucket: b}
	p.Warmup, p.Baseline, p.Fault, p.Recovery = p.buckets(warmupBuckets), p.buckets(baselineBuckets), p.buckets(faultBuckets), p.buckets(recoveryBuckets)
	return p
}

// LoadControl lets a scenario add load for its fault window (overload).
type LoadControl interface {
	// Start adds n closed-loop clients; Stop ends them and waits.
	Start(n int)
	Stop()
}

// Env is what a scenario's Apply and Clear work with.
type Env struct {
	C     Cluster
	P     Params
	Extra LoadControl
	// ExtraClients is how many clients an overload adds for its fault window.
	ExtraClients int

	drains, served atomic.Int64
}

// Served is the requests drained workers admitted while announcing their drain, summed over a roll.
func (e *Env) Served() int64 { return e.served.Load() }

// Drains is how many workers a drain-roll has drained so far.
func (e *Env) Drains() int { return int(e.drains.Load()) }

// Scenario is one fault and what to expect of it. Every scenario has the same two pre-registered primary
// measures: the success rate in the fault window and the attempts per request in it.
type Scenario struct {
	Name        string
	Group       string
	Variant     string
	Description string
	// CannotShow is what the embedded mock cluster cannot show for this scenario; it is printed by
	// `benchmark chaos -list` and in the doc.
	CannotShow string
	// SingleWorker is true when the fault is on the first worker only; the validity gate then counts
	// the other workers' slots as the surviving ones.
	SingleWorker bool
	// Overload inverts the client check: the fault window must offer more clients than slots.
	Overload bool
	// Rolling faults every worker in turn (drain-roll); one worker is out at a time.
	Rolling bool

	params func(workers int, scale float64) Params
	apply  func(ctx context.Context, e *Env) error
	clear  func(ctx context.Context, e *Env) error
	// evidenced says, from the mock workers' own counters, whether the fault touched anything.
	evidenced func(ev Evidence) (bool, string)
}

// Evidence is what the fault left behind, read from the mock workers and the harness.
type Evidence struct {
	Stats                 mockworker.FaultStats
	Drains                int
	ServedWhileAnnouncing int64
	ControlPlane          string
	// WindowFailures is the failed requests of the fault window, GatewayNoCapacity the 503s the
	// gateway gave for lack of a worker in it (overload evidence).
	WindowFailures    int
	GatewayNoCapacity float64
}

// Params returns the scenario's settings at a time scale for a cluster of this many workers.
func (s Scenario) Params(workers int, scale float64) Params { return s.params(workers, scale) }

// GatewayMutate returns the gateway configuration edit the scenario needs.
func (s Scenario) GatewayMutate(p Params) func(*config.Config) {
	if p.HeaderTimeout <= 0 {
		return nil
	}
	return func(c *config.Config) { c.Gateway.UpstreamHeaderTimeout = p.HeaderTimeout }
}

func scaledHeader(scale float64, base, floor time.Duration) time.Duration {
	return max(time.Duration(scale*float64(base)), floor)
}

func (s Scenario) String() string { return s.Name }

func single(name, group, variant, desc, cannot string, mut func(*Params), apply, clear func(context.Context, *Env) error, ev func(Evidence) (bool, string)) Scenario {
	return Scenario{Name: name, Group: group, Variant: variant, Description: desc, CannotShow: cannot, SingleWorker: true,
		params: func(_ int, scale float64) Params {
			p := newParams(scale)
			if mut != nil {
				mut(&p)
			}
			return p
		},
		apply: apply, clear: clear, evidenced: ev}
}

func errorBurst(name, variant, desc string, rate float64, mode mockworker.FailureMode, cannot string) Scenario {
	return single(name, "error-burst", variant, desc, cannot, func(p *Params) { p.Rate, p.Mode = rate, mode },
		func(_ context.Context, e *Env) error { return e.faults().SetErrorRate(e.P.Rate, e.P.Mode) },
		func(_ context.Context, e *Env) error { return e.faults().SetErrorRate(0, e.P.Mode) },
		func(ev Evidence) (bool, string) {
			return ev.Stats.Errored > 0, fmt.Sprintf("%d requests failed by the worker", ev.Stats.Errored)
		})
}

func (e *Env) faults() FaultHandle {
	f := e.C.Faults(FaultedWorker)
	if f == nil {
		return failHandle{}
	}
	return f
}

// failHandle is returned for an unknown worker so a scenario reports an error instead of panicking.
type failHandle struct{ FaultHandle }

func (failHandle) SetErrorRate(float64, mockworker.FailureMode) error { return errNoWorker }
func (failHandle) Crash() error                                       { return errNoWorker }
func (failHandle) Restart() error                                     { return errNoWorker }
func (failHandle) SetExtraDelay(time.Duration)                        {}
func (failHandle) SetBlackhole(bool)                                  {}
func (failHandle) Clear()                                             {}
func (failHandle) Stats() mockworker.FaultStats                       { return mockworker.FaultStats{} }

var errNoWorker = fmt.Errorf("chaos: the cluster has no worker %s to fault", FaultedWorker)

var scenarios = buildScenarios()

func buildScenarios() []Scenario {
	const noBreaker = "The breaker's detection of a persistent fault cannot be told from the registry's here, because the embedded agent reports a dead worker within one heartbeat."
	slow := func(name, variant, desc string, headerFactor float64) Scenario {
		return single(name, "slow-worker", variant, desc,
			"The delay holds the mock's slot but not a CPU: a real slow worker (a long prefill, a GPU stall) also degrades its neighbours on the same device, which a mock cannot.",
			func(p *Params) {
				p.Delay = max(time.Duration(5*p.Scale*float64(time.Second)), 1500*time.Millisecond)
				p.HeaderTimeout = time.Duration(headerFactor * float64(p.Delay))
			},
			func(_ context.Context, e *Env) error { e.faults().SetExtraDelay(e.P.Delay); return nil },
			func(_ context.Context, e *Env) error { e.faults().SetExtraDelay(0); return nil },
			func(ev Evidence) (bool, string) {
				return ev.Stats.Delayed > 0, fmt.Sprintf("%d requests delayed", ev.Stats.Delayed)
			})
	}
	list := []Scenario{
		single("worker-crash", "worker-crash", "",
			"The first worker's listener closes and its connections are reset; it listens again when the fault ends. The agent reports FAILED within one heartbeat and the registry drops the worker within a refresh, so this is a registry-plus-retry baseline, not a breaker test.",
			"Whether the circuit breaker detects a crash: the registry gets there first. It shows that a crash costs the clients nothing the retry cannot absorb, and that nothing regresses. A real process kill (SIGKILL) differs in that the OS resets connections the same way, but also frees the port for a replacement.",
			nil,
			func(_ context.Context, e *Env) error { return e.faults().Crash() },
			func(_ context.Context, e *Env) error { return e.faults().Restart() },
			func(ev Evidence) (bool, string) {
				return ev.Stats.Crashes > 0, fmt.Sprintf("%d crashes, control plane saw the worker as %s", ev.Stats.Crashes, ev.ControlPlane)
			}),
		single("worker-freeze", "worker-freeze", "",
			"The first worker reads requests and never answers (a hung process); its /stats keeps answering, so the agent says READY and only the gateway's own header timeout (lowered to 2 s) can detect it.",
			"A frozen process stops its heartbeat too, which the registry notices after the suspect threshold (10 s here); the mock keeps answering /stats so that the breaker is the only detector. SIGSTOP of a real worker is the process-level version (not run).",
			func(p *Params) { p.HeaderTimeout = scaledHeader(p.Scale, 2*time.Second, 500*time.Millisecond) },
			func(_ context.Context, e *Env) error { e.faults().SetBlackhole(true); return nil },
			func(_ context.Context, e *Env) error { e.faults().SetBlackhole(false); return nil },
			func(ev Evidence) (bool, string) {
				return ev.Stats.Blackholed > 0, fmt.Sprintf("%d requests held without an answer", ev.Stats.Blackholed)
			}),
		slow("slow-worker-below-timeout", "header-timeout-below-delay",
			"The first worker takes an extra 5 s before it answers and the gateway's header timeout (2 s) is below that: its requests time out, which is a fault.", 0.4),
		slow("slow-worker-above-timeout", "header-timeout-above-delay",
			"The first worker takes an extra 5 s before it answers and the header timeout (8 s) is above that: its requests are slow but succeed, so a breaker must not open (the false-positive check).", 1.6),
		errorBurst("error-burst-unavailable", "unavailable-100",
			"Every request to the first worker is answered 503 mock_failure (a fault, retried on another worker).", 1, mockworker.ModeUnavailable, noBreaker),
		errorBurst("error-burst-unavailable-partial", "unavailable-50",
			"Half of the requests to the first worker are answered 503 mock_failure.", 0.5, mockworker.ModeUnavailable, noBreaker),
		errorBurst("error-burst-500", "error-100",
			"Every request to the first worker is answered 500 mock_failure. A 500 is not retried today; the clients see it.", 1, mockworker.ModeError,
			"Whether a 500 would be ejected by a ratio rule (Phase 15 D4): on master nothing reacts to it, which is the baseline."),
		single("midstream", "midstream", "",
			"Half of the requests to the first worker start their response and then break the connection after two tokens.",
			"Midstream failures cannot be retried by anyone once output has begun; this shows how many clients see one and that the worker keeps being chosen.",
			func(p *Params) { p.Rate, p.Mode = 0.5, mockworker.ModeMidstream },
			func(_ context.Context, e *Env) error { return e.faults().SetErrorRate(e.P.Rate, e.P.Mode) },
			func(_ context.Context, e *Env) error { return e.faults().SetErrorRate(0, e.P.Mode) },
			func(ev Evidence) (bool, string) {
				return ev.Stats.Errored > 0, fmt.Sprintf("%d requests broken midstream", ev.Stats.Errored)
			}),
		single("conn-reset", "conn-reset", "",
			"Every request to the first worker has its connection closed without a response; the gateway sees a reset before any byte and may retry.",
			noBreaker,
			func(p *Params) { p.Rate, p.Mode = 1, mockworker.ModeDrop },
			func(_ context.Context, e *Env) error { return e.faults().SetErrorRate(e.P.Rate, e.P.Mode) },
			func(_ context.Context, e *Env) error { return e.faults().SetErrorRate(0, e.P.Mode) },
			func(ev Evidence) (bool, string) {
				return ev.Stats.Errored > 0, fmt.Sprintf("%d connections dropped", ev.Stats.Errored)
			}),
	}
	list = append(list, Scenario{
		Name: "drain-roll", Group: "drain-roll", Rolling: true, SingleWorker: true,
		Description: "Every worker is replaced in turn, one at a time: the mock announces draining and keeps serving for a period above the heartbeat plus the registry refresh, then refuses and finishes what is in flight; the agent deregisters; a fresh worker takes its place.",
		CannotShow:  "Process signals: the harness calls the same drain path a SIGTERM takes (mock drain and agent.Shutdown) in process. It tells the registry at once instead of through the next probe, so the announce period only has to cover the gateway's refresh. A real preStop is Phase 17.",
		params: func(workers int, scale float64) Params {
			p := newParams(scale)
			p.DrainAnnounce = embedded.HeartbeatInterval + embedded.RegistryRefresh + 400*time.Millisecond
			// One roll step takes the announce period, the drain and the replacement's start (about 3 s):
			// the fault window must hold a step per worker whatever the scale.
			need := time.Duration(workers) * 4 * time.Second
			p.Fault = max(p.Fault, (need/p.Bucket+1)*p.Bucket)
			return p
		},
		apply: func(ctx context.Context, e *Env) error {
			for _, w := range e.C.Specs() {
				if err := e.C.DrainWorker(ctx, w.ID); err != nil {
					return fmt.Errorf("draining %s: %w", w.ID, err)
				}
				e.drains.Add(1)
				e.served.Add(e.C.ServedWhileAnnouncing(w.ID)) // before the replacement hides the drained mock
				if err := e.C.ReplaceWorker(ctx, w.ID); err != nil {
					return fmt.Errorf("replacing %s: %w", w.ID, err)
				}
			}
			return nil
		},
		clear: func(context.Context, *Env) error { return nil },
		evidenced: func(ev Evidence) (bool, string) {
			return ev.Drains > 0, fmt.Sprintf("%d workers drained and replaced, %d requests served while announcing", ev.Drains, ev.ServedWhileAnnouncing)
		},
	})
	list = append(list, Scenario{
		Name: "overload", Group: "overload", Overload: true,
		Description: "More closed-loop clients than the cluster has request slots for the length of the fault window: the workers' slots and queues fill and the gateway answers 503.",
		CannotShow:  "What a real queue in the gateway does (Phase 15 D11): on master there is none, so this shows the rejections the clients get. Closed-loop clients also back off on a 503, which softens the overload an open-loop crowd would cause.",
		params:      func(_ int, scale float64) Params { return newParams(scale) },
		apply: func(_ context.Context, e *Env) error {
			e.Extra.Start(e.ExtraClients)
			return nil
		},
		clear: func(_ context.Context, e *Env) error { e.Extra.Stop(); return nil },
		evidenced: func(ev Evidence) (bool, string) {
			return ev.WindowFailures > 0 || ev.GatewayNoCapacity > 0, fmt.Sprintf("%d failed requests, %.0f gateway no-capacity refusals in the fault window", ev.WindowFailures, ev.GatewayNoCapacity)
		},
	})
	return list
}

// SafeSlotFraction mirrors the Phase 7 rule (runner.SafeSlotFraction): closed-loop clients must stay within
// this share of the slots that are left serving. The runner package imports this one, so the value is
// repeated here and a test pins that the two agree.
const SafeSlotFraction = 0.6

// ClientPlan is how many closed-loop clients a run uses and what that is measured against.
type ClientPlan struct {
	// Base clients run for the whole run; Extra join for the fault window (overload only).
	Base, Extra int
	// Slots is every configured worker's request slots (from the cluster plan, not read from the registry), Surviving the slots left serving once the fault takes the
	// first worker's (for a crash, freeze or error fault the faulted worker is not counted even when it
	// is still READY and still accepts connections; for a rolling drain one worker is out at a time).
	// An overload has no worker out.
	Slots, Surviving int
}

// Clients plans the clients of a scenario on a cluster. requested of 0 means the default: a safe share of
// the surviving slots. An overload runs the same base clients and adds enough for the fault window to
// offer 1.5 times the slots.
func Clients(sc Scenario, specs []embedded.WorkerSpec, requested int) ClientPlan {
	p := ClientPlan{Slots: embedded.Slots(specs)}
	p.Surviving = p.Slots
	if sc.SingleWorker && len(specs) > 0 {
		p.Surviving -= specs[0].MaxConcurrency
	}
	p.Base = requested
	if p.Base <= 0 {
		p.Base = max(int(SafeSlotFraction*float64(p.Surviving)), 1)
	}
	if sc.Overload {
		p.Extra = max(p.Slots+p.Slots/2-p.Base, 1)
	}
	return p
}

// Scenarios lists the scenarios in a stable order.
func Scenarios() []Scenario { return append([]Scenario(nil), scenarios...) }

// Names lists the scenario names, sorted.
func Names() []string {
	var out []string
	for _, s := range scenarios {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

// Lookup finds a scenario by name.
func Lookup(name string) (Scenario, error) {
	for _, s := range scenarios {
		if s.Name == name {
			return s, nil
		}
	}
	return Scenario{}, fmt.Errorf("unknown scenario %q (choose one of: %v, or all)", name, Names())
}

// WriteList prints every scenario with what it shows and what it cannot.
func WriteList(w io.Writer) {
	for _, s := range scenarios {
		_, _ = fmt.Fprintf(w, "%s\n  %s\n  Cannot show: %s\n\n", s.Name, s.Description, s.CannotShow)
	}
	_, _ = fmt.Fprintln(w, "Every scenario has the same two pre-registered primary measures: the success rate in the fault window and the attempts per request in it. Everything else is descriptive.")
}
