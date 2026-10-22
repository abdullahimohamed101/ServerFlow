package chaos

import (
	"context"
	"testing"
	"time"

	"serverflow/internal/bench/embedded"
)

// Worker drain under load (Phase 15 PR F, D15, acceptance 18): every worker of the cluster is replaced in turn while
// closed-loop clients run. embedded.DrainWorker does what a SIGTERM does to a worker and its agent (the mock reports
// draining and keeps serving for its announce period, the agent reports DRAINING and deregisters), then
// ReplaceWorker starts a fresh one.

// rollWorkers drains and replaces every worker, one at a time, and returns the client requests and failures, the
// retries the gateway made, and the requests the drained mocks served while announcing.
func rollWorkers(t *testing.T, announce time.Duration) (reqs, fails int64, retries float64, served int64, statuses map[int]int) {
	t.Helper()
	r := newRig(t, rigOpts{announce: announce, workers: 3})
	r.start()
	defer func() {
		select {
		case <-r.stop:
		default:
			r.finish()
		}
	}()
	time.Sleep(500 * time.Millisecond) // baseline traffic
	retries0 := r.val("inference_retries_total")
	reqs0, fails0 := r.reqs.Load(), r.fails.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, w := range r.c.Specs() {
		if err := r.c.DrainWorker(ctx, w.ID); err != nil {
			t.Fatalf("draining %s: %v", w.ID, err)
		}
		served += r.c.ServedWhileAnnouncing(w.ID)
		if err := r.c.ReplaceWorker(ctx, w.ID); err != nil {
			t.Fatalf("replacing %s: %v", w.ID, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	r.finish()
	return r.reqs.Load() - reqs0, r.fails.Load() - fails0, r.val("inference_retries_total") - retries0, served, r.statuses()
}

// With an announce period above the heartbeat plus the registry refresh the roll costs the clients nothing, and the
// gateway needs no retry because of it: the worker stopped being offered before it began refusing.
func TestDrainRollWithAnnounceCostsNeitherFailuresNorRetries(t *testing.T) {
	announce := embedded.HeartbeatInterval + embedded.RegistryRefresh + 400*time.Millisecond
	reqs, fails, retries, served, st := rollWorkers(t, announce)
	t.Logf("announce %v: %d requests, %d failed, %v retries, %d requests served by drained workers while announcing, statuses %v", announce, reqs, fails, retries, served, st)
	if reqs < 100 {
		t.Fatalf("only %d requests ran: the load did not", reqs)
	}
	if fails != 0 {
		t.Fatalf("%d client failures during a rolling drain with announce: %v", fails, st)
	}
	if retries != 0 {
		t.Fatalf("%v retries during a rolling drain with announce: a draining worker must stop being offered before it refuses", retries)
	}
}

// With no announce the worker refuses the moment it drains; the requests routed from a registry view that has not yet
// caught up are retried on another worker. The clients still see nothing, but the retries are the price: they are
// reported, not asserted to be non-zero.
func TestDrainRollWithoutAnnounceIsCoveredByRetries(t *testing.T) {
	reqs, fails, retries, served, st := rollWorkers(t, 0)
	t.Logf("announce 0: %d requests, %d failed, %v retries, %d served while announcing, statuses %v", reqs, fails, retries, served, st)
	if reqs < 100 {
		t.Fatalf("only %d requests ran: the load did not", reqs)
	}
	if fails != 0 {
		t.Fatalf("%d client failures during a rolling drain without announce (a retry should cover the window): %v", fails, st)
	}
	if served != 0 {
		t.Fatalf("%d requests were served while announcing with announce 0", served)
	}
}
