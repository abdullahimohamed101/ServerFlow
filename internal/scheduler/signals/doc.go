// Package signals holds what one gateway knows about the work it has sent to each worker (ADR-022): a ledger of
// the requests and estimated token cost it has in flight per worker, and a short history of when it started and
// finished requests, which lets it correct a worker-reported active count for the age of the report (the
// "ledger" load overlay).
//
// The package is pure arithmetic over values the caller passes in. It reads no clock (every method that needs the
// time takes it as an argument), does no I/O and starts no goroutines, so the same code runs in the gateway, in
// tests with a fake clock, and in a simulator. A Tracker has its own lock; the router calls it while holding its
// own, so the lock order is router, then tracker, and the tracker never calls back out.
//
// Terminology. A worker is identified by Key (worker ID and the instant it registered), so a re-registered
// worker starts with an empty ledger. A report is a worker's heartbeat as the gateway's registry snapshot shows
// it: reported counts, and the gateway-local instant the report was made (snapshot fetch time minus heartbeat
// age). Local means this gateway's own in-flight count for the worker.
//
// The overlay. The original rule is max(local, reported). It is wrong in the stale-high direction: requests this
// gateway finished after the report are still in reported, so a worker it has emptied looks full until the next
// heartbeat. The ledger rule is
//
//	effective = max(local, reported + started_since_report - finished_since_report)
//
// where "since the report" comes from the history. The report time is uncertain (round trip, registry
// processing, the worker's sampling delay) and the history is bucketed, so the boundary is classified so that the
// error is always an over-count: a start in the boundary bucket counts as after the report, a finish in the
// boundary bucket or the one after counts as before it. The floor local keeps the guarantee the overlay exists
// for (a stale-low report is still covered by what this gateway knows it sent). When the history cannot answer
// (a worker the tracker has not followed since before the report, a report older than the window, a history
// that was reset) the rule falls back to max(local, reported) and says so.
package signals
