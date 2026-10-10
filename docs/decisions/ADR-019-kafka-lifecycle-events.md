# ADR-019: Kafka Lifecycle Events, a Usage Consumer, and Their Delivery Semantics

Status: Accepted (Phase 12)
Date: 2026-10-10

## Context

Spec sections 20 to 22 want every inference request to leave a trail on Kafka and a consumer that turns it into usage records,
without Kafka ever being on the request path (section 27: no unbounded queues; section 36: Kafka outage). ADR-016 gave the gateway
one `Observer` seam for this. Decisions below were approved with the Phase 12 plan (D1 to D20); this record keeps the ones that are
hard to reverse or easy to misread, and the findings made while building it.

## Decision

- **The producer is an Observer.** `internal/events.Observer` maps the seven Observer calls to five event types
  (`received`, `routed`, `first_token`, `completed`, `failed`). Requests refused before admission emit nothing (an unauthenticated flood
  cannot fill the buffer; they are already counted by the auth and rate limit metrics). Exactly one terminal event per admitted request.
  The per-request state (attempts so far, first token sent) lives on the request context returned by `RequestStarted`, so there is no map
  to leak and no lock shared between requests.
- **Never blocks, bounded, drop-newest.** `Observer` methods make one non-blocking send into a channel of `events.buffer_size`
  (10,000). When it is full the newest event is dropped and counted (`event_publish_failures_total{reason}`). One drain goroutine encodes and
  hands records to the `Sink`, whose own bound is `events.max_buffered_records`. A stuck sink therefore costs bounded memory and zero request
  latency (tested with a sink that never returns, 1,000,000 calls).
- **Event content is structurally limited.** Events are built by copying plain values from the Observer structs into a type with only
  primitive fields; the codec validates every string (length, prefix, control characters) and the closed enums. A reflection test fails if a
  string field is added without review, and a canary test proves a prompt, system message, response text and API key never reach an event,
  a `%+v` dump or a log.
- **Event IDs are deterministic**: `evt_` plus 32 hex of SHA-256(request_id, event type, attempt_id). The same logical event always has the
  same ID, so no path can double count it. `usage_records` is additionally unique on `request_id`.
- **JSON, one topic `inference.lifecycle.v1`, key = `request_id`.** The topic name carries the major version. Rules for evolution are in
  `docs/events/inference-lifecycle-v1.md`; golden fixtures only ever grow.
- **Client library: franz-go pinned at v1.21.7.** Pure Go, idempotent producer, non-blocking produce, group consumer. v1.22.x needs Go 1.26,
  a repo-wide toolchain change that is not this phase's call. Only `internal/kafka` imports it (a test fails otherwise); `events.Sink` and
  `usage.Source` are the seams.
- **Delivery semantics (spec section 22), exactly.** Producer: `acks=all`, idempotent. Duplicates are possible at the consumer (redelivery
  after a crash before the offset commit, or a deliberate replay) and absorbed by the idempotent insert. Loss is possible and counted: buffer
  full, client buffer full, delivery timeout, encode failure, shutdown flush timeout; SIGKILL, OOM or a host crash lose what is buffered and
  that loss is NOT counted. Ordering is per request only. Usage therefore undercounts by at most the drop counters plus a hard-kill buffer,
  and never overcounts from retries. Nothing here is exactly once and it is not billing grade.
- **The consumer commits after the database, never before**, in batches of one transaction (`INSERT ... ON CONFLICT DO NOTHING`). A record
  it cannot use (garbage, a newer `schema_version`, oversize, a row the database refuses) is recorded by coordinates only in
  `usage_rejected_events` and skipped, so one poison message never blocks a partition. With PostgreSQL down it pauses and retries with capped
  backoff and commits nothing.
- **Token counts are labelled, not trusted equally.** `tokens_source` is `usage` (the worker's usage object), `chunks` (streamed content
  chunks counted: output only, an approximation) or `estimate` (nothing reported; counts are null). A bounded scanner reads bytes already
  flowing to the client, keeps a 2 KiB sliding window that is overwritten as the response passes, and emits integers only. Reports group
  by `tokens_source`.
- **Broker for development and CI: Redpanda v25.1.1** (`docker.redpanda.com/redpandadata/redpanda:v25.1.1`; that host only fronts Docker
  Hub and gives no rate-limit relief, and no unauthenticated mirror exists on public.ecr.aws, quay.io or ghcr.io, see
  `docs/development/ci.md`, so CI caches the image and pulls with retries), with SASL/SCRAM so a client that forgets credentials fails locally as it would in CI. Redpanda is Kafka API
  compatible, not Apache Kafka; one verification run is made against `apache/kafka:3.9.1` and recorded in the plan. There is no Apache Kafka
  job in the nightly workflow. Redpanda's community edition is BSL licensed; this use is allowed.

## Findings and deviations made while building it

- **`AllowIdempotentProduceCancellation` is required.** With the idempotent producer, franz-go never fails a record whose request may have
  reached the broker, so after a broker crash the buffered records were neither delivered nor failed (observed: 100 records still pending
  after 40 seconds with `RecordDeliveryTimeout` of 4 seconds), which would pin memory for ever. The option lets the delivery timeout apply to
  in-flight records at the cost of possible duplicates if we re-sent them. We never re-send; the consumer absorbs duplicates. With it, the
  outage test accounts for every event (delivered or counted) within the timeout.
- **`events.batch_max_bytes` replaces the planned `batch_max_records`**: the client has no record-count cap, only a batch size in bytes
  (default 1 MiB) plus `linger`.
- **Metrics are registered with `gateway.WithExtraCollectors`**, a new Option, because each gateway Server owns a private Prometheus
  registry (the plan assumed a shared one). `Observer.Collectors()` is what `cmd/gateway` passes.
- **Per-request state is on the context**, not in a map keyed by request ID (the plan's wording left this open).
- **Terminal events carry only the confirmed model**; `received` carries the model as the client asked for it (bounded to 128 characters and
  control-character free). Client-controlled text therefore never becomes a usage dimension.
- **`Admission` gained `APIKeyID` and `Completion` gained `TokensSource`, `InputTokens`, `OutputTokens`** (additive).
- **The Kafka consumer holds rebalances between a poll and its commit** (`BlockRebalanceOnPoll`), so a member never works on partitions
  another has been given; `Close` must allow rebalances or it blocks (found when a test hung).

## Consequences

- Operators get a content-free event stream and per-request usage rows; the gateway behaves identically with events off (the default) or
  with the broker down, slow or full.
- Another Kafka client or a schema registry can be introduced behind the same interfaces; the contract is `pkg/protocol/events.go`.
- Not covered: multi-broker clusters, TLS to a managed service, rebalancing under load, real vLLM response shapes for token capture
  (Phase 13). See the plan's Implementation Notes.
