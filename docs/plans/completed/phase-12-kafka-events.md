# Phase 12 — Kafka: Lifecycle Events and a Usage Consumer

Status: Completed 2026-10-10 (all decisions D1-D20 approved with their defaults; see Implementation Notes for deviations)
Owner: coding agent
Depends on: the prep plan `prep-lifecycle-observer-and-config-split.md` (the `gateway.Observer` seam and the per-feature config files) must be merged first; Phase 9 (tenants, API keys, migrations) and Phase 8 (the "must run, not skip" CI pattern, the Redis-style client package, the overhead measurements).
Spec: `docs/architecture/serverflow-spec.md` §6 (lifecycle), §7 (IDs), §17 (cost estimate), §19 (usage_records), §20–22 (Kafka, consumers, delivery semantics), §27 (no unbounded queues), §29 (Kafka metrics), §36 (Kafka outage), §45 (tenants), §58 Phase 12, §63 (agent rules)
Hand-offs: the prep plan gives one `Observer` with seven methods; this phase adds the third implementation (after metrics and, in parallel, tracing). Phase 2's 25 ms p95 gateway overhead budget and Phase 8's measurements still hold. Phase 10 owns the Prometheus registry; this phase registers its Kafka metrics in it.

## Outcome

Every admitted inference request leaves a small, content-free trail of lifecycle events on Kafka, and a separate `usage-consumer` turns the terminal events into durable per-request usage rows in PostgreSQL. Kafka is never on the synchronous path: with the broker down, slow or full, clients get the same responses at the same latency, and the loss is counted, not hidden.

```text
client ─► gateway ──(Observer calls, nanoseconds)──► bounded buffer ──► batcher ──► Kafka  inference.lifecycle.v1
              │                                       drop + metric when full           │ key = request_id
              └─► worker (unchanged)                                                    ▼
                                                              usage-consumer (group) ─► PostgreSQL usage_records
                                                              idempotent insert keyed by event_id / request_id
```

Acceptance headline (spec §58): *producer emits received, routed, first_token, completed, failed; usage consumer; stop-consumer / replay.* Demonstrated by a repeatable test and a documented manual run: stop the consumer, send N requests, start it, totals equal N; replay the whole topic, totals unchanged.

## Non-Goals

- No exactly-once anywhere (spec §22). No Kafka transactions, no outbox.
- Not billing-grade. Usage is "best-effort accurate" (see D9 for exactly what can be lost or duplicated). Invoicing needs a different design.
- No `worker.lifecycle.v1` or `cluster.scaling.v1` producers (named in spec §20; no producer exists until worker drain and autoscaling phases), no analytics or autoscaling consumers (spec §21).
- No schema registry, Avro or protobuf (D5). No event for rejected requests (D3).
- No Kafka in `/readyz`, no Kafka-driven routing or rate limiting, no Kafka on any request-path code.
- No quota enforcement from usage, no per-tenant Prometheus labels (Phase 10), no reports UI. A small SQL view and an admin summary command only.
- No multi-broker or Kubernetes operations work (Phases 16–17), no Kafka Connect, no topic management service.
- Events are **off by default**; Phases 2–11 behave exactly as before.

## Current Architecture

- `cmd/usage-consumer/main.go` is a Phase 0 stub (loads config, builds a logger). There is no `internal/events`, no Kafka config, no Kafka dependency in `go.mod` (pgx, go-redis, yaml, prometheus only; `go 1.25.0`).
- `docker-compose.yml` already has a `kafka` service (`bitnami/kafka:3.7`, unvalidated and Docker Hub hosted; it is revisited in Phase 16, not here).
- After the prep plan: the gateway calls `Observer` methods (`RequestStarted`, `RequestAdmitted`, `RequestRejected`, `AttemptStarted`, `FirstToken`, `AttemptEnded`, `RequestCompleted`) with small immutable value structs; observers are registered with `gateway.WithObserver`, must not block, and a panicking observer is recovered.
- IDs: `protocol.NewRequestID` (`req_…`), `NewAttemptID` (`att_…`); tenant IDs `ten_…`, key IDs `key_…` come from Postgres (`internal/postgres/ids.go`). Attempt history is append-only (spec §7).
- The gateway knows an **estimate** (`ratelimit.EstimateRequestCost`, input + `max_tokens`, ADR-015) but no actual token counts: it proxies bodies unchanged (ADR-003) and parses no `usage`. The mock worker returns `usage` for non-streaming responses only and emits no usage chunk when streaming.
- Postgres: forward-only embedded migrations `0001`, `0002` (`migrations/`, checksum-checked), applied by `serverflow-admin migrate up`; `internal/postgres` is the only package importing pgx. Spec §19 lists `usage_records`; it does not exist yet.
- Redis phase precedent reused here: one package owns the client library (`internal/redis`), secrets are redacted in `String`/`LogValue`/`MarshalJSON`, a non-loopback server without TLS and credentials is refused, `scripts/dev-redis.sh` mirrors CI including authentication, and `scripts/quality.sh` has a `must_run` stanza with named tests and a skip message.
- Local machine: Docker is available. CI runners are rate-limited on Docker Hub, so CI pulls Redis from `public.ecr.aws/docker/library/redis:7`.

## Decisions (confirm before implementation)

Each decision has a default; "approve all defaults" is a valid answer.

- **D1. Layout and the Observer.** The producer is a third `gateway.Observer` (`internal/events.Observer`) that maps Observer calls to events and hands them to a bounded publisher. Packages:
  - `pkg/protocol/events.go`: the event types, envelope, `Encode`/`Decode`/`Validate`, event-type constants. It is the cross-component contract (ARCHITECTURE: `pkg/protocol` is the shared contract), so the consumer never imports `internal/gateway`.
  - `internal/events`: the observer, the bounded publisher (buffer, batching, drop policy, metrics), the `Sink` interface, and `eventstest` (recording sink, in-memory broker).
  - `internal/kafka`: the **only** package importing the Kafka client library: producer `Sink`, consumer `Source`, TLS and SASL, redaction, health logging (the `internal/redis` pattern).
  - `internal/usage`: the consumer loop (decode, dedupe, batch, commit), depending on `Source` and `Store` interfaces.
  - `internal/postgres/usage.go`: the `Store` (pgx stays confined).
  Dependency direction stays `cmd → internal → pkg/protocol`; `internal/gateway` never imports `internal/events`.
- **D2. Kafka client library: `github.com/twmb/franz-go` pinned at `v1.21.7`** (pure Go, with its `kgo` client; transitive: `pkg/kmsg`, `klauspost/compress`, `pierrec/lz4/v4`; five modules in all including itself). Evaluation, from `go list -m` and a scratch-module build on this machine:

  | | franz-go | segmentio/kafka-go | IBM/sarama |
  | --- | --- | --- | --- |
  | Latest | v1.22.1 (needs go 1.26.0), **v1.21.7 needs go 1.25.0** | v0.4.51 (go 1.23), released 2026-04 | v1.61.1 (needs go 1.26.0) |
  | Pure Go (`CGO_ENABLED=0` builds) | yes (verified for v1.22.1 and v1.21.7) | yes (verified) | not checked |
  | Idempotent producer, non-blocking `TryProduce`, bounded `MaxBufferedRecords` | yes, built in | no idempotent producer or transactions (from its documentation, to be re-checked) | idempotent yes; async producer API is channel-based |
  | Consumer groups, manual commit after processing | yes, mature | exists; known rebalance and commit rough edges | yes |
  | Dependencies | 4 external modules | 3 | many (sizeable tree) |

  Choice: franz-go `v1.21.7`, because it is the only candidate that meets the repo's `go 1.25.0` floor at a current version, gives idempotent produce and a non-blocking bounded produce path (what D10 needs) without a hand-built layer, and is pure Go. `v1.22.x` would force `go 1.26.0` in `go.mod`, a repo-wide toolchain change that is not this phase's call; Dependabot may propose it, and that PR should be judged on its own. This is the third external client dependency (after pgx and go-redis); the standard library has no Kafka client and a hand-written wire protocol is far riskier than the dependency. All use is inside `internal/kafka`; `go mod tidy -diff` and `govulncheck` must stay clean. Segmentio is the fallback if franz-go proves unworkable; the `Sink`/`Source` interfaces make the switch local.
- **D3. Event types and mapping.** Five types, named like the spec example (`inference.request.completed`): `inference.request.received`, `.routed`, `.first_token`, `.completed`, `.failed`.

  | Event | Emitted from | Per |
  | --- | --- | --- |
  | `received` | `RequestAdmitted` (auth, rate limit and model validation passed, so tenant and model are known) | request |
  | `routed` | `AttemptStarted` | attempt |
  | `first_token` | `FirstToken` | request (first attempt that produced one) |
  | `completed` | `RequestCompleted`, success | request |
  | `failed` | `RequestCompleted`, anything else (worker error, no capacity, timeout, client disconnect with `failure_class: client_closed`) | request |

  `AttemptEnded` produces no event of its own: the terminal event carries `attempts[]` (attempt ID, worker, outcome, failure class, duration), so retry history is preserved (spec §7) without a sixth type. Requests refused before admission (`RequestRejected`: auth, rate limit, validation, unknown model) produce **no event** in v1: they have no worker or usage, an unauthenticated flood must not be able to fill the buffer, and they are already counted by `rate_limit_rejections_total` and the auth metrics. Exactly one terminal event (`completed` xor `failed`) per admitted request, tested.
- **D4. Envelope, payload, privacy.** Every event carries: `event_id`, `event_type`, `schema_version`, `timestamp` (UTC, RFC 3339 with nanoseconds, when the event happened on the gateway), `source` (gateway instance ID), `request_id`, `attempt_id` (the attempt concerned; empty for `received`), `tenant_id` (empty when auth is off), `api_key_id` (the `key_…` ID, never the prefix or hash), `model`, `worker_id` (empty before routing). Payload by type:
  - `received`: `stream`, `estimated_cost_tokens` (spec §17 estimate).
  - `routed`: `attempt_number`, `strategy`.
  - `first_token`: `ttft_ms`.
  - `completed` / `failed`: `stream`, `http_status`, `duration_ms`, `ttft_ms` (if any), `input_tokens`, `output_tokens` and `tokens_source` (D8, tokens omitted when unknown), `estimated_cost_tokens`, `attempts[]`, and for `failed` a `failure_class` from a closed enum (`no_capacity`, `worker_error`, `timeout`, `client_closed`, `internal`, …).

  Deliberately **not** in events, ever: prompts, messages, completions or any response text, request or response bodies and headers, API keys or secrets (not even the prefix), client IP, free-text upstream error strings (only the closed enum), Kafka or Redis or Postgres credentials, the full config. The spec example's `queue_ms` is omitted: the gateway cannot measure worker queue time, and an invented number is worse than a missing field (it can arrive additively when workers report it). Enforcement is structural, not by review: event structs have only primitive typed fields built by explicit copy from the Observer value structs (which carry no content); the encoder rejects strings over a length cap; and a canary test (acceptance 4) proves a prompt and a key never reach an encoded event.
- **D5. Encoding, versioning and evolution.** JSON, UTF-8, snake_case, one event per Kafka record; `schema_version: 1` in the envelope; the topic name carries the major version (`…v1`). No schema registry (one team, one repo, one consumer; JSON keeps `kcat`-level debuggability). Rules, enforced by tests:
  1. Adding an optional field is compatible and does not bump the version. Consumers ignore unknown fields.
  2. Removing, renaming, retyping or changing the meaning of a field is breaking: new `schema_version` **and** new topic `inference.lifecycle.v2`; v1 keeps being produced until consumers move (a dual-publish window, to be planned then).
  3. A field's name is never reused for a different meaning; deprecated fields stay documented.
  4. A consumer meeting a `schema_version` newer than it supports does not crash or guess: it records a reject (D13) and moves on.
  5. Golden fixtures live in `pkg/protocol/testdata/events/v1/*.json` (one per type, plus a retry case). A compatibility test decodes every committed fixture with the current code, so a drifting struct fails CI; fixtures are only added, never edited.
- **D6. Event ID and dedupe keys.** `event_id = "evt_" + first 32 hex chars of SHA-256(request_id | event_type | attempt_id)`: deterministic, so any code path that emits the same logical event twice (a double Observer call, a producer retry that outlives idempotence, a replayed publish) yields the same ID and cannot be double counted. This satisfies spec §22 (`event_id`, or request + type + attempt) with one key. The usage table is additionally unique on `request_id` for terminal events (D14), so even two terminal events with different IDs (a bug) record usage once and are counted as `duplicate`. Alternative (random IDs plus a unique `(request_id, event_type, attempt_id)` constraint) is equivalent in effect but spreads the rule over two places.
- **D7. Topic, keying, partitions, retention.** One topic this phase: `inference.lifecycle.v1`. **Key = `request_id`**: all events of one request land on one partition in the order produced, which is the only ordering anything needs (`routed` before `completed`); usage aggregation is commutative, so per-tenant ordering is not needed, and keying by tenant would pin a hot tenant to one partition and one consumer. Default 6 partitions (local and CI broker; production sizing is Phase 16), replication factor from the broker (1 locally), `retention.ms` 7 days (the replay window and the maximum outage the consumer can recover from; documented), `cleanup.policy=delete`. The gateway does not create topics (`events.auto_create_topic` does not exist): `scripts/dev-kafka.sh` and the tests create it, and the operations doc gives the production command. A missing topic is a delivery failure counted and logged once per outage, never a crash.
- **D8. Token counts: where the numbers come from.** The gateway has only an estimate today. Decision: the gateway captures **reported usage when present** and labels everything with `tokens_source`: `usage` (the worker's `usage` object: non-streaming responses, or a final streaming chunk when `stream_options.include_usage` is set), `chunks` (streaming without usage: count of `data:` chunks carrying non-empty delta content as an approximation of output tokens; input stays unknown), or `estimate` (neither; tokens omitted, only `estimated_cost_tokens`). Capture is a bounded pass over bytes already flowing to the client: it extracts integers only and retains no text. This needs two small additive changes outside the events package: `Completion` gains `InputTokens`, `OutputTokens`, `TokensSource`, and the proxy path gets the scanner. A one-day spike first measures the cost and confirms that vLLM's response shape works (not verifiable until Phase 13 on real vLLM; mock worker and the OpenAI format are what is testable now). If the spike shows measurable overhead, fall back to `estimate` only and say so; the schema does not change. Rows with different sources are never silently summed as if equal: the report groups by `tokens_source`.
- **D9. Delivery semantics (spec §22), stated precisely.** Producer: `acks=all`, idempotent producer on, bounded delivery timeout. Promises and non-promises:
  - **Duplicates:** possible at the consumer (redelivery after a crash before offset commit; a replay by choice). Harmless: the consumer is idempotent (D6, D14). The producer's own retries are deduplicated by the broker within a session.
  - **Loss:** possible and counted. Events are dropped when the in-memory buffer is full, when the client's own bounded buffer is full, when delivery fails past `events.delivery_timeout`, on encode failure, and after a shutdown flush timeout; a `SIGKILL`, OOM kill or host crash loses whatever is buffered. Every drop except a hard kill increments `event_publish_failures_total{reason}`.
  - **Ordering:** per request, events on one partition are in produced order. Across requests and tenants no order is promised. Because of drops, a consumer must never require a complete sequence: a `completed` without a `routed`, or a `routed` without a terminal event, is normal.
  - **Consequence:** usage rows undercount by at most the drop counters plus a hard-kill buffer's worth; they never overcount from retries. Documented as such. Nothing here is "exactly once".
- **D10. Never block the request path; bounded buffering; outage behaviour (spec §36).** The Observer method builds a small struct and does one non-blocking send into a bounded channel (`events.buffer_size`, default 10,000 events; events are capped at 16 KiB, so the hard memory ceiling is documented as buffer_size × cap, in practice a few MB). When full, the **newest** event is dropped and counted (`reason=buffer_full`); the call never waits and takes no lock a slow path can hold. A single drain goroutine encodes and hands records to the client with its non-blocking produce call (`reason=producer_full` if the client's own bounded buffer, `events.max_buffered_records`, is full). Batching: `events.linger` (default 50 ms), `events.batch_max_records` (default 500). Outage: the client reconnects with exponential backoff and jitter on its own; records not acknowledged within `events.delivery_timeout` (default 30 s) are dropped with `reason=delivery_failed`, so a dead broker costs bounded memory and a bounded loss window, never growth. The gateway **starts without Kafka** (no fail-fast, unlike `rate_limit.mode=required`: events are best-effort by design); the outage is logged once with the broker list (never credentials) and recovery once. Inference continues untouched through all of it. No Kafka state enters `/readyz`.
- **D11. Shutdown flush.** On SIGTERM `cmd/gateway` shuts the HTTP server down first (so in-flight requests complete and emit their terminal events), then calls `Close(ctx)` on the events observer: stop accepting (late events count `reason=shutdown`), drain the channel, flush the client, bounded by `events.shutdown_flush_timeout` (default 5 s, must not exceed `gateway.shutdown_timeout`; validated). Whatever is not flushed in time is counted as dropped and logged with a count. Documented limit: SIGKILL loses the buffer.
- **D12. Configuration** (new file `internal/config/events.go`, per the prep split; env override via the existing `SERVERFLOW_` mechanism). `events.mode` (`off` default | `on`), `events.brokers` (list), `events.topic` (default `inference.lifecycle.v1`), `events.client_id`, `events.source` (default hostname), `events.tls` (bool; optional `events.tls_ca_file`), `events.sasl_mechanism` (`none` default | `plain` | `scram-sha-256` | `scram-sha-512`), `events.sasl_username`, `events.sasl_password` (**secret**), `events.compression` (`snappy` default), the buffer and timing keys from D10–D11, and consumer keys `events.consumer.group_id` (default `serverflow-usage`), `start_offset` (`earliest` default for a new group | `latest`), `batch_size` (500), `batch_timeout` (1 s), `metrics_addr`. Secret handling copies `RedisConfig`: the password is redacted in `String`, `GoString`, `LogValue` and `MarshalJSON`; the Kafka client is built so no error path can echo it (errors from `internal/kafka` are scrubbed and a test feeds a broker that echoes the credential, as in Phase 8); the broker list is validated as `host:port` (no embedded credentials). Validation: `on` needs at least one broker and a topic; positive durations; a non-loopback broker without TLS **and** SASL is refused unless `events.allow_insecure_transport` is set; `shutdown_flush_timeout ≤ gateway.shutdown_timeout`; SASL mechanism requires username and password. Settings do not apply while `mode=off`, so a malformed `events` section cannot stop a gateway that never uses it (the Redis rule).
- **D13. The usage consumer (`cmd/usage-consumer`, `internal/usage`).** A consumer group member on `inference.lifecycle.v1` using Postgres from the existing `postgres` config. Behaviour:
  - Only `completed` and `failed` are processed; other types are skipped (and committed past).
  - **Commit after the database commits, never before** (autocommit off). A batch (up to `batch_size` records or `batch_timeout`) becomes one transaction of `INSERT … ON CONFLICT DO NOTHING`; offsets are committed only after it succeeds. A crash between the two replays the batch and the conflict clause absorbs it.
  - **Poison messages never block a partition.** Undecodable JSON, a failed `Validate`, an unknown or newer `schema_version`, or an oversized record is recorded in `usage_rejected_events` (topic, partition, offset, reason, time; **no payload**), counted, logged without the payload, and skipped.
  - **Postgres down:** the batch is retried with capped exponential backoff, offsets are not committed, polling pauses (memory stays bounded by `batch_size`), and recovery is logged once. Kafka down: the client reconnects; the consumer idles.
  - Graceful shutdown finishes or abandons the current batch, commits what is durable, exits within a timeout. `/healthz` and `/metrics` on `events.consumer.metrics_addr`. A second instance in the same group splits partitions (rebalance tested only against a real broker).
- **D14. Usage table and migration** (`migrations/0003_usage_records.sql`, forward-only). `usage_records`: `event_id text PRIMARY KEY`, `request_id text NOT NULL UNIQUE`, `tenant_id text NOT NULL DEFAULT ''` (empty = auth off; **no foreign key**, so consumption never depends on tenant rows or ordering), `api_key_id text`, `model text NOT NULL`, `worker_id text NOT NULL DEFAULT ''`, `outcome text` constrained to `completed|failed`, `failure_class text`, `http_status integer`, `input_tokens bigint` and `output_tokens bigint` (nullable: unknown is not zero), `tokens_source text` constrained to `usage|chunks|estimate`, `estimated_cost_tokens bigint`, `attempts integer`, `ttft_ms`, `duration_ms`, `occurred_at timestamptz NOT NULL` (the event time, so replays do not move usage into "today"), `recorded_at timestamptz DEFAULT now()`, `kafka_partition`, `kafka_offset` (provenance only). Indexes `(tenant_id, occurred_at)` and `(model, occurred_at)`. Also `usage_rejected_events` and a plain view `usage_hourly` (requests, failures, summed tokens by `tenant_id`, `model`, `tokens_source`, hour): "persist summaries" (spec §21) with no second write path to drift. `serverflow-admin usage summary --since … [--tenant …]` reads the view. A migration test follows the existing pattern (`migrations_test.go`); the checksum rules apply.
- **D15. Stop-consumer / replay acceptance is a test and a runbook.** `TestUsageStopAndReplay` (integration, must run): start consumer; send 50 requests through a real in-process gateway with the events observer (mock upstream); wait for 50 rows. Stop the consumer; send 200 more; assert rows still 50 and lag ≥ 200 events (via the broker's group offsets); start the consumer; assert exactly 250 rows and lag drains to 0 within a bound. Then replay: start a consumer under a **fresh group ID** with `start_offset=earliest` (equivalent to resetting offsets); it re-reads all events; assert the row count, per-tenant sums and `SUM` of tokens are unchanged and `usage_consumer_records_total{result="duplicate"}` equals the events replayed. Also kill the consumer between the DB commit and the offset commit (fault hook) and assert no loss and no double count. The runbook `docs/operations/kafka-and-usage.md` repeats this by hand with the dev broker and also resets a real group's offsets with the broker CLI in the container, once, during verification (recorded in the PR).
- **D16. Metrics (spec §29).** Gateway side, registered in the existing registry: `event_publish_failures_total{reason}` (`buffer_full|producer_full|delivery_failed|encode|shutdown`), `event_publish_latency_seconds` (histogram, enqueue to broker acknowledgement), plus `events_published_total` and `events_buffer_depth` (gauge) as the operator's drop-rate denominator. Consumer side: `consumer_lag{group,topic}` (sum over partitions, from fetch high watermarks minus the last processed offset; no per-partition label), `usage_consumer_records_total{result="inserted|duplicate|skipped|rejected"}`, `usage_consumer_db_errors_total`. No tenant, request or model labels. Logs carry request ID, tenant ID and event type, never payloads.
- **D17. Broker for local development and CI.** `scripts/dev-kafka.sh start|stop|status|reset|brokers|…` modelled on `dev-redis.sh` (fixed port `127.0.0.1:59092`, one named container per port, public throwaway credentials, no persistence, creates the topic). Image: **Redpanda, `docker.redpanda.com/redpandadata/redpanda:v25.1.1`**: a single-binary Kafka-API broker that starts in seconds with `--mode dev-container`, with no Docker Hub dependency (I confirmed the manifest is reachable from this machine; I have not pulled or run it). Why not `apache/kafka`: it is the reference implementation, but its official image is published on Docker Hub only (not found on `public.ecr.aws`, `quay.io` or `ghcr.io` when probed), and Docker Hub rate-limited this repo's CI runners. Trade-off, stated plainly: Redpanda is Kafka API compatible, not Kafka; behaviour differences could hide a bug. Mitigations: tests use only the standard protocol features (produce, group consume, commit); the verification run repeats the integration suite once against `apache/kafka:3.9.1` on the Mac (Docker Hub pull, authenticated if needed) and the result goes in the PR; whether a nightly job runs against Apache Kafka is an open question. Redpanda's licence (BSL for the community edition, free for this use) is noted in the docs. The broker enables SASL/SCRAM with a fixed public test user when that works within a one-hour timebox, so a client that forgets credentials fails locally the way it would in CI (the Phase 8 lesson); otherwise plaintext on loopback with SASL covered by unit tests plus one manual run, and that is said in the PR. `docker-compose.yml`'s Bitnami service is left for Phase 16.
- **D18. CI and the "must run, not skip" pattern.** `ci.yml`'s integration job gains a "Start Kafka" step (`docker run` of the Redpanda image, wait until it answers, create the topic), job name unchanged. `scripts/quality.sh` gains `must_run kafka SERVERFLOW_TEST_KAFKA_BROKERS "SERVERFLOW_TEST_KAFKA_BROKERS is not set" "./internal/kafka/... ./internal/events/... ./internal/usage/... ./tests/integration/..." <named tests>` (initial names: `TestProducerDeliversToRealBroker`, `TestBrokerOutageDoesNotBlockAndDropsAreCounted`, `TestConsumerGroupCommitsAfterDatabase`, `TestUsageStopAndReplay`, `TestGatewayEventsEndToEnd`), the integration step fails on a skip or a missing named test, and `full` includes it only when the Kafka variable is set (loud notice otherwise, like Postgres and Redis). Test clients pass credentials (a guard test, as in Phase 8). Each broker test uses a unique topic and group name so parallel packages and reruns do not interact.
- **D19. What is tested without a broker.** `Sink` and `Source` are interfaces. `eventstest` provides a recording sink, a blocking/slow/failing sink, and an in-memory broker (partitions by key hash, offsets, consumer groups with manual commit, a "crash before commit" hook). Without Kafka we can fully test: Observer-to-event mapping and sequences, schema, fixtures and canaries, buffer-full and drop accounting, shutdown flush, never-blocks, the consumer's dedupe, commit-after-DB ordering, poison handling and replay, config and redaction. Only a real broker proves: client settings (acks, idempotence), real outages and reconnects, rebalance, lag, and protocol-level SASL/TLS.
- **D20. Overhead budget.** Observer cost per event is tens of nanoseconds to low microseconds and allocation-light; the budget is: with events on and the broker **up**, **down**, and **slow**, gateway p95 overhead is within run-to-run spread of events off and never more than +1 ms (the Phase 2 25 ms p95 budget is not touched), memory flat, goroutine count flat. Measured with the Phase 2 overhead test and the Phase 7 harness, with the broker killed mid-run and slowed by a TCP proxy (the Phase 8 failure-matrix technique), recorded in `docs/benchmarks/phase-12-kafka-events.md`. A Go benchmark of the Observer with a fake sink pins ns/op and allocs/op.

## Proposed Design

```go
// pkg/protocol/events.go
type Event struct { // envelope; payload fields are typed and optional
    EventID, EventType string; SchemaVersion int; Timestamp time.Time
    Source, RequestID, AttemptID, TenantID, APIKeyID, Model, WorkerID string
    Received *ReceivedData; Routed *RoutedData; FirstToken *FirstTokenData; Terminal *TerminalData
}
func (e Event) Validate() error
func Encode(e Event) ([]byte, error)           // enforces length caps, drops nothing silently
func Decode(b []byte) (Event, error)           // tolerant of unknown fields, strict on version

// internal/events
type Record struct { Key, Value []byte; EnqueuedAt time.Time }
type Sink interface {                           // implemented by internal/kafka and eventstest
    Produce(r Record, done func(error)) error   // never blocks; ErrFull when the client buffer is full
    Flush(ctx context.Context) error
    Close() error
}
func NewObserver(sink Sink, cfg Config, reg prometheus.Registerer, log *slog.Logger) *Observer // implements gateway.Observer
func (o *Observer) Close(ctx context.Context) error // flush on shutdown (D11)

// internal/usage
type Source interface { Poll(ctx context.Context, max int) ([]Message, error); Commit(ctx context.Context, upTo []Offset) error; Lag() int64; Close() }
type Store  interface { InsertUsage(ctx context.Context, rows []Row) (inserted int, err error); RecordReject(ctx context.Context, r Reject) error }
```

`gateway.WithObserver(events.NewObserver(...))` is added in `cmd/gateway` only when `events.mode=on`. The Observer holds no request state beyond what the Observer events carry; the terminal event's `attempts[]` is assembled from `AttemptStarted`/`AttemptEnded` kept in a small per-request struct stored on the context-free `Completion` value by the gateway (the prep plan's `Completion` already carries attempts); if the prep plan's value structs lack a field needed here (token counts, D8), the addition is made to the gateway side, additively, in this phase.

## Affected Files / Components

New: `pkg/protocol/events.go` (+tests, `testdata/events/v1/`), `internal/events/` (+`eventstest`), `internal/kafka/`, `internal/usage/`, `internal/postgres/usage.go`, `migrations/0003_usage_records.sql`, `internal/config/events.go`, `scripts/dev-kafka.sh`, ADR-016 or next free number (events, delivery semantics, client library, broker choice), `docs/operations/kafka-and-usage.md`, `docs/benchmarks/phase-12-kafka-events.md`, `docs/events/inference-lifecycle-v1.md` (the schema reference and evolution rules).
Changed: `cmd/gateway` (build and register the observer, flush on shutdown), `cmd/usage-consumer` (replace the stub), `cmd/admin` (`usage summary`), `internal/gateway` (token capture and `Completion` fields only, D8), `internal/config/config.go` (one `Events` field), `.github/workflows/ci.yml`, `scripts/quality.sh`, `Makefile` (`dev-kafka`, `test-kafka`), `docs/development/ci.md`, `ARCHITECTURE.md` (Shared Services, Kafka paragraph), README phase table, `go.mod`/`go.sum` (franz-go and transitive modules, listed in the PR).
Untouched: scheduler, registry, rate limiter, auth, worker code, benchmark harness (except use).

## Acceptance Criteria

1. **Schema and mapping:** a recording-sink test drives the gateway through success (streaming and not), retry on a second worker, no capacity, upstream error, timeout, client disconnect, and an auth/rate-limit/validation refusal, and asserts the exact event sequence per D3 (refusals emit nothing; exactly one terminal event per admitted request; `routed` once per attempt; `attempts[]` complete and ordered; no attempt overwritten).
2. **Fixtures and evolution:** golden v1 fixtures decode under current code; an added unknown field is ignored; a `schema_version` above supported yields a reject, not a panic; `Validate` refuses missing IDs, bad types and over-long strings.
3. **Event ID:** deterministic and stable (same inputs, same ID; differing type or attempt, differing ID); property test over random inputs; an event without an ID cannot be encoded.
4. **Privacy canary:** a request whose prompt, system message, response text and API key contain unique canary strings produces events in which no canary byte appears (checked on the encoded bytes, on `fmt`/`%+v` output, and in logs), and a reflection test proves event structs contain only primitive typed fields.
5. **Never blocks:** with a sink that blocks forever, 1 million Observer calls finish within a fixed time bound with the request path unaffected; buffer depth never exceeds `buffer_size`; drops equal `event_publish_failures_total{buffer_full}`; goroutines and memory are bounded (checked with `-race`).
6. **Outage matrix (real broker):** broker stopped, black-holed, slow, and topic missing: inference requests keep succeeding with unchanged status and latency within D20; drops are counted by reason; the gateway starts with the broker down; outage and recovery each logged once; after recovery new events arrive; no goroutine or connection leak.
7. **Shutdown:** SIGTERM with N in-flight requests delivers all N terminal events within `shutdown_flush_timeout`; with the broker down the flush gives up on time and the unflushed count is logged and counted.
8. **Delivery semantics (real broker):** every produced event is received at least once; duplicates (forced by consumer crash before commit) produce no duplicate usage rows; per-request events arrive in order on one partition.
9. **Consumer correctness:** only terminal events create rows; unique on `event_id` and `request_id`; commit happens only after the database commit (fault-injection test: crash between them loses nothing and double counts nothing); a poison record (garbage, newer version, oversize) is rejected and the partition keeps moving; Postgres down pauses and recovers without loss or memory growth.
10. **Stop / replay:** `TestUsageStopAndReplay` as in D15 passes repeatably (run 10 times), including the lag observation and the replay-leaves-totals-unchanged check; the manual runbook reproduces it with the dev broker.
11. **Migration:** `0003` applies on a database with `0001`–`0002`, is idempotent under the runner, passes the checksum and constraint tests; `usage_hourly` totals equal a direct `SUM` over `usage_records`.
12. **Config and secrets:** defaults keep events off and earlier behaviour identical (existing tests unmodified); the SASL password appears in no log, error, panic, `%v`/`%+v`/`%#v`, JSON dump or help text, including against a broker that echoes it; an insecure remote broker is refused; validation errors name the key, never the value; `mode=off` ignores a malformed section.
13. **Metrics:** the D16 series exist with bounded labels, change as asserted by the tests above, and `consumer_lag` reflects a stopped-then-started consumer.
14. **Overhead:** D20 numbers recorded for events off, on/up, on/down, on/slow; p95 within spread and ≤ +1 ms; Observer benchmark ns/op and allocs/op recorded.
15. **Build hygiene:** `go.mod` stays `go 1.25.0`; `CGO_ENABLED=0 go build ./...` succeeds; `go mod tidy -diff`, `go mod verify`, `govulncheck`, `gofmt`, `go vet`, `golangci-lint`, `go test -race ./...` pass with and without the brokers (Kafka tests skipping cleanly without); the Kafka tests **cannot silently skip** in `scripts/quality.sh integration`; every CI step including the Kafka container is run locally step by step before pushing.

## Verification Plan

- **Focused:** `go test -race -count=5 ./pkg/protocol/... ./internal/events/... ./internal/usage/... ./internal/config/...` without a broker (in-memory broker); then the same plus `./internal/kafka/... ./tests/integration/...` against `scripts/dev-kafka.sh`.
- **Integration:** real gateway(s) with the events observer, mock workers, real Postgres and broker; the outage matrix (stop, black-hole and slow via a TCP proxy, missing topic); `TestUsageStopAndReplay` ×10; a process test of `cmd/gateway` and `cmd/usage-consumer` (startup with the broker down, SIGTERM flush).
- **Quality gate:** `scripts/quality.sh full` with Postgres, Redis and Kafka all running; CI steps replayed locally.
- **Overhead:** D20 runs, repeated until the spread is characterised; Docker Desktop clock and sleep effects controlled (`caffeinate`) as in Phase 8.
- **Mutation checks** (copy tree, apply one change, expect a named test to fail): block on a full buffer instead of dropping; drop the oldest instead of newest without counting; lose or zero the event ID; change the key from `request_id`; commit offsets before the database write; remove `ON CONFLICT` (double write); write usage for `received`/`routed`; add a prompt or key field to an event (canary must fail); pass an unredacted password into an error; flush before the HTTP server drains; skip `Close`; swallow decode errors so a poison message stalls the partition; reuse a field name with a new type (fixture test must fail).
- **Independent verifier brief** (read-only, fresh context): re-derive from spec §20–22, §29, §36 what must hold; confirm the Observer never blocks by reading every code path from `Observer` method to socket; try to leak a canary through every field, log line, error and metric label; apply the mutations above blind; challenge the delivery-semantics statement (D9) by finding a path that duplicates or loses without being counted; run the stop/replay demonstration from the runbook alone, as a stranger would. Then a review pass and a security look at the SASL/TLS handling and the new binary's exposed port (AGENTS.md: security-sensitive changes get explicit review).
- **Cannot be verified on this machine now:** behaviour on multi-broker clusters, TLS to a real managed Kafka, SASL against Apache Kafka's own implementation beyond the one-off run, rebalancing with more than one consumer under load, and real vLLM response shapes for D8 (Phase 13). `docker pull` and running the broker image were not attempted while planning, and the franz-go build was checked with the local Go 1.27.1 toolchain (module metadata says go 1.25.0, but a build under the 1.25 toolchain itself has not been run; CI uses `go.mod`'s version and will).

## Risks

- **Duplicate or inflated usage (billing-style).** Mitigated by deterministic event IDs, the two unique keys, commit-after-DB, replay tests, and the explicit non-goal of billing-grade accuracy. The opposite risk, undercounting from drops, is visible in metrics but not recoverable; documented.
- **Unbounded buffer memory.** Two bounded buffers, event size cap, drop policy tested; a mutation (block or grow) must fail a test.
- **Schema drift between producer and consumer.** One shared type in `pkg/protocol`, fixtures that only grow, evolution rules, version-aware consumer.
- **Poison messages stalling a partition.** Reject-and-skip with coordinates only; tested; a flood of rejects is counted and logged rate-limited.
- **Token numbers of mixed quality** (`usage` vs `chunks` vs `estimate`). Labelled per row and grouped in reports; the D8 spike may reduce this phase to estimates, which is acceptable and stated.
- **CI broker flakiness and image supply.** Redpanda from a non-Docker-Hub registry, readiness wait with a bound, unique topics and groups per test, the log kept on failure; Redpanda is not Kafka, so one Apache Kafka run is part of verification and a nightly job is an open question.
- **Shutdown losing the buffer.** Ordered shutdown, flush timeout, counted remainder; SIGKILL loss is documented, not engineered away.
- **Client dependency risk.** Third client dependency; confined to one package behind interfaces; pinned below the version that would raise the repo's Go floor; vulnerabilities watched by `govulncheck`.
- **Hot-path creep.** Token capture (D8) is the only change on the proxy path; it has its own overhead gate and a fall-back to estimates.
- **Secrets.** SASL password and broker error text; handled as in Phase 8, with a hostile-broker test.
- **Scope creep** toward analytics/autoscaling consumers, topic management, or exactly-once; the Non-Goals say no.

## Implementation Steps

1. `pkg/protocol` events: types, validation, codec, fixtures, ID derivation, canary/reflection tests. (Independent.)
2. `internal/events`: `Sink`, bounded publisher, drop accounting, metrics, `eventstest` (recording sink, in-memory broker); Observer mapping and sequence tests against the prep plan's Observer. (Independent after the prep plan lands.)
3. `internal/config/events.go`, redaction and validation tests; `cmd/gateway` wiring and shutdown order.
4. `scripts/dev-kafka.sh`, the Redpanda container (SASL timebox), `internal/kafka` producer `Sink`; real-broker tests; the outage matrix; CI step and `quality.sh` stanza (so CI proves the broker path from the first push).
5. D8 spike and, if kept, the gateway token capture and `Completion` fields; overhead gate.
6. `migrations/0003`, `internal/postgres/usage.go`, `internal/usage` consumer logic against the in-memory broker and a real Postgres.
7. `internal/kafka` consumer `Source`, `cmd/usage-consumer`, `admin usage summary`; `TestUsageStopAndReplay` and the fault-injection tests; lag metric.
8. Overhead runs and benchmark note; ADR, schema reference, operations runbook (including the manual stop/replay and the broker-CLI offset reset), README and ARCHITECTURE; one Apache Kafka compatibility run; all CI steps locally; mutation checks; independent verification; fixes; narrower second verification; harden; `prepare-pr`; stop for approval.

Steps 1, 2 and 6 (logic against fakes) can proceed in parallel once the prep plan is merged; 4 and 7 need the broker.

**Local container note (verified):** this Mac runs Docker under Colima, which shares only the home directory with containers. A bind mount from `/tmp` or the macOS scratch directories appears EMPTY (a first Grafana provisioning check failed this way). Compose files and scripts must mount paths inside the repository, and nothing may rely on `/tmp`. Containers reach a service bound to the Mac's loopback through `host.docker.internal` (checked).

## Implementation Notes

Written after the work, 2026-10-10. Decisions D1 to D20 were implemented as approved except where listed under Deviations. Records of the
design: ADR-019, `docs/events/inference-lifecycle-v1.md`, `docs/operations/kafka-and-usage.md`, `docs/benchmarks/phase-12-kafka-events.md`.

### What was built

`pkg/protocol/events.go` (contract, codec, validation, deterministic IDs, five golden fixtures); `internal/events` (Observer, bounded
publisher, `Sink`, `eventstest` with recording, blocking, full, failing and slow sinks, an in-memory broker and store);
`internal/config/events.go`; `internal/kafka` (producer, consumer, admin helpers, health, `kafkatest`); `internal/usage` (consumer
loop); `internal/postgres/usage.go` and `migrations/0003_usage_records.sql` (table, rejects, `usage_hourly` view); `cmd/usage-consumer`;
`serverflow-admin usage summary`; `scripts/dev-kafka.sh`; the token scanner in `internal/gateway/tokens.go`; CI, `quality.sh` and
Makefile wiring.

### Acceptance criteria to evidence

| # | Evidence |
| --- | --- |
| 1 Schema and mapping | `TestEventSequencesThroughAStaticGateway` (non-stream, stream, error, drop, unavailable, timeout, client leaves, four refusals emit nothing), `TestEventsAfterARetryListEveryAttemptInOrder` (registry retry; one terminal per request), `TestTerminalClassification`, `TestRefusedRequestsEmitNothing`. Authentication and rate-limit refusals are covered at observer level (the gateway sequence for them is pinned by `internal/gateway/observer_test.go`); a gateway-level test of those two needs Postgres or Redis wiring and was not added |
| 2 Fixtures and evolution | `TestFixturesWrittenOnceAndDecodeUnderCurrentCode`, `TestUnknownFieldsAreIgnoredAndNewerVersionIsRejected`, `TestValidateRefusals`, consumer `TestPoisonRecordsAreRejectedAndTheStreamKeepsMoving` |
| 3 Event ID | `TestEventIDDeterministicAndDistinct` (5,000 random inputs), `Validate` refuses a wrong or missing ID |
| 4 Privacy canary | `TestNoPromptResponseOrKeyReachesAnEvent` (encoded bytes, `%v %+v %#v`, logs at debug), `TestEventTypesHoldOnlyPrimitives` (reflection) |
| 5 Never blocks | `TestStuckSinkNeverBlocksAndDropsAreCounted` (1,000,000 calls, depth, goroutines, drop count equals metric, bounded Close), `TestFullBufferDropsTheNewestAndKeepsTheOldest` |
| 6 Outage matrix | `TestBrokerOutageDoesNotBlockAndDropsAreCounted` (private broker: crash, restart, freeze; max Enqueue 0.36 ms; every event delivered or counted; one outage line and one recovery line; goroutines back to baseline), `TestMissingTopicDropsAreCountedAndNothingBlocks`, `TestProducerStartsWithoutABrokerAndBoundsItsBuffer`, `TestProcessGatewayStartsWithKafkaDownAndStopsOnTime`. "Slow" is covered by the frozen broker and `SlowSink`; a broker that is slow but alive with network latency was not run |
| 7 Shutdown | `TestProcessGatewaySIGTERMFlushesTheTerminalEventsOfInflightRequests` (five in-flight streams, all five `completed` events delivered; mutation: skipping `Close` fails it), `TestCloseFlushesEverythingAccepted`, stuck-sink Close bound, `TestProcessGatewayStartsWithKafkaDownAndStopsOnTime` |
| 8 Delivery semantics | `TestProducerDeliversToRealBroker` (all events received, key = request ID, one partition per request, in order), `TestUsageCrashBetweenDatabaseAndOffsetCommit`, `TestReplayAndDuplicatesAreAbsorbed` |
| 9 Consumer correctness | `internal/usage` tests (only terminal events, commit after DB, poison, bad row isolated, DB failure pauses and recovers, shutdown finishes the batch), `TestConsumerGroupCommitsAfterDatabase` (real broker), Postgres tests |
| 10 Stop / replay | `TestUsageStopAndReplay`: 50 rows, consumer stopped, 200 more sent, rows still 50, lag 600 events (200 requests), restart inserts exactly the 200, lag 0, fresh-group replay reads 250 duplicates and totals are identical. **10 consecutive runs passed** (1.05 to 1.32 s each). Runbook section in `docs/operations/kafka-and-usage.md`; the manual broker-CLI offset reset (`rpk group seek --to-start`) is documented but was not run by hand beyond the fresh-group variant |
| 11 Migration | `TestMigration0003CreatesUsageTablesAndTheViewMatchesDirectSums`, `TestInsertUsageIsIdempotentAndUniqueOnRequest`, `TestInsertUsageReportsBadDataAndLeavesNothingBehind`, `TestRecordRejectsIsRepeatable`, `migrations` and `internal/postgres` migration tests (checksum, idempotence) |
| 12 Config and secrets | `internal/config/events_test.go` (off ignores a malformed section, validation table, redaction under every verb, slog and JSON, env overrides, errors never echo values), `internal/kafka` `TestConfigNeverPrintsThePassword`, `TestHostileBrokerCannotMakeTheClientEchoThePassword` (a fake broker echoes the password in its SASL error; mutation: without scrubbing the test fails), process tests check the logs |
| 13 Metrics | `metric()` assertions in `internal/events` tests; `TestProcessGatewayStartsWithKafkaDown...` reads `event_publish_failures_total` from `/metrics`; `TestProcessUsageConsumerEndToEnd` reads `usage_consumer_records_total`; `consumer_lag` is exposed and `Consumer.Lag()` is asserted to be 0 after commit. A test asserting the lag metric while the consumer is stopped is not separate: the stopped-state lag is asserted with the broker's own group offsets |
| 14 Overhead | `docs/benchmarks/phase-12-kafka-events.md`: events up/down/frozen within the spread of off (+10 to +30 us non-stream, +110 to +170 us streaming TTFT when up), Observer 2.0 us and 35 allocs per lifecycle, scanner 130 ns and 0 allocs per chunk, token capture indistinguishable from master in five alternating runs |
| 15 Build hygiene | `go.mod` still `go 1.25.0`; `go mod tidy -diff` clean; `CGO_ENABLED=0`-style cross-compiles pass; the gate (below) ran once to green with Postgres, Redis and Kafka up |

### The gate

`scripts/quality.sh full` under the full-suite lock with PostgreSQL 16, Redis 7 and Redpanda up: gofmt, go vet, golangci-lint (0 issues),
`go test ./...`, `go test -race ./...`, `integration` (postgres 4 named tests, redis 7, **kafka 8 named tests ran and passed, nothing
skipped**), build with four cross-compiles, `go mod verify`, `go mod tidy` unchanged: exit 0 at 22:32 on 2026-10-09. Two earlier attempts
did not count: one hit ephemeral port exhaustion right after the benchmarks (unrelated packages, `can't assign requested address`), one was
killed from outside; a third found a flaky start-up assumption in my retry-events test (fixed: wait until the gateway routes a request).
`govulncheck` is not installed here and was not run.

### Deviations from the plan

- `events.batch_max_bytes` (default 1 MiB) replaces `batch_max_records`: the client has no record-count cap.
- `AllowIdempotentProduceCancellation` is set on the producer. Without it the client never fails a record whose request may have reached the
  broker, so a dead broker kept 100 buffered records pending for over 40 seconds against a 4 second delivery timeout. See ADR-019.
- Metrics are registered through a new option `gateway.WithExtraCollectors` because each gateway Server owns a private registry.
  Phase 10's registry changes may fold this in.
- Per-request observer state is on the request context (no map). `NewObserver` takes no registerer; `Collectors()` is used instead.
- The terminal event's model is the gateway-confirmed one only; `received` carries the model as asked (bounded).
- `Admission.APIKeyID` and `Completion.TokensSource/InputTokens/OutputTokens` were added (additive).
- The admin helpers in `internal/kafka` (`Admin`: create and delete topic, partitions, group lag, record count, produce) exist for tests and
  runbook tooling; the gateway never creates topics.
- The plan named the benchmark file `phase-12-events-overhead.md`; it is `phase-12-kafka-events.md`.
- `TestEventsOverhead` and the per-test private broker use Docker; both skip without it.

### Verification beyond the tests

- **Mutation checks** (one change each, a named test fails): block on a full buffer; drop the oldest uncounted; zero the event ID; retype
  `ttft_ms`; add a `Prompt` field to an event (reflection and fixture tests fail); write usage for non-terminal events; swallow undecodable
  records; commit offsets before the database write; remove `ON CONFLICT`; skip `Close` at shutdown (needed a long linger in the test to be
  caught, now fixed); key by event ID instead of request ID; skip the password scrub. All 12 were killed. Not run: "flush before the HTTP
  server drains" (the process test depends on the order but no separate mutation was applied), "pass an unredacted password into an error"
  beyond the scrub mutation.
- **Apache Kafka compatibility** (one-off, `apache/kafka:3.9.1`, KRaft, SASL/PLAIN, pulled from Docker Hub without rate limiting): the
  producer, consumer, unauthenticated-refusal, missing-topic, end-to-end, stop/replay, crash, SIGTERM-flush and usage-consumer process
  tests all passed. The outage test was not run against it (it uses a private Redpanda container). SASL/SCRAM against Apache's own
  implementation was not exercised; the Apache run used PLAIN.
- **CI image (corrected in the fix round)**: the first version of this note said `docker.redpanda.com` avoids Docker Hub. It does not: it
  proxies Docker Hub authentication and the anonymous limit applies (see `docs/development/ci.md` for the evidence and the checked
  alternatives). CI now caches the image and pulls with retries; a cold pull from a GitHub runner has not been run. The CI job uses the same script,
  port 59092 and `docker logs` on failure.
- No Apache Kafka job was added to the nightly workflow (open question D17 answered "no").

### What could not be verified

Multi-broker clusters; TLS to a real managed Kafka; SASL/SCRAM on Apache Kafka; rebalancing with more than one consumer; real vLLM
response shapes for token capture (Phase 13; the scanner is tested on the OpenAI format and the mock worker; a server that puts `usage`
before the choices in a non-streaming body falls back to `estimate`); the GitHub-hosted runner pulling the Redpanda image; behaviour under
the Go 1.25 toolchain itself (built and tested with 1.27.1; CI uses the `go.mod` version); `govulncheck` (not installed on this machine,
CI runs it).

### Risks left

- A hard kill, OOM or host crash loses the unsent buffer and that loss is not counted.
- Usage is undercounted by drops and is not billing grade; the `tokens_source` label must be respected by anything that reads the table.
- The stop/replay and outage tests run against Redpanda in CI; Redpanda is not Kafka, and only the one-off Apache run above covers the difference.
- franz-go is pinned at v1.21.7 below the version that would raise the repo's Go floor to 1.26.
- Ephemeral port exhaustion: a full-suite run right after the overhead benchmarks hit `can't assign requested address` in unrelated
  packages until TIME_WAIT sockets drained (about two minutes); reruns passed.
