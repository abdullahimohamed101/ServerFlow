# Lifecycle events, Kafka and the usage consumer

What runs, how to configure it, how to see that it works, and what to do when it does not. Design and guarantees: ADR-019. Event
schema: `docs/events/inference-lifecycle-v1.md`.

## What it does

With `events.mode: on` the gateway publishes small content-free events about every admitted inference request to the Kafka topic
`inference.lifecycle.v1`. The `usage-consumer` binary reads the terminal events (`completed`, `failed`) and writes one row per
request to PostgreSQL (`usage_records`). Kafka is never on the request path: with the broker down, slow or full, clients get the same
responses at the same latency, and the lost events are counted.

## Configuration

Section `events` (environment overrides use `SERVERFLOW_EVENTS_...`, for example `SERVERFLOW_EVENTS_SASL_PASSWORD`):

| Key | Default | Meaning |
| --- | --- | --- |
| `mode` | `off` | `on` publishes from the gateway. Other keys are ignored (not even validated) while off |
| `brokers` | none | `host:port` list, no scheme or credentials |
| `topic` | `inference.lifecycle.v1` | create it yourself (below); the gateway never creates topics |
| `tls`, `tls_ca_file` | off | TLS to the brokers (TLS 1.2 or later) |
| `sasl_mechanism`, `sasl_username`, `sasl_password` | `none` | `plain`, `scram-sha-256`, `scram-sha-512`. The password is redacted everywhere it could be printed |
| `allow_insecure_transport` | off | permit a broker that is not this machine without TLS or SASL (a trusted private network only) |
| `compression` | `snappy` | `none`, `snappy`, `lz4`, `zstd`, `gzip` |
| `buffer_size` | 10000 | in-memory events between the request path and the producer; full means drop the newest |
| `max_buffered_records` | 10000 | the producer's own bound |
| `linger`, `batch_max_bytes` | 50ms, 1 MiB | batching |
| `delivery_timeout` | 30s | an event not acknowledged by then is dropped and counted |
| `shutdown_flush_timeout` | 5s | flush on SIGTERM; must not exceed `gateway.shutdown_timeout` |
| `consumer.group_id` | `serverflow-usage` | |
| `consumer.start_offset` | `earliest` | for a group with no committed offset: `earliest` or `latest` |
| `consumer.batch_size`, `batch_timeout` | 500, 1s | one batch is one database transaction |
| `consumer.metrics_addr` | `127.0.0.1:9103` | `/healthz` and `/metrics`; must be loopback unless `allow_insecure_transport` |

The memory ceiling of the gateway side is `buffer_size` times the 16 KiB event cap in theory and a few MB in practice.

### Create the topic (production)

```bash
rpk topic create inference.lifecycle.v1 -p 6 -c retention.ms=604800000 -c cleanup.policy=delete
# Apache Kafka: kafka-topics.sh --create --topic inference.lifecycle.v1 --partitions 6 --config retention.ms=604800000
```

Seven days of retention is the replay window and the longest outage the consumer can recover from. Partition count and replication are
production sizing (Phase 16). A missing topic is a delivery failure that is counted and logged once per outage, never a crash.

### Apply the migration

```bash
serverflow-admin migrate up      # applies 0003_usage_records.sql
serverflow-admin usage summary --since 24h [--tenant NAME]
```

The summary reads `usage_records` directly and is exact to the instant: `--since 24h` is the last 24 hours to the second, `--since` also
takes an RFC 3339 time. It prints one line per tenant, model and `tokens_source` (never summed across sources); token sums are exact
decimal numbers. The `usage_hourly` view holds the same data bucketed by UTC hour for reports and dashboards (an hour is counted whole).

### Migration, deploy order, rollback

`0003_usage_records.sql` is forward-only: there is no down migration, and the runner refuses to edit an applied file. It only adds three
objects (`usage_records`, `usage_rejected_events`, the `usage_hourly` view), so the old binaries ignore them, **but an older binary refuses
a database that is ahead of it**: a gateway built before this phase with `auth.mode: required` checks the migration list at start and
stops with `migrate: the database is at version 3 but this binary only knows 2 migrations; run a newer serverflow-admin`. With
`auth.mode: off` the gateway never opens the database and is unaffected. Therefore:

1. Roll the gateways to a build that includes migration 0003 (events stay off by default) **before** running `serverflow-admin migrate up`,
   or run the migration in the same change window and expect older gateways with authentication on to refuse a restart until upgraded.
2. Start `usage-consumer` after the migration (it refuses to start on an unmigrated database).
3. Rolling back the code after the migration is allowed only to builds that know migration 0003. To remove the feature, stop
   `usage-consumer` and set `events.mode: off`; the tables stay (drop them by hand if you must; nothing else references them).

### Retention of usage rows

Nothing deletes from `usage_records` or `usage_rejected_events`: there is no retention job. At about 250 bytes of heap plus two indexes per
request, a million requests a day grows the table by roughly 0.5 GB a month. Clean up by hand, for example:

```sql
DELETE FROM usage_records WHERE occurred_at < now() - interval '180 days';
DELETE FROM usage_rejected_events WHERE rejected_at < now() - interval '30 days';
```

Run it in batches off-peak. Deleting rows older than the topic's retention is safe; deleting newer rows and then replaying the topic
brings them back (the insert is idempotent), which can be useful and can be a surprise.

### Least-privilege database role for the consumer

`usage-consumer` needs to read the migration table (its startup check) and to insert into two tables; it never updates, deletes or reads
anything else. Create a role like this (the grants are tested in `internal/postgres/usage_grants_test.go`):

```sql
CREATE ROLE usage_consumer LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA public TO usage_consumer;
GRANT SELECT ON schema_migrations TO usage_consumer;
GRANT INSERT ON usage_records, usage_rejected_events TO usage_consumer;
```

`serverflow-admin usage summary` is run with the admin role (it reads `usage_records`); give a reporting role `GRANT SELECT ON usage_records`.

## Event ID

`event_id = "evt_" + first 32 hex characters of SHA-256(request_id || 0x00 || event_type || 0x00 || attempt_id)`: the three strings are joined by
single NUL bytes (so `("ab","c")` and `("a","bc")` differ), `attempt_id` is empty for `received`. Reference (any language works):

```go
sum := sha256.Sum256([]byte(requestID + "\x00" + eventType + "\x00" + attemptID))
id := "evt_" + hex.EncodeToString(sum[:])[:32]
```

Test vector: request `req_0123456789abcdef`, type `inference.request.completed`, attempt `att_1111111111111111` gives
`evt_fb773ebffce17706505c495fc4412a31` (`TestEventIDTestVector`).

## Behaviours worth knowing

- **`delivery_timeout` is approximate.** The client fails a whole batch for a partition when the first record's timeout passes and only
  checks timeouts before sending a request and after a reply, so with a frozen broker batches fail tens of seconds apart (30 to 80 s
  observed with a 30 s setting) rather than record by record. Memory stays bounded throughout (the two buffers), and every record is
  either delivered or counted.
- **The event shape is nested on purpose.** The spec's example is a flat object; the implementation puts the type-specific fields under
  `payload` so the envelope is identical for every type and unknown payload fields are additive. Consumers should read the schema
  reference, not the spec example.
- **An unknown model after admission** (registry mode: the request was admitted, then the registry did not know the model) produces
  `received` and a `failed` event with `failure_class: internal` (the gateway's error is a 404-class refusal that the closed enum does not
  name) and no model.
- **`consumer_lag`** is the broker's committed offsets against partition end offsets, summed, refreshed every 5 seconds; a stopped
  consumer process exports nothing, and a refresh that fails (broker unreachable) keeps the last value. A new group with no commit shows the
  whole retained log until its first commit.

## Metrics

Gateway (on its `/metrics`): `events_published_total`, `events_buffer_depth`, `event_publish_failures_total{reason}` with reason
`buffer_full`, `producer_full`, `delivery_failed`, `encode`, `shutdown`, and `event_publish_latency_seconds`. Consumer: `consumer_lag{group,topic}`,
`usage_consumer_records_total{result}` (`inserted`, `duplicate`, `skipped`, `rejected`), `usage_consumer_db_errors_total`. Alert on a rising
`event_publish_failures_total` (usage is being undercounted) and on `consumer_lag` that does not fall.

## Local development

```bash
scripts/dev-postgres.sh start && scripts/dev-kafka.sh start        # Postgres 16 and Redpanda (SASL/SCRAM), throwaway
export SERVERFLOW_POSTGRES_DSN="$(scripts/dev-postgres.sh dsn)"
go run ./cmd/admin migrate up
export SERVERFLOW_EVENTS_MODE=on SERVERFLOW_EVENTS_BROKERS="$(scripts/dev-kafka.sh brokers)" \
  SERVERFLOW_EVENTS_SASL_MECHANISM=scram-sha-256 SERVERFLOW_EVENTS_SASL_USERNAME="$(scripts/dev-kafka.sh user)" \
  SERVERFLOW_EVENTS_SASL_PASSWORD="$(scripts/dev-kafka.sh password)"
go run ./cmd/gateway &            # with a mock worker as the upstream (docs/development/mock-worker.md)
go run ./cmd/usage-consumer &
```

## Stop the consumer, send requests, start it, replay (the Phase 12 demonstration)

The automated version is `TestUsageStopAndReplay`. By hand, with the setup above and `GROUP=serverflow-usage`:

1. Send 50 requests; wait until `serverflow-admin usage summary` shows 50 requests.
2. Stop the consumer (Ctrl-C or SIGTERM: it finishes the batch in hand and commits what is durable).
3. Send 200 more. The summary still says 50. Lag is visible with the broker CLI:
   `scripts/dev-kafka.sh rpk group describe serverflow-usage` (lag is in events, three per request).
4. Start the consumer. It reads the backlog; the summary reaches 250 and the group lag reaches 0.
5. Replay: stop it and either start it with a new group (`SERVERFLOW_EVENTS_CONSUMER_GROUP_ID=replay-1`, `start_offset` earliest) or
   reset the real group's offsets while it is stopped:
   `scripts/dev-kafka.sh rpk group seek serverflow-usage --to start --topics inference.lifecycle.v1` (the flag is `--to start`, two words; verified
   by hand on Redpanda 25.1.1: `--to-start` is an unknown flag, and a group that still has a live member is refused with
   `INVALID_OPERATION: seeking a non-empty group is not allowed`, so stop every consumer of the group first). Apache Kafka's equivalent is
   `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute --group serverflow-usage --topic inference.lifecycle.v1`.
   Start the consumer again. `usage_consumer_records_total{result="duplicate"}` rises by the number of terminal events read and the
   summary does not change, because the insert is idempotent on `event_id` and `request_id`.

## When things go wrong

| Symptom | Meaning and action |
| --- | --- |
| Gateway log: `kafka is unreachable` | brokers cannot be reached; requests are unaffected; events are buffered then dropped after `delivery_timeout`. Logged once per outage, with `kafka connection recovered` once after |
| `event_publish_failures_total{reason="buffer_full"}` rises | the producer cannot keep up or is stuck; usage is undercounted by this amount |
| `{reason="delivery_failed"}` rises, topic missing | create the topic; check credentials (the error never contains the password) |
| `usage_consumer_db_errors_total` rises | PostgreSQL unreachable; the consumer pauses, commits nothing, and recovers by itself (logged once each way) |
| `usage_rejected_events` has rows | records the consumer could not use; they hold coordinates and a reason only. Look at the record on the topic with the broker CLI |
| Lag grows with the consumer running | database slow or down, or too few partitions per consumer; see the DB metric above |

Hard limits, stated plainly: SIGKILL, an OOM kill or a host crash lose the unsent buffer, and that loss is not counted. Rejected
requests (authentication, rate limit, validation) have no events. Usage is best-effort accurate, not billing grade.

## Broker choice

The development and CI broker is Redpanda (Kafka API compatible, not Apache Kafka; BSL community edition). Production may use any Kafka
API broker. The compatibility run against Apache Kafka 3.9.1 is recorded in the Phase 12 plan's implementation notes.
