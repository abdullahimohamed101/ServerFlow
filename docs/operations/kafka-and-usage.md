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

The summary reads the `usage_hourly` view, one line per tenant, model and `tokens_source` (never summed across sources).

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
   `scripts/dev-kafka.sh rpk group seek serverflow-usage --to-start`.
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
