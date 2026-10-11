# Inference lifecycle events, schema version 1

Topic `inference.lifecycle.v1`, one JSON event per Kafka record, key = `request_id` (all events of one request share a partition and
stay in order). The contract is `pkg/protocol/events.go`; golden examples are in `pkg/protocol/testdata/events/v1/`. Events are off by
default (`events.mode`). Why and how: ADR-019.

## Envelope

| Field | Type | Notes |
| --- | --- | --- |
| `event_id` | string | `evt_` + 32 hex, deterministic: SHA-256 of `request_id`, `event_type` and `attempt_id` joined by single NUL (0x00) bytes, first 32 hex characters. Same logical event, same ID. Reference code and a test vector: `docs/operations/kafka-and-usage.md` |
| `event_type` | string | one of the five types below |
| `schema_version` | int | `1` |
| `timestamp` | RFC 3339, UTC, nanoseconds | when it happened on the gateway |
| `source` | string | the gateway instance (`events.source`, default the host name) |
| `request_id` | string | `req_...` |
| `attempt_id` | string, optional | `att_...` the attempt concerned; absent for `received`; for the terminal event the last attempt |
| `tenant_id` | string, optional | `ten_...`; absent when authentication is off |
| `api_key_id` | string, optional | `key_...`, never the key or its prefix |
| `model` | string, optional | only ever a model the gateway has confirmed (configured in static mode, registered in registry mode). **Empty on `received` (the wire carries `"model":""`; consumers must treat empty as "no confirmed model")** (in registry mode the request is admitted before the model is matched, so it would be client text). Present on `routed`, `first_token` and the terminal event; the terminal event of a request that never reached a worker (for example an unknown model) has an empty model too. Older producers of this schema version put the client's string on `received`; consumers must treat that field as unverified |
| `worker_id` | string, optional | absent before routing |
| `payload` | object | by type. The spec's example is flat; the envelope/payload split is a deliberate choice so every type shares one envelope |

## Types and payloads

| Type | Emitted when | Payload |
| --- | --- | --- |
| `inference.request.received` | the request passed authentication, validation and the rate limit | `stream`, `estimated_cost_tokens` |
| `inference.request.routed` | an attempt started (once per attempt) | `attempt_number`, `strategy` |
| `inference.request.first_token` | the first streamed chunk was written (once per request) | `ttft_ms` |
| `inference.request.completed` | the request ended with a 2xx and no gateway error | terminal payload |
| `inference.request.failed` | anything else, including `499` when the client left | terminal payload plus `failure_class` |

Terminal payload: `stream`, `http_status`, `duration_ms`, `ttft_ms` (if any), `input_tokens` and `output_tokens` (absent when unknown),
`tokens_source`, `estimated_cost_tokens`, `attempts[]` (`attempt_id`, `number`, `worker_id`, `outcome`, `failure_class`, `duration_ms`),
and for `failed` a `failure_class` from the closed set `no_capacity`, `worker_unavailable`, `worker_error`, `timeout`, `client_closed`,
`internal`.

`tokens_source`: `usage` (the worker's own usage object: both counts), `chunks` (streamed content chunks counted: `output_tokens` only, an
approximation), `estimate` (nothing was reported: no counts, use `estimated_cost_tokens`). Never sum different sources as if equal.

Requests refused before admission (authentication, rate limit, validation, unknown model in static mode) produce no event. A request refused
after admission (registry mode: unknown model, no capacity) produces `received` and then `failed`.

## Never in an event

Prompts, messages, completions or any response text, request or response bodies and headers, API keys or secrets (not even a prefix),
client IPs, free-text upstream errors, credentials, configuration. Every string field is length-capped and validated; a test fails if a
string field is added without review.

## Delivery

At least once from the gateway's point of view only when the broker is healthy: events can be dropped (counted in
`event_publish_failures_total{reason}`) and a hard kill loses the buffer. The consumer must not require a complete sequence: a `completed`
without a `routed`, or a `routed` without a terminal event, is normal. Duplicates are normal too and harmless to an idempotent consumer.

## Evolution rules

1. Adding an optional field is compatible and does not bump the version. Consumers ignore unknown fields.
2. Removing, renaming, retyping or changing the meaning of a field is breaking: new `schema_version` and a new topic
   `inference.lifecycle.v2`; v1 keeps being produced until consumers move.
3. A field name is never reused for a different meaning; deprecated fields stay documented here.
4. A consumer meeting a `schema_version` newer than it supports records a reject (coordinates only) and moves on.
5. Fixtures are only ever added. The compatibility test decodes every committed fixture with the current code.
