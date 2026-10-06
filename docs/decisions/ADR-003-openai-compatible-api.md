# ADR-003: OpenAI-Compatible Public API, Internal Normalization

Status: Accepted
Date: 2026-10-05

## Context

ServerFlow's gateway is the only client-facing surface. Clients, SDKs, and
benchmark tools already speak the OpenAI HTTP API, and vLLM exposes the same
shape. Spec §9 requires the public API to resemble OpenAI's while keeping the
system's internals independent of OpenAI-specific structures.

## Decision

- The gateway exposes OpenAI-compatible endpoints. Phase 2 implements
  `GET /v1/models` and `POST /v1/chat/completions` (stream and non-stream).
- Requests are validated and translated into a normalized internal
  `InferenceRequest` (`pkg/protocol`). Scheduling, admission, and cost
  estimation depend only on that type, never on OpenAI wire structs
  (`internal/api`).
- The client's original JSON body is forwarded to the upstream unchanged after
  validation, so fields the gateway does not model are not dropped.
- Because the validated bytes are the forwarded bytes, validation must read
  the body exactly as the upstream will. Go's JSON decoder matches keys
  case-insensitively and keeps the last duplicate; Python upstreams such as
  vLLM match exactly. The gateway therefore rejects (400) any body with a
  duplicate key, or with a key that differs from a validated field only by
  case (`Model`, `Max_Tokens`), and checks every token-limit field that is
  present, not just the first.
- Gateway-originated errors use an OpenAI-shaped body
  (`{"error":{"message","type","code"}}`) carrying the spec §51 error code.
  Upstream error responses received before streaming starts are passed through.
- Streaming uses Server-Sent Events, relayed without buffering.

## Alternatives

- **Custom API**: free to design for scheduling needs, but breaks existing
  SDKs and tools and adds client work for no benefit at this stage.
- **Pass raw OpenAI structs through the whole system**: least code now, but
  couples the scheduler and registry to one vendor's schema and blocks a
  compatible-but-different backend later.
- **Re-serialize the parsed request upstream**: normalizes everything, but
  silently drops unmodeled OpenAI fields and breaks compatibility as the API
  evolves.

## Consequences

- **Positive**: drop-in compatibility with OpenAI SDKs and vLLM; internals stay
  vendor-neutral; new OpenAI fields work without gateway changes.
- **Negative**: two representations of a request (wire and normalized) must be
  kept consistent; the gateway validates only the fields it models, so some
  invalid requests are rejected by the upstream instead. Forwarding the
  original bytes makes the gateway's limits only as strong as its parser
  agrees with the upstream's, hence the strict-key rule above. A request that
  sets no token limit is not capped by the gateway, since capping would mean
  rewriting the body.
- **Follow-ups**: `/v1/completions` is deferred (Phase 7 if needed); error
  classification for retries arrives with Phase 6.

## Status

Accepted. Recorded during Phase 2.
