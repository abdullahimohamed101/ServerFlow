# ADR-015: Distributed Rate Limiting on Redis, Leases, Estimates and Failure Behaviour

Status: Accepted (Phase 8)
Date: 2026-10-09

## Context

Phase 9 stores each tenant's `requests_per_minute`, `tokens_per_minute` and `max_concurrent_requests` and carries them in the
request's `Principal`. Nothing enforced them. Spec sections 16-18, 36 and 46 ask for distributed rate limiting on Redis
(token bucket, atomic), a request cost estimate of `input_tokens + max_tokens`, and a failure policy that fails closed for rate
limiting. The point of the phase: several gateways share one quota, and what happens when Redis is slow or down is decided in advance.

## Decision

- **Off by default.** `rate_limit.mode` is `off` or `required`. Off, the gateway never touches Redis and behaves exactly as in
  Phase 9. `required` needs a reachable, authenticated Redis at startup (the process exits otherwise) and, for tenant quotas,
  `auth.mode: required` (quotas need an identity). With auth off, only per-model caps apply; a `required` configuration with no
  tenant auth and no model caps is refused as having nothing to enforce.
- **Token buckets, one atomic Lua script per request.** Requests/min, tokens/min, concurrency and an optional per-model requests/min
  cap are checked in one script call. Either every quota admits the request and all are charged, or none is charged: a request
  refused for tokens does not spend a request token or a concurrency slot. A bucket holds `quota * burst_seconds / 60` tokens
  (default 60 s: one minute of quota) and refills continuously at `quota / 60` per second. New buckets start full.
  All arithmetic is on integers (levels are scaled by 60,000, so a refill of `e` ms adds `e * quota`); quotas are capped at 10^9 and
  bursts at 3,600 s so every intermediate stays exactly representable in a Lua double, and numbers are written with `%.0f` because
  Lua's default conversion keeps only 14 digits. `internal/ratelimit/model.go` is the same algorithm in plain Go; the tests run both on
  randomised sequences (with a fake clock) and require identical answers.
- **Redis `TIME`, never the gateway's clock.** Gateways with skewed clocks agree. Redis's own clock is the one thing the limiter
  trusts, and the phase found out what that costs: on Docker Desktop for Mac the Redis container's clock fell 14-45 s behind while the host
  slept and then jumped forward, and a bucket cannot tell a forward step from real idle time, so it credited the step as refill (our
  first benchmark admitted 2.4 times the quota). Nothing in the algorithm can fix that; what we did is bound it and keep the cases we can
  tell apart exact. Elapsed time is capped at the burst window, so a forward step mints at most one burst per bucket; a clock that
  steps back refills nothing (never removes tokens), and a bucket's stamp never moves backwards, so when the clock returns it credits
  only the real time since the last update. A clock that stays wrong in the past freezes refill for the size of the step (it fails towards
  refusing). Production Redis on a host with NTP does not do this; tests that depend on exact admission counts against Docker Desktop
  should run with the Mac awake (`caffeinate`) and the clock offset stable, and the benchmark note says how it was run. The only way to
  inject a clock is `ratelimit.Config.Clock`, which the configuration file cannot set.
- **Cost estimate (spec 17).** `input_tokens + max_tokens`: ASCII at about three bytes per token, every non-ASCII character as a whole
  token, a few framing tokens per message and for the reply. The reply part is the client's `max_tokens` (the larger of `max_tokens` and
  `max_completion_tokens` when both are present) or `gateway.max_tokens_limit` when the request names none. Every byte of the request body that is not already counted as message text (image parts, tool call arguments, the `tools` list, any other field) is
  charged at the same three bytes per token, so such content cannot be used to avoid the input charge (a base64 image is charged by its encoded size, which
  over-charges images). Bounded at 2^32, monotonic, not
  refunded after the response. A request bigger than the whole token bucket is charged the bucket's size (needs a full bucket).
  **Known limitation: this is an approximation, not an upper bound.** (1) The gateway forwards the body unchanged (ADR-003), so when the
  request has no `max_tokens` the backend may generate up to its own limit (vLLM defaults to the context length) while the tenant is
  charged `gateway.max_tokens_limit` (4096 by default): such a tenant can consume more backend capacity than its token quota suggests. The
  gateway does not rewrite the body to close this; the mitigations are the worker's own maximum length setting and Phase 15
  (admission control). (2) The input estimate is typically LOW for very dense text (digits, base64, hex, minified JSON and code can take
  1.5 to 3 characters per token); the 1 MiB request body cap is the outer bound on the input. (3) Tenants are charged at admission: a
  request that then fails (503 NO_CAPACITY, an upstream 5xx, an unknown model in registry mode) still consumed its request and token
  quota; nothing is refunded.
- **Concurrency as leases.** In-flight requests are members of a per-tenant sorted set scored by expiry. A gateway renews its own
  in-flight leases every `lease_ttl/3` (one call per tenant with leases, so a tenant's keys share a slot) and releases them when the
  handler ends; a crashed gateway's leases lapse within `lease_ttl`. Renewal never revives a released or expired lease, so a renewal
  that races a release cannot resurrect the slot; the price is that a lease that lapsed while Redis was unreachable for longer than
  `lease_ttl` is not restored for a request that is still running (the tenant may briefly exceed its limit by those requests). Release is
  asynchronous and never blocks the handler (bounded queue, four workers); when the queue is full, or Redis is backing off, a release is
  dropped and the lease expires by itself. The local lease map is bounded by `rate_limit.max_local_leases`; over the bound a request is
  refused as unavailable in both failure modes (the bound protects the gateway's memory, not quotas).
- **Where in the request.** authenticate, read and parse the body, allowed models, **limiter**, route. The slot is released by a `defer` in
  the chat handler, so success, errors, client disconnects and panics all give it back.
- **Rejections.** `429 RATE_LIMITED` with `Retry-After` in whole seconds (at least 1), computed from the bucket that failed (when several
  fail, the longest wait is reported; concurrency reports 1 s, a hint rather than a promise). The body names the limit type
  (`requests`, `tokens`, `concurrency`, `model`) and nothing about other tenants. `503 RATE_LIMIT_UNAVAILABLE` with `Retry-After` when the
  check could not be made.
- **Failure behaviour (spec 36).** `redis.on_failure`: **closed** (default) refuses requests that need a check with 503; **open** admits
  them, logs, and counts `rate_limit_bypassed_total`. Requests with nothing to enforce (all quotas 0, no model cap) never call Redis and
  are unaffected either way. Every call is bounded by `redis.timeout` (default 50 ms; a slow Redis counts as failed). Running out of the gateway's own connection pool (`redis.pool_size`, default 64) is *busy*, not an outage: no backoff and no outage log, and the request follows the failure mode for that one call. The driver does not retry a script on its own, because a script Redis already applied would be charged twice. After a failure
  Redis is left alone for `redis.backoff` (default 1 s) and only one probe is let through when the backoff ends, so a dead Redis costs one
  timeout per second, not one per request, and no goroutines or connections pile up. The outage and the recovery are each logged once.
  Closed protects quotas and turns a Redis outage into an API outage for limited tenants; open protects availability and lets quotas lapse.
  A call that timed out after Redis applied it leaves a lease that expires by itself.
- **Request metadata (spec 18, optional).** `redis.request_metadata: true` writes `request:{id}` = tenant, model, worker, attempt with
  `redis.request_metadata_ttl` (1 h) when a worker is chosen (and again on a retry). Best effort through a bounded queue; skipped while
  Redis backs off; never blocks or fails a request. Nothing reads it yet. Default off.
- **Key layout.** `rl:{<tenant>}:req|tok|conc` and `rl:model:{<model>}`. Names are percent-encoded (braces, `%`, non-printable ASCII), so a
  name can neither close the hash tag nor spell another key, and are limited to 256 bytes. Only models listed in
  `rate_limit.model_requests_per_minute` ever become keys, and only those names are ever put on the wire: for any other model the
  limiter sends a constant placeholder as the model key, so a client-chosen model name can neither create a keyspace entry nor make the
  command (and the time Redis and the gateway spend on it) grow with the name. A model name longer than 128 characters
  (`protocol.MaxModelLen`) is refused at parse time as MODEL_NOT_FOUND, which also bounds metrics labels and logs. (A first version
  escaped and sent the client's model name on every limited request; an independent review showed a 1 MiB name could trip the Redis
  timeout and arm the global backoff for every tenant.) Keys expire on their own (twice
  the burst window), so memory follows active tenants. **Redis Cluster is not supported:** the combined script touches tenant keys and a model
  key in one call, which Cluster refuses across slots.
- **Driver.** `github.com/redis/go-redis/v9`, confined to `internal/redis`. **v9.22.0** was chosen deliberately: v9.23.0 declares Go 1.26, and
  v9.22.0 is the newest release whose `go` directive (1.24) matches this repository's floor. New modules in `go.mod`:
  `github.com/redis/go-redis/v9 v9.22.0` and (indirect) `go.uber.org/atomic v1.11.0`; `go.sum` also lists the hashes of go-redis's own test
  dependencies. No existing module changed version.
- **Secrets and transport.** The password comes from config or `SERVERFLOW_REDIS_PASSWORD`. `RedisConfig` and `redis.Config` print it redacted
  under `%v`, `%+v`, `%#v`, slog and JSON; errors never echo the address (a URL may carry a password) and the client scrubs the password from
  driver errors. `redis.CheckTransport` asks the driver (`ParseURL` for a `redis://`, `rediss://` or `unix://` address; the options the
  driver would use otherwise) what the address resolves to, and for any host that is not loopback, `localhost` or a unix socket requires TLS
  **and** a password, unless `redis.allow_insecure_transport` is set. TLS is 1.2 or later with verification.
- **Test infrastructure.** Redis tests run when `SERVERFLOW_TEST_REDIS_ADDR` and `SERVERFLOW_TEST_REDIS_PASSWORD` are set and skip otherwise;
  CI sets them and fails if they skip. `scripts/dev-redis.sh` starts Redis 7 with `--requirepass` (Docker if available) exactly as CI does, and
  `redistest` refuses to build a client without a password; a test fails if any other file in the repository builds one directly.

## Consequences

- Redis is on the request path for tenants with quotas: one round trip per request (about 0.4 ms added at p50 locally; see
  `docs/benchmarks/phase-8-rate-limits.md`), bounded by a timeout and a backoff.
- Estimates are not usage: a tenant is charged `input + max_tokens`, so a large `max_tokens` spends quota faster than the model generates.
  A refund after the response is the natural follow-up.
- Unauthenticated traffic is **not rate limited** (a known gap: tenant quotas need an identity). Floods of it are still the job of a front proxy or per-IP limiter (ADR-014's advice stands).
- Quota changes take effect within `auth.cache_ttl` (the authenticator's cache); a bucket keeps its state and is clamped to a lowered quota.
- Redis 7 is used in CI and documentation (Redis 8 changed its license); Valkey is wire-compatible. The code uses plain commands and one Lua script.
- Out of scope: admission control and queues (Phase 15), priority tiers (Phase 22), per-tenant metric labels (Phase 10), usage records
  (Phase 12), a response cache, Redis Cluster/Sentinel.
