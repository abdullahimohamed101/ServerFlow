# Operating Redis and rate limits

Rate limiting is **off by default**. This page covers turning it on, what each setting does, what happens when Redis fails, and
how to run the tests. The design and its trade-offs are in ADR-015.

## Quick start (local)

```bash
make dev-redis                                  # throwaway Redis 7 on 127.0.0.1:56379 WITH a password, like CI's (DEV_REDIS_PORT to change)
export SERVERFLOW_REDIS_ADDRESS="$(scripts/dev-redis.sh addr)"
export SERVERFLOW_REDIS_PASSWORD="$(scripts/dev-redis.sh password)"
export SERVERFLOW_RATE_LIMIT_MODE=required SERVERFLOW_AUTH_MODE=required   # tenant quotas need identities (see postgres-and-auth.md)
go run ./cmd/admin tenant create acme --rpm 600 --tpm 100000 --max-concurrent 8
go run ./cmd/gateway
scripts/dev-redis.sh stop
```

Without authentication only per-model caps can be enforced:
`SERVERFLOW_RATE_LIMIT_MODE=required SERVERFLOW_RATE_LIMIT_MODEL_REQUESTS_PER_MINUTE=qwen-7b=300`.

`make test-redis` runs the Redis-backed tests against that instance. Without `SERVERFLOW_TEST_REDIS_ADDR` and
`SERVERFLOW_TEST_REDIS_PASSWORD` those tests skip; CI sets them and fails if any skip. The test Redis requires a password on purpose:
a test client that forgets it fails locally the way it would in CI. On Docker Desktop for Mac the Redis container's clock can step
while the Mac sleeps, which makes exact-count tests and the benchmark unreliable; keep the Mac awake (`caffeinate -dimsu go test ...`).

## Configuration

| Setting (env `SERVERFLOW_...`) | Default | Meaning |
| --- | --- | --- |
| `rate_limit.mode` (`RATE_LIMIT_MODE`) | `off` | `off` or `required`. `required` needs Redis at startup, and `auth.mode: required` for tenant quotas (or model caps). |
| `rate_limit.burst_seconds` | 60 | Seconds of quota a bucket holds (1-3600). 60 means a tenant may burst one minute's quota. |
| `rate_limit.lease_ttl` | 60s | How long a concurrency slot lives without renewal (10s-1h, at least 3x `redis.timeout`). A running request renews every third of it. |
| `rate_limit.max_local_leases` | 100000 | Slots one gateway tracks; over it requests get 503. |
| `rate_limit.model_requests_per_minute` (`RATE_LIMIT_MODEL_REQUESTS_PER_MINUTE=model=n,model=n`) | none | Global per-model request cap across all tenants and gateways. Works with auth off. |
| `redis.address` (`REDIS_ADDRESS`) | `redis:6379` | `host:port`, or a `redis://`, `rediss://` or `unix://` URL (a URL may carry a password and database). |
| `redis.password` (`REDIS_PASSWORD`) | none | A secret: never logged, echoed or printed; config prints it as `<redacted>`. |
| `redis.db`, `redis.tls` | 0, false | Database number; TLS 1.2+ with verification. |
| `redis.timeout` | 50ms | Bounds every Redis call. A slower Redis is treated as failed. |
| `redis.pool_size` | 64 | Connections to Redis (1-1000). Running out of them is the gateway's own congestion, not an outage: the request gets 503 (closed) or is admitted and counted (open), with no backoff and no outage log. |
| `redis.backoff` | 1s | After a failure Redis is left alone this long (one probe, then back to normal). |
| `redis.on_failure` | `closed` | `closed` or `open`; see below. |
| `redis.request_metadata`, `redis.request_metadata_ttl` | false, 1h | Best-effort `request:{id}` records (tenant, model, worker, attempt). Nothing reads them yet. |
| `redis.allow_insecure_transport` | false | Allow a Redis that is not this machine without TLS or without a password. |

Quotas themselves (`requests_per_minute`, `tokens_per_minute`, `max_concurrent_requests`, `0` = unlimited) are tenant settings
(`serverflow-admin tenant set-quota`). A change takes effect within `auth.cache_ttl` (30s). A tenant with all three at 0 costs no Redis round trip.

## What a client sees

- `429 RATE_LIMITED`, `Retry-After: N` (whole seconds, at least 1). The message names the limit (requests, tokens, concurrent requests,
  model) and nothing else. For requests and tokens the wait is exact: a client that waits it gets in (unless other traffic took the tokens).
  For concurrency it is a 1 s hint: a slot frees when some request finishes.
- `503 RATE_LIMIT_UNAVAILABLE`, `Retry-After`: Redis could not answer and the gateway fails closed (or this gateway holds too many leases).
- Cost is `input_tokens + max_tokens` (the larger of `max_tokens` and `max_completion_tokens` when both are sent; `gateway.max_tokens_limit`
  when neither is). It is charged at admission and **never refunded**: a request that then fails (503 NO_CAPACITY, an upstream 5xx, an
  unknown model in registry mode) still consumed its request and token quota. A request larger than the whole token bucket needs a full bucket.

## Known limits of the estimate and of the scope

- **Not an upper bound.** The gateway forwards the body unchanged, so a request without `max_tokens` is charged `gateway.max_tokens_limit`
  (4096 by default) while the backend may generate up to its own limit (vLLM defaults to the context length). Set the worker's own maximum
  length (for vLLM `--max-model-len`, and a default `max_tokens` where the backend supports one) to the same value; Phase 15 adds admission
  control.
- **Dense text is under-counted.** Input is estimated at three ASCII characters per token; digits, base64, hex, minified JSON and code can take
  1.5 to 3. The 1 MiB request body cap bounds the input.
- **Registry mode charges unknown models.** In `gateway.worker_source: registry` the registry decides which models exist, and the limiter runs
  before the router picks a worker, so a request for a model that turns out not to exist (404) has already consumed the tenant's request and
  token quota. Tenants with an `allowed_models` list are not affected (a forbidden model is refused before the limiter, and not charged).
  This was left as is on purpose: moving the limiter after routing would mean checking quotas after a worker slot has been reserved and
  then undoing the reservation on every refusal, a much bigger change to the scheduling path than this phase should make. Requests for
  non-existent models are client errors the tenant pays for; the per-tenant quota bounds the cost.
- **Unauthenticated traffic is not rate limited.** Tenant quotas need an identity (ADR-014's front-proxy or per-IP limiter advice stands).

## When Redis fails

| | `closed` (default) | `open` |
| --- | --- | --- |
| Requests that need a check | `503 RATE_LIMIT_UNAVAILABLE` | admitted unchecked; `rate_limit_bypassed_total` counts them |
| Requests with nothing to enforce | unaffected | unaffected |
| Quotas | protected | lapse during the outage |
| Cost of a dead Redis | one timeout (`redis.timeout`) per `redis.backoff`, then instant answers | the same |
| Logs | one line when the outage starts, one when it ends (also in open mode) | same, plus `rate_limit_bypassed` on each request line |

Recovery is automatic. Concurrency slots taken before an outage lapse after `lease_ttl`; a request still running when its lease lapsed
(an outage longer than `lease_ttl`) is not counted again, so a tenant can briefly exceed `max_concurrent_requests` by those requests.
A gateway that dies holding slots frees them within `lease_ttl`.

Choose `closed` when quotas protect money or shared capacity, `open` when availability matters more; both are tested.
At startup in `required` mode an unreachable Redis, a wrong password, or an unsafe transport stops the gateway with a clear message.

## After an outage, and at shutdown

- **Slots whose release was dropped.** A release is sent asynchronously through a bounded queue and is dropped when the queue is full or Redis
  is backing off. Such a lease is no longer renewed, but it holds its slot until it expires, up to `lease_ttl` (60 s by default). So for up
  to a minute after Redis recovers a tenant with a small `max_concurrent_requests` can see `429` for `concurrency` although nothing of its is
  running. `rate_limit_dropped_releases_total` counts the drops and `rate_limit_local_leases` the leases a gateway tracks; lower
  `rate_limit.lease_ttl` (minimum 10 s) to shorten the window.
- **A sub-millisecond race.** Release happens after the response is written, asynchronously. A client that fires its next request the moment
  it receives a response can arrive before the release script runs and get a spurious `429` (concurrency) when it is exactly at its limit.
  Retrying after the `Retry-After` second succeeds.
- **Shutdown.** When shutdown begins the renewer and the release workers stop; the gateway then drains in-flight requests (up to
  `gateway.shutdown_timeout`) and flushes the queued releases for up to 5 s. A long stream that outlives `lease_ttl` during a drain loses its
  lease (its slot may be given to another request) because it is no longer renewed. Leases of requests that finish are released.
- **Clock steps.** The limiter trusts Redis's clock (ADR-015). After a step *back* a bucket whose stamp is ahead refills nothing until the
  clock catches up or the key expires (at most twice the burst window), and `Retry-After` can then be too short. A step *forward* credits
  refill (at most one burst per bucket).

## Security notes

- Keep Redis private: no public Redis (spec section 44). A remote Redis needs TLS and a password, or `allow_insecure_transport`.
- The password is only in config or `SERVERFLOW_REDIS_PASSWORD`, never a flag. Prefer an ACL user limited to the keys and commands used. The
  scripts run as the connecting user, so every command *inside* them needs permission, not just `EVAL`. Verified against a real ACL user:

  ```text
  ACL SETUSER serverflow on >PASSWORD ~rl:* ~request:* -@all +evalsha +eval +time +hmget +hset +pexpire +zremrangebyscore +zcard +zadd +zscore +zrem +set +hello +ping
  ```

  (`rl:*` are the limiter's keys, `request:*` the optional metadata, which needs `+set`; add `+select` if `redis.db` is not 0.) `+hello` is optional (the client falls back without it); a missing `+eval` only shows after a `SCRIPT FLUSH` or restart. A missing command
  does not fail loudly: the script errors, which the gateway treats as Redis being unavailable (503 in closed mode).
- Tenant and model names are escaped in keys; only models named in `model_requests_per_minute` become keys.
- This limits authenticated tenants. Floods of unauthenticated requests still need a front proxy or per-IP limiter.
- **Memory.** Keys are small and expire on their own (twice the burst window), so memory follows active tenants. Do not run this Redis with
  `maxmemory-policy noeviction` at its limit: a write that cannot be stored makes the script fail with OOM, and tenants without a concurrency limit get
  503 in closed mode (open mode: their limits lapse). Tenants with `max_concurrent_requests` can keep working under OOM, because their script
  does a write first and Redis then skips the OOM check for the rest of it, but after any failure the limiter's backoff briefly returns 503 to everyone. Do not use `allkeys-lru`/`allkeys-random` either: evicting limiter keys resets those
  buckets and leases to full, so quotas silently reset. Give Redis headroom (set `maxmemory` well above the working set) and, if you must cap
  it, use a `volatile-*` policy: every limiter key has a TTL, and keys without one (other applications sharing the instance) are never evicted.
  Prefer a dedicated instance. Alert on `evicted_keys` and on `rate_limit_rejections_total{limit="unavailable"}`.
- Redis Cluster is not supported (the script touches a tenant's keys and a model key together). Redis 7 is tested; Valkey is wire-compatible.

## Metrics

`rate_limit_rejections_total{limit}` (`requests`, `tokens`, `concurrency`, `model`, `unavailable`), `rate_limit_bypassed_total`,
`rate_limit_decision_seconds`, `rate_limit_local_leases` (gauge: leases this gateway tracks) and `rate_limit_dropped_releases_total`
(releases not sent; see above). No tenant labels (cardinality is Phase 10's decision); the request log line has `tenant_id`, `rate_limit`
(the limit hit) and `est_cost`.
