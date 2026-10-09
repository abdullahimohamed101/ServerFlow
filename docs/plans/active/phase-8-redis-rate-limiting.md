# Phase 8 — Redis: Distributed Rate Limiting

Status: In progress (plan approved with all defaults; implementation under way)
Owner: coding agent
Depends on: Phase 9 (tenant quotas and API keys; PR #9) and Phase 6 (request path). Uses Phase 7 (the benchmark harness) for the acceptance run.
Spec: `docs/architecture/serverflow-spec.md` §16–18, §29, §36, §44, §45–46, §51, §58 Phase 8, §63
Hand-offs: Phase 9 stores `requests_per_minute`, `tokens_per_minute`, `max_concurrent_requests` per tenant ("0 = none configured") and carries them in the request's `Principal`; Phase 7's `multi-tenant` workload (one aggressive client, several normal ones) is the acceptance load; the Phase 9 operations guide listed "front limiter required" for unauthenticated floods, which this phase does not replace (it limits authenticated tenants).

## Outcome

Three gateways, one quota. A tenant's requests per minute, tokens per minute, and concurrent requests are enforced
across every gateway that shares one Redis, so an aggressive tenant cannot exceed its quota by spreading load, and
a well-behaved tenant is unaffected. A rejected request gets `429 RATE_LIMITED` with an honest `Retry-After`. What
happens when Redis is slow or down is a decision made in advance, tested, and written down.

```text
client ─ key ─► gateway A ─┐
client ─ key ─► gateway B ─┼─►  Redis  (one atomic script per request: requests/min + tokens/min + concurrency)
client ─ key ─► gateway C ─┘          ▲  quotas come from the tenant's policy (Postgres, cached by Phase 9's authenticator)
```

Acceptance headline (spec §58): *3 gateways share one quota; document failure behavior.* Measured with the Phase 7
harness: an aggressive tenant and several normal tenants spread over three gateways, then Redis made unavailable.

## Non-Goals

- No admission control or backpressure queues (spec §16, §27; Phase 15), no priority tiers or fair queuing
  (Phase 22), no per-tenant Prometheus labels (Phase 10 decides cardinality), no usage records (Phase 12).
- No response cache (spec §18 mentions a deterministic cache: "only when appropriate"; it is risky and not part of this
  phase's acceptance), and no routing-hint reads that change scheduling.
- No refund of unused estimated tokens after a response (charged on the estimate; documented as conservative).
- No limiter for unauthenticated traffic: tenant limits need identity, so `required` limiting needs `auth.mode=required`.
  A front proxy or per-IP limiter remains the answer for anonymous floods (Phase 9 documentation).
- No local fallback limiter and no Redis Cluster/Sentinel operation (the key layout is cluster-safe; running one is not tested).
- No change to the scheduler, registry, retry logic, or the benchmark harness code.
- Rate limiting is **off by default**; Phases 2–9 behave exactly as before.

## Current Architecture

- `internal/config` has `RedisConfig{Address}` (default `redis:6379`) and `AdmissionConfig{MaxGlobalRequests}`, both unused.
  Nothing talks to Redis; there is no Redis dependency in `go.mod` (only yaml, prometheus, pgx).
- Phase 9: `authenticate` puts a `*auth.Principal{TenantID, KeyID, Policy}` in the request info; `TenantPolicy` carries
  `RequestsPerMinute`, `TokensPerMinute`, `MaxConcurrent` (0 = none configured). The authenticator caches policy for
  `cache_ttl` (30 s), so a quota change takes effect within that time.
- The chat handler parses the body into an `InferenceRequest` (messages, `MaxTokens`), then routes. No token estimate
  exists in the gateway; the benchmark harness uses about 4 characters per token. `max_tokens` may be absent (ADR-003: the
  gateway does not rewrite bodies), and `gateway.max_tokens_limit` (default 4096) bounds what a client may ask.
- Errors: `RATE_LIMITED` is in spec §51 but not implemented. `api.Error` supports `Retry-After`.
- Local Redis: not installed (Homebrew has `redis` and the Docker daemon is running). The Postgres phase taught that a
  local test server must behave like CI's (password authentication), or tests pass locally and fail in CI.

## Decisions (confirm before implementation)

- **D1 Client library: `github.com/redis/go-redis/v9`.** This is the second external client dependency. Reasons: the
  standard library has no Redis client, hand-rolling RESP with pooling, reconnects, TLS and script caching is more risk than
  a small dependency, and go-redis is the maintained standard. It brings a couple of small transitive modules, all listed in
  the PR. All use stays inside `internal/redis`. Pin a version that builds with the module's Go 1.24 floor (check, as with pgx).
- **D2 Algorithm: token buckets, evaluated atomically in one Lua script per request.** Requests/min and tokens/min are
  buckets with capacity equal to the quota and a refill of quota per 60 s (burst = one minute's quota; configurable burst
  factor `rate_limit.burst_seconds`, default 60). Concurrency is a lease set. One script call checks **all** dimensions and
  consumes **none** unless all pass, so a request refused for tokens does not spend a request token. The script uses
  Redis `TIME` (never the gateway's clock) so gateways with skewed clocks agree.
- **D3 Concurrency as leases, not a bare counter.** In-flight requests are members of a per-tenant sorted set scored by
  lease expiry. A gateway that crashes cannot leak slots: leases expire (`rate_limit.lease_ttl`, default 60 s). A gateway
  renews its own in-flight leases every `lease_ttl/3` in one batched call, so a long stream keeps its slot, and releases on
  completion. A lost renewal for a live request only means its slot may be reclaimed early; documented.
- **D4 Cost estimate (spec §17: `input_tokens + max_tokens`).** Input tokens are estimated from the request's text (about
  four characters per token, rounded up, plus a small per-message overhead), a documented over-estimate. `max_tokens` is the
  client's value, or `gateway.max_tokens_limit` when absent (the worst case the gateway would allow). No ML, no
  refunds. The estimate is computed once, stored on the request, and logged.
- **D5 Quotas come from Phase 9's policy.** 0 means "not limited" for that dimension. A tenant with all three at 0 skips
  Redis entirely (no round trip). Quota changes apply within the authenticator's `cache_ttl`; the bucket keeps its state and
  is clamped to the new capacity.
- **D6 Order of checks.** authenticate → read and parse body → allowed models → **rate limit** → route. The limiter runs
  after the body is parsed (the token cost needs it) and before any worker is touched. The concurrency lease is released
  when the handler finishes, whatever the outcome (success, error, client disconnect, panic).
- **D7 The per-model limit (spec §18 `rate:model:{model}`).** An optional global requests-per-minute cap per model,
  configured (`rate_limit.model_requests_per_minute: {model: n}`), checked in the same script. It applies even with auth off.
  A model not listed has no model cap. Only registry-confirmed or statically configured models reach the limiter, so
  client-chosen names cannot create keys.
- **D8 Rejection.** `429 RATE_LIMITED`, `Retry-After` in whole seconds (at least 1) computed by the script from the
  bucket that failed, and an error body that names which limit was hit (`requests`, `tokens`, `concurrency`, `model`) and
  nothing about other tenants. Informational `X-RateLimit-*` headers on successful responses are a follow-up.
- **D9 Failure behaviour (spec §36: fail closed for rate limiting).** `redis.on_failure` is `closed` (default) or
  `open`. **Closed**: when Redis cannot answer within `redis.timeout` (default 50 ms), the request gets
  `503 RATE_LIMIT_UNAVAILABLE` with `Retry-After`. **Open**: the request is admitted, a warning is logged once per outage, and
  `rate_limit_bypassed_total` counts it. After a failure the limiter backs off for `redis.backoff` (default 1 s) so a dead Redis
  costs one timeout per second, not one per request, and recovery is logged once. A slow-but-alive Redis is treated as failed
  after the timeout. The default protects quotas; `open` protects availability; both are tested and documented.
- **D10 Request metadata (spec §18 `request:{id}:worker`, optional).** With `redis.request_metadata: true` the gateway writes
  `request:{id}` → tenant, model, worker, attempt, with a TTL (default 1 h) when a worker is chosen. Best effort: it never blocks
  a request, never fails one, and is skipped while Redis is backing off. Default off. Nothing reads it yet (debugging and the
  later sticky-routing work); the plan says so.
- **D11 Configuration.** `redis.address`, `redis.password` (secret: redacted like the Postgres DSN, never logged or echoed),
  `redis.db`, `redis.tls`, `redis.timeout`, `redis.backoff`, `redis.on_failure`, `redis.request_metadata`; and
  `rate_limit.mode` (`off` default | `required`), `burst_seconds`, `lease_ttl`, `model_requests_per_minute`. Validation: positive
  durations, `lease_ttl` at least 3× the renewal interval and at least 10 s, `required` needs a reachable Redis at startup
  (fail fast, like auth) and for tenant limits needs `auth.mode=required` (otherwise only model caps apply, and the config says so).
  A non-loopback Redis without TLS or a password is refused unless `redis.allow_insecure_transport` is set (the same guard as the
  Postgres DSN, using the driver's parsed options, never a hand-written parser).
- **D12 Redis key layout is cluster-safe.** Per-tenant keys share a hash tag: `rl:{<tenant_id>}:req`, `rl:{<tenant_id>}:tok`,
  `rl:{<tenant_id>}:conc`; the model cap is `rl:model:{<model>}`. Keys expire on their own (idle buckets are deleted after twice the
  refill period), so memory is bounded by active tenants.
- **D13 Metrics and logs.** `rate_limit_rejections_total{limit}` (spec §29; limit in `requests|tokens|concurrency|model|unavailable`),
  `rate_limit_bypassed_total`, `rate_limit_decision_seconds` histogram. No tenant label. Log lines carry `tenant_id`, `limit`, and the
  estimated cost. Never the API key or the Redis password.
- **D14 Test infrastructure, mirrored to CI from the start.** Database tests run when `SERVERFLOW_TEST_REDIS_ADDR` (and
  `SERVERFLOW_TEST_REDIS_PASSWORD`) are set and skip otherwise; CI must set them and fails if they skipped (the Phase 9 pattern).
  `scripts/dev-redis.sh` starts a throwaway Redis 7 **with a password** (Docker if the daemon is running, else a local
  `redis-server`), so a missing or wrong password fails locally the way it does in CI. CI gains a Redis 7 container started with
  `--requirepass`. Every test client passes the password; a test helper builds clients and a guard test fails if a client is created
  without one.
- **D15 Overhead budget.** One Redis round trip per limited request. Target: added gateway overhead below 5 ms p95 against a
  local Redis, measured with the Phase 7 overhead test or harness; no round trip for tenants with no quotas.
- **D16 Local concurrency is bounded too.** Leases are held in memory per gateway (a map of request ID to lease), capped by
  `rate_limit.max_local_leases` (default 100,000), so a bug or an attack cannot grow memory without bound; over the cap the
  request is refused as `RATE_LIMIT_UNAVAILABLE`.

## Proposed Design

```go
// internal/ratelimit
type Limits struct{ RequestsPerMinute, TokensPerMinute, MaxConcurrent int }       // 0 = unlimited
type Request struct{ TenantID, Model string; Cost int; Limits Limits }
type Decision struct{ Allowed bool; Limit string; RetryAfter time.Duration; Release func() }
type Limiter interface { Allow(ctx context.Context, r Request) (Decision, error) } // error = Redis unavailable
func NewRedis(client *redis.Client, cfg Config, clock ...) *RedisLimiter           // script, renewer, backoff
func EstimateCost(msgs []protocol.Message, prompt string, maxTokens, limit int) int
```

The gateway holds a `ratelimit.Limiter` behind an option (`WithLimiter`), mirroring `WithAuthenticator`: required mode with no limiter
cannot serve. The handler calls it after parsing, maps `Decision`/`error` to the response per D8/D9, and defers `Release`. The
Redis implementation lives in `internal/redis` (client construction, TLS, the transport guard, redaction) and `internal/ratelimit`
(script, estimation, renewal, backoff). A fake `Limiter` drives the gateway tests without Redis.

## Affected Files / Components

New: `internal/redis`, `internal/ratelimit`, ADR-015 (algorithm, failure behaviour, leases, estimates), `docs/operations/redis-and-rate-limits.md`,
`scripts/dev-redis.sh`, tests, a benchmark note `docs/benchmarks/phase-8-rate-limits.md`.
Changed: `internal/config` (new `rate_limit`, extended `redis`), `internal/gateway` (option, handler call, metrics, request-info fields),
`internal/api` (`ErrRateLimited`, `ErrRateLimitUnavailable`), `cmd/gateway` (wire the limiter, startup check), `Makefile`
(`dev-redis`, `test-redis`), `.github/workflows/ci.yml` (Redis container, skip detection), `go.mod`/`go.sum` (go-redis and its
transitive modules), README, ARCHITECTURE.

## Acceptance Criteria

1. Algorithm, unit-tested against a fake clock and a real Redis: requests/min, tokens/min and concurrency are each enforced
   exactly at the boundary (quota N admits N, refuses N+1), refill is continuous, burst equals the configured window, and a refusal
   on one dimension consumes nothing on the others.
2. **Shared quota:** three gateways (in-process) with one Redis and one API key: the total admitted in a minute is within 2% of the
   quota (plus the burst), however the load is spread; the same holds with the aggressive tenant and normal tenants together, the
   normal tenants seeing no rejections.
3. Concurrency: N simultaneous streams admitted, N+1 refused with 429 across gateways; completion, error, client disconnect and
   handler panic all release the lease; a killed gateway's leases expire within `lease_ttl`; a stream longer than `lease_ttl` keeps its
   slot through renewal.
4. Cost estimate: monotonic in input size and `max_tokens`, bounded, handles absent `max_tokens`, multi-byte text, and huge inputs
   without overflow; documented over-estimate.
5. Rejections: `429 RATE_LIMITED`, `Retry-After` ≥ 1 that is accurate (a client that waits it succeeds), body names only the limit type;
   metrics increment with bounded labels; no tenant data leaks across tenants.
6. Failure behaviour: Redis stopped, black-holed, slow, and returning errors, each in `closed` and `open` mode: closed gives 503
   `RATE_LIMIT_UNAVAILABLE` within the timeout and then immediately during the backoff; open admits and counts; recovery is automatic;
   outage and recovery are logged once; no goroutine or connection leak; startup in `required` mode fails fast without Redis.
7. Quotas of 0 mean unlimited and cost no Redis round trip; a quota change takes effect within the authenticator's cache TTL;
   the per-model cap applies with auth off.
8. Request metadata (when enabled): written with a TTL when a worker is chosen, absent when disabled, never delays or fails a request,
   skipped during backoff.
9. Defaults unchanged: with `rate_limit.mode=off` every earlier test passes and a differential run against master is identical.
10. Secrets: the Redis password appears in no log, error, panic or help text; a non-loopback Redis without TLS/password is refused;
    every test client passes the password.
11. Overhead: added p95 below 5 ms against a local Redis (harness or overhead test), recorded in the benchmark note.
12. `gofmt`, `go vet`, `go build`, `go test -race ./...`, `golangci-lint run ./...` pass, with Redis and (tests skipping) without; the
    CI workflow, including the Redis container and skip detection, is run locally step by step before pushing.

## Verification Plan

- Unit: the Lua script's semantics through a real Redis (boundaries, refill, atomic multi-dimension refusal, lease expiry and renewal,
  idle-key expiry) and a pure model of the algorithm cross-checked by randomized sequences (fake time); estimation properties; config
  validation including the transport guard.
- Gateway tests with a fake `Limiter`: mapping of every outcome to status, headers, body, metrics, logs; release on every exit path
  (including panics and client disconnects); order of checks; required-without-limiter fails closed.
- Integration against real Redis with password auth: three in-process gateways sharing a quota; the benchmark harness's
  `multi-tenant` workload against them; failure matrix with a TCP proxy that cuts, black-holes and slows Redis; real binaries for the
  startup and configuration failures; a process test of `cmd/gateway`.
- Security probes: key and password never logged; no tenant can see or influence another's buckets (key injection through tenant or
  model names, hash-tag tricks, huge model names); script injection impossible (no string-built Lua); memory bound under many tenants.
- Mutation testing of the algorithm and failure paths (boundary comparisons, refill math, release, backoff, closed versus open, lease
  expiry, the all-or-nothing rule). `-race -count=10`, also under CPU load.
- Independent `verify-change` and `review-change`, `harden-change`, then `prepare-pr` and stop for approval. The limiter protects
  tenants from each other, so it gets an explicit security look (AGENTS.md).

## Risks

- **Redis is now on the request path.** Mitigated by the timeout, the backoff, tenants without quotas skipping it, and a documented
  closed/open choice. `closed` turns a Redis outage into an API outage for limited tenants; that is the spec's choice and the default.
- **Estimates are not usage.** A tenant is charged `input + max_tokens`, so large `max_tokens` values spend quota faster than the
  model generates. Documented; a refund after the response is the natural follow-up.
- **Lease accounting under failure.** A crashed gateway's slots are held up to `lease_ttl`; a very long stream relies on renewal.
  Both documented and tested; `lease_ttl` is tunable.
- **Second external dependency** (go-redis and a couple of transitive modules), confined to `internal/redis`.
- **License.** Redis 8 changed its license; CI and docs use Redis 7 (and note that Valkey is wire-compatible). The code uses plain
  commands and one Lua script only.
- **Test fragility.** Timing-based tests use the injected clock for the algorithm; real-time tests (leases, outages) use generous
  bounds and are run under load before declaring done.
- **Clock skew** is avoided by using Redis `TIME`, but a Redis failover could move time; the bucket logic clamps negative elapsed time.
- **Scope creep toward admission control.** Global backpressure and queues stay in Phase 15; the PR says so.

## Implementation Steps

1. `internal/ratelimit`: the pure model (buckets, leases, cost estimate) and its tests with a fake clock. (Independent.)
2. `internal/redis`: client construction, TLS, the transport guard, redaction; `scripts/dev-redis.sh`; CI container and skip detection.
3. The Lua script and `RedisLimiter` (all-or-nothing check, renewal, backoff, closed/open); tests against real Redis, plus the randomized
   cross-check against the pure model.
4. Config (`redis`, `rate_limit`), `api` errors, gateway option and handler integration, metrics, logs; fake-limiter tests.
5. Request metadata (optional), the per-model cap, `cmd/gateway` wiring and startup check.
6. Integration: three gateways, the multi-tenant harness run, the failure matrix, process tests, security probes.
7. ADR-015, operations doc, benchmark note, README, ARCHITECTURE; run every CI step locally; independent verification and review;
   fixes; narrower second verification; harden; small commits; `prepare-pr`; stop for approval.

Steps 1 and 2 can proceed independently; 3 to 5 build on them.
