# Phase 8 — Redis: Distributed Rate Limiting

Status: Completed (implemented, independently verified twice; see the PR)
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
- No local fallback limiter and no Redis Cluster/Sentinel operation (Redis Cluster is not supported: the single acquire script touches a tenant's keys and a model key in one call; see D12).
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
- **D7 (amended: registry mode)** In registry mode the registry, not a static list, decides which models exist, so the limiter runs before the router and an unknown model (404) still consumes the tenant's quota; only configured model-cap names ever become keys. Documented, not changed.
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
- **D12 Redis key layout.** Per-tenant keys share a hash tag: `rl:{<tenant_id>}:req`, `rl:{<tenant_id>}:tok`,
  `rl:{<tenant_id>}:conc`; the model cap is `rl:model:{<model>}`. Keys expire on their own (idle buckets are deleted after twice the
  burst window), so memory is bounded by active tenants. *Correction after implementation:* the single acquire script touches a tenant's keys
  and a model key in one call, so **Redis Cluster is not supported** (a CROSSSLOT/MOVED error would fail closed like any Redis error); only
  the tenant keys are slot-friendly. Names are escaped, and a model with no cap is sent as a constant placeholder.
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

## Implementation Notes (deviations and additions)

All of D1-D16 were implemented as approved. Differences and additions, in the order a reviewer is likely to care:

- **Redis clock steps (new finding).** The limiter trusts Redis `TIME`, and Docker Desktop's Redis clock fell 14-45 s behind while the Mac
  slept and then jumped forward; a forward step is indistinguishable from idle time, so buckets were credited with it (first benchmarks
  admitted up to 2.4x the quota). The algorithm caps elapsed time at the burst window, never removes tokens for a backward step, and now
  also never moves a bucket's stamp backwards (script and pure model, with a test); the residual risk (a forward step credits at most one
  burst) is in ADR-015. The benchmark was re-run with the Mac awake (`caffeinate`) and the clock offset stable: exactly 800 of 800.
  Real-clock tests use generous bounds; lease tests use a 1.5 s TTL.
- **Cluster.** The single script touches tenant keys and a model key, so Redis Cluster is not supported (the plan said the layout is cluster-safe;
  the tenant keys are, the combined call is not). Documented.
- **Renewal** is one call per tenant with in-flight leases (not one batched call across tenants), to keep each call within one hash slot.
- **Release is asynchronous** (bounded queue, four workers; dropped when full or Redis is backing off; the lease then expires by itself).
- **Health tracking lives in `internal/redis.Client`** (backoff, one probe, one log line per outage/recovery), shared by the limiter and the
  metadata recorder, rather than in the limiter. `Client.Run` is detached from the caller's cancellation so a client that hangs up neither
  abandons an applied script nor counts as a Redis failure.
- **Configuration additions:** `redis.allow_insecure_transport`, `redis.request_metadata_ttl`; `redis.address` may be a `redis://`, `rediss://` or
  `unix://` URL parsed by the driver; the transport guard requires TLS **and** a password for a remote Redis; `rate_limit.mode: required` with
  auth off and no model caps is refused as having nothing to enforce; `rate_limit.lease_ttl` must be at least 10 s and 3x `redis.timeout`.
  `RedisConfig` also redacts itself under JSON marshalling.
- **Cost estimate:** a request larger than the whole token bucket is charged the bucket's size (needs a full bucket) instead of being refused
  forever. Non-ASCII characters count as one token each.
- **Over the local lease cap** a request is refused as unavailable in both failure modes (the cap protects the gateway, not quotas).
- **Retry-After for concurrency** is a fixed 1 s hint. When several quotas refuse, the longest wait is reported.
- **`redis_errors_total`** (spec section 29) was not added; `rate_limit_rejections_total{limit="unavailable"}` and the log lines cover it.
- **Gateway files touched:** `server.go`, `handlers.go`, `middleware.go`, `metrics.go`, `ratelimit.go` (new) and two lines in `attempt.go`
  (`recordWorker` after a worker is chosen, and after a retry's worker is chosen); the retry/attempt logic is otherwise unchanged. `internal/auth`,
  `internal/postgres`, `internal/scheduler`, `internal/registry`, `internal/worker`, `internal/mockworker` and `internal/bench` are untouched.
- **CI:** Redis is started with `docker run ... --requirepass` in a step (a service container cannot take command arguments), and a
  "Redis tests must run, not skip" step mirrors the PostgreSQL one.
- **Benchmark:** the harness binary cannot authenticate (it sends fake `sk-bench-*` keys), so the measurement offers the harness's own
  `multi-tenant` plan in-process to three gateways (`TestBenchmarkRateLimits`, opt-in). The harness is not modified.

### Mutation check (38 mutants of the algorithm, failure paths and secrets handling, plus 3 added)

Method: copy the tree to /tmp, apply one textual mutation, run the owning packages against the password-protected Redis. First pass: 29
killed, 7 survived, 2 did not compile. Survivors and what was done:

- `retry-after min 1 dropped` (api): a test now checks `Retry-After` is never below 1 for 0 and negative waits. Killed.
- `password not scrubbed` (redis client): a test with a server that echoes the password in its error asserts the password is absent from the
  returned errors (`Run`, `Set`, `Get`) and from the log; a gateway-level test asserts it is absent from the response, log and `/metrics`. Killed.
- `negative elapsed not clamped`, `stamp may move back`: new tests drive Redis's frozen test clock backwards and forwards and compare
  script and model (a step back neither creates nor destroys tokens; the stamp does not move back, so the return credits one second, not the
  minute the step spanned). Killed.
- `release not once`: a test counts Redis commands for five `Release` calls (exactly one). Killed. `recovery never logged` (a compiling
  variant) is killed by the outage-log tests.
- `renew revives (no XX)` is an **equivalent mutant**: the script checks `ZSCORE` first and skips ids that are not held or have expired,
  so `XX` is defence in depth. A stronger mutant that removes both the check and `XX` is killed (`renew without existence check or XX`), and
  the new test that renews an expired lease and then admits proves a ghost lease cannot block a tenant.
- `bucket boundary < to <=` is an **equivalent mutant**: when the level equals what the request needs, the wait computed is 0 and only a
  wait greater than 0 refuses, so `<` and `<=` give the same answers. The mutant that forces the equal case to wait (`<=` with a wait of at
  least one tick) is killed. New tests pin the exact edge against Redis for requests, tokens and the model cap: a drained bucket one
  millisecond short of one request refuses with a 1 ms wait, and exactly at the refill admits and drains to zero.
- `refill window clamp removed` is an **equivalent mutant** in behaviour: the level is clamped to the bucket's capacity right afterwards, so
  a longer elapsed time gives the same result; the clamp only keeps the intermediate product below 2^53. The new test that idles 10 hours
  asserts the bucket holds exactly its capacity.

All other mutants (boundary in the pure model, refill doubled, ceiling rounding, oversized-cost clamp, all-or-nothing, concurrency
boundary, lease purge, lease renewal expiry, lease storage, model bucket charge, longest-wait selection, quota-0 skip, release forgetting the
lease, leaked leases on refusal/error, closed vs open, lease cap, per-model cap on unlisted models, backoff arming, single probe, outage
logged once, detached context, password printing, transport guard, estimate rounding, key escaping, release on exit, bypass counter, limit
label) were killed in the first pass.

### Clock finding

Docker Desktop's Redis container clock fell 14-45 s behind the host while the Mac slept and jumped forward afterwards (found by sampling
`redis-cli TIME` against the host clock every 150 ms). A bucket cannot tell a forward step from idleness, so the aggressive tenant in the
first benchmark runs was admitted 1052-1922 times against a bound of 800. With the Mac kept awake (`caffeinate`) and the offset stable the same run
admitted exactly 800. The step-back handling (no tokens removed, stamp never moves back) is exact; a forward step can still credit up to one
burst per bucket. Production Redis on an NTP-synchronised host does not behave like this, but a failover to a host with a wrong clock would.

### Test stability under load

The new ratelimit, redis and gateway tests pass with `-race -count=5` under 20 CPU-burning processes. One existing test,
`TestAClientWhoLeavesStopsTheRetries` (`internal/gateway/attempt_test.go`), fails under that load, and **it fails identically on an unmodified
copy of master** (`git archive master`, `-race -count=40`, same load), so it is pre-existing and unrelated to this phase; it passes in all
normal runs. Not changed here. In one unloaded `go test ./...` run `TestProcessGatewayRetriesAroundAFlakyWorker` returned one 503 NO_CAPACITY
(it passed in the race run and the repeat); also not touched.

### Clock finding: what a backward step leaves behind

After a clock step back, a bucket whose stamp is ahead of the clock refills nothing until the clock catches up or the key expires (at most
twice the burst window), and `Retry-After` can then be too short. Shutdown behaviour (renewals and release workers stop when shutdown begins;
releases are flushed by `Close(5s)`; a stream that outlives `lease_ttl` during a drain loses its lease) is documented in the operations guide.

### Security review round (independent review findings)

Each item was reproduced first, fixed with a test that fails without the fix, and committed separately.

- **P1-1 client-controlled model name on the Redis wire.** `keyModel(r.Model)` was built and sent as KEYS[4] on every limited request, even
  when the model had no cap, with no length check (escaping triples a name), so a 1 MiB model name in registry mode (`AnyModel`) made every
  limited call send about 2.3 MB, tripped the 50 ms timeout and armed the global backoff for all tenants. Now an uncapped model sends the
  constant placeholder `rl:model:{}` (nothing of the client's name is escaped, measured or sent), and a model name longer than
  `protocol.MaxModelLen` is MODEL_NOT_FOUND at parse time. Tests: 20 limited calls with a 1 MiB name send under 40 KB to Redis through
  the test proxy's byte counter (45 MB without the fix) and arm no backoff; the 404 path; a normal and a capped model still work.
- **P2-1 estimate is not an upper bound.** No body rewrite. Documented as a known limitation (ADR-015, operations guide); comments no longer
  say "the worst the gateway would allow"; the larger of `max_tokens` and `max_completion_tokens` is charged.
- **P2-2 dense text.** Three characters per token (still approximate and typically low for the densest text).
- **P2-3 pool exhaustion is not an outage.** `redis.pool_size` (default 64, 1-1000); `goredis.ErrPoolTimeout` becomes `redis.ErrBusy`: no
  backoff, no outage log, 503 when closed, bypass when open. Real read/dial timeouts still arm the backoff. Tests with a saturated 1-connection pool.
- **P2-4 dev-redis.sh** names the container `serverflow-test-redis-<port>`; status/stop/reset touch only that port's container and ping
  checks the published port (verified with two instances).
- **P2-5 invisible leases and drops.** `rate_limit_local_leases` and `rate_limit_dropped_releases_total` are exported; the post-outage lease
  window and the asynchronous-release race are documented.
- **P2-6** no-refund and the unauthenticated gap are documented (ADR-015, operations guide).
- **P3** `MaxRetries` is now disabled in the driver (test: a server that drops the connection sees exactly one script command); a
  malformed `redis.*` is only refused when something uses Redis (test); the backward-clock and shutdown behaviour is documented; the
  real-clock tests use the frozen clock or bounds derived from measured duration; plan D12 is corrected; dev-redis.sh says its fixed password is
  public and bound to loopback.
- **Existing flaky tests.** `TestAClientWhoLeavesStopsTheRetries`: the gateway learns of a disconnect asynchronously, and the test released
  the slow worker's 503 right after the client cancelled, so under load the 503 could arrive first and be retried. The worker never read the
  request body, so net/http never watched its connection and no disconnect event existed to wait for; the worker now reads the body and the test
  waits for the cancellation of the first attempt before releasing it. 50 runs under `-race` and 20 busy processes pass (the same loop failed on
  master). `TestProcessGatewayRetriesAroundAFlakyWorker`: the cluster uses a 200 ms heartbeat and a 500 ms suspect timeout, so a half-second
  stall of a loaded machine makes every worker suspect and the gateway correctly answers NO_CAPACITY with `eligible_workers: 0`. That is a test
  assumption, not a product bug; the test now waits for the workers to report again and repeats the request (at most five blips, logged).
- **Environment note.** `go test ./...` on this Mac opens thousands of loopback connections; with a second test run in parallel the ephemeral
  ports were exhausted (`can't assign requested address`, about 9,000 sockets in TIME_WAIT) and unrelated tests failed until they drained.

### Second review round (verifier's notes and mutation results)

- **Estimator bypass (fixed).** Image parts, tool call arguments and the `tools` field were not in the parsed messages, so 500 KB of them was
  estimated at 14-21 tokens. `EstimateRequestCost` now also charges every byte of the body that is not already counted as message text at three bytes
  per token (over-charges JSON framing a little and images a lot). Test: four 500 KB bodies (text, tools, image part, tool call arguments) are estimated within
  10% of each other, and the test fails with the old call.
- `go mod tidy -diff` is empty (adds `github.com/kylelemons/godebug` indirect, pulled in by prometheus `testutil` in the gateway tests).
- The ACL list in the operations guide was wrong (scripts run as the connecting user); the corrected list was verified against a real ACL user.
  `maxmemory`/eviction guidance added. A host-only `redis.address` is accepted again while Redis is unused (regression test). `dev-redis.sh` names
  the container from the port (round one).
- **Registry mode, unknown model (documented, not changed).** The limiter runs before the router, so a 404 for an unknown model has already
  charged the tenant. Moving the limiter after routing would mean undoing a reserved worker slot on every refusal; the cost is bounded by the
  tenant's own quota. Tenants with an allow-list are not charged for forbidden models.
- **Mutation survivors:** S15 (bucket PEXPIRE window/2), S13 (lease purge now-1000), R03 (quota clamp), R15 (negative cost), R24 (Close without
  drain), C07 (Set not detached), C13 (Set during backoff), C14 (RetryAfter remainder), E08 (prompt), K04/K05 (key escaping) and G02 (refusal continues to the
  upstream) each have a test that fails against the mutant; all re-run and killed. S26 (plain number instead of `%.0f` in the stored level) is an
  **equivalent mutant on Redis 7**: Redis converts script numbers with `%.17g`, so 16-digit levels survive either way (checked with `redis-cli`);
  the explicit format stays for servers whose Lua converts with 14 digits, and the earlier ADR wording was corrected.
- **TestReleaseAndRenewalRunInTheBackground** passed 20/20 with `-race` and 15/15 under 20 busy processes here; the verifier's 2-in-5 failure is
  not reproducible. The most likely cause is the environment (the Docker VM's clock stepping forward makes a lease expire in Redis time while the test is
  holding it), not a renewal bug; the test is unchanged.
