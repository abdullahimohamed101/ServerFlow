# Phase 9 — PostgreSQL: Tenants, API Keys, Model Configs, Benchmark Metadata

Status: Implemented; independent review and verification done, all findings fixed (see Implementation Notes). Awaiting the coordinator's move to completed.
Owner: coding agent
Depends on: Phase 6 (PR #7). Runs in parallel with Phase 7 (benchmark harness); the two share no code, see "Parallel work".
Spec: `docs/architecture/serverflow-spec.md` §8, §19, §44–46, §48 (`internal/auth`, `internal/postgres`, `migrations`), §51, §52, §58 Phase 9, §63
Hand-offs: Phase 8 (Redis rate limiting) reads the tenant quotas stored here; Phase 7's `multi-tenant` workload sends the API keys created here; Phase 7's `benchmark_runs` table lives here and is wired to the harness in a small follow-up after both phases merge.

## Outcome

ServerFlow knows who is calling. Tenants and their API keys live in PostgreSQL behind versioned
migrations; keys are stored only as hashes; the gateway can require a valid key, attaches the tenant to every
request, log line and `InferenceRequest`, and refuses suspended tenants, revoked or expired keys, and models a
tenant may not use. Model configs and benchmark-run metadata have their tables and store methods ready for the
phases that consume them. An admin CLI creates tenants and keys, and prints a new key exactly once. The
routing hot path never waits on the database: keys are verified from a bounded cache, and a database outage
degrades safely.

```text
admin CLI ──► PostgreSQL (tenants, api_keys[hash], models, benchmark_runs)
                  ▲  cache-aside, TTL, singleflight
client ─ Bearer sf_… ─► gateway: auth middleware ─► tenant context ─► (Phase 8 limits) ─► router ─► worker
```

Acceptance headline (spec): *tenants, API keys (hashed), model configs, benchmark metadata, migrations.*

## Non-Goals

- No rate limiting or quota enforcement (requests/min, tokens/min, concurrency): Phase 8 (Redis). The quota
  fields are stored and passed through; nothing here acts on them.
- No priority tiers or fair queuing (Phase 22), no usage records or Kafka events (Phase 12), no per-tenant
  Prometheus labels (Phase 10 decides cardinality), no worker or deployment configs.
- No HTTP admin API (a larger attack surface); administration is the CLI this phase.
- No OAuth, JWTs, or user accounts; no TLS termination (spec §44: external TLS). The control plane keeps its
  own shared token from Phase 4.
- No change to the scheduler, registry, agent, retry logic, or the benchmark harness.
- Authentication stays **off by default**, so Phases 2–6 behave exactly as before.

## Current Architecture

- `internal/config` already has `PostgresConfig{DSN}` with a placeholder default and nothing that uses it.
  `InferenceRequest` already has `TenantID` and `Priority`, never set. `protocol` has no tenant types.
- The gateway has no client authentication. Requests carry no identity; `Authorization` is deliberately not
  forwarded to workers (header allow-list). `/healthz`, `/readyz`, `/metrics` are open; `/v1/models` and
  `/v1/chat/completions` are the API.
- Errors use `internal/api` (`INVALID_REQUEST`, `MODEL_NOT_FOUND`, `NO_CAPACITY`, …); `UNAUTHORIZED` is listed in
  spec §51 but not implemented.
- No migrations directory, no SQL, no database dependency in `go.mod` (only `yaml.v3` and the Prometheus client).
- PostgreSQL 16 binaries are installed locally via Homebrew; Docker is not required for tests.
- Spec §19: Postgres is durable metadata, **not** the routing hot path, heartbeats, or the token stream.
  Spec §44: hashed API key storage, request and token limits, secrets from the environment.

## Decisions (confirm before implementation)

- **D1 Driver: `github.com/jackc/pgx/v5` (`pgxpool`).** This is the project's first database dependency, so it
  needs a reason: the standard library has no PostgreSQL driver, `lib/pq` is in maintenance mode, and pgx is the
  maintained standard with pooling, context support and prepared statements. Its transitive dependencies are
  reviewed and listed in the PR; nothing else is added. Alternative considered: `database/sql` with the pgx
  stdlib adapter (more portable, fewer features) — rejected for this project's single-database scope.
- **D2 Migrations: plain SQL, embedded, forward-only, with a tiny in-repo runner.** Files
  `migrations/0001_<name>.sql` are compiled in by a small `migrations` package (`//go:embed`). The runner
  creates `schema_migrations(version, name, checksum, applied_at)`, takes an advisory lock so two processes cannot
  migrate at once, applies each pending file in its own transaction, and refuses to run if an applied file's
  checksum changed. No `golang-migrate` (a heavy dependency tree for ~150 lines of need).
- **D3 Schema.** `tenants(id, name unique, status active|suspended, requests_per_minute, tokens_per_minute,
  max_concurrent_requests, allowed_models text[] (null = all), priority, created_at, updated_at)`;
  `api_keys(id, tenant_id → tenants, label, prefix unique, secret_hash bytea, status active|revoked,
  created_at, expires_at null, revoked_at null, last_used_at null)`; `models(name pk, status enabled|disabled,
  display_name, max_tokens_limit null, notes, created_at, updated_at)`; `benchmark_runs(id pk, created_at,
  git_commit, git_dirty, model, worker_count, gpu_type, scheduler, concurrency_or_rate, workload,
  prompt_distribution jsonb, max_tokens, duration_seconds, seed, repeat_index, schema_version, result jsonb)`.
  IDs are prefixed random strings (`ten_…`, `key_…`), matching `req_` and `att_`. Constraints enforce status
  values and non-negative quotas. Quotas are typed columns, not a JSON blob, so they can be validated and indexed.
- **D4 API key format and hashing.** A key is `sf_<prefix>_<secret>`: an 8-hex-character public prefix and a
  256-bit secret from `crypto/rand`, base64url. Only the prefix and `SHA-256(secret)` are stored. A fast hash is
  correct here: the secret is high-entropy random, so password-stretching adds latency without adding safety.
  Verification looks the key up by prefix, then compares hashes in constant time. The plaintext is shown once at
  creation, never stored, never logged, and never echoed in errors.
- **D5 Gateway authentication is a middleware gated by `auth.mode`** (`off` by default; `required`). In
  `required` mode `/v1/*` needs `Authorization: Bearer sf_…` before the body is read; `/healthz`, `/readyz` and
  `/metrics` stay open. Authentication failures are a uniform `401 UNAUTHORIZED` with
  `WWW-Authenticate: Bearer`, whether the key is missing, malformed, unknown, revoked or expired (no hint that a
  key exists). A suspended tenant gets `403 FORBIDDEN`. `X-Request-ID` is still assigned first so failures are
  traceable. The client's credentials continue never to reach a worker.
- **D6 The database is not on the hot path.** A new `internal/auth` package verifies keys against a bounded
  cache in front of a small `KeyStore` interface that `internal/postgres` implements. Positive entries live
  `auth.cache_ttl` (30 s); unknown keys are cached negatively for `auth.negative_ttl` (5 s); the cache is size
  bounded (`auth.cache_size`, 10,000, least-recently-used eviction) so random keys cannot grow memory; concurrent
  misses for one key share one lookup (singleflight). **Revocation therefore takes effect within `cache_ttl`**,
  which is documented and tested.
- **D7 Failure behaviour.** Database down and key cached: keep serving it for up to `auth.stale_grace`
  (5 minutes) past its TTL, logging once per outage. Database down and key not cached: fail **closed** with
  `503 AUTH_UNAVAILABLE` (not 401, since the key may be valid) and `Retry-After`. A suspended tenant or revoked key
  discovered during an outage cannot be seen until the database returns; this is the stated trade-off. At
  startup, `required` mode fails fast if the database cannot be reached within `postgres.connect_timeout`.
- **D8 Tenant context.** On success the request carries `TenantID`, the key's ID and the tenant's policy
  (quotas, allowed models, priority) through the request context into `InferenceRequest.TenantID`, and into the
  request and attempt log lines as `tenant_id` and `api_key_id` (never the key or its prefix). No Prometheus
  label: tenants are unbounded; Phase 10 decides how to expose them.
- **D9 What the gateway enforces this phase:** key valid, tenant active, key not expired, and the requested model
  in the tenant's `allowed_models` when that is set (`403 FORBIDDEN`, same message for forbidden and nonexistent
  models so a tenant cannot probe the catalogue). `/v1/models` lists only the tenant's allowed models. Quotas and
  priority are carried, not enforced.
- **D10 Admin CLI `cmd/admin`** (`serverflow-admin`): `migrate up|status`; `tenant create|list|show|suspend|
  activate|set-quota|set-models`; `key create --tenant T [--label L] [--expires 30d]|list|revoke`; `model
  add|list|enable|disable`. The DSN comes from config or `SERVERFLOW_POSTGRES_DSN`; the CLI prints a new key once
  and warns it cannot be shown again; destructive actions (revoke, suspend) print what they changed. No delete of
  tenants (suspend instead) to keep history.
- **D11 Config.** `postgres.dsn` (secret: never logged or echoed), `postgres.max_conns` (10),
  `postgres.connect_timeout` (5 s); new `auth.mode`, `auth.cache_ttl`, `auth.negative_ttl`, `auth.cache_size`,
  `auth.stale_grace`, with validation (positive durations, stale_grace ≥ cache_ttl, mode known) and env overrides in the
  existing style. A DSN with `sslmode=disable` to a non-loopback host is rejected unless explicitly allowed.
- **D12 Layering.** `internal/postgres` (pool, store, migration runner), `internal/auth` (key format, hashing,
  verification, cache; depends on an interface), `migrations` (embedded SQL), `cmd/admin`. The gateway depends on
  `internal/auth` only, never on `pgx`, so gateway tests use fakes and a test never needs a database unless it is
  about the database.
- **D13 Test infrastructure.** Database tests run against a real PostgreSQL when
  `SERVERFLOW_TEST_POSTGRES_DSN` is set and skip otherwise, except that CI must set it (a CI step fails if the
  tests were skipped). Each test gets its own schema or database for isolation. `make dev-postgres` starts a
  throwaway Postgres 16 from the Homebrew binaries in `.data/` on port 55432 (no Docker needed); `make
  test-postgres` runs the gated tests; the CI workflow gains a `postgres:16` service container.
- **D14 Secrets and hygiene.** `.data/` is gitignored. DSNs and keys never appear in logs, errors, panics, or
  test output. The CLI reads secrets from flags only for non-secret fields; the DSN is env or config.
- **D15 Multiple keys per tenant; no key reuse.** Rotation is create-new, then revoke-old. Revoked and expired
  keys keep their rows (audit); prefixes are never reused. `last_used_at` is updated asynchronously and at most once
  a minute per key, so it never adds a write to the request path.
- **D16 Benchmark metadata and model configs are tables and store methods only** this phase (create, get, list)
  plus CLI for models. The gateway does not consult `models` yet (the registry and `gateway.models` remain the
  source), and the benchmark harness does not write `benchmark_runs` until the wiring follow-up.

## Proposed Design

```go
// internal/auth
type Principal struct{ TenantID, KeyID string; Policy TenantPolicy }
type TenantPolicy struct{ Status string; AllowedModels []string; RequestsPerMinute, TokensPerMinute, MaxConcurrent int; Priority int }
type KeyStore interface { LookupKey(ctx context.Context, prefix string) (KeyRecord, error) }   // implemented by internal/postgres
type Authenticator struct{ /* store, bounded LRU, singleflight, clock */ }
func (a *Authenticator) Authenticate(ctx context.Context, bearer string) (*Principal, error)  // typed errors: ErrInvalid, ErrExpired, ErrRevoked, ErrSuspended, ErrUnavailable
func GenerateKey() (plaintext, prefix string, hash []byte)
```

Gateway: an `authenticate` wrapper applied to the `/v1` routes in `newWithUpstream`/`NewRegistry` when
`auth.mode=required`; it puts the `Principal` in the request context (`infoFrom` gains tenant fields). The chat
handler copies `TenantID` into `InferenceRequest` and checks `allowed_models` after parsing the model (before
routing). Errors use new `api` helpers: `ErrUnauthorized`, `ErrForbidden`, `ErrAuthUnavailable`.

## Affected Files / Components

New: `internal/auth`, `internal/postgres`, `migrations`, `cmd/admin`, ADR-014 (API keys, hashing, cache and
failure behaviour), `docs/operations/postgres-and-auth.md`, tests, `scripts/dev-postgres.sh`.
Changed: `internal/config` (new `auth` section, `postgres` fields), `internal/gateway` (middleware, request info,
logs, `/v1/models` filter), `internal/api` (three errors), `cmd/gateway` (wire the authenticator, startup check),
`Makefile`, `.github/workflows/ci.yml`, `go.mod`/`go.sum` (pgx only), `.gitignore`, README, ARCHITECTURE.

## Acceptance Criteria

1. Migrations: `migrate up` on an empty database creates every table; running it again changes nothing; a
   modified applied file is refused; two concurrent `migrate up` calls do not corrupt (advisory lock); `status`
   lists applied and pending.
2. Keys: generated keys have the documented shape and 256 bits of secret; only the prefix and hash are stored
   (no plaintext in any column, log, error, or CLI output other than the one-time print); hashes compare in
   constant time; a wrong secret with a right prefix fails the same way as an unknown key.
3. Gateway `required` mode: no key, malformed key, unknown key, wrong secret, revoked key, and expired key each get
   an identical `401 UNAUTHORIZED` body and `WWW-Authenticate`; a suspended tenant gets `403`; a valid key reaches
   the worker with the tenant in the logs; `/healthz`, `/readyz`, `/metrics` stay open; the client's
   `Authorization` never reaches a worker.
4. Tenant policy: a model outside `allowed_models` is `403 FORBIDDEN` with the same text as an unknown model for
   that tenant; `/v1/models` is filtered; `InferenceRequest.TenantID` is set; `tenant_id` and `api_key_id` are in
   request and attempt logs and the key is in no log line.
5. Hot path: with a warm cache the database sees no query per request (counted in a test); concurrent first
   requests for one key cause one lookup; random keys cannot grow the cache past `cache_size`; a flood of random
   keys is answered 401 from the negative cache without a database query per request. Known limits (ADR-014): a
   flood of *distinct* random keys costs one lookup each, bounded by a cap below the pool size; unknown keys over
   the cap are shed with 503 (so a valid key the gateway has not cached can be shed too) while a quarter of the cap
   stays reserved for refreshing cached keys; a request for a cached key past its TTL waits at most `refresh_wait`
   (250 ms) for the refresh and is otherwise served stale; a front proxy or per-IP limiter is required until Phase 8.
6. Revocation: after `key revoke`, the key stops working within `cache_ttl` and not before the DB write; an
   expiry crossing is honoured at the next verification.
7. Outage: database stopped while a key is cached → still served up to `stale_grace`, then `503 AUTH_UNAVAILABLE`;
   uncached key → `503` at once with `Retry-After`; recovery after the database returns; the outage is logged once,
   not per request. Gateway start in `required` mode with no database fails fast with a clear error.
8. `auth.mode=off` (default): every Phase 2–6 test passes unchanged and behaviour is identical (differential check
   against master).
9. CLI: every subcommand works against a real database, prints a key once, and refuses bad input with a clear
   message; no secrets in its output beyond the one-time key.
10. `benchmark_runs` and `models` store methods round-trip all fields, including nulls and JSON.
11. Secrets: the DSN's password appears in no log, error, panic, or `--help`; config errors do not echo it.
12. `gofmt`, `go vet`, `go build`, `go test -race ./...`, `golangci-lint run ./...` pass with a Postgres available;
    without one the gated tests skip and CI fails if they skipped; the only new module dependency is pgx and its
    listed transitive ones.

## Verification Plan

- Unit tests for key generation and parsing (malformed forms, truncation, unicode, huge input), constant-time
  comparison use, the cache (TTL, negative TTL, LRU bound, singleflight, stale grace, clock fake), and the
  gateway middleware with a fake `KeyStore` (every failure kind, identical bodies, headers, logs).
- Store and migration tests against a real PostgreSQL 16: schema, constraints (bad status, negative quota),
  idempotent migration, checksum drift, concurrent migrate, key lookup, revoke and suspend, JSON round-trips.
- Integration: real gateway + real Postgres + mock worker: end to end with a valid key, a revoked key, a model
  not allowed, the cache under concurrency, and a database stop/start mid-run. A process test of `cmd/admin` and
  `cmd/gateway` with environment-supplied DSN.
- Security probes: timing consistency of unknown-prefix versus wrong-secret, key enumeration via error text,
  log scraping for the plaintext key and DSN password, SQL injection through every CLI string and every key
  prefix (parameters only), cache-flood memory bound.
- Mutation testing of the verification path (status checks, expiry comparison, hash compare, cache TTL and
  bound, uniform-error mapping, allowed-model check). `-race -count=10`, also under load.
- Independent `verify-change` and `review-change` with explicit security review (AGENTS.md: authentication and
  secrets are security-sensitive), `harden-change`, `prepare-pr`, and stop for approval.

## Risks

- **First external dependency.** pgx brings a few transitive modules; mitigated by reviewing and listing them,
  pinning versions, and keeping all pgx use inside `internal/postgres`.
- **Revocation latency.** A revoked key works for up to `cache_ttl` (30 s) and, during a database outage, up to
  `stale_grace`. Documented; tunable; the trade is no database round trip per request.
- **Fail-closed on outage** turns a database outage into an API outage for uncached keys. Chosen deliberately (an
  unauthenticated gateway is worse); the grace period softens it for active keys.
- **Hash choice.** SHA-256 of a 256-bit random secret is safe, but it must never be used for human-chosen
  secrets; the ADR says so and the CLI never accepts a user-supplied key.
- **Information leaks.** Uniform 401 bodies, a uniform 403 for model access, constant-time comparison, and no key
  material in logs; verified by probes.
- **Migration mistakes are permanent.** Forward-only with checksums, tested on an empty and a populated database;
  the first migration is small and reviewed line by line.
- **Test fragility.** Database tests need Postgres; `make dev-postgres` and the CI service container make that
  routine, and the skip-detection prevents silent green.
- **Scope creep toward enforcement.** Quotas and priority are stored only; the PR says so to avoid a half-built
  limiter.

## Parallel work (Phases 7 and 9)

Disjoint from Phase 7: this phase adds `internal/auth`, `internal/postgres`, `migrations`, `cmd/admin`, and
changes `internal/config`, the gateway middleware and handlers, `internal/api`, `go.mod`, the Makefile and CI.
Phase 7 touches none of those except small additions to the Makefile and README, which merge cleanly. The
couplings are one-way and later: Phase 7's `multi-tenant` workload needs keys from here to see enforcement, and
the harness gets a Postgres-backed `benchmark_runs` writer in a follow-up after both merge.

## Implementation Steps

1. Key format, generation, hashing, parsing and their tests (`internal/auth`, no database). (Independent.)
2. Migration runner, first migration, embedded files, tests against Postgres; `make dev-postgres`, CI service.
3. Store methods (tenants, keys, models, benchmark runs) with tests.
4. Authenticator with the cache, singleflight, negative cache, stale grace; unit tests with fakes.
5. Config (`auth`, `postgres`), `api` errors, gateway middleware, tenant context in logs and `InferenceRequest`,
   `/v1/models` filtering, `cmd/gateway` wiring and startup check.
6. `cmd/admin` and its tests.
7. Integration and process tests, security probes; ADR-014, operations doc, README, ARCHITECTURE.
8. Gates, independent verification and review, fixes, narrower second verification, harden, small commits,
   `prepare-pr`, and stop for approval.

Steps 1 and 2 can proceed independently; 3–6 build on them.

## Implementation Notes (deviations and additions)

- **pgx v5.8.0, not the newest.** v5.9 to v5.11 require Go 1.25; v5.8.0 is the last release that builds
  under the repository's `go 1.24.0` (and `go.work`). Raising the floor is a separate decision.
- **Config validation of the TLS rule is not part of `Validate()`.** The placeholder default DSN
  (`sslmode=disable` to host `postgres`) would otherwise fail every binary. `ValidatePostgresTransport()`
  runs where the database is used: `cmd/admin` and `cmd/gateway` in `required` mode.
- **Additions beyond the plan:** `Connection: close` on authentication rejections (found by a test:
  net/http drains an unread body before replying, so a stalled upload held the 401); a one-second
  backoff after a failed key lookup; separate bounded positive and negative caches; the
  `auth_rejections_total{status}` counter; `PostgresConfig` redacts its DSN under every formatting path;
  `postgrestest` (a throwaway database per test, refusing non-loopback servers).
- **Small touch to attempt logging:** `internal/gateway/attempt.go` log lines gained `tenant_id` and
  `api_key_id` attributes (log attributes only; retry logic untouched).
- **`InferenceRequest.Priority`** is set from the tenant's priority alongside `TenantID`; nothing reads it yet.
- **Not done:** the plan's "differential check against master" is covered by the unchanged Phase 2-6
  test suites passing with auth off, not by a side-by-side run. `last_used_at` writes are best effort.
- **Known limits:** a flood of distinct well-formed random keys costs one indexed lookup each (bounded
  by the pool and lookup timeout); rate limiting that is Phase 8. `sslmode=allow|prefer` were not (superseded: see round 2 below)
  rejected by the transport rule.
- **Mutation check (manual, 25 mutants in a scratch copy):** expiry comparison and removal, key and tenant status checks, hash comparison, constant-time helper, state revealed before the secret matched, cache TTL (never expires, off by one), negative cache and TTL, LRU bound, stale grace, fail-open on outage, singleflight, 401/403 mapping, distinct error bodies, allowed-model check, models-list filtering, tenant propagation, allow-list nil semantics, auth bypass, revoke no-op, model-forbidden text. All 25 were caught by the tests; none survived. Some kills may be compile failures rather than assertions; this was not separated.

### Security review fixes (round 2)

- **P2-1 lookup flood.** `Config.MaxLookups` caps concurrent store lookups (gateway: pool size - 2, min 2);
  a quarter is reserved for refreshing cached keys; unknown keys over the cap are shed with 503 and no
  database access, and shedding never arms the global backoff. Test:
  `TestFloodCannotHideARevocationAndIsShedWithoutTouchingTheStore`. ADR-014 and the operations guide say
  that pool saturation from outside reaches the stale state, that `stale_grace` stays 5 minutes, and that a
  front proxy or per-IP limiter is required until Phase 8.
- **P2-2 singleflight window.** `fetch` re-checks the caches under the lock before starting a flight. Tests:
  `TestFetchRechecksTheCacheBeforeStartingALookup`; `TestConcurrentFirstRequestsShareOneLookup` ran
  `-race -count=50` clean.
- **P2-3 TLS guard.** `postgres.CheckTransport` (pgconn.ParseConfig) requires TLS for every attempt to any
  non-local host; the hand-written parser in config was deleted. pgx v5.8.0 has no `hostaddr` support, so
  that setting cannot redirect a connection. Tests: `TestCheckTransport*`.
- **P2-4 error classes.** Store errors are not-found / busy / bad record / outage; bad records neither arm the
  backoff nor serve stale copies (500 for that key). Migration 0002 forbids NULL elements in `allowed_models`.
  Tests: `TestUnreadableRecordIsPerKeyNotAnOutage`, `TestBusyStoreIsNotAnOutage`, `TestLookupKeyClassifiesFailures`.
- **P3 items.** postgrestest resolves hosts through pgconn (`TestCheckLoopback`); `gateway.WithAuthenticator`
  (a nil authenticator fails closed: `TestRequiredButNotWiredFailsClosed`) and a startup line when auth is OFF;
  `key revoke` with a whole key uses the prefix and warns; the migration lock times out
  (`TestMigrateGivesUpWhenAnotherMigratorHoldsTheLock`); wall-clock sleeps replaced by the fake clock and an
  `onWait` hook, except the process test's 1.3 s wait (a separate binary has its own clock); docs updated.
- **Deliberately left:** pgx v5.8.0 pin; tenant name case sensitivity; no CHECK(expires_at > created_at);
  unbounded quota ints; dev-postgres trust auth and default port (a `DEV_POSTGRES_PORT` override was added so
  two worktrees can each run one); the global LRU mutex (a sharded LRU is future work); exactly-one-space Bearer.

### Security review fixes (round 3: verifier findings)

- **Hanging database (blackhole).** A request for a cached key past its TTL waits at most `refresh_wait` (250 ms)
  for the refresh and otherwise serves the stale copy (valid for `stale_grace`); the lookup carries on in its own
  goroutine and requests that arrive meanwhile do not wait at all. Uncached keys still wait up to the 3 s lookup
  timeout. Tests: `TestHangingStoreDoesNotStallCachedKeys` (30 concurrent clients, one shared flight, latency under
  1 s with a 5 s lookup timeout, and a revocation made meanwhile is seen once the database answers). The "one slow
  request per second" sentence in ADR-014 and the operations guide was replaced with the accurate statement.
- **Out-of-range numbers.** Quotas, priority, max tokens and the benchmark counters are range-checked in the store
  (int32 columns; the CLI inherits it) with a plain message; `22003` is also mapped. Reproduced first: the mutant
  that removes the model max-tokens check prints the driver's "unable to encode (*int)(0x...)" text, which the new
  tests forbid. Tests: `TestNumericFlagsAreRangeCheckedBeforeTheDatabase` (every flag at 0, 1, 2147483647 and
  2147483648, 99999999999, 9223372036854775807, negatives), `TestStoreRangeChecks`, `TestCheckRange`.
- **Control characters.** Names, labels, display names, notes, benchmark text and model lists reject control,
  format (bidi, zero-width), line/paragraph separator and invalid-UTF-8 characters (`badRune`). The CLI also
  filters every byte it prints, so rows written around the application cannot inject escapes when listed.
  Tests: `TestControlCharactersAreRefused`, `TestListingNeverEmitsTerminalEscapes`,
  `TestValidateTextRejectsControlAndInvisibleCharacters`. Free text is now single-line (newline and tab are refused).
- **Docs.** GRANT example including `schema_migrations`; exact timing wording (about 2 us between cached paths, about
  55 us for a fresh random prefix, plus the 5 s versus 30 s cadence oracle); Status line; acceptance criterion 5 states
  its known limits; the gateway start log now has `control_plane_token` and `api_key_auth` fields (the old
  `auth` field was the control-plane token).
- **Mutation survivors from the verifier (22).** Killed by new tests: au-dummy-removed, cfg-cache-ttl-max,
  adm-expiry-max, adm-expiry-zero, adm-empty-models-all, adm-tls-validation-off, adm-collision-retry,
  au-stale-boundary, au-stale-masks-notfound, au-downuntil-not-reset, ty-allow-prefix, pg-tenant-name-prefix,
  pg-label-nul, srv-run-removed, au-ttl-neg-age, au-recovered-never-clears, lru-not-lru, pg-name-check,
  pg-set-status-validate, and key-nonconstant (structurally: `TestHashComparisonIsStructurallyConstantTime` asserts
  `HashesEqual` is only `subtle.ConstantTimeCompare`; constant time itself is not observable from a test).
  **Equivalent, not killable:** `key-len` (`!=` to `<`): a longer key is still refused, by the decoded-secret length check
  in the same function, so behaviour is identical. `au-neg-remove-on-success`: a prefix is never in the negative
  cache when a lookup succeeds (an expired negative entry is removed before the lookup starts and a fresh one short
  circuits it), so the removal is defensive. Re-run on the current code: all 20 observable mutants plus 8 new ones
  for the new code killed; the 3 survivors are the equivalent ones above and one no-op mutant of mine.

### Final small round (narrow verification findings)

- **F1 black-holed database.** A connection attempt that gets no answer while the pool has spare capacity is now an
  outage (backoff; later uncached keys refused at once; "unavailable" logged once). Busy means every pool connection is
  in use (`pool.Stat()`), which is the only context expiry still classed busy. Test:
  `TestBlackholedDatabaseIsAnOutageAndLaterKeysFailFast` (a listener that accepts and never answers; four back-to-back
  unseen keys: only the first waits). Known edge: with a pool of exactly the number of connections stuck in a
  black-holed handshake the check cannot tell saturation from a hang.
- **F2 SQLSTATE 53300** is busy (no backoff, rate-limited log). Test: `TestTooManyConnectionsIsBusyNotAnOutage` (a role with
  CONNECTION LIMIT 1).
- **F3 small pools.** `postgres.max_conns >= 4` is required when `auth.mode: required` (config error otherwise), so the lookup cap
  `max_conns - 2` is always below the pool and at least 2. Tests: `TestRequiredAuthNeedsAPoolOfAtLeastFourConnections`, `TestLookupCapLeavesTwoConnectionsFree`.
- **Constructor gap.** `NewRegistry` and the new `NewFromConfig` honour `auth.mode`; `New` (which only sees `GatewayConfig`) takes
  authentication only through `WithAuthenticator`. `cmd/gateway`'s wiring check and the cap are small tested functions.
  Tests: `TestConstructorsHonourAuthMode`, `TestCheckAuthWiring`, `TestRequiredModeWithoutAnAuthenticatorIsCaughtBeforeServing`.
- **Bug found by the new tests:** the admin CLI filtered its normal output but printed errors (which can echo a flag name from the
  command line) through the raw writer. Both are filtered now. Test: `TestStderrIsFilteredToo`.
- **Docs:** metrics HELP and ADR list 401/403/500/503; ADR names 0002; the 250 ms refresh wait is a fixed constant; the ops guide has the query and
  repair for rows that block 0002 and the busy/outage distinction.
- **Mutants killed by new tests:** shed arms backoff, refresh counter leak, refresh counted as unseen, 10x refresh timer, TLS prefix
  hostnames (`localhost.evil.com`, `127.0.0.1.evil.com`, `0.0.0.0`), error while reading rows treated as not found, plainWriter for bidi
  marks, U+2028/2029 and U+FFFD, the stderr writer, and the two main.go items.
- **Equivalent mutants (no test possible):** p-key-len and au-neg-remove-on-success (see round 3); n-badrecord-arms-backoff (the next line resets
  the backoff, so arming it first changes nothing); n-tls-trim-brackets (a bracketed IPv6 host is already unbracketed by the driver);
  n-lookup-final-rowserr (the final `rows.Err()` after a successful scan cannot fail on a fully read single-row result);
  n-pw-return-len (the returned count is not observable through `fmt.Fprintf`'s use); n-maperr-22003 (range checks now run first, so the
  driver code is unreachable from the CLI); n-warn-unlimited (log rate limiting is cosmetic); n-maxlookups-min (the default for values below 2
  is unreachable once configuration requires 4 connections).
