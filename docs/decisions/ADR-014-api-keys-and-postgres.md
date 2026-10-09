# ADR-014: API Keys, PostgreSQL Metadata, Key Cache and Failure Behaviour

Status: Accepted (Phase 9)
Date: 2026-10-08

## Context

Until Phase 9 the gateway knew nothing about who called it. Spec sections 19, 44 and 45 call for
tenants, hashed API keys, model configs and benchmark metadata in PostgreSQL, and for the gateway
to authenticate. Authentication is security-sensitive, so the decisions are recorded here with
their trade-offs. Rate limiting and quota enforcement are Phase 8; this phase stores quotas and
priority and carries them, nothing more.

## Decision

- **Key format.** `sf_<8 hex>_<43 base64url chars>`: a public prefix that finds the row and a
  256-bit secret from `crypto/rand`. Parsing accepts exactly one spelling (fixed length, lowercase
  hex, canonical base64url with the spare tail bits zero), so two strings can never denote one key.
  Parse errors never contain the input.
- **Hashing.** Only the prefix and `SHA-256(secret bytes)` are stored; the plaintext is printed once
  by `serverflow-admin key create` and exists nowhere else. A fast hash is correct because the
  secret is high entropy random; stretching (bcrypt/argon2) would add per-request latency for no
  gain. It must never be used for human-chosen secrets, which is why the CLI never accepts a
  supplied key. Hashes are compared with `crypto/subtle`. A key with an unknown prefix runs the same
  comparison against a dummy hash, and a statistical test checks that unknown-prefix and wrong-secret
  answers take indistinguishable time.
- **What a client can learn.** Every failure to present a working key (absent header, wrong scheme,
  malformed, unknown, wrong secret, revoked, expired) is the same `401 UNAUTHORIZED` body with
  `WWW-Authenticate: Bearer`. Revocation, expiry and suspension are decided only after the secret
  matched, so someone without the secret cannot learn a key's state. A holder of a valid key whose
  tenant is suspended gets `403 FORBIDDEN`. The reason is logged and nothing else: logs carry
  `auth_failure` (`missing`, `invalid`, `revoked`, `expired`, `suspended`, `unavailable`) but never the
  key or its prefix. Authentication runs before the body is read and the response carries
  `Connection: close`, because net/http otherwise drains an unread body before replying, which let a
  stalled unauthenticated upload hold the response (found by a test).
- **Model access.** A tenant with `allowed_models` set gets `403 FORBIDDEN` for any model outside
  the list, checked before the model is looked up, so an existing-but-forbidden model and a
  nonexistent one return the same text. Tenants without a list keep the normal `404`.
  `/v1/models` shows only a tenant's models. `allowed_models` NULL means all, an empty array none.
- **Database off the hot path.** `internal/auth.Authenticator` verifies from a cache in front of a
  `KeyStore` interface that `internal/postgres` implements. A looked-up key is trusted for
  `auth.cache_ttl` (30 s); unknown prefixes are remembered for `auth.negative_ttl` (5 s). The positive
  and negative caches are separate LRUs bounded by `auth.cache_size` (10,000 each), so random keys fill
  only the negative cache and cannot evict a real customer's key. Concurrent misses for one prefix share
  one lookup (singleflight), detached from any one client's cancellation, bounded by a lookup timeout.
  Malformed keys are refused before any cache or database access.
- **Lookup capacity and floods.** An unauthenticated client can send unlimited distinct, well-formed
  random keys, each of which is a cache miss. Concurrent store lookups are therefore capped
  (`MaxLookups`, set by the gateway to pool size minus two, minimum 2), so lookups can never take every
  pool connection. A quarter of the cap (at least one slot) is reserved for refreshing keys that are
  already cached; keys never seen before may use only the rest. Over the cap, unknown keys are shed
  immediately with `503 AUTH_UNAVAILABLE` and `Retry-After`, without touching the database and
  without arming the global backoff (which an attacker could otherwise use to keep every stale key
  alive). Consequences: while a flood saturates the new-key slots a valid key the gateway has not
  cached is also shed; cached keys still refresh and a revocation is still seen within `cache_ttl`.
  Pool saturation by *other* clients of the same database (outside the cap) reaches the same state as
  an outage for refreshes: lookups time out as "busy", the key stays served from its stale copy for up
  to `stale_grace` (default 5 minutes, unchanged), and no backoff is armed. **A front proxy or per-IP
  rate limiter is required in production until Phase 8 adds rate limiting.**
- **Error classification.** The store tells the authenticator what kind of failure it saw: *not found*
  (negative cache), *busy* (no connection before the deadline: not an outage, no backoff, a cached key
  may be served stale), *bad record* (one key's row cannot be decoded, for example a NULL element in
  `allowed_models`: not an outage, no backoff, no stale copy, `500` for that key only, logged at most
  every ten seconds), and anything else (connection failures: an outage with backoff and stale serving).
  Migration 0002 adds a CHECK forbidding NULL elements in `allowed_models`.
  **Consequence: revocation, suspension and quota changes take effect within `cache_ttl`.**
  Expiry is compared with the clock on every request from the cached record, so it is exact.
- **Outage behaviour.** If the database is down: a cached key keeps working for `auth.stale_grace`
  (5 min) beyond its TTL (still checked for expiry); an uncached key gets `503 AUTH_UNAVAILABLE` with
  `Retry-After` (not 401: it may be valid). The first such request waits for its lookup to fail, which
  can take up to the lookup timeout (3 s) when the database hangs rather than refuses; later requests
  within the backoff are refused immediately. After a failed lookup the store is left alone for one
  second so a dead or hanging database costs one slow request per second, not one per request. The
  outage is logged once and its end once. A revocation made during an outage is not seen until the
  database returns (or the grace ends). Fail-closed was chosen over fail-open: an unauthenticated
  gateway is worse than an unavailable one. `required` mode refuses to start if the database is
  unreachable or a migration is pending.
- **Defaults are off.** `auth.mode: off` is the default; the gateway then never touches PostgreSQL and
  behaves exactly as in Phase 6.
- **Schema and migrations.** Plain embedded SQL, forward-only, `0001_initial.sql`. The runner records
  `(version, name, checksum)` in `schema_migrations`, applies each file in its own transaction under
  a session advisory lock, and refuses when an applied file's checksum changed or the database is newer
  than the binary. Quotas are typed columns (0 = none configured). Revoked keys keep their rows;
  tenants are suspended, never deleted. `last_used_at` is written asynchronously at most once a minute
  per key, never on the request path.
- **Driver.** `github.com/jackc/pgx/v5` (pgxpool), the maintained standard (lib/pq is in maintenance
  mode; the standard library has no driver). All use is inside `internal/postgres`; the gateway
  depends on `internal/auth` only. All SQL uses bind parameters; the only identifier ever built into
  SQL is a test helper's generated database name. **Version v5.8.0 was chosen deliberately:** later
  releases (v5.9 to v5.11) require Go 1.25, which would raise the repository's Go floor and need a
  `go.work` change; v5.8.0 builds with the existing Go 1.24 directive. Bumping later is a one-line
  change.
- **New modules** (go.mod): `github.com/jackc/pgx/v5 v5.8.0`; indirect:
  `github.com/jackc/pgpassfile v1.0.0`, `github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761`,
  `github.com/jackc/puddle/v2 v2.2.2`, `golang.org/x/sync v0.17.0`, and `golang.org/x/text` raised from
  v0.28.0 to v0.29.0.
- **Secrets hygiene and transport.** The DSN comes from config or `SERVERFLOW_POSTGRES_DSN`, never a
  flag. Config errors never echo it; `PostgresConfig` prints redacted under `%v`, `%+v`, `%#v` and `slog`;
  connection errors have the password scrubbed. `postgres.CheckTransport` asks the driver
  (`pgconn.ParseConfig`) what the DSN resolves to, so `PG*` environment variables, a `host=` query
  parameter, several hosts and fallbacks are all honoured, and requires every connection attempt to use
  TLS (`sslmode` `require`, `verify-ca` or `verify-full`) for any host that is not loopback, `localhost`
  or a unix socket. An unset sslmode, `allow`, `prefer` and `disable` are refused, because each can fall
  back to plain text. `postgres.allow_insecure_transport` is the escape hatch. The check runs in
  `cmd/gateway` (required mode) and `cmd/admin`, not in `Validate()`, because the placeholder default DSN
  would fail every binary. Errors never echo the DSN. `require` encrypts but does not authenticate the
  server; prefer `verify-full`.
- **Migration runner.** Takes a session advisory lock by polling `pg_try_advisory_lock` for up to 60 s
  and then fails with a message naming the problem, so a hung migrator cannot wedge the others
  forever. Session locks need a stable connection: a transaction-pooling proxy (PgBouncer) between the
  migrator and the database is not supported. `schema_migrations` is resolved through the connection's
  `search_path`. Migration files must not contain their own `BEGIN`/`COMMIT` or statements that cannot
  run in a transaction (`CREATE INDEX CONCURRENTLY`).
- **Metrics.** One counter, `auth_rejections_total{status}`, with three possible label values.
  Tenant and key IDs are not labels; Phase 10 decides how to expose tenants.

## Alternatives considered

- *golang-migrate*: a heavy dependency tree for about 150 lines of need.
- *bcrypt/argon2 for key hashes*: pointless for 256-bit random secrets and costly per request.
- *Fail open on outage*: rejected, see above.
- *HTTP admin API*: a bigger attack surface than a CLI with direct database access.
- *Per-request database lookups*: would put PostgreSQL on the routing path (spec section 19 forbids it).

## Consequences

- A revoked key or suspended tenant can work for up to `cache_ttl` (and up to `stale_grace` during a
  database outage). Operators tune these; the CLI says so when it revokes or suspends.
- Database outages become API outages for uncached keys.
- Quotas and priority are carried (`Principal`, `InferenceRequest`) but not enforced until Phase 8/22.
- The gateway has no knowledge of the `models` table yet; the registry and `gateway.models` remain
  the source of served models.

## Known limits

- The cache is guarded by one mutex (a global LRU); a sharded LRU is future work if profiling shows contention.
- Tenant names are case sensitive; quota fields are plain integers without upper bounds; `expires_at` is
  not constrained to be after `created_at`; `Authorization: Bearer` must have exactly one space. These
  were reviewed and left as they are.
- `scripts/dev-postgres.sh` is trust-auth on loopback for local use only.
