# Operating PostgreSQL and API key authentication

## Quick start (local)

```bash
make dev-postgres                       # throwaway Postgres 16 on 127.0.0.1:55432 (DEV_POSTGRES_PORT to change) (data in .data/, gitignored)
export SERVERFLOW_POSTGRES_DSN="$(scripts/dev-postgres.sh dsn)"
go run ./cmd/admin migrate up
go run ./cmd/admin tenant create acme --rpm 600 --models mock-model
go run ./cmd/admin key create --tenant acme --label laptop --expires 90d    # prints the key ONCE
SERVERFLOW_AUTH_MODE=required go run ./cmd/gateway
curl -H "Authorization: Bearer sf_..." localhost:8080/v1/models
scripts/dev-postgres.sh stop
```

`make test-postgres` runs the database-backed tests against that instance. Without
`SERVERFLOW_TEST_POSTGRES_DSN` those tests skip; CI sets it and fails if they skip.

## Configuration

| Setting (env) | Default | Meaning |
| --- | --- | --- |
| `postgres.dsn` (`SERVERFLOW_POSTGRES_DSN`) | placeholder | A secret. Never logged or echoed. |
| `postgres.max_conns` | 10 | Pool size (1-100). In `auth.mode: required` it must be at least 4: key lookups use at most `max_conns - 2` connections. |
| `postgres.connect_timeout` | 5s | Connection and startup check timeout. |
| `postgres.allow_insecure_transport` | false | Permit a DSN that does not require TLS to a non-local host. By default any host other than loopback/`localhost`/a unix socket needs `sslmode=require`, `verify-ca` or `verify-full` (unset, `allow`, `prefer` and `disable` are refused). `PG*` environment variables count. |
| `auth.mode` (`SERVERFLOW_AUTH_MODE`) | `off` | `off` or `required`. |
| `auth.cache_ttl` | 30s | How long a key lookup is trusted. **Revocation delay.** |
| `auth.negative_ttl` | 5s | How long an unknown key prefix is remembered. |
| `auth.cache_size` | 10000 | Bound on each cache. |
| `auth.stale_grace` | 5m | How long past TTL a cached key survives a database outage. |

The gateway only opens the database in `required` mode. It then fails to start if the database is
unreachable or a migration is pending.

## Administration (`serverflow-admin`, `cmd/admin`)

`migrate up|status`; `tenant create|list|show|suspend|activate|set-quota|set-models`;
`key create|list|revoke`; `model add|list|enable|disable`. Run it without arguments for usage.

- A key is printed once on stdout (warning on stderr). Lost keys cannot be recovered: create a new
  one, then revoke the old (`key revoke <id or prefix>`; if you paste a whole key, only its prefix is used and a warning tells you to clear your shell history). Rotation is create-then-revoke.
- Tenants are suspended, never deleted. Quotas (`--rpm`, `--tpm`, `--max-concurrent`, `--priority`)
  are stored for Phase 8; the gateway does not enforce them yet.
- `tenant set-models --models a,b|--all|--none` controls `allowed_models`.

## What a client sees

| Situation | Response |
| --- | --- |
| No/garbled/unknown/revoked/expired key | `401 UNAUTHORIZED`, identical body, `WWW-Authenticate: Bearer` |
| Valid key, suspended tenant | `403 FORBIDDEN` |
| Model not in the tenant's list (or nonexistent) | `403 FORBIDDEN`, same text for both |
| Database down or lookups saturated, key not cached | `503 AUTH_UNAVAILABLE`, `Retry-After` |
| One key's database row is unreadable | `500 INTERNAL_ERROR` for that key |

`/healthz`, `/readyz` and `/metrics` stay open. The client's `Authorization` header is never sent
to a worker.

## Reading the logs

The gateway logs `api key authentication is OFF` at startup when `auth.mode` is `off`. Request lines gain `tenant_id` and `api_key_id` when authenticated, and `auth_failure` when
refused (`missing`, `invalid`, `revoked`, `expired`, `suspended`, `unavailable`, `fault`). Keys, prefixes and
the DSN never appear. `auth key store unavailable` is logged once per outage and `auth key store
recovered` once at its end. `auth_rejections_total{status}` counts refusals.

## Failure behaviour

- Database down, key cached: served until `cache_ttl + stale_grace`, then 503. A revocation made
  during the outage is not seen until the database returns.
- Database down, key not cached: 503 with `Retry-After`. "Down" includes a database that accepts connections and never answers: after the first lookup times out, the following second's requests are refused at once. A pool with every connection in use, or a server with no free connection slots (SQLSTATE 53300), is treated as busy rather than down: no backoff, cached keys served stale, unseen keys refused, log lines rate limited. The first request waits for its lookup to fail,
  which can take up to the 3 s lookup timeout if the database hangs instead of refusing; requests in
  the following second are refused immediately. The store is retried at most once a second.
- Database hanging (packets dropped, not refused): a cached key past its TTL waits at most 250 ms
  (the refresh wait: a fixed 250 ms, not configurable) and is served stale while the lookup finishes in the background; requests that arrive
  meanwhile do not wait. A key the gateway has not seen waits up to the 3 s lookup timeout.
- Lookup flood (many distinct random keys): at most `pool size - 2` lookups run at once and a quarter of
  them is reserved for refreshing cached keys. Beyond the cap unknown keys get 503 without a database
  query, so a valid key the gateway has not cached may also get 503 while a flood runs; cached keys
  keep refreshing, so revocation still takes effect within `cache_ttl`. Saturating the pool from outside
  has the same effect as an outage on refreshes: cached keys are served stale for up to `stale_grace`.
  **Put a reverse proxy or per-IP rate limiter in front of the gateway: there is no rate limiting until Phase 8.**
- A key row that cannot be decoded gives 500 for that key only and is logged; it is not treated as an outage.
- Revocation or suspension: effective within `cache_ttl` on every gateway.
- Expiry: exact (checked against the clock on every request).

## Migrations

If `migrate up` fails on 0002 (`tenants_allowed_models_no_null_elements`), some tenant has a NULL element in
`allowed_models`. Nothing was applied. Find and repair the rows, then run it again:

```sql
SELECT id, name FROM tenants WHERE array_position(allowed_models, NULL) IS NOT NULL;
UPDATE tenants SET allowed_models = array_remove(allowed_models, NULL)
 WHERE array_position(allowed_models, NULL) IS NOT NULL;
```

Review the tenants' lists afterwards (`serverflow-admin tenant show NAME`).

Forward-only SQL in `migrations/`, applied by `migrate up` under an advisory lock, one transaction per
file. The runner takes a session advisory lock (polling for up to 60 s, then failing with a clear message), so
it cannot run through a transaction-pooling proxy such as PgBouncer, and it resolves `schema_migrations`
through the connection's `search_path`. Files must not contain their own `BEGIN`/`COMMIT` or
`CREATE INDEX CONCURRENTLY`. Never edit an applied file: the checksum is recorded and `migrate up` refuses on drift. Add a new
numbered file instead. An older binary refuses a newer database.

## Backups and the security of the database

The database holds hashes, not keys, but tenant data is still sensitive: restrict network access,
use TLS (`sslmode=require` or `verify-full`) and a dedicated least-privilege role (the gateway needs
SELECT on `api_keys`/`tenants`, UPDATE of `api_keys.last_used_at`, and SELECT on `schema_migrations`
(its startup check also calls `to_regclass`); the admin CLI needs more). Without the last grant the
gateway refuses to start in `required` mode. For example, with a role `serverflow_gateway`:

```sql
GRANT SELECT ON api_keys, tenants, schema_migrations TO serverflow_gateway;
GRANT UPDATE (last_used_at) ON api_keys TO serverflow_gateway;
```

`scripts/dev-postgres.sh` creates a throwaway cluster for local use only. It uses password (SCRAM) authentication with a fixed, public test password, like the CI service, so a connection that omits or mistypes a password fails locally exactly as it does in CI.
