# Operating PostgreSQL and API key authentication

## Quick start (local)

```bash
make dev-postgres                       # throwaway Postgres 16 on 127.0.0.1:55432 (data in .data/, gitignored)
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
| `postgres.max_conns` | 10 | Pool size (1-100). |
| `postgres.connect_timeout` | 5s | Connection and startup check timeout. |
| `postgres.allow_insecure_transport` | false | Permit `sslmode=disable` to a non-local host. |
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
  one, then revoke the old (`key revoke <id or prefix>`). Rotation is create-then-revoke.
- Tenants are suspended, never deleted. Quotas (`--rpm`, `--tpm`, `--max-concurrent`, `--priority`)
  are stored for Phase 8; the gateway does not enforce them yet.
- `tenant set-models --models a,b|--all|--none` controls `allowed_models`.

## What a client sees

| Situation | Response |
| --- | --- |
| No/garbled/unknown/revoked/expired key | `401 UNAUTHORIZED`, identical body, `WWW-Authenticate: Bearer` |
| Valid key, suspended tenant | `403 FORBIDDEN` |
| Model not in the tenant's list (or nonexistent) | `403 FORBIDDEN`, same text for both |
| Database down, key not cached | `503 AUTH_UNAVAILABLE`, `Retry-After` |

`/healthz`, `/readyz` and `/metrics` stay open. The client's `Authorization` header is never sent
to a worker.

## Reading the logs

Request lines gain `tenant_id` and `api_key_id` when authenticated, and `auth_failure` when
refused (`missing`, `invalid`, `revoked`, `expired`, `suspended`, `unavailable`). Keys, prefixes and
the DSN never appear. `auth key store unavailable` is logged once per outage and `auth key store
recovered` once at its end. `auth_rejections_total{status}` counts refusals.

## Failure behaviour

- Database down, key cached: served until `cache_ttl + stale_grace`, then 503. A revocation made
  during the outage is not seen until the database returns.
- Database down, key not cached: 503 at once. The store is retried at most once a second.
- Revocation or suspension: effective within `cache_ttl` on every gateway.
- Expiry: exact (checked against the clock on every request).

## Migrations

Forward-only SQL in `migrations/`, applied by `migrate up` under an advisory lock, one transaction per
file. Never edit an applied file: the checksum is recorded and `migrate up` refuses on drift. Add a new
numbered file instead. An older binary refuses a newer database.

## Backups and the security of the database

The database holds hashes, not keys, but tenant data is still sensitive: restrict network access,
use TLS (`sslmode=require` or `verify-full`) and a dedicated least-privilege role (the gateway needs
SELECT on `api_keys`/`tenants` and UPDATE of `api_keys.last_used_at`; the admin CLI needs more).
`scripts/dev-postgres.sh` creates a trust-auth cluster for local use only.
