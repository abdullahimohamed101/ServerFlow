# Running ServerFlow in containers (one machine)

`make dev-up` brings up the whole system from a clean checkout: a gateway in front of three mock workers (100, 60 and 20 tokens/s),
each with its own worker agent, registered with a control plane; API-key authentication and Redis rate limits backed by PostgreSQL
and Redis; lifecycle events through Redpanda into usage rows by the usage consumer; Prometheus and Grafana with the committed
dashboards; traces in Jaeger. `make dev-smoke` proves it works with 13 end-to-end checks. Decisions and reasons: ADR-021.

This is a development and demonstration stack for one machine. It is hardened (non-root, read-only, no capabilities, private network,
loopback-only ports, generated secrets) but it is not a production deployment: no TLS between services, one broker, one database,
nothing replicated.

## Requirements

Docker with Compose 2.24 or newer (Docker Desktop, Colima, or Docker Engine with the compose plugin), `git`, `make`, `bash` and `curl`.
`buildx` is not required: the Dockerfile builds with the classic builder too (and with BuildKit when present). On macOS with Colima only
the home directory is shared with containers, so clone under your home directory. About 2 GB of memory is used by the stack at rest
(measured numbers: `docs/benchmarks/phase-16-docker-deployment.md`); Colima's default 2 GB VM is too small, use 4 GB.

## Quick start

```bash
git clone <repository> && cd ServerFlow
make dev-up        # builds six images, generates secrets under .dev/, starts everything, creates two tenants and their keys
make dev-smoke     # 13 checks; with the default single gateway the last line says "12 of 13 checks passed, 0 failed, 1 skipped" (S13 needs GATEWAYS=3)
export SF_KEY=$(cat .dev/api_key)
curl -s -H "Authorization: Bearer $SF_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"mock-model","messages":[{"role":"user","content":"hello"}],"max_tokens":16}' \
  http://127.0.0.1:18080/v1/chat/completions
make dev-down      # removes containers and networks; nothing is left behind
```

URLs (all on `127.0.0.1` only): gateway `http://127.0.0.1:18080`, control plane `:19090` (token in `.dev/controlplane.env`), Prometheus `:19091`,
Grafana `:13000` (user `admin`, password in `.dev/grafana.env`), Jaeger `:16686`. The first `make dev-up` copies
`deploy/compose/.env.example` to `.env`; edit it to change ports, profiles, replicas and mock-worker behaviour.

## Commands

| Command | What it does |
| --- | --- |
| `make dev-up` | validates `.env`, generates secrets, pulls images (with retries), builds, starts, bootstraps tenants and keys. Idempotent. |
| `make dev-smoke` | the end-to-end checks S1 to S13 (below) |
| `make dev-audit` | checks the running containers: read-only, no capabilities, no-new-privileges, unprivileged, memory limit, loopback-only ports, no anonymous volume |
| `make dev-down` | removes containers and networks (named volumes of `PERSIST=1` stay) |
| `make dev-reset` | `dev-down` plus named volumes, `.dev/` (secrets, keys) and the `serverflow/*:dev` images |
| `make dev-ps`, `make dev-logs SERVICE=gateway` | status and the last 100 log lines |
| `make dev-config` | the rendered compose file without any secret value (safe to paste into an issue) |
| `make dev-pull` | pulls every image with retries and backoff |
| `make dev-psql`, `dev-redis-cli`, `dev-rpk ARGS="topic list"` | shells into the data services (the password never appears on a command line) |
| `make dev-key` | tells you where the API key file is (never prints the key) |

## Topology

```text
host 127.0.0.1 only:  gateway/lb :18080   control plane :19090   Prometheus :19091   Grafana :13000   Jaeger UI :16686
edge network (only services that publish a port) -------------------------------------------------------------
backend network (internal: true, no route to the internet, nothing else published)
   gateway x N --> control plane <-- worker-agent-1..3 --> mock-worker-1..3 (100 / 60 / 20 tok/s)
      |  |  |--> postgres (keys, usage)  redis (quotas)  redpanda (events) --> usage-consumer --> postgres
      |  +-----> jaeger (OTLP/HTTP)            prometheus --scrapes--> gateway, control plane, mock workers --> grafana
   one-shot jobs: migrate, redpanda-init
```

Profiles (`PROFILES=` in `.env`, default all but `scale`): `core` (control plane, mock workers, agents, gateway), `data` (PostgreSQL, Redis,
migrate: enables API keys and rate limits), `events` (Redpanda, topic init, usage consumer; needs `data`), `observability` (Prometheus,
Grafana), `tracing` (Jaeger). `scale` (the nginx load balancer) is added automatically when `GATEWAYS` is greater than 1. With only `core`
the gateway runs with authentication and rate limiting off and logs that; `make dev-smoke` then lists every check it cannot run as `SKIPPED`.

Start-up order is by health: PostgreSQL, then `migrate`, then the gateway and the usage consumer; Redis before the gateway; Redpanda, then
`redpanda-init`; control plane before agents and gateway; each mock worker before its agent. Prometheus, Grafana and Jaeger gate nothing.

## Configuration (`.env`)

Values may contain only letters, digits and `. _ : / @ , -`; anything else, an unknown name, a port outside 1024-65535, a duplicate port
or an unknown profile is refused before Docker starts, naming the variable and never printing the value. Passwords and tokens do not
belong here (they are generated).

| Variable | Default | Meaning |
| --- | --- | --- |
| `PROFILES` | all but `scale` | comma-separated profiles; must include `core` |
| `GATEWAYS` | `1` | gateway replicas (1-8); more than 1 puts nginx in front |
| `GATEWAY_PORT`, `CONTROL_PLANE_PORT`, `PROMETHEUS_PORT`, `GRAFANA_PORT`, `JAEGER_UI_PORT` | `18080`, `19090`, `19091`, `13000`, `16686` | host ports (127.0.0.1). Empty `CONTROL_PLANE_PORT=` does not publish the control plane. |
| `SF_STRATEGY` | `round-robin` | `random`, `round-robin`, `least-active`, `least-queue` |
| `WORKER_CONCURRENCY`, `WORKER_QUEUE` | `8`, `64` | mock worker slots and queue (12 benchmark clients overload smaller ones) |
| `MOCK_n_TPS`, `MOCK_n_TTFT` | 100/60/20, 100ms/150ms/300ms | speed and time to first token of worker n |
| `MOCK_n_FAILURE_RATE`, `MOCK_n_FAILURE_MODE`, `MOCK_n_STARTUP_DELAY`, `MOCK_n_SEED` | 0, `error`, 0s, n | failure injection (modes: `error`, `unavailable`, `drop`, `midstream`) |
| `TRACING_SAMPLE_RATIO` | `1.0` | fraction of requests traced |
| `PERSIST` | `0` | `1` keeps PostgreSQL, Redis, Redpanda, Prometheus and Grafana data in named volumes |
| `PROMETHEUS_RETENTION` | `2d` | |
| `COMPOSE_PROJECT_NAME` | `serverflow-dev` | change it (and the ports) to run a second copy side by side |
| `POSTGRES_IMAGE`, `REDIS_IMAGE`, `REDPANDA_IMAGE`, `PROMETHEUS_IMAGE`, `GRAFANA_IMAGE`, `JAEGER_IMAGE`, `NGINX_IMAGE` | digest-pinned, none from Docker Hub | override to use another mirror |

The containers read the `SERVERFLOW_*` variables documented in `internal/config` (compose sets them; a test fails if a name is misspelled
or has no effect). Notable: `SERVERFLOW_METRICS_LISTEN=0.0.0.0:9100|9101` with `SERVERFLOW_METRICS_TOKEN` (the guard that refuses a
non-loopback metrics address without a token stays on), `SERVERFLOW_CONTROL_PLANE_ALLOW_INSECURE_TRANSPORT=true` (the control-plane token
crosses the private network in plain http: ADR-021), `SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT=true`.

## Secrets

`scripts/dev-up.sh` generates them on first use into `.dev/` (directory 0700, gitignored): `postgres.env`, `dsn.env`, `redis.env`, `kafka.env`,
`controlplane.env`, `metrics.env`, `grafana.env` (mode 0600), `metrics_token` (0644: Prometheus runs as `nobody` and a bind-mounted file's mode
cannot be set from Compose), and the API keys `api_key` (tenant `dev`, generous quota) and `api_key_limited` (tenant `limited`, 3 requests per
minute). A service receives only the files it needs, through `env_file`. Nothing secret is in an image, a layer, `docker history`, the compose
file, an argument list or a log (searched by `make dev-smoke` check S12 and, once, by scanning saved image layers). Anyone who can read the
Docker socket can read a container's environment: that is the same trust boundary as your machine. `make dev-reset` deletes everything.

## Persistence

By default every data directory is a size-capped tmpfs: `make dev-down` leaves no volume, and a new `make dev-up` starts empty (keys are
regenerated and tenants recreated; usage rows and metrics are gone). `PERSIST=1` adds `docker-compose.persist.yml` with named volumes
(`<project>_pgdata`, `redisdata`, `redpandadata`, `prometheusdata`, `grafanadata`); `dev-down` keeps them and `dev-reset` removes them.

## Smoke checks

`make dev-smoke` prints `PASS`, `FAIL` or `SKIPPED` per check and ends with `smoke: P of N checks passed, F failed, S skipped`; it exits non-zero
on a failure and never prints a key, token or password. Every wait is a poll with a deadline.

| Check | Asserts |
| --- | --- |
| S1 | gateway `/readyz` is 200 within 60 s and `/v1/models` lists `mock-model` |
| S2 | no key and a bad key are 401 (needs `data`) |
| S3, S4 | a non-streaming completion has `choices` and `usage`; a streaming one has at least two `data:` chunks and ends with `[DONE]` |
| S5 | 24 requests reach at least two workers (`scheduler_selections_total` in Prometheus) |
| S6 | a throwaway tenant limited to 3 requests per minute gets 200 200 200 then 429 with `Retry-After` |
| S7 | Prometheus reports the gateway(s), control plane and three mock workers up, and requests counted |
| S8 | Grafana is healthy and the four dashboards exist |
| S9 | usage rows for tenant `dev` appear (Redpanda to PostgreSQL) |
| S10 | one Jaeger trace holds gateway and mock-worker spans |
| S11 | `kill mock-worker-3`: it leaves the pool, requests stay 200, and it returns after `start` |
| S12 | no generated secret value appears in `docker compose logs` |
| S13 | with `GATEWAYS>1`: requests reach two or more replicas and the 3-per-minute tenant is admitted at most 3 of 12 times across all of them |

## Scaling the gateway

`GATEWAYS=3 make dev-up` runs three replicas on the backend network and an unprivileged nginx (`deploy/compose/nginx.conf`: Docker DNS
re-resolved every 2 s, no buffering, long read timeout for SSE) publishes the gateway port instead. Prometheus finds replicas by DNS.
Rate-limit quotas are shared through Redis (check S13). Going back to `GATEWAYS=1` removes the load balancer.

## Failure injection and chaos operations

There is no runtime control endpoint on the mock workers; failures are start-up flags and container operations. Measured on the
development Mac (Colima), with the default heartbeat settings:

- **Always failing worker.** `MOCK_3_FAILURE_RATE=1` in `.env`, then `make dev-up`: only `mock-worker-3` is recreated (its agent keeps running).
  With `MOCK_3_FAILURE_MODE=unavailable` (503, which the gateway retries on another worker, ADR-012) all clients still get 200 and
  `inference_retries_total` counts the retried attempts (30 of 30 requests succeeded, 19 retries in the measurement). With the default
  mode `error` (500) the gateway does not retry (`gateway.retry_statuses` is 502 and 503), so about a third of the requests fail: that
  is the contract, not a fault of the stack.
- **Kill a worker** (`docker compose -p <project> kill mock-worker-3`, project name from `COMPOSE_PROJECT_NAME`): the container stays down
  (the restart policy `on-failure` does not restart a `kill`). Its agent notices the dead backend and the control plane marks the worker
  not eligible within a few seconds; requests keep succeeding; `... start mock-worker-3` brings it back and it is eligible again as soon
  as the agent re-registers (checked by smoke S11).
- **Pause** (`... pause mock-worker-3`): the agent's probe times out and the worker is `FAILED` and not eligible after about 3 s (measured);
  `unpause` makes it eligible again within a few seconds. Requests during the pause all returned 200.
- **Heartbeat loss.** `... kill worker-agent-3` (an `kill`, not a `stop`: a clean stop makes the agent deregister the worker at once): the
  control plane marks the worker `suspect` and not eligible after 5 s, `UNHEALTHY` after about 11 s and `LOST` after about 31 s (the default
  `worker.suspect_timeout`, `unhealthy_timeout` and `lost_timeout`); `... start worker-agent-3` re-registers it immediately.
- **Network cut.** `docker network disconnect <project>_backend <container>` isolates a container (reconnect with `connect`).

## Networks and what they do not protect

`backend` is `internal: true` (no route out). `edge` is an ordinary bridge joined only by the services that publish a port: gateway or load balancer,
control plane, Prometheus, Grafana and Jaeger. Those five can reach the internet, and Grafana can reach `backend`. The control-plane token travels in
plain http over these networks (ADR-021 explains why and how far that is acceptable). If you do not need the control-plane API from the host, set
`CONTROL_PLANE_PORT=` (empty) in `.env`: it then lives on `backend` only. The gateway warns that `worker_networks` is empty; anyone holding the
control-plane token could register a worker at any private address.

Several checkouts share the image tags `serverflow/*:dev` and the project name `serverflow-dev` unless you set `COMPOSE_PROJECT_NAME`; then the tag
follows the project name and `dev-down`/`dev-reset` touch only that project.

Bumping an image: see "Operational notes" in ADR-021 (Dependabot's compose support is best effort). Dependabot is configured to propose only minor and patch updates of the compose images: a major version (for example Postgres 18, whose image wants its data mount at `/var/lib/postgresql` and left the tmpfs at the old path unhealthy in Dependabot's first PR, #31; Redis 8; Redpanda v26; Prometheus 3; Grafana 13) is a deliberate change made by hand, with `make dev-smoke` and a note.

## Troubleshooting

- **A port is in use.** `make dev-up` checks the host ports first and names the variable to change in `.env`. Two copies can run side by side with
  a different `COMPOSE_PROJECT_NAME` and different ports.
- **Colima / macOS.** Only paths under your home directory are visible to containers; run from a checkout there. `host.docker.internal` is not used.
- **Image pull fails (429, timeouts).** `scripts/dev-pull.sh` retries five times with growing backoff. The anonymous registries have unpublished
  limits; set the matching `*_IMAGE` variable to another mirror, or `docker login` and use the Docker Hub reference (the digest is the same).
- **Disk full in the VM.** Redpanda once leaked hundreds of MB per start; here its data is tmpfs. `docker system df` shows usage; `make dev-reset` removes
  this project's images and volumes only.
- **The stack does not become healthy.** `make dev-up` prints `docker compose ps` and the last 20 log lines of each unhealthy service (logs hold no
  secrets). `make dev-logs SERVICE=...` for more.
- **Restarting a data service loses its data.** Data directories are tmpfs, which does not survive a container stop. After `docker compose stop` and
  `start` of `redpanda` the lifecycle topic is gone and events are dropped (requests still succeed, S9 fails honestly) until `make dev-up` runs the
  `redpanda-init` job again; the same holds for PostgreSQL (tenants and keys) and Redis. Use `PERSIST=1` when that matters.
- **Out of memory.** Use fewer profiles (`PROFILES=core,data`) or give the VM more memory.
- **`make dev-up` says Compose is too old.** Compose 2.24 added `required: false`, `!reset` and `--env-file` handling this stack relies on.

## What is deliberately not here

TLS between services, secrets management, HA data stores, image signing or SBOMs, Kubernetes manifests (Phase 17 reuses these six images, the
`healthcheck` binary as an exec probe and the variable names above), a liveness endpoint on the worker agent (Phase 17 decides), native Windows
support for the scripts (use WSL2).
