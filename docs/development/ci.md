# Continuous integration

GitHub Actions is the source of truth for whether a change is good. This page explains what it runs,
how to run the same checks locally, and the one setting only the repository owner can change.

## One script, two places

CI and developers both run `scripts/quality.sh`. A stage that passes locally runs the same commands
in CI, so the two cannot drift apart.

| Command | What it checks |
| --- | --- |
| `scripts/quality.sh lint` | `gofmt`, `go vet`, `golangci-lint` (same version as CI, v2.14.0) |
| `scripts/quality.sh unit` | `go test ./...`; database-backed tests skip unless their server is configured |
| `scripts/quality.sh race` | `go test -race ./...` |
| `scripts/quality.sh integration` | the database-backed tests, which **must run**: a skip, or a missing named test, is a failure |
| `scripts/quality.sh build` | `go build`, cross-compiles (linux amd64/arm64, darwin arm64, windows amd64), `go mod verify`, `go mod tidy -diff` |
| `scripts/quality.sh tracing` | the Phase 11 tracing tests with the race detector; named acceptance tests **must run** (no Docker or service needed) |
| `scripts/quality.sh vuln` | `govulncheck` (installs it if needed), failing on every called vulnerability (no exception list; the one that existed was removed on 2026-10-11, ADR-018) |
| `scripts/quality.sh observability` | `promtool` check of the Prometheus config and rules and the rule tests, then `internal/observability` (dashboards and rules against the metrics a running cluster exports). Needs `promtool` or Docker (`scripts/promtool.sh`; image `quay.io/prometheus/prometheus:v2.53.0`, override with `PROMTOOL_IMAGE`) |
| `scripts/quality.sh deploy` | Phase 16, the container deployment's static checks (no Docker daemon, no image pulls): `tests/deploy` and `cmd/healthcheck` tests whose names **must run**, the W1/W3 config tests, `hadolint` on the Dockerfile, `shellcheck` on the dev scripts, and `docker compose config` for 6 profile sets x 5 file sets. `hadolint` and `shellcheck` are required in CI and skipped loudly on a laptop without them. Also `make deploy-check` |
| `scripts/quality.sh full` | lint, unit, race, integration (when configured), observability (when promtool or Docker is available), deploy, build. Also `make quality` |

`integration` needs a PostgreSQL, a Redis and a Kafka-compatible broker that all require credentials, exactly like the CI services. Locally:

```bash
scripts/dev-postgres.sh start                   # throwaway Postgres 16, password auth, 127.0.0.1:55432
export SERVERFLOW_TEST_POSTGRES_DSN="$(scripts/dev-postgres.sh dsn)"
scripts/dev-redis.sh start                      # throwaway Redis 7, password auth, 127.0.0.1:56379
export SERVERFLOW_TEST_REDIS_ADDR="$(scripts/dev-redis.sh addr)" SERVERFLOW_TEST_REDIS_PASSWORD="$(scripts/dev-redis.sh password)"
scripts/dev-kafka.sh start                      # throwaway Redpanda, SASL/SCRAM required, 127.0.0.1:59092
export SERVERFLOW_TEST_KAFKA_BROKERS="$(scripts/dev-kafka.sh brokers)" SERVERFLOW_TEST_KAFKA_USER="$(scripts/dev-kafka.sh user)" SERVERFLOW_TEST_KAFKA_PASSWORD="$(scripts/dev-kafka.sh password)"
scripts/quality.sh full
```

The Kafka tests are named in `quality.sh integration` (`must_run kafka`), so a skipped one fails the gate. The broker is
Redpanda; it is Kafka API compatible, not Apache Kafka (ADR-019).

**Where the image comes from, honestly.** The Phase 16 container stack uses `mirror.gcr.io/redpandadata/redpanda` instead, pinned by the same digest
(checked 2026-10-11: the manifest answers anonymously and the index lists linux/amd64 and linux/arm64); the survey below, which did not include
`mirror.gcr.io`, is why the Docker Hub front (`docker.redpanda.com`) was first chosen. Today `scripts/dev-kafka.sh` pulls `mirror.gcr.io` first and falls back to the Docker Hub front (see the update below). `mirror.gcr.io` is a cache run by Google with no published
availability or rate guarantee, so the compose job pulls with retries and caches the tarball; a full cold pull from a GitHub runner is not verified.

`docker.redpanda.com` is not an independent registry: an anonymous request to
`https://docker.redpanda.com/v2/redpandadata/redpanda/manifests/v25.1.1` answers 401 with
`www-authenticate: Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`, i.e. it hands out Docker Hub tokens, and
an anonymous pull is counted against Docker Hub's limit (100 pulls per hour per source address, shared by GitHub-hosted runners).
Alternatives were checked on 2026-10-10 with anonymous manifest requests and none exists: `public.ecr.aws` (`redpandadata/redpanda`,
`redpanda/redpanda` answer 404; its mirror only carries official Docker library images), `quay.io` (`redpandadata/redpanda`,
`redpanda/redpanda`, `redpanda-data/redpanda` answer 401 even with an anonymous token) and `ghcr.io` (`redpanda-data/redpanda`,
`redpandadata/redpanda` answer 403). So the CI job (a) caches the image as a `docker save` tarball with `actions/cache`, keyed by the image
reference, so most runs pull nothing, and (b) on a cache miss pulls with five attempts and growing backoff
(`scripts/dev-kafka.sh pull`). Updating the image tag changes the key and refills the cache.

**Update (2026-10-11).** A first CI run failed with `docker: toomanyrequests: Rate exceeded` on a cold pull, which is exactly the shared-runner
limit above. `mirror.gcr.io` (Google's pull-through cache) was then found to serve the same image anonymously, with the same index digest as
Docker Hub (`sha256:e52ef1cf...`, amd64 and arm64), so it is now the default image (`mirror.gcr.io/redpandadata/redpanda:v25.1.1`) and
`docker.redpanda.com` is the fallback `pull` tries once if the mirror fails (`DEV_KAFKA_IMAGE` / `DEV_KAFKA_FALLBACK_IMAGE` override both). The
mirror has no published availability guarantee, hence the fallback, the retries and the cache.

What is verified and what is not: the image pulls and runs on the development Mac, and the cache/load/retry steps were written but a **cold pull
from a GitHub-hosted runner has not been run**, so a rate-limited first run on a busy runner can still fail the Kafka step. If that
happens: re-run the job (the limit is hourly), or add a `docker login` step with a Docker Hub account kept in a repository secret before
`Start Kafka` (authenticated pulls have a higher limit). The repository owner owns that secret; none is added here. The outage test starts a second, private broker on `SERVERFLOW_TEST_KAFKA_PRIVATE_PORT_BASE`
and needs Docker.

Registry images other than Redpanda follow the same rule. The PostgreSQL and Redis images are started by workflow steps, not as `services:` containers (the
runner pulls those without retries and cannot fall back), through `scripts/ci-docker-pull.sh <image> [tarball]`. Anonymous pulls from shared runners are
throttled by request rate (`toomanyrequests: Rate exceeded`) and by data volume (`Data limit exceeded`), so the script tries a list of sources in order,
3 rounds with 5 s then 15 s between rounds, tags the winner as the canonical reference, and saves a tarball that `actions/cache` restores
(key = reference + digest from `scripts/ci-docker-pull.sh digest <image>`, so most runs pull nothing). Sources for `public.ecr.aws/docker/library/<name>`:
that, `mirror.gcr.io/library/<name>`, `docker.io/library/<name>`. Since 2026-10-16 `public.ecr.aws` has refused GitHub-hosted runners (PR #39), so on a runner the first source fails and the pull is served by `mirror.gcr.io`; the order is kept so a runner or laptop where ECR works still uses it. Checked anonymously on 2026-10-11 (manifest GET, body sha256): `postgres:16` is
`sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d` and `redis:7` is
`sha256:4fa24486b8bcca8eec45ee0eb166edc674795e53a2b53d1a9ef263eecebaac85` on all three. Optional: if `DOCKER_HUB_USERNAME` and `DOCKER_HUB_TOKEN` are in the
step's environment the script runs `docker login` first. The `Start PostgreSQL` and `Start Redis` steps of `ci.yml` and `nightly.yml` map them from the repository secrets of the same names; the repository owner creates those secrets (none exists and none is added here), and without them, or on a pull request from a fork, both values are empty and pulls stay anonymous. The Kafka and compose pulls (`scripts/dev-kafka.sh`, `scripts/dev-pull.sh`) do not log in. `internal/ciguard`
fails the build if a workflow runs `docker run` or `docker pull` of a registry image without the script, or adds a `services:` container.
Not verified: a cold pull from a GitHub-hosted runner.

A local server that trusts everyone hides a missing password until CI fails (this happened once), so the
local server deliberately requires one.

## Workflows

`.github/workflows/ci.yml` runs on every pull request and on pushes to `master`:

| Job | Runs | Notes |
| --- | --- | --- |
| `lint` | `quality.sh lint` | golangci-lint installed by the official action |
| `test` | `quality.sh unit` and `race` | matrix: the minimum Go version in `go.mod` (`go-version-file`, currently 1.26), and current stable |
| `integration` | `quality.sh integration` | PostgreSQL 16 service, a password-protected Redis 7 and a SASL-protected Redpanda (`Start Kafka` step: cached image, pull with retries); test log uploaded on failure |
| `build` | `quality.sh build` | cross-compiles, module tidiness |
| `observability (promtool, dashboards)` | `quality.sh observability` | pinned promtool 2.53.0 release archive, SHA-256 verified; no Docker Hub |
| `vulncheck` | `quality.sh vuln` | advisory on pull requests (a new advisory is not the PR's fault), blocking nightly |
| `deploy (lint, config, static tests)` | `quality.sh deploy` | Phase 16: pinned `hadolint` v2.15.1 release (SHA-256 verified), `shellcheck` and the compose CLI from the runner image; about 2 minutes, no image pulls |
| `compose smoke (advisory)` | `make dev-up`, `make dev-smoke`, `make dev-down` | Phase 16: the whole system in containers with `GATEWAYS=3` and 13 end-to-end checks that must all run (`0 failed, 0 skipped`; a single gateway skips S13); images from `mirror.gcr.io`, `quay.io` and `gcr.io` (never Docker Hub; `public.ecr.aws` was dropped on 2026-10-16 when it started refusing GitHub-hosted runners, PR #39, ADR-021) pulled by `scripts/dev-pull.sh` with retries and backoff; the Redpanda image is cached as a tarball like the integration job's; 25 minute limit; stack status and secret-free logs are uploaded on failure |

A new push to a pull request cancels the run in progress. Every job has a timeout. Actions are pinned to
commit SHAs with the version in a comment; Dependabot (`.github/dependabot.yml`) updates them and the Go
modules weekly, and is told not to take dependency versions that raise the module's Go floor.

`.github/workflows/nightly.yml` runs at 03:23 UTC and on demand: the race detector three times under CPU
load (where flaky tests show up first), the gateway overhead budget test, `govulncheck`, the container stack with `GATEWAYS=3` (so check S13,
gateway scaling and the shared quota, runs; plus `make dev-audit`) and an advisory Trivy report (HIGH and CRITICAL findings with a fix, six
ServerFlow images, scanner pinned by digest on `public.ecr.aws`; its vulnerability database download can be rate limited, which is why it never gates).

### Lessons of the first Phase 16 run

The runner's Compose is v2.38.2 (local development uses a newer one). Three things differed: `docker compose config -q` checks that `env_file` entries exist
even with `--no-env-resolution` (the static job now creates empty placeholder files with `scripts/dev-placeholders.sh` and removes them), `up --wait` rejects
services without a healthcheck (`dev-up.sh` waits itself), and a step output that nothing wrote became `docker save ""` (outputs are now read with a guard, and
`TestWorkflowOutputsAndDockerCommands` checks every `steps.X.outputs.Y`). To reproduce locally, put the v2.38.2 `docker-compose` release binary behind a `docker`
wrapper that handles the `compose` subcommand, clone the branch with `--depth 1` into a fresh directory and run the workflow steps in order.

## Required checks (owner action)

Branch protection can only be set by the repository owner. In GitHub: Settings, Branches, add a rule for
`master`, require status checks to pass before merging, and select these checks:

- `lint (gofmt, vet, golangci-lint)`
- `unit and race tests (Go minimum)` and `unit and race tests (Go stable)`
- `integration (PostgreSQL)`
- `build (cross-compile, go.mod tidy and verify)`

`govulncheck` is left out on purpose. So are `deploy (lint, config, static tests)` and `compose smoke (advisory)` for now: add the static `deploy`
job once it has been green for a while, and `compose smoke` after ten consecutive green runs on `master` (it depends on three anonymous
registries and a cold Docker pull on a shared runner, neither of which has been observed on a GitHub-hosted runner from here). Check names change if a job's `name:` is edited.

## Adding a database-backed test

Tests that need a server skip with a fixed message when its environment variable is unset. To make CI
enforce that a new family of such tests actually runs, add a `must_run` call in `integration()` in
`scripts/quality.sh` with the packages and the names of a few tests that must pass, and add the service to
the `integration` job. The first CI run on the Postgres phase failed because of exactly this kind of gap.

## Validating workflow changes

GitHub Actions cannot run on a laptop, but its files can be linted:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@latest
```

Pinned SHAs can be checked against their tags with the GitHub CLI; see the pull request that introduced
this setup for the one-liner used.
