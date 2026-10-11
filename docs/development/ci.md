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
| `scripts/quality.sh full` | lint, unit, race, integration (when configured), build. Also `make quality` |

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

**Where the image comes from, honestly.** `docker.redpanda.com` is not an independent registry: an anonymous request to
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

Registry images other than Redpanda follow the same rule. The PostgreSQL and Redis images (`public.ecr.aws/docker/library`, an ECR Public mirror)
are started by workflow steps, not as `services:` containers (the runner pulls those without retries), through `scripts/ci-docker-pull.sh <image> [tarball]`:
five attempts, delay 5 s growing x3, a cache tarball restored by `actions/cache` keyed by the image reference. A rate limited anonymous pull
(`toomanyrequests: Rate exceeded`) therefore retries instead of failing the job. `internal/ciguard` fails the build if a workflow runs `docker run` or `docker pull` of a
registry image without that script, or adds a `services:` container. Not verified: a cold pull from a GitHub-hosted runner.

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

A new push to a pull request cancels the run in progress. Every job has a timeout. Actions are pinned to
commit SHAs with the version in a comment; Dependabot (`.github/dependabot.yml`) updates them and the Go
modules weekly, and is told not to take dependency versions that raise the module's Go floor.

`.github/workflows/nightly.yml` runs at 03:23 UTC and on demand: the race detector three times under CPU
load (where flaky tests show up first), the gateway overhead budget test, and `govulncheck`.

## Required checks (owner action)

Branch protection can only be set by the repository owner. In GitHub: Settings, Branches, add a rule for
`master`, require status checks to pass before merging, and select these checks:

- `lint (gofmt, vet, golangci-lint)`
- `unit and race tests (Go minimum)` and `unit and race tests (Go stable)`
- `integration (PostgreSQL)`
- `build (cross-compile, go.mod tidy and verify)`

`govulncheck` is left out on purpose. Check names change if a job's `name:` is edited.

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
