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
| `scripts/quality.sh vuln` | `govulncheck` (installs it if needed) |
| `scripts/quality.sh observability` | `promtool` check of the Prometheus config and rules and the rule tests, then `internal/observability` (dashboards and rules against the metrics a running cluster exports). Needs `promtool` or Docker (`scripts/promtool.sh`; image `quay.io/prometheus/prometheus:v2.53.0`, override with `PROMTOOL_IMAGE`) |
| `scripts/quality.sh full` | lint, unit, race, integration (when configured), build. Also `make quality` |

`integration` needs a PostgreSQL and a Redis that both require a password, exactly like the CI services. Locally:

```bash
scripts/dev-postgres.sh start                   # throwaway Postgres 16, password auth, 127.0.0.1:55432
export SERVERFLOW_TEST_POSTGRES_DSN="$(scripts/dev-postgres.sh dsn)"
scripts/dev-redis.sh start                      # throwaway Redis 7, password auth, 127.0.0.1:56379
export SERVERFLOW_TEST_REDIS_ADDR="$(scripts/dev-redis.sh addr)" SERVERFLOW_TEST_REDIS_PASSWORD="$(scripts/dev-redis.sh password)"
scripts/quality.sh full
```

A local server that trusts everyone hides a missing password until CI fails (this happened once), so the
local server deliberately requires one.

## Workflows

`.github/workflows/ci.yml` runs on every pull request and on pushes to `master`:

| Job | Runs | Notes |
| --- | --- | --- |
| `lint` | `quality.sh lint` | golangci-lint installed by the official action |
| `test` | `quality.sh unit` and `race` | matrix: the minimum Go version in `go.mod` (`go-version-file`, currently 1.26), and current stable |
| `integration` | `quality.sh integration` | PostgreSQL 16 service and a password-protected Redis 7; test log uploaded on failure |
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
