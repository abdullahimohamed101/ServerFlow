# Chore: raise the Go floor to 1.26

Status: Completed (2026-10-09)

## Outcome

The repository's minimum Go version is 1.26 (`go 1.26.0` in `go.mod` and `go.work`). Nothing else
changed: no dependency was bumped, tracing is untouched.

## Why

Go 1.25 is out of support now that 1.27 is current. `golang.org/x/net` v0.60.0+ (fixes GO-2026-6617,
6612, 6611, 6610, 6603) and OpenTelemetry v1.47+ need Go 1.26, so the floor has to move first.

## What changed

- `go.mod` and `go.work`: `go 1.26.0`. `go mod tidy` changed nothing else; `go.work.sum` unchanged.
- Version mentions: `README.md` (Quick Start), `docs/development/setup.md` (prerequisites table),
  `docs/development/ci.md` (the matrix row now says the minimum is currently 1.26),
  `docs/decisions/ADR-014` (dated update note), `.github/dependabot.yml` (stale "go 1.24.0" comment).
- New guard `internal/gofloor`: fails when go.mod, go.work, README, setup.md and ci.md disagree on the
  floor (verified to fail on a deliberate mismatch).
- CI needed no edit: the `minimum` matrix job and the benchmark job use `go-version-file: go.mod`;
  `stable`, integration and govulncheck use `go-version: stable`. nightly.yml is the same.
  scripts/quality.sh/.ps1, Makefile and docker-compose.yml hard-code no Go version; there is no Dockerfile.

## Verified

See the PR description for the exact commands: build, vet, four cross-compiles and tests under
`GOTOOLCHAIN=go1.26.0` and the local 1.27.1; golangci-lint 2.14.0 (built with go1.27.1, 0 issues);
the full gate with Postgres and Redis.

## Not done (deliberately), for the Phase 11 follow-up

- Bump OpenTelemetry to a 1.47+ release that fits and `golang.org/x/net` to v0.60.0 or newer.
- Delete the temporary govulncheck exception block.
- Update ADR-018.
- Revisit the `.github/dependabot.yml` ignores: pgx `>=5.9.0` is already stale (we are on v5.9.2) and
  go-redis `>=9.23.0` (declares Go 1.26) is now allowed by the floor. Left alone here to avoid an
  unrelated dependency change.

## `go get -u` candidates NOT applied

pgx v5.9.2 -> v5.11.0; go-redis v9.22.0 -> v9.23.0; prometheus client_golang v1.24.1 -> v1.25.0,
common v0.70.1 -> v0.72.0, client_model v0.6.2 -> v0.6.3, procfs v0.21.1 -> v0.22.0;
klauspost/compress v1.19.1 -> v1.20.1; golang.org/x/net v0.57.0 -> v0.61.0, sys v0.47.0 -> v0.49.0,
text v0.41.0 -> v0.43.0, sync v0.22.0 -> v0.24.0, mod v0.38.0 -> v0.42.0, tools v0.48.0 -> v0.51.0;
google.golang.org/protobuf v1.36.11 -> v1.36.12; plus indirect/test-only (puddle, cpuid, testify
v1.12.1, objx, go-internal, pty, go-str2duration, atomic, oauth2).
