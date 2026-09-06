# Phase 0 — Repository Foundation

Status: Active
Owner: coding agent
Depends on: nothing (repo is a clean scaffold)

## Outcome

Establish the ServerFlow monorepo foundation per spec §48 and §58 Phase  ̂0:

- Go workspace/module with a buildable, testable skeleton
- Typed configuration package (file/env/flags, validated, fail-fast)
- Structured JSON logging (stdlib `log/slog`)
- Makefile + existing PowerShell quality gate
- Docker Compose skeleton (unverified locally — no Docker installed)
- CI workflow (GitHub Actions)
- Real `README.md`, `ARCHITECTURE.md`, setup docs, and ADR-001
- Acceptance: `make test`, `make lint`, `make build` succeed (or documented Windows equivalent)

## Non-Goals

- No inference, no gateway routing, no scheduler, no workers
- No Redis/Kafka/Postgres wiring — infra services exist only as a compose skeleton
- No Kubernetes, no vLLM, no Docker image builds
- No distributed behavior of any kind
- No OpenAI-compatible API yet

## Current Architecture

- Repo is a scaffold: `AGENTS.md`, template `ARCHITECTURE.md`, `docs/plans/`, `docs/decisions/` (empty), `scripts/quality.ps1` (existing quality gate), `scripts/setup-worktree.ps1`
- Git repo on `master`, a few scaffold commits, no remote
- Local tooling: **no Go, no Docker, no make**; Python 3.14 and Git available

## Proposed Design

### Module layout

Monorepo per spec §48, created as a full skeleton with `.gitkeep` placeholders. Only `internal/config` and `internal/telemetry` get real implementations in Phase 0; everything else is an empty package placeholder so the structure exists and `go build ./...` has something to compile.



### Module path

`serverflow` — no git remote exists, so no fake GitHub path. If a remote is added later, `go mod edit -module` renames it in one step. Documented in `docs/development/setup.md`.

### Configuration (`internal/config`)

- Typed structs: `Config` with `Gateway`, `Scheduler`, `Worker`, `Admission`, `Redis`, `Postgres` sections (mirroring spec §50 example)
- Load order: defaults → config file (YAML, `gopkg.in/yaml.v3` → env vars → CLI flags
- `Validate()` on startup; fail fast with a clear error
- No `os.Getenv` outside this package

### Logging (`internal/telemetry`)

- `log/slog` JSON handler (stdlib, zero deps)
- Helper to attach request/tenant/worker fields to loggers
- Level configurable via config

### Commands

- `cmd/gateway`, `cmd/worker-agent`, `cmd/control-plane`, `cmd/usage-consumer`, `cmd/benchmark`: minimal `main.go` that loads config, sets up logger, logs a startup line, exits 0 — enough to exercise the foundation packages and give `go build ./...` real work

### Build/quality

- `Makefile`: `fmt`, `vet`, `lint` (golangci-lint if present, else warn+skip), `test`, `test-race`, `build`
- Windows path: existing `scripts/quality.ps1 -Mode Full` + direct `go` commands (no `make` on this machine)
- `.github/workflows/ci.yml`: fmt, vet, test, test-race, build on ubuntu-latest

### Compose skeleton

- `docker-compose.yml`: redis, postgres, kafka (KRaft single-node), prometheus, grafana, otel-collector — pinned images, ports, healthchecks where trivial. No app services yet (no images exist). Marked as skeleton; not validated locally (no Docker)

### Docs

- `README.md`: intro, quick start, phase status table
- `ARCHITECTURE.md`: replace template with real Phase 0 content (overview, components, boundaries, external systems, testing, dev commands, risks
- `docs/development/setup.md`: tooling requirements, module-path note, Windows-vs-make commands
- `docs/decisions/ADR-001-go-gateway.md`: Go for gateway/control plane (spec §49

## Affected Files/Components

```
go.mod, go.work, Makefile, .gitignore, docker-compose.yml
.github/workflows/ci.yml
cmd/{gateway,worker-agent,control-plane,usage-consumer,benchmark}/main.go
internal/config/{config.go,load.go,validate.go,config_test.go}
internal/telemetry/{logger.go,logger_test.go}
internal/{api,auth,admission,gateway,scheduler,registry,worker,models,ratelimit,cache,events,postgres,redis}/.gitkeep
pkg/protocol/.gitkeep
worker/{runtime,vllm}/.gitkeep
benchmark/{workloads,reports,analysis}/.gitkeep
schemas/events/.gitkeep, migrations/.gitkeep
deploy/{docker,kubernetes,helm}/.gitkeep
observability/{prometheus,grafana,otel}/.gitkeep
tests/{integration,e2e,chaos,load}/.gitkeep
docs/{architecture,adr,benchmarks,operations,development}/.gitkeep
README.md, ARCHITECTURE.md
docs/development/setup.md
docs/decisions/ADR-001-go-gateway.md
docs/plans/active/phase-0-foundation.md (this file)
```

## Acceptance Criteria

1. `go build ./...` succeeds
2. `go vet ./...` clean
3. `go test ./...` passes (config + telemetry tests)
4. `go test -race ./...` passes
5. `gofmt -l .` empty
6. `scripts/quality.ps1 -Mode Full` passes (existing gate)
7. CI workflow defined for the above
8. Docs updated: README, ARCHITECTURE, setup, ADR-001
9. No behavior beyond foundation (no routing/inference/infra wiring)



## Verification Plan

1. Install Go (needs user decision + network approval; `winget install GoLang.Go` or manual download
2. `go mod download` (first run needs network approval — pulls `gopkg.in/yaml.v3`
3. Run: `gofmt -l .`, `go vet ./...`, `go test ./...`, `go test -race ./...`, `go build ./...`
4. Run `scripts/quality.ps1 -Mode Full`
5. Docker Compose validation deferred until Docker is installed (`docker compose config`
6. When accepted, move this plan to `docs/plans/completed/`

## Risks

- **Go not installed** — acceptance cannot be fully verified until installed. Default: install latest stable via winget (needs approval). If user declines, deliver code + CI and mark local verification as pending
- **No Docker** — compose skeleton unvalidated; deferred
- **Network blocked in sandbox** — first `go mod download` requires approval
- **Module path `serverflow`** — rename needed if a remote is added later (documented)
- **Windows has no `make`** — Makefile targets mirrored by `quality.ps1` + direct go commands
- **Go version directive** — `go.mod` pins a conservative version; toolchain auto-upgrades if newer Go installed

## Ordered Implementation Steps

1. (Optional, user-approved) Create branch `feature/phase-0-foundation`
2. Create directory skeleton + `.gitkeep` placeholders
3. `go.mod` (`module serverflow`) + `go.work`
4. `internal/config`: structs, loader (defaults→YAML→env→flags), validation, tests
5. `internal/telemetry`: slog JSON logger + field helpers, tests
6. `cmd/*/main.go` stubs exercising config + logger
7. `Makefile` + `.gitignore`
8. `docker-compose.yml` skeleton
9. `.github/workflows/ci.yml`
10. Docs: README, ARCHITECTURE, setup, ADR-001
11. Verify per Verification Plan; fix issues
12. Move plan to `docs/plans/completed/` when accepted