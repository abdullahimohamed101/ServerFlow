#!/usr/bin/env bash
# The quality gate. CI (.github/workflows) and developers run THIS script, so "what CI checks" and
# "what I ran locally" are the same commands, not two lists that drift apart.
#
#   scripts/quality.sh lint         gofmt, go vet, golangci-lint
#   scripts/quality.sh unit         go test ./...   (database tests skip unless their env is set)
#   scripts/quality.sh race         go test -race ./...
#   scripts/quality.sh integration  the database-backed tests, which MUST run (a skip is a failure)
#   scripts/quality.sh build        go build, cross-compiles, go mod tidy/verify
#   scripts/quality.sh vuln         govulncheck
#   scripts/quality.sh tracing      the Phase 11 tracing tests, which MUST run (no Docker needed)
#   scripts/quality.sh full         lint, unit, race, integration (if configured), build
#
# integration needs the test servers described in docs/development/ci.md:
#   SERVERFLOW_TEST_POSTGRES_DSN  (scripts/dev-postgres.sh start; scripts/dev-postgres.sh dsn)
#   SERVERFLOW_TEST_REDIS_ADDR and SERVERFLOW_TEST_REDIS_PASSWORD  (scripts/dev-redis.sh start; addr; password)
# Unset variables make `integration` fail; `full` skips it with a loud notice instead, so a laptop
# without the servers can still run everything else.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'quality: %s\n' "$*" >&2; exit 1; }

lint() {
  step "gofmt"
  unformatted="$(gofmt -l .)"
  if [ -n "$unformatted" ]; then
    printf '%s\n' "$unformatted" >&2
    fail "gofmt: the files above are not formatted (run gofmt -w .)"
  fi
  step "go vet"
  go vet ./...
  step "golangci-lint"
  if command -v golangci-lint >/dev/null 2>&1; then
    golangci-lint run ./...
  elif [ "${CI:-}" = "true" ]; then
    fail "golangci-lint is not installed in CI"
  else
    fail "golangci-lint is not installed (https://golangci-lint.run/welcome/install/)"
  fi
}

unit() {
  step "go test"
  go test ./...
}

race() {
  step "go test -race"
  go test -race ./...
}

# must_run <label> <env var> <skip message> <test packages> <test name>...
# Runs the packages verbosely with the race detector, then fails if any test skipped for want of the
# server, or if a named test did not run and pass. In CI a silent skip would look like a green build.
must_run() {
  local label="$1" envvar="$2" skipmsg="$3" pkgs="$4"
  shift 4
  [ -n "${!envvar:-}" ] || fail "$label tests need $envvar (see docs/development/ci.md)"
  local log
  log="$(mktemp "${TMPDIR:-/tmp}/serverflow-$label.XXXXXX")"
  step "$label tests must run, not skip"
  # shellcheck disable=SC2086 # $pkgs is a deliberate word list
  if ! go test -race -count=1 -v $pkgs >"$log" 2>&1; then
    tail -80 "$log"
    printf 'quality: full log kept at %s\n' "$log" >&2
    exit 1
  fi
  if grep -q "$skipmsg" "$log"; then
    fail "$label tests were skipped ($skipmsg); the server is not configured"
  fi
  local t
  for t in "$@"; do
    grep -q -- "--- PASS: $t " "$log" || fail "expected $t to run and pass"
  done
  rm -f "$log"
  printf '%s: %d named tests ran and passed, nothing skipped\n' "$label" "$#"
}

integration() {
  must_run postgres SERVERFLOW_TEST_POSTGRES_DSN "SERVERFLOW_TEST_POSTGRES_DSN is not set" \
    "./internal/postgres/... ./internal/auth/... ./cmd/admin/... ./tests/integration/..." \
    TestMigrateUpCreatesSchemaAndIsIdempotent TestFullWorkflowAndKeyShownOnce \
    TestProcessAuthEndToEnd TestGatewayWithRealPostgresHotPathAndFloods
  must_run redis SERVERFLOW_TEST_REDIS_ADDR "SERVERFLOW_TEST_REDIS_ADDR is not set" \
    "./internal/redis/... ./internal/ratelimit/... ./internal/gateway/... ./tests/integration/..." \
    TestRunAndPingWithPassword TestRequestsBoundaryAgainstRedis TestScriptMatchesModelOnRandomSequences \
    TestFailureClosedAcrossTheMatrix TestThreeGatewaysShareOneRequestQuota \
    TestRedisFailureMatrixThroughTheGateway TestProcessRateLimitingEndToEnd
}

# tracing runs the tracing tests with the race detector and fails if a named acceptance test did not run and pass.
# They need no Docker and no service: spans go to in-memory exporters and a fake OTLP receiver.
tracing_tests() {
  local log t
  log="$(mktemp "${TMPDIR:-/tmp}/serverflow-tracing.XXXXXX")"
  step "tracing tests must run, not skip"
  if ! go test -race -count=1 -v ./internal/tracing/... ./internal/gateway/... ./internal/mockworker/... ./internal/config/... ./tests/integration/... >"$log" 2>&1; then
    tail -80 "$log"
    printf 'quality: full log kept at %s\n' "$log" >&2
    exit 1
  fi
  for t in TestOneTraceAcrossGatewayAndWorker TestARetriedRequestShowsTwoWorkerAttemptsInOneTrace TestNoSecretsInSpansAndOnlyAllowListedKeys \
    TestADeadCollectorNeverTouchesRequests TestGatewayDoesNotDependOnOpenTelemetry TestExportReachesTheReceiver TestStreamingRequestSpanTree; do
    grep -q -- "--- PASS: $t " "$log" || fail "expected $t to run and pass"
  done
  rm -f "$log"
  printf 'tracing: the named tests ran and passed\n'
}

build() {
  step "go build"
  go build ./...
  local target
  for target in linux/amd64 linux/arm64 darwin/arm64 windows/amd64; do
    step "cross-compile $target"
    GOOS="${target%/*}" GOARCH="${target#*/}" go build ./...
  done
  step "go mod verify"
  go mod verify
  step "go mod tidy (must change nothing)"
  go mod tidy -diff || fail "go.mod/go.sum are not tidy (run go mod tidy)"
}

# ---------------------------------------------------------------------------------------------------------
# EXCEPTION LIST for `vuln`. Each entry is module:OSV-id and covers exactly that advisory in exactly that
# module (an excepted id in another module, or another id in the same module, still fails). They are
# printed on every run; they are NOT fixed. Why: ADR-018 "Known vulnerability exception". The fix for
# these five golang.org/x/net advisories is x/net v0.60.0, which needs Go 1.26, and this module's floor is
# Go 1.25.0 (the same reason OpenTelemetry stays at v1.46.x).
# DELETE THIS BLOCK when the Go floor is raised to 1.26 and golang.org/x/net >= v0.60.0 is taken.
VULN_EXCEPTIONS=(
  golang.org/x/net:GO-2026-6617
  golang.org/x/net:GO-2026-6612
  golang.org/x/net:GO-2026-6611
  golang.org/x/net:GO-2026-6610
  golang.org/x/net:GO-2026-6603
)
# ---------------------------------------------------------------------------------------------------------

# v1.1.4 panics ("unexpected expr: *ast.KeyValueExpr") on Go 1.27, which CI's "stable" resolves to.
# govulncheck exits 0 in JSON mode, so scripts/vulnfilter decides: it fails on any called vulnerability that
# is not in VULN_EXCEPTIONS (stdlib findings included; they are fixed by the toolchain CI uses).
vuln() {
  step "govulncheck"
  if ! command -v govulncheck >/dev/null 2>&1; then
    go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
    PATH="$(go env GOPATH)/bin:$PATH"
  fi
  local json args=() e
  json="$(mktemp "${TMPDIR:-/tmp}/serverflow-vuln.XXXXXX")"
  govulncheck -format json ./... >"$json" || { rm -f "$json"; fail "govulncheck itself failed"; }
  for e in "${VULN_EXCEPTIONS[@]}"; do args+=(-allow "$e"); done
  go run ./scripts/vulnfilter "${args[@]}" <"$json" || { rm -f "$json"; fail "govulncheck: vulnerabilities outside the exception list"; }
  rm -f "$json"
}

full() {
  lint
  unit
  race
  if [ -n "${SERVERFLOW_TEST_POSTGRES_DSN:-}" ] && [ -n "${SERVERFLOW_TEST_REDIS_ADDR:-}" ]; then
    integration
  else
    printf '\nquality: SKIPPING integration (SERVERFLOW_TEST_POSTGRES_DSN or SERVERFLOW_TEST_REDIS_ADDR is not set). CI will run it.\n'
  fi
  build
}

case "${1:-}" in
  lint) lint ;;
  unit) unit ;;
  race) race ;;
  integration) integration ;;
  build) build ;;
  vuln) vuln ;;
  tracing) tracing_tests ;;
  full) full ;;
  *) printf 'usage: %s lint|unit|race|integration|build|vuln|tracing|full\n' "$0" >&2; exit 2 ;;
esac
