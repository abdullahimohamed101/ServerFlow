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
#   scripts/quality.sh full         lint, unit, race, integration (if configured), build
#
# integration needs the test servers described in docs/development/ci.md:
#   SERVERFLOW_TEST_POSTGRES_DSN  (scripts/dev-postgres.sh start; scripts/dev-postgres.sh dsn)
#   SERVERFLOW_TEST_REDIS_ADDR and SERVERFLOW_TEST_REDIS_PASSWORD  (scripts/dev-redis.sh start; addr; password)
#   SERVERFLOW_TEST_KAFKA_BROKERS, _USER and _PASSWORD  (scripts/dev-kafka.sh start; brokers; user; password)
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
  # Kafka (Phase 12). The usage tests also need the database, so they run after the Postgres block has set it up.
  must_run kafka SERVERFLOW_TEST_KAFKA_BROKERS "SERVERFLOW_TEST_KAFKA_BROKERS is not set" \
    "./internal/kafka/... ./internal/events/... ./internal/usage/... ./tests/integration/..." \
    TestProducerDeliversToRealBroker TestBrokerOutageDoesNotBlockAndDropsAreCounted \
    TestConsumerGroupCommitsAfterDatabase TestUsageStopAndReplay TestGatewayEventsEndToEnd \
    TestUsageCrashBetweenDatabaseAndOffsetCommit TestProcessGatewaySIGTERMFlushesTheTerminalEventsOfInflightRequests \
    TestProcessUsageConsumerEndToEnd
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

# v1.1.4 panics ("unexpected expr: *ast.KeyValueExpr") on Go 1.27, which CI's "stable" resolves to.
vuln() {
  step "govulncheck"
  if ! command -v govulncheck >/dev/null 2>&1; then
    go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
    PATH="$(go env GOPATH)/bin:$PATH"
  fi
  govulncheck ./...
}

full() {
  lint
  unit
  race
  if [ -n "${SERVERFLOW_TEST_POSTGRES_DSN:-}" ] && [ -n "${SERVERFLOW_TEST_REDIS_ADDR:-}" ] && [ -n "${SERVERFLOW_TEST_KAFKA_BROKERS:-}" ]; then
    integration
  else
    printf '\nquality: SKIPPING integration (SERVERFLOW_TEST_POSTGRES_DSN, SERVERFLOW_TEST_REDIS_ADDR or SERVERFLOW_TEST_KAFKA_BROKERS is not set). CI will run it.\n'
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
  full) full ;;
  *) printf 'usage: %s lint|unit|race|integration|build|vuln|full\n' "$0" >&2; exit 2 ;;
esac
