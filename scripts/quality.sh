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
#   scripts/quality.sh observability  promtool on the Prometheus config, rules and rule tests, and the dashboard checks
#   scripts/quality.sh deploy       Phase 16: the container deployment's static checks (no Docker daemon, no image pulls): the
#                                   tests/deploy tests (which MUST run), hadolint, shellcheck, `docker compose config` of every
#                                   file/profile combination
#   scripts/quality.sh full         lint, unit, race (this includes the tracing tests), integration and observability (if configured), deploy, build
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

# show_failures prints what went wrong in a verbose go test log before its tail: the names of failed tests,
# failed packages, panics and data races. CI keeps only the last lines of a long log, and by then the failing
# test's name has usually scrolled away.
show_failures() {
  local log="$1"
  printf '\n--- failed tests, packages, panics and races in the full log ---\n' >&2
  grep -E '^(--- FAIL|    --- FAIL|FAIL|panic:|WARNING: DATA RACE)' "$log" >&2 || printf '(none matched; see the tail below)\n' >&2
  printf -- '--- last 80 lines ---\n' >&2
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
    show_failures "$log"
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
  observability_integration
}

# The Redis and PostgreSQL metric families the dashboards and alerts use (redis_up, the pool series) come from
# the real clients; this stanza makes sure that check ran against real servers and did not skip.
observability_integration() {
  [ -n "${SERVERFLOW_TEST_POSTGRES_DSN:-}" ] || fail "observability integration tests need SERVERFLOW_TEST_POSTGRES_DSN (see docs/development/ci.md)"
  must_run observability SERVERFLOW_TEST_REDIS_ADDR "is not set; skipping the Redis and PostgreSQL families check" \
    "./internal/observability/..." TestRedisAndPostgresFamiliesAreExported
}

# tracing runs the tracing tests with the race detector and fails if a named acceptance test did not run and pass.
# They need no Docker and no service: spans go to in-memory exporters and a fake OTLP receiver.
tracing_tests() {
  local log t
  log="$(mktemp "${TMPDIR:-/tmp}/serverflow-tracing.XXXXXX")"
  step "tracing tests must run, not skip"
  if ! go test -race -count=1 -v ./internal/tracing/... ./internal/gateway/... ./internal/mockworker/... ./internal/config/... ./tests/integration/... >"$log" 2>&1; then
    show_failures "$log"
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

# Phase 10: the Prometheus configuration, recording and alert rules and their promtool tests, then the dashboard
# and rule checks against the metrics a running cluster exports (internal/observability). promtool comes from
# scripts/promtool.sh (a binary on PATH, else a container); without either this fails here, and `full` reports
# a loud skip locally and fails in CI.
observability() {
  export SERVERFLOW_REQUIRE_PROMTOOL=1
  step "promtool check config"
  scripts/promtool.sh check config observability/prometheus/prometheus.yml
  step "promtool check config (container stack, syntax only: the token file is generated at run time)"
  scripts/promtool.sh check config --syntax-only observability/prometheus/compose/prometheus.yml
  step "promtool check rules"
  scripts/promtool.sh check rules observability/prometheus/rules/recording.yml observability/prometheus/rules/alerts.yml
  step "promtool test rules"
  scripts/promtool.sh test rules observability/prometheus/tests/recording.test.yml observability/prometheus/tests/alerts.test.yml
  step "dashboards, rules and exported metrics"
  go test -race -count=1 ./internal/observability/...
}

# ---------------------------------------------------------------------------------------------------------
# Phase 16: static checks of the container deployment (docs/operations/docker-deployment.md, ADR-021). No Docker
# daemon and no image pulls: the tests parse the Dockerfile, the compose files, the Prometheus configuration and the
# scripts; `docker compose config` needs only the CLI. hadolint, shellcheck and the compose CLI are required in CI
# (CI=true) and reported as skipped, loudly, on a laptop that lacks them.
deploy() {
  local log t
  log="$(mktemp "${TMPDIR:-/tmp}/serverflow-deploy.XXXXXX")"
  step "deploy tests must run, not skip"
  if ! go test -race -count=1 -v ./tests/deploy/... ./cmd/healthcheck/... ./internal/version/... \
    -run 'Test' >"$log" 2>&1; then
    show_failures "$log"
    tail -80 "$log"
    printf 'quality: full log kept at %s\n' "$log" >&2
    exit 1
  fi
  for t in TestDockerfile TestDockerignore TestGitignoreCoversGeneratedFiles TestComposeStructure TestOverrideFilesOnlyNarrow \
    TestStartupOrdering TestPrometheusComposeMatchesLocal TestTargetsMatchComposeServices TestEnvVariablesAreHonoured \
    TestMockWorkerCommandsParse TestControlPlaneClientsOptIntoPlainHTTP TestMakefileTargets TestDevUpCheckEnv TestNoSecretsCommitted TestComposeLeavesNoDefaultSecrets \
    TestHealthy2xx TestNon2xxFails TestRedirectIsNotFollowed TestTimeout TestNonHTTPPortAndClosedPort TestOnlyLoopbackAndWellFormedArguments; do
    grep -q -- "--- PASS: $t " "$log" || fail "expected $t to run and pass"
  done
  rm -f "$log"
  printf 'deploy: the named deployment tests ran and passed\n'
  # The configuration guards the container stack leans on (W1 and W3 of the plan), with the race detector like the rest.
  log="$(mktemp "${TMPDIR:-/tmp}/serverflow-deploy.XXXXXX")"
  go test -race -count=1 -v -run 'TestControlPlaneInsecureTransport|TestTracingAllowInsecureTransportEnv' ./internal/config/ >"$log" 2>&1 || { tail -60 "$log"; fail "the control-plane/tracing insecure-transport tests failed"; }
  for t in TestControlPlaneInsecureTransportDefaultsOff TestControlPlaneInsecureTransportMatrix TestControlPlaneInsecureTransportEnv TestTracingAllowInsecureTransportEnv; do
    grep -q -- "--- PASS: $t " "$log" || fail "expected $t to run and pass"
  done
  rm -f "$log"

  step "hadolint"
  if command -v hadolint >/dev/null 2>&1; then
    hadolint deploy/docker/Dockerfile
  elif [ "${CI:-}" = "true" ]; then
    fail "hadolint is not installed in CI"
  else
    printf 'quality: SKIPPING hadolint (not installed; CI runs the pinned release).\n'
  fi

  step "shellcheck"
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck scripts/dev-up.sh scripts/dev-smoke.sh scripts/dev-pull.sh
  elif [ "${CI:-}" = "true" ]; then
    fail "shellcheck is not installed in CI"
  else
    printf 'quality: SKIPPING shellcheck (not installed; CI runs it).\n'
  fi

  step "docker compose config (every file and profile combination, no secrets needed)"
  if docker compose version >/dev/null 2>&1; then
    local envfile=deploy/compose/.env.example prof files
    for prof in core core,data core,data,events core,data,observability core,tracing 'core,data,events,observability,tracing,scale'; do
      for files in "" "-f docker-compose.persist.yml" "-f docker-compose.scale.yml" "-f docker-compose.private-control-plane.yml" \
        "-f docker-compose.persist.yml -f docker-compose.scale.yml -f docker-compose.private-control-plane.yml"; do
        local profs=() p
        for p in ${prof//,/ }; do profs+=(--profile "$p"); done
        # shellcheck disable=SC2086 # $files is a deliberate word list
        docker compose -p serverflow-ci --env-file "$envfile" -f docker-compose.yml $files "${profs[@]}" config --no-env-resolution -q \
          || fail "docker compose config failed for profiles [$prof] files [$files]"
      done
    done
    printf 'deploy: docker compose config passed for 6 profile sets x 5 file sets\n'
  elif [ "${CI:-}" = "true" ]; then
    fail "the docker compose plugin is not available in CI"
  else
    printf 'quality: SKIPPING docker compose config (no compose plugin). CI runs it.\n'
  fi

  step "YAML syntax of the deployment files"
  if command -v python3 >/dev/null 2>&1; then
    python3 scripts/check-yaml.py docker-compose.yml docker-compose.persist.yml docker-compose.scale.yml docker-compose.private-control-plane.yml \
      observability/prometheus/compose/prometheus.yml observability/prometheus/compose/targets.yml 2>&1 | tail -3 || true
  fi
}

have_promtool() { command -v promtool >/dev/null 2>&1 || { command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; }; }

# Fails on any vulnerability govulncheck reports as called (stdlib findings included; the toolchain CI uses
# fixes those). There is no exception list: ADR-018 records the one that existed and its removal.
# v1.1.4 panics ("unexpected expr: *ast.KeyValueExpr") on Go 1.27, which CI's "stable" resolves to.
vuln() {
  step "govulncheck"
  if ! command -v govulncheck >/dev/null 2>&1; then
    go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
    PATH="$(go env GOPATH)/bin:$PATH"
  fi
  govulncheck ./... || fail "govulncheck: vulnerabilities found"
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
  if have_promtool; then
    observability
  elif [ "${CI:-}" = "true" ]; then
    fail "neither promtool nor Docker is available in CI"
  else
    printf '\nquality: SKIPPING observability (no promtool binary and no running Docker). CI will run it.\n'
  fi
  deploy
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
  observability) observability ;;
  deploy) deploy ;;
  full) full ;;
  *) printf 'usage: %s lint|unit|race|integration|build|vuln|tracing|observability|deploy|full\n' "$0" >&2; exit 2 ;;
esac
