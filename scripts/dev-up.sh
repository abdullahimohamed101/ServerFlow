#!/usr/bin/env bash
# One-command local deployment of ServerFlow in containers (Phase 16, ADR-021, docs/operations/docker-deployment.md).
#
#   scripts/dev-up.sh up            validate .env, generate secrets, build, start, create the dev tenants and keys
#   scripts/dev-up.sh down          stop and remove the containers and networks (named volumes of PERSIST=1 stay)
#   scripts/dev-up.sh reset         down, remove volumes, .dev/ and the serverflow/*:dev images
#   scripts/dev-up.sh smoke         scripts/dev-smoke.sh: the end-to-end checks
#   scripts/dev-up.sh audit         check the running containers: hardening, loopback-only ports, no anonymous volume
#   scripts/dev-up.sh ps|logs [SERVICE]|config|pull|psql|redis-cli|rpk [ARGS]|key
#   scripts/dev-up.sh --check-env FILE   validate an environment file and exit (needs no Docker)
#
# Secrets (database, Redis and broker passwords, control-plane, metrics and Grafana credentials, API keys) are
# generated on first use from /dev/urandom into .dev/ (mode 0700, gitignored), never echoed, never in an image,
# an argument list or a compose file. Services receive them only through env_file.
#
# Compatible with bash 3.2 (macOS /bin/bash).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DEV_DIR="${DEV_UP_DIR:-.dev}" # DEV_UP_DIR only exists so tests can point the pure-shell paths at a temporary directory
ENV_FILE=".env"
ENV_EXAMPLE="deploy/compose/.env.example"
COMPOSE_ENV="$DEV_DIR/compose.env"

say() { printf 'dev-up: %s\n' "$*"; }
die() { printf 'dev-up: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------------------------------------
# .env validation. The file is data, never code: it is parsed line by line, never sourced or evaluated, and a
# value may only contain [A-Za-z0-9._:/@-], so it cannot add a flag, a quote, a variable or a second line.
# Errors name the variable and never print its value.
# ---------------------------------------------------------------------------------------------------------
KNOWN_KEYS="PROFILES GATEWAYS GATEWAY_PORT CONTROL_PLANE_PORT PROMETHEUS_PORT GRAFANA_PORT JAEGER_UI_PORT SF_STRATEGY
WORKER_CONCURRENCY WORKER_QUEUE TRACING_SAMPLE_RATIO PERSIST PROMETHEUS_RETENTION COMPOSE_PROJECT_NAME
MOCK_1_TPS MOCK_1_TTFT MOCK_1_FAILURE_RATE MOCK_1_FAILURE_MODE MOCK_1_STARTUP_DELAY MOCK_1_SEED
MOCK_2_TPS MOCK_2_TTFT MOCK_2_FAILURE_RATE MOCK_2_FAILURE_MODE MOCK_2_STARTUP_DELAY MOCK_2_SEED
MOCK_3_TPS MOCK_3_TTFT MOCK_3_FAILURE_RATE MOCK_3_FAILURE_MODE MOCK_3_STARTUP_DELAY MOCK_3_SEED
POSTGRES_IMAGE REDIS_IMAGE REDPANDA_IMAGE PROMETHEUS_IMAGE GRAFANA_IMAGE JAEGER_IMAGE NGINX_IMAGE"

PORT_KEYS="GATEWAY_PORT CONTROL_PLANE_PORT PROMETHEUS_PORT GRAFANA_PORT JAEGER_UI_PORT"
PROFILE_SET="core data events observability tracing"

# Validated values land in these two parallel lists (bash 3.2 has no associative arrays).
V_KEYS=""
V_VALS=""

bad() { printf 'dev-up: %s: %s has an invalid value (the value is not shown)\n' "$1" "$2" >&2; return 1; }

# check_value KEY VALUE FILE: 0 when VALUE is acceptable for KEY.
check_value() {
  local k="$1" v="$2" f="$3" n
  if [ -z "$v" ]; then
    case "$k" in
      CONTROL_PLANE_PORT | *_IMAGE) return 0 ;; # empty: unpublished control plane / default image
      *) bad "$f" "$k" ;;
    esac
    return
  fi
  case "$k" in
    PROFILES)
      local p
      for p in $(printf '%s' "$v" | tr ',' ' '); do
        case " $PROFILE_SET " in *" $p "*) ;; *) bad "$f" "$k"; return 1 ;; esac
      done
      case ",$v," in *,core,*) ;; *) bad "$f" "$k"; return 1 ;; esac
      case ",$v," in *,events,*) case ",$v," in *,data,*) ;; *) bad "$f" "$k"; return 1 ;; esac ;; esac
      case "$v" in ,* | *, | *,,*) bad "$f" "$k"; return 1 ;; esac
      ;;
    GATEWAYS) is_int_in "$v" 1 8 || bad "$f" "$k" ;;
    GATEWAY_PORT | CONTROL_PLANE_PORT | PROMETHEUS_PORT | GRAFANA_PORT | JAEGER_UI_PORT) is_int_in "$v" 1024 65535 || bad "$f" "$k" ;;
    SF_STRATEGY) case "$v" in random | round-robin | least-active | least-queue) ;; *) bad "$f" "$k" ;; esac ;;
    WORKER_CONCURRENCY) is_int_in "$v" 1 1024 || bad "$f" "$k" ;;
    WORKER_QUEUE) is_int_in "$v" 0 100000 || bad "$f" "$k" ;;
    TRACING_SAMPLE_RATIO) printf '%s' "$v" | grep -Eq '^(0|1|0\.[0-9]{1,4}|1\.0{1,4})$' || bad "$f" "$k" ;;
    PERSIST) case "$v" in 0 | 1) ;; *) bad "$f" "$k" ;; esac ;;
    PROMETHEUS_RETENTION) printf '%s' "$v" | grep -Eq '^[0-9]{1,3}[mhd]$' || bad "$f" "$k" ;;
    COMPOSE_PROJECT_NAME) printf '%s' "$v" | grep -Eq '^[a-z0-9][a-z0-9_-]{0,39}$' || bad "$f" "$k" ;;
    MOCK_?_TPS)
      if printf '%s' "$v" | grep -Eq '^[0-9]{1,6}(\.[0-9]{1,3})?$' && [ "${v%%.*}" -ge 1 ]; then :; else bad "$f" "$k"; fi
      ;;
    MOCK_?_TTFT | MOCK_?_STARTUP_DELAY) printf '%s' "$v" | grep -Eq '^[0-9]{1,5}(\.[0-9]{1,3})?(ms|s|m)$' || bad "$f" "$k" ;;
    MOCK_?_FAILURE_RATE) printf '%s' "$v" | grep -Eq '^(0|1|0\.[0-9]{1,4}|1\.0{1,4})$' || bad "$f" "$k" ;;
    MOCK_?_FAILURE_MODE) case "$v" in error | unavailable | drop | midstream) ;; *) bad "$f" "$k" ;; esac ;;
    MOCK_?_SEED) is_int_in "$v" 0 999999999 || bad "$f" "$k" ;;
    *_IMAGE)
      # host/path[:tag][@sha256:digest]; an explicit non-Docker-Hub host; never :latest.
      printf '%s' "$v" | grep -Eq '^[a-z0-9-]+(\.[a-z0-9-]+)+(:[0-9]+)?/[a-z0-9._/-]+(:[A-Za-z0-9._-]+)?(@sha256:[0-9a-f]{64})?$' || { bad "$f" "$k"; return 1; }
      case "$v" in docker.io/* | index.docker.io/* | registry-1.docker.io/*) bad "$f" "$k"; return 1 ;; esac
      case "$v" in *:latest | *:latest@*) bad "$f" "$k"; return 1 ;; esac
      ;;
    *) bad "$f" "$k" ;;
  esac
  n=$?
  return $n
}

is_int_in() {
  printf '%s' "$1" | grep -Eq '^[0-9]{1,9}$' || return 1
  [ "$1" -ge "$2" ] && [ "$1" -le "$3" ]
}

# parse_env FILE: validates FILE into V_KEYS/V_VALS or returns non-zero after naming every problem.
parse_env() {
  local f="$1" line lineno=0 k v rc=0 first=1
  [ -f "$f" ] || { printf 'dev-up: %s does not exist\n' "$f" >&2; return 1; }
  V_KEYS="" V_VALS=""
  # shellcheck disable=SC2094 # check_value only prints the file's name
  while IFS= read -r line || [ -n "$line" ]; do
    lineno=$((lineno + 1))
    if [ "$first" = 1 ]; then first=0; line="${line#$'\xef\xbb\xbf'}"; fi # a UTF-8 byte order mark
    line="${line%$'\r'}"                                                  # Windows line endings
    case "$line" in '' | '#'*) continue ;; esac
    if ! printf '%s' "$line" | grep -Eq '^[A-Z][A-Z0-9_]*=[A-Za-z0-9._:/@,-]*$'; then
      # Name the variable when the line has one; never print the value.
      k="${line%%=*}"
      if printf '%s' "$k" | grep -Eq '^[A-Z][A-Z0-9_]*$' && [ "$k" != "$line" ]; then
        printf 'dev-up: %s line %d: %s has an invalid value (only letters, digits and . _ : / @ , - are allowed; the value is not shown)\n' "$f" "$lineno" "$k" >&2
      else
        printf 'dev-up: %s line %d is not NAME=VALUE (the line is not shown)\n' "$f" "$lineno" >&2
      fi
      rc=1
      continue
    fi
    k="${line%%=*}"
    v="${line#*=}"
    case " $(printf '%s' "$KNOWN_KEYS" | tr '\n' ' ') " in
      *" $k "*) ;;
      *) printf 'dev-up: %s line %d: unknown variable %s\n' "$f" "$lineno" "$k" >&2; rc=1; continue ;;
    esac
    case " $V_KEYS " in *" $k "*) printf 'dev-up: %s line %d: %s is set twice\n' "$f" "$lineno" "$k" >&2; rc=1; continue ;; esac
    if ! check_value "$k" "$v" "$f"; then rc=1; continue; fi
    V_KEYS="$V_KEYS $k"
    V_VALS="$V_VALS
$k=$v"
  done <"$f"
  [ "$rc" = 0 ] || return 1
  check_unique_ports "$f"
}

# val KEY DEFAULT: the validated value (or DEFAULT when the key is absent). An empty value stays empty.
val() {
  local k="$1" d="$2" l
  case " $V_KEYS " in
    *" $k "*)
      l="$(printf '%s\n' "$V_VALS" | grep "^$k=" | head -1)"
      printf '%s' "${l#*=}"
      ;;
    *) printf '%s' "$d" ;;
  esac
}

check_unique_ports() {
  local seen=" " k v
  for k in $PORT_KEYS; do
    case "$k" in GATEWAY_PORT) v="$(val "$k" 18080)" ;; CONTROL_PLANE_PORT) v="$(val "$k" 19090)" ;; PROMETHEUS_PORT) v="$(val "$k" 19091)" ;; GRAFANA_PORT) v="$(val "$k" 13000)" ;; *) v="$(val "$k" 16686)" ;; esac
    [ -n "$v" ] || continue
    case "$seen" in *" $v "*) printf 'dev-up: %s: %s uses a port another variable already uses\n' "$1" "$k" >&2; return 1 ;; esac
    seen="$seen$v "
  done
}

if [ "${1:-}" = "--check-env" ]; then
  [ $# -eq 2 ] || die "usage: scripts/dev-up.sh --check-env FILE"
  parse_env "$2" && say "$2 is valid" && exit 0
  exit 1
fi

CMD="${1:-}"
[ $# -gt 0 ] && shift

# ---------------------------------------------------------------------------------------------------------
# Resolved settings (after parse_env).
# ---------------------------------------------------------------------------------------------------------
load_settings() {
  if [ ! -f "$ENV_FILE" ]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    say "created $ENV_FILE from $ENV_EXAMPLE (ports and options live there; it holds no secrets)"
  fi
  parse_env "$ENV_FILE" || die "$ENV_FILE is invalid; nothing was started"
  PROFILES="$(val PROFILES core,data,events,observability,tracing)"
  GATEWAYS="$(val GATEWAYS 1)"
  PROJECT="$(val COMPOSE_PROJECT_NAME serverflow-dev)"
  PERSIST="$(val PERSIST 0)"
  GATEWAY_PORT="$(val GATEWAY_PORT 18080)"
  CONTROL_PLANE_PORT="$(val CONTROL_PLANE_PORT 19090)"
  PROMETHEUS_PORT="$(val PROMETHEUS_PORT 19091)"
  GRAFANA_PORT="$(val GRAFANA_PORT 13000)"
  JAEGER_UI_PORT="$(val JAEGER_UI_PORT 16686)"
  # CONTROL_PLANE_PORT present but empty means: do not publish it.
  CP_PRIVATE=0
  case " $V_KEYS " in *" CONTROL_PLANE_PORT "*) [ -z "$CONTROL_PLANE_PORT" ] && CP_PRIVATE=1 ;; esac
  has_profile() { case ",$PROFILES," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }
}

compose_files() {
  FILES=(-f docker-compose.yml)
  [ "$PERSIST" = 1 ] && FILES+=(-f docker-compose.persist.yml)
  [ "$GATEWAYS" -gt 1 ] && FILES+=(-f docker-compose.scale.yml)
  [ "$CP_PRIVATE" = 1 ] && FILES+=(-f docker-compose.private-control-plane.yml)
  return 0
}

# dc ARGS...: docker compose for this project with the sanitized environment and active profiles.
dc() {
  local profs=() p
  for p in $(printf '%s' "$PROFILES" | tr ',' ' '); do profs+=(--profile "$p"); done
  [ "$GATEWAYS" -gt 1 ] && profs+=(--profile scale)
  docker compose -p "$PROJECT" --env-file "$COMPOSE_ENV" "${FILES[@]}" "${profs[@]}" "$@"
}

# ---------------------------------------------------------------------------------------------------------
# Secrets: generated once, kept across runs (a PERSIST=1 database keeps its password), never printed.
# ---------------------------------------------------------------------------------------------------------
rand_hex() { od -An -N"${1:-24}" -tx1 /dev/urandom | tr -d ' \n'; }

write_secret() { # FILE CONTENT-with-placeholders-already-expanded; mode 0600
  local f="$DEV_DIR/$1"
  [ -s "$f" ] && return 0
  (umask 077 && printf '%s\n' "$2" >"$f")
  chmod 600 "$f"
}

ensure_secrets() {
  umask 077
  mkdir -p "$DEV_DIR"
  chmod 700 "$DEV_DIR"
  local pg metrics
  if [ ! -s "$DEV_DIR/postgres.env" ]; then
    pg="$(rand_hex)"
    write_secret postgres.env "POSTGRES_PASSWORD=$pg"
    write_secret dsn.env "SERVERFLOW_POSTGRES_DSN=postgres://serverflow:$pg@postgres:5432/serverflow?sslmode=disable"
  fi
  [ -s "$DEV_DIR/redis.env" ] || write_secret redis.env "SERVERFLOW_REDIS_PASSWORD=$(rand_hex)"
  [ -s "$DEV_DIR/kafka.env" ] || write_secret kafka.env "SERVERFLOW_EVENTS_SASL_PASSWORD=$(rand_hex)"
  [ -s "$DEV_DIR/controlplane.env" ] || write_secret controlplane.env "SERVERFLOW_CONTROL_PLANE_TOKEN=$(rand_hex 32)"
  if [ ! -s "$DEV_DIR/metrics.env" ]; then
    metrics="$(rand_hex 32)"
    write_secret metrics.env "SERVERFLOW_METRICS_TOKEN=$metrics"
    # The Prometheus container runs as `nobody` and compose cannot set the mode of a bind-mounted file, so this one
    # file is world-readable; its directory is 0700 and the token only protects a throwaway local stack.
    (umask 022 && printf '%s' "$metrics" >"$DEV_DIR/metrics_token")
    chmod 644 "$DEV_DIR/metrics_token"
  fi
  [ -s "$DEV_DIR/grafana.env" ] || write_secret grafana.env "GF_SECURITY_ADMIN_PASSWORD=$(rand_hex)"
}

# Writes the environment compose reads: the validated .env plus what the active profiles imply. Nothing secret.
# Image tag: "dev" for the default project, else the project name, so two checkouts with different COMPOSE_PROJECT_NAME
# do not share (and dev-reset does not delete) each other's serverflow/*:TAG images.
image_tag() { if [ "$PROJECT" = serverflow-dev ]; then printf dev; else printf '%s' "$PROJECT"; fi; }

write_compose_env() {
  local f="$COMPOSE_ENV.tmp" version commit
  version="$(git describe --always --dirty 2>/dev/null || echo dev)"
  commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  version="$(printf '%s' "$version" | tr -c 'A-Za-z0-9._-' '-')"
  commit="$(printf '%s' "$commit" | tr -c 'A-Za-z0-9' '-')"
  {
    printf '%s\n' "$V_VALS" | grep -v '^$' || true
    printf 'SF_VERSION=%s\nSF_COMMIT=%s\nSF_TAG=%s\n' "$version" "$commit" "$(image_tag)"
    # Resolved settings for scripts/dev-smoke.sh and the make targets (not read by compose).
    printf 'SF_PROFILES=%s\nSF_GATEWAYS=%s\nSF_GATEWAY_PORT=%s\nSF_CONTROL_PLANE_PORT=%s\nSF_PROMETHEUS_PORT=%s\nSF_GRAFANA_PORT=%s\nSF_JAEGER_UI_PORT=%s\nSF_PERSIST=%s\n' \
      "$PROFILES" "$GATEWAYS" "$GATEWAY_PORT" "$CONTROL_PLANE_PORT" "$PROMETHEUS_PORT" "$GRAFANA_PORT" "$JAEGER_UI_PORT" "$PERSIST"
    if has_profile data; then
      printf 'SF_AUTH_MODE=required\nSF_RATE_LIMIT_MODE=required\n'
    else
      printf 'SF_AUTH_MODE=off\nSF_RATE_LIMIT_MODE=off\n'
    fi
    if has_profile events; then printf 'SF_EVENTS_MODE=on\n'; else printf 'SF_EVENTS_MODE=off\n'; fi
    if has_profile tracing; then
      printf 'SF_TRACING_ENABLED=true\nSF_OTLP_ENDPOINT=http://jaeger:4318\n'
    else
      printf 'SF_TRACING_ENABLED=false\nSF_OTLP_ENDPOINT=\n'
    fi
  } >"$f"
  # A duplicate key would be an error for compose: the explicit lines above win over the user's, which cannot
  # set them (they are not in KNOWN_KEYS).
  mv "$f" "$COMPOSE_ENV"
}

# ---------------------------------------------------------------------------------------------------------
# Pre-flight.
# ---------------------------------------------------------------------------------------------------------
preflight_tools() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH"
  docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable (is Docker, Colima or Docker Desktop running?)"
  local v major minor
  v="$(docker compose version --short 2>/dev/null || true)"
  [ -n "$v" ] || die "the Docker Compose plugin is missing (need Compose 2.24 or newer)"
  v="${v#v}"
  major="${v%%.*}"
  minor="${v#*.}"
  minor="${minor%%.*}"
  if ! { [ "$major" -gt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -ge 24 ]; }; } 2>/dev/null; then
    die "Docker Compose $v is too old; 2.24 or newer is required"
  fi
  command -v curl >/dev/null 2>&1 || die "curl is required"
  [ "${1:-}" = "verbose" ] || return 0
  if docker buildx version >/dev/null 2>&1; then say "builder: BuildKit (buildx), compose $v"; else say "builder: classic (no buildx plugin; works, no build cache between builds), compose $v"; fi
}

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# Ports our own running project already publishes are not collisions (up is idempotent).
own_ports() {
  docker ps --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Ports}}' 2>/dev/null | grep -Eo '127\.0\.0\.1:[0-9]+' | cut -d: -f2 | sort -u || true
}

preflight_ports() {
  local mine k p var rc=0
  mine="$(own_ports)"
  for k in GATEWAY_PORT CONTROL_PLANE_PORT PROMETHEUS_PORT GRAFANA_PORT JAEGER_UI_PORT; do
    eval "p=\$$k"
    [ -n "$p" ] || continue
    case "$k" in
      PROMETHEUS_PORT | GRAFANA_PORT) has_profile observability || continue ;;
      JAEGER_UI_PORT) has_profile tracing || continue ;;
    esac
    printf '%s\n' "$mine" | grep -qx "$p" && continue
    if port_in_use "$p"; then
      printf 'dev-up: port %s (%s) is already in use on 127.0.0.1; set %s to a free port in %s\n' "$p" "$k" "$k" "$ENV_FILE" >&2
      rc=1
    fi
    var="$k"
  done
  : "$var"
  [ "$rc" = 0 ] || die "nothing was started"
}

preflight_disk() {
  local img avail
  img="$(dc config --images 2>/dev/null | grep -E '/postgres:' | head -1 || true)"
  [ -n "$img" ] && docker image inspect "$img" >/dev/null 2>&1 || return 0
  avail="$(docker run --rm --pull never --network none --entrypoint df "$img" -Pk / 2>/dev/null | awk 'NR==2{print int($4/1024)}' || true)"
  if [ -n "$avail" ] && [ "$avail" -lt 5120 ]; then
    say "WARNING: the container engine reports only ${avail} MB free; free some space or run 'docker system df'"
  fi
}

# ---------------------------------------------------------------------------------------------------------
# up / down / reset
# ---------------------------------------------------------------------------------------------------------
# wait_stack SECONDS: succeeds when every container of the project is up and healthy (no healthcheck counts as up) or,
# for the one-shot jobs, exited with 0. Fails at once on an unhealthy container or a failed job, and at the deadline.
wait_stack() {
  local deadline=$(($(date +%s) + $1)) rows pending="" bad
  while :; do
    rows="$(dc ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}' 2>/dev/null || true)"
    if [ -n "$rows" ]; then
      bad="$(printf '%s\n' "$rows" | awk -F'|' '($2=="exited" && $4!="0") || $3=="unhealthy" || $2=="dead" {print $1}')"
      if [ -n "$bad" ]; then printf 'dev-up: not healthy: %s\n' "$(printf '%s' "$bad" | tr '\n' ' ')" >&2; return 1; fi
      pending="$(printf '%s\n' "$rows" | awk -F'|' '!(($2=="running" && ($3=="" || $3=="healthy")) || ($2=="exited" && $4=="0")) {print $1}')"
      [ -n "$pending" ] || return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then printf 'dev-up: timed out waiting for: %s\n' "$(printf '%s' "$pending" | tr '\n' ' ')" >&2; return 1; fi
    sleep 2
  done
}

wait_failed() {
  printf 'dev-up: the stack did not become healthy; status and the last log lines of the unhealthy services follow\n' >&2
  dc ps -a >&2 || true
  local svc
  for svc in $(dc ps -a --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null | awk '$2!="running" || ($3!="" && $3!="healthy") {print $1}' | sort -u); do
    printf -- '--- %s (last 20 lines) ---\n' "$svc" >&2
    dc logs --no-color --tail 20 "$svc" >&2 2>&1 || true
  done
}

bootstrap_tenants() {
  has_profile data || return 0
  local existing
  existing="$(dc run --rm -T --no-deps admin tenant list 2>/dev/null || true)"
  if ! printf '%s\n' "$existing" | awk -F'\t' '$2=="dev"{f=1} END{exit !f}'; then
    dc run --rm -T --no-deps admin tenant create dev --rpm 6000 --tpm 100000000 --max-concurrent 64 >/dev/null 2>&1 || die "could not create the dev tenant"
    rm -f "$DEV_DIR/api_key"
  fi
  if ! printf '%s\n' "$existing" | awk -F'\t' '$2=="limited"{f=1} END{exit !f}'; then
    dc run --rm -T --no-deps admin tenant create limited --rpm 3 --tpm 100000 --max-concurrent 2 >/dev/null 2>&1 || die "could not create the limited tenant"
    rm -f "$DEV_DIR/api_key_limited"
  fi
  # A key is shown exactly once, on standard output; it goes straight into a 0600 file and never through a terminal.
  if [ ! -s "$DEV_DIR/api_key" ]; then
    (umask 077 && dc run --rm -T --no-deps admin key create --tenant dev --label dev-up 2>/dev/null >"$DEV_DIR/api_key") || die "could not create the dev key"
    chmod 600 "$DEV_DIR/api_key"
    grep -Eq '^sf_[A-Za-z0-9_-]{20,}$' "$DEV_DIR/api_key" || { rm -f "$DEV_DIR/api_key"; die "the admin tool did not return a key"; }
  fi
  if [ ! -s "$DEV_DIR/api_key_limited" ]; then
    (umask 077 && dc run --rm -T --no-deps admin key create --tenant limited --label dev-up 2>/dev/null >"$DEV_DIR/api_key_limited") || die "could not create the limited key"
    chmod 600 "$DEV_DIR/api_key_limited"
    grep -Eq '^sf_[A-Za-z0-9_-]{20,}$' "$DEV_DIR/api_key_limited" || { rm -f "$DEV_DIR/api_key_limited"; die "the admin tool did not return a key"; }
  fi
}

# Services of profiles that are no longer active (an earlier run used more profiles, or GATEWAYS>1 and its load balancer)
# are not orphans to compose, so --remove-orphans leaves them running (the load balancer would even hold the gateway
# port). Remove the containers of this project whose service is not in the active set.
remove_inactive_services() {
  local active n svc
  active="$(dc --profile tools config --services 2>/dev/null)"
  [ -n "$active" ] || return 0
  for n in $(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --filter "label=com.docker.compose.oneoff=False" --format '{{.Names}}'); do
    svc="$(docker inspect "$n" --format '{{index .Config.Labels "com.docker.compose.service"}}')"
    printf '%s\n' "$active" | grep -qx "$svc" && continue
    docker rm -fv "$n" >/dev/null
    say "removed $svc (its profile is not active)"
  done
}

cmd_up() {
  local t0 t1
  t0=$(date +%s)
  load_settings
  preflight_tools verbose
  compose_files
  ensure_secrets
  write_compose_env
  remove_inactive_services
  preflight_ports
  preflight_disk
  say "profiles: $PROFILES, gateways: $GATEWAYS, project: $PROJECT"
  say "building the images (first build downloads Go modules and takes a minute or two)"
  # Pull the third-party images first, with retries, so a rate-limited registry is a clear message.
  "$ROOT/scripts/dev-pull.sh" --skip-present || die "could not pull the images (see above)"
  # One build at a time: five parallel Go compilations of the same build stage exhaust a 4 GB VM (observed: the
  # compiler is OOM-killed), and serial builds reuse the first one's layers.
  export COMPOSE_PARALLEL_LIMIT=1
  if ! dc --profile tools build ${DEV_UP_NO_CACHE:+--no-cache} >"$DEV_DIR/build.log" 2>&1; then
    tail -25 "$DEV_DIR/build.log" >&2
    die "the image build failed (full log: $DEV_DIR/build.log)"
  fi
  say "starting"
  # No `--wait`: Compose 2.38 (GitHub's runners) refuses it for services that have no healthcheck (the one-shot jobs have
  # none), Compose 5.6 tolerates that. wait_stack below does the waiting for every version.
  if ! dc up -d --remove-orphans >"$DEV_DIR/up.log" 2>&1 || ! wait_stack "${DEV_UP_WAIT_TIMEOUT:-240}"; then
    tail -5 "$DEV_DIR/up.log" >&2
    wait_failed
    die "docker compose up failed or timed out"
  fi
  bootstrap_tenants
  t1=$(date +%s)
  say "up in $((t1 - t0))s"
  print_urls
}

print_urls() {
  local host=127.0.0.1
  say "gateway        http://$host:$GATEWAY_PORT   (curl -H \"Authorization: Bearer \$(cat .dev/api_key)\" http://$host:$GATEWAY_PORT/v1/models)"
  [ -n "$CONTROL_PLANE_PORT" ] && say "control plane  http://$host:$CONTROL_PLANE_PORT   (token: .dev/controlplane.env)"
  has_profile observability && say "prometheus     http://$host:$PROMETHEUS_PORT"
  has_profile observability && say "grafana        http://$host:$GRAFANA_PORT   (user admin, password in .dev/grafana.env)"
  has_profile tracing && say "jaeger         http://$host:$JAEGER_UI_PORT"
  has_profile data && say "API keys       .dev/api_key (tenant dev), .dev/api_key_limited (3 requests per minute)"
  say "next: make dev-smoke    stop: make dev-down"
}

# Everything this project owns, by label, in case compose cannot read its files (for example after .dev/ was deleted).
remove_by_label() {
  local ids
  ids="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")"
  # shellcheck disable=SC2086 # $ids is a list of container ids
  [ -z "$ids" ] || docker rm -fv $ids >/dev/null
  ids="$(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT")"
  # shellcheck disable=SC2086 # $ids is a list of network ids
  [ -z "$ids" ] || docker network rm $ids >/dev/null 2>&1 || true
}

cmd_down() {
  load_settings
  preflight_tools
  compose_files
  local flag="" tail_args=(--remove-orphans)
  [ "${1:-}" = "-v" ] && tail_args+=(-v)
  if [ -f "$COMPOSE_ENV" ] && [ -f "$DEV_DIR/postgres.env" ]; then
    dc --profile tools --profile scale down --timeout 20 "${tail_args[@]}" >/dev/null 2>&1 || true
  fi
  remove_by_label
  : "$flag"
  # Anonymous volumes (64 hex characters) are never wanted, not even in PERSIST mode; named volumes stay unless -v.
  local anon
  anon="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" | grep -E '^[0-9a-f]{64}$' || true)"
  # shellcheck disable=SC2086 # $anon is a list of volume names
  [ -z "$anon" ] || docker volume rm $anon >/dev/null 2>&1 || true
  if [ "${1:-}" = "-v" ]; then
    local vols
    vols="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")"
    # shellcheck disable=SC2086 # $vols is a list of volume names
    [ -z "$vols" ] || docker volume rm $vols >/dev/null
  fi
  local left
  left="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')"
  [ "$left" = 0 ] || die "$left containers of project $PROJECT are still present"
  say "down: containers and networks of project $PROJECT removed"
}

cmd_reset() {
  load_settings
  preflight_tools
  cmd_down -v
  local img
  for img in gateway control-plane worker-agent mock-worker usage-consumer admin; do
    docker image rm "serverflow/$img:$(image_tag)" >/dev/null 2>&1 || true
  done
  # The classic builder leaves every superseded build (the 2.4 GB build stage and the old image layers) as dangling images.
  # Remove only the ones labelled as ours.
  docker image prune -f --filter "label=org.serverflow.stage=build" >/dev/null 2>&1 || true
  docker image prune -f --filter "label=org.opencontainers.image.title=ServerFlow" >/dev/null 2>&1 || true
  rm -rf "$DEV_DIR"
  say "reset: volumes, .dev/ and the serverflow/*:dev images removed ($ENV_FILE is kept)"
}

# audit: checks the RUNNING containers of this project against what ADR-021 promises (AC 2, 3, 5): read-only root,
# all capabilities dropped, no-new-privileges, not privileged, no process running as root, memory limit, every published port
# on 127.0.0.1, only the expected services publishing, no anonymous volume. Prints one line per violation.
cmd_audit() {
  load_settings
  preflight_tools
  local n rc=0 out total=0 pub
  for n in $(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --filter "label=com.docker.compose.oneoff=False" --format '{{.Names}}'); do
    total=$((total + 1))
    out="$(docker inspect "$n" --format '{{index .Config.Labels "com.docker.compose.service"}}|{{.HostConfig.ReadonlyRootfs}}|{{.HostConfig.CapDrop}}|{{.HostConfig.CapAdd}}|{{.HostConfig.SecurityOpt}}|{{.HostConfig.Privileged}}|{{.Config.User}}|{{.HostConfig.Memory}}|{{.HostConfig.NetworkMode}}|{{range .Mounts}}{{if and (eq .Type "volume") (eq (len .Name) 64)}}ANON{{end}}{{end}}')"
    case "$out" in
      *"|true|[ALL]|[]|[no-new-privileges:true]|false|"*) ;;
      *) printf 'audit: %s: not read-only / cap_drop ALL / no-new-privileges / unprivileged (%s)\n' "${out%%|*}" "${out%%|*}" >&2; rc=1 ;;
    esac
    # The effective user of every process in a running container, from the engine's own process table. Root is not
    # accepted for any container (no exceptions: the third-party images run as 70, 999, 101, 65534, 472, 10001).
    if [ "$(docker inspect "$n" --format '{{.State.Running}}')" = true ]; then
      if docker top "$n" 2>/dev/null | tail -n +2 | awk '{print $1}' | grep -qxE 'root|0'; then
        printf 'audit: %s runs a process as root\n' "${out%%|*}" >&2; rc=1
      fi
    fi
    case "$out" in *"ANON"*) printf 'audit: %s mounts an anonymous volume\n' "${out%%|*}" >&2; rc=1 ;; esac
    case "$out" in *"|0|"*) printf 'audit: %s has no memory limit\n' "${out%%|*}" >&2; rc=1 ;; esac
    case "$out" in *"|host|"*) printf 'audit: %s uses the host network\n' "${out%%|*}" >&2; rc=1 ;; esac
    pub="$(docker port "$n" 2>/dev/null | grep -v '^127\.0\.0\.1:' | grep -v ' -> 127\.0\.0\.1:' || true)"
    [ -z "$pub" ] || { printf 'audit: %s publishes a port that is not on 127.0.0.1\n' "${out%%|*}" >&2; rc=1; }
  done
  # Only these services may publish a port.
  for n in $(docker ps --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}} {{.Ports}}' | grep -- '->' | awk '{print $1}'); do
    case "$n" in
      *-gateway-* | *-lb-* | *-control-plane-* | *-prometheus-* | *-grafana-* | *-jaeger-*) ;;
      *) printf 'audit: %s publishes a port but is not one of gateway, lb, control-plane, prometheus, grafana, jaeger\n' "$n" >&2; rc=1 ;;
    esac
  done
  [ "$total" -gt 0 ] || die "audit: no containers of project $PROJECT are running"
  [ "$rc" = 0 ] || die "audit failed"
  say "audit: $total containers: read-only, cap_drop ALL, no-new-privileges, unprivileged, non-root, memory-limited, no anonymous volumes, published ports on 127.0.0.1 only"
}

need_stack() {
  load_settings
  preflight_tools
  compose_files
  [ -f "$COMPOSE_ENV" ] || die "the stack has not been started here yet: run make dev-up"
}

# shellcheck disable=SC2016 # redis-cli and rpk run single-quoted scripts that are expanded inside the container, where the secret is
case "$CMD" in
  secrets) ensure_secrets ;; # pure shell, no Docker: used by tests/deploy under `bash -u`
  up) cmd_up ;;
  down) cmd_down ;;
  reset) cmd_reset ;;
  smoke) exec "$ROOT/scripts/dev-smoke.sh" "$@" ;;
  audit) cmd_audit ;;
  compose) need_stack; dc --profile tools "$@" ;; # internal: used by dev-smoke.sh
  ps) need_stack; dc ps -a ;;
  logs) need_stack; dc logs --no-color --tail 100 "$@" ;;
  config) need_stack; dc config --no-env-resolution ;;
  pull) exec "$ROOT/scripts/dev-pull.sh" "$@" ;;
  psql) need_stack; dc exec postgres psql -U serverflow -d serverflow "$@" ;;
  redis-cli) need_stack; dc exec redis sh -c 'REDISCLI_AUTH="$SERVERFLOW_REDIS_PASSWORD" exec redis-cli "$@"' sh "$@" ;;
  rpk) need_stack; dc exec redpanda bash -c 'export RPK_BROKERS=redpanda:9092 RPK_USER=serverflow RPK_PASS="$SERVERFLOW_EVENTS_SASL_PASSWORD" RPK_SASL_MECHANISM=SCRAM-SHA-256; exec rpk "$@"' bash "$@" ;;
  key)
    [ -s "$DEV_DIR/api_key" ] || die "no API key yet: run make dev-up (needs the data profile)"
    say "the dev tenant's key is in $DEV_DIR/api_key (mode 0600); use it with:"
    # shellcheck disable=SC2016 # printed literally for the user to paste
    printf '  export SF_KEY=$(cat %s/api_key)\n' "$DEV_DIR"
    ;;
  *)
    printf 'usage: scripts/dev-up.sh up|down|reset|smoke|audit|ps|logs [SERVICE]|config|pull|psql|redis-cli|rpk|key | --check-env FILE\n' >&2
    exit 2
    ;;
esac
