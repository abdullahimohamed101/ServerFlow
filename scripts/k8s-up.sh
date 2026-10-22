#!/usr/bin/env bash
# The local Kubernetes deployment of ServerFlow on kind (Phase 17, docs/operations/kubernetes-deployment.md).
# This is the PR 2 skeleton: the cluster, the images, the Secrets and the data tier (PostgreSQL, Redis, Redpanda) with its
# one-shot jobs. The gateway, control plane and workers arrive in PR 3.
#
#   scripts/k8s-up.sh up        preflight, secrets, cluster, images, namespace, Secrets, data tier, jobs; idempotent
#   scripts/k8s-up.sh down      delete the kind cluster and its kubeconfig (nothing else is touched)
#   scripts/k8s-up.sh reset     down, then remove .dev/k8s/ (the images and the shared .dev/ secrets stay)
#   scripts/k8s-up.sh status    pods, volumes and jobs of the namespace
#   scripts/k8s-up.sh smoke     the data-tier checks D1-D7; the last line is "smoke: N of N checks passed, 0 failed, 0 skipped"
#   scripts/k8s-up.sh render    the rendered manifests of the kind overlay (no Secret content), safe to paste
#   scripts/k8s-up.sh check     the static checks without a cluster (render, kubeconform, tests/k8s): same as `make k8s-check`
#   scripts/k8s-up.sh audit|roll-test   not implemented yet (PR 3 and PR 6)
#   scripts/k8s-up.sh --check-context NAME   exit 0 only for the one allowed context, kind-serverflow (needs no Docker or kubectl)
#
# Safety (AC 12): every kubectl call goes through kc(), which passes the dedicated kubeconfig .dev/k8s/kubeconfig and the
# context kind-serverflow explicitly and is preceded by guard_context, which refuses any other current context and any
# API server that is not on loopback. KUBECONFIG, and so ~/.kube/config, is never read. Nothing is installed: kind and
# kubectl must be on PATH at the versions in deploy/kubernetes/tools.env.
#
# Secrets are the ones scripts/dev-up.sh generates into .dev/ (mode 0700/0600, gitignored). They reach the cluster only
# through a pipe into `kubectl apply` and are never printed or placed on a command line.
#
# Compatible with bash 3.2 (macOS /bin/bash) and bash 5 under `set -u`.
# shellcheck disable=SC2016 # the kubectl exec scripts are single-quoted on purpose: the shell inside the pod expands the Secret's value
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# A kubeconfig or context from the environment must never steer a call of this script.
unset KUBECONFIG KUBECONTEXT || true

CLUSTER=serverflow
CONTEXT="kind-$CLUSTER"
NAMESPACE=serverflow
# K8S_UP_DIR only exists so tests can point the shell paths at a temporary directory (like DEV_UP_DIR in dev-up.sh).
K8S_DIR="${K8S_UP_DIR:-.dev/k8s}"
case "$K8S_DIR" in /*) ;; *) K8S_DIR="$ROOT/$K8S_DIR" ;; esac
KUBECONFIG_FILE="$K8S_DIR/kubeconfig"
DEV_DIR="${DEV_UP_DIR:-.dev}"
OVERLAY=deploy/kubernetes/overlays/kind
KIND_CONFIG=deploy/kubernetes/kind/cluster.yaml
LOAD_RESTRICTOR=--load-restrictor=LoadRestrictionsNone
# ServerFlow images built from deploy/docker/Dockerfile and loaded into the node (PR 3 adds the other five).
IMAGES="admin"
# .dev/<name>.env -> Secret <name> (PR 3 and PR 4 add controlplane, metrics and grafana).
SECRETS="postgres dsn redis kafka"
MIN_MEMORY_MB=4096
MIN_DISK_MB=8192

say() { printf 'k8s-up: %s\n' "$*"; }
die() { printf 'k8s-up: %s\n' "$*" >&2; exit 1; }

pin() { "$ROOT/scripts/k8s-tool.sh" pin "$1"; }

# ---------------------------------------------------------------------------------------------------------
# The context guard.
# ---------------------------------------------------------------------------------------------------------
# require_context NAME: succeeds only for the one context this script may talk to.
require_context() {
  [ "$1" = "$CONTEXT" ] || die "refusing to use the kube context '$1': only $CONTEXT is allowed (the dedicated kubeconfig is $KUBECONFIG_FILE; ~/.kube/config is never read)"
}

if [ "${1:-}" = "--check-context" ]; then
  [ $# -eq 2 ] || die "usage: scripts/k8s-up.sh --check-context NAME"
  require_context "$2"
  say "$2 is the allowed context"
  exit 0
fi

# kc ARGS...: kubectl against the dedicated kubeconfig and context, nothing else.
kc() { kubectl --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" "$@"; }

# guard_context: run before the first kc of every command that talks to the cluster.
guard_context() {
  command -v kubectl >/dev/null 2>&1 || die "kubectl is not installed (the pinned version is $(pin KUBERNETES_VERSION); see docs/operations/kubernetes-deployment.md)"
  [ -f "$KUBECONFIG_FILE" ] || die "there is no cluster here: $KUBECONFIG_FILE does not exist (run make k8s-up)"
  local cur server
  cur="$(kubectl --kubeconfig "$KUBECONFIG_FILE" config current-context 2>/dev/null)" || die "cannot read the current context of $KUBECONFIG_FILE"
  require_context "$cur"
  server="$(kubectl --kubeconfig "$KUBECONFIG_FILE" config view --minify --context "$CONTEXT" -o 'jsonpath={.clusters[0].cluster.server}' 2>/dev/null)" || die "cannot read the API server of $CONTEXT"
  case "$server" in
    https://127.0.0.1:* | https://localhost:* | 'https://[::1]:'*) ;;
    *) die "refusing: the API server of $CONTEXT is not on loopback ($server)" ;;
  esac
}

# ---------------------------------------------------------------------------------------------------------
# Pre-flight.
# ---------------------------------------------------------------------------------------------------------
minor_of() { local v="${1#v}"; v="${v#*.}"; printf '%s' "${v%%.*}"; }

preflight_tools() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH"
  docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable (is Docker, Colima or Docker Desktop running?)"
  command -v kind >/dev/null 2>&1 || die "kind $(pin KIND_VERSION) is not installed; install it from the pinned release (docs/operations/kubernetes-deployment.md)"
  kind version 2>/dev/null | grep -q "kind $(pin KIND_VERSION) " || die "kind must be $(pin KIND_VERSION) (found: $(kind version 2>/dev/null | head -1))"
  command -v kubectl >/dev/null 2>&1 || die "kubectl $(pin KUBERNETES_VERSION) is not installed (docs/operations/kubernetes-deployment.md)"
  local have want diff
  have="$(kubectl version --client 2>/dev/null | sed -n 's/^Client Version: //p' | head -1)"
  want="$(pin KUBERNETES_VERSION)"
  [ -n "$have" ] || die "cannot read the kubectl version"
  diff=$(($(minor_of "$have") - $(minor_of "$want")))
  [ "$diff" -ge -1 ] && [ "$diff" -le 1 ] || die "kubectl $have is more than one minor version away from the node image's Kubernetes $want"
  say "docker context: $(docker context show 2>/dev/null || echo unknown); kind $(pin KIND_VERSION), kubectl $have, node $want"
}

preflight_memory() {
  local bytes mb
  bytes="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  mb=$((bytes / 1024 / 1024))
  [ "$mb" -ge "$MIN_MEMORY_MB" ] || die "the container engine has $mb MB of memory; a kind node and the data tier need at least $MIN_MEMORY_MB MB (Colima's default VM has less: see docs/operations/kubernetes-deployment.md). Nothing was started."
}

preflight_disk() {
  local avail
  avail="$(docker run --rm --pull never --network none --entrypoint df "$(pin KIND_NODE_IMAGE)" -Pk / 2>/dev/null | awk 'NR==2{print int($4/1024)}' || true)"
  [ -n "$avail" ] || { say "WARNING: could not measure the free disk space of the container engine"; return 0; }
  [ "$avail" -ge "$MIN_DISK_MB" ] || die "the container engine has $avail MB free; at least $MIN_DISK_MB MB are needed (docker system df shows what can be reclaimed). Nothing was started."
}

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

preflight_ports() {
  local p rc=0
  while read -r p; do
    if port_in_use "$p"; then
      printf 'k8s-up: port %s is already in use on 127.0.0.1; the host ports are fixed in %s\n' "$p" "$KIND_CONFIG" >&2
      rc=1
    fi
  done < <(grep -o 'hostPort: [0-9]*' "$KIND_CONFIG" | cut -d' ' -f2)
  [ "$rc" = 0 ] || die "nothing was started"
}

cluster_exists() { kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; }

# ---------------------------------------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------------------------------------
create_cluster() {
  umask 077
  mkdir -p "$K8S_DIR"
  chmod 700 "$K8S_DIR"
  if cluster_exists; then
    say "the cluster $CLUSTER exists; refreshing its kubeconfig"
    kind export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" >/dev/null
  else
    preflight_ports
    "$ROOT/scripts/ci-docker-pull.sh" "$(pin KIND_NODE_IMAGE)" || die "could not pull the kind node image"
    preflight_disk
    say "creating the kind cluster $CLUSTER (the first time takes a minute or two)"
    kind create cluster --name "$CLUSTER" --config "$KIND_CONFIG" --kubeconfig "$KUBECONFIG_FILE" --wait 180s >/dev/null 2>"$K8S_DIR/kind.log" || {
      tail -15 "$K8S_DIR/kind.log" >&2
      die "kind could not create the cluster (full log: $K8S_DIR/kind.log)"
    }
  fi
  chmod 600 "$KUBECONFIG_FILE"
}

image_tag() { printf dev; }

build_images() {
  local version commit img from
  version="$(git describe --always --dirty 2>/dev/null || echo dev)"
  commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  version="$(printf '%s' "$version" | tr -c 'A-Za-z0-9._-' '-')"
  commit="$(printf '%s' "$commit" | tr -c 'A-Za-z0-9' '-')"
  # The Dockerfile's base images, pulled with retries (a rate limited registry is then a clear message).
  from="$(awk 'toupper($1)=="FROM" && $2 !~ /^\$/ && $2 != "runtime" && $2 != "build" {print $2}' deploy/docker/Dockerfile | sort -u)"
  # shellcheck disable=SC2086 # $from is a list of image references
  "$ROOT/scripts/dev-pull.sh" --skip-present $from || die "could not pull the base images of the Dockerfile"
  # One build at a time: parallel Go compilations of the same build stage exhaust a small VM (Phase 16).
  for img in $IMAGES; do
    say "building serverflow/$img:$(image_tag)"
    docker build -q -f deploy/docker/Dockerfile --target "$img" -t "serverflow/$img:$(image_tag)" \
      --build-arg "VERSION=$version" --build-arg "COMMIT=$commit" . >"$K8S_DIR/build-$img.log" 2>&1 || {
      tail -25 "$K8S_DIR/build-$img.log" >&2
      die "the image build failed (log: $K8S_DIR/build-$img.log)"
    }
  done
}

# kind load docker-image can fail with a containerd image store; the archive route is the fallback (docs record which works).
load_images() {
  local img ref
  for img in $IMAGES; do
    ref="serverflow/$img:$(image_tag)"
    if ! kind load docker-image "$ref" --name "$CLUSTER" >/dev/null 2>&1; then
      say "kind load docker-image failed for $ref; trying docker save | kind load image-archive"
      docker save "$ref" -o "$K8S_DIR/$img.tar"
      kind load image-archive "$K8S_DIR/$img.tar" --name "$CLUSTER" >/dev/null || die "could not load $ref into the cluster"
      rm -f "$K8S_DIR/$img.tar"
    fi
  done
}

render() { "$ROOT/scripts/k8s-tool.sh" kubectl kustomize "$LOAD_RESTRICTOR" "$OVERLAY"; }

# apply_phase PHASE: applies the objects of the rendered overlay that carry serverflow.dev/phase=PHASE.
apply_phase() { render | kc apply -f - -l "serverflow.dev/phase=$1" >/dev/null || die "kubectl apply failed for phase $1"; }

create_secrets() {
  local n f
  for n in $SECRETS; do
    f="$DEV_DIR/$n.env"
    [ -s "$f" ] || die "$f is missing or empty (scripts/dev-up.sh secrets creates it)"
    kc -n "$NAMESPACE" create secret generic "$n" --from-env-file="$f" --dry-run=client -o yaml | kc apply -f - >/dev/null || die "could not create the Secret $n"
  done
}

# explain: what a person needs when a wait times out, without any secret (events and the last log lines).
explain() {
  printf 'k8s-up: %s; status, events and the last log lines follow\n' "$1" >&2
  kc -n "$NAMESPACE" get pods,pvc,jobs >&2 2>&1 || true
  kc -n "$NAMESPACE" get events --sort-by=.lastTimestamp 2>&1 | tail -20 >&2 || true
  local p
  for p in $(kc -n "$NAMESPACE" get pods -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
    printf -- '--- %s (last 20 lines) ---\n' "$p" >&2
    kc -n "$NAMESPACE" logs --tail 20 "$p" --all-containers >&2 2>&1 || true
  done
}

wait_statefulset() {
  kc -n "$NAMESPACE" rollout status "statefulset/$1" --timeout="${K8S_UP_WAIT:-300}s" >/dev/null 2>&1 || { explain "statefulset/$1 did not become ready"; die "statefulset/$1 did not become ready"; }
}

# wait_job NAME: succeeds when the Job has succeeded, fails at once when it has failed, and at the deadline.
wait_job() {
  local deadline=$(($(date +%s) + ${K8S_UP_WAIT:-300})) ok failed
  while :; do
    ok="$(kc -n "$NAMESPACE" get job "$1" -o 'jsonpath={.status.succeeded}' 2>/dev/null || true)"
    [ "${ok:-0}" -ge 1 ] 2>/dev/null && return 0
    failed="$(kc -n "$NAMESPACE" get job "$1" -o 'jsonpath={.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
    if [ "$failed" = True ]; then explain "job/$1 failed"; die "job/$1 failed"; fi
    if [ "$(date +%s)" -ge "$deadline" ]; then explain "job/$1 did not finish"; die "job/$1 did not finish in time"; fi
    sleep 2
  done
}

cmd_up() {
  local t0 t1
  t0=$(date +%s)
  preflight_tools
  preflight_memory
  "$ROOT/scripts/dev-up.sh" secrets
  create_cluster
  guard_context
  build_images
  load_images
  say "applying the namespace and the ServiceAccounts"
  apply_phase foundation
  create_secrets
  say "applying the data tier"
  apply_phase data
  wait_statefulset postgres
  wait_statefulset redis
  wait_statefulset redpanda
  say "running the migration and the topic jobs"
  # A finished Job's pod template cannot be changed in place; both jobs are idempotent, so delete and recreate.
  kc -n "$NAMESPACE" delete job migrate redpanda-init --ignore-not-found --wait=true >/dev/null
  apply_phase jobs
  wait_job migrate
  wait_job redpanda-init
  t1=$(date +%s)
  say "up in $((t1 - t0))s"
  say "next: make k8s-smoke    stop: make k8s-down"
}

# ---------------------------------------------------------------------------------------------------------
# down / reset / status
# ---------------------------------------------------------------------------------------------------------
cmd_down() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed"
  command -v kind >/dev/null 2>&1 || die "kind is not installed"
  mkdir -p "$K8S_DIR"
  # --kubeconfig keeps kind away from ~/.kube/config.
  if cluster_exists; then
    kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" >/dev/null || die "kind could not delete the cluster"
  fi
  rm -f "$KUBECONFIG_FILE"
  local left
  left="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER" | wc -l | tr -d ' ')"
  [ "$left" = 0 ] || die "$left containers of the kind cluster $CLUSTER are still present"
  say "down: the cluster $CLUSTER and its kubeconfig are gone"
}

cmd_reset() {
  cmd_down
  rm -rf "$K8S_DIR"
  say "reset: $K8S_DIR removed (the serverflow/*:dev images and the secrets in $DEV_DIR stay; make dev-reset removes them)"
}

cmd_status() {
  guard_context
  kc -n "$NAMESPACE" get pods,svc,pvc,jobs -o wide
}

# ---------------------------------------------------------------------------------------------------------
# smoke (data tier): each check is PASS or FAIL; the last line is the contract of the Phase 16 smoke.
# ---------------------------------------------------------------------------------------------------------
PASSED=0
FAILED=0
TOTAL=0
check() { # ID DESCRIPTION  (reads the exit status of the command given as the remaining arguments)
  local id="$1" desc="$2"
  shift 2
  TOTAL=$((TOTAL + 1))
  if "$@"; then
    PASSED=$((PASSED + 1))
    printf 'PASS %s %s\n' "$id" "$desc"
  else
    FAILED=$((FAILED + 1))
    printf 'FAIL %s %s\n' "$id" "$desc"
  fi
}

pods_ready_no_restarts() {
  local rows
  rows="$(kc -n "$NAMESPACE" get pods -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.phase}{" "}{range .status.containerStatuses[*]}{.ready}{"/"}{.restartCount}{" "}{end}{"\n"}{end}')"
  [ -n "$rows" ] || return 1
  # Completed Job pods are phase Succeeded; every other container must be ready (true/0).
  printf '%s\n' "$rows" | awk '$2=="Succeeded"{next} {for(i=3;i<=NF;i++) if($i!="true/0") bad=1} END{exit bad}'
}

job_succeeded() { [ "$(kc -n "$NAMESPACE" get job "$1" -o 'jsonpath={.status.succeeded}' 2>/dev/null || true)" = 1 ]; }

psql_q() { kc -n "$NAMESPACE" exec postgres-0 -- psql -U serverflow -d serverflow -Atc "$1"; }

topic_exists() {
  # The password is expanded inside the pod from its Secret; it never passes through this shell.
  kc -n "$NAMESPACE" exec redpanda-0 -- bash -c 'export RPK_BROKERS=redpanda:9092 RPK_USER=serverflow RPK_PASS="$SERVERFLOW_EVENTS_SASL_PASSWORD" RPK_SASL_MECHANISM=SCRAM-SHA-256; rpk topic describe inference.lifecycle.v1 -s' 2>/dev/null | grep -q 'inference.lifecycle.v1'
}

redis_cli() { kc -n "$NAMESPACE" exec redis-0 -- sh -c "$1"; }

redis_needs_auth() { redis_cli 'redis-cli ping' 2>&1 | grep -q NOAUTH && redis_cli 'REDISCLI_AUTH="$SERVERFLOW_REDIS_PASSWORD" redis-cli ping' | grep -q PONG; }

# restart_pod NAME: deletes the pod and waits for its replacement (a different UID) to be Ready.
restart_pod() {
  local old uid deadline=$(($(date +%s) + ${K8S_UP_WAIT:-300}))
  old="$(kc -n "$NAMESPACE" get pod "$1" -o 'jsonpath={.metadata.uid}')"
  kc -n "$NAMESPACE" delete pod "$1" --wait=true >/dev/null
  while :; do
    uid="$(kc -n "$NAMESPACE" get pod "$1" -o 'jsonpath={.metadata.uid}' 2>/dev/null || true)"
    if [ -n "$uid" ] && [ "$uid" != "$old" ] && [ "$(kc -n "$NAMESPACE" get pod "$1" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)" = True ]; then
      return 0
    fi
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 2
  done
}

migrate_ok() { job_succeeded migrate && [ "$(psql_q "select to_regclass('public.tenants') is not null")" = t ]; }

topic_ok() { job_succeeded redpanda-init && topic_exists; }

postgres_keeps_data() {
  local marker
  marker="m$(date +%s)"
  psql_q "create table if not exists k8s_smoke_marker(v text); insert into k8s_smoke_marker values ('$marker')" >/dev/null || return 1
  restart_pod postgres-0 || return 1
  psql_q "select count(*) from k8s_smoke_marker where v = '$marker'" | grep -qx 1 || return 1
  psql_q "drop table k8s_smoke_marker" >/dev/null
}

redpanda_keeps_topic() { restart_pod redpanda-0 && topic_exists; }

redis_resets() {
  redis_cli 'REDISCLI_AUTH="$SERVERFLOW_REDIS_PASSWORD" redis-cli set k8s_smoke 1' >/dev/null || return 1
  restart_pod redis-0 || return 1
  [ -z "$(redis_cli 'REDISCLI_AUTH="$SERVERFLOW_REDIS_PASSWORD" redis-cli get k8s_smoke' 2>/dev/null | tr -d '\r\n')" ]
}

cmd_smoke() {
  guard_context
  check D1 "every pod is ready with zero restarts" pods_ready_no_restarts
  check D2 "the migrate job succeeded and the schema exists" migrate_ok
  check D3 "the redpanda-init job succeeded and the lifecycle topic exists" topic_ok
  check D4 "redis refuses an unauthenticated client and answers PONG to the Secret's password" redis_needs_auth
  check D5 "PostgreSQL keeps its data across a pod restart (PVC)" postgres_keeps_data
  check D6 "Redpanda keeps the lifecycle topic across a pod restart (PVC)" redpanda_keeps_topic
  check D7 "Redis loses its keys across a pod restart (emptyDir, by design)" redis_resets
  printf 'smoke: %d of %d checks passed, %d failed, 0 skipped\n' "$PASSED" "$TOTAL" "$FAILED"
  [ "$FAILED" = 0 ]
}

# check: the static checks (no cluster); the same stage CI runs.
cmd_check() { exec "$ROOT/scripts/quality.sh" k8s; }

CMD="${1:-}"
[ $# -gt 0 ] && shift
case "$CMD" in
  up) cmd_up ;;
  down) cmd_down ;;
  reset) cmd_reset ;;
  status) cmd_status ;;
  smoke) cmd_smoke ;;
  render) render ;;
  check) cmd_check ;;
  audit | roll-test) die "$CMD is not implemented yet (the audit arrives with the workloads in PR 3, the rolling-update test in PR 6)" ;;
  *)
    printf 'usage: scripts/k8s-up.sh up|down|reset|status|smoke|render|check|audit|roll-test | --check-context NAME\n' >&2
    exit 2
    ;;
esac
