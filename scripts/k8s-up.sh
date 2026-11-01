#!/usr/bin/env bash
# The local Kubernetes deployment of ServerFlow on kind (Phase 17, docs/operations/kubernetes-deployment.md).
# The cluster, the six images, the Secrets, the data tier (PostgreSQL, Redis, Redpanda) with its one-shot jobs (PR 2) and the
# core system: control plane, gateway, mock workers with their agents, usage consumer, the tenants and keys (PR 3).
# Prometheus, Grafana and Jaeger with tracing switched on (PR 4). NetworkPolicy arrives in PR 5, the rolling-update test in PR 6.
#
#   scripts/k8s-up.sh up        preflight, secrets, cluster, images, namespace, Secrets, data tier, jobs; idempotent
#   scripts/k8s-up.sh down      delete the kind cluster and its kubeconfig (nothing else is touched)
#   scripts/k8s-up.sh reset     down, then remove .dev/k8s/ (the images and the shared .dev/ secrets stay)
#   scripts/k8s-up.sh status    pods, volumes and jobs of the namespace
#   scripts/k8s-up.sh smoke     the end-to-end checks K1-K15 and D2-D7; the last line is
#                               "smoke: N of N checks passed, 0 failed, 0 skipped"
#   scripts/k8s-up.sh audit     per container: user, read-only root, capabilities, privilege escalation, seccomp, requests and
#                               limits, token mounts; fails on any exception; the last line is "audit: N pods, M containers, 0 exceptions"
#   scripts/k8s-up.sh diagnose  status, events, descriptions and the last log lines of every pod (secret-free), for a failed run
#   scripts/k8s-up.sh render    the rendered manifests of the kind overlay (no Secret content), safe to paste
#   scripts/k8s-up.sh check     the static checks without a cluster (render, kubeconform, tests/k8s): same as `make k8s-check`
#   scripts/k8s-up.sh roll-test not implemented yet (PR 6)
#   scripts/k8s-up.sh --check-context NAME   exit 0 only for the one allowed context, kind-serverflow (needs no Docker or kubectl)
#
# Safety (AC 12): every kubectl call goes through kc(), which passes the dedicated kubeconfig .dev/k8s/kubeconfig and the
# context kind-serverflow explicitly and is preceded by guard_context, which refuses any other current context and any
# API server that is not on loopback. KUBECONFIG, and so ~/.kube/config, is never read. Nothing is installed: kind and
# kubectl must be on PATH: kind at exactly the version in deploy/kubernetes/tools.env, kubectl within one minor version of
# the node's Kubernetes (the static checks, scripts/k8s-tool.sh, use exactly the pinned kubectl). DOCKER_HOST and
# DOCKER_CONTEXT are not pinned: the script uses whatever engine `docker` points at and prints its context.
#
# Secrets are the ones scripts/dev-up.sh generates into .dev/ (mode 0700/0600, gitignored). They reach the cluster only
# through a pipe into `kubectl apply` and are never printed or placed on a command line.
#
# Compatible with bash 3.2 (macOS /bin/bash) and bash 5 under `set -u`.
# shellcheck disable=SC2016 # the kubectl exec scripts are single-quoted on purpose: the shell inside the pod expands the Secret's value
set -euo pipefail
# Never trace: a traced run would print every expanded variable. (SHELLOPTS is readonly, so it cannot be unset here; the
# generator child below is started without it.)
set +x +v

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

# A kubeconfig or context from the environment must never steer a call of this script.
unset KUBECONFIG KUBECONTEXT || true

CLUSTER=serverflow
CONTEXT="kind-$CLUSTER"
NAMESPACE=serverflow
say() { printf 'k8s-up: %s\n' "$*"; }
die() { printf 'k8s-up: %s\n' "$*" >&2; exit 1; }

# K8S_UP_DIR (tests use it) names the state directory, and `reset` deletes it, so it must be a directory strictly inside
# $ROOT/.dev: no `..` or `.` components, not .dev itself, not a symlink, no symlink on the way.
resolve_state_dir() {
  local d="$1" real a
  case "$d" in /*) ;; *) d="$ROOT/$d" ;; esac
  case "$d/" in */../* | */./*) die "refusing K8S_UP_DIR '$1': it must not contain '..' or '.' components" ;; esac
  case "$d" in *//* | */) die "refusing K8S_UP_DIR '$1': it must not contain '..', '.' or empty components" ;; esac
  [ ! -L "$d" ] || die "refusing K8S_UP_DIR '$1': it is a symlink"
  [ ! -L "$ROOT/.dev" ] || die "refusing: $ROOT/.dev is a symlink"
  case "$d" in "$ROOT/.dev/"?*) ;; *) die "refusing K8S_UP_DIR '$1': it must be inside $ROOT/.dev (and not .dev itself)" ;; esac
  # The deepest part of the path that exists must not resolve through a symlink (mkdir -p would follow it out of .dev).
  a="$d"
  while [ ! -e "$a" ] && [ ! -L "$a" ]; do a="$(dirname "$a")"; done
  real="$(cd -P "$a" && pwd -P)"
  [ "$real" = "$a" ] || die "refusing K8S_UP_DIR '$1': $a resolves through a symlink to $real"
  printf '%s' "$d"
}
K8S_DIR="$(resolve_state_dir "${K8S_UP_DIR:-.dev/k8s}")"
KUBECONFIG_FILE="$K8S_DIR/kubeconfig"
OWNER_MARK="$K8S_DIR/owns-cluster"
DEV_DIR="${DEV_UP_DIR:-.dev}"
OVERLAY=deploy/kubernetes/overlays/kind
KIND_CONFIG=deploy/kubernetes/kind/cluster.yaml
LOAD_RESTRICTOR=--load-restrictor=LoadRestrictionsNone
# ServerFlow images built from deploy/docker/Dockerfile and loaded into the node (the six of Phase 16).
IMAGES="admin gateway control-plane worker-agent mock-worker usage-consumer"
# .dev/<name>.env -> Secret <name> (grafana: the admin password; metrics: Prometheus mounts its token as a file).
SECRETS="postgres dsn redis kafka controlplane metrics grafana"
# The entry points kind maps to 127.0.0.1 (deploy/kubernetes/kind/cluster.yaml -> the NodePorts of the overlay); a test pins them.
GATEWAY_URL=http://127.0.0.1:28080
CONTROL_PLANE_URL=http://127.0.0.1:29090
PROMETHEUS_URL=http://127.0.0.1:29091
GRAFANA_URL=http://127.0.0.1:23000
JAEGER_URL=http://127.0.0.1:26686
ADMIN_POD=deploy/kubernetes/tools/admin-pod.yaml
PROBE_POD=deploy/kubernetes/tools/netpol-probe.yaml
MIN_MEMORY_MB=6144
MIN_DISK_MB=8192

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
  if [ "$diff" -lt -1 ] || [ "$diff" -gt 1 ]; then
    die "kubectl $have is more than one minor version away from the node image's Kubernetes $want"
  fi
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
    [ -f "$OWNER_MARK" ] || die "a kind cluster named $CLUSTER exists but this checkout did not create it ($OWNER_MARK is missing); refusing to deploy into it. Delete it yourself (kind delete cluster --name $CLUSTER) if it is yours."
    say "the cluster $CLUSTER exists; refreshing its kubeconfig"
    kind export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" >/dev/null
  else
    preflight_ports
    "$ROOT/scripts/ci-docker-pull.sh" "$(pin KIND_NODE_IMAGE)" || die "could not pull the kind node image"
    preflight_disk
    say "creating the kind cluster $CLUSTER (the first time takes a minute or two)"
    : >"$OWNER_MARK"
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
    kc -n "$NAMESPACE" create secret generic "$n" --from-env-file="$f" --dry-run=client -o yaml | kc apply --server-side --field-manager=serverflow-k8s-up -f - >/dev/null || die "could not create the Secret $n"
  done
}

# explain: what a person needs when a wait times out, without any secret (events and the last log lines).
explain() {
  printf 'k8s-up: %s; status, events and the last log lines follow\n' "$1" >&2
  kc -n "$NAMESPACE" get deployments,pods,pvc,jobs >&2 2>&1 || true
  kc -n "$NAMESPACE" get events --sort-by=.lastTimestamp 2>&1 | tail -20 >&2 || true
  local p
  # The one-off admin pods are left out: the log of a `key create` pod is an API key (it is deleted at once in a normal run).
  for p in $(kc -n "$NAMESPACE" get pods -l 'app.kubernetes.io/component!=admin' -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
    printf -- '--- %s (last 20 lines) ---\n' "$p" >&2
    kc -n "$NAMESPACE" logs --tail 20 "$p" --all-containers >&2 2>&1 || true
  done
}

wait_statefulset() {
  kc -n "$NAMESPACE" rollout status "statefulset/$1" --timeout="${K8S_UP_WAIT:-300}s" >/dev/null 2>&1 || { explain "statefulset/$1 did not become ready"; die "statefulset/$1 did not become ready"; }
}

wait_deployment() {
  kc -n "$NAMESPACE" rollout status "deployment/$1" --timeout="${K8S_UP_WAIT:-300}s" >/dev/null 2>&1 || { explain "deployment/$1 did not become ready"; die "deployment/$1 did not become ready"; }
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

# ---------------------------------------------------------------------------------------------------------
# The admin tool inside the cluster, and the tenants and keys.
# ---------------------------------------------------------------------------------------------------------
# admin_run ARGS...: runs the admin tool in a one-off pod (deploy/kubernetes/tools/admin-pod.yaml: the Secret `dsn`, the
# restricted security context), waits for it to finish, prints its log (standard output and error together) and deletes the
# pod. Each argument must match [A-Za-z0-9._:-]+ (they are substituted into the pod's args). A bare pod that is read after it
# ends is used instead of `kubectl run --rm -i`: attaching to a container that exits at once loses output.
admin_run() {
  local name args sep="" a phase="" deadline rc=0
  name="admin-$$-$RANDOM"
  args="["
  for a in "$@"; do
    case "$a" in
      '' | *[!A-Za-z0-9._:-]*) die "admin_run: refusing the argument '$a'" ;;
    esac
    args="$args$sep\"$a\""
    sep=","
  done
  args="$args]"
  sed -e "s|__ADMIN_POD_NAME__|$name|" -e "s|__ADMIN_ARGS__|$args|" "$ADMIN_POD" | kc create -f - >/dev/null || die "could not create the admin pod"
  deadline=$(($(date +%s) + 120))
  while :; do
    phase="$(kc -n "$NAMESPACE" get pod "$name" -o 'jsonpath={.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded | Failed) break ;; esac
    if [ "$(date +%s)" -ge "$deadline" ]; then phase=Timeout; break; fi
    sleep 1
  done
  if [ "$phase" = Succeeded ]; then
    kc -n "$NAMESPACE" logs "$name" 2>/dev/null || rc=1
  else
    rc=1
    # Never print a line that looks like an API key (the log of a failed pod has none, but it is not worth finding out).
    kc -n "$NAMESPACE" logs "$name" 2>&1 | grep -v '^sf_' | tail -5 >&2 || true
    printf 'k8s-up: the admin pod %s ended as %s\n' "$name" "${phase:-unknown}" >&2
  fi
  kc -n "$NAMESPACE" delete pod "$name" --now --ignore-not-found >/dev/null 2>&1 || true
  return "$rc"
}

# create_key TENANT FILE: a key is shown exactly once, on standard output; it goes straight into a 0600 file.
create_key() {
  local out
  out="$(admin_run key create --tenant "$1" --label k8s-up)" || return 1
  umask 077
  printf '%s\n' "$out" | grep -E '^sf_[A-Za-z0-9_-]{20,}$' | head -1 >"$2" || true
  out=""
  chmod 600 "$2" 2>/dev/null || true
  if ! grep -Eq '^sf_[A-Za-z0-9_-]{20,}$' "$2" 2>/dev/null; then
    rm -f "$2"
    return 1
  fi
}

# bootstrap_tenants: the tenants `dev` (generous) and `limited` (3 requests per minute) and a key for each, as scripts/dev-up.sh
# does for Compose, in .dev/k8s/api_key and .dev/k8s/api_key_limited (a key is valid only in the database that issued it).
bootstrap_tenants() {
  local existing
  existing="$(admin_run tenant list)" || die "could not list the tenants"
  if ! printf '%s\n' "$existing" | awk -F'\t' '$2=="dev"{f=1} END{exit !f}'; then
    admin_run tenant create dev --rpm 6000 --tpm 100000000 --max-concurrent 64 >/dev/null || die "could not create the dev tenant"
    rm -f "$K8S_DIR/api_key"
  fi
  if ! printf '%s\n' "$existing" | awk -F'\t' '$2=="limited"{f=1} END{exit !f}'; then
    admin_run tenant create limited --rpm 3 --tpm 100000 --max-concurrent 2 >/dev/null || die "could not create the limited tenant"
    rm -f "$K8S_DIR/api_key_limited"
  fi
  [ -s "$K8S_DIR/api_key" ] || create_key dev "$K8S_DIR/api_key" || die "could not create the dev key"
  [ -s "$K8S_DIR/api_key_limited" ] || create_key limited "$K8S_DIR/api_key_limited" || die "could not create the limited key"
}

# ensure_secrets: the generator Compose uses. It is started without the shell options of the environment (SHELLOPTS=xtrace would make
# it print the passwords it generates) and prints nothing.
ensure_secrets() {
  env -u SHELLOPTS -u BASHOPTS -u BASH_ENV -u ENV -u BASH_XTRACEFD -u PS4 "$ROOT/scripts/dev-up.sh" secrets
}

cmd_up() {
  local t0 t1
  t0=$(date +%s)
  preflight_tools
  preflight_memory
  ensure_secrets
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
  say "applying Prometheus, Grafana and Jaeger"
  apply_phase observability
  # Jaeger first: the gateways and workers export spans to it from their first request.
  wait_deployment jaeger
  say "applying the core system (control plane, workers, usage consumer, gateway)"
  apply_phase core
  wait_deployment control-plane
  wait_deployment mock-worker
  wait_deployment usage-consumer
  wait_deployment gateway
  wait_deployment prometheus
  wait_deployment grafana
  say "creating the tenants and keys"
  bootstrap_tenants
  t1=$(date +%s)
  say "up in $((t1 - t0))s"
  say "gateway        $GATEWAY_URL   (key: $K8S_DIR/api_key, tenant dev; $K8S_DIR/api_key_limited allows 3 requests per minute)"
  say "control plane  $CONTROL_PLANE_URL   (token: $DEV_DIR/controlplane.env)"
  say "prometheus     $PROMETHEUS_URL   grafana $GRAFANA_URL (user admin, password in $DEV_DIR/grafana.env)   jaeger $JAEGER_URL"
  say "next: make k8s-smoke    stop: make k8s-down"
}

# ---------------------------------------------------------------------------------------------------------
# down / reset / status
# ---------------------------------------------------------------------------------------------------------
cmd_down() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed"
  command -v kind >/dev/null 2>&1 || die "kind is not installed"
  mkdir -p "$K8S_DIR"
  # --kubeconfig keeps kind away from ~/.kube/config. Only a cluster this checkout created is ever deleted.
  if cluster_exists; then
    if [ ! -f "$OWNER_MARK" ] && [ ! -f "$KUBECONFIG_FILE" ]; then
      die "a kind cluster named $CLUSTER exists, but neither $OWNER_MARK nor $KUBECONFIG_FILE shows that this checkout created it; refusing to delete it. If it is yours: kind delete cluster --name $CLUSTER"
    fi
    kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" >/dev/null || die "kind could not delete the cluster"
  fi
  rm -f "$KUBECONFIG_FILE" "$OWNER_MARK"
  local left
  left="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER" | wc -l | tr -d ' ')"
  [ "$left" = 0 ] || die "$left containers of the kind cluster $CLUSTER are still present"
  say "down: the cluster $CLUSTER and its kubeconfig are gone"
}

cmd_reset() {
  cmd_down
  # K8S_DIR was validated at start: a directory strictly inside $ROOT/.dev, not a symlink.
  [ -n "$K8S_DIR" ] && [ "$K8S_DIR" != "$ROOT/.dev" ] && rm -rf "$K8S_DIR"
  say "reset: $K8S_DIR removed (the serverflow/*:dev images and the secrets in $DEV_DIR stay; make dev-reset removes them)"
}

cmd_status() {
  guard_context
  kc -n "$NAMESPACE" get deployments,statefulsets,pods,svc,pvc,jobs -o wide
}

# ---------------------------------------------------------------------------------------------------------
# smoke: each check is PASS or FAIL; the last line is the contract of the Phase 16 smoke. Checks never print a key, token or
# password: keys reach curl on standard input, never an argument. Every wait is a poll with a deadline.
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
  # Completed Job pods are phase Succeeded and skipped; every other pod must be Running (a Pending pod has no container
  # statuses yet) with every container ready and never restarted (true/0).
  printf '%s\n' "$rows" | awk '$2=="Succeeded"{next} $2!="Running"{bad=1} NF<3{bad=1} {for(i=3;i<=NF;i++) if($i!="true/0") bad=1} END{exit bad}'
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

# --- helpers for the end-to-end checks ---
BODY='{"model":"mock-model","messages":[{"role":"user","content":"say hello"}],"max_tokens":16}'
SBODY='{"model":"mock-model","messages":[{"role":"user","content":"say hello"}],"max_tokens":16,"stream":true}'
SCRATCH="$K8S_DIR/smoke"

# poll SECONDS COMMAND...: true as soon as the command succeeds, false at the deadline.
poll() {
  local deadline=$(($(date +%s) + $1))
  shift
  while ! "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 1
  done
}

# gw_curl KEYFILE curl-args...: the key is read from KEYFILE and handed to curl on standard input (an empty KEYFILE sends none).
gw_curl() {
  local keyfile="$1" key
  shift
  if [ -n "$keyfile" ]; then
    read -r key <"$keyfile" || return 1
    printf 'header = "Authorization: Bearer %s"\n' "$key" | curl -sS -K - --max-time 60 "$@"
  else
    curl -sS --max-time 60 "$@"
  fi
}

# cp_curl PATH: GET on the control plane's published port with the token from .dev/controlplane.env, handed over on stdin.
cp_curl() {
  local tok
  tok="$(sed -n 's/^SERVERFLOW_CONTROL_PLANE_TOKEN=//p' "$DEV_DIR/controlplane.env")"
  [ -n "$tok" ] || return 1
  printf 'header = "Authorization: Bearer %s"\n' "$tok" | curl -sS -K - --max-time 10 "$CONTROL_PLANE_URL$1"
}

# make_tenant NAME RPM: a throwaway tenant and its key in $SCRATCH/NAME.key (smoke tenants are named smoke-*).
make_tenant() {
  admin_run tenant create "$1" --rpm "$2" --tpm 100000000 --max-concurrent 8 >/dev/null || return 1
  create_key "$1" "$SCRATCH/$1.key"
}

mock_worker_pods() { kc -n "$NAMESPACE" get pods -l app.kubernetes.io/component=mock-worker -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'; }
gateway_pods() { kc -n "$NAMESPACE" get pods -l app.kubernetes.io/component=gateway -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'; }

# eligible_workers: the worker IDs the registry would route to, one per line.
eligible_workers() { cp_curl '/v1/workers?eligible=true' | grep -o '"worker_id":"[^"]*"' | cut -d'"' -f4; }

k1_pods_ready() { poll 60 pods_ready_no_restarts; }

gateway_readyz_200() { [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$GATEWAY_URL/readyz")" = 200 ]; }
# The body goes into a variable first: `curl | grep -q` under pipefail fails when grep closes the pipe early.
gateway_lists_model() {
  local body
  body="$(gw_curl "$K8S_DIR/api_key" -s "$GATEWAY_URL/v1/models")" || return 1
  case "$body" in *mock-model*) return 0 ;; *) return 1 ;; esac
}

k2_gateway_ready() {
  poll 60 gateway_readyz_200 && poll 30 gateway_lists_model
}

k3_auth_required() {
  local c1 c2
  c1="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")"
  printf 'header = "Authorization: Bearer sf_live_0000000000000000000000000000000000000000"\n' >"$SCRATCH/bad.cfg"
  c2="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -K "$SCRATCH/bad.cfg" -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")"
  [ "$c1" = 401 ] && [ "$c2" = 401 ]
}

k4_completion() {
  local out code
  out="$(gw_curl "$K8S_DIR/api_key" -s -w '\n%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")" || return 1
  code="$(printf '%s' "$out" | tail -1)"
  [ "$code" = 200 ] && printf '%s' "$out" | grep -q '"choices"' && printf '%s' "$out" | grep -q '"usage"'
}

k5_streaming() {
  local n
  gw_curl "$K8S_DIR/api_key" -sN -H 'Content-Type: application/json' -d "$SBODY" "$GATEWAY_URL/v1/chat/completions" >"$SCRATCH/stream.txt" || return 1
  n="$(grep -c '^data:' "$SCRATCH/stream.txt" || true)"
  [ "$n" -ge 2 ] && [ "$(grep '^data:' "$SCRATCH/stream.txt" | tail -1 | tr -d '\r')" = "data: [DONE]" ]
}

# worker_ok_count POD: worker_requests_total{result="ok"} of a mock worker, read through the API server's pod proxy (the mock
# worker's /metrics needs no token; the gateway's does, and the proxy would not forward one).
worker_ok_count() {
  kc get --raw "/api/v1/namespaces/$NAMESPACE/pods/$1:9000/proxy/metrics" 2>/dev/null | sed -n 's/^worker_requests_total{result="ok"} \([0-9][0-9]*\).*/\1/p' | head -1
}

workers_with_requests() {
  local p c n=0
  for p in $(mock_worker_pods); do
    c="$(worker_ok_count "$p")"
    if [ "${c:-0}" -gt 0 ]; then n=$((n + 1)); fi
  done
  [ "$n" -ge 3 ]
}

k6_three_workers() {
  local c
  for _ in 1 2 3 4; do
    for c in 1 2 3 4 5 6; do
      gw_curl "$K8S_DIR/api_key" -s -o /dev/null -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions" &
    done
    wait
  done
  poll 45 workers_with_requests
}

k7_rate_limit() {
  local name="smoke-limited-$$" codes="" retry_after="" c first4
  make_tenant "$name" 3 || return 1
  for _ in 1 2 3 4 5; do
    c="$(gw_curl "$SCRATCH/$name.key" -s -D "$SCRATCH/h.txt" -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")"
    codes="$codes $c"
    if [ "$c" = 429 ] && [ -z "$retry_after" ]; then retry_after="$(grep -i '^retry-after:' "$SCRATCH/h.txt" | tr -d '\r' | cut -d: -f2 | tr -d ' ')"; fi
  done
  first4="$(printf '%s\n' "$codes" | awk '{print $1, $2, $3, $4}')"
  [ "$first4" = "200 200 200 429" ] && [ -n "$retry_after" ]
}

# --- K8: Prometheus ---
# prom_count EXPR: the value of a single-sample instant query, 0 when the result is empty (e.g. count of nothing).
prom_count() {
  local out n
  out="$(curl -sS --max-time 10 -G --data-urlencode "query=$1" "$PROMETHEUS_URL/api/v1/query")" || return 1
  case "$out" in *'"status":"success"'*) ;; *) return 1 ;; esac
  n="$(printf '%s' "$out" | sed -n 's/.*"value":\[[0-9.]*,"\([0-9][0-9.e+]*\)"\].*/\1/p')"
  printf '%s\n' "${n:-0}"
}

# prom_targets_up: every gateway pod, the control plane and every mock-worker pod is a target with up == 1, and none is down.
prom_targets_up() {
  local gw cp mw down want_gw want_mw
  want_gw="$(gateway_pods | wc -l | tr -d ' ')"
  want_mw="$(mock_worker_pods | wc -l | tr -d ' ')"
  gw="$(prom_count 'count(up{job="serverflow-gateway"} == 1)')" || return 1
  cp="$(prom_count 'count(up{job="serverflow-control-plane"} == 1)')" || return 1
  mw="$(prom_count 'count(up{job="serverflow-mock-worker"} == 1)')" || return 1
  down="$(prom_count 'count(up{job=~"serverflow-.*"} == 0)')" || return 1
  [ "$want_gw" -ge 2 ] && [ "$gw" = "$want_gw" ] && [ "$cp" = 1 ] && [ "$want_mw" -ge 3 ] && [ "$mw" = "$want_mw" ] && [ "$down" = 0 ]
}

requests_counted() { prom_count 'sum(inference_requests_total)' | awk '{exit !($1 > 0)}'; }

k8_prometheus_scrapes() {
  poll 120 prom_targets_up && poll 90 requests_counted
}

# --- K9: Grafana ---
# grafana_curl PATH: GET with the admin password from .dev/grafana.env, handed to curl on standard input.
grafana_curl() {
  local pw
  pw="$(sed -n 's/^GF_SECURITY_ADMIN_PASSWORD=//p' "$DEV_DIR/grafana.env")"
  [ -n "$pw" ] || return 1
  printf 'user = "admin:%s"\n' "$pw" | curl -sS -K - --max-time 10 "$GRAFANA_URL$1"
}

grafana_database_ok() {
  local body
  body="$(curl -sS --max-time 5 "$GRAFANA_URL/api/health")" || return 1
  printf '%s' "$body" | tr -d ' \n' | grep -q '"database":"ok"'
}

# The five dashboards of observability/grafana/dashboards, found by their fixed uids, and the datasource answering a query.
grafana_provisioned() {
  local body uid n
  body="$(grafana_curl '/api/search?type=dash-db')" || return 1
  body="$(printf '%s' "$body" | tr -d ' \n')"
  for uid in sf-cluster-overview sf-scheduler sf-resilience sf-model sf-worker; do
    case "$body" in *"\"uid\":\"$uid\""*) ;; *) return 1 ;; esac
  done
  n="$(printf '%s' "$body" | grep -o '"type":"dash-db"' | wc -l | tr -d ' ')"
  [ "$n" = 5 ] || return 1
  body="$(grafana_curl '/api/datasources/uid/serverflow-prometheus/health')" || return 1
  printf '%s' "$body" | tr -d ' \n' | grep -q '"status":"OK"'
}

k9_grafana_dashboards() {
  poll 90 grafana_database_ok && poll 60 grafana_provisioned
}

K10_TENANT=""
usage_seen() {
  admin_run usage summary --since 1h --tenant "$K10_TENANT" | grep -o 'requests=[0-9]*' | cut -d= -f2 | awk '{s+=$1} END{exit !(s>=3)}'
}

k10_usage_rows() {
  K10_TENANT="smoke-usage-$$"
  make_tenant "$K10_TENANT" 600 || return 1
  for _ in 1 2 3; do gw_curl "$SCRATCH/$K10_TENANT.key" -s -o /dev/null -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions"; done
  poll 90 usage_seen
}

# --- K11: Jaeger ---
K11_RID=""
# iso_utc EPOCH: RFC 3339 UTC (GNU date first, BSD date as the fallback).
iso_utc() {
  date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ
}

# The trace of request K11_RID holds spans of the gateway and of a mock worker (both services, one trace).
trace_has_gateway_and_worker() {
  local now out
  now=$(date +%s)
  out="$(curl -sS --max-time 10 -G --data-urlencode 'query.service_name=serverflow-gateway' \
    --data-urlencode "query.attributes={\"serverflow.request_id\":\"$K11_RID\"}" \
    --data-urlencode "query.start_time_min=$(iso_utc $((now - 7200)))" --data-urlencode "query.start_time_max=$(iso_utc $((now + 300)))" \
    "$JAEGER_URL/api/v3/traces")" || return 1
  out="$(printf '%s' "$out" | tr -d ' \n')"
  case "$out" in *'"service.name","value":{"stringValue":"serverflow-gateway"}'*) ;; *) return 1 ;; esac
  case "$out" in *'"service.name","value":{"stringValue":"serverflow-mock-worker"}'*) return 0 ;; *) return 1 ;; esac
}

k11_trace_in_jaeger() {
  gw_curl "$K8S_DIR/api_key" -s -D "$SCRATCH/k11.txt" -o /dev/null -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions" || return 1
  K11_RID="$(awk 'tolower($1)=="x-request-id:"{print $2}' "$SCRATCH/k11.txt" | tr -d '\r')"
  [ -n "$K11_RID" ] || return 1
  # The span batch is flushed every 5 s.
  poll 90 trace_has_gateway_and_worker
}

K12_VICTIM=""
victim_ready() { eligible_workers | grep -qx "$K12_VICTIM"; }
victim_gone() { ! eligible_workers | grep -qx "$K12_VICTIM"; }
replacement_registered() { [ "$(eligible_workers | wc -l | tr -d ' ')" -ge 3 ] && victim_gone; }

k12_worker_failover() {
  local bad=0 c
  K12_VICTIM="$(mock_worker_pods | head -1)"
  [ -n "$K12_VICTIM" ] || return 1
  poll 30 victim_ready || return 1
  kc -n "$NAMESPACE" delete pod "$K12_VICTIM" --wait=false >/dev/null || return 1
  for _ in 1 2 3 4 5 6; do
    c="$(gw_curl "$K8S_DIR/api_key" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")"
    [ "$c" = 200 ] || bad=$((bad + 1))
  done
  poll 60 victim_gone || return 1
  for _ in 1 2 3 4 5 6; do
    c="$(gw_curl "$K8S_DIR/api_key" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")"
    [ "$c" = 200 ] || bad=$((bad + 1))
  done
  [ "$bad" = 0 ] || return 1
  # The Deployment starts a replacement: three eligible workers again, none of them the victim.
  poll 120 replacement_registered
}

# k14_shared_quota (S13's check): a tenant limited to 3 requests per minute sends 30 requests through the node port, which
# kube-proxy spreads over the gateways. A per-gateway counter would admit about 3 per gateway (up to 9); the shared Redis
# counter admits at most 3 in all, and the 429s must have been issued by at least two different gateway pods (their logs).
k14_shared_quota() {
  local name="smoke-scale-$$" admitted=0 c p rejecting=0 n
  [ "$(gateway_pods | wc -l | tr -d ' ')" -ge 2 ] || return 1
  make_tenant "$name" 3 || return 1
  for _ in $(seq 1 30); do
    c="$(gw_curl "$SCRATCH/$name.key" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")"
    if [ "$c" = 200 ]; then admitted=$((admitted + 1)); fi
  done
  if [ "$admitted" -lt 1 ] || [ "$admitted" -gt 3 ]; then return 1; fi
  for p in $(gateway_pods); do
    n="$(kc -n "$NAMESPACE" logs "$p" --since=10m 2>/dev/null | grep -c 'rate limited' || true)"
    if [ "${n:-0}" -gt 0 ]; then rejecting=$((rejecting + 1)); fi
  done
  [ "$rejecting" -ge 2 ]
}

# netpol_probe COMPONENT TARGETS: runs the one-off probe pod (deploy/kubernetes/tools/netpol-probe.yaml) with the given component
# label (netpol-probe: selected by no allow rule; admin: may reach PostgreSQL only), waits for it to finish, prints its log
# ("IP:PORT open" or "IP:PORT closed" per target) and deletes it. TARGETS is a space-separated list of IPv4:PORT words.
netpol_probe() {
  local name="netpol-probe-$$-$RANDOM" phase="" deadline rc=0
  case "$1" in netpol-probe | admin) ;; *) die "netpol_probe: refusing the component '$1'" ;; esac
  case "$2" in '' | *[!0-9.:\ ]*) die "netpol_probe: refusing the targets '$2'" ;; esac
  sed -e "s|__PROBE_NAME__|$name|" -e "s|__PROBE_COMPONENT__|$1|" -e "s|__PROBE_TARGETS__|$2|" "$PROBE_POD" | kc create -f - >/dev/null || return 1
  deadline=$(($(date +%s) + 90))
  while :; do
    phase="$(kc -n "$NAMESPACE" get pod "$name" -o 'jsonpath={.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded | Failed) break ;; esac
    if [ "$(date +%s)" -ge "$deadline" ]; then phase=Timeout; break; fi
    sleep 1
  done
  if [ "$phase" = Succeeded ]; then
    kc -n "$NAMESPACE" logs "$name" 2>/dev/null || rc=1
  else
    rc=1
    printf 'K15 diagnostic: the probe pod %s ended as %s\n' "$name" "${phase:-unknown}" >&2
  fi
  kc -n "$NAMESPACE" delete pod "$name" --now --ignore-not-found >/dev/null 2>&1 || true
  return "$rc"
}

service_ip() { kc -n "$NAMESPACE" get service "$1" -o 'jsonpath={.spec.clusterIP}'; }

# k15_network_policy_enforced: the proof that NetworkPolicy is enforced by this cluster's CNI, not merely accepted by the API. A pod
# that no allow rule selects must fail to connect to Redis, PostgreSQL, Redpanda and the control plane (it is addressed by ClusterIP, so
# a blocked DNS lookup cannot fake the result); a pod labelled like the admin tool must reach PostgreSQL (the control: the probe itself
# works and an allow rule is honoured) but not Redis; and the gateway path through the node port still answers 200 (the allow rules
# for the traffic that exists did not break it). If the outside pod connects anywhere, the CNI does not enforce policy: fail loudly.
k15_network_policy_enforced() {
  local redis pg rp cp out ctl code
  kc -n "$NAMESPACE" get networkpolicy default-deny-all >/dev/null 2>&1 || { echo "K15 diagnostic: the default-deny NetworkPolicy does not exist" >&2; return 1; }
  redis="$(service_ip redis)"
  pg="$(service_ip postgres)"
  rp="$(service_ip redpanda)"
  cp="$(service_ip control-plane)"
  if [ -z "$redis" ] || [ -z "$pg" ] || [ -z "$rp" ] || [ -z "$cp" ]; then
    echo "K15 diagnostic: a Service has no ClusterIP" >&2
    return 1
  fi
  out="$(netpol_probe netpol-probe "$redis:6379 $pg:5432 $rp:9092 $cp:9090")" || return 1
  if [ "$(printf '%s\n' "$out" | grep -c ' closed$')" != 4 ]; then
    printf 'K15 diagnostic: a pod outside the allow-list connected (NetworkPolicy is NOT enforced by this cluster'"'"'s CNI):\n%s\n' "$out" >&2
    return 1
  fi
  ctl="$(netpol_probe admin "$pg:5432 $redis:6379")" || return 1
  if ! printf '%s\n' "$ctl" | grep -qx "$pg:5432 open" || ! printf '%s\n' "$ctl" | grep -qx "$redis:6379 closed"; then
    printf 'K15 diagnostic: the control probe (admin) did not see PostgreSQL open and Redis closed:\n%s\n' "$ctl" >&2
    return 1
  fi
  code="$(gw_curl "$K8S_DIR/api_key" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GATEWAY_URL/v1/chat/completions")" || return 1
  [ "$code" = 200 ]
}

# k13_no_secrets_leaked: none of the generated secret values (the .dev/*.env values and the API keys) appears in any non-Secret
# object, event, pod log, or in the node's container logs. The values are searched, never printed; the check also fails if it
# searched next to nothing (too few needles, or empty haystacks), so it cannot pass vacuously.
k13_no_secrets_leaked() {
  local needles="$SCRATCH/needles" hay="$SCRATCH/haystack" f p hits=0 nneedles bytes node_hits leftover node="$CLUSTER-control-plane"
  : >"$needles"
  for f in "$DEV_DIR"/*.env; do
    [ -f "$f" ] || continue
    case "$f" in */compose.env) continue ;; esac
    sed -n 's/^[A-Z_]*=//p' "$f"
  done | grep -E '.{12,}' >>"$needles" || true
  for f in "$K8S_DIR"/api_key*; do
    [ -f "$f" ] || continue
    tr -d '\n' <"$f"
    echo
  done | grep -E '.{12,}' >>"$needles" || true
  nneedles="$(wc -l <"$needles" | tr -d ' ')"
  [ "$nneedles" -ge 6 ] || return 1
  # 1. every non-Secret object and the events
  kc -n "$NAMESPACE" get pods,deployments,replicasets,statefulsets,jobs,services,configmaps,serviceaccounts,persistentvolumeclaims,poddisruptionbudgets,events -o yaml >"$hay" 2>/dev/null || return 1
  bytes="$(wc -c <"$hay" | tr -d ' ')"
  [ "$bytes" -gt 5000 ] || return 1
  hits=$((hits + $(grep -F -c -f "$needles" "$hay" || true)))
  echo "K13 diagnostic: objects and events matched $hits needle line(s) of $nneedles needles" >&2
  # 2. every container log (the previous instance of a restarted container too)
  : >"$hay"
  for p in $(kc -n "$NAMESPACE" get pods -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'); do
    kc -n "$NAMESPACE" logs "$p" --all-containers --tail=-1 >>"$hay" 2>/dev/null || true
    kc -n "$NAMESPACE" logs "$p" --all-containers --previous --tail=-1 >>"$hay" 2>/dev/null || true
  done
  bytes="$(wc -c <"$hay" | tr -d ' ')"
  [ "$bytes" -gt 1000 ] || return 1
  hits=$((hits + $(grep -F -c -f "$needles" "$hay" || true)))
  echo "K13 diagnostic: after pod logs the running total is $hits" >&2
  # 3. the node's container logs (kubelet writes them under /var/log/pods; the log directory of a deleted pod, the admin pods'
  # included, lingers until the kubelet collects it). Nothing is excluded: the search is repeated for up to 90 s and fails only
  # if a secret is still found, and the admin pods' directories must be gone by then.
  node_hits=1
  local deadline=$(($(date +%s) + 90))
  while :; do
    docker exec "$node" sh -c 'cat /var/log/pods/*/*/*.log 2>/dev/null' >"$hay" 2>/dev/null || return 1
    bytes="$(wc -c <"$hay" | tr -d ' ')"
    [ "$bytes" -gt 1000 ] || return 1
    node_hits="$(grep -F -c -f "$needles" "$hay" || true)"
    leftover="$(docker exec "$node" sh -c 'ls -d /var/log/pods/serverflow_admin-* 2>/dev/null | wc -l' 2>/dev/null | tr -d ' ')"
    if [ "$node_hits" = 0 ] && [ "${leftover:-1}" = 0 ]; then break; fi
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 2
  done
  echo "K13 diagnostic: node logs matched $node_hits line(s), total $hits" >&2
  hits=$((hits + node_hits))
  : >"$hay"
  [ "$hits" = 0 ]
}

smoke_cleanup() {
  rm -f "${SCRATCH:?}"/* 2>/dev/null || true
  rmdir "$SCRATCH" 2>/dev/null || true
}

cmd_smoke() {
  guard_context
  command -v curl >/dev/null 2>&1 || die "curl is not installed"
  [ -s "$K8S_DIR/api_key" ] || die "there is no API key in $K8S_DIR (make k8s-up creates it)"
  (umask 077 && mkdir -p "$SCRATCH")
  trap smoke_cleanup EXIT
  local started
  started=$(date +%s)
  check K1 "every pod is ready with zero restarts" k1_pods_ready
  check D2 "the migrate job succeeded and the schema exists" migrate_ok
  check D3 "the redpanda-init job succeeded and the lifecycle topic exists" topic_ok
  check D4 "redis refuses an unauthenticated client and answers PONG to the Secret's password" redis_needs_auth
  check K2 "the gateway is ready through the node port and lists mock-model" k2_gateway_ready
  check K3 "a request without a key and one with a bad key are 401" k3_auth_required
  check K4 "a non-streaming completion has choices and usage" k4_completion
  check K5 "a streaming completion ends with [DONE]" k5_streaming
  check K6 "24 requests reached all three worker pods (worker_requests_total, read through the API server)" k6_three_workers
  check K7 "a tenant limited to 3 per minute gets 200 200 200 then 429 with Retry-After" k7_rate_limit
  check K8 "Prometheus: every gateway pod, the control plane and every worker pod is up, none is down, and inference_requests_total is non-zero" k8_prometheus_scrapes
  check K9 "Grafana is healthy, provisions the five dashboards and its datasource answers Prometheus" k9_grafana_dashboards
  check K10 "usage rows for a fresh tenant appeared (gateway, Redpanda, consumer, PostgreSQL)" k10_usage_rows
  check K11 "a request's trace in Jaeger holds spans of the gateway and of a mock worker" k11_trace_in_jaeger
  check K12 "deleting a worker pod: it left the pool, requests stayed 200, its replacement registered" k12_worker_failover
  check K14 "the rate limit is shared by the gateways: 30 requests, at most 3 admitted, 429s from two or more gateway pods" k14_shared_quota
  check K15 "NetworkPolicy is enforced: a pod outside the allow-list cannot reach Redis, PostgreSQL, Redpanda or the control plane, the admin label reaches PostgreSQL only, the gateway still answers 200" k15_network_policy_enforced
  check D5 "PostgreSQL keeps its data across a pod restart (PVC)" postgres_keeps_data
  check D6 "Redpanda keeps the lifecycle topic across a pod restart (PVC)" redpanda_keeps_topic
  check D7 "Redis loses its keys across a pod restart (emptyDir, by design)" redis_resets
  check K13 "no generated secret appears in any object, event, pod log or node container log (searched, never printed)" k13_no_secrets_leaked
  printf 'duration: %ds\n' "$(($(date +%s) - started))"
  printf 'smoke: %d of %d checks passed, %d failed, 0 skipped\n' "$PASSED" "$TOTAL" "$FAILED"
  [ "$FAILED" = 0 ]
}

# ---------------------------------------------------------------------------------------------------------
# audit: what Pod Security admitted, read back from the API (AC 3). One line per container; any exception fails the command.
# ---------------------------------------------------------------------------------------------------------
cmd_audit() {
  guard_context
  local rows
  # One line per pod: pod|automount|pod runAsUser|pod runAsNonRoot|pod seccomp|volumes|then per container, separated by ';':
  # name,runAsUser,runAsNonRoot,readOnlyRootFilesystem,allowPrivilegeEscalation,privileged,drop,add,seccomp,cpuRequest,memRequest,memLimit,cpuLimit
  rows="$(kc -n "$NAMESPACE" get pods -o 'jsonpath={range .items[*]}{.metadata.name}{"|"}{.spec.automountServiceAccountToken}{"|"}{.spec.securityContext.runAsUser}{"|"}{.spec.securityContext.runAsNonRoot}{"|"}{.spec.securityContext.seccompProfile.type}{"|"}{range .spec.volumes[*]}{.name}{","}{end}{"|"}{range .spec.containers[*]}{.name}{","}{.securityContext.runAsUser}{","}{.securityContext.runAsNonRoot}{","}{.securityContext.readOnlyRootFilesystem}{","}{.securityContext.allowPrivilegeEscalation}{","}{.securityContext.privileged}{","}{.securityContext.capabilities.drop}{","}{.securityContext.capabilities.add}{","}{.securityContext.seccompProfile.type}{","}{.resources.requests.cpu}{","}{.resources.requests.memory}{","}{.resources.limits.memory}{","}{.resources.limits.cpu}{";"}{end}{"\n"}{end}')" || die "cannot read the pods"
  [ -n "$rows" ] || die "no pods in the namespace $NAMESPACE"
  printf '%s\n' "$rows" | awk -F'|' '
    function mib(q,   n) { if (q ~ /Mi$/) { n = q; sub(/Mi$/, "", n); return n + 0 } if (q ~ /Gi$/) { n = q; sub(/Gi$/, "", n); return (n + 0) * 1024 } return -1 }
    function bad(msg) { exceptions++; printf "EXCEPTION %s: %s\n", pod, msg }
    {
      pod = $1; pods++
      if ($2 != "false") bad("automountServiceAccountToken is not false (" $2 ")")
      if ($6 ~ /kube-api-access/) bad("an API token volume is mounted")
      n = split($7, cs, ";")
      for (i = 1; i <= n; i++) {
        if (cs[i] == "") continue
        split(cs[i], f, ",")
        c = f[1]; user = (f[2] != "" ? f[2] : $3); nonroot = (f[3] != "" ? f[3] : $4)
        seccomp = (f[9] != "" ? f[9] : $5)
        total++
        printf "%-44s user=%-6s nonroot=%-4s readOnlyRoot=%-4s noPrivEsc=%-5s drop=%-8s seccomp=%-14s req=%s/%s limit=%s\n", pod "/" c, user, nonroot, f[4], f[5], f[7], seccomp, f[10], f[11], f[12]
        if (user == "" || user == "0") bad(c ": runs as root or has no explicit runAsUser (" user ")")
        if (nonroot != "true") bad(c ": runAsNonRoot is not true")
        if (f[4] != "true") bad(c ": readOnlyRootFilesystem is not true")
        if (f[5] != "false") bad(c ": allowPrivilegeEscalation is not false")
        if (f[6] == "true") bad(c ": privileged")
        if (f[7] != "[\"ALL\"]") bad(c ": does not drop exactly ALL capabilities (" f[7] ")")
        if (f[8] != "") bad(c ": adds capabilities (" f[8] ")")
        if (seccomp != "RuntimeDefault") bad(c ": seccomp is not RuntimeDefault (" seccomp ")")
        if (f[10] == "" || f[11] == "") bad(c ": has no cpu or memory request")
        if (f[12] == "") bad(c ": has no memory limit")
        if (f[13] != "") bad(c ": has a CPU limit")
        m = mib(f[12]); if (m >= 0) sum += m
      }
    }
    END {
      printf "audit: sum of the memory limits of the running containers: %d MiB\n", sum
      printf "audit: %d pods, %d containers, %d exceptions\n", pods, total, exceptions
      exit (exceptions > 0 || total == 0)
    }' || die "the audit found exceptions"
}

# diagnose: what CI keeps when a run fails. Pod descriptions show the names of the Secrets a pod reads, never their values.
cmd_diagnose() {
  guard_context
  explain "diagnostics requested"
  kc -n "$NAMESPACE" describe pods 2>&1 || true
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
  secrets) ensure_secrets ;;
  check) cmd_check ;;
  diagnose) cmd_diagnose ;;
  audit) cmd_audit ;;
  roll-test) die "roll-test is not implemented yet (the rolling-update test is PR 6)" ;;
  *)
    printf 'usage: scripts/k8s-up.sh up|down|reset|status|smoke|render|check|diagnose|audit|roll-test | --check-context NAME\n' >&2
    exit 2
    ;;
esac
