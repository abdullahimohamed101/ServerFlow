#!/usr/bin/env bash
# Runs the pinned static tools of the Kubernetes workflow without installing anything on the host (Phase 17, D1, D4).
#
#   scripts/k8s-tool.sh kubectl ARGS...       the pinned kubectl (used for `kubectl kustomize`)
#   scripts/k8s-tool.sh kubeconform ARGS...   the pinned kubeconform
#   scripts/k8s-tool.sh pin NAME              print a value of deploy/kubernetes/tools.env
#
# A binary on PATH is used only when it reports exactly the pinned version (CI installs the pinned releases, checksum
# verified); otherwise the pinned container image runs with the repository mounted read-only at /work, no capabilities
# and, for kubectl, no network. Paths in ARGS are relative to the repository root, where this script starts them.
# Compatible with bash 3.2.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PINS="deploy/kubernetes/tools.env"

pin() {
  local v
  v="$(sed -n "s/^$1=//p" "$PINS" | head -1)"
  [ -n "$v" ] || { printf 'k8s-tool: %s has no %s\n' "$PINS" "$1" >&2; exit 1; }
  printf '%s' "$v"
}

in_container() { # IMAGE NETWORK ARGS...
  local image="$1" net="$2"
  shift 2
  command -v docker >/dev/null 2>&1 || { printf 'k8s-tool: neither the pinned binary nor docker is available (see docs/operations/kubernetes-deployment.md)\n' >&2; exit 1; }
  exec docker run --rm -i --network "$net" --cap-drop ALL --security-opt no-new-privileges \
    -v "$ROOT":/work:ro -w /work "$image" "$@"
}

tool="${1:-}"
[ $# -gt 0 ] && shift
case "$tool" in
  pin) [ $# -eq 1 ] || { echo "usage: k8s-tool.sh pin NAME" >&2; exit 2; }; pin "$1"; echo ;;
  kubectl)
    want="$(pin KUBERNETES_VERSION)"
    if command -v kubectl >/dev/null 2>&1 && kubectl version --client 2>/dev/null | grep -q "Client Version: $want\$"; then
      exec kubectl "$@"
    fi
    in_container "$(pin KUBECTL_IMAGE)" none "$@"
    ;;
  kubeconform)
    want="$(pin KUBECONFORM_VERSION)"
    if command -v kubeconform >/dev/null 2>&1 && [ "$(kubeconform -v 2>/dev/null)" = "$want" ]; then
      exec kubeconform "$@"
    fi
    in_container "$(pin KUBECONFORM_IMAGE)" bridge "$@"
    ;;
  *) echo "usage: scripts/k8s-tool.sh kubectl|kubeconform ARGS... | pin NAME" >&2; exit 2 ;;
esac
