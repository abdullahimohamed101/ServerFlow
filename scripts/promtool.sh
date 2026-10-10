#!/usr/bin/env bash
# Run promtool (Prometheus' config, rule and rule-test checker) from the repository root, wherever it can be found:
#   1. a promtool binary on PATH (CI installs the pinned release; see .github/workflows/ci.yml),
#   2. else a container: $PROMTOOL_IMAGE with the repository mounted read-only at /repo.
# The image is the official Prometheus image from Quay (not Docker Hub, which rate-limits shared CI runners).
# Locally a Docker Hub copy works too: PROMTOOL_IMAGE=prom/prometheus:v2.53.0.
# With neither it exits 3, so callers can tell "could not run" from "checked and failed".
#
#   scripts/promtool.sh check config observability/prometheus/prometheus.yml
#   scripts/promtool.sh check rules observability/prometheus/rules/*.yml
#   scripts/promtool.sh test rules observability/prometheus/tests/*.test.yml
set -euo pipefail

PROMTOOL_VERSION="${PROMTOOL_VERSION:-2.53.0}"
PROMTOOL_IMAGE="${PROMTOOL_IMAGE:-quay.io/prometheus/prometheus:v${PROMTOOL_VERSION}}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if command -v promtool >/dev/null 2>&1; then
  exec promtool "$@"
fi

# The repository must be under a directory the container runtime shares (on Colima and Docker Desktop for Mac:
# the home directory); /tmp is not, and a mount from there appears empty.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  exec docker run --rm --entrypoint promtool -v "$PWD:/repo:ro" -w /repo "$PROMTOOL_IMAGE" "$@"
fi

printf 'promtool: neither a promtool binary nor a usable docker was found (image %s)\n' "$PROMTOOL_IMAGE" >&2
exit 3
