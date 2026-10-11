#!/usr/bin/env bash
# Pulls a registry image for CI with retries and backoff, and optionally keeps it as a `docker save` tarball that
# actions/cache restores on later runs. CI must not run `docker run <registry image>` (or `docker pull`) directly:
# an anonymous pull from a shared runner is rate limited (HTTP 429 "toomanyrequests: Rate exceeded") and one
# failed pull would fail the whole job. internal/ciguard fails the build if a workflow does.
#
#   scripts/ci-docker-pull.sh <image> [tarball]
#
# With a tarball path: an existing tarball is loaded instead of pulling (a corrupt one is ignored and the image
# is pulled), and after a pull the image is saved there for the cache step to store.
# CI_DOCKER_PULL_DELAY sets the first delay in seconds (default 5; it grows x3 per failure: 5, 15, 45, 135).
set -euo pipefail

image="${1:-}"
tarball="${2:-}"
[ -n "$image" ] || { echo "usage: ci-docker-pull.sh <image> [tarball]" >&2; exit 2; }
attempts=5
delay="${CI_DOCKER_PULL_DELAY:-5}"

if [ -n "$tarball" ] && [ -f "$tarball" ]; then
  if docker load -i "$tarball" >/dev/null; then
    echo "ci-docker-pull: loaded $image from the cache"
    exit 0
  fi
  echo "ci-docker-pull: the cached tarball $tarball is unusable; pulling instead" >&2
fi

for i in $(seq 1 "$attempts"); do
  if docker pull "$image" >/dev/null; then
    if [ -n "$tarball" ]; then
      mkdir -p "$(dirname "$tarball")"
      docker save "$image" -o "$tarball"
    fi
    echo "ci-docker-pull: pulled $image"
    exit 0
  fi
  if [ "$i" -lt "$attempts" ]; then
    echo "ci-docker-pull: pulling $image failed (attempt $i of $attempts); retrying in ${delay}s" >&2
    sleep "$delay"
    delay=$((delay * 3))
  fi
done
echo "ci-docker-pull: could not pull $image after $attempts attempts (rate limited? see docs/development/ci.md)" >&2
exit 1
