#!/usr/bin/env bash
# Gets a registry image for CI: tries several sources in order, with retries and backoff, tags the one that works
# as the canonical reference the job uses, and optionally keeps a `docker save` tarball that actions/cache restores.
# CI must not `docker run`/`docker pull` a registry image or use a `services:` container directly (internal/ciguard
# fails the build): an anonymous pull from a shared runner is throttled by request rate ("toomanyrequests: Rate
# exceeded") and by data volume ("Data limit exceeded"), and one failed pull would fail the whole job.
#
#   scripts/ci-docker-pull.sh <image> [tarball]   make <image> available locally (load the tarball, else pull + save)
#   scripts/ci-docker-pull.sh digest <image>      print the image's manifest digest (sha256:...) or "unknown"; for
#                                                 cache keys. Needs no Docker daemon; never fails.
#
# <image> is the canonical reference, for example public.ecr.aws/docker/library/redis:7. Sources, in order:
#   public.ecr.aws/docker/library/<name>  ->  mirror.gcr.io/library/<name>  ->  docker.io/library/<name>
# (the three serve identical digests, checked 2026-10-11, docs/development/ci.md). An image that is not under
# public.ecr.aws/docker/library/ has only its own reference as source. CI_DOCKER_SOURCES (space separated
# references of the same image) overrides the list.
#
# Tries: 3 rounds over all sources; after a round in which every source failed, sleeps CI_DOCKER_PULL_DELAY seconds
# (default 5, growing x3), never after the last failure. With DOCKER_HUB_USERNAME and DOCKER_HUB_TOKEN in the
# environment, it runs `docker login` first (optional: the Start PostgreSQL and Start Redis steps of ci.yml and nightly.yml pass them from the repository
# secrets of the same names; no such secret exists unless the owner creates one, and without them pulls are anonymous).
set -euo pipefail

prefix="public.ecr.aws/docker/library/"
accept='Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'

sources_of() {
  local image="$1"
  if [ -n "${CI_DOCKER_SOURCES:-}" ]; then echo "$CI_DOCKER_SOURCES"; return; fi
  case "$image" in
    "$prefix"*) local rest="${image#"$prefix"}"; echo "$image mirror.gcr.io/library/$rest docker.io/library/$rest" ;;
    *) echo "$image" ;;
  esac
}

sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi | cut -c1-64; }

digest() {
  local image="$1" rest name tag body
  case "$image" in
    "$prefix"*) rest="${image#"$prefix"}" ;;
    *) echo unknown; return 0 ;;
  esac
  name="${rest%%:*}"; tag="${rest#*:}"
  # Hash the manifest body served by Google's anonymous mirror: that is the digest Docker reports for the image.
  if body="$(curl -fsS --max-time 20 -H "$accept" "https://mirror.gcr.io/v2/library/$name/manifests/$tag" 2>/dev/null)" && [ -n "$body" ]; then
    printf 'sha256:%s\n' "$(printf '%s' "$body" | sha256)"
  else
    echo unknown
  fi
}

if [ "${1:-}" = digest ]; then
  [ -n "${2:-}" ] || { echo "usage: ci-docker-pull.sh digest <image>" >&2; exit 2; }
  digest "$2"
  exit 0
fi

image="${1:-}"
tarball="${2:-}"
[ -n "$image" ] || { echo "usage: ci-docker-pull.sh <image> [tarball] | digest <image>" >&2; exit 2; }
rounds=3
delay="${CI_DOCKER_PULL_DELAY:-5}"

if [ -n "$tarball" ] && [ -f "$tarball" ]; then
  if docker load -i "$tarball" >/dev/null; then
    echo "ci-docker-pull: loaded $image from the cache"
    exit 0
  fi
  echo "ci-docker-pull: the cached tarball $tarball is unusable; pulling instead" >&2
fi

if [ -n "${DOCKER_HUB_USERNAME:-}" ] && [ -n "${DOCKER_HUB_TOKEN:-}" ]; then
  printf '%s' "$DOCKER_HUB_TOKEN" | docker login -u "$DOCKER_HUB_USERNAME" --password-stdin >/dev/null || echo "ci-docker-pull: docker login failed; continuing anonymously" >&2
fi

read -r -a sources <<<"$(sources_of "$image")"
for round in $(seq 1 "$rounds"); do
  for src in "${sources[@]}"; do
    if docker pull "$src" >/dev/null; then
      [ "$src" = "$image" ] || docker tag "$src" "$image"
      if [ -n "$tarball" ]; then
        mkdir -p "$(dirname "$tarball")"
        docker save "$image" -o "$tarball"
      fi
      echo "ci-docker-pull: pulled $image from $src"
      exit 0
    fi
    echo "ci-docker-pull: pulling $src failed (round $round of $rounds)" >&2
  done
  if [ "$round" -lt "$rounds" ]; then
    echo "ci-docker-pull: every source failed; retrying in ${delay}s" >&2
    sleep "$delay"
    delay=$((delay * 3))
  fi
done
echo "ci-docker-pull: could not get $image from any of: ${sources[*]} after $rounds rounds (throttled? see docs/development/ci.md for the optional docker login)" >&2
exit 1
