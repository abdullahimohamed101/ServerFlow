#!/usr/bin/env bash
# Pulls every image the container stack needs, with retries and growing backoff: the third-party images of
# docker-compose.yml and the base images of deploy/docker/Dockerfile. Anonymous registries (public.ecr.aws,
# mirror.gcr.io, quay.io) rate-limit shared addresses such as CI runners; a 429 or a dropped connection is the usual
# failure and the next attempt usually works (ADR-021, docs/development/ci.md).
#
#   scripts/dev-pull.sh [--skip-present] [IMAGE...]     (no IMAGE: every one the compose file and Dockerfile name)
#
# Environment: DEV_PULL_ATTEMPTS (5), DEV_PULL_DELAY (first backoff in seconds, 5; tripled each attempt),
# DEV_PULL_DOCKER (the docker binary, for tests). Nothing here handles credentials: it never logs in.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

DOCKER="${DEV_PULL_DOCKER:-docker}"
attempts="${DEV_PULL_ATTEMPTS:-5}"
first_delay="${DEV_PULL_DELAY:-5}"
skip_present=0
images=()
for a in "$@"; do
  case "$a" in
    --skip-present) skip_present=1 ;;
    -*) echo "dev-pull: unknown option $a" >&2; exit 2 ;;
    *) images+=("$a") ;;
  esac
done

if [ ${#images[@]} -eq 0 ]; then
  envargs=()
  [ -f .dev/compose.env ] && envargs=(--env-file .dev/compose.env)
  while IFS= read -r img; do
    case "$img" in serverflow/*) ;; *) [ -n "$img" ] && images+=("$img") ;; esac
  done < <("$DOCKER" compose ${envargs[@]+"${envargs[@]}"} --profile '*' config --no-env-resolution --images | sort -u)
  while IFS= read -r img; do
    [ -n "$img" ] && images+=("$img")
  done < <(awk 'toupper($1)=="FROM" && $2 !~ /^\$/ && $2 != "runtime" && $2 != "build" {print $2}' deploy/docker/Dockerfile | sort -u)
fi

pull_one() {
  local img="$1" i delay="$first_delay"
  if [ "$skip_present" = 1 ] && "$DOCKER" image inspect "$img" >/dev/null 2>&1; then
    return 0
  fi
  for ((i = 1; i <= attempts; i++)); do
    if "$DOCKER" pull -q "$img" >/dev/null 2>&1; then
      printf 'dev-pull: pulled %s\n' "${img%%@*}"
      return 0
    fi
    if [ "$i" -lt "$attempts" ]; then
      printf 'dev-pull: pulling %s failed (attempt %d of %d); retrying in %ss\n' "${img%%@*}" "$i" "$attempts" "$delay" >&2
      sleep "$delay"
      delay=$((delay * 3))
    fi
  done
  printf 'dev-pull: could not pull %s after %d attempts (rate limited or unreachable?). Point the matching *_IMAGE variable in .env at another mirror, log in with docker login, or load a saved image.\n' "${img%%@*}" "$attempts" >&2
  return 1
}

rc=0
for img in "${images[@]}"; do
  pull_one "$img" || rc=1
done
exit $rc
