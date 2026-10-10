#!/usr/bin/env bash
# A throwaway Kafka-compatible broker (Redpanda) for local development and the Kafka tests. It listens on
# 127.0.0.1:59092 only and REQUIRES SASL/SCRAM authentication, exactly like the CI container, so a client that
# forgets its credentials fails here the way it would fail in CI (the Postgres and Redis phases learned this
# the hard way). The user and password are public and fixed, so this is NOT a place for real data and must
# never be exposed to a network. Nothing is persisted (the container is removed on stop).
#
# Redpanda is Kafka API compatible, not Apache Kafka (ADR-019). It is used because it starts in seconds. NOTE: its
# registry host, docker.redpanda.com, is only a front for Docker Hub (the token realm is auth.docker.io and an
# anonymous pull counts against Docker Hub's 100-per-hour limit), and no unauthenticated mirror exists on
# public.ecr.aws, quay.io or ghcr.io (checked 2026-10-10, docs/development/ci.md). So `pull` retries with
# backoff, and CI caches the image. The image's licence (BSL for the community edition) allows this use.
# DEV_KAFKA_IMAGE overrides the image.
#
# One named container per port (serverflow-test-kafka-<port>), so several worktrees can each run their own and
# stop only their own. Set DEV_KAFKA_PORT to use another port.
#   scripts/dev-kafka.sh start|stop|status|reset|brokers|user|password|image|pull|topic <name> [partitions]|logs
#
# Tests use: export SERVERFLOW_TEST_KAFKA_BROKERS="$(scripts/dev-kafka.sh brokers)"
#            export SERVERFLOW_TEST_KAFKA_USER="$(scripts/dev-kafka.sh user)"
#            export SERVERFLOW_TEST_KAFKA_PASSWORD="$(scripts/dev-kafka.sh password)"
set -euo pipefail

port="${DEV_KAFKA_PORT:-59092}"
name="${DEV_KAFKA_CONTAINER:-serverflow-test-kafka-$port}"
image="${DEV_KAFKA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v25.1.1}"
user=serverflow-test
password=serverflow-test-kafka-password
topic=inference.lifecycle.v1

command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || {
  echo "dev-kafka: a running Docker daemon is required" >&2
  exit 1
}

# pull fetches the image, retrying with backoff: a rate-limited anonymous pull (HTTP 429) is the usual failure.
pull() {
  local i delay="${DEV_KAFKA_PULL_DELAY:-5}"
  for i in 1 2 3 4 5; do
    if docker pull "$image" >/dev/null; then return 0; fi
    echo "dev-kafka: pulling $image failed (attempt $i of 5); retrying in ${delay}s" >&2
    sleep "$delay"
    delay=$((delay * 3))
  done
  echo "dev-kafka: could not pull $image (rate limited? log in with docker login, or load a saved image)" >&2
  return 1
}

running() { docker ps --format '{{.Names}}' | grep -qx "$name"; }
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; }
rpk() { docker exec "$name" rpk "$@"; }
# rpk with the test user's credentials, for the Kafka API.
rpk_auth() { docker exec "$name" rpk "$@" -X user="$user" -X pass="$password" -X sasl.mechanism=SCRAM-SHA-256 -X brokers=127.0.0.1:9092; }

wait_ready() {
  local i
  for i in $(seq 1 120); do
    if rpk cluster health >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "dev-kafka: the broker did not become ready; log follows" >&2
  docker logs --tail 40 "$name" >&2 || true
  return 1
}

create_topic() {
  local t="$1" parts="${2:-6}"
  rpk_auth topic create "$t" -p "$parts" -r 1 -c retention.ms=604800000 -c cleanup.policy=delete >/dev/null 2>&1 || true
}

start() {
  if running; then
    echo "dev-kafka: already running ($name)"
  else
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker image inspect "$image" >/dev/null 2>&1 || pull
    # The external listener is what the host connects to; the internal one is for rpk inside the container.
    docker run -d --name "$name" -p "127.0.0.1:$port:$port" "$image" \
      redpanda start --mode dev-container --smp 1 --memory 512M \
      --kafka-addr "internal://0.0.0.0:9092,external://0.0.0.0:$port" \
      --advertise-kafka-addr "internal://127.0.0.1:9092,external://127.0.0.1:$port" \
      --set redpanda.enable_sasl=true --set 'redpanda.superusers=["'"$user"'"]' >/dev/null
    echo "dev-kafka: started $image as $name on 127.0.0.1:$port"
  fi
  wait_ready
  # Idempotent: creating an existing user or topic is not an error here.
  rpk security user create "$user" -p "$password" --mechanism SCRAM-SHA-256 >/dev/null 2>&1 || true
  local i
  for i in $(seq 1 40); do
    if rpk_auth cluster info >/dev/null 2>&1; then break; fi
    sleep 0.5
  done
  create_topic "$topic" 6
  if ! port_open; then echo "dev-kafka: port $port is not reachable from the host" >&2; exit 1; fi
  echo "dev-kafka: brokers 127.0.0.1:$port (throwaway, SASL/SCRAM required like CI), topic $topic"
}

stop() {
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker rm -f "$name" >/dev/null
    echo "dev-kafka: stopped"
  else
    echo "dev-kafka: not running"
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status)
    if running && port_open && rpk_auth cluster info >/dev/null 2>&1; then echo "dev-kafka: running on 127.0.0.1:$port"; else echo "dev-kafka: not running"; exit 3; fi ;;
  reset) stop; start ;;
  brokers) echo "127.0.0.1:$port" ;;
  user) echo "$user" ;;
  password) echo "$password" ;;
  image) echo "$image" ;;
  pull) pull ;;
  topic) create_topic "${2:?usage: $0 topic <name> [partitions]}" "${3:-6}" ;;
  rpk) shift; rpk_auth "$@" ;;
  logs) docker logs --tail "${2:-50}" "$name" ;;
  *) echo "usage: $0 start|stop|status|reset|brokers|user|password|image|pull|topic <name> [partitions]|rpk <args>|logs" >&2; exit 2 ;;
esac
