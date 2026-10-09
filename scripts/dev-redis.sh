#!/usr/bin/env bash
# A throwaway Redis 7 for local development and the Redis tests. It listens on 127.0.0.1:56379
# only and REQUIRES A PASSWORD (--requirepass), exactly like the CI service, so a test client that
# forgets the password fails here the way it would fail in CI (the Postgres phase learned this the
# hard way). The password is public and fixed, so this is NOT a place for real data and must never
# be exposed to a network. Nothing is persisted (--save "" --appendonly no).
#
# Docker is used when its daemon is running (image redis:7, container serverflow-test-redis);
# otherwise a local redis-server is started in the background with the same flags.
#
# Set DEV_REDIS_PORT to use another port (e.g. when several worktrees run one each).
#   scripts/dev-redis.sh start|stop|status|reset|addr|password
#
# Tests use: export SERVERFLOW_TEST_REDIS_ADDR="$(scripts/dev-redis.sh addr)"
#            export SERVERFLOW_TEST_REDIS_PASSWORD="$(scripts/dev-redis.sh password)"
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
port="${DEV_REDIS_PORT:-56379}"
name="${DEV_REDIS_CONTAINER:-serverflow-test-redis}"
image="${DEV_REDIS_IMAGE:-redis:7}"
password=serverflow-test-redis-password
pidfile="$root/.data/redis-$port.pid"
logfile="$root/.data/redis-$port.log"

use_docker() { command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; }
find_server() {
  local d
  for d in /opt/homebrew/bin /usr/local/bin /usr/bin; do
    if [ -x "$d/redis-server" ]; then echo "$d/redis-server"; return; fi
  done
  command -v redis-server 2>/dev/null || true
}

ping_ok() {
  # Authenticated ping through a throwaway shell so the password is not on a process list for long.
  if use_docker && docker ps --format '{{.Names}}' | grep -qx "$name"; then
    docker exec "$name" redis-cli -a "$password" --no-auth-warning ping 2>/dev/null | grep -q PONG
  elif [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    return 0
  else
    return 1
  fi
}

start() {
  mkdir -p "$root/.data"
  if use_docker; then
    if docker ps --format '{{.Names}}' | grep -qx "$name"; then
      echo "dev-redis: already running"
    else
      docker rm -f "$name" >/dev/null 2>&1 || true
      docker run -d --name "$name" -p "127.0.0.1:$port:6379" "$image" \
        redis-server --requirepass "$password" --save "" --appendonly no >/dev/null
      echo "dev-redis: started $image as $name on 127.0.0.1:$port"
    fi
  else
    server="$(find_server)"
    if [ -z "$server" ]; then
      echo "dev-redis: neither a running Docker daemon nor redis-server found; install one (brew install redis, or start Docker)" >&2
      exit 1
    fi
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
      echo "dev-redis: already running"
    else
      "$server" --port "$port" --bind 127.0.0.1 --requirepass "$password" --save "" --appendonly no \
        --daemonize yes --pidfile "$pidfile" --logfile "$logfile" --dir "$root/.data" >/dev/null
      echo "dev-redis: started $server on 127.0.0.1:$port"
    fi
  fi
  for _ in $(seq 1 50); do
    if ping_ok; then break; fi
    sleep 0.2
  done
  echo "dev-redis: address 127.0.0.1:$port (throwaway, password required like CI)"
}

stop() {
  if use_docker && docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker rm -f "$name" >/dev/null
    echo "dev-redis: stopped"
  elif [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    kill "$(cat "$pidfile")"
    rm -f "$pidfile"
    echo "dev-redis: stopped"
  else
    echo "dev-redis: not running"
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status)
    if ping_ok; then echo "dev-redis: running on 127.0.0.1:$port"; else echo "dev-redis: not running"; exit 3; fi ;;
  reset) stop; start ;;
  addr) echo "127.0.0.1:$port" ;;
  password) echo "$password" ;;
  *) echo "usage: $0 start|stop|status|reset|addr|password" >&2; exit 2 ;;
esac
