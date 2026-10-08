#!/usr/bin/env bash
# A throwaway PostgreSQL for local development and the database tests. It lives in
# .data/postgres under the repository (gitignored), listens on 127.0.0.1:55432 only
# (no unix socket), and trusts connections from localhost for one dedicated test
# superuser. It is NOT a place for real data and must never be exposed to a network.
#
#   scripts/dev-postgres.sh start|stop|status|reset|dsn
#
# Tests use the DSN it prints: export SERVERFLOW_TEST_POSTGRES_DSN="$(scripts/dev-postgres.sh dsn)"
set -euo pipefail
# macOS needs a valid locale or the postmaster aborts at startup.
export LC_ALL="${LC_ALL:-en_US.UTF-8}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data="$root/.data/postgres"
log="$root/.data/postgres.log"
port=55432
user=sf_test
db=serverflow_test
dsn="postgres://$user@127.0.0.1:$port/$db?sslmode=disable"

find_bin() {
  local name="$1" d
  for d in "${PG_BIN_DIR:-}" /opt/homebrew/opt/postgresql@16/bin /opt/homebrew/bin /usr/local/opt/postgresql@16/bin /usr/lib/postgresql/16/bin; do
    if [ -n "$d" ] && [ -x "$d/$name" ]; then echo "$d/$name"; return; fi
  done
  if command -v "$name" >/dev/null 2>&1; then command -v "$name"; return; fi
  echo "dev-postgres: $name not found; install PostgreSQL 16 or set PG_BIN_DIR" >&2
  exit 1
}

running() { "$(find_bin pg_ctl)" -D "$data" status >/dev/null 2>&1; }

start() {
  mkdir -p "$root/.data"
  if [ ! -f "$data/PG_VERSION" ]; then
    echo "dev-postgres: creating a throwaway cluster in .data/postgres"
    "$(find_bin initdb)" -D "$data" -U "$user" --auth-local=trust --auth-host=trust -E UTF8 --no-instructions >/dev/null
    # Only IPv4 loopback may connect; the server also listens on nothing else.
    cat > "$data/pg_hba.conf" <<HBA
host all all 127.0.0.1/32 trust
HBA
    cat >> "$data/postgresql.conf" <<CONF
listen_addresses = '127.0.0.1'
port = $port
unix_socket_directories = ''
fsync = off
synchronous_commit = off
full_page_writes = off
max_connections = 200
CONF
  fi
  if running; then
    echo "dev-postgres: already running"
  else
    "$(find_bin pg_ctl)" -D "$data" -l "$log" -w start >/dev/null
    echo "dev-postgres: started on 127.0.0.1:$port"
  fi
  if ! "$(find_bin psql)" -X -q -h 127.0.0.1 -p "$port" -U "$user" -d postgres -tAc "select 1 from pg_database where datname='$db'" | grep -q 1; then
    "$(find_bin createdb)" -h 127.0.0.1 -p "$port" -U "$user" "$db"
  fi
  echo "dev-postgres: DSN (throwaway, no password): $dsn"
}

case "${1:-}" in
  start) start ;;
  stop)
    if [ -f "$data/PG_VERSION" ] && running; then "$(find_bin pg_ctl)" -D "$data" -m fast -w stop >/dev/null; echo "dev-postgres: stopped"; else echo "dev-postgres: not running"; fi ;;
  status)
    if [ -f "$data/PG_VERSION" ] && running; then echo "dev-postgres: running on 127.0.0.1:$port"; else echo "dev-postgres: not running"; exit 3; fi ;;
  reset)
    if [ -f "$data/PG_VERSION" ] && running; then "$(find_bin pg_ctl)" -D "$data" -m immediate -w stop >/dev/null; fi
    rm -rf "$data" "$log"
    start ;;
  dsn) echo "$dsn" ;;
  *) echo "usage: $0 start|stop|status|reset|dsn" >&2; exit 2 ;;
esac
