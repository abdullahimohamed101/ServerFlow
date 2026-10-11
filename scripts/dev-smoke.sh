#!/usr/bin/env bash
# End-to-end checks of the running container stack (`make dev-smoke`, Phase 16, ADR-021). Each check prints
# PASS, FAIL or SKIPPED with its name; the last line is
#
#   smoke: P of N checks passed, F failed, S skipped
#
# and the exit status is non-zero on any failure. A check whose service is not in the active profiles is SKIPPED
# (the CI job fails on any skip). Every wait is a poll with a deadline, never a bare sleep. The script prints no
# key, token or password: keys go to curl through a config on standard input, never an argument, and the
# throwaway tenants it creates are named smoke-*.
#
# Environment: SMOKE_ONLY="S1 S3" runs only those checks.
set -u # no pipefail: grep -q closes pipes early and would turn a match into a failure

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
DEV_DIR=".dev"
COMPOSE_ENV="$DEV_DIR/compose.env"
UP="$ROOT/scripts/dev-up.sh"

[ -f "$COMPOSE_ENV" ] || { echo "smoke: the stack has not been started here: run make dev-up" >&2; exit 1; }

cfg() { grep "^$1=" "$COMPOSE_ENV" | head -1 | cut -d= -f2-; }
PROFILES="$(cfg SF_PROFILES)"
GATEWAYS="$(cfg SF_GATEWAYS)"
GW="http://127.0.0.1:$(cfg SF_GATEWAY_PORT)"
CP_PORT="$(cfg SF_CONTROL_PLANE_PORT)"
PROM="http://127.0.0.1:$(cfg SF_PROMETHEUS_PORT)"
GRAF="http://127.0.0.1:$(cfg SF_GRAFANA_PORT)"
JAEGER="http://127.0.0.1:$(cfg SF_JAEGER_UI_PORT)"
has_profile() { case ",$PROFILES," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }
dc() { "$UP" compose "$@"; }

SCRATCH="$DEV_DIR/smoke"
rm -rf "$SCRATCH"
(umask 077 && mkdir -p "$SCRATCH")
trap 'rm -rf "$SCRATCH"' EXIT

TOTAL=0 PASSED=0 FAILED=0 SKIPPED=0
START=$(date +%s)

pass() { TOTAL=$((TOTAL + 1)); PASSED=$((PASSED + 1)); printf 'PASS    %s\n' "$*"; }
fail() { TOTAL=$((TOTAL + 1)); FAILED=$((FAILED + 1)); printf 'FAIL    %s\n' "$*"; }
skip() { TOTAL=$((TOTAL + 1)); SKIPPED=$((SKIPPED + 1)); printf 'SKIPPED %s\n' "$*"; }
want() { [ -z "${SMOKE_ONLY:-}" ] || case " $SMOKE_ONLY " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# gw_curl KEYFILE curl-args...: the key is read from KEYFILE and handed to curl on standard input.
gw_curl() {
  local keyfile="$1"
  shift
  if [ -n "$keyfile" ]; then
    printf 'header = "Authorization: Bearer %s"\n' "$(cat "$keyfile")" | curl -sS -K - --max-time 60 "$@"
  else
    curl -sS --max-time 60 "$@"
  fi
}

KEY="$DEV_DIR/api_key"
if has_profile data; then [ -s "$KEY" ] || { echo "smoke: no API key; run make dev-up" >&2; exit 1; }; else KEY=""; fi

BODY='{"model":"mock-model","messages":[{"role":"user","content":"say hello"}],"max_tokens":16}'
SBODY='{"model":"mock-model","messages":[{"role":"user","content":"say hello"}],"max_tokens":16,"stream":true}'

# poll SECONDS COMMAND...: true as soon as the command succeeds, false at the deadline.
poll() {
  local deadline=$(($(date +%s) + $1))
  shift
  while ! "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 1
  done
}

prom_q() { curl -sS --max-time 10 -G --data-urlencode "query=$1" "$PROM/api/v1/query"; }
# prom_sum QUERY: the sum of the result values, or empty.
prom_sum() { prom_q "$1" | grep -Eo '"value":\[[0-9.]+,"[0-9.e+-]+"\]' | sed -E 's/.*,"([^"]+)"\]/\1/' | awk '{s+=$1} END{if (NR>0) printf "%d", s}'; }

make_tenant() { # NAME RPM -> writes $SCRATCH/NAME.key
  dc run --rm -T --no-deps admin tenant create "$1" --rpm "$2" --tpm 100000000 --max-concurrent 8 >/dev/null 2>&1 || return 1
  dc run --rm -T --no-deps admin key create --tenant "$1" --label smoke 2>/dev/null >"$SCRATCH/$1.key" || return 1
  grep -Eq '^sf_[A-Za-z0-9_-]{20,}$' "$SCRATCH/$1.key"
}

# ------------------------------------------------------------------------------------------------ S1
if want S1; then
  ready() { [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$GW/readyz")" = 200 ]; }
  models() { gw_curl "$KEY" -s "$GW/v1/models" | grep -q mock-model; }
  if poll 60 ready && poll 20 models; then pass "S1 gateway ready and lists mock-model"; else fail "S1 gateway not ready within 60s or /v1/models lacks mock-model"; fi
fi

# ------------------------------------------------------------------------------------------------ S2
if want S2; then
  if has_profile data; then
    c1="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions")"
    printf 'header = "Authorization: Bearer sf_live_0000000000000000000000000000000000000000"\n' >"$SCRATCH/bad.cfg"
    c2="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -K "$SCRATCH/bad.cfg" -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions")"
    if [ "$c1" = 401 ] && [ "$c2" = 401 ]; then pass "S2 requests without a key and with a bad key are 401"; else fail "S2 expected 401 and 401, got $c1 and $c2"; fi
  else
    skip "S2 authentication (profile data is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S3
if want S3; then
  out="$(gw_curl "$KEY" -s -w '\n%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions")"
  code="$(printf '%s' "$out" | tail -1)"
  if [ "$code" = 200 ] && printf '%s' "$out" | grep -q '"choices"' && printf '%s' "$out" | grep -q '"usage"'; then
    pass "S3 non-streaming completion has choices and usage"
  else
    fail "S3 non-streaming completion: status $code"
  fi
fi

# ------------------------------------------------------------------------------------------------ S4
if want S4; then
  gw_curl "$KEY" -sN -H 'Content-Type: application/json' -d "$SBODY" "$GW/v1/chat/completions" >"$SCRATCH/stream.txt"
  n="$(grep -c '^data:' "$SCRATCH/stream.txt" || true)"
  if [ "$n" -ge 2 ] && [ "$(grep '^data:' "$SCRATCH/stream.txt" | tail -1)" = "data: [DONE]" ]; then
    pass "S4 streaming completion: $n data chunks, ends with [DONE]"
  else
    fail "S4 streaming completion: $n data chunks, no [DONE] at the end"
  fi
fi

# ------------------------------------------------------------------------------------------------ S5
if want S5; then
  if has_profile observability; then
    for batch in 1 2 3 4; do
      for i in 1 2 3 4 5 6; do
        gw_curl "$KEY" -s -o /dev/null -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions" &
      done
      wait
    done
    spread() {
      local n
      n="$(prom_q 'scheduler_selections_total > 0' | grep -o '"worker_id":"[^"]*"' | sort -u | wc -l | tr -d ' ')"
      [ "$n" -ge 2 ]
    }
    if poll 45 spread; then pass "S5 24 requests spread over at least two workers (scheduler_selections_total)"; else fail "S5 scheduler_selections_total shows fewer than two workers"; fi
  else
    skip "S5 request spread (profile observability is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S6
if want S6; then
  if has_profile data; then
    name="smoke-limited-$$"
    if make_tenant "$name" 3; then
      codes=""
      retry_after=""
      for i in 1 2 3 4 5; do
        gw_curl "$SCRATCH/$name.key" -s -D "$SCRATCH/h.txt" -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions" >"$SCRATCH/code.txt"
        c="$(cat "$SCRATCH/code.txt")"
        codes="$codes $c"
        if [ "$c" = 429 ] && [ -z "$retry_after" ]; then retry_after="$(grep -i '^retry-after:' "$SCRATCH/h.txt" | tr -d '\r' | cut -d: -f2 | tr -d ' ')"; fi
      done
      first4="$(echo $codes | cut -d' ' -f1-4)"
      if [ "$first4" = "200 200 200 429" ] && [ -n "$retry_after" ]; then
        pass "S6 limited tenant: 200 200 200 then 429 with Retry-After"
      else
        fail "S6 limited tenant statuses:$codes (retry-after: ${retry_after:-none})"
      fi
    else
      fail "S6 could not create the throwaway tenant"
    fi
  else
    skip "S6 rate limit (profile data is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S7
if want S7; then
  if has_profile observability; then
    expected=$((1 + 1 + 3))
    all_up() {
      local gw cp mw req
      gw="$(prom_sum 'up{job="serverflow-gateway"} == 1')"
      cp="$(prom_sum 'up{job="serverflow-control-plane"} == 1')"
      mw="$(prom_sum 'up{job="serverflow-mock-worker"} == 1')"
      req="$(prom_sum 'inference_requests_total')"
      [ "${gw:-0}" -ge "$GATEWAYS" ] && [ "${cp:-0}" -ge 1 ] && [ "${mw:-0}" -ge 3 ] && [ "${req:-0}" -gt 0 ]
    }
    if poll 60 all_up; then pass "S7 Prometheus: gateway ($GATEWAYS), control plane, 3 mock workers up; requests counted"; else fail "S7 Prometheus targets not all up within 60s (expected $expected+)"; fi
  else
    skip "S7 Prometheus targets (profile observability is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S8
if want S8; then
  if has_profile observability; then
    gf() { # path -> body; the password goes through a curl config on stdin
      printf 'user = "admin:%s"\n' "$(grep '^GF_SECURITY_ADMIN_PASSWORD=' "$DEV_DIR/grafana.env" | cut -d= -f2-)" | curl -sS -K - --max-time 10 "$GRAF$1"
    }
    healthy() { gf /api/health | grep -q '"database": *"ok"'; }
    dashboards_ok() {
      local uid
      for uid in sf-cluster-overview sf-worker sf-scheduler sf-model; do gf "/api/dashboards/uid/$uid" | grep -q "\"uid\": *\"$uid\"" || return 1; done
    }
    if poll 60 healthy && poll 30 dashboards_ok; then pass "S8 Grafana healthy with the four dashboards"; else fail "S8 Grafana unhealthy or a dashboard is missing"; fi
  else
    skip "S8 Grafana (profile observability is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S9
if want S9; then
  if has_profile events; then
    usage_seen() {
      dc run --rm -T --no-deps admin usage summary --since 1h --tenant dev 2>/dev/null | grep -Eq '[1-9][0-9]*'
    }
    if poll 60 usage_seen; then pass "S9 usage rows for tenant dev appeared (Redpanda to PostgreSQL)"; else fail "S9 no usage rows for tenant dev within 60s"; fi
  else
    skip "S9 usage pipeline (profile events is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S10
if want S10; then
  if has_profile tracing; then
    iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ; }
    both() {
      local t now
      now=$(date +%s)
      # Jaeger v2 query API (OTLP JSON): the newest trace that has a mock-worker span; it must also hold gateway spans.
      t="$(curl -sS --max-time 10 -G "$JAEGER/api/v3/traces" --data-urlencode "query.service_name=serverflow-mock-worker" \
        --data-urlencode "query.num_traces=1" --data-urlencode "query.start_time_min=$(iso $((now - 3600)))" \
        --data-urlencode "query.start_time_max=$(iso $((now + 300)))")"
      printf '%s' "$t" | grep -q '"stringValue":"serverflow-gateway"' && printf '%s' "$t" | grep -q '"stringValue":"serverflow-mock-worker"'
    }
    if poll 45 both; then pass "S10 one trace holds gateway and mock-worker spans"; else fail "S10 no trace with both gateway and mock-worker spans in Jaeger"; fi
  else
    skip "S10 tracing (profile tracing is off)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S11
if want S11; then
  if [ -n "$CP_PORT" ]; then
    cp_worker() { # prints the state of worker mock-3
      printf 'header = "Authorization: Bearer %s"\n' "$(grep '^SERVERFLOW_CONTROL_PLANE_TOKEN=' "$DEV_DIR/controlplane.env" | cut -d= -f2-)" | curl -sS -K - --max-time 5 "http://127.0.0.1:$CP_PORT/v1/workers/mock-3"
    }
    ready3() { cp_worker | grep -Eq '"state": *"READY"'; }
    gone3() { ! cp_worker | grep -Eq '"state": *"READY"' || cp_worker | grep -Eq '"health": *"(UNHEALTHY|LOST|SUSPECT)"'; }
    if ! poll 30 ready3; then
      fail "S11 failover: mock-3 was not READY before the test"
    else
      dc kill mock-worker-3 >/dev/null 2>&1
      bad=0
      for i in 1 2 3 4 5 6; do
        c="$(gw_curl "$KEY" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions")"
        [ "$c" = 200 ] || bad=$((bad + 1))
      done
      if poll 40 gone3; then removed=1; else removed=0; fi
      for i in 1 2 3 4 5 6; do
        c="$(gw_curl "$KEY" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions")"
        [ "$c" = 200 ] || bad=$((bad + 1))
      done
      dc start mock-worker-3 >/dev/null 2>&1
      if poll 45 ready3; then back=1; else back=0; fi
      if [ "$bad" = 0 ] && [ "$removed" = 1 ] && [ "$back" = 1 ]; then
        pass "S11 killed mock-3: left the pool, requests stayed 200, came back after start"
      else
        fail "S11 failover: non-200 responses=$bad, left the pool=$removed, back=$back"
      fi
    fi
  else
    skip "S11 failover (the control plane is not published: CONTROL_PLANE_PORT is empty)"
  fi
fi

# ------------------------------------------------------------------------------------------------ S12
if want S12; then
  : >"$SCRATCH/needles"
  for f in "$DEV_DIR"/*.env "$DEV_DIR"/api_key*; do
    [ -f "$f" ] || continue
    case "$f" in */compose.env) continue ;; esac
    # one needle per value: the part after the first '=' of KEY=VALUE files, the whole content of key files
    case "$f" in
      *.env) sed -n 's/^[A-Z_]*=//p' "$f" ;;
      *) tr -d '\n' <"$f"; echo ;;
    esac
  done | grep -E '.{12,}' >>"$SCRATCH/needles"
  # the password inside the DSN is covered by postgres.env; the DSN as a whole is also a needle
  dc logs --no-color >"$SCRATCH/logs.txt" 2>&1
  hits="$(grep -F -c -f "$SCRATCH/needles" "$SCRATCH/logs.txt" || true)"
  nneedles="$(wc -l <"$SCRATCH/needles" | tr -d ' ')"
  if [ "$hits" = 0 ] && [ "$nneedles" -gt 0 ]; then pass "S12 none of $nneedles generated secret values appears in the container logs"; else fail "S12 $hits log lines contain a generated secret ($nneedles values searched)"; fi
fi

# ------------------------------------------------------------------------------------------------ S13
if want S13; then
  if [ "$GATEWAYS" -gt 1 ] && has_profile data && has_profile observability; then
    name="smoke-scale-$$"
    if make_tenant "$name" 3; then
      admitted=0
      for i in $(seq 1 12); do
        c="$(gw_curl "$SCRATCH/$name.key" -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions")"
        [ "$c" = 200 ] && admitted=$((admitted + 1))
      done
      for i in $(seq 1 30); do gw_curl "$KEY" -s -o /dev/null -H 'Content-Type: application/json' -d "$BODY" "$GW/v1/chat/completions" & done
      wait
      spread_gw() {
        local n
        n="$(prom_q 'inference_requests_total{job="serverflow-gateway"} > 0' | grep -o '"instance":"[^"]*"' | sort -u | wc -l | tr -d ' ')"
        [ "$n" -ge 2 ]
      }
      if [ "$admitted" -le 3 ] && [ "$admitted" -ge 1 ] && poll 45 spread_gw; then
        pass "S13 $GATEWAYS gateways: requests reached 2+ instances; the 3-per-minute tenant was admitted $admitted of 12 times"
      else
        fail "S13 scale: admitted $admitted of 12 (quota 3), or fewer than two gateway instances served requests"
      fi
    else
      fail "S13 could not create the throwaway tenant"
    fi
  elif [ "$GATEWAYS" -le 1 ]; then
    skip "S13 gateway scaling and shared quota (GATEWAYS is 1)"
  else
    skip "S13 gateway scaling (needs profiles data and observability)"
  fi
fi

END=$(date +%s)
printf 'smoke: %d of %d checks passed, %d failed, %d skipped\n' "$PASSED" "$TOTAL" "$FAILED" "$SKIPPED"
[ "$FAILED" = 0 ]
