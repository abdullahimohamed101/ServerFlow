#!/usr/bin/env bash
# Phase 11 demo: run a small ServerFlow cluster (control plane, three mock workers (one always failing) behind real agents, a gateway
# in registry mode) with tracing on, send requests until one is retried because a worker fails, then fetch the
# trace from Jaeger's query API and print its tree. Everything listens on 127.0.0.1.
#
#   make dev-tracing && scripts/trace-demo.sh
#
# Settings (environment):
#   TRACE_DEMO_UI_PORT    Jaeger UI/query port         (default 16686; must match make dev-tracing)
#   TRACE_DEMO_OTLP_PORT  Jaeger OTLP/HTTP port        (default 14318)
#   TRACE_DEMO_PORT_BASE  first of six ports used      (default 18080: gateway, +1 control plane, +2..+4 workers)
#   TRACE_DEMO_OUT        directory for logs and JSON  (default .data/trace-demo)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ui_port="${TRACE_DEMO_UI_PORT:-16686}"
otlp_port="${TRACE_DEMO_OTLP_PORT:-14318}"
base="${TRACE_DEMO_PORT_BASE:-18080}"
out="${TRACE_DEMO_OUT:-$root/.data/trace-demo}"
gw_port=$base cp_port=$((base + 1)) w1_port=$((base + 2)) w2_port=$((base + 3)) w3_port=$((base + 4))
ui="http://127.0.0.1:$ui_port"
model=mock-model

command -v python3 >/dev/null || { echo "trace-demo: python3 is required to print the trace tree" >&2; exit 1; }
curl -fsS -o /dev/null "$ui/" || { echo "trace-demo: Jaeger is not answering on $ui; run: make dev-tracing" >&2; exit 1; }

mkdir -p "$out" "$root/bin"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cd "$root"
for c in control-plane worker-agent mock-worker gateway; do go build -o "bin/$c" "./cmd/$c"; done

start() { local name=$1; shift; "$@" >"$out/$name.log" 2>&1 & pids+=($!); }
otlp="http://127.0.0.1:$otlp_port"

SERVERFLOW_CONTROL_PLANE_ADDR="127.0.0.1:$cp_port" start control-plane bin/control-plane
start mock-ok     bin/mock-worker --addr="127.0.0.1:$w1_port" --worker-id=mock-ok    --model=$model --ttft=80ms  --tokens-per-second=200 --otlp-endpoint="$otlp"
start mock-ok2    bin/mock-worker --addr="127.0.0.1:$w3_port" --worker-id=mock-ok2   --model=$model --ttft=60ms  --tokens-per-second=200 --otlp-endpoint="$otlp"
start mock-flaky  bin/mock-worker --addr="127.0.0.1:$w2_port" --worker-id=mock-flaky --model=$model --ttft=40ms  --tokens-per-second=200 --failure-rate=1 --failure-mode=unavailable --otlp-endpoint="$otlp"
sleep 1
for pair in "mock-ok:$w1_port" "mock-flaky:$w2_port" "mock-ok2:$w3_port"; do
  id=${pair%%:*} port=${pair##*:}
  SERVERFLOW_WORKER_ID=$id SERVERFLOW_WORKER_MODEL=$model SERVERFLOW_WORKER_BACKEND_URL="http://127.0.0.1:$port" \
    SERVERFLOW_WORKER_ADVERTISE_URL="http://127.0.0.1:$port" SERVERFLOW_WORKER_CONTROL_PLANE_URL="http://127.0.0.1:$cp_port" \
    start "agent-$id" bin/worker-agent
done
SERVERFLOW_GATEWAY_PORT=$gw_port SERVERFLOW_GATEWAY_WORKER_SOURCE=registry SERVERFLOW_GATEWAY_CONTROL_PLANE_URL="http://127.0.0.1:$cp_port" \
  SERVERFLOW_SCHEDULER_STRATEGY=round-robin SERVERFLOW_TRACING_ENABLED=true SERVERFLOW_TRACING_ENDPOINT="$otlp" \
  start gateway bin/gateway

gw="http://127.0.0.1:$gw_port"
for _ in $(seq 1 100); do
  curl -fsS "$gw/v1/models" 2>/dev/null | grep -q "$model" && break
  sleep 0.2
done
curl -fsS "$gw/v1/models" | grep -q "$model" || { echo "trace-demo: the gateway never saw a worker; see $out/*.log" >&2; exit 1; }

body='{"model":"'$model'","stream":true,"messages":[{"role":"user","content":"hello tracing"}]}'
ok_id="" retry_id=""
for i in $(seq 1 12); do
  hdr="$out/headers.$i"
  curl -fsS -D "$hdr" -o /dev/null -H 'Content-Type: application/json' -d "$body" "$gw/v1/chat/completions"
  rid=$(awk 'tolower($1)=="x-request-id:"{print $2}' "$hdr" | tr -d '\r')
  if grep -qi '^x-serverflow-attempts: 2' "$hdr"; then retry_id=${retry_id:-$rid}; else ok_id=${ok_id:-$rid}; fi
  [ -n "$ok_id" ] && [ -n "$retry_id" ] && break
done
echo "request without retry: ${ok_id:-none}"
echo "request with a retry:  ${retry_id:-none}"

echo "waiting for the 5 s span batch to flush..."
sleep 7

show() {
  local rid=$1 label=$2
  [ -n "$rid" ] || return 0
  python3 - "$ui" "$rid" "$out/trace-$label.json" <<'PY'
import datetime, json, sys, urllib.parse, urllib.request

ui, rid, dump = sys.argv[1:4]
now = datetime.datetime.now(datetime.timezone.utc)
q = {
    "query.service_name": "serverflow-gateway",
    "query.attributes": json.dumps({"serverflow.request_id": rid}),
    "query.start_time_min": (now - datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "query.start_time_max": (now + datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
}
raw = urllib.request.urlopen(ui + "/api/v3/traces?" + urllib.parse.urlencode(q), timeout=10).read()
open(dump, "wb").write(raw)

def val(v):
    for k in ("stringValue", "intValue", "boolValue", "doubleValue"):
        if k in v:
            return v[k]
    return "?"

spans = []  # (service, span)
for chunk in json.loads(raw).get("result", {}).get("resourceSpans", []):
    svc = next((val(a["value"]) for a in chunk["resource"]["attributes"] if a["key"] == "service.name"), "?")
    for sc in chunk.get("scopeSpans", []):
        for sp in sc.get("spans", []):
            spans.append((svc, sp))
attr = lambda sp, k: next((val(a["value"]) for a in sp.get("attributes", []) if a["key"] == k), None)
roots = [sp for _, sp in spans if sp["name"] == "gateway.receive" and attr(sp, "serverflow.request_id") == rid]
if not roots:
    print("no gateway.receive span found for", rid); sys.exit(1)
tid = roots[0]["traceId"]
spans = [(svc, sp) for svc, sp in spans if sp["traceId"] == tid]
kids = {}
for svc, sp in spans:
    kids.setdefault(sp.get("parentSpanId") or None, []).append((svc, sp))
keep = ("serverflow.attempt", "serverflow.worker_id", "serverflow.attempt.outcome", "serverflow.attempt.class",
        "serverflow.attempts", "http.response.status_code", "serverflow.scheduler.strategy")
def show(svc, sp, depth):
    ms = (int(sp["endTimeUnixNano"]) - int(sp["startTimeUnixNano"])) / 1e6
    tags = " ".join(f"{k.split('serverflow.')[-1]}={attr(sp, k)}" for k in keep if attr(sp, k) is not None)
    links = ",".join(l["spanId"][:8] for l in sp.get("links", []))
    ev = ",".join(e["name"] for e in sp.get("events", []))
    extra = (" link->" + links if links else "") + (" event:" + ev if ev else "")
    print("  " * depth + f"{sp['name']} [{svc}] {ms:.1f}ms span={sp['spanId'][:8]} {tags}{extra}")
    for csvc, c in sorted(kids.get(sp["spanId"], []), key=lambda x: int(x[1]["startTimeUnixNano"])):
        show(csvc, c, depth + 1)
print("trace", tid, "request", rid)
for svc, sp in sorted(kids.get(None, []), key=lambda x: int(x[1]["startTimeUnixNano"])):
    show(svc, sp, 1)
PY
}
echo; echo "== request without a retry =="; show "$ok_id" ok
echo; echo "== request with a retry =="; show "$retry_id" retry
echo; echo "Open $ui and search service serverflow-gateway, tag serverflow.request_id=<id>. Logs and JSON: $out"
