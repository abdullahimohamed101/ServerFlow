#!/usr/bin/env python3
"""D23 experiment (run from the repository root with the stack up: python3 docs/benchmarks/phase-15-assets/d23-kill-experiment.py agent 40)

D23 experiment against the Phase 16 compose stack (make dev-up): fault one worker's agent or backend under load and
timestamp what the control plane, Prometheus and the gateway's routing do. Usage: kill_exp.py agent|freeze
Secrets are read from .dev files and never printed."""
import json, os, subprocess, sys, threading, time, urllib.request, re, datetime

ROOT = os.environ.get("SF_ROOT", ".")  # the repository root, where make dev-up wrote .dev/
mode = sys.argv[1]
dur = float(sys.argv[2]) if len(sys.argv) > 2 else 45
api_key = open(ROOT + "/.dev/api_key").read().strip()
cp_token = [l.split("=", 1)[1].strip() for l in open(ROOT + "/.dev/controlplane.env") if "TOKEN" in l.split("=")[0]][0]
GW = "http://127.0.0.1:18080/v1/chat/completions"
CP = "http://127.0.0.1:19090/v1/workers"
PROM = "http://127.0.0.1:19091/api/v1/alerts"
target = "mock-3"
container = {"agent": "serverflow-dev-worker-agent-3-1", "freeze": "serverflow-dev-mock-worker-3-1"}[mode]

t0 = None
events = []  # (t, kind, detail)
lock = threading.Lock()
stop = False
activeAt = {}


def now():
    return time.time()


def rec(kind, detail):
    with lock:
        events.append((now(), kind, detail))


def req_loop():
    body = json.dumps({"model": "mock-model", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 4}).encode()
    def one(start):
        r = urllib.request.Request(GW, data=body, headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=120) as resp:
                resp.read()
                rec("req", {"start": start, "end": now(), "status": resp.status, "attempts": resp.headers.get("X-ServerFlow-Attempts")})
        except urllib.error.HTTPError as e:
            rec("req", {"start": start, "end": now(), "status": e.code, "attempts": e.headers.get("X-ServerFlow-Attempts")})
        except Exception as e:
            rec("req", {"start": start, "end": now(), "status": -1, "err": type(e).__name__})
    while not stop:
        threading.Thread(target=one, args=(now(),), daemon=True).start()
        time.sleep(0.25)


def get(url, hdr=None):
    r = urllib.request.Request(url, headers=hdr or {})
    with urllib.request.urlopen(r, timeout=5) as resp:
        return json.load(resp)


def poll_loop():
    while not stop:
        try:
            ws = get(CP, {"Authorization": "Bearer " + cp_token})["workers"]
            w = [x for x in ws if x["worker_id"] == target][0]
            rec("cp", {"eligible": w["eligible"], "health": w["health"], "state": w["state"], "age": w["heartbeat_age_seconds"]})
        except Exception as e:
            rec("cp", {"err": type(e).__name__})
        try:
            al = get(PROM)["data"]["alerts"]
            for a in al:
                if a["labels"].get("worker_id") == target and a["state"] == "firing":
                    activeAt[a["labels"]["alertname"]] = a["activeAt"]
            mine = sorted({a["labels"]["alertname"] + ":" + a["state"] for a in al if a["labels"].get("worker_id") in (target, None) and a["labels"]["alertname"].startswith("ServerFlow")})
            rec("alerts", mine)
        except Exception as e:
            rec("alerts", ["err"])
        time.sleep(0.5)


threading.Thread(target=req_loop, daemon=True).start()
threading.Thread(target=poll_loop, daemon=True).start()
time.sleep(6)
t0 = now()
if mode == "agent":
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)
else:
    subprocess.run(["docker", "pause", container], check=True, capture_output=True)
rec("fault", mode)
time.sleep(dur)
stop = True
time.sleep(1)
# restore
if mode == "agent":
    subprocess.run(["docker", "start", container], capture_output=True)
else:
    subprocess.run(["docker", "unpause", container], capture_output=True)

# timeline
def rel(t):
    return round(t - t0, 1)

first_inelig = next((rel(t) for t, k, d in events if k == "cp" and t > t0 and d.get("eligible") is False), None)
first = {}
for t, k, d in events:
    if k == "alerts" and t > t0:
        for a in d:
            first.setdefault(a, rel(t))
reqs = [d for t, k, d in events if k == "req" and d["start"] > t0]
failed = [d for d in reqs if d["status"] != 200]
retried = [d for d in reqs if d.get("attempts") == "2"]
print("mode", mode, "load req/s ~4; requests after the fault:", len(reqs), "failed:", len(failed), "with 2 attempts:", len(retried))
print("control plane: first not eligible at", first_inelig, "s after the fault")
for a, t in sorted(first.items(), key=lambda x: x[1]):
    print("alert", a, "first seen at", t, "s")
cp = [(rel(t), d) for t, k, d in events if k == "cp" and t > t0 and "health" in d]
last = None
for t, d in cp:
    if last != (d["health"], d["state"], d["eligible"]):
        print("  cp t=%5.1f health=%s state=%s eligible=%s age=%.1f" % (t, d["health"], d["state"], d["eligible"], d["age"]))
        last = (d["health"], d["state"], d["eligible"])
json.dump({"t0": t0, "reqs": reqs}, open("/tmp/p15g-kill-%s.json" % mode, "w"))
for n, v in activeAt.items():
    ts = datetime.datetime.fromisoformat(re.sub(r"(\.\d{6})\d*", r"\1", v).replace("Z", "+00:00")).timestamp()
    print("alert", n, "became active (rule evaluation) at", rel(ts), "s")
print("t0 (utc)", datetime.datetime.utcfromtimestamp(t0).isoformat())
