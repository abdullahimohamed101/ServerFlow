# Kubernetes deployment (Phase 17)

Status: **draft, extended by PR 6 of Phase 17** (`docs/plans/active/phase-17-kubernetes.md`). The static foundation, the data tier and
the core system (control plane, gateway, workers, usage consumer) exist, PR 4 added Prometheus, Grafana and Jaeger, and PR 5 adds NetworkPolicy
(default deny in both directions plus the allow rules below, proved by smoke check K15). PR 6 adds the rolling-update measurement with its negative control (`make k8s-roll-test`, section "Rolling update and the drain timeline"); it has not run on a cluster yet. The decision record is written in PR 7 (`docs/decisions/ADR-023-kubernetes-deployment.md`
holds the PR 3 and PR 4 sections meanwhile; ADR-022 is Phase 14's scheduler record, so this record is ADR-023).

## Rolling update and the drain timeline (PR 6)

`make k8s-roll-test` (`scripts/k8s-up.sh roll-test`) answers "do clients notice a gateway rollout?" with a measurement and a control,
not with the absence of an error message. It needs the cluster of `make k8s-up` (the smoke is not rerun; the command waits for every
pod to be Ready with zero restarts and for the gateway to serve `mock-model`) and Go on the host (it builds `tests/k8s/e2e`).

**The load.** 24 closed-loop clients (each sends its next request when its last one ends; after a failure it pauses 50 ms so that a
refusing gateway is not hammered) call `http://127.0.0.1:28080` (kind's port mapping, node, kube-proxy, pod: the whole path) with the key of
the `dev` tenant, read from its 0600 file by the load generator and never printed. Half the clients keep their connection alive, half open a
connection per request, and every client alternates non-streaming and streaming requests. Only the standard library HTTP client is used.
There are no client retries: each request is sent once, and the transport's own replay of a request on a fresh connection is disabled.
Every request is judged completely: a non-2xx status is a failure of class `http_<status>_<error code>`; a stream must carry well-formed
server-sent events of the OpenAI shape and end with `[DONE]` (a connection that breaks or ends before it is `stream_truncated`; a line that is
no SSE field, a payload that is not JSON, an event without `choices` or an event after `[DONE]` is `sse_malformed`; an error event is
`stream_error_<code>`); a non-streaming body must be complete JSON with a choice (`body_truncated`, `body_malformed`); transport failures are
`connection_refused`, `connection_reset`, `connection_closed` (EOF before any response), `timeout`, `transport_other`.

**The procedure** (the phase length is 20 s; `ROLL_PHASE_SECONDS` shortens it for debugging, which makes the pass criterion unlikely to hold):
baseline 20 s; then rollouts that alternate two kinds of change, starting with the ConfigMap (a new generated name, so a new hash, and `GOMEMLIMIT`
100MiB to 99MiB) and then the pod template annotation `serverflow.dev/image-tag` (an annotation change stands in for a new image tag, because no
second image is built). Each is applied from the repository's own render with that one edit, `kubectl rollout status` is awaited, and every replaced
pod must be gone and replaced by a new one. **The rollouts continue until at least 3000 requests were made during them**, at least two and at most
`ROLL_MAX_ROLLOUTS` (6). D20 of the plan said "twice"; the first CI run measured about 38 requests/s (the mock workers, 24 clients, each rollout
about 28 s), so two rollouts made 2123 requests, not 3000. The bound is not lowered: the rollouts go on, and a run that hits the cap short of 3000 fails
saying so. The load continues 20 s after the last rollout. The report gives, per phase and in total, the requests, the failures by class, the number of
rollouts and gateway pods replaced, the durations of every rollout (to `rollout status`, and until the last old pod disappeared), the load generator's
throughput (overall and during the rollouts, so a shortfall is visible), and the rule-of-three bound: with no failure in n requests the true failure
rate is below 3/n at 95 % confidence (3000 requests: 0.1 %), which is printed instead of "no failures, therefore no outage".

**Pass** = zero failed requests in any phase and at least 3000 requests during the rollouts.

**Negative control.** The same procedure runs again with exactly one difference, `SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE=0s`, and it
continues rolling (same cap) until a failure a missing drain explains has been seen. Why this control and not "resilience off" (the plan's first idea):
the first CI run, with resilience off, produced 1551 failures of which 1546 were `http_503_no_capacity`, 867 of them in the baseline before any rollout.
Without the queue, 24 clients exceed what three mock workers accept, and the gateway rejects regardless of any rollout; that is a capacity effect and
would make a control pass for the wrong reason. With resilience on and the grace at 0 the queue, the admission limits and the capacity are those of the real
run. What the grace changes is visible in `internal/gateway/server.go`: on SIGTERM the gateway sets `draining` (`/readyz` 503, `Connection: close`), and
with a grace it keeps the listener open for that long while kube-proxy converges; with the grace 0 it closes the listener at once. Requests that the
node still routes to the terminating pod during the propagation window (a second or two) are then refused or reset, and new connections of the
connection-per-request clients are the likeliest to hit it. In the first run's resilience-off control only five such failures appeared
(`connection_closed` 4, `connection_reset` 1), which is why the control now keeps rolling until it sees one and fails if the cap is reached without.
The control is **judged on drain-attributable failures inside the rollout phases only**: connection refused, reset or closed, timeouts, cut or malformed
streams and bodies, 502 and 504. Capacity 503s (any 503, any 429), failures in the baseline or after the rollouts, `transport_other`, stream error
events and requests the generator could not build do not count; capacity 503s are reported separately, with their baseline count. If no drain-attributable failure appears the
command fails with "the negative control produced no drain-attributable failure during the rollouts: the test cannot see the outage". It is **not** verified
that this control reliably produces failures on a cluster; the next CI run decides, and if it does not, the fallback is to also shorten
`terminationGracePeriodSeconds` and `shutdown_timeout` in the control so that in-flight streams are cut (the other half of the drain), which changes the
control's definition and is therefore not done speculatively.
The original gateway objects are applied again at the end (an exit trap, also on error or interrupt; signals are ignored while it restores), the extra ConfigMaps are deleted and the
restoring rollout is awaited. The restore is silent and the verdict line is printed after it, so it is the last line; a restore that fails turns even a passing run into `roll-test: FAIL` with a non-zero exit status, and an interrupt exits 130 or 143. After each rollout the command checks that every gateway pod that existed before is gone and that as many exist now; a rollout that replaced nothing stops the run. The negative control passes only if the server failed requests *during the rollouts* (a failure in the baseline, or a request the generator could not build, does not count). Reports, a per-request log (`unix ms,client,stream,keep-alive,status,class,latency ms`) and the pod observations
stay in `.dev/k8s/roll/`; `make k8s-reset` removes them.

**Per replaced pod.** The harness polls the gateway pods once a second and records every pod with a `deletionTimestamp`. SIGTERM was sent at
`deletionTimestamp - deletionGracePeriodSeconds` (45 s), and the pod object is gone about when its containers have exited, so "SIGTERM to gone"
is reported per pod (to the second). The time from SIGTERM to the *last request that pod accepted* is **not obtainable** from the client side: the gateway puts no
pod identity in a response, and adding one to production code was out of scope for this change. SIGTERM-to-gone is its upper bound.

### The drain timeline (design values; measured values are to be filled in by CI)

For a pod replaced by a rolling update (t = 0 when the API server sets `deletionTimestamp`; its replacement was already Ready, `maxSurge: 1`).
The settings are `readiness_grace` 5 s, `shutdown_timeout` 30 s, `terminationGracePeriodSeconds` 45 s, no `preStop` hook (D19).

| t | Kubernetes | gateway | measured |
| --- | --- | --- | --- |
| 0 | the EndpointSlice marks the pod terminating and not ready; kube-proxy rewrites its rules on its next sync; the kubelet sends SIGTERM | `/readyz` 503, `Connection: close` on every later response, `gateway_draining 1`, still serving | to be measured by CI |
| 0 to 5 s | rules converge; keep-alive clients get `Connection: close` and reconnect to other pods | the listener stays open for the grace; connections that still arrive from stale rules are served | to be measured by CI: time until the last new connection reaches this pod |
| 5 s | | queue and listener close; in-flight requests and streams run on | to be measured by CI |
| up to 30 s | | in-flight work finishes; at 30 s after the signal what remains is cut | to be measured by CI: SIGTERM to gone, per pod, max |
| 45 s | the kubelet sends SIGKILL if the container is still alive | | to be measured by CI: whether any pod needed it (it must not) |

The numbers (propagation time, SIGTERM-to-gone per pod, rollout durations, request counts, the control's failures) go into
`docs/benchmarks/phase-17-kubernetes.md` after the first CI run; none is stated here because none has been measured. If the control fails
requests and the real run does not, the readiness grace is long enough for this cluster; if the real run fails, the fallback written in D19 is the
native `sleep` preStop hook, and the grace and `terminationGracePeriodSeconds` are the values to tune, with the evidence, in a follow-up.

### Observation runs (optional, not gating, not in the code)

Run them by hand while a load runs (`.dev/k8s/roll/e2e run --key-file .dev/k8s/api_key` reads control lines on standard input, `stop` ends it) and
record what the report says: `kubectl delete pod` of one gateway under load; a rollout to an image that never becomes ready (for example
`kubectl set image deployment/gateway gateway=serverflow/gateway:missing`; `progressDeadlineSeconds` marks it stalled, the old pods keep serving,
expect zero failures, then `kubectl rollout undo deployment/gateway`); a worker rolling restart (`kubectl rollout restart deployment/mock-worker`: with the 6 s
drain announce expected zero failures); a control-plane restart (observe only: the registry is in memory and a restart is not zero-impact).

## Network policy (PR 5)

`deploy/kubernetes/base/networkpolicy.yaml` (phase `foundation`, applied before any pod exists): `default-deny-all` selects every pod and denies
ingress and egress; `allow-dns-egress` lets every pod reach CoreDNS (kube-system, `k8s-app=kube-dns`, port 53); then one policy per component, named after
it, selects `app.kubernetes.io/name=serverflow` plus `app.kubernetes.io/component=<name>` and lists exactly the traffic below. Ports are numeric container ports
(policy is evaluated after the Service's DNAT, so the Service port is not what matches). A new workload without a policy can neither send nor receive, and a
test fails until it has one.

| From | To (container port) | Why it exists (source in the manifests) |
| --- | --- | --- |
| gateway | control-plane 9090 | `SERVERFLOW_GATEWAY_CONTROL_PLANE_URL` (worker registry) |
| gateway | mock-worker 9000 | inference and health, to the pod IPs the registry returns (the worker advertises `$(POD_IP):9000`) |
| gateway | redis 6379, postgres 5432, redpanda 9092 | Secrets `redis`, `dsn`, `kafka` (rate limit, API keys, lifecycle events) |
| gateway | jaeger 4318 | OTLP/HTTP traces |
| mock-worker (agent) | control-plane 9090 | registration and heartbeat |
| mock-worker | jaeger 4318 | `--otlp-endpoint` |
| usage-consumer | redpanda 9092, postgres 5432 | Secrets `kafka`, `dsn` |
| migrate Job, admin pods | postgres 5432 | Secret `dsn` |
| redpanda-init Job | redpanda 9092 | `RPK_BROKERS` |
| redpanda | redpanda 9092 | the broker advertises the Service name `redpanda:9092` and so dials itself (a precaution; not observed) |
| prometheus | gateway 9100, control-plane 9101, mock-worker 9000 | the scrape jobs of `prometheus.yml` (pod discovery) |
| grafana | prometheus 9090 | the datasource |
| prometheus | the Kubernetes API (below) | pod discovery |
| node (below) | gateway 8080, control-plane 9090, mock-worker 9000, prometheus 9090, grafana 3000, jaeger 16686 | NodePorts 30080, 30090, 30091, 30300, 30686; kubelet HTTP probes; the API server's pod proxy (smoke K6) |

Every row is an egress rule on the left component and an ingress rule on the right one. Not allowed, and therefore denied: the control plane dials
nothing (it makes no outbound call), Redis and PostgreSQL dial nothing, Jaeger and Grafana's only outbound is Prometheus (Grafana), no pod reaches the
Internet (any update or telemetry check of Redpanda or Grafana is expected to fail quietly; unverified), nothing but the gateway and the usage consumer/jobs reaches PostgreSQL, nothing but the gateway
reaches Redis. `tests/k8s/netpol_test.go` derives this table from the rendered manifests (env, args, ConfigMaps, Secrets read, the worker's advertised
address, the Prometheus configuration, probes and NodePort Services) and requires the policies to equal it in both directions; 32 mutations (a missing or loosened
rule, `0.0.0.0/0`, an undeclared or named port, a rule without `from`/`ports`, a new workload, a new dial) are each named.

**Node-sourced traffic.** NodePort traffic from the host (kind publishes it on 127.0.0.1 and the node receives it from Docker's bridge), the kubelet's probes and the
API server's pod proxy do not come from a pod. Their source address is the node's: the pod network's gateway on the node (`10.244.0.1`, the host side of the pod's
veth) or the node's address on kind's Docker network. The latter is not known statically (`172.18.0.x` on a default engine, others elsewhere), so the allowed
sources are `10.244.0.1/32`, `172.16.0.0/12` and `192.168.0.0/16`, on those pods and ports only. The pod CIDR `10.244.0.0/16` is deliberately not allowed, so a
pod cannot use these rules, and nothing is open to `0.0.0.0/0`. Which of the node addresses a given path really presents, and whether the CNI applies policy to
node-originated traffic at all, is **not verified**: if the advisory job shows probes or NodePorts failing, widen or correct these ranges in one place.

**The Kubernetes API (Prometheus only).** Egress to `172.16.0.0/12` and `192.168.0.0/16` on 6443 (the API server on the control-plane node, after DNAT) and to
`10.96.0.1/32` on 443 (the `kubernetes` Service, for an implementation that matches before DNAT). No other pod has an ipBlock rule. Everything else Prometheus
reaches is a pod rule.

**Enforcement (K15).** NetworkPolicy objects are accepted by any API server; they only do something when the CNI enforces them. kind's default CNI is kindnet,
and since kind v0.24.0 kindnet embeds `sigs.k8s.io/kube-network-policies`: the kind v0.33.0 kindnetd source enforces ingress, egress and `ipBlock` rules (nftables with NFQUEUE, so the runner's kernel needs the nft queue modules; kindnetd runs fail-open, so a stopped queue listener lets traffic through, and if it cannot start the controller it logs that it is skipping network policies and enforces nothing). That was read from the sources, not observed on a cluster (kind and kubectl cannot run where this was written), so the smoke does not assume it. NodePort traffic is evaluated before SNAT, so its source is the Docker bridge gateway (172.x), while `10.244.0.1/32` is the source of kubelet probes. K15 runs two short-lived pods built from `tools/netpol-probe.yaml`
(restricted-compliant, Redis' image already on the node, deleted afterwards): an *outsider* (component `netpol-probe`, selected by no allow rule) tries Redis,
PostgreSQL, Redpanda and the control plane by ClusterIP, and all four must fail (addressed by IP, so a blocked DNS lookup cannot fake it); a *control* labelled `admin`
must reach PostgreSQL (so the probe tool works and an allow rule is honoured) and must not reach Redis; and a completion through the gateway NodePort must still be 200.
If the outsider connects anywhere K15 prints `NetworkPolicy is NOT enforced by this cluster's CNI` and fails. On a cluster whose CNI ignores policy the
manifests are still correct and still applied, but the isolation described here does not exist, and the smoke says so.

The plaintext links of the earlier sections are unchanged (the control-plane token and OTLP are still HTTP): policy confines who can send to the control plane
(the gateway, the worker agents and the node) and to Jaeger (the gateway and the workers), it does not encrypt. The NodePort exposure of the control plane to the
host (127.0.0.1) is unchanged.

## Observability (PR 4)

```text
namespace serverflow
  prometheus   Deployment x1 (Recreate), emptyDir 512Mi, 2 d retention        Service :9090, NodePort 30091 -> host 127.0.0.1:29091
  grafana      Deployment x1 (Recreate), emptyDir                              Service :3000, NodePort 30300 -> host 127.0.0.1:23000   (user admin)
  jaeger       Deployment x1 (Recreate), in-memory traces                      Service jaeger :4318 (OTLP/HTTP, cluster only)
                                                                               Service jaeger-ui :16686, NodePort 30686 -> host 127.0.0.1:26686
  ConfigMaps generated by kustomize from the repository files (names carry a content hash: a change rolls the pod):
    prometheus-config   observability/prometheus/kubernetes/prometheus.yml   (the one Kubernetes-specific file)
    prometheus-rules    observability/prometheus/rules/{recording,alerts}.yml
    grafana-datasources, grafana-dashboard-provider   observability/grafana/provisioning/{datasources,dashboards}/*.yml
    grafana-dashboards  observability/grafana/dashboards/*.json   (five dashboards)
  Secrets created by the script: grafana (admin password) in addition to metrics (the scrape token, mounted into Prometheus as a file)
```

The rules, dashboards and Grafana provisioning are the files the host and Compose stacks use, read through `configMapGenerator` (which is why the
render needs `--load-restrictor=LoadRestrictionsNone`); a test compares every ConfigMap with its source files and fails if a file of those directories is
missing in the cluster. Prometheus' configuration needed a Kubernetes variant (pod discovery instead of DNS or files, as Compose needed one for the token);
a test keeps its global settings, rule files, job names, scrape intervals and sample limits equal to `prometheus.yml` and `compose/prometheus.yml`, and
`quality.sh observability` runs promtool on it (syntax only: the token file and API credentials exist only in the cluster).

**The one API credential.** Prometheus discovers targets with the Kubernetes API (`kubernetes_sd_configs`, role `pod`, namespace `serverflow`). Every pod keeps
`automountServiceAccountToken: false` (the audit still reports it); the Prometheus pod alone projects an explicit, expiring ServiceAccount token, the
cluster CA and its namespace under `/var/run/secrets/kubernetes.io/serviceaccount`. Its ServiceAccount is bound by a namespaced `Role` that allows
`get`, `list`, `watch` on `pods` and nothing else; tests fail if another pod projects a token, if the Role grows (resources, verbs), or if any cluster-scoped
RBAC appears. Targets are chosen by the pod's component label and a named container port, so the gateway replicas (`metrics`, 9100, bearer token), the
control plane (`metrics`, 9101, bearer token) and the mock workers (`http`, 9000) are all scraped, and `worker_id` is the pod name, which is the ID the
agents register with, so the worker-side series join the control plane's per-worker gauges.

**Tracing is on.** The gateway ConfigMap sets `SERVERFLOW_TRACING_ENABLED=true`, endpoint `http://jaeger:4318`, sample ratio 1.0 and plaintext allowed (inside the
cluster; since PR 5 only the gateway and the mock workers may reach Jaeger's OTLP port); the mock workers get `--otlp-endpoint=http://jaeger:4318 --trace-insecure-ok` and record spans only for
requests the gateway sampled. Traces live in Jaeger's memory and are gone with its pod.

Grafana: anonymous access and sign-up off, the password from the Secret, the dashboards provisioned read-only (they cannot be saved from the UI). Open
`http://127.0.0.1:23000`, user `admin`, password in `.dev/grafana.env`. Prometheus is `http://127.0.0.1:29091`, the Jaeger UI `http://127.0.0.1:26686`.

## The core system (PR 3)

```text
namespace serverflow
  control-plane   Deployment x1, strategy Recreate (in-memory registry)   Service :9090, NodePort 30090 -> host 127.0.0.1:29090
  gateway         Deployment x3 (base 2), RollingUpdate maxUnavailable 0 maxSurge 1, PDB maxUnavailable 1,
                  terminationGracePeriodSeconds 45                        Service :8080, NodePort 30080 -> host 127.0.0.1:28080
  mock-worker     Deployment x3: [mock-worker :9000] + [worker-agent, status on 127.0.0.1:9102], worker ID = pod name, pod IP advertised
  usage-consumer  Deployment x1 (exec probes, loopback health port)
  gateway-config  ConfigMap generated by kustomize (name carries a content hash: a change rolls the gateways)
  Secrets created by the script: postgres, dsn, redis, kafka, controlplane, metrics
```

Gateway settings are explicit (D12): `resilience.mode=on`, `readiness_grace=5s`, `shutdown_timeout=30s`, `worker_networks=10.244.0.0/16` (the kind
pod subnet), tracing on since PR 4 (see above). Tests assert the relations (grace < shutdown timeout < termination grace - 10 s, maxUnavailable 0, at least
two replicas, PDB selects the gateway) against the rendered manifests and show each one failing when broken. Readiness of the gateway is `/readyz`,
liveness `/healthz` (a worker outage never restarts gateways); the worker pod is Ready only when the mock worker answers `/readyz` and the agent
reports registered with an accepted heartbeat (`healthcheck` exec probes on loopback). The workers are not behind any Service: the gateway dials the
pod IPs the registry returns.

Tenants and keys: `make k8s-up` runs the admin tool in one-off pods (`deploy/kubernetes/tools/admin-pod.yaml`, hardened like every workload, created
bare and read after it ends because attaching to a container that exits at once loses output; deviation from the plan's `kubectl run --rm -i`),
creates tenants `dev` and `limited`, and writes their keys to `.dev/k8s/api_key` and `.dev/k8s/api_key_limited` (0600). The key is in the pod's
log on the node until the pod is deleted, a residual window inside a throwaway cluster that check K13 searches afterwards.

### Smoke and audit

`make k8s-smoke` prints PASS/FAIL per check and ends `smoke: N of N checks passed, 0 failed, 0 skipped`:
K1 pods ready, zero restarts; D2-D4 jobs and Redis auth; K2 gateway ready and `mock-model` listed through the node port; K3 401 without/with a bad key;
K4 completion; K5 stream ends `[DONE]`; K6 24 requests reach all three worker pods (read from `worker_requests_total` through the API server's pod
proxy, the smoke does not depend on Prometheus for it); K7 limited tenant `200 200 200 429` with `Retry-After`; K10 usage rows for a fresh tenant; K12 deleting a
worker pod (leaves the pool, requests stay 200, replacement registers); K14 shared quota (30 requests, at most 3 admitted, 429s logged by at least two
gateway pods); K15 NetworkPolicy is enforced (see "Network policy"), after K14 and before the restarts of D5-D7; D5-D7 restarts of PostgreSQL, Redpanda, Redis; K13 none of the generated secrets is in any non-Secret object, event, pod log or the node's
container logs (searched, never printed; fails if it searched next to nothing). K8 Prometheus (the smoke runs it right after K7, once requests exist): every gateway pod, the control plane and every worker pod is a target with `up == 1`
(counts equal the pod counts, so a replica Prometheus never found fails), no `serverflow-*` target is down, and `sum(inference_requests_total)` is
positive; K9 Grafana: `/api/health` reports the database ok, the search lists exactly five dashboards with the uids `sf-cluster-overview`, `sf-scheduler`,
`sf-resilience`, `sf-model` and `sf-worker`, and the datasource `serverflow-prometheus` passes its health check (the admin password is handed to curl on
standard input); K11 Jaeger: one request through the gateway, its `X-Request-ID` is looked up through the v3 query API (`serverflow.request_id`), and the
trace must hold spans of `serverflow-gateway` and `serverflow-mock-worker` (polled up to 90 s: spans are exported in 5 s batches). K8, K9 and K11 run
before K12, which deletes a worker pod and would change the target counts. The checks' decision logic is run in tests against canned answers
(`tests/k8s/observability_test.go`), the cluster itself only in CI.
`make k8s-audit` reads the pods back from the API and prints, per container, user, non-root, read-only root, privilege escalation, dropped capabilities,
seccomp, requests and limits; any exception fails it; the last line is `audit: N pods, M containers, 0 exceptions`.

## The foundation and data tier (PR 2)

```text
host 127.0.0.1 only (kind extraPortMappings; the gateway and control plane NodePorts exist since PR 3, Prometheus, Grafana and Jaeger since PR 4):
    gateway :28080   control plane :29090   Prometheus :29091   Grafana :23000   Jaeger :26686
kind cluster "serverflow", one node, pod CIDR 10.244.0.0/16, kubeconfig .dev/k8s/kubeconfig, context kind-serverflow
namespace serverflow   (Pod Security Admission: enforce, audit and warn = restricted, pinned to v1.36)
  postgres   StatefulSet x1, PVC 1Gi          Service postgres :5432
  redis      StatefulSet x1, emptyDir         Service redis    :6379
  redpanda   StatefulSet x1, PVC 1Gi          Service redpanda :9092      (SASL/SCRAM required)
  Jobs: migrate (serverflow/admin), redpanda-init (creates inference.lifecycle.v1)
  Secrets (created by the script from .dev/, never in git): postgres, dsn, redis, kafka
```

PostgreSQL, Redis and Redpanda are **single instances for development**. A node loss loses the volumes; nothing here claims high
availability. The images and digests are the ones `docker-compose.yml` pins, and a test fails if the two files disagree.

## Prerequisites (nothing is installed for you)

| Tool | Version | Why |
| --- | --- | --- |
| Docker | engine with **at least 6 GB of memory** (4 GB until PR 4) and 8 GB of free disk | runs the kind node; `k8s-up.sh` refuses to start below that |
| `kind` | v0.33.0 (`deploy/kubernetes/tools.env`) | creates the cluster |
| `kubectl` | v1.36.x (one minor either way of the node's Kubernetes 1.36.4) | talks to it |

`deploy/kubernetes/tools.env` is the single place that pins every version, checksum and digest (kind, the node image, kubectl,
kubeconform). The static checks need none of these on the host: `scripts/k8s-tool.sh` runs the pinned `kubectl` and `kubeconform`
container images (digest-pinned, repository mounted read-only, no capabilities) when a binary of exactly the pinned version is not
on `PATH`.

**Colima's default VM has 4 GiB (3.8 GiB usable), which is below the 6 GB minimum, and resizing it restarts every container of every
other project in it.** Run the cluster commands on a larger engine (Docker Desktop with 8 GB, a second Colima profile, a Linux
host) or leave them to CI. The static checks (`make k8s-check`) need neither the memory nor kind.

## Commands

| Command | What it does |
| --- | --- |
| `make k8s-check` | the static checks with no cluster: `kubectl kustomize` of base and overlay, `kubeconform -strict`, the `tests/k8s` policy tests, shellcheck |
| `make k8s-render` | prints the rendered overlay (no Secret content: safe to paste into an issue) |
| `make k8s-up` | preflight, secrets, cluster, images, namespace, Secrets, data tier, jobs; idempotent |
| `make k8s-smoke` | K1-K15 and D2-D7 (see above); the last line is `smoke: N of N checks passed, 0 failed, 0 skipped` |
| `make k8s-down` | deletes the kind cluster and its kubeconfig and nothing else |
| `make k8s-reset` | `down`, then removes `.dev/k8s/` (the secrets in `.dev/` and the `serverflow/*:dev` images stay; `make dev-reset` removes them) |
| `make k8s-audit` | per-container security and resource audit of the running pods |
| `make k8s-roll-test` | the rolling-update measurement and its negative control (see "Rolling update and the drain timeline"); about 10 minutes; restores the gateway configuration afterwards; the last line is `roll-test: PASS (...)` or `roll-test: FAIL (...)` |

`scripts/k8s-up.sh status|diagnose` show pods, events and the last log lines (no secret values).

The manifests are rendered with `kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/kubernetes/overlays/kind` and applied
from a pipe, one phase at a time (`serverflow.dev/phase`: foundation, data, jobs). Never run `kubectl apply -k` on these directories: the
default load restrictor forbids the `observability/` files that later pull requests read, and `apply -k` has no flag to relax it
(a test pins the command).

### What `make k8s-up` does

1. Preflight: Docker reachable with at least 6 GB, `kind` and `kubectl` at the pinned versions, the five host ports free.
2. `scripts/dev-up.sh secrets`: the same generator Compose uses writes `.dev/{postgres,dsn,redis,kafka,...}.env` (0600 in a 0700
   directory). Both stacks use the same files; the service names in the DSNs are the same on purpose.
3. Creates the cluster from `deploy/kubernetes/kind/cluster.yaml` with `--kubeconfig .dev/k8s/kubeconfig` (the node image is pulled
   through `scripts/ci-docker-pull.sh`, with retries).
4. Builds `serverflow/admin:dev` from the unchanged `deploy/docker/Dockerfile` and loads it with `kind load docker-image` (fallback:
   `docker save` and `kind load image-archive`). The other five images join in PR 3.
5. Applies the namespace and ServiceAccounts, creates the Secrets (`kubectl create secret generic --from-env-file ... --dry-run=client
   -o yaml | kubectl apply -f -`: values never appear on a command line), applies the data tier, waits for the three StatefulSets.
6. Deletes and re-applies the two Jobs (a Job's template cannot change in place; both are idempotent) and waits for them.
   Every wait has a deadline; on a timeout the script prints status, events and the last 20 log lines of every pod.

## Safety: it can only ever touch its own cluster

Every `kubectl` call of `scripts/k8s-up.sh` goes through `kc()`, which passes `--kubeconfig .dev/k8s/kubeconfig --context kind-serverflow`
explicitly. Before the first call, `guard_context` refuses any current context other than `kind-serverflow` and any API server that is
not on loopback, and the script unsets `KUBECONFIG` so `~/.kube/config` is never read. `kind create|delete|export` are always given
`--name serverflow --kubeconfig ...` so kind does not write into `~/.kube/config` either. Tests run the script against a fake `kubectl`
reporting other contexts and servers and assert that nothing but the two `config` reads is called.

### What the scripts will and will not delete

- `K8S_UP_DIR` (default `.dev/k8s`) is the state directory and `make k8s-reset` deletes it, so the script refuses any value that is not a directory
  strictly inside `<repo>/.dev` (no `..`, `.` or empty components, not `.dev` itself, no symlink on the way). Tests use a throwaway directory there.
- `down` and `reset` delete a kind cluster named `serverflow` only if this checkout created it: `up` writes `.dev/k8s/owns-cluster` before
  `kind create`, and the dedicated kubeconfig also counts. A cluster of that name with neither is refused loudly (by `up` as well, which would otherwise
  deploy into it); delete it yourself with `kind delete cluster --name serverflow` if it is yours. The only delete is
  `kind delete cluster --name serverflow --kubeconfig .dev/k8s/kubeconfig`; the only recursive `rm` is the validated state directory. Tests run `down` and
  `reset` against stub `kind` and `docker` binaries and check every call.
- `serverflow/admin:dev` is built with the same tag Compose uses and is not labelled for this project (a build is a build, kind copies it into the node);
  `make k8s-reset` and `down` therefore never remove images, and the plan's "Docker objects labelled for this project" (AC 12) holds for the kind node
  containers (kind labels them `io.x-k8s.kind.cluster=serverflow`), not for that image.
- The Secrets are applied with `kubectl apply --server-side`, so the object does not carry a `last-applied-configuration` annotation holding their data
  (client-side apply would store it there, readable by anyone who can `get` the object). The server-side flag was not run against kind (see "What is verified");
  the k3s run used client-side apply.

## Security posture

Every container: numeric non-root user, `runAsNonRoot`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true` (writable
paths are `emptyDir` with a `sizeLimit`, Redis' password file on a memory-backed one), `capabilities.drop: [ALL]`, seccomp
`RuntimeDefault`, `automountServiceAccountToken: false`, a ServiceAccount per workload bound to nothing, no host namespaces or
`hostPath`. The namespace enforces Pod Security `restricted`, so a pod that violates this is rejected at admission (this is only
exercised once a cluster runs; the static tests state the same rules independently).

Secrets are generated into `.dev/` and exist in the cluster as Kubernetes Secrets: **base64 in etcd, readable by anyone who can run
`kubectl` in the namespace, visible as environment to anyone who can `exec`**. This is the same trust boundary as Compose's Docker socket
and is acceptable for a throwaway local cluster; a real deployment needs encryption at rest, an external secret store and RBAC that
denies `get secrets`, none of which is built or claimed. No Secret and no secret-looking value is in any file under
`deploy/kubernetes/` (tested, including the rendered output).

`scripts/k8s-up.sh` never traces and starts the secret generator (`dev-up.sh secrets`) without the environment's shell options, because
`SHELLOPTS=xtrace` in the caller's environment would make the generator print the passwords it creates (a test shows that it does when started by hand and that
it does not through `k8s-up.sh`). A shell you started with `bash -x` or `set -x` and `export SHELLOPTS` yourself is your own risk; `dev-up.sh` itself still
honours the environment, as before.

The container fallbacks of `scripts/k8s-tool.sh` mount only `deploy/` and `observability/` read-only into the kubectl container (which has no network) and nothing
into the kubeconform container (which has network for its schemas and receives the manifests on standard input), so `.dev/` and its secrets are never visible to them.

Residual notes: `kc` and the preflight accept a PATH `kubectl` within one minor version of the node's Kubernetes (the static checks use exactly the pinned one);
`DOCKER_HOST` and `DOCKER_CONTEXT` are not pinned, so the script uses whatever engine `docker` currently points at and prints its context.

The control-plane token crosses the cluster network in plain HTTP (ADR-021 W1). Since PR 5, NetworkPolicy confines who can reach the control plane to the
gateway, the worker agents and the node (NodePort, probes); it does not encrypt, and a CNI that does not enforce policy (K15 reports it) confines nothing.

## Resources

Memory limits (requests are 50-70 % of them, no CPU limits): PostgreSQL 256 Mi, Redis 64 Mi, Redpanda 768 Mi, jobs 64 Mi and 128 Mi. The
sum of all limits plus about 1 GiB for the kind node must fit in the engine memory `k8s-up.sh` demands; a test computes it (currently
2848 MiB of limits, 3872 MiB with the node, against 6144 MiB: the minimum was raised from 4096 in PR 4, which would have left 224 MiB of slack). PR 3
added the control plane, three gateways, three worker pods and the consumer; PR 4 adds Prometheus, Grafana and Jaeger at 256 Mi each (requests 128 Mi).
These are ceilings, not measurements; nothing has been measured on a running cluster. PR 6 may revise them.

## State: what survives what

| Event | PostgreSQL | Redpanda | Redis |
| --- | --- | --- | --- |
| pod restart / deletion | keeps data (PVC) | keeps the topic (PVC) | loses keys (emptyDir; rate-limit counters are rebuildable) |
| `make k8s-down` | everything is deleted with the node | same | same |

## What is verified and what is not

Verified in this pull request (no cluster): the manifests render with the pinned `kubectl`; `kubeconform -strict` accepts the rendered
overlay against the Kubernetes 1.36.4 schemas; `tests/k8s` states the hardening, probe, resource, label, image-pin and secret policies and
proves each fails when broken; the scripts are shellcheck-clean, run under bash 3.2 and 5 with `set -u`, and refuse a foreign context.

**Also verified once, on a real API server that is not kind:** the same manifests, applied by the same `scripts/k8s-up.sh` functions (`apply_phase`,
`create_secrets`, `wait_statefulset`, `wait_job`, `cmd_smoke`, sourced from a scratch copy of the script and driven by a `kubectl` shim) against a
`rancher/k3s:v1.36.4-k3s1` container (Kubernetes v1.36.4, arm64, memory capped at 2.2 GB, then deleted). Result: the namespace with Pod Security
`restricted` admitted PostgreSQL, Redis and Redpanda with these security contexts and a privileged test pod was rejected; the three StatefulSets
became Ready, the PVCs bound, `migrate` and `redpanda-init` completed, smoke checks D1-D7 passed (7 of 7), re-applying changed nothing, and a search of
every non-Secret object, event, pod log, pod description and the node's container logs for the generated secret values found none (searched, never
printed). This is evidence about the manifests and the script logic. It is **not** a kind run: k3s differs from kind (flannel instead of kindnet,
`local-path` instead of kind's `standard` StorageClass), and it needed the VM's `br_netfilter` kernel module, which Colima's VM does not load: without
it a k3s container there cannot reach any ClusterIP (pod-to-Service traffic times out; the Jobs failed with `cannot reach the database` and the script's
failure output was checked to be secret-free). kind's kindnet does not use a bridge, so it may not be affected; that is unverified.

**PR 6, not run anywhere.** Verified without a cluster: the load generator's classification and aggregation against a fake HTTP server (a truncated stream, a
malformed SSE, an error event, an event after `[DONE]`, a truncated body, 4xx/5xx statuses with their codes, refused, reset, closed and timed-out connections, that a
request is sent exactly once, that the key appears in no output, the phases, the verdict, the control's inverted verdict and the rule of three); the objects
the script applies to roll the gateway (the real render with one edit each, checked against the original through the pinned `kubectl`); the procedure in the script; the CI
wiring; shellcheck v0.10.0 on `scripts/k8s-up.sh`. NOT run: the command on a cluster, so whether 24 clients make 3000 requests during two rollouts, whether the drain really
gives zero failures, whether the control fails (the nightly job decides), the real propagation and drain times, and the CI step's duration. Honest limits of the design: the
load generator is a client on the host side of kind's port mapping, which adds its own hop; a failure there is counted, not explained; 24 clients on one machine are not a
production load; an annotation change stands in for an image tag change.

**PR 5, not run anywhere.** Verified without a cluster: the render and `kubeconform -strict` (59 resources valid) with the pinned tools through Docker, the `tests/k8s` suite
including the policy-equals-traffic check, its 32 mutations, the probe-pod hardening and K15's decision logic against canned probe output, shellcheck v0.10.0, `bash -n`.
NOT run: any policy on a cluster, so whether kind v0.33.0's kindnet enforces NetworkPolicy, which source address NodePort, kubelet and API-server-proxy traffic presents
(the node ranges above are a reasoned guess), whether pods start and become Ready under default-deny (probes, the Redpanda broker's self-connection, Jaeger and Grafana
start-up), Prometheus reaching the API server on 6443, busybox `nc` behaving as the probe assumes, and K15 itself. The advisory CI job is the first real run.

**PR 4, not run anywhere yet.** Verified without a cluster: the render (pinned `kubectl` through Docker), `kubeconform -strict` (44 resources valid), the `tests/k8s` and
`tests/deploy` suites including the new observability tests, promtool on the Kubernetes Prometheus configuration (syntax only), shellcheck v0.10.0 on `scripts/k8s-up.sh`
and `scripts/quality.sh`, `bash -n`. NOT run: anything on a cluster, so Pod Security admission of the three new pods (the projected token volume in particular),
the images under `readOnlyRootFilesystem` and the chosen users (Grafana 472:0, Jaeger 10001), whether the Jaeger image listens on all interfaces by default (it does
in Compose, which uses the same image), whether `kubernetes_sd_configs` authenticates with the projected token, the metrics token file's permissions for user
65534, the Jaeger v3 query API's exact JSON as K11 parses it, and K8, K9 and K11 themselves. The advisory CI job is the first real run.

**PR 3, not run anywhere yet.** No Docker, no cluster and no `kubectl`/`kind` were available while writing it. What ran: the `tests/k8s` suite against a render
produced by a throwaway kustomize-library program standing in for `kubectl kustomize` (not the pinned kubectl; not installed), the script under
`bash -n`, and the stubbed-`kc` tests of `admin_run` and the audit. NOT run: kubeconform, shellcheck, `kubectl apply` of any of it (so Pod Security
admission of the new pods, probes, the agent registering, the gateway becoming Ready, the NodePorts, every smoke check K1-K14 and the audit's
jsonpath), any image build, `kind load`. The advisory CI job `k8s kind smoke` is the first real run.

**Not verified until the advisory job `k8s kind smoke` (`.github/workflows/kubernetes.yml`) has run on GitHub:** that kind itself creates the
cluster with this configuration; that `kind load docker-image` works with the runner's Docker; that the node pulls the three third-party images from
`mirror.gcr.io` on a runner; that kind's `standard` StorageClass binds the claims; the amd64 builds of everything (the development Mac is arm64; CI is
the amd64 proof). Also unverified: other Kubernetes minors, multi-node behaviour, and every managed Kubernetes difference
(`docs/plans/active/phase-17-kubernetes.md`, "What cannot be verified here").

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `the container engine has N MB of memory; ... need at least 4096` | the engine is too small (Colima default); use a larger engine, see Prerequisites |
| pods cannot reach any Service (`nc: Operation timed out` on a ClusterIP, `cannot reach the database`) | the engine's kernel lacks `br_netfilter` (seen in Colima's VM with k3s); pods reach each other by IP but Service NAT fails on the pod bridge; use an engine whose kernel has the module |
| `kind v0.33.0 is not installed` / version mismatch | install the pinned release yourself; the script never installs anything |
| `refusing to use the kube context` | something pointed `.dev/k8s/kubeconfig` at another cluster; `make k8s-down && make k8s-up` recreates it |
| `port N is already in use` | the host ports are fixed in `deploy/kubernetes/kind/cluster.yaml` (they differ from Compose's); stop what uses it |
| a pod in `ImagePullBackOff` | the node pulls the third-party images itself and retries with backoff; a rate-limited registry resolves itself or needs a different mirror; a `serverflow/*` image in this state was not loaded: run `make k8s-up` again |
| pod rejected by PodSecurity | the message names the violated rule; the manifests are meant to satisfy `restricted` |
| `K15 ... NetworkPolicy is NOT enforced` | the cluster's CNI does not enforce NetworkPolicy; with kind that means the default CNI was disabled or is too old to enforce; the policies are applied but protect nothing |
| a pod is not Ready or a NodePort times out after PR 5 | a policy is missing a source: check which node address reaches the pod (`kubectl get networkpolicy -n serverflow -o yaml`, the node ranges in `networkpolicy.yaml`) and which flow the table lacks |
| StatefulSet stuck Pending | the PVC has no StorageClass: kind ships `standard`; another cluster is not supported by these scripts |

To clean up completely: `make k8s-down` (the cluster and kubeconfig), `make k8s-reset` (also `.dev/k8s/`), `make dev-reset` (secrets and images).
