# ADR-023: Kubernetes deployment on a local kind cluster, and the drain it relies on

Status: Accepted (Phase 17)
Date: 2026-11-02

(The Phase 17 plan and some earlier docs call this record ADR-022; ADR-022 is the workload-aware scheduler of Phase 14, so this one is
ADR-023.) The decisions D1-D25 are in `docs/plans/completed/phase-17-kubernetes.md`; this record keeps the ones that are hard to reverse,
easy to misread or security relevant: D1, D2, D6, D7, D9, D10, D11, D14, D16, D17 and D19. The measurements are in
`docs/benchmarks/phase-17-kubernetes.md`, the operating guide is `docs/operations/kubernetes-deployment.md`.

## Context

Phase 16 runs the whole system in Docker Compose on one machine. Spec section 58 asks of Phase 17: "Deploy, service, configmap/secret,
readiness/liveness, resource limits; rolling gateway update with no outage". ADR-021 and the Phase 16 plan (D29) handed over what Kubernetes
must decide: the worker agent's liveness signal, the registry and image tags, TLS for the control-plane token, Secrets instead of `env_file`,
metrics scraping without `dns_sd`, and `gateway.worker_networks`. Phase 15 (`docs/operations/resilience.md`) left real balancers and
termination grace periods to this phase. No Kubernetes cluster or tool existed on the development machine, the Docker VM there has 4 GiB, and
the owner delegated the open decisions to the coordinator (plan, "Decisions on the open questions"); every default was taken and any of them can
be overridden on review.

## Decision

**D1. Plain YAML with kustomize (`deploy/kubernetes/{base,overlays/kind,kind}`), no Helm.** Kustomize is inside `kubectl`, the rendered
output is plain YAML that a reviewer can read and a Go test can parse, and there is no template language to test. `configMapGenerator` appends a
content hash to a ConfigMap's name, so changing a setting rolls the pods, which is also how the rolling-update test triggers a rollout.
The cost: kustomize's default load restrictor forbids reading `observability/` from `deploy/kubernetes/`, so the scripts and docs run
`kubectl kustomize --load-restrictor=LoadRestrictionsNone ... | kubectl apply -f -` (a test pins the command) and `kubectl apply -k` does not work.
Rejected: Helm (a second binary and language, harder diffs, no multi-environment need; a chart can wrap the same base later), one big `kubectl apply -f`
file (no overlays), Jsonnet or cue (a dependency for no gain).

**D2. kind, one node, a dedicated kubeconfig and context.** kind is upstream Kubernetes (API server, kubelet, Pod Security Admission, kube-proxy,
CoreDNS) in a container of the Docker the project already uses, and the same thing runs on a laptop and on a GitHub-hosted runner. Every
`k8s-*` command passes `--kubeconfig .dev/k8s/kubeconfig` and the context `kind-serverflow`, refuses any other context or any non-loopback API
server, and unsets `KUBECONFIG`, so `~/.kube/config` is never read (tested against a fake `kubectl`). Versions are pinned in
`deploy/kubernetes/tools.env`: kind v0.33.0, Kubernetes v1.36.4 (one minor below kind's newest default), the node image by digest from
`mirror.gcr.io`. Rejected: Colima's built-in k3s (reconfigures the user's only Docker VM, differs from upstream, CI cannot reproduce it), k3d (a
second runtime; the named fallback had kindnet not enforced NetworkPolicy), minikube (a VM or driver layer for no benefit).

**D6. The worker agent gets a loopback status endpoint.** The agent had no HTTP server, so Kubernetes could only observe "process exists". It now serves
`/healthz` and `/readyz` on `127.0.0.1:9102` when `worker.status_listen` is set (default off, loopback enforced). `/healthz` ignores the control plane,
so a control-plane outage cannot make Kubernetes restart every agent; `/readyz` needs registration, a recent accepted heartbeat and a backend that
answered. In a pod these are exec probes running the `healthcheck` binary against loopback (kubelet's httpGet targets the pod IP, which a loopback
listener does not answer), so nothing new is exposed on the pod network. Compose uses the same endpoint for the agent healthcheck Phase 16 had to
disable. Rejected: a heartbeat marker file (needs an agent change anyway and says less), no agent probe (the gap above), a separate agent Deployment (D7).

**D7. One pod per worker: the mock worker and the agent are two regular containers; pod IP advertised.** The worker ID is the pod name (downward API
through `$(POD_NAME)` argument expansion, no shell), the agent registers `http://$(POD_IP):9000`, and pod readiness is "mock worker `/readyz` and agent
`/readyz`", so a rolling update keeps the old pod until the new one is registered, not merely running (spec section 39). A replaced pod gets a new ID and never
supersedes its predecessor; a pod killed hard leaves its old ID to go `suspect`, `unhealthy`, `lost`, as in Compose. Phase 18's GPU worker has the same
shape. Rejected: a separate agent Deployment (different failure domain, supersede races), a StatefulSet (stable IDs nobody needs), native sidecar
containers (ordering the drain contract does not need; it holds with concurrent SIGTERM).

**D9. Secrets are generated like Compose's, delivered as Kubernetes Secrets, never in git, never printed.** `scripts/dev-up.sh secrets` calls the existing
generator, so both stacks use the same `.dev/*.env` files. `k8s-up.sh` creates one Secret per concern from those files through a pipe (values never on a
command line) and applies them server-side so no `last-applied-configuration` annotation holds the data; the Prometheus token is a file volume. Tenant and key
bootstrap runs `admin` in one-off hardened pods whose single stdout line goes to a 0600 file. K13 searches pod logs, non-Secret objects, events and the
node's logs for the generated values (searched, never printed). Honest limits: Secrets are base64 in etcd, readable by anyone who can run `kubectl` in the
namespace, and visible as environment to anyone who can `exec`; the kubelet keeps a bootstrap pod's log on the node until the pod is deleted. A real
deployment needs encryption at rest, an external store, RBAC that denies `get secrets` and rotation; none is built or claimed.

**D10. Stateful dependencies are single-instance StatefulSets.** PostgreSQL and Redpanda have a PVC from kind's `standard` class (a restarted pod keeps
its keys and its topic, `kind delete cluster` still leaves nothing); Redis uses an `emptyDir` (its counters are rebuildable). They run under Pod Security
`restricted` with the Phase 16 users and `readOnlyRootFilesystem`. No HA of any kind is claimed. Rejected: operators and Bitnami-style charts (moving
images, heavy CRDs, HA nobody tests), Deployments for state, hostPath volumes (blocked by `restricted`).

**D11. The control plane has one replica and the `Recreate` strategy; the gateway rolls with `maxUnavailable: 0`.** The registry is in memory, so two control
planes would hold two divergent registries; a control-plane restart empties it until the agents re-register (the gateway keeps its view for
`registry_max_staleness`) and **is not zero-impact**. The gateway runs three replicas in the overlay (one cannot roll without an outage) with `maxSurge: 1`,
`minReadySeconds: 5`, a PodDisruptionBudget and only preferred spreading (one node). `SERVERFLOW_GATEWAY_WORKER_NETWORKS` is the pod CIDR pinned in the kind
config, which closes ADR-021's "empty worker_networks" item for this deployment. Gateway readiness is `/readyz` (D13): if every worker is gone the gateways
become NotReady and clients see the node refuse the connection rather than the gateway's JSON `503 NO_CAPACITY`. Liveness is `/healthz` and never depends on
another service (a test enforces it).

**D14. Metrics through plain `kubernetes_sd_configs`, no ServiceMonitor.** The Prometheus operator's CRDs would be a dependency of every cluster and could not
be tested without installing it. A third Prometheus file (`observability/prometheus/kubernetes/prometheus.yml`) has the same global settings, rule files,
job names, intervals and sample limits as the host and Compose ones (a drift test), picks pods by label and the metrics port by name, and sets `worker_id`
to the pod name, the agent's registered ID. The discovery needs the Kubernetes API, the only API credential in the namespace: every pod has
`automountServiceAccountToken: false` except Prometheus, which projects an expiring token bound by a namespaced Role limited to `get/list/watch` on pods.
Rejected: DNS discovery through headless Services (no pod name for `worker_id`), static targets (pod IPs change on every rollout), annotation discovery
(a convention nothing enforces).

**D16. NetworkPolicy: default deny both ways, one policy per component, written from the manifests, enforcement proved.** `default-deny-all`, DNS to CoreDNS,
and one policy per workload; the allow rules are derived from the manifests (who reads which Secret, which URLs and addresses appear in env, args and
ConfigMaps, the advertised worker port, the scrape jobs) and a test makes the rendered policies equal that derivation in both directions, with mutations
for every kind of loosening (32 in `TestNetworkPolicyCatchesMutations`). Ports are numeric container ports, TCP only, and the only ipBlocks are the node
sources and Prometheus' API server rule, never `0.0.0.0/0`. Enforcement is not assumed: smoke check K15 runs an outsider pod (must fail to reach Redis,
PostgreSQL, Redpanda and the control plane), a pod labelled `admin` (must reach PostgreSQL only) and a gateway request (must be 200), and fails loudly when
the CNI does not enforce policy. On the GitHub runner the smoke ended `21 of 21 checks passed`, K15 included (K1-K15 and D2-D7 make 21), so kind v0.33.0's kindnet enforced the
policies there. Rejected: Calico or Cilium in kind (a large dependency for a local cluster), allowing any source on the NodePort pods (every pod could then
reach the control plane's port). Limits: the node-source ranges (`10.244.0.1/32`, `172.16.0.0/12`, `192.168.0.0/16`) are wider than the actual kind
node subnet and were chosen by reasoning about kind's addressing; they worked on the runner and have not been narrowed (follow-up). Policy is layer 3/4 and
authenticates no pod.

**D17. Security context, and what is deliberately weaker than production.** The namespace enforces Pod Security `restricted`; every container runs as a numeric
non-root user with `readOnlyRootFilesystem`, all capabilities dropped, no privilege escalation, `RuntimeDefault` seccomp, no host namespaces or hostPath.
`make k8s-audit` lists this per container and fails on an exception (its CI result is not recorded in this repository). Weaker than production, on purpose and documented: the control-plane token
crosses the cluster in plain HTTP (W1, ADR-021; NetworkPolicy narrows who can send it, nothing encrypts it, and a pod that holds the token can register a
worker); OTLP is plaintext; there is no TLS externally, no Ingress and no service mesh; images are built locally and loaded, never published or signed
(D5: no registry in this phase); Secrets as D9.

**D19. The drain: `readiness_grace` 5 s, `shutdown_timeout` 30 s, `terminationGracePeriodSeconds` 45 s, no `preStop`.** The gateway already implements the wait
in its own SIGTERM handling and tests it; the usual `preStop: sleep` would be a second, untested copy of the same wait (and the image has no `sleep`).
`SERVERFLOW_RESILIENCE_MODE=on` is set explicitly in the ConfigMap, because without it there is no drain at all, and static tests fail if the grace is not
below `shutdown_timeout` or the termination period is under `shutdown_timeout` + 10 s. The claim "a gateway rollout is invisible to clients" is a
**measurement with a control** (D20, `make k8s-roll-test`): 24 closed-loop clients with no retries and no replay by the transport, through the host entry
point, while the gateway pods are replaced by alternating rollouts until at least 3000 requests were made during them; pass is zero failures of every
class and the report prints the rule-of-three bound instead of "no failures, therefore no outage". A negative control changes only the readiness grace
(to 0) and must produce a drain-attributable failure inside the rollouts, otherwise the command fails because the test cannot see the outage.
Rejected as the control: resilience off, which also removes the queue and changes capacity (the first CI run's control failed 1546 requests with
`http_503_no_capacity`, 867 before any rollout: load, not the drain).

## Consequences

- On the GitHub runner, `make k8s-up` plus the smoke ended `smoke: 21 of 21 checks passed`, and the rolling-update run made 4892 requests with 0 failed,
  3360 of them during three rollouts that replaced nine gateway pods (about 38 requests/s). With no failure in 3360 requests the true failure rate during a
  rollout is below about 0.09 % at 95 % confidence (rule of three, 3/3360), and below about 0.1 % for the 3000-request gate. This is one kind
  topology, one machine type and a few runs; it is not a statement about a cloud load balancer.
- The negative control saw failures, but its count varied: 9 drain-attributable failures in 1954 rollout requests in one run, 3 of 1881 in an earlier run,
  5 with the older resilience-off control. It reliably produced at least one, which is all the test needs, but it is a weak signal and the count is not a
  stable quantity. SIGTERM to the pod object being gone was about 4-5 s with the drain and 0-1 s without (an upper bound for "last request served"; the
  client cannot attribute a response to a pod). See the benchmark note.
- Where the test needed changing, the plan's text lost: D20 said "twice" and the first CI run made 2123 requests in two rollouts, so the loop runs until
  3000 requests (at most 6 rollouts).
- One entry point is documented and tested: `scripts/k8s-up.sh` through `make k8s-*`. `kubectl apply -k` fails by design (D1).
- The single control plane and the in-memory registry are a limit of the code, not of the manifests; fixing them (persistence, leader election) is future work.
- Gateway readiness couples to worker availability (D11), and the grace of 5 s is enough only if the front balancer converges within it. In kind kube-proxy is
  the whole path; behind a cloud load balancer the deregistration delay can be tens of seconds and the grace, or a native `sleep` preStop, would need
  retuning with a measurement there.
- The advisory CI jobs (`k8s static`, `k8s kind smoke`) are not required. They are proposed as required after ten consecutive green runs on master (owner
  decision, `docs/development/ci.md`).

## What was not verified

A real cloud load balancer (target registration, health-check periods, deregistration delay), multi-node behaviour (scheduling, spread, node drain with a
PDB across nodes, `externalTrafficPolicy`), GPU nodes (Phase 18), managed-Kubernetes differences (EKS, GKE, AKS CNIs, kube-proxy modes), other Kubernetes
minors than 1.36.4, `linux/amd64` anywhere but the CI runner, Docker Desktop and rootful Linux Docker, Ingress, TLS and cert-manager, encryption at rest and
external secret stores, and the cold and warm `k8s-up` times and memory and disk on a developer machine (not measured; the Colima VM here has 3.81 GiB). The
independent verifier brief of the plan (a clean-room run of docs, a second foreign context, a client written in another tool, deliberate breakage) was not
run by the implementers; see the Implementation Notes of the completed plan.

## Alternatives considered, summarised

| Question | Chosen | Rejected |
| --- | --- | --- |
| Manifests | kustomize | Helm, one big file, Jsonnet or cue |
| Local cluster | kind | Colima k3s, k3d, minikube |
| Worker liveness | loopback status endpoint in the agent | marker file, no probe, separate agent Deployment |
| Worker pod | mock worker and agent in one pod | agent Deployment, StatefulSet, native sidecars |
| Secrets | generated like Compose, Kubernetes Secrets | committed values, an external store (not built) |
| State | single StatefulSets with PVCs | operators, HA, hostPath |
| Scraping | `kubernetes_sd_configs` with a namespaced Role | ServiceMonitor, DNS discovery, static targets |
| NetworkPolicy | default deny, derived allows, K15 | no policy, Calico or Cilium in kind |
| Drain | in-process grace, no preStop, 45 s termination | `preStop: sleep` |
| Rolling-update control | readiness grace 0 | resilience off |
