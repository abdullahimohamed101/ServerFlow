# ADR-023: Kubernetes deployment (Phase 17)

Status: draft. Written section by section by the Phase 17 pull requests; PR 7 completes it. (The plan and docs say ADR-022 for this record,
but ADR-022 is the workload-aware scheduler, so this one is ADR-023.) Decisions D1-D25 are in `docs/plans/active/phase-17-kubernetes.md`.

## PR 3: the core system

- Worker pod: mock worker plus agent as two regular containers; ID = pod name; pod IP advertised; Ready = mock worker `/readyz` and agent `/readyz` (D6, D7).
- Gateway: explicit drain settings in a generated ConfigMap, 45 s termination grace, no preStop, `maxUnavailable: 0`, PDB (D11, D12, D19). Enforced by tests, measured in PR 6.
- Control plane: one replica, `Recreate`, because the registry is in memory.
- Entry points: NodePorts 30080 and 30090 mapped by kind to 127.0.0.1 only; nothing else is published.
- Tenants and keys come from one-off admin pods in the restricted namespace; the key reaches only a 0600 file.
- Known limits: the control-plane token crosses the cluster in plain HTTP (W1); Secrets are base64 in etcd; no NetworkPolicy until PR 5 (added there); nothing here has run on a cluster as of this PR's writing.

## PR 4: observability

- Prometheus, Grafana and Jaeger run as one-replica Deployments with the images and digests `docker-compose.yml` pins (a test enforces equality). The rules, dashboards and Grafana provisioning are read from `observability/` by `configMapGenerator`, not copied; a test compares each generated ConfigMap with its source files.
- Prometheus gets its own configuration file (`observability/prometheus/kubernetes/prometheus.yml`), the third sibling after the host and Compose ones, because a cluster needs pod discovery. A test keeps the global settings, rule files, job names, intervals and sample limits equal; `worker_id` is the pod name, the agents' registered ID (D6).
- Pod discovery needs the Kubernetes API, the only API credential in the namespace: every pod keeps `automountServiceAccountToken: false`, and Prometheus alone projects an expiring token explicitly, bound by a namespaced Role limited to `get/list/watch` on pods. Alternative rejected: DNS discovery through headless Services, which cannot yield the pod name for `worker_id` and would add Services only for metrics.
- Tracing is on (gateway ConfigMap and mock-worker flags) with ratio 1.0 and plaintext OTLP inside the cluster; the control plane and the worker agent do not trace. Jaeger keeps traces in memory.
- Published only on 127.0.0.1 through kind NodePorts: Prometheus 29091, Grafana 23000, Jaeger UI 26686. Jaeger's OTLP port is a ClusterIP Service only.
- Smoke K8 (targets up, requests counted), K9 (Grafana health, five dashboards, datasource health) and K11 (a trace with gateway and mock-worker spans) complete K1-K14. The engine memory minimum rises from 4096 to 6144 MB because the sum of limits plus the node is 3872 MiB.
- Known limits: Prometheus and Grafana keep no state across a pod restart; the metrics token and the Grafana password are Kubernetes Secrets (base64 in etcd); control-plane and OTLP traffic is plaintext until NetworkPolicy (PR 5) narrows who can send; nothing in this PR has run on a cluster.

## PR 5: NetworkPolicy

- Default deny for ingress and egress on every pod of the namespace (`default-deny-all`), DNS to CoreDNS for all, and one policy per component. The allow rules are derived from the manifests (who reads which Secret, which URLs and addresses appear in env, args and ConfigMaps, the worker's advertised port, the Prometheus scrape jobs), written down as a matrix in `docs/operations/kubernetes-deployment.md`, and a test makes the policies equal that derivation in both directions, with mutations for every kind of loosening. A workload without a policy cannot communicate, so adding one forces a policy.
- Ports are numeric container ports, TCP only. Pod peers are selected by the component label inside the namespace; the only ipBlocks are the node sources and Prometheus' API server rule, never `0.0.0.0/0`.
- Node-sourced traffic (NodePort from the host, kubelet probes, the API server's pod proxy) is allowed from `10.244.0.1/32`, `172.16.0.0/12` and `192.168.0.0/16` on exactly the pods and ports that need it. Alternative rejected: allowing any source on the NodePort pods, which would also let every pod reach the control plane's port. The ranges are a reasoned guess about kind's addressing, not an observation.
- Prometheus reaches the API server on 6443 in the Docker ranges and on 10.96.0.1:443 (an implementation may match before or after DNAT).
- Enforcement is proved, not assumed: smoke K15 runs an outsider pod (must fail to reach Redis, PostgreSQL, Redpanda and the control plane by ClusterIP), a control pod labelled `admin` (must reach PostgreSQL only) and a gateway request (must be 200). Whether the pinned kind v0.33.0 kindnet enforces policy was not verified when this was written; K15 fails loudly if it does not. Alternative rejected: installing another CNI (Calico, Cilium), which would add a large dependency to a local cluster.
- Known limits: the control-plane token and OTLP are still plaintext HTTP, now confined to the permitted senders; a CNI that does not enforce policy gives no isolation; policy is layer 3/4 and does not authenticate a pod; nothing in this PR has run on a cluster.

## PR 6: the rolling-update measurement

- The claim "a gateway rollout is invisible to clients" is tested as a measurement with a control (D20): 24 closed-loop clients through the host entry point while all three gateway pods are replaced twice (a ConfigMap hash change, then a pod template annotation change); pass is zero failures of any class and at least 3000 requests during the rollouts, and the report prints the rule-of-three bound (3/n at 95 %) rather than "no failures, therefore no outage". The negative control (resilience off, readiness grace 0) must produce failures, otherwise the command fails because the test cannot see the outage.
- The load generator (`tests/k8s/e2e`, standard library only) classifies every request completely: status and error code, truncated or malformed streams, truncated bodies, and the transport failures. It sends each request once, and net/http's replay of a request on a fresh connection is disabled so that it cannot hide a failure. Alternative rejected: `benchmark run --target`, which counts failures as a percentage and treats up to 5 % as valid.
- The changes are applied from the repository's own render with exactly one edit each (a test compares them with the original), and the gateway is restored by an exit trap. An annotation change stands in for an image tag change because no second image is built.
- Not obtainable from the client: the time from SIGTERM to the last request a given pod accepted, because responses carry no pod identity. The harness reports SIGTERM (deletionTimestamp minus the grace period) to the pod object's disappearance, which bounds it from above. Adding a pod header to the gateway was out of scope.
- CI: the step runs in the nightly and manual runs and on pull requests that touch the gateway manifests, the drain code, the script or the load generator; the job budget rises from 30 to 40 minutes. Advisory, like the rest of the job.
- Drain timeline numbers are not stated anywhere yet: the first CI runs fill `docs/benchmarks/phase-17-kubernetes.md`. D13/D19 values are tuned only with that evidence. The ADR stays a draft until PR 7.
- Known limits: nothing in this PR has run on a cluster; whether 24 clients reach 3000 requests during the rollouts is unverified (a short run fails with a message saying so rather than passing); the load comes from the host side of kind's port mapping.
