# ADR-023: Kubernetes deployment (Phase 17)

Status: draft. Written section by section by the Phase 17 pull requests; PR 7 completes it. (The plan and docs say ADR-022 for this record,
but ADR-022 is the workload-aware scheduler, so this one is ADR-023.) Decisions D1-D25 are in `docs/plans/active/phase-17-kubernetes.md`.

## PR 3: the core system

- Worker pod: mock worker plus agent as two regular containers; ID = pod name; pod IP advertised; Ready = mock worker `/readyz` and agent `/readyz` (D6, D7).
- Gateway: explicit drain settings in a generated ConfigMap, 45 s termination grace, no preStop, `maxUnavailable: 0`, PDB (D11, D12, D19). Enforced by tests, measured in PR 6.
- Control plane: one replica, `Recreate`, because the registry is in memory.
- Entry points: NodePorts 30080 and 30090 mapped by kind to 127.0.0.1 only; nothing else is published.
- Tenants and keys come from one-off admin pods in the restricted namespace; the key reaches only a 0600 file.
- Known limits: the control-plane token crosses the cluster in plain HTTP (W1); Secrets are base64 in etcd; no NetworkPolicy until PR 5; nothing here has run on a cluster as of this PR's writing.

## PR 4: observability

- Prometheus, Grafana and Jaeger run as one-replica Deployments with the images and digests `docker-compose.yml` pins (a test enforces equality). The rules, dashboards and Grafana provisioning are read from `observability/` by `configMapGenerator`, not copied; a test compares each generated ConfigMap with its source files.
- Prometheus gets its own configuration file (`observability/prometheus/kubernetes/prometheus.yml`), the third sibling after the host and Compose ones, because a cluster needs pod discovery. A test keeps the global settings, rule files, job names, intervals and sample limits equal; `worker_id` is the pod name, the agents' registered ID (D6).
- Pod discovery needs the Kubernetes API, the only API credential in the namespace: every pod keeps `automountServiceAccountToken: false`, and Prometheus alone projects an expiring token explicitly, bound by a namespaced Role limited to `get/list/watch` on pods. Alternative rejected: DNS discovery through headless Services, which cannot yield the pod name for `worker_id` and would add Services only for metrics.
- Tracing is on (gateway ConfigMap and mock-worker flags) with ratio 1.0 and plaintext OTLP inside the cluster; the control plane and the worker agent do not trace. Jaeger keeps traces in memory.
- Published only on 127.0.0.1 through kind NodePorts: Prometheus 29091, Grafana 23000, Jaeger UI 26686. Jaeger's OTLP port is a ClusterIP Service only.
- Smoke K8 (targets up, requests counted), K9 (Grafana health, five dashboards, datasource health) and K11 (a trace with gateway and mock-worker spans) complete K1-K14. The engine memory minimum rises from 4096 to 6144 MB because the sum of limits plus the node is 3872 MiB.
- Known limits: Prometheus and Grafana keep no state across a pod restart; the metrics token and the Grafana password are Kubernetes Secrets (base64 in etcd); control-plane and OTLP traffic is plaintext until NetworkPolicy (PR 5) narrows who can send; nothing in this PR has run on a cluster.
