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
