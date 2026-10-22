# Kubernetes deployment (Phase 17)

Status: **first draft, written with PR 2 of Phase 17** (`docs/plans/active/phase-17-kubernetes.md`). Only the static foundation and the
data tier exist. The gateway, control plane and workers (PR 3), Prometheus, Grafana and Jaeger (PR 4), NetworkPolicy (PR 5) and the
rolling-update measurement (PR 6) are not built; sections for them are marked and will be filled by those pull requests. The
decision record (ADR-022) is written in PR 7.

## What exists in this pull request

```text
host 127.0.0.1 only (kind extraPortMappings, declared now, nothing listens until PR 3/4 add the Services):
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
| Docker | engine with **at least 4 GB of memory** and 8 GB of free disk | runs the kind node; `k8s-up.sh` refuses to start below that |
| `kind` | v0.33.0 (`deploy/kubernetes/tools.env`) | creates the cluster |
| `kubectl` | v1.36.x (one minor either way of the node's Kubernetes 1.36.4) | talks to it |

`deploy/kubernetes/tools.env` is the single place that pins every version, checksum and digest (kind, the node image, kubectl,
kubeconform). The static checks need none of these on the host: `scripts/k8s-tool.sh` runs the pinned `kubectl` and `kubeconform`
container images (digest-pinned, repository mounted read-only, no capabilities) when a binary of exactly the pinned version is not
on `PATH`.

**Colima's default VM has 4 GiB (3.8 GiB usable), which is below the 4 GB minimum, and resizing it restarts every container of every
other project in it.** Run the cluster commands on a larger engine (Docker Desktop with 8 GB, a second Colima profile, a Linux
host) or leave them to CI. The static checks (`make k8s-check`) need neither the memory nor kind.

## Commands

| Command | What it does |
| --- | --- |
| `make k8s-check` | the static checks with no cluster: `kubectl kustomize` of base and overlay, `kubeconform -strict`, the `tests/k8s` policy tests, shellcheck |
| `make k8s-render` | prints the rendered overlay (no Secret content: safe to paste into an issue) |
| `make k8s-up` | preflight, secrets, cluster, images, namespace, Secrets, data tier, jobs; idempotent |
| `make k8s-smoke` | the data-tier checks D1-D7; the last line is `smoke: N of N checks passed, 0 failed, 0 skipped` |
| `make k8s-down` | deletes the kind cluster and its kubeconfig and nothing else |
| `make k8s-reset` | `down`, then removes `.dev/k8s/` (the secrets in `.dev/` and the `serverflow/*:dev` images stay; `make dev-reset` removes them) |
| `make k8s-audit`, `make k8s-roll-test` | not implemented yet (PR 3 and PR 6) |

`scripts/k8s-up.sh status|diagnose` show pods, events and the last log lines (no secret values).

The manifests are rendered with `kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/kubernetes/overlays/kind` and applied
from a pipe, one phase at a time (`serverflow.dev/phase`: foundation, data, jobs). Never run `kubectl apply -k` on these directories: the
default load restrictor forbids the `observability/` files that later pull requests read, and `apply -k` has no flag to relax it
(a test pins the command).

### What `make k8s-up` does

1. Preflight: Docker reachable with at least 4 GB, `kind` and `kubectl` at the pinned versions, the five host ports free.
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

The control-plane token will cross the cluster network in plain HTTP (ADR-021 W1) once PR 3 adds the control plane; NetworkPolicy (PR 5)
narrows who can reach it, it does not encrypt.

## Resources

Memory limits (requests are 50-70 % of them, no CPU limits): PostgreSQL 256 Mi, Redis 64 Mi, Redpanda 768 Mi, jobs 64 Mi and 128 Mi. The
sum of all limits plus about 1 GiB for the kind node must fit in the engine memory `k8s-up.sh` demands; a test computes it (currently
2304 MiB of 4096). PR 3 and PR 4 raise both numbers.

## State: what survives what

| Event | PostgreSQL | Redpanda | Redis |
| --- | --- | --- | --- |
| pod restart / deletion | keeps data (PVC) | keeps the topic (PVC) | loses keys (emptyDir; rate-limit counters are rebuildable) |
| `make k8s-down` | everything is deleted with the node | same | same |

## What is verified and what is not

Verified in this pull request (no cluster): the manifests render with the pinned `kubectl`; `kubeconform -strict` accepts the rendered
overlay against the Kubernetes 1.36.4 schemas; `tests/k8s` states the hardening, probe, resource, label, image-pin and secret policies and
proves each fails when broken; the scripts are shellcheck-clean, run under bash 3.2 and 5 with `set -u`, and refuse a foreign context.

**Not verified until the advisory job `k8s kind data tier` (`.github/workflows/kubernetes.yml`) has run on GitHub:** that kind creates the
cluster; that `kind load docker-image` works with the runner's Docker; that the node pulls the three third-party images from
`mirror.gcr.io`; that PodSecurity `restricted` admits every pod; that PostgreSQL, Redis and Redpanda actually start and become Ready with these
security contexts (read-only root, uid 70/999/101, emptyDir mounts); that the Jobs complete; and that D1-D7 pass. Also unverified:
`linux/amd64` (the development Mac is arm64; CI is the amd64 proof), other Kubernetes minors, multi-node behaviour, and every managed
Kubernetes difference (`docs/plans/active/phase-17-kubernetes.md`, "What cannot be verified here").

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `the container engine has N MB of memory; ... need at least 4096` | the engine is too small (Colima default); use a larger engine, see Prerequisites |
| `kind v0.33.0 is not installed` / version mismatch | install the pinned release yourself; the script never installs anything |
| `refusing to use the kube context` | something pointed `.dev/k8s/kubeconfig` at another cluster; `make k8s-down && make k8s-up` recreates it |
| `port N is already in use` | the host ports are fixed in `deploy/kubernetes/kind/cluster.yaml` (they differ from Compose's); stop what uses it |
| a pod in `ImagePullBackOff` | the node pulls the third-party images itself and retries with backoff; a rate-limited registry resolves itself or needs a different mirror; a `serverflow/*` image in this state was not loaded: run `make k8s-up` again |
| pod rejected by PodSecurity | the message names the violated rule; the manifests are meant to satisfy `restricted` |
| StatefulSet stuck Pending | the PVC has no StorageClass: kind ships `standard`; another cluster is not supported by these scripts |

To clean up completely: `make k8s-down` (the cluster and kubeconfig), `make k8s-reset` (also `.dev/k8s/`), `make dev-reset` (secrets and images).
