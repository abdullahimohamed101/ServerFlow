# Phase 17 benchmark note: Kubernetes (started in PR 2)

Every number here is from a run, with its machine. PR 2 measured what can be measured without a cluster on the development Mac. The cluster
numbers come from the advisory kind job on GitHub-hosted runners (PR #79 and its re-runs), copied from the job logs by hand when Phase 17 was
closed; the logs themselves are not stored in the repository and expire. What was not measured says so.

Machine: macOS arm64 (Apple silicon), 24 GB RAM; Docker engine Colima 0.10.3, Docker 29.8.2, Ubuntu 24.04 aarch64 VM with 2 CPUs and 3.81 GiB
of memory, 60 GiB disk of which 28 GiB were free at the start. Date 2026-10-22.

## Versions resolved and how

| Item | Value | Evidence |
| --- | --- | --- |
| kind | v0.33.0 (released 2026-08-26) | GitHub releases API; `kind-linux-amd64.sha256sum` read and pinned |
| Kubernetes | v1.36.4 (node image and `kubectl`) | listed in kind v0.33.0's release notes; one minor below the release's default 1.37.0 (about three weeks old on 2026-10-22), because a default that new has had little time to meet real use |
| node image | `kindest/node:v1.36.4@sha256:099e0493...` served by `mirror.gcr.io/kindest/node` | the index manifest fetched anonymously from `mirror.gcr.io` hashes to the same digest as the release notes (`curl` + `shasum`), and `docker pull mirror.gcr.io/kindest/node:v1.36.4@sha256:099e...` succeeded anonymously (below); `/kind/version` inside the image prints `v1.36.4` |
| kubectl | v1.36.4: container `registry.k8s.io/kubectl:v1.36.4@sha256:b8d523e7...` and the Linux amd64 binary (SHA-256 from `dl.k8s.io/release/v1.36.4/bin/linux/amd64/kubectl.sha256`) | `docker pull` by tag printed the digest; `docker run ... version --client` prints `Client Version: v1.36.4`, Kustomize v5.8.1 |
| kubeconform | v0.8.0 (2026-06-04): container `ghcr.io/yannh/kubeconform@sha256:faffaf43...` and the Linux amd64 tarball (SHA-256 from the release's `CHECKSUMS`) | `kubeconform -v` prints `v0.8.0`; `-kubernetes-version 1.36.4` fetched its schemas and validated a test object and the rendered overlay (14 resources valid) |
| shellcheck (local lint only, not a gate) | `mirror.gcr.io/koalaman/shellcheck:stable` | both new scripts are clean |

## Sizes and times measured in PR 2

| Measurement | Result |
| --- | --- |
| `kindest/node:v1.36.4` image (arm64) | 352 MB as Docker reports it (the plan estimated about 1 GB); anonymous pull from `mirror.gcr.io` by digest took 10.9 s on a home connection |
| `registry.k8s.io/kubectl:v1.36.4` image (arm64) | 19.0 MB |
| `ghcr.io/yannh/kubeconform:v0.8.0` image (arm64) | 11.9 MB |
| `k8s-tool.sh kubectl kustomize` of the overlay (container, warm) | 1.5 s |
| rendered overlay | 14 objects, 644 lines; sum of memory limits of all pods 1280 MiB (a test prints it) |

Registry findings: `registry.k8s.io` answers anonymously with a redirect to Google Artifact Registry and serves `kubectl`; `ghcr.io` serves
`kubeconform` after an anonymous token (the CI job downloads the release binaries instead, so neither registry is used in CI); `mirror.gcr.io`
serves `kindest/node` anonymously. Docker Hub is not used for anything.

## A real API server, once: k3s v1.36.4 in a capped container (not kind)

To exercise the manifests and script logic without a kind cluster (the VM has 3.81 GiB and the plan forbids resizing it), `rancher/k3s:v1.36.4-k3s1`
ran as one privileged container with `--memory 2200m`, then `docker rm -fv`. It is **not** kind (flannel and `local-path` instead of kindnet and
`standard`), so it is evidence about the manifests, not about kind.

| Measurement | Result |
| --- | --- |
| k3s alone after start | 432 MiB (container memory) |
| k3s with PostgreSQL, Redis, Redpanda and idle jobs finished | 717-724 MiB total, 2.1 GiB cap not approached (3.9 % CPU at rest) |
| StatefulSets from `apply` to Ready (images already pulled) | under a minute for all three (observed, not timed precisely) |
| `migrate` Job | completed in 2 s, `redpanda-init` in 3 s |
| smoke D1-D7 including three pod restarts | passed 7 of 7 in a few minutes (not timed precisely) |
| PodSecurity | namespace `restricted:v1.36` admitted all pods; a privileged pod was rejected with `violates PodSecurity "restricted:v1.36"` |
| secrets hunt (non-Secret objects, events, logs, descriptions, node container logs, the render) | 0 matches for the generated values |
| the VM's kernel | lacks `br_netfilter` by default; k3s could not reach ClusterIPs until the module was loaded. **How:** `modprobe br_netfilter` from a throwaway *privileged* container with the VM's `/lib/modules` mounted read-only, which loads the module into the shared Colima VM's kernel (the VM the user's other containers run in). It was unloaded the same way after the run (`modprobe -r`); the coordinator has since said not to repeat this, and it was not repeated |

## Cluster measurements from CI (kind job on a GitHub-hosted runner)

Source: the logs of the advisory `k8s kind smoke` job on PR #79 (Phase 17 PR 6), as read by the person who closed the phase. The runner is a
GitHub-hosted Linux runner (amd64); its CPU count was not recorded here. Several runs of the job are cited, and where they differ the difference is
shown rather than averaged. Nothing in this section was measured on the development Mac.

| Measurement | Result |
| --- | --- |
| cluster smoke | `smoke: 21 of 21 checks passed` (K1-K15 and D2-D7, the last line of the contract; K15 is the NetworkPolicy enforcement check) |
| kind smoke job duration | about 9 min 35 s to 9 min 58 s (runs seen), against the 40-minute budget of the job |
| `k8s static` job duration | about 1 to 4 minutes |

## Rolling update and the drain (PR 6)

`make k8s-roll-test`: 24 closed-loop clients, no client retries, through the host entry point of kind, while the three gateway pods are replaced by
alternating rollouts (a ConfigMap hash change, then an annotation change) until at least 3000 requests were made during the rollouts. Full procedure:
`docs/operations/kubernetes-deployment.md`.

### The main run (the real run, with the drain)

| Measurement | Result |
| --- | --- |
| requests in total | 4892 |
| failed requests | 0 |
| requests during the 3 rollouts | 3360 |
| gateway pods replaced | 9 |
| throughput | about 38 requests/s |
| rollout duration | 21-23 s to `kubectl rollout status`; 28 s until the last old pod was gone |
| SIGTERM to the pod object gone (upper bound for "last request served") | about 4-5 s per replaced pod |
| rule-of-three bound | 0 failures in 3360 requests: the true failure rate during rollouts is below about 0.09 % at 95 % confidence (3/3360); the gate of 3000 gives below about 0.1 % |
| verdict | `roll-test: PASS` |

### Earlier runs of the same job, recorded as observed

The first run of the job (before the loop that keeps rolling until 3000 requests, and before the control was redefined) did not pass:

| Run | Requests | During rollouts | Failures | Notes |
| --- | --- | --- | --- | --- |
| with the drain, first run | 3674 | 2123 in 2 rollouts (gate of 3000 unmet) | 0 | about 38 requests/s; 22 s to `rollout status`, 28 s until the last old pod was gone, twice |
| control with resilience off and grace 0, first run | 4874 | 2431 | 1551: `http_503_no_capacity` 1546 (867 in the baseline), `connection_closed` 4, `connection_reset` 1 | about 55 requests/s overall, about 50 during the rollouts; 22-24 s per rollout |

The capacity rejections showed that "resilience off" conflates load with the drain, which is why the control became "readiness grace 0, resilience on".
The first run's SIGTERM to gone was 4-5 s per pod with the drain (about 5 s at most, six pods) and 0-1 s in the resilience-off control.

### The negative control (readiness grace 0)

| Run | Drain-attributable failures | Rollout requests |
| --- | --- | --- |
| one run (the later of the two readiness-grace-0 runs; whether it is the run of the main result above was not recorded) | 9 | 1954 |
| an earlier run (readiness grace 0) | 3 | 1881 |
| the first run's resilience-off control, counting only drain-attributable classes (`connection_closed` 4, `connection_reset` 1) | 5 | 2431 |

SIGTERM to gone was about 0-1 s without the drain.

**How to read the control.** It must see at least one drain-attributable failure inside the rollouts, and it did in every run listed; that is what shows
the test can see the outage. The count varied from 3 to 9 across runs of about the same size, so it is not a stable quantity and must not be quoted as
"the drain prevents N failures": there are only three data points, from different versions of the control, and no variance estimate. The real run and
the control also differ in more than the drain's visible effect (the control rolls until it fails, the real run until 3000 requests), so their request
counts are not comparable.

### What is not available from these runs

- The time from SIGTERM to the last request a given pod accepted: responses carry no pod identity. SIGTERM to gone is an upper bound only.
- Propagation time (how long until no new connection reaches a terminating pod): not obtainable with the current harness. The 5 s grace is shown
  sufficient for this kind topology by zero failures in 3360 requests, not by a measured propagation time.
- A per-pod p99: a run replaces nine pods; aggregating several runs was not done.
- Observation runs (pod delete under load, a stalled rollout, a worker roll, a control-plane restart): not run as recorded observations.
- Any figure for another machine, another runner type, a cloud balancer or more than one node.

## Not measured

The Colima VM here has 3.81 GiB, below the 4 GB the scripts demand, and the decision was not to resize it or create a second profile, so none of the
following exists as a number: cold and warm `make k8s-up` on a developer machine, kind node memory and the disk a loaded cluster uses (the sum of memory
limits, 2848 MiB, and the engine minimum of 6144 MiB are computed by a test from the manifests, not measured), the data tier's real memory use against its
limits, a 60 s load at concurrency 12 with restart counts (acceptance criterion 5), and whether `kind load docker-image` works with Docker 29's image store
locally (the CI job loads the images on the runner; the `docker save` fallback is written and unused). The CI job's time against the budget is above.
