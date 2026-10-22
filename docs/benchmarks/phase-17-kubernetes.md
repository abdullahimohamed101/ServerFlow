# Phase 17 benchmark note: Kubernetes (started in PR 2)

Every number here is from a run, with its machine. PR 2 measures what can be measured without a cluster; the cluster numbers (cold and
warm `k8s-up`, node memory, Colima disk, drain and rolling-update runs) are **not measured yet** and are listed as such. PR 3 and PR 6 fill them.

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
| the VM's kernel | lacks `br_netfilter` by default; k3s could not reach ClusterIPs until the module was loaded (and unloaded again afterwards) |

## Not measured yet (needs a kind cluster)

The Colima VM here has 3.81 GiB, below the 4 GB the scripts demand, and the plan's decision is not to resize it or create a second profile, so none of
the following was run locally on kind: cold and warm `make k8s-up`, kind node memory and the disk a loaded cluster uses, whether `kind load docker-image` works
with Docker 29's image store (the `docker save` fallback is written, untested), the CI job's duration against the 30-minute budget, the data tier's
real memory use against its limits. They are recorded by the first advisory CI run and by the PRs that follow.
