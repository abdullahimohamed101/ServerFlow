# Phase 16: the container stack, measured

One machine: Apple Silicon Mac, Colima (Docker engine, linux/arm64, 2 CPUs, 4 GB), Compose 5.6.0, **classic builder (no buildx)**, 2026-10-11. Other agents were
building and running containers on the same engine, so times are indicative. Nothing here describes another CPU, amd64, Docker Desktop or a GitHub runner.

## Start-up and smoke (full profile set, GATEWAYS=1)

| Step | Time |
| --- | --- |
| `scripts/dev-pull.sh`, nothing present (7 third-party images and 2 base images from `public.ecr.aws`, `mirror.gcr.io`, `quay.io`, `gcr.io`) | 19 s |
| `make dev-up`, images pulled but ServerFlow layers pruned (build included, Go modules downloaded) | 109 s |
| first sequential build of six images, earlier run | 63 s (a parallel build of five targets was OOM-killed on this 4 GB VM, so builds are serial: `COMPOSE_PARALLEL_LIMIT=1`) |
| `make dev-up`, images built, stack down | 15-25 s |
| `make dev-up` after a new commit (version stamp changes, binaries rebuilt) | 55-70 s |
| `make dev-smoke` (13 checks) | 19 s |
| `GATEWAYS=3 make dev-up` and smoke 13 of 13 | 22 s and about 25 s |

A true cold build with `--no-cache` for six targets filled the VM disk (each target recompiles the 2.4 GB build stage): do not do that; `make dev-reset`
removes the labelled dangling build stages the classic builder leaves behind. Reproducibility: two builds (one with `--no-cache`) of the same commit gave
identical SHA-256 for all seven binaries. The CI runner time is not measured (no push).

## Image sizes (arm64)

| Image | On disk (`docker images`) | Compressed | Binary |
| --- | --- | --- | --- |
| gateway | 54 MB | 12 MB | 29.8 MB |
| usage-consumer | 36.2 MB | 8.9 MB | 16.1 MB |
| mock-worker | 34.5 MB | 8.4 MB | 14.5 MB |
| admin | 29.9 MB | 7.0 MB | 11.5 MB |
| control-plane | 26.6 MB | 6.4 MB | 8.8 MB |
| worker-agent | 26 MB | 6.2 MB | 8.3 MB |

(each includes the 5.4 MB `healthcheck` binary and the distroless base).

## Memory

Resident memory per container right after `make dev-smoke` (full profile set), total about 460 MiB (limits sum to about 2.4 GB):
redpanda 145, grafana 125, postgres 80, prometheus 33, jaeger 27, gateway 9, control plane 7, three mock workers 3-6 each, redis 6, usage consumer 5, three agents 2.5 each.

A 60 s `cmd/benchmark` run at concurrency 12 (`--workload mixed`, profiles core, observability, tracing, auth off because the harness sends no API key): 480 sent, 480
succeeded, 0 failed, 8.0 req/s, latency p50 1202 ms / p95 3453 ms, TTFT p95 302 ms. No container restarted or was OOM-killed; peaks: postgres 71, grafana 51, prometheus 23,
jaeger 21, gateway 9.6, control plane 7.5, mock workers 7.7 MiB.

## Behaviour measured with the stack up

- Always-failing worker: `MOCK_3_FAILURE_RATE=1`, mode `unavailable`: 30 of 30 requests 200, 19 retries counted; mode `error` (500) is not retried by design (retry statuses 502, 503): 10 of 30 failed.
- Kill the agent: not eligible after 5 s (`suspect`), `UNHEALTHY` at about 11 s, `LOST` at about 31 s. Pause the worker: `FAILED` after about 3 s. Kill the worker: leaves the pool, requests stay 200.
- Redpanda stopped: requests still 200, S9 fails honestly; after `make dev-up` re-runs `redpanda-init` the pipeline recovers (tmpfs data does not survive a stop).
- Grafana log: no provisioning errors (one warning that `.gitkeep` is not a provisioning file). Panel queries against the running Prometheus: 29 panels return data, 4 do not
  (failure and retry paths with nothing to show), 0 query errors.
- Port collision on each of the five ports: `dev-up` aborts before starting anything, names the variable; moving it works.

## Secrets hunt (generated values searched, never printed)

Zero hits in: `docker history --no-trunc` and `docker save` of the six images, `make dev-config` output, the argument list (`docker inspect`) and `docker top` of every container, every
container log (smoke S12) and the repository working tree and history. A deliberately baked `ENV` secret in a test image is found by the same layer scan.
