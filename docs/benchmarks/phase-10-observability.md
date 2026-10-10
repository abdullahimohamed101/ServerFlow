# Phase 10: observing a benchmark live, and what the instrumentation costs

Evidence for the Phase 10 acceptance criteria that `go test` cannot give: Prometheus and Grafana running beside a local cluster while
`cmd/benchmark` loads it, an alert firing when a worker is stopped, and the overhead of the instrumentation. Everything here was run on one
Mac (Apple M5 Pro, Go 1.27.1, Colima for Docker) shared with other agents running test suites, so absolute latencies are noisy; the
before/after comparisons are interleaved for that reason. Raw outputs are in `docs/benchmarks/phase-10-assets/`.

## The live run

Setup (all on `127.0.0.1`, ports 58000-58099): one control plane (API `58001`, metrics `58002`), three mock workers at 100, 60 and 20
tokens/s (`58011-58013`, each serving its own `/metrics`) behind three worker agents, one gateway in registry mode with `least-active`
(data `58080`, metrics `58003`), Prometheus `58090` and Grafana `58091` from `observability/docker-compose.yml` (project
`sf-p10-observability`; ports, targets file and the Grafana password came from the untracked `observability/.env`). Prometheus scraped the host
processes through `host.docker.internal`, which reaches loopback-bound listeners on Colima: this settles the plan's open question for this
machine. The targets file is `docs/benchmarks/phase-10-assets/targets.demo.yml`.

```sh
bin/benchmark run --target http://127.0.0.1:58080 --control-plane http://127.0.0.1:58001 --scheduler least-active --workers 3 --model mock-model --duration 120s --concurrency 12 --seed 1
```

### What the first run showed (run A)

The first run used the mock workers' defaults (4 concurrent requests and a queue of 32 each, as the agents registered them) with 8 clients. The
dashboards and queries showed immediately what the harness only reported at the end: a 5xx ratio of about 20% in the Prometheus rule
(`cluster:inference_errors:ratio_rate5m` = 0.200), `inference_failures_total{reason="NO_CAPACITY"}` = 97 at the midpoint, and the benchmark itself
marked the run invalid (`error rate 19.44% (174 of 895 requests failed)`). The cause is the scheduler excluding workers whose active count
(the larger of the worker's last heartbeat and the gateway's own count) has reached the registered concurrency; with heartbeats two seconds apart
a worker looks full for a while after its requests finish. That is existing scheduling behaviour, not changed here, and is worth Phase 14's attention.
Queries at the end of run A: `run-a-undersized-prometheus-queries.txt`.

### The recorded run (run B)

The workers were restarted with `--max-concurrency=8 --queue-size=64` and the command above was run for the full 120 s with 12 clients. The
harness reported: sent 1198, succeeded 1198, failed 0, 9.98 req/s, latency p50 733 ms / p95 3453 ms, TTFT p95 302 ms, valid
(`run-b-benchmark-report.md`). Prometheus, queried about 85 s into the window (`run-b-prometheus-queries.txt`):

| Query | Value |
| --- | --- |
| `up` | 5 of 5 targets (gateway, control plane, three mock workers) |
| `cluster:inference_requests:rate1m` | 6.70 req/s (1-minute rate while the window was still filling) |
| `cluster:inference_request_duration_seconds` p50 / p95 / p99 (5 min) | 0.91 s / 3.83 s / 4.77 s (the 5-minute window still held run A's tail) |
| `cluster:inference_ttft_seconds:p95_rate5m` | 0.399 s (bucket-interpolated; harness: 0.302 s) |
| `cluster:worker_output_tokens:rate1m` | 467 tokens/s from the workers' counters; per worker 248.5 / 167.4 / 51.2 (mock-1 / 2 / 3) |
| `model:worker_health:sum` | 3 eligible workers |
| `cluster:scheduler_decision_duration_seconds:p95_rate5m` | 48 microseconds (SLO: under 10 ms) |
| `cluster:inference_gateway_overhead_seconds:p95_rate5m` | 0.39 ms (SLO: under 25 ms) |
| `scheduler_ineligible_selections_total` | 0 |
| `cluster:inference_errors:ratio_rate5m` | 0.107, because the 5-minute and 1-hour windows still contained run A's failures; `ServerFlowAvailabilityBurnFast` was firing for that reason, which is the alert working |

**Selection distribution against the benchmark report** (acceptance 12: within 5 percentage points). The scheduler's selection counters
(`scheduler_selections_total`), the mock workers' own completed counters (`worker_requests_total{result="ok"}`) and the harness's per-worker table all
describe the same skew toward the faster workers under `least-active`:

| Worker | Selections (Prometheus) | Share | Worker counter | Benchmark report | Share |
| --- | --- | --- | --- | --- | --- |
| mock-1 (100 tok/s) | 670 | 53.51% | 670 | 648 | 53.55% |
| mock-2 (60 tok/s) | 435 | 34.74% | 435 | 419 | 34.63% |
| mock-3 (20 tok/s) | 147 | 11.74% | 147 | 143 | 11.82% |

The largest difference in share is 0.12 percentage points (`selection-distribution.txt`; the Prometheus totals include a few requests after
the harness stopped counting).

**The dashboards, through Grafana.** Grafana provisioned the four dashboards (`sf-cluster-overview`, `sf-model`, `sf-scheduler`, `sf-worker`) from
the repository and the datasource test returned "Successfully queried the Prometheus API". Every panel query was then replayed through Grafana's
datasource API (`/api/ds/query`, datasource uid `serverflow-prometheus`, variables set to "All", last 20 minutes): 33 panels, all returned
data except the two GPU utilisation panels, which is expected because mock workers report no GPU numbers (their descriptions say so)
(`grafana-panel-queries.txt`). Grafana's log had no provisioning errors; its only error lines were one bundled plugin registering twice
(`xychart`, an image defect) and, before the empty `alerting` and `plugins` provisioning directories were added, two "directory does not exist"
lines. **No screenshots were taken**: the panel queries above are the evidence, so what the panels look like on screen was not observed.

### Alert: stop a worker

With 8 clients loading the cluster (run C, 75 s, `--allow-errors`), the backend process of `mock-3` was killed with SIGKILL and Prometheus' alert
state polled every 0.5 s (`alert-worker-backend-killed.txt`):

| Time after the kill | Observed |
| --- | --- |
| 1.5 s | the worker's agent reported FAILED: `worker_health{worker_id="mock-3",state="FAILED"}` = 0 (next scrape) |
| 5.1 s | `ServerFlowWorkerNotServing{worker_id="mock-3"}` **firing**; `ServerFlowTargetDown` pending (its mock-worker target no longer answers) |

5.1 s is inside the 10 s failed-worker target plus one evaluation interval (5 s). The control plane is scraped every 5 s and the fleet rule group is
evaluated every 5 s for this reason. The load kept being served: of 605 requests, 604 succeeded; the gateway retried 5 attempts on other workers
(`inference_retries_total{reason="connect"}` 4, `reason="reset"` 1) and one request failed.

Then the worker's agent was killed too (`alert-worker-agent-killed.txt`): the heartbeat age passed 10 s, the worker went UNHEALTHY at 12.7 s and
`ServerFlowWorkerHeartbeatStale` was **firing at 16.3 s after the kill**, then LOST at 32.6 s. The alert needs the heartbeat age to pass 10 s, then one 5 s
scrape and one 5 s evaluation, so this path takes up to 20 s and misses "10 s plus one evaluation interval" (15 s) by about a second; the realistic
failure (the backend dies, the agent reports it) meets it at 5.1 s.

Alerts with a `for:` hold (the burn-rate pair) stayed pending or fired according to their rules while run A's failures sat in their windows;
they are not part of this demonstration beyond that.

## Cost of the instrumentation

Budget (plan D16): the metrics observer adds at most 20 microseconds per request and no allocation in the steady state; `TestGatewayOverhead` and
the Phase 8 limiter benchmark move by less than their spread; collecting 1,000 workers takes under 50 ms.

All benchmarks: `go test ... -benchmem -cpu 1`, ten runs each, **base and new binaries alternated run by run** on the same loaded machine (load
average 4 to 11 from other agents), `docs/benchmarks/phase-10-assets/overhead-benchmarks-ab.txt`. Base is the merge commit this branch started from,
with only the new benchmark function added so both measure the same thing.

| Benchmark | Base ns/op (mean +- sd) | New ns/op (mean +- sd) | Allocs/op base to new |
| --- | --- | --- | --- |
| `BenchmarkMetricsObserverEvents/one_worker` (one request's events through the metrics observer) | 189 +- 3 | 132 +- 1 | 1 to 0 |
| `BenchmarkMetricsObserverEvents/sixteen_workers` | 188 +- 3 | 136 +- 2 | 1 to 0 |
| `BenchmarkObserverRequest/static_nonstream` (a whole in-process request) | 10848 +- 531 | 10887 +- 579 | 101 to 100 |
| `BenchmarkObserverRequest/static_stream` | 11785 +- 642 | 11905 +- 619 | 108 to 107 |
| `BenchmarkObserverRequest/static_nonstream_two_nop_observers` | 11070 +- 589 | 11148 +- 414 | 101 to 100 |

The observer's own cost fell, from 189 to 132 ns and from one allocation to none, because the statuses and per-model instruments are cached (the
old code formatted the status with `strconv.Itoa` per request). It is far under the 20 microsecond budget. A whole request is unchanged within the
spread (+0.4% non-streaming, +1.0% streaming, against a standard deviation of about 5%), with one allocation fewer. On the quiet machine before
any change the same benchmarks gave 7063 +- 59 and 7539 +- 39 ns/op; the machine was not quiet later, which is why the table above is interleaved.
`BenchmarkMetricsObserverEvents` drives a whole registry-mode request with a first token, so the new scheduler, overhead and selection series are
inside the 132 ns.

`TestGatewayOverhead` (budget 25 ms, `gateway-overhead-test-ab.txt`, three alternating repetitions): p95 gateway overhead, non-streaming, base 1.52 / 0.79 /
0.68 ms and new 0.61 / 0.68 / 0.64 ms; streaming time to first chunk, base 0.81 / 0.59 / 0.58 ms and new 0.60 / 0.59 / 0.64 ms. Before any change on the
quiet machine: 0.52 / 0.47 / 0.57 ms and 0.49 / 0.46 / 0.47 ms. All are orders of magnitude under the budget and inside the machine's noise.

Phase 8 rate-limit benchmark (`SERVERFLOW_BENCH_RATELIMIT=1 go test -run TestBenchmarkRateLimits ./tests/integration`, throwaway Redis, two alternating
runs each, `phase8-limiter-benchmark-ab.txt`), p95 in microseconds:

| Scenario | Base (run 1 / 2) | New (run 1 / 2) |
| --- | --- | --- |
| no tenant, nothing to enforce | 1137 / 1190 | 1149 / 1052 |
| tenant with no quotas | 768 / 905 | 866 / 799 |
| tenant with 3 quotas (one Redis round trip) | 2039 / 1641 | 1555 / 1586 |
| added by one Redis round trip (p95) | 1271 / 736 | 689 / 788 |

Collection with 1,000 registered workers (`BenchmarkCollectWorkers1000`, a `Gather` of the control plane's collector, ten runs): 5.2 to 11.8 ms, median
about 6.4 ms, 101,000 allocations and 4.7 MB per scrape. The 50 ms budget holds with a wide margin. Encoding the response is not included; it is
linear in the series count (about 7,000 series for 1,000 workers: seven per-worker gauges each).

## Not verified here

- How the panels look in a browser; no screenshots exist.
- A GPU worker: the GPU panels and `gpu_*` series were exercised with a worker that reports GPU numbers only in tests (`internal/observability`).
- The CI `observability` job itself (it installs the pinned promtool release); the same checks ran locally through the container
  (`quay.io/prometheus/prometheus:v2.53.0` has the same digest as the Docker Hub image used here).
