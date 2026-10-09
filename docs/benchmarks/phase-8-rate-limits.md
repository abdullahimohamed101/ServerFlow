# Phase 8 measurements: shared quotas, overhead and a Redis outage

Reproduce: `make dev-redis`, then (keep the Mac awake, see "Method")

```bash
SERVERFLOW_TEST_REDIS_ADDR=127.0.0.1:56379 SERVERFLOW_TEST_REDIS_PASSWORD=$(scripts/dev-redis.sh password) \
SERVERFLOW_BENCH_RATELIMIT=1 caffeinate -dimsu go test -count=1 -v -run TestBenchmarkRateLimits ./tests/integration
```

## Method

- **Load.** The Phase 7 harness's own `multi-tenant` plan (`internal/bench/workload`, seed 1): one aggressive tenant sends ten times what each of
  four normal tenants send (about 71% of requests), offered open-loop at 300 requests per second for 20 s to **three gateways in one process**,
  round-robin, each with its own Redis client and limiter, sharing one Redis 7 (Docker, password-protected). Requests carry real API keys
  (an in-memory key store behind the real authenticator). The harness binary itself cannot offer this load: it sends fake `sk-bench-*` keys,
  which the authenticator rightly refuses, and the harness is not modified in this phase. The upstream answers immediately; this measures the
  limiter, not inference.
- **Machine.** One Apple Silicon Mac, Redis in Docker Desktop (a Linux VM), everything on loopback. Numbers show relative cost and
  correctness, not production latency.
- **Clock.** The limiter trusts Redis's clock. On Docker Desktop that clock fell 14-45 s behind the host while the Mac slept and then
  jumped forward; the limiter credits a forward step as refill, so earlier, unattended runs admitted **1052, 1609, 1899 and 1922** requests
  for the aggressive tenant against a bound of 800. We found the cause by sampling `redis-cli TIME` against the host clock (steps of
  14-45 s). The run below was made with `caffeinate -dimsu` and a sampler that saw the offset stay under 0.2 s throughout. See ADR-015.

## Shared quota (3 gateways, 1 Redis, 20 s)

Aggressive tenant quota 600 requests/min (a bucket of 600 that refills 10 per second); four normal tenants 6000/min each.

| Tenant | Offered | Accepted | Rejected (429) |
| --- | ---: | ---: | ---: |
| aggressive (quota 600/min) | 4303 | **799** | 3504 |
| normal 1 (quota 6000/min) | 443 | 443 | 0 |
| normal 2 | 421 | 421 | 0 |
| normal 3 | 411 | 411 | 0 |
| normal 4 | 423 | 423 | 0 |

The most the aggressive tenant may be given in 20 s is the full bucket plus what refilled: 600 + 10 x 20 = 800. It was given **799 (99.9%)**,
however the requests were spread over the three gateways. Normal tenants saw no rejections. The correctness tests assert the same
at the boundary: quota N admits exactly N across gateways, N+1 is refused, concurrently, with a frozen clock.

## Overhead of one Redis round trip (1 gateway, 4000 requests, concurrency 16, instant upstream)

| Case | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| no tenant, nothing to enforce (no Redis call) | 366 us | 1.06 ms | 1.28 ms |
| tenant with no quotas (no Redis call) | 328 us | 780 us | 1.25 ms |
| tenant with three quotas (one script call) | 891 us | 2.10 ms | 3.03 ms |
| **added by the round trip** | **564 us** | **1.32 ms** | |

Re-measured after the final cost-estimator change (an earlier run measured 493 us / 572 us added; the spread between runs on a laptop is that large). Added p95 is well below the 5 ms budget (acceptance criterion 11); a tenant with all quotas at 0 costs no Redis call (tests count Redis
commands: 0).

## Redis outage (3 gateways, 200 req/s for 15 s, Redis black-holed from 5 s to 10 s, `redis.timeout` 50 ms, `redis.backoff` 1 s)

| Mode | Succeeded | 503 RATE_LIMIT_UNAVAILABLE | Other |
| --- | ---: | ---: | ---: |
| closed | 1930 | 1071 | 0 |
| open | 3001 | 0 | 0 |

Closed refused about the 5 s of traffic during the outage (200 x 5 = 1000, plus the first few requests that waited out the timeout), and
service resumed without a restart; open admitted everything and counted the bypasses. Per-request behaviour is covered by the failure-matrix
tests (cut, black-holed, slow and erroring Redis, both modes): the first request in an outage fails within the timeout, the following ones
within the backoff are answered in microseconds without touching the network, one probe goes through when the backoff ends, and the outage and
recovery are each logged once.
