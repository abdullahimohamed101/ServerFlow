# ServerFlow — Distributed LLM Inference Platform

**Principal-Engineer Build Specification**

**Project name:** ServerFlow (formerly InferGrid)
**Primary language:** Go
**Secondary language:** Python
**Primary inference runtime:** vLLM
**Infrastructure:** Docker, Kubernetes, Redis, Kafka, PostgreSQL, Prometheus, Grafana, OpenTelemetry
**Primary engineering themes:** distributed systems, inference serving, scheduling, concurrency, fault tolerance, observability, performance engineering, resource management

---

## 1. Executive Summary

Build a production-style distributed inference platform capable of serving open-source large language models through an OpenAI-compatible API.

The platform must accept inference requests, authenticate clients, enforce quotas, determine which model is required, identify eligible inference workers, intelligently schedule requests, stream generated tokens back to the client, detect failures, reroute work when appropriate, collect detailed telemetry, persist operational data, and dynamically adapt to changing workloads.

The system should eventually support:

- Multiple API gateway replicas
- Multiple LLM models
- Multiple inference workers
- GPU-backed inference
- Streaming token generation
- Worker registration and health checking
- Model-aware routing
- Load-aware routing
- Workload-aware scheduling
- Distributed rate limiting
- Admission control
- Request prioritization
- Redis-based ephemeral shared state
- Kafka-based inference event streaming
- PostgreSQL-backed durable metadata
- Prometheus metrics
- Grafana dashboards
- OpenTelemetry tracing
- Kubernetes deployment
- GPU-aware worker scheduling
- Autoscaling
- Dynamic model placement
- Load testing
- Fault injection
- Scheduler benchmarking

The project must not become a collection of infrastructure technologies with no purpose.

Every major component must correspond to a documented system requirement.

---

## 2. Project Thesis

The fundamental problem is:

> Efficiently serve heterogeneous LLM inference requests across limited and failure-prone compute resources while minimizing latency and maximizing throughput, fairness, reliability, and hardware utilization.

The project is NOT:

- an LLM chatbot
- an AI assistant
- an LLM training project
- an LLM fine-tuning project
- a custom transformer implementation
- a CUDA kernel project
- a wrapper around an external commercial LLM API
- a Kubernetes demonstration
- a Kafka demonstration

The actual product is the **inference control plane and routing layer**.

```text
                       CLIENTS
                          |
                          v
                +------------------+
                |  API GATEWAY     |
                |                  |
                | Auth             |
                | Rate limiting    |
                | Validation       |
                | Streaming        |
                +--------+---------+
                         |
                         v
                +------------------+
                | INFERENCE ROUTER |
                | / SCHEDULER      |
                +--------+---------+
                         |
               +---------+-----------+
               |         |           |
               v         v           v
           WORKER A   WORKER B   WORKER C
               |         |           |
               v         v           v
             vLLM      vLLM        vLLM
               |         |           |
               v         v           v
             GPU A      GPU B       GPU C
```

The central engineering contribution is everything between the client and inference engine.

---

## 3. Engineering Goals

The platform should optimize five dimensions.

### 3.1 Latency

Minimize:

- request queue time
- time to first token (TTFT)
- total request latency
- p95 latency
- p99 latency

### 3.2 Throughput

Maximize:

- requests completed per second
- input tokens processed per second
- output tokens generated per second
- concurrent requests handled

### 3.3 Utilization

Maximize useful utilization of:

- GPUs
- inference workers
- model replicas

Avoid:

- idle GPUs while other workers are overloaded
- poor model placement
- excessive queue imbalance

### 3.4 Reliability

The system should tolerate:

- worker crashes
- inference runtime crashes
- gateway crashes
- slow workers
- network errors
- worker restarts
- Kafka outages
- Redis degradation
- partially unavailable clusters

### 3.5 Fairness

One client should not monopolize cluster resources.

Eventually support:

- per-user quotas
- per-tenant quotas
- model-specific quotas
- request priorities
- maximum concurrent requests
- token-budget enforcement

---

## 4. Core Design Principle

Use a strict separation between:

```text
CONTROL PLANE
```

and:

```text
DATA PLANE
```

### Data Plane

Handles actual inference traffic.

```text
Client
  |
Gateway
  |
Router
  |
Inference Worker
  |
vLLM
```

Performance sensitive. Must stay lean.

### Control Plane

Determines how the system should operate.

Responsible for:

- worker registration
- worker state
- model inventory
- desired replicas
- scheduling configuration
- scaling
- model placement
- configuration
- policy

Eventually:

```text
Control Plane
      |
      +-- Worker Registry
      +-- Model Registry
      +-- Placement Controller
      +-- Autoscaler
      +-- Policy Engine
```

Do not mix these responsibilities unnecessarily.

---

## 5. System Architecture

Target long-term architecture:

```text
                             CLIENTS
                                |
                                v
                      +------------------+
                      |   LOAD BALANCER  |
                      +--------+---------+
                               |
                 +-------------+--------------+
                 v             v              v
            GATEWAY 1     GATEWAY 2     GATEWAY N
                 |             |               |
                 +-------------+---------------+
                               |
                               v
                    +---------------------+
                    | INFERENCE SCHEDULER |
                    +----------+----------+
                               |
                  +------------+------------+
                  v            v            v
               WORKER A     WORKER B     WORKER C
               Qwen         Qwen         Llama
                  |            |            |
                vLLM         vLLM         vLLM
                  |            |            |
                GPU 1        GPU 2        GPU 3


SHARED SERVICES

Redis
- rate limits
- request metadata
- ephemeral worker state
- cache
- distributed coordination where appropriate

Kafka
- inference.request.received
- inference.request.routed
- inference.first_token
- inference.completed
- inference.failed
- worker.registered
- worker.failed
- scaling events

PostgreSQL
- API keys
- users / tenants
- persistent model configuration
- worker configuration
- usage summaries
- experiment metadata

Prometheus
- operational metrics

Grafana
- dashboards

OpenTelemetry
- distributed tracing

Kubernetes
- gateways
- control plane
- workers
- GPU node scheduling
- services
- worker lifecycle
```

---

## 6. Request Lifecycle

An end-to-end walkthrough of a request.

### Example request

```http
POST /v1/chat/completions
```

```json
{
  "model": "qwen-7b",
  "messages": [
    { "role": "user", "content": "Explain TCP congestion control." }
  ],
  "stream": true,
  "max_tokens": 500
}
```

### Lifecycle steps

1. Client submits request
2. Gateway assigns request_id
3. Gateway authenticates API key
4. Gateway checks distributed rate limit
5. Gateway validates requested model
6. Gateway estimates request cost
7. Admission control checks cluster capacity
8. Scheduler requests eligible worker list
9. Worker registry filters:
10. healthy workers, matching model, available capacity
11. Scheduling algorithm ranks workers
12. Scheduler chooses worker
13. Gateway/router forwards request
14. Worker sends request to local vLLM
15. vLLM starts generation
16. First token produced
17. Token streamed: worker -> gateway -> client
18. Remaining tokens streamed
19. Request completes
20. Metrics recorded
21. Kafka completion event emitted
22. Usage statistics asynchronously processed
23. Request tracing completes

---

## 7. Critical IDs

Generate and propagate identifiers across every component.

Required IDs:

```text
request_id
trace_id
tenant_id
api_key_id
worker_id
model_id
attempt_id
```

A request can have multiple attempts:

```text
Request req_123

Attempt 1
worker-02
FAILED

Attempt 2
worker-04
SUCCESS
```

Do not overwrite attempt history.

---

## 8. API Gateway

- Go
- HTTP server
- OpenAI-compatible endpoints
- authentication
- validation
- request ID creation
- tenant context
- rate limiting
- admission control
- streaming proxy
- error normalization
- tracing
- metrics
- scheduler invocation

Initial endpoints:

```text
GET  /healthz
GET  /readyz
GET  /metrics

GET  /v1/models

POST /v1/chat/completions
POST /v1/completions
```

Do not immediately implement every OpenAI endpoint.

---

## 9. API Compatibility

The public API should intentionally resemble the OpenAI API:

- existing SDK compatibility
- easy testing
- familiar interface
- interchangeable with another compatible backend

Internally, do NOT couple the system to OpenAI-specific structures.

Use translation:

```text
External OpenAI Request  ->  Normalized Internal Request  ->  Worker Request
```

Internal representation:

```go
type InferenceRequest struct {
    RequestID   string
    TenantID    string
    Model       string
    Messages    []Message
    Prompt      string
    Stream      bool
    MaxTokens   int
    Temperature float64
    Priority    int
}
```

---

## 10. Worker Architecture

```text
+----------------------------+
| Worker Agent               |
|                            |
| registration               |
| heartbeat                  |
| metrics                    |
| request proxy              |
| capacity reporting         |
+--------------+-------------+
               |
               v
            vLLM server
               |
               v
              GPU
```

Worker agent responsibilities:

- register worker
- announce model
- expose health state
- expose queue state
- expose resource utilization
- forward inference request
- emit request metrics
- report readiness

Worker metadata:

```json
{
  "worker_id": "worker-qwen-01",
  "model": "qwen-7b",
  "status": "healthy",
  "active_requests": 4,
  "queue_depth": 9,
  "queued_input_tokens": 12500,
  "recent_tokens_per_second": 621,
  "gpu_utilization": 88,
  "gpu_memory_used_mb": 18421,
  "last_heartbeat": "..."
}
```

---

## 11. Worker State Machine

```text
REGISTERING
    |
    v
LOADING_MODEL
    |
    v
WARMING
    |
    v
READY
    |
    v
DRAINING
    |
    v
TERMINATED
```

Failure states:

```text
UNHEALTHY
LOST
FAILED
```

Never route traffic to REGISTERING, LOADING_MODEL, WARMING, DRAINING, UNHEALTHY, LOST, or FAILED workers. Only READY workers accept new work.

---

## 12. Heartbeats

- every 2 seconds
- worker_id
- timestamp
- model
- active_requests
- queue_depth
- token throughput
- GPU utilization
- GPU memory
- health

Registry rules:

```text
heartbeat age < 5 sec   -> healthy
heartbeat age 5-10 sec  -> suspect
heartbeat age > 10 sec  -> unhealthy
```

Make thresholds configurable.

---

## 13. Scheduler Interface

```go
type Scheduler interface {
    SelectWorker(
        ctx context.Context,
        req *InferenceRequest,
        workers []WorkerSnapshot,
    ) (*WorkerSnapshot, error)
}
```

Every scheduling strategy implements this interface. This allows comparative benchmarking.

---

## 14. Scheduling Algorithms

Implement progressively.

1. **Random** — baseline.
2. **Round robin** — A -> B -> C -> A -> B -> C. Deterministic baseline.
3. **Least Active Requests** — pick minimum active_requests.
4. **Least Queue Depth** — pick minimum queue_depth.
5. **Least Queued Tokens** — min sum(estimated remaining token work).
6. **Weighted Least Work** — estimated_work = queued_input_tokens + expected_output_tokens; predicted_wait = estimated_work / recent_worker_token_throughput; choose lowest.
7. **Latency-Aware** — score = alpha*predicted_queue_time + beta*recent_ttft + gamma*gpu_pressure + delta*active_requests, weights configurable.

Do NOT claim a scheduler is superior until benchmarking proves it.

---

## 15. Scheduling Invariants

The scheduler must never:

- choose an unhealthy worker
- choose a worker without the requested model
- choose a draining worker
- choose a worker at hard concurrency capacity
- silently discard a request
- block indefinitely

Provide a reason when no worker is eligible:

```json
{
  "error": "capacity_exhausted",
  "model": "qwen-7b",
  "eligible_workers": 0
}
```

---

## 16. Admission Control

Routing and admission control are separate concepts.

- Routing: which worker runs this request?
- Admission: should we accept this request at all?

Admission conditions:

- global outstanding requests > threshold
- model queue > threshold
- estimated queue wait > maximum
- tenant concurrency > quota

Return 429 Too Many Requests or 503 Service Unavailable, with Retry-After when meaningful.

---

## 17. Request Cost Estimation

Start simple:

```text
input_tokens + max_tokens
```

Possible later model:

```text
estimated_cost = input_tokens * input_weight + predicted_output_tokens * output_weight
```

Potential features: model size, history, prompt type, tenant, temperature, tool usage. Do not build ML prediction initially.

---

## 18. Redis Responsibilities

Shared ephemeral infrastructure:

- Distributed rate limiting: `rate:tenant:{tenant_id}`, `rate:model:{model}`. Token-bucket or sliding-window, atomic operations.
- Distributed concurrency counters: `tenant:{id}:active_requests`
- Request routing hints: `request:{id}:worker`
- Deterministic request cache (SHA256 of model, prompt, parameters). Enable only when appropriate; never cache arbitrary nondeterministic output.

---

## 19. PostgreSQL Responsibilities

Durable metadata: tenants, api_keys, models, worker_configs, usage_records, scheduler_experiments, benchmark_runs, deployment_configs.

NOT: live heartbeats, routing hot path, token stream, or a Kafka replacement.

---

## 20. Kafka Architecture

Kafka stays OFF the synchronous inference path. Publish lifecycle events asynchronously:

Topics:

- inference.lifecycle.v1
- worker.lifecycle.v1
- cluster.scaling.v1

Event example:

```json
{
  "event_type": "inference.request.completed",
  "schema_version": 1,
  "request_id": "req_...",
  "attempt_id": "att_...",
  "worker_id": "worker-qwen-03",
  "model": "qwen-7b",
  "tenant_id": "tenant_123",
  "input_tokens": 412,
  "output_tokens": 638,
  "queue_ms": 124,
  "ttft_ms": 441,
  "generation_ms": 3192,
  "timestamp": "..."
}
```

---

## 21. Kafka Consumers

- Usage consumer: aggregate requests/tokens/models/tenants; persist summaries
- Analytics consumer: prompt length, output length, arrival patterns, model popularity
- Autoscaling consumer: workload signals (autoscaling not run via Kafka initially)

---

## 22. Event Delivery Semantics

At-least-once delivery. Idempotent consumers. Deduplicate on event_id or then request_id + event_type + attempt_id. Never attempt exactly-once across the entire system.

---

## 23. Streaming Responses

Use Server-Sent Events compatible with OpenAI. Stream tokens worker -> gateway -> client. Do NOT buffer the entire completion. Track accepted, worker-selected, worker-started, first-token, and completion timestamps to derive queue, TTFT, generation, and total latency.

---

## 24. Client Timeouts

When a client disconnects, propagate context cancellation to worker and vLLM. Do not waste GPU work on abandoned requests. Cancellation behavior must be tested.

---

## 25. Retry Policy

- Maximum worker attempts: 2
- Retry only on connection-before-generation failure, worker unavailable, or explicitly retryable transport failure.
- Failure before first token: retry allowed.
- Failure after first token: TERMINATE the stream with an error (never transparently restart once output has begun).
- Document this policy explicitly.

---

## 26. Circuit Breakers

Track worker failure behavior. States: CLOSED, OPEN, HALF_OPEN.

Example policy: 5 failures within 10s -> OPEN; wait 20s -> HALF_OPEN; probe success -> CLOSED. All configurable.

---

## 27. Backpressure

- max_global_queue
- max_model_queue
- max_worker_queue
- max_tenant_concurrency

Reject work once limits are reached. NEVER unbounded memory queues.

---

## 28. Observability

- Prometheus metrics
- OpenTelemetry traces: gateway.receive, rate_limit, scheduler.select, worker.forward, inference, first_token, completion
- Structured JSON logs, always including request_id, trace_id, tenant_id, worker_id, model, attempt_id
- Never parse plain text for operational analysis

---

## 29. Prometheus Metrics

Gateway: inference_requests_total, inference_requests_active, inference_request_duration_seconds, inference_ttft_seconds, inference_queue_duration_seconds, inference_failures_total, rate_limit_rejections_total, admission_rejections_total

Scheduler: scheduler_decisions_total, scheduler_decision_duration_seconds, scheduler_worker_score, scheduler_no_capacity_total

Worker: worker_active_requests, worker_queue_depth, worker_queued_tokens, worker_tokens_generated_total, worker_input_tokens_total, worker_output_tokens_total, worker_request_duration_seconds, worker_ttft_seconds, worker_health

GPU: gpu_utilization_percent, gpu_memory_used_bytes, gpu_memory_total_bytes

Kafka: event_publish_failures_total, event_publish_latency_seconds, consumer_lag

Redis: rate_limit_check_duration_seconds, redis_errors_total, cache_hits_total, cache_misses_total

---

## 30. Grafana Dashboards

Four dashboards minimum:

- Cluster Overview: requests/sec, tokens/sec, p50/p95/p99 latency, p95 TTFT, error rate, healthy workers, GPU utilization
- Worker Dashboard: queue, active requests, GPU utilization, tokens/sec, latency, TTFT, error rate
- Scheduler Dashboard: routed requests, queue imbalance, decisions, selection distribution, no-capacity events
- Model Dashboard: requests/sec, tokens/sec, active replicas, queue depth, latency, TTFT

---

## 31. Service-Level Objectives

Initial targets (evolve with data):

- Gateway availability: 99.9% during benchmark window
- Routing decision p95: < 10 ms
- Gateway overhead (excluding inference): < 25 ms p95
- Failed-worker detection: < 10 sec
- Zero routing to known-unhealthy workers
- No unbounded queues

Performance targets depend on hardware; compare against baselines rather than hard-coded absolutes.

---

## 32. Benchmark Methodology

Persist: git commit, model, worker count, GPU type, scheduler, concurrency, prompt distribution, max_tokens, duration, random seed, date.

---

## 33. Benchmark Workloads

- Uniform Short: 100-300 in, 50-150 out
- Uniform Long: 2k-8k in, 500-1500 out
- Mixed: 60% short / 30% medium / 10% long
- Burst: normal -> sudden 10x -> normal
- Hot Model: 90% Qwen, 10% Llama
- Multi-tenant: one aggressive + several normal clients

---

## 34. Benchmark Output

Report requests sent/succeeded/failed; throughput (req/s, input tokens/s, output tokens/s); latency p50/p95/p99; TTFT p50/p95/p99; queue average/p95; GPU utilization; queue imbalance (Jain index); error rate. Machine-readable JSON plus human-readable report.

---

## 35. Scheduler Experiment Framework

Command:

```bash
tool bench
--scheduler round-robin --workers 4 --concurrency 100 --duration 300s --workload mixed
```

Compare:

```bash
tool bench compare run_001 run_002
```

Output a comparison table with deltas (throughput %, p95 TTFT %, p95 latency %, GPU imbalance %).

---

## 36. Fault Injection

```bash
make chaos-worker-kill
make chaos-worker-delay
make chaos-network-loss
```

Experiments: worker crash, slow worker (5s delay), flapping health, Redis failure (fail closed for rate limiting, fail open for cache by default), Kafka outage (inference continues; events buffer briefly, retry asynchronously, bounded buffer, drop with metric).

---

## 37. Kubernetes Architecture

No Kubernetes until local multi-worker works. Namespace: serverflow. Deployments: gateway, control-plane, analytics-consumer, usage-consumer; stateful: redis, kafka, postgres; GPU workloads: inference-worker-qwen, inference-worker-llama; monitoring: prometheus, grafana, otel-collector.

---

## 38. GPU Worker Resources

```yaml
resources:
  limits:
    nvidia.com/gpu: 1
```

Support node labels, affinity, taints/tolerations later.

---

## 39. Worker Readiness

Pod RUNNING is not READY. `/readyz` on worker returns success ONLY when initialized + vLLM responsive + model loaded + warmup complete.

---

## 40. Graceful Worker Drain

On SIGTERM: worker -> DRAINING; stop accepting new work; allow in-flight to finish; terminate. Test rolling deployments under load.

---

## 41. Autoscaling

Signals: queue depth, queued tokens, TTFT, active requests, GPU utilization, arrival rate. Simple policy example:

```
IF queued_tokens > X FOR 30s THEN replicas++
IF queue ~0 AND util < 20% for 5 min THEN replicas--
```

Use cooldown windows to avoid oscillation.

---

## 42. Model-Aware Autoscaling

Per-model pool:

```
ModelPool { model, desired, ready, pending, demand }
```

---

## 43. Dynamic Model Placement (advanced)

Two separate scheduling problems:

- placement scheduler: which model runs on which GPU
- request scheduler: which request goes to which replica

---

## 44. Security

- API key authentication
- hashed API key storage
- TLS externally
- no public vLLM workers
- no public Redis/Kafka
- secrets via env/K8s secrets
- request/prompt size limits
- max generation tokens
- vLLM behind the gateway; never trust runtime auth as perimeter

---

## 45. Tenant Model

tenant_id, name, status, default_quota; API key belongs to tenant. Policies: requests/min, tokens/min, concurrent requests, allowed models, priority tiers.

---

## 46. Rate Limiting

Token bucket; start requests/minute, add tokens/minute. Why: 100 tiny requests != 100 enormous ones.

---

## 47. Priority Scheduling

P0 interactive, P1 normal, P2 batch. Avoid starvation (aging, weighted fair queueing). Do not implement until scheduler is stable.

---

## 48. Repository Layout

```text
ServerFlow
|-- README.md
|-- AGENTS.md
|-- ARCHITECTURE.md
|-- Makefile
|-- go.work
|-- docker-compose.yml
|-- cmd/gateway, worker-agent, control-plane, usage-consumer, benchmark
|-- internal/api, auth, admission, config, gateway, scheduler, registry, worker, models, ratelimit, cache, events, telemetry, postgres, redis
|-- pkg/protocol
|-- worker/runtime, vllm
|-- benchmark/workloads, reports, analysis
|-- schemas/events
|-- migrations
|-- deploy/docker, kubernetes(base/overlays), helm
|-- observability/prometheus, grafana, otel
|-- scripts
|-- tests/integration, e2e, chaos, load
|-- docs/{architecture,adr,benchmarks,operations,development}
```

---

## 49. Architecture Decision Records

- ADR-001: Go for gateway/control plane
- ADR-002: vLLM as inference runtime
- ADR-003: OpenAI-compatible public API
- ADR-004: Redis for ephemeral shared state
- ADR-005: Kafka for async lifecycle events
- ADR-006: PostgreSQL for durable metadata
- ADR-007: At-least-once event processing
- ADR-008: Scheduler interface
- ADR-009: Failure-after-first-token policy

Each includes Context, Decision, Alternatives, Consequences, Status.

---

## 50. Configuration

Typed config: config file, env vars, CLI flags. Use SERVERFLOW_-prefixed env variables (defaults: gateway.port=8080, scheduler.strategy=least-work, heartbeat 2s, unhealthy 10s).

---

## 51. Error Model

Errors: INVALID_REQUEST, UNAUTHORIZED, RATE_LIMITED, MODEL_NOT_FOUND, NO_CAPACITY, HEALTH/UNAVAILABLE, UPSTREAM_TIMEOUT, WORKER_UNAVAILABLE, INFERENCE_FAILED, INTERNAL_ERROR. Structured bodies.

---

## 52. Testing Pyramid

Unit: scheduler, rate limit, state transitions, validation, cost estimate, retry, circuit breaker. Integration: gateway, auth, rate limit, failures, streaming with Redis+Postgres+mock workers. E2E: full path with real/mock vLLM. Load and chaos later.

---

## 53. Mock Inference Worker

Configurable TTFT, tokens/sec, failure rate, queue, model:

```bash
mock --ttft=200ms --tokens-per-second=50 --failure-rate=.01 --model=qwen
```

Build early; unlock GPU-free testing.

---

## 54. Simulation Mode

Varied worker characteristics (100 t/s, 60 t/s, 20 t/s) to test scheduler learning.

---

## 55. CI Pipeline

Format, lint, unit, race, integration, build, Docker build. Go: gofmt, go vet, go test ./..., go test -race ./....

---

## 56. Branch Discipline

- main
- feature/<feature>
- fix/<fix>
- experiment/<scheduler>

Small intentional commits; no giant unrelated commits.

---

## 57. Documentation

README, ARCHITECTURE, AGENTS, docs/setup, local-development, request-lifecycle, scheduler, worker-lifecycle, failure-model, benchmarking, kubernetes.

---

## 58. Phased Implementation Plan

**Phase 0 — Foundation** (DONE: go workspace, config, logging, Makefile, CI, compose skeleton, docs). Acceptance: make test/lint/build all pass.

**Phase 1 — Single-worker vLLM baseline.** Run one model, call /v1/chat/completions, test streaming, capture latency/TTFT/tokens-sec, document memory.

**Phase 2 — Gateway MVP.** /healthz /readyz /v1/models /v1/chat/completions, streaming proxy, request IDs, structured logs, metrics. Measure overhead < 25ms p95.

**Phase 3 — Mock worker framework.** A CLI with configurable TTFT/tokens-per-sec/model/failures, streaming, queue. Run 3 concurrently.

**Phase 4 — Worker registry.** Registration, heartbeat, state machine, health timeout, worker/model lookup. Test worker death.

**Phase 5 — Scheduler framework.** Interface + random, round-robin, least-active, least-queue. Strategy via config; deterministic unit tests.

**Phase 6 — Multi-worker routing.** 3+ workers, retries, request attempts, model compatibility, no routing to unhealthy workers. Acceptance: 1000 synthetic requests distribution.

**Phase 7 — Baseline benchmark harness.** Load generation (concurrency/duration/prompt/rate/scheduler), persisted results, scheduler comparison.

**Phase 8 — Redis.** Distributed rate limiting; optional request metadata. 3 gateways share one quota; document failure behavior.

**Phase 9 — PostgreSQL.** Tenants, API keys (hashed), model configs, benchmark metadata. Migrations.

**Phase 10 — Prometheus + Grafana.** Instrument everything, dashboards, observe benchmark live.

**Phase 11 — OpenTelemetry.** Traces end-to-end; break spans; pick a request and inspect its full distributed trace.

**Phase 12 — Kafka.** Producer emits received, routed, first_token, completed, failed events; usage consumer. stop-consumer/replay acceptance.

**Phase 13 — Real vLLM worker pool.** Replace mocks with 2+ real; benchmark RR / least-queue / least-work; real GPU data.

**Phase 14 — Workload-aware scheduler.** Queued tokens, throughput, TTFT, GPU pressure, request size, hypotheses + experiments, document, correct predictions.

**Phase 15 — Fault tolerance.** Circuit breakers, retries, drain, bounded queues, backpressure; chaos benchmark.

**Phase 16 — Dockerized multi-service.** Control plane, mock workers, redis, kafka, postgres, prometheus, grafana, otel, one-command dev-up.

**Phase 17 — Kubernetes.** Deploy, service, configmap/secret, readiness/liveness, resource limits; rolling gateway update with no outage.

**Phase 18 — K8s GPU workers.** GPU nodes/device plugin/requests/readiness/drain; real inference.

**Phase 19 — Autoscaling.** Signals, rigid policy + cooldowns; compare to fixed replicas; measure scale-up, latency, utilization.

**Phase 20 — Model-aware scaling.** Per-model pools; independent demand-driven scaling.

**Phase 21 — Model placement controller.** Minimize expected queue delay + transition cost, model memory vs demand.

**Phase 22 — Advanced fair scheduling.** Priority tiers, token quotas, fair queues, batch vs interactive compare.

**Phase 23 — Final performance study.** RR vs least-queue, least-queue vs least-work, failures, bursts, autoscaling, mixed model, multi-tenant, cache effects, kafka overhead, gateway scaling.

**Phase 24 — Production polish.** Docs, diagrams, dashboards, demo, chaos video, ADR cleanup, security review, reproducible benchmarks.

---

## 59. Milestone Releases

- v0.1 single gateway
- v0.2 multi-worker routing
- v0.3 distributed state
- v0.4 observability
- v0.5 Kafka events
- v0.6 real GPU pool
- v0.7 adaptive scheduler
- v0.8 fault tolerance
- v0.9 Kubernetes
- v1.0 autoscaling + benchmark report

---

## 60. Definition of MVP

Client -> Gateway -> Scheduler -> 3 inference/mock workers, with health detection, streaming, multiple strategies, benchmark harness, request metrics.

---

## 61. Definition of V1

OpenAI-compatible gateway; auth; distributed rate limiting; Redis/Postgres; worker registry; model-aware routing; workload-aware scheduling; retries; circuit breakers; Prometheus/Grafana/OTel; Kafka; Docker; real workers; reproducible benchmarks.

---

## 62. Definition of Advanced

Kubernetes, GPU pools, autoscaling, model-aware scaling, dynamic placement, priority queues, tenant fairness, chaos testing, cost-aware routing.

---

## 63. Coding Agent Rules

1. Read AGENTS.md and ARCHITECTURE.md
2. Know current phase; do not jump ahead
3. Create/update tests with every behavior change
4. Run tests before declaring complete
5. Preserve contracts unless versioned
6. Keep packages cohesive; avoid cycles
7. No global mutable state; pass context.Context; propagate cancellation
8. Structured errors and logs; instrument request stages
9. Never unbounded queues, infinite retries, or swallowed errors
10. Never assume worker health; no stale routing without policy
11. Keep scheduler testable; add benchmarks for perf changes; document decisions
12. Simple before complex; measure before optimize

---

## 64. Agent Anti-Patterns

Do NOT: unneeded microservices, premature abstractions, Kafka as RPC, Postgres for heartbeats, Redis as source of truth, Kafka on hot path, plaintext keys, custom consensus, custom DB/runtime, train models, rewrite vLLM, K8s in MVP, generic 500 errors, god packages, mixing planes.

---

## 65. Development Philosophy

Each phase: Build -> Measure -> Identify bottleneck -> Hypothesize -> Change -> Benchmark -> Compare -> Document. Justify every infra choice with measured reasons.

---

## 66. Questions the System Must Answer

Which workers healthy? Models overloaded? Why routed worker? Queue time? TTFT? Most-queued worker? Failure rate? Recovery time after worker death? Best scheduler for p95? Mixed prompts? Fair access? Scaling need? Time to add capacity?

---

## 67. Final Demonstration Script

1. Dashboards
2. 3 Qwen workers
3. 100 concurrent requests
4. Traffic balancing
5. Scheduler switch (round-robin -> least-work)
6. Compare p95 TTFT
7. Kill a worker -> heartbeat loss -> unhealthy -> reroute
8. Restore worker -> back in pool
9. Spike -> queue grows
10. Autoscaler adds replica -> loads model -> readies -> traffic normal
11. Kafka consumer processing events
12. End-to-end trace

---

## 68. Final Resume/Wording Goal

Only numbers from reproducible experiments. Target: "Two lagged token-aware scheduler in p95 TTFT by 32%, throughput improved 11% vs round-robin." No fabricated metrics.

---

## 69. Project Success Criteria

Demonstrate backend APIs/streaming/auth/rate limiting; distributed discovery/failures/retries/health/distributed state/events; concurrency bounded/cancellation; performance engineering p95/p99 analysis; AI infra mirror + token scheduling; K8s/Docker/GPU; observability tracing; Kafka lifecycle events; and engineering judgment (justified tech).

---

## 70. First Execution Instruction

Start with what THIS spec defines — Phase 1 after Phase 0 is complete.

Follow phases sequentially. For each phase:

1. Create a plan under docs/plans/active/
2. Identify files/packages
3. Define interfaces first
4. Implement the smallest correct version
5. Write unit/integration tests
6. Static analysis + race detection
7. Acceptance tests
8. Benchmark results
9. Documentation
10. Commit after the phase passes acceptance

> Make the simplest turn work, measure it, then introduce complexity only when the system gives us a reason to.

**Source of truth:** This document is the authoritative execution spec for ServerFlow. All future work should reference it. Retire obsolete docs when the name changes.