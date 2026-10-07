# ADR-010: An In-Memory Worker Registry with Heartbeat-Derived Health

Status: Accepted
Date: 2026-10-07

## Context

The scheduler can only route to workers it knows are alive, ready, and serving
the requested model (spec sections 11, 15). Workers come and go: they load models,
drain, crash, and restart. The registry that tracks them sits on the control
plane, off the request path, and must give a consistent answer to "which workers
may serve this model right now?" without trusting what a worker claims about
its own health (spec section 63, rule 10).

## Decision

- **The registry is in memory** in the control plane process. It is not durable:
  after a control plane restart workers see their next heartbeat answered 404 and
  register again, so it repopulates within about one heartbeat interval. Redis
  (Phase 8) and Postgres (Phase 9, durable worker configuration only) come later.
- **Health comes from heartbeat age measured by the registry at receipt**, never
  from timestamps or claims in the heartbeat. Under 5s is healthy, 5 to 10s suspect,
  over 10s unhealthy (state UNHEALTHY), over 30s lost (state LOST), then evicted
  after a retention period. All thresholds are configurable. State is computed from
  age on read, so a dead worker reads as dead even if the sweeper is not running.
- **Only READY, healthy workers are eligible.** Suspect workers are not offered by
  default; a scheduler can opt in.
- **Registrations are incarnations.** Registering returns a secret registration ID.
  A worker restarting under the same worker ID supersedes the old incarnation, and
  heartbeats or deregistrations carrying a stale ID are rejected (409
  `stale_registration`). An agent that is told it was superseded **stops** (exit 1)
  instead of registering again: re-registering would steal the identity back and the
  two processes would fight forever (found in review). An unknown worker gets 404 and
  an illegal transition gets 409 `illegal_transition`; both tell the agent to register
  again as a fresh incarnation.
- **A drain is one-way for an incarnation**, even through FAILED, so the scheduler can
  trust DRAINING as a signal that the worker will not serve again.
- **The worker agent is a sidecar for the control plane protocol only.** It probes
  its backend, registers, heartbeats, and deregisters. It is not in the data path;
  the gateway talks to the backend directly. A dead backend with a live agent is
  reported FAILED at once rather than waiting for the heartbeat timeout.
- **The control plane API is protected by a shared-secret bearer token.** Without it
  any process that can reach the API could register as a worker and steer prompts to
  an arbitrary address once routing exists. The control plane binds loopback by
  default and refuses to listen elsewhere without a token. Because a web page can reach
  a loopback service, requests with an `Origin` header are refused, bodies must be JSON,
  and without a token the `Host` must be a loopback name (DNS rebinding). Real API-key
  authentication is Phase 9.
- **Trust model for registered addresses**: a worker with the token is trusted to name
  its own address. The control plane never connects to it, but the gateway will, so
  address validation rejects credentials, queries, fragments, `..` segments, spaces,
  and unroutable hosts. A host must be a canonical IP literal or a strict ASCII DNS
  name; every other spelling (fullwidth digits, decimal, hex, or octal integers, zone ids,
  a trailing dot) is refused, because the dialer accepts spellings that `net.ParseIP` does
  not. Unspecified, link-local (cloud metadata), multicast, and broadcast addresses are
  refused. This is a guard against honest mistakes, not a boundary: a worker may still
  name a hostname that resolves to a forbidden address, so the gateway must re-check
  the resolved address when it dials (Phase 5/6).

## Alternatives

- **Redis keys with TTLs**: heartbeats refresh a TTL and expiry means death. Simple
  and shared across control plane replicas, but it loses the suspect and unhealthy
  distinction, adds a hot dependency to a Phase 4 component, and spec section 19
  keeps heartbeats out of Postgres. Revisit in Phase 8 if the control plane needs
  replicas.
- **Trust worker-reported health or timestamps**: cheap, but a hung worker can
  keep claiming to be healthy, and skewed clocks break age calculations.
- **A proxying agent in the data path**: lets the agent enforce limits and report
  exact load, at the cost of an extra hop on every token. Rejected for now (spec
  section 4: keep the data plane lean).
- **Consensus or gossip membership (etcd, Consul, SWIM)**: robust but far heavier
  than one control plane process needs, and the spec forbids building custom
  consensus.

## Consequences

- **Positive**: a dead worker is noticed within the configured threshold with no
  timers to tune; restarts and control plane restarts self-heal; the contract
  (`WorkerSnapshot`) is exactly what the scheduler interface needs; nothing in the
  request path depends on the registry beyond cached snapshots.
- **Negative**: the control plane is a single point of failure for discovery (the
  gateway must keep serving from cached snapshots, a Phase 5 requirement); registry
  state is lost on restart until workers re-register; a shared token is coarse until
  Phase 9.
- **Follow-ups**: transition events to Kafka (Phase 12), metrics (Phase 10), a vLLM
  backend for the agent (Phase 13), and moving ephemeral state to Redis if the
  control plane is replicated.

## Status

Accepted. Recorded during Phase 4.
