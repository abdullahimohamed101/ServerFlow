# ADR-021: Containerized Deployment on One Machine, and the Insecure-Transport Knob It Needs

Status: Accepted (Phase 16)
Date: 2026-10-11

## Context

Phases 0 to 12 left every piece of ServerFlow runnable as a host process, plus three unrelated Docker helpers (throwaway test servers, a
Prometheus and Grafana pair for host processes, a standalone Jaeger) and a root `docker-compose.yml` that was an unvalidated Phase 0
skeleton with well-known passwords. Spec section 58 asks for "control plane, mock workers, redis, kafka, postgres, prometheus, grafana,
otel, one-command dev-up". The decisions D1 to D30 of the Phase 16 plan were approved with their defaults; this record keeps the ones
that are hard to reverse, easy to misread, or security relevant, and what building them taught us.

(The plan called this record ADR-020. Phase 15 also planned an ADR-020, so this one is numbered 021.)

## Decision

- **One multi-stage Dockerfile, six images** (`deploy/docker/Dockerfile`): a single `build` stage compiles every command once, and one final
  target per image (`gateway`, `control-plane`, `worker-agent`, `mock-worker`, `usage-consumer`, `admin`) copies its binary plus
  `healthcheck` onto `gcr.io/distroless/static-debian13:nonroot` (no shell, no package manager, user 65532). Built with `CGO_ENABLED=0
  -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X ...version.Version -X ...version.Commit"`, so identical inputs give identical
  binaries (measured: two `--no-cache` builds, identical SHA-256 for all seven). Layers are not claimed reproducible. The build uses only
  syntax the classic builder accepts, so it works without `buildx`; the cost is no Go build cache between builds.
- **Base images are pinned by tag and digest, literally, and none comes from Docker Hub.** Go builder from `public.ecr.aws`, runtime from
  `gcr.io`, PostgreSQL and Redis from `public.ecr.aws`, Prometheus and Jaeger from `quay.io`, Redpanda and Grafana from `mirror.gcr.io`
  (a cache run by Google: no published availability guarantee, so pulls retry with backoff and CI caches the Redpanda tarball),
  nginx from `public.ecr.aws`. The digest is the multi-arch index digest, identical on Docker Hub, so a mirror cannot substitute content.
  A test fails any reference that is unpinned, `:latest` or Docker Hub; each reference is overridable from `.env`.
- **Hardening is checked, not hoped for.** Every container is `read_only`, `cap_drop: [ALL]`, `no-new-privileges`, memory- and
  pids-limited, log-rotated, non-root and on an `internal: true` network with no route out. Writable paths are size-capped tmpfs. Only the
  gateway (or the nginx load balancer for `GATEWAYS>1`), the control plane, Prometheus, Grafana and the Jaeger UI publish a port, always on
  `127.0.0.1`. A static test pins this per service and a runtime audit (`make dev-audit`) checks the running containers.
  Findings while building it: Redpanda's `rpk` writes `/etc/redpanda` and `~/.config`, so those are tmpfs; a service that uses an image with a
  `VOLUME` needs a tmpfs on that path or Docker creates an anonymous volume (the one-shot `redpanda-init` job did, the audit caught it); and
  Compose carries an anonymous volume over when it recreates a container, so `down` removes them explicitly.
- **Ephemeral by default.** All data lives in tmpfs, so `make dev-down` leaves no container, network or volume; `PERSIST=1` swaps named volumes
  in through `docker-compose.persist.yml`.
- **Secrets are generated, never committed, never printed.** `scripts/dev-up.sh` writes `.dev/*.env` (mode 0600, directory 0700, gitignored)
  from `/dev/urandom` on first use, one file per concern (database, Redis, broker, control-plane token, metrics token, Grafana) so a service
  receives only what it needs, and only through `env_file` (`docker compose config` prints `environment:` values but, with
  `--no-env-resolution`, not env files). Redis reads its password from a tmpfs config file rather than argv; the Redpanda bootstrap user
  comes from `RP_BOOTSTRAP_USER` built inside the container. API keys are captured from the admin tool's single stdout line straight into a
  0600 file. The one world-readable file is `.dev/metrics_token`, because Prometheus runs as `nobody` and Compose cannot set a bind mount's
  mode. `.env` is hostile input: parsed (never sourced), charset-restricted, range-checked, with errors naming the variable and never the
  value; a Go test feeds it injection attempts.
- **Metrics listeners keep the guard on.** In containers the gateway and control plane bind `0.0.0.0` for metrics, so
  `SERVERFLOW_METRICS_TOKEN` is set and Prometheus presents it from a file; the config check that refuses a non-loopback metrics address
  without a token still protects the binaries (verified by starting them without the token).
- **`control_plane.allow_insecure_transport` (W1).** See the security review below.
- **Tracing and version stamping.** `SERVERFLOW_TRACING_ALLOW_INSECURE_TRANSPORT` gets the environment overlay the YAML key always lacked (W3).
  `internal/version` carries link-time `Version` and `Commit` that `serverflow_build_info` and the tracing `service.version` prefer over VCS
  data, which `-buildvcs=false` removes (W2). `cmd/healthcheck` (W4) is a standard-library-only probe that refuses non-loopback hosts,
  redirects and non-2xx answers, because distroless has no `curl`.
- **CI.** A static `deploy` job (hadolint pinned binary with checksum, shellcheck, `docker compose config` for every file and profile
  combination, the named tests which must run) and an advisory `compose smoke` job that builds, starts, runs 13 checks and requires
  `0 failed, 0 skipped`. Trivy and the scaled smoke run nightly. Dependabot gets the `docker` and `docker-compose` ecosystems.
- **Where the Prometheus configuration for containers lives.** `observability/prometheus/compose/` (not `targets/compose.yml`, as planned):
  the host-process stack mounts the whole `observability/prometheus` directory and loads every `targets/*.yml`, so a compose targets file
  there would make that stack scrape service names that do not resolve.

## Security review: `control_plane.allow_insecure_transport`

AGENTS.md requires an explicit review of security-sensitive changes. This is the review of W1.

- **What changed.** One boolean, default `false`, environment `SERVERFLOW_CONTROL_PLANE_ALLOW_INSECURE_TRANSPORT`, YAML
  `control_plane.allow_insecure_transport`. It is consulted in exactly two places: `Config.validateWorkerSource` (the gateway's
  `gateway.control_plane_url`) and `Config.ValidateAgent` (the agent's `worker.control_plane_url`). Both used to refuse `http://` to a
  non-loopback host while a control-plane token is set. Nothing else reads it: the control plane's own rule (a non-loopback listener
  requires a token, at least 16 characters) is unchanged, so turning the flag on never makes the control plane reachable without a token.
- **Why it is needed.** In containers the control plane must listen on a non-loopback address, therefore must have a token, therefore the
  clients' guard refused plaintext `http://control-plane:9090`. The alternatives were TLS inside the dev stack (certificate generation and
  trust for eight services, for a throwaway stack) or a loopback sidecar per client (does not reach the control plane).
- **Consistent with its siblings.** It has the same name, default, environment spelling and semantics as `postgres`, `redis`, `events` and
  `tracing` `.allow_insecure_transport`; the refusal message tells the operator to set it "only on a network you control".
- **Exposure.** Only `docker-compose.yml` sets it. The token then crosses the stack's private `internal: true` network in cleartext, where
  any other container on that network could observe it (a container with `CAP_NET_RAW` or on a shared bridge could sniff or spoof; the
  stack's own containers have no capabilities, and a third-party image is outside our control). The token guards a throwaway control plane
  that is also published on `127.0.0.1` only. This is acceptable for a one-machine development stack and is not a production pattern; Phase
  17 must use TLS or a service mesh for this path (D29).
- **No leak path.** The refusal error contains neither the URL nor the token (tested with canary values); the gateway's start-up log prints
  only whether a token is set; the token reaches containers only through `env_file`, is absent from images, `docker history`, the compose
  file, `docker compose config --no-env-resolution`, container argument lists and every container log (searched in the smoke test and by a
  manual scan of the saved image layers). Anyone who can read the Docker socket can read a container's environment: the same trust boundary
  as the developer's machine.
- **Default behaviour unchanged.** Tests pin: the default is false; an `http://` remote with a token is refused by both the gateway and the
  agent, and accepted only with the flag; `https://`, loopback and no-token cases are unaffected; a short token is still refused with the
  flag on; an unparsable environment value leaves the default; the YAML key works. `TestEnvVariablesAreHonoured` fails if the compose file
  stops setting it or misspells it.
- **Residual risk.** An operator can set it on a real network, exactly as with the four sibling flags; the defence is the default, the
  message, the documentation and review. Not independently reviewed by a second person at the time of writing.

## Consequences

- `make dev-up` gives a complete, hardened, secret-free local system in about a minute once images exist, and `make dev-smoke` proves it
  with 13 end-to-end checks in under half a minute.
- The compose stack is the reference for Phase 15 chaos runs (kill, pause and disconnect operations, documented with their measured
  behaviour) and for Phase 17 (the same six images, the `healthcheck` binary as exec probe, the same variable names).
- Open items: the worker agent has no liveness endpoint (Phase 17 decides how it proves liveness); `linux/amd64`, Docker Desktop, rootful
  Linux Docker and Podman are not verified from the development machine; Dependabot's handling of non-Hub registries is unverified;
  `mirror.gcr.io` has no availability guarantee.
