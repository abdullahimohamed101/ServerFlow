# ServerFlow container images (ADR-021). One build stage compiles every binary once; one final target per image:
#
#   docker build -f deploy/docker/Dockerfile --target gateway -t serverflow/gateway:dev .
#
# Targets: gateway, control-plane, worker-agent, mock-worker, usage-consumer, admin. Context is the repository root.
# Only syntax the classic builder and BuildKit both accept is used (no cache mounts, heredocs or BUILDPLATFORM), so
# this builds without buildx. FROM lines are literal and digest-pinned (no FROM ${ARG}); bump them with the
# Dependabot "docker" ecosystem. The builder comes from mirror.gcr.io (Google's anonymous Docker Hub mirror, same digests) because Docker Hub rate-limits shared CI runners and public.ecr.aws refused them from 2026-10-16.

FROM mirror.gcr.io/library/golang:1.27.3@sha256:2920bb124f1a2e12a1d39f0cab756ae0068441ee1da7655dec7f5e374944095b AS build
# Labels let `make dev-reset` find the dangling build stages the classic builder leaves behind (each is 2+ GB).
LABEL org.serverflow.stage="build"
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off GOFLAGS=-mod=readonly
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
COPY migrations ./migrations
ARG VERSION=dev
ARG COMMIT=unknown
# -trimpath, -buildvcs=false and an empty build id make identical inputs give identical binaries; the stamp comes from
# the build args, not from VCS data (there is no .git in the context).
RUN go build -trimpath -buildvcs=false \
      -ldflags="-s -w -buildid= -X serverflow/internal/version.Version=${VERSION} -X serverflow/internal/version.Commit=${COMMIT}" \
      -o /out/ \
      ./cmd/gateway ./cmd/control-plane ./cmd/worker-agent ./cmd/mock-worker ./cmd/usage-consumer ./cmd/admin ./cmd/healthcheck

# Shared runtime base: distroless static (no shell, no package manager), non-root, the health probe binary.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 AS runtime
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/abdullahimohamed101/ServerFlow" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.title="ServerFlow"
COPY --from=build /out/healthcheck /usr/local/bin/healthcheck
USER 65532:65532

FROM runtime AS gateway
COPY --from=build /out/gateway /usr/local/bin/gateway
EXPOSE 8080 9100
ENTRYPOINT ["/usr/local/bin/gateway"]

FROM runtime AS control-plane
COPY --from=build /out/control-plane /usr/local/bin/control-plane
EXPOSE 9090 9101
ENTRYPOINT ["/usr/local/bin/control-plane"]

FROM runtime AS worker-agent
COPY --from=build /out/worker-agent /usr/local/bin/worker-agent
ENTRYPOINT ["/usr/local/bin/worker-agent"]

FROM runtime AS mock-worker
COPY --from=build /out/mock-worker /usr/local/bin/mock-worker
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/mock-worker"]

FROM runtime AS usage-consumer
COPY --from=build /out/usage-consumer /usr/local/bin/usage-consumer
ENTRYPOINT ["/usr/local/bin/usage-consumer"]

FROM runtime AS admin
COPY --from=build /out/admin /usr/local/bin/admin
ENTRYPOINT ["/usr/local/bin/admin"]
