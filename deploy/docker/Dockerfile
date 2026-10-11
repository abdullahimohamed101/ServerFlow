# ServerFlow container images (ADR-021). One build stage compiles every binary once; one final target per image:
#
#   docker build -f deploy/docker/Dockerfile --target gateway -t serverflow/gateway:dev .
#
# Targets: gateway, control-plane, worker-agent, mock-worker, usage-consumer, admin. Context is the repository root.
# Only syntax the classic builder and BuildKit both accept is used (no cache mounts, heredocs or BUILDPLATFORM), so
# this builds without buildx. FROM lines are literal and digest-pinned (no FROM ${ARG}); bump them with the
# Dependabot "docker" ecosystem. The builder comes from public.ecr.aws because Docker Hub rate-limits shared CI runners.

FROM public.ecr.aws/docker/library/golang:1.26.9@sha256:d7722066f0b60ceccb6c0643cbed1f5f9e15506ac237146c95333504d7805d89 AS build
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
FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS runtime
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
