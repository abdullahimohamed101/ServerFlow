# Chore: OpenTelemetry v1.47.0, golang.org/x/net v0.61.0, remove the govulncheck exception

Status: Complete (2026-10-11). Branch `chore/otel-xnet-bump`, from master 4b9fd32 (Phases 0-12, Go floor 1.26).

## Outcome

The five `golang.org/x/net` advisories that ADR-018 accepted through an enforced exception list (GO-2026-6617, 6612, 6611, 6610, 6603) are fixed,
the exception is deleted, and `scripts/quality.sh vuln` is plain `govulncheck ./...` again. Phase 11 criterion 14 is met without an exception.

## What changed

| Module | Before | After |
|---|---|---|
| `go.opentelemetry.io/otel`, `/trace`, `/metric`, `/sdk`, `/exporters/otlp/otlptrace`, `.../otlptracehttp` | v1.46.0 | v1.47.0 |
| `go.opentelemetry.io/otel/log` (new indirect, required by the 1.47 modules) | none | v1.47.0 |
| `golang.org/x/net` | v0.59.0 | v0.61.0 (newest; `go list -m -versions`) |
| `golang.org/x/sys`, `x/text`, `x/sync` (pulled up by x/net and otel requirements, not chosen) | v0.48.0, v0.42.0, v0.23.0 | v0.49.0, v0.43.0, v0.24.0 |

- No source change was needed: `internal/tracing` (custom batch processor, resource wrapper, exporter interface, tracetest) compiled and its tests
  passed unmodified. The v1.47.0 changelog items that touch what this repository uses: a default attribute-value depth limit of 64 (the code only
  sets scalar attributes), `OTEL_TRACES_SAMPLER*` empty values treated as unset (the code ignores `OTEL_*` anyway), and the Go 1.25 drop. The
  headline change (stable Logs API/SDK) is not used.
- `scripts/quality.sh`: `VULN_EXCEPTIONS` and the vulnfilter wiring removed; the pinned `govulncheck@v1.8.0` install stays. `scripts/vulnfilter`
  and its tests deleted (nothing else referenced them).
- `.github/dependabot.yml`: the whole `ignore` block removed. otel (`>=1.47.0`) was the hold for this change. pgx (`>=5.9.0`) was stale: we are on
  5.9.2 and v5.11.0 declares Go 1.25. go-redis (`>=9.23.0`) existed because v9.23.0 declares Go 1.26, which the floor now allows. No ignore remained
  necessary, so none was kept. Consequence: the next weekly Dependabot group PR will propose pgx v5.11.0 and go-redis v9.23.0 (reviewed there).
- Docs updated: ADR-018 (historical exception marked, new "Removal of the vulnerability exception" section), docs/operations/tracing.md,
  docs/development/ci.md (vuln row), the Phase 11 plan (update note), the Go 1.26 plan (pointer).

## Not applied (`go list -m -u all` candidates, direct ones)

pgx v5.9.2 to v5.11.0; go-redis v9.22.0 to v9.23.0; franz-go v1.21.7 to v1.22.1 and kmsg v1.13.1 to v1.14.0; client_golang v1.24.1 to v1.25.0;
procfs v0.21.1 to v0.22.0; plus indirect and test-only modules (klauspost/compress, lz4, puddle, cpuid, go-jose, go-openapi/*, contrib
otelgrpc/otelhttp, stdouttrace v1.46.0 to v1.47.0 as a graph-only entry, and others).

## Verification

- `govulncheck` v1.8.0 (the version pinned in `quality.sh`; no newer one was needed) with `GOTOOLCHAIN=go1.27.2`: source mode `./...` and binary mode on
  gateway, usage-consumer, admin, control-plane and mock-worker all print "No vulnerabilities found" (no x/net, grpc or otel finding). DB updated 2026-10-08.
  `GOTOOLCHAIN=go1.27.2 scripts/quality.sh vuln` exits 0 with no exception.
- gofmt, `go vet`, golangci-lint (0 issues), `go mod tidy -diff` (empty), `GOTOOLCHAIN=go1.26.0 CGO_ENABLED=0 go build ./...` (floor builds).
- Focused `-race -count=3`: internal/tracing/..., internal/gateway, internal/mockworker, internal/config, cmd/gateway, internal/gofloor: all pass, tests unmodified.
- One gate with Postgres, Redis and Redpanda: `quality.sh full` (unit, race, integration, promtool, dashboards, builds, four cross-compiles, mod verify, tidy), `tracing`, `observability`: pass.
- Binary size (`-trimpath -ldflags="-s -w"`): gateway 30 895 042 to 30 945 938 bytes (+0.2%); mock-worker 15 718 802 to 15 736 482 (+0.1%).
- `TestTracingOverhead` once: off p95 956 us, unsampled added p95 -5 us, 100% sampled added p50 +18 us and p95 +75 us; same as the Phase 11 record, inside noise.
- `scripts/trace-demo.sh` against Jaeger 2.22.0: a retried request is one trace; attempt 1 `worker.forward` is `retried` (status_503) with a worker `inference` span
  `failed`, attempt 2 links to attempt 1; same shape as documented.

## What remains

- Dependabot will propose the unapplied bumps above.
- A real inference worker still has to propagate `traceparent` (Phase 13); unchanged.
