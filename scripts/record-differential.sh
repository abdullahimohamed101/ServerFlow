#!/usr/bin/env bash
# Re-records the gateway differential fixtures (internal/gateway/testdata/differential/*.json).
#
# Run this ONLY on a tree whose production code is the behaviour to be preserved (Phase 14 PR A recorded it once,
# from unmodified master at 32b20d4 plus the test harness). The ordinary test run compares against the fixtures:
#   go test ./internal/gateway -run TestDifferentialAgainstRecordedFixtures
# Toolchain version, commit, version, ports, IDs and timestamps are normalised in the harness, so the result does
# not depend on the Go version used to record.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [ -n "$(git status --porcelain -- . ':!internal/gateway/testdata/differential' ':!scripts/record-differential.sh' ':!internal/gateway/differential_test.go')" ]; then
  echo "record-differential: the working tree has other changes; record from an unmodified tree" >&2
  exit 1
fi
rm -rf internal/gateway/testdata/differential
go test ./internal/gateway -run TestDifferentialAgainstRecordedFixtures -record-differential -count=1 -timeout 300s
echo "recorded: $(git rev-parse --short HEAD) (+ uncommitted fixtures)"
