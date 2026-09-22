#!/bin/sh
# Manual unit test script for the worker pool race condition and graceful
# shutdown fixes (pool.go, worker.go, daemon.go).
#
# Usage: sh test.sh
set -e

cd "$(dirname "$0")"

echo "==> 1/5 Build all packages"
go build ./...

echo "==> 2/5 Vet modified packages"
go vet ./internal/worker/ ./internal/cli/ ./internal/reader/...

echo "==> 3/5 Worker pool unit tests (race detector, verbose)"
# Covers:
#   - Push after Shutdown does not panic (no send on closed channel)
#   - Push/Shutdown concurrency race
#   - Push unblocked when Shutdown cancels the pool context
#   - Shutdown timeout protection
#   - Worker stops on context cancellation / queue close
go test -race -count=1 -v ./internal/worker/

echo "==> 4/5 Related package tests (reader, cli)"
go test -race -count=1 ./internal/reader/... ./internal/cli/...

echo "==> 5/5 Full test suite (same as 'make test')"
go test -cover -race -count=1 ./...

echo "All tests passed."
