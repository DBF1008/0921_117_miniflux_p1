#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Unit test runner for Miniflux.
#
# Usage:
#   ./test.sh                       # run all unit tests with the race detector
#   ./test.sh ./internal/worker     # run specific packages
#
# Notes:
#   - API integration tests (./internal/api) require a running server and the
#     TEST_MINIFLUX_* environment variables; they skip automatically when the
#     variables are not set.
#   - A few tests start local HTTP servers (httptest); they need permission to
#     bind a loopback port. If they fail with "bind: operation not permitted",
#     run the script in an environment that allows loopback network access.
set -uo pipefail

cd "$(dirname "$0")"

if [ "$#" -eq 0 ]; then
	PKGS=(./...)
else
	PKGS=("$@")
fi

echo "==> go vet"
if ! go vet "${PKGS[@]}"; then
	echo "==> go vet failed"
	exit 1
fi

echo "==> go test (race detector enabled)"
if ! go test -race -count=1 -shuffle=on "${PKGS[@]}"; then
	echo "==> some tests failed"
	exit 1
fi

echo "==> all tests passed"
