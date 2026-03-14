#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-full}"
CHUNKDB_WITH_TLS="${CHUNKDB_WITH_TLS:-OFF}"
STRESS_REPEAT="${STRESS_REPEAT:-1}"

PARALLEL_JOBS=4
if command -v nproc >/dev/null 2>&1; then
  PARALLEL_JOBS="$(nproc)"
elif command -v sysctl >/dev/null 2>&1; then
  PARALLEL_JOBS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
fi

cmake_args=(
  -S "${ROOT_DIR}"
  -B "${BUILD_DIR}"
  -DCHUNKDB_BUILD_TESTS=ON
  -DCHUNKDB_BUILD_EXPERIMENTAL_LAYOUT=OFF
  -DCHUNKDB_WITH_TLS="${CHUNKDB_WITH_TLS}"
)

if [[ -n "${CMAKE_GENERATOR:-}" ]]; then
  cmake_args+=(-G "${CMAKE_GENERATOR}")
fi

cmake "${cmake_args[@]}"
cmake --build "${BUILD_DIR}" --parallel "${PARALLEL_JOBS}"

ctest --test-dir "${BUILD_DIR}" -L smoke --output-on-failure

if [[ "${STRESS_REPEAT}" -gt 1 ]]; then
  ctest --test-dir "${BUILD_DIR}" -L stress --repeat "until-fail:${STRESS_REPEAT}" --output-on-failure
else
  ctest --test-dir "${BUILD_DIR}" -L stress --output-on-failure
fi
