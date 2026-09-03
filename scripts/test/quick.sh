#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-quick}"
CHUNKDB_WITH_TLS="${CHUNKDB_WITH_TLS:-OFF}"
CHUNKDB_WERROR="${CHUNKDB_WERROR:-OFF}"

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
  -DCHUNKDB_WERROR="${CHUNKDB_WERROR}"
)

if [[ -n "${CMAKE_GENERATOR:-}" ]]; then
  cmake_args+=(-G "${CMAKE_GENERATOR}")
fi
if [[ -n "${CMAKE_CXX_COMPILER_LAUNCHER:-}" ]] && command -v "${CMAKE_CXX_COMPILER_LAUNCHER}" >/dev/null 2>&1; then
  cmake_args+=(-DCMAKE_CXX_COMPILER_LAUNCHER="${CMAKE_CXX_COMPILER_LAUNCHER}")
fi

cmake "${cmake_args[@]}"
cmake --build "${BUILD_DIR}" --parallel "${PARALLEL_JOBS}"
ctest --test-dir "${BUILD_DIR}" -L smoke --output-on-failure
