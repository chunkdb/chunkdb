#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
configuration="${1:-}"
case "${configuration}" in
  gcc|gcc-tls|tsan|asan) ;;
  *) echo "Usage: $0 <gcc|gcc-tls|tsan|asan> [ctest args...]" >&2; exit 2 ;;
esac
shift

jobs="${PARALLEL_JOBS:-6}"
if [[ ! "${jobs}" =~ ^[1-9][0-9]*$ ]]; then
  echo "PARALLEL_JOBS must be a positive integer" >&2
  exit 2
fi

# Isolate checkouts: older source mtimes must not reuse another checkout's objects.
source_key="$(printf '%s' "${ROOT_DIR}" | git -C "${ROOT_DIR}" hash-object --stdin)"
cache_name="chunkdb-check-${configuration}-${source_key}"
# The fixed source path makes CMake and optional ccache reusable across runs.
# A fixed container name prevents simultaneous writers to the same volume.
nice -n 10 docker run --rm --name "${cache_name}" \
  --mount "type=bind,source=${ROOT_DIR},target=/src,readonly" \
  --mount "type=volume,source=${cache_name},target=/build" \
  --env "CHUNKDB_CHECK_CONFIG=${configuration}" --env "PARALLEL_JOBS=${jobs}" \
  chunkdb:stand bash -c '
set -euo pipefail
cmake_args=(-S /src -B /build -DCHUNKDB_BUILD_TESTS=ON -DCHUNKDB_WERROR=ON)
ctest_defaults=(-L smoke)
case "${CHUNKDB_CHECK_CONFIG}" in
  gcc|gcc-tls)
    cmake_args+=(-DCMAKE_BUILD_TYPE=Release)
    if [[ "${CHUNKDB_CHECK_CONFIG}" == gcc-tls ]]; then
      cmake_args+=(-DCHUNKDB_WITH_TLS=ON -DCMAKE_REQUIRE_FIND_PACKAGE_OpenSSL=ON)
    else
      cmake_args+=(-DCHUNKDB_WITH_TLS=OFF)
    fi
    ;;
  tsan)
    cmake_args+=(-DCMAKE_BUILD_TYPE=Debug -DCHUNKDB_WITH_TLS=OFF
      "-DCMAKE_CXX_FLAGS=-fsanitize=thread -fno-omit-frame-pointer"
      "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread")
    export TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1
    ctest_defaults+=(-E "process_lock|durability_kill_recovery")
    ;;
  asan)
    cmake_args+=(-DCMAKE_BUILD_TYPE=Debug -DCHUNKDB_WITH_TLS=OFF
      "-DCMAKE_CXX_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer"
      "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined")
    export ASAN_OPTIONS=abort_on_error=1:detect_leaks=1
    export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
    ;;
esac
launcher=""
if command -v ccache >/dev/null 2>&1; then
  launcher=ccache
  export CCACHE_DIR=/build/.ccache
fi
cmake_args+=("-DCMAKE_CXX_COMPILER_LAUNCHER=${launcher}")
nice -n 10 cmake "${cmake_args[@]}"
nice -n 10 cmake --build /build --parallel "${PARALLEL_JOBS}"
if [[ $# -eq 0 ]]; then set -- "${ctest_defaults[@]}"; fi
nice -n 10 ctest --test-dir /build --output-on-failure "$@"
' bash "$@"
