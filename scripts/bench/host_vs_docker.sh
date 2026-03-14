#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-bench-compare}"
IMAGE_TAG="${IMAGE_TAG:-chunkdb:bench-compare}"
OPS="${OPS:-20000}"

PARALLEL_JOBS=4
if command -v nproc >/dev/null 2>&1; then
  PARALLEL_JOBS="$(nproc)"
elif command -v sysctl >/dev/null 2>&1; then
  PARALLEL_JOBS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
fi

HOST_OUT="$(mktemp "${TMPDIR:-/tmp}/chunkdb-host-bench.XXXXXX")"
DOCKER_OUT="$(mktemp "${TMPDIR:-/tmp}/chunkdb-docker-bench.XXXXXX")"
HOST_PARSED="$(mktemp "${TMPDIR:-/tmp}/chunkdb-host-parsed.XXXXXX")"
DOCKER_PARSED="$(mktemp "${TMPDIR:-/tmp}/chunkdb-docker-parsed.XXXXXX")"
cleanup() {
  rm -f "${HOST_OUT}" "${DOCKER_OUT}" "${HOST_PARSED}" "${DOCKER_PARSED}"
}
trap cleanup EXIT

parse_metrics() {
  local input_file="$1"
  local output_file="$2"

  awk '
    /^[a-z_]+[[:space:]]+ops=/ {
      name = $1
      ops = ""
      p95 = ""
      p99 = ""
      for (i = 1; i <= NF; ++i) {
        if ($i ~ /^ops_s=/) {
          split($i, a, "=")
          ops = a[2]
        } else if ($i ~ /^p95_us=/) {
          split($i, a, "=")
          p95 = a[2]
        } else if ($i ~ /^p99_us=/) {
          split($i, a, "=")
          p99 = a[2]
        }
      }
      if (ops != "" && p95 != "" && p99 != "") {
        print name, ops, p95, p99
      }
    }
  ' "${input_file}" > "${output_file}"
}

echo "==> Building host benchmark binary"
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCHUNKDB_BUILD_TESTS=OFF -DCHUNKDB_WITH_TLS=OFF
cmake --build "${BUILD_DIR}" --target chunkdb_bench --parallel "${PARALLEL_JOBS}"

echo "==> Running host benchmark: chunkdb_bench --ops ${OPS}"
"${BUILD_DIR}/chunkdb_bench" --ops "${OPS}" | tee "${HOST_OUT}"

echo "==> Building Docker benchmark image (build stage)"
docker build --target build -t "${IMAGE_TAG}" "${ROOT_DIR}" >/dev/null

echo "==> Running Docker benchmark: chunkdb_bench --ops ${OPS}"
docker run --rm "${IMAGE_TAG}" /src/build/chunkdb_bench --ops "${OPS}" | tee "${DOCKER_OUT}"

parse_metrics "${HOST_OUT}" "${HOST_PARSED}"
parse_metrics "${DOCKER_OUT}" "${DOCKER_PARSED}"

echo
echo "Host vs Docker comparison (ops=${OPS})"
printf "%-24s %12s %12s %11s %11s %11s %11s %14s\n" \
  "scenario" "host_ops/s" "docker_ops/s" "host_p95" "docker_p95" "host_p99" "docker_p99" "overhead_ops_%"

awk '
  NR == FNR {
    order[++count] = $1
    host_ops[$1] = $2 + 0
    host_p95[$1] = $3 + 0
    host_p99[$1] = $4 + 0
    next
  }
  {
    docker_ops[$1] = $2 + 0
    docker_p95[$1] = $3 + 0
    docker_p99[$1] = $4 + 0
  }
  END {
    for (i = 1; i <= count; ++i) {
      name = order[i]
      h_ops = host_ops[name]
      d_ops = docker_ops[name]
      h_p95 = host_p95[name]
      d_p95 = docker_p95[name]
      h_p99 = host_p99[name]
      d_p99 = docker_p99[name]
      overhead = "n/a"
      if (h_ops > 0 && d_ops >= 0) {
        overhead_value = ((h_ops - d_ops) / h_ops) * 100.0
        overhead = sprintf("%.2f", overhead_value)
      }
      printf "%-24s %12.2f %12.2f %11.2f %11.2f %11.2f %11.2f %14s\n",
        name, h_ops, d_ops, h_p95, d_p95, h_p99, d_p99, overhead
    }
  }
' "${HOST_PARSED}" "${DOCKER_PARSED}"
