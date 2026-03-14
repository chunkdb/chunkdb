#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-layout-ab}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/bench/artifacts/layout-ab/runs}"
REPEATS="${REPEATS:-3}"
OPS_LIST="${OPS_LIST:-20000 100000}"
SCENARIOS="${SCENARIOS:-sparse_world_writes dense_world_writes cold_start_reads warm_cache_reads mixed_rw_70_30}"
DURABILITIES="${DURABILITIES:-relaxed fsync-wal fsync-checkpoint}"
LAYOUTS="${LAYOUTS:-fs_split_v1 fs_region_v1}"
REGION_SPAN="${REGION_SPAN:-16}"
RECOVERY_KILL_AFTER_SEC="${RECOVERY_KILL_AFTER_SEC:-2}"
RECOVERY_SCENARIO="${RECOVERY_SCENARIO:-dense_world_writes}"

PARALLEL_JOBS=4
if command -v nproc >/dev/null 2>&1; then
  PARALLEL_JOBS="$(nproc)"
elif command -v sysctl >/dev/null 2>&1; then
  PARALLEL_JOBS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
short_sha="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
platform="$(uname -s | tr '[:upper:]' '[:lower:]')"
RUN_DIR="${OUT_BASE}/${timestamp}-${short_sha}-${platform}"

mkdir -p "${RUN_DIR}/raw"

METADATA_FILE="${RUN_DIR}/metadata.txt"
RESULTS_TSV="${RUN_DIR}/results.tsv"
SUMMARY_TSV="${RUN_DIR}/summary.tsv"
SUMMARY_MD="${RUN_DIR}/summary.md"

cat > "${METADATA_FILE}" <<EOF
timestamp_utc=${timestamp}
platform=${platform}
git_commit=$(git -C "${ROOT_DIR}" rev-parse HEAD)
git_branch=$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD)
region_span=${REGION_SPAN}
repeats=${REPEATS}
ops_list=${OPS_LIST}
scenarios=${SCENARIOS}
durabilities=${DURABILITIES}
layouts=${LAYOUTS}
recovery_kill_after_sec=${RECOVERY_KILL_AFTER_SEC}
recovery_source_scenario=${RECOVERY_SCENARIO}
build_dir=${BUILD_DIR}
EOF

if command -v uname >/dev/null 2>&1; then uname -a > "${RUN_DIR}/system_uname.txt"; fi
if command -v lscpu >/dev/null 2>&1; then lscpu > "${RUN_DIR}/system_lscpu.txt"; fi
if command -v sw_vers >/dev/null 2>&1; then sw_vers > "${RUN_DIR}/system_sw_vers.txt"; fi
if command -v wmic >/dev/null 2>&1; then wmic cpu get name > "${RUN_DIR}/system_wmic_cpu.txt" 2>/dev/null || true; fi
if command -v c++ >/dev/null 2>&1; then c++ --version > "${RUN_DIR}/compiler_version.txt" 2>&1 || true; fi
if command -v cmake >/dev/null 2>&1; then cmake --version > "${RUN_DIR}/cmake_version.txt" 2>&1 || true; fi

echo "==> Configuring and building chunkdb_layout_ab_bench"
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" \
  -DCHUNKDB_BUILD_TESTS=ON \
  -DCHUNKDB_BUILD_EXPERIMENTAL_LAYOUT=ON \
  -DCHUNKDB_WITH_TLS=OFF > "${RUN_DIR}/cmake_configure.log" 2>&1
cmake --build "${BUILD_DIR}" --target chunkdb_layout_ab_bench --parallel "${PARALLEL_JOBS}" > "${RUN_DIR}/cmake_build.log" 2>&1

cat > "${RESULTS_TSV}" <<'EOF'
platform	run_type	scenario	layout	durability	ops	repeat	total_s	ops_s	p50_us	p95_us	p99_us	files	dirs	bytes_chk	bytes_wal	cpu_user_s	cpu_sys_s	peak_rss_bytes	raw_log_path
EOF

extract_metric() {
  local line="$1"
  local key="$2"
  echo "${line}" | tr ' ' '\n' | awk -F= -v k="${key}" '$1==k{print $2}'
}

append_result_row() {
  local run_type="$1"
  local scenario="$2"
  local layout="$3"
  local durability="$4"
  local ops="$5"
  local repeat="$6"
  local line="$7"
  local log_path="$8"

  local total_s ops_s p50_us p95_us p99_us files dirs bytes_chk bytes_wal cpu_user_s cpu_sys_s peak_rss
  total_s="$(extract_metric "${line}" "total_s")"
  ops_s="$(extract_metric "${line}" "ops_s")"
  p50_us="$(extract_metric "${line}" "p50_us")"
  p95_us="$(extract_metric "${line}" "p95_us")"
  p99_us="$(extract_metric "${line}" "p99_us")"
  files="$(extract_metric "${line}" "files")"
  dirs="$(extract_metric "${line}" "dirs")"
  bytes_chk="$(extract_metric "${line}" "bytes_chk")"
  bytes_wal="$(extract_metric "${line}" "bytes_wal")"
  cpu_user_s="$(extract_metric "${line}" "cpu_user_s")"
  cpu_sys_s="$(extract_metric "${line}" "cpu_sys_s")"
  peak_rss="$(extract_metric "${line}" "peak_rss_bytes")"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "${platform}" "${run_type}" "${scenario}" "${layout}" "${durability}" "${ops}" "${repeat}" \
    "${total_s}" "${ops_s}" "${p50_us}" "${p95_us}" "${p99_us}" "${files}" "${dirs}" "${bytes_chk}" "${bytes_wal}" \
    "${cpu_user_s}" "${cpu_sys_s}" "${peak_rss}" "${log_path}" >> "${RESULTS_TSV}"
}

run_case() {
  local run_type="$1"
  local scenario="$2"
  local layout="$3"
  local durability="$4"
  local ops="$5"
  local repeat="$6"
  local case_id="${run_type}-${scenario}-${layout}-${durability}-ops${ops}-r${repeat}"
  local case_dir="${RUN_DIR}/raw/${case_id}"
  local data_dir="${case_dir}/data"
  local log_file="${case_dir}/bench.log"

  mkdir -p "${case_dir}"

  local cmd=(
    "${BUILD_DIR}/chunkdb_layout_ab_bench"
    --scenario "${scenario}"
    --ops "${ops}"
    --layout "${layout}"
    --durability "${durability}"
    --region-span "${REGION_SPAN}"
    --data-dir "${data_dir}"
    --keep-data
  )

  echo "==> ${case_id}"
  "${cmd[@]}" > "${log_file}" 2>&1
  local result_line
  result_line="$(grep '^RESULT ' "${log_file}" | tail -n 1 || true)"
  if [[ -z "${result_line}" ]]; then
    echo "missing RESULT line in ${log_file}" >&2
    exit 1
  fi
  append_result_row "${run_type}" "${scenario}" "${layout}" "${durability}" "${ops}" "${repeat}" "${result_line}" "${log_file}"
}

run_recovery_case() {
  local layout="$1"
  local durability="$2"
  local ops="$3"
  local repeat="$4"
  local case_id="recovery-${layout}-${durability}-ops${ops}-r${repeat}"
  local case_dir="${RUN_DIR}/raw/${case_id}"
  local data_dir="${case_dir}/data"
  local writer_log="${case_dir}/writer.log"
  local recovery_log="${case_dir}/recovery.log"
  mkdir -p "${case_dir}"

  local writer_cmd=(
    "${BUILD_DIR}/chunkdb_layout_ab_bench"
    --scenario "${RECOVERY_SCENARIO}"
    --ops 1000000000
    --layout "${layout}"
    --durability "${durability}"
    --region-span "${REGION_SPAN}"
    --data-dir "${data_dir}"
    --keep-data
  )

  echo "==> ${case_id} (forced stop + recovery probe)"
  "${writer_cmd[@]}" > "${writer_log}" 2>&1 &
  local writer_pid="$!"
  sleep "${RECOVERY_KILL_AFTER_SEC}"
  kill -9 "${writer_pid}" >/dev/null 2>&1 || true
  wait "${writer_pid}" >/dev/null 2>&1 || true

  local recovery_cmd=(
    "${BUILD_DIR}/chunkdb_layout_ab_bench"
    --scenario recovery_probe
    --ops "${ops}"
    --layout "${layout}"
    --durability "${durability}"
    --region-span "${REGION_SPAN}"
    --data-dir "${data_dir}"
    --keep-data
    --no-reset
  )
  "${recovery_cmd[@]}" > "${recovery_log}" 2>&1
  local result_line
  result_line="$(grep '^RESULT ' "${recovery_log}" | tail -n 1 || true)"
  if [[ -z "${result_line}" ]]; then
    echo "missing RESULT line in ${recovery_log}" >&2
    exit 1
  fi
  append_result_row "recovery" "recovery_probe" "${layout}" "${durability}" "${ops}" "${repeat}" "${result_line}" "${recovery_log}"
}

for durability in ${DURABILITIES}; do
  for scenario in ${SCENARIOS}; do
    for ops in ${OPS_LIST}; do
      for repeat in $(seq 1 "${REPEATS}"); do
        for layout in ${LAYOUTS}; do
          run_case "scenario" "${scenario}" "${layout}" "${durability}" "${ops}" "${repeat}"
        done
      done
    done
  done
done

for durability in ${DURABILITIES}; do
  for ops in ${OPS_LIST}; do
    for repeat in $(seq 1 "${REPEATS}"); do
      for layout in ${LAYOUTS}; do
        run_recovery_case "${layout}" "${durability}" "${ops}" "${repeat}"
      done
    done
  done
done

SUMMARY_TMP="${RUN_DIR}/summary.unsorted.tsv"

awk -F'\t' '
NR==1 { next }
{
  key = $2 FS $3 FS $4 FS $5 FS $6
  count[key] += 1
  total_s[key] += $8 + 0
  ops_s[key] += $9 + 0
  p50[key] += $10 + 0
  p95[key] += $11 + 0
  p99[key] += $12 + 0
  files[key] += $13 + 0
  dirs[key] += $14 + 0
  chk[key] += $15 + 0
  wal[key] += $16 + 0
  cpu_user[key] += $17 + 0
  cpu_sys[key] += $18 + 0
  rss[key] += $19 + 0
}
END {
  print "run_type\tscenario\tlayout\tdurability\tops\trepeats\tavg_total_s\tavg_ops_s\tavg_p50_us\tavg_p95_us\tavg_p99_us\tavg_files\tavg_dirs\tavg_bytes_chk\tavg_bytes_wal\tavg_cpu_user_s\tavg_cpu_sys_s\tavg_peak_rss_bytes"
  for (k in count) {
    n = count[k]
    split(k, parts, FS)
    printf "%s\t%s\t%s\t%s\t%s\t%d\t%.6f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.6f\t%.6f\t%.2f\n",
      parts[1], parts[2], parts[3], parts[4], parts[5], n,
      total_s[k] / n, ops_s[k] / n, p50[k] / n, p95[k] / n, p99[k] / n,
      files[k] / n, dirs[k] / n, chk[k] / n, wal[k] / n, cpu_user[k] / n, cpu_sys[k] / n, rss[k] / n
  }
}
' "${RESULTS_TSV}" > "${SUMMARY_TMP}"

{
  head -n 1 "${SUMMARY_TMP}"
  tail -n +2 "${SUMMARY_TMP}" | sort
} > "${SUMMARY_TSV}"

{
  echo "# Layout A/B Summary"
  echo
  echo "- run_dir: \`${RUN_DIR}\`"
  echo "- platform: \`${platform}\`"
  echo "- region_span: \`${REGION_SPAN}\`"
  echo "- repeats: \`${REPEATS}\`"
  echo "- ops_list: \`${OPS_LIST}\`"
  echo
  echo "## Aggregated Results"
  echo
  echo '```tsv'
  cat "${SUMMARY_TSV}"
  echo '```'
} > "${SUMMARY_MD}"

echo "layout_ab_run_dir=${RUN_DIR}"
echo "results_tsv=${RESULTS_TSV}"
echo "summary_tsv=${SUMMARY_TSV}"
echo "summary_md=${SUMMARY_MD}"
