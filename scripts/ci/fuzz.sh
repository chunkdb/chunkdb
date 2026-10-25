#!/usr/bin/env bash
# Builds the libFuzzer targets with Clang and runs each for a fixed time.
# Usage: scripts/ci/fuzz.sh SECONDS [TARGET...]   (default: every target)
# Starts from generated seeds, the corpus already in build/fuzz/fuzz/corpus and
# the kept crash inputs in fuzz/regression. A crash, timeout or out-of-memory
# input is written to build/fuzz/ and fails the script.
set -euo pipefail

seconds="${1:?usage: fuzz.sh SECONDS [TARGET...]}"
shift
targets=("$@")
if [ "${#targets[@]}" -eq 0 ]; then
  targets=(cql request chunk_form wal image manifest slots migrations backup)
fi

cmake -S . -B build/fuzz -DCMAKE_CXX_COMPILER="${CXX:-clang++}" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCHUNKDB_FUZZ=ON \
  -DCHUNKDB_BUILD_TESTS=OFF -DCHUNKDB_WITH_TLS=OFF -DCHUNKDB_WERROR=ON
build_targets=(chunkdb_fuzz_seeds)
for name in "${targets[@]}"; do build_targets+=("chunkdb_fuzz_$name"); done
cmake --build build/fuzz -j 3 --target "${build_targets[@]}"
build/fuzz/fuzz/bin/chunkdb_fuzz_seeds build/fuzz/fuzz/seeds

for name in "${targets[@]}"; do
  mkdir -p "build/fuzz/fuzz/corpus/$name"
  cp -n "build/fuzz/fuzz/seeds/$name"/* "build/fuzz/fuzz/corpus/$name/" 2>/dev/null || true
  echo "::group::fuzz $name"
  "build/fuzz/fuzz/bin/chunkdb_fuzz_$name" "build/fuzz/fuzz/corpus/$name" "fuzz/regression/$name" \
    -max_total_time="$seconds" -timeout=10 -rss_limit_mb=2048 \
    -max_len=1048576 -artifact_prefix=build/fuzz/ -print_final_stats=1
  echo "::endgroup::"
done
