# Storage Layout A/B Benchmark Snapshot

This report documents the experimental A/B comparison between:

- `fs_split_v1` (current default backend)
- `fs_region_v1` (experimental prototype, benchmark-only)

## Status and Scope

- `fs_region_v1` is **experimental** and used only for layout decision benchmarking.
- No protocol changes are introduced by this benchmark snapshot.
- No data migration support is included in this benchmark snapshot.
- `fs_split_v1` remains default and unchanged for server/runtime defaults.

## Scenarios

- `sparse_world_writes`
- `dense_world_writes`
- `cold_start_reads`
- `warm_cache_reads`
- `mixed_rw_70_30`
- recovery probe after forced-stop (`recovery_probe`)

## Durability Modes

- `relaxed`
- `fsync-wal`
- `fsync-checkpoint`

## Run Matrix

- ops: `20000`, `100000`
- repeats: `3`
- layouts: `fs_split_v1`, `fs_region_v1`
- durability modes: `3`
- scenarios: `5` + recovery probe

Scenario details used by `chunkdb_layout_ab_bench` in this benchmark run:

- `sparse_world_writes`: random block coordinates in `[-1023, 1023]` per axis
- `dense_world_writes`: deterministic writes in `[0, 511] x [0, 511]`
- `cold_start_reads`: preseed + reopen + timed read pass
- `warm_cache_reads`: one warm-up pass before timed reads
- `mixed_rw_70_30`: `70%` reads / `30%` writes on dense coordinates
- `recovery_probe`: repeated reads of `(0,0)` after forced stop to measure restart/replay path without sparse reload amplification

## Single Entrypoint

```bash
scripts/bench/layout_ab.sh
```

The script configures CMake with:

- `-DCHUNKDB_BUILD_EXPERIMENTAL_LAYOUT=ON`

so the experimental benchmark target is built only for this opt-in path.

Example full run command used for the measured snapshot below:

```bash
./scripts/bench/layout_ab.sh
```

The script writes reproducible artifacts outside the source tree by default:

- `${CHUNKDB_BENCH_OUTPUT_DIR:-${TMPDIR:-/tmp}/chunkdb-bench-runs}/layout-ab/runs/<timestamp>-<sha>-<platform>/`

Key files:

- `metadata.txt`
- `results.tsv` (raw per-run records)
- `summary.tsv` (aggregated means)
- `summary.md` (rendered table + run metadata)
- `raw/*/bench.log` and `raw/*/recovery.log` (per-case outputs)

Use `OUT_BASE` to intentionally place reviewed layout A/B run bundles elsewhere.
Only small curated summaries should be copied into `docs/benchmarks/` for repository history.

## Measured Snapshot (2026-03-14, Darwin/arm64)

Machine:

- Apple M1 Pro class machine (`arm64`)
- macOS `26.3.1` (Darwin `25.3.0`)

Versioned artifact snapshot committed in repository:

- [docs/benchmarks/layout_ab/2026-03-14-darwin/metadata.txt](benchmarks/layout_ab/2026-03-14-darwin/metadata.txt)
- [docs/benchmarks/layout_ab/2026-03-14-darwin/results.tsv](benchmarks/layout_ab/2026-03-14-darwin/results.tsv)
- [docs/benchmarks/layout_ab/2026-03-14-darwin/summary.tsv](benchmarks/layout_ab/2026-03-14-darwin/summary.tsv)

Runtime artifact directory from the measured run:

- `bench/artifacts/layout-ab/runs/20260314T115511Z-1adc1aa-darwin/`

Aggregated `ops/s` delta (`fs_region_v1` vs `fs_split_v1`, mean over both ops sizes and all durability modes):

| scenario | fs_split_v1 ops/s | fs_region_v1 ops/s | delta |
| --- | ---:| ---:| ---:|
| sparse_world_writes | 67,222.22 | 32,684.20 | **-51.38%** |
| dense_world_writes | 81,739.36 | 86,064.24 | +5.29% |
| warm_cache_reads | 1,630,774.98 | 1,664,191.41 | +2.05% |
| mixed_rw_70_30 | 166,177.71 | 123,426.12 | **-25.73%** |
| cold_start_reads | 84,797.67 | 7,621.92 | **-91.01%** |

Notes:

- `fs_region_v1` dramatically reduces image-file fanout in cold/warm paths (`avg_files` ~`5` vs `1025`), but currently this does not translate into end-to-end gains for sparse writes or cold-start reads.
- Sparse-path metadata pressure remains high in both layouts because WAL is still per-regular-chunk in the current prototype.

## Metrics Collected

- Throughput and latency:
  - `ops/s`, `p50_us`, `p95_us`, `p99_us`
- Storage metadata pressure:
  - file count
  - directory count
  - bytes on chunk-image files (`.chk` and `.rgn`)
  - bytes on WAL files (`.wal`)
- Recovery cost:
  - forced-stop followed by `recovery_probe` timing
- Resource usage (best effort):
  - CPU user/system seconds
  - peak RSS bytes

## Decision Gate

Proceed to a full region-backend implementation only if all are true:

1. `>= 30%` gain in `sparse_world_writes`
2. no `> 10%` regression in dense/hot paths
3. no new correctness failures

Dense/hot paths for this benchmark snapshot are evaluated from:

- `dense_world_writes`
- `warm_cache_reads`
- `mixed_rw_70_30`

## Current Recommendation

`NO-GO` for full region-backend implementation at this time.

Gate evaluation against the measured snapshot:

1. `>= 30%` gain in `sparse_world_writes`: **FAILED** (`-51.38%` aggregate)
2. no `> 10%` regression in dense/hot paths: **FAILED** (`mixed_rw_70_30` aggregate `-25.73%`)
3. no new correctness failures: **PASSED** in local validation (`quick.sh` smoke gate for production path with experimental OFF, plus opt-in experimental build/test path including `storage_layout_region`)

Conclusion:

- Keep `fs_split_v1` as the only production layout.
- Keep `fs_region_v1` benchmark-only and experimental.
- Revisit region backend only after a new prototype addresses:
  - region read/modify/write overhead in cold/sparse paths,
  - WAL co-location strategy (region-aware WAL) to reduce per-chunk WAL pressure.

Platform status for this benchmark snapshot:

- This committed measured snapshot is from Darwin/arm64.
- Linux/Windows A/B runs are not yet included in repository history and should be added in a follow-up benchmark pass.

Planned follow-up commands:

- Linux: `./scripts/bench/layout_ab.sh`
- Windows (PowerShell): `bash scripts/bench/layout_ab.sh` (MSYS2/Git-Bash environment)
