# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Optimization Stage (TDD)

- added WAL group commit controls for relaxed mode:
  - new `StoreConfig.wal_group_commit_updates`
  - new CLI flag `--wal-group-commit-updates`
  - new test coverage in `tests/wal_group_commit_tests.cpp`
- optimized write hot path:
  - removed extra per-`SET` payload copies used only for delta serialization
  - replaced temporary WAL record allocations with batched append buffer
  - flushes pending WAL batch on clean shutdown and before eviction
- optimized protocol/engine parse path:
  - added low-allocation `Protocol::ParseLineView`
  - switched command dispatch to case-insensitive view-based parsing
  - switched integer parsing to `std::from_chars`
- benchmark and documentation refresh:
  - added before/after benchmark artifact pair on Apple M1 Pro (32 GB)
  - updated performance docs with measured optimization deltas

## v0.1.1-alpha - 2026-03-13

Stage 3 stabilization alpha with a terminology/positioning polish and stronger validation coverage for the current engine.

### Storage Model and Scope

- reaffirmed project identity as a specialized chunk/grid storage engine
- standardized wording around:
  - chunk-native protocol
  - chunk-oriented access model
  - bit-packed block storage
  - WAL/checkpoint durability modes
- clarified that benchmarks are workload-scoped behavior characterization for `chunkdb`

### Protocol and Runtime Coverage

- server-path benchmark now covers:
  - `PING`
  - `INFO`
  - `SET`
  - `GET`
  - `CHUNK`
  - `CHUNKBIN`
  - mixed read/write (70/30)
- benchmark output includes per-scenario latency percentiles (`p50`, `p95`, `p99`)

### Durability and Recovery Validation

- dedicated stress test combining:
  - concurrent access
  - hot chunk contention
  - forced eviction pressure
  - repeated load/unload cycles
- kill-recovery durability test expanded to both:
  - `fsync-wal`
  - `fsync-checkpoint`
- added WAL recovery edge-case tests:
  - truncated trailing record handling
  - truncated header handling when checkpoint image exists
- added long-run WAL/checkpoint cycle tests for growth/trigger/correctness behavior

### Documentation and Public Presentation

- README/alpha/performance docs rewritten for a clearer self-contained identity
- benchmark framing rewritten to focus on workload-fit and reproducible behavior
- release-facing language polished for a clear alpha-level guarantees/limitations boundary

### Current Boundaries

- no new major feature areas were added in this release
- alpha remains single-backend (`fs_split_v1`) with explicit limitations

## v0.1.0-alpha - 2026-03-13

First public engineering alpha milestone for `chunk`, positioned as a specialized chunk/grid storage engine.

### Included

- core chunk hierarchy and configurable geometry (`block_bits`, chunk sizes, large-chunk sizes)
- `fs_split_v1` backend:
  - large chunk directory
  - regular chunk `.chk` image
  - per-chunk `.wal` delta log
- delta WAL + threshold checkpoint write path
- durability modes:
  - `relaxed`
  - `fsync-wal`
  - `fsync-checkpoint`
- TCP command server with:
  - worker pool
  - buffered request parsing
  - token auth
  - `CHUNKBIN` binary chunk response
- cache limit and eviction
- inter-process `data_dir` lock
- benchmark executables:
  - `chunkdb_bench` (direct storage path)
  - `chunkdb_server_bench` (end-to-end server path)
- test coverage for:
  - protocol/auth/error handling
  - storage and recovery
  - concurrency and eviction
  - process lock behavior
  - durability kill-recovery scenario

### Known Limitations

- only one backend is included in alpha (`fs_split_v1`)
- no distributed features
- no cross-chunk transactions / full ACID semantics
- benchmark scope is narrow and workload-specific

### Positioning Note

`chunk` does not make broad cross-system performance claims.
Any comparative analysis must be scenario-specific, durability-matched, and fully reproducible.
