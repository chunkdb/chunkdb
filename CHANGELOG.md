# Changelog

All notable changes to this project will be documented in this file.

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

`chunk` does not claim broad performance superiority over PostgreSQL/Redis.
Comparisons are only valid in narrowly defined, reproducible chunk/grid workloads with matched durability settings.
