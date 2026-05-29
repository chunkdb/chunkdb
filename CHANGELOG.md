# Changelog

All notable changes to this project will be documented in this file.

Release naming note:
- Starting with `v1.0.0`, `chunkdb` follows [Semantic Versioning](https://semver.org/)
  against the surface defined in `docs/COMPATIBILITY.md`.
- `preview`/`engineering alpha` describe the earlier `v0.1.x` line.

## Unreleased

## v1.0.0 - 2026-05-29

First **stable** release. The stable channel commits to the compatibility and
support boundary documented in `docs/COMPATIBILITY.md`; preview caveats no
longer apply to the surfaces declared stable there.

### Stability / compatibility

- added `docs/COMPATIBILITY.md`: semver policy, stable surfaces (on-disk
  `fs_split_v1` format readability, wire protocol, durability contract, CLI),
  and explicit non-guarantees
- stable support boundary: Linux native, macOS native, Windows native non-TLS
- explicitly **out of stable claims**: experimental `fs_region_v1` backend and
  Windows native TLS (tracked in #6)

### Protocol

- added batch commands `MSET` (multi-block write) and `MGET` (multi-block read)
  returning a `*N` array reply, reducing round-trips on high-latency links

### Reliability / portability fixes

- file-descriptor budget is now fitted per platform at startup: Linux/macOS
  clamp `max_open_wal_streams` to `RLIMIT_NOFILE`; Windows raises and clamps to
  the CRT stdio limit (`_setmaxstdio`/`_getmaxstdio`), fixing `EMFILE` under
  open-heavy workloads; the server also fits `max_pending_clients` to the budget
- bounded startup recovery scan so large worlds do not stall on launch
- constant-time AUTH token comparison; internal errors no longer leak
  filesystem paths to clients
- thread-safe CRC32 table initialization
- fixed signed-shift UB in little-endian decode of high bytes
- async-signal-safe shutdown handling
- correct IPv6 bracketed-address parsing in the connection URI parser
- CLI/clients reject commands containing CR/LF (request-injection guard)

### CI / tests

- all build/test, crash, and stress gates green on Linux, macOS, and Windows
- made Linux-fragile networking tests deterministic (no reliance on OS
  socket-buffer or single-worker scheduling behavior); see
  `docs/CI_PORTABILITY_NOTES.md`

### Optimization (carried from the preview line)

- WAL group-commit controls for relaxed mode (`wal_group_commit_updates`,
  `--wal-group-commit-updates`)
- write hot path: removed per-`SET` payload copies, batched WAL append buffer,
  flush on clean shutdown and before eviction
- parse path: low-allocation `Protocol::ParseLineView`, case-insensitive
  view-based dispatch, `std::from_chars` integer parsing

### Known limitations (after the stable cut)

- single-backend stable (`fs_split_v1`); `fs_region_v1` remains experimental
- no cross-chunk transactions / full ACID; no replication
- sparse-write throughput is bounded by the file-per-chunk layout
  (`docs/KNOWN_LIMITATIONS.md`)

## v0.1.1-preview (engineering alpha) - 2026-03-13

Stabilization update with terminology/positioning polish and stronger validation coverage for the current engine.

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
