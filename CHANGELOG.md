# Changelog

All notable changes to this project will be documented in this file.

Release naming note:
- Starting with `v1.0.0`, `chunkdb` follows [Semantic Versioning](https://semver.org/)
  against the surface defined in `docs/COMPATIBILITY.md`.
- `preview`/`engineering alpha` describe the earlier `v0.1.x` line.

## Unreleased

### Protocol (additive)

- `CHUNKSETBIN <cx> <cy> [STATE] <payload_length>`: binary chunk write. The
  request line is followed by exactly `payload_length` raw bytes (the packed
  layout `CHUNKBIN` / `CHUNKBIN STATE` returns) and an empty line, so full
  chunk writes are no longer bounded by the request-line limit and large
  geometries can be written over the protocol. Length mismatches within the
  geometry bound are drained and rejected with `INVALID_ARGUMENT`; unframeable
  requests (malformed header, oversized length, bad terminator, or an
  unauthenticated session) are refused and the connection closed
- `--max-line-bytes` server flag exposes the previously fixed 65536-byte
  request-line limit

### Compatibility

- Windows native TLS is now a stable support claim for the MSYS2 MinGW64
  toolchain with MSYS2 OpenSSL. The `Build and Test TLS (windows-latest)` job
  runs the same `server_integration` TLS cases that back the Linux and macOS
  claims on every change. MSVC and other OpenSSL distributions remain untested
  and unclaimed. Closes #6

### Internal

- CI gains a Windows native TLS gate: an MSYS2 MinGW64 build with
  `CHUNKDB_WITH_TLS=ON` that runs the smoke tests, asserts OpenSSL was
  actually linked, and checks `AUTH` + `PING` over TLS with
  `openssl s_client`. `docs/WINDOWS_NATIVE.md` documents the same check.
  (see Compatibility below)
- the writer-lock heartbeat no longer consumes the generic `ATOMICWRITE`
  failpoints. Its metadata write runs on a background thread every 250 ms and
  could claim a failpoint armed for a concurrent conditional-intent write,
  which made that write succeed and `world_ops_regression` fail its
  `assert(threw)` intermittently under the ASan CI gate
- the Build and Test CI jobs (Linux, macOS, Windows, and both TLS jobs) now
  build with `CHUNKDB_WERROR=ON`; `scripts/test/quick.sh` accepts a
  `CHUNKDB_WERROR` environment variable (default `OFF`). Two GCC
  `-Wrange-loop-construct` diagnostics this exposed in
  `bench/layout_ab_bench.cpp` and `tests/durability_kill_recovery_test.cpp`
  are fixed
- `scripts/release/generate_checksums.sh` writes the bare artifact file name
  into each `.sha256` sidecar regardless of the directory argument, so
  `sha256sum -c` works next to the downloaded file

## v1.2.0 - 2026-09-03

### Security

- the runtime container image no longer ships a default `CHUNKDB_TOKEN`.
  Previously `Dockerfile` set `CHUNKDB_TOKEN=dev-token` in the runtime stage
  and repeated the same credential in the default `CMD` and `HEALTHCHECK`, so
  every image carried a publicly known auth token; because the environment
  variable outranks `--token` during resolution it also silently overrode an
  operator's explicit flag. The server already refuses to start when auth is
  enabled with an empty token, and that guardrail now applies to the image.
  **Breaking for container users:** `CHUNKDB_TOKEN` (or a mounted
  `--token-file`) must be supplied, and `docker compose up` fails fast without
  it. The compose service also publishes on `127.0.0.1` instead of all
  interfaces
- the `HEALTHCHECK` probe no longer authenticates: it sends `PING`, which is
  answered before the auth gate, so it needs no credential
- startup logs which token source was used (never the value) and warns when a
  lower-priority source is shadowed, so an ambient `CHUNKDB_TOKEN` silently
  overriding `--token` is visible
- fixed an out-of-bounds stack write in the server's socket readiness wait.
  `select()`'s `fd_set` is a fixed `FD_SETSIZE`-wide bitmap and `FD_SET`
  performs an unchecked store, so any descriptor at or above that bound wrote
  past the stack object. Descriptors come directly from `accept()` and an
  unauthenticated client could drive them past the bound under the
  file-descriptor limits this project ships, reaching the defect pre-auth via
  the busy-response path. The wait now uses `poll()`/`WSAPoll()`, matching the
  accept loop; this also fixes a latent bug where the `EINTR` retry reused
  `fd_set`s that `select()` had already cleared
- defense in depth: the region slot bound check in `WriteRegionSlotState` runs
  before the heap writes rather than after; conditional-intent filenames are
  validated for component grammar and containment before the reconstructed
  path reaches `resize_file()`/`remove()`; atomic-write temp files are created
  with `O_EXCL | O_NOFOLLOW` (POSIX) and `CREATE_NEW` plus
  `FILE_FLAG_OPEN_REPARSE_POINT` (Windows); token and key material is ignored
  by `.dockerignore` and `.gitignore`

### Fixed

- fixed a data race in the WAL append-stream cache: capacity checks scanned
  other chunks' `wal_stream_initialized` flag and `ofstream` state under the
  cache mutex only, while the owning thread reset them under the chunk mutex
  when a checkpoint or eviction closed the stream. The flag is now atomic and
  the cache no longer inspects another chunk's stream object; caught by the
  TSan gate in `world_ops` (background maintenance)
- fixed a `-Wignored-qualifiers` warning in the socket readiness wait that
  broke `CHUNKDB_WERROR=ON` builds with GCC

### Internal

- `chunk_store.cpp`, `server.cpp`, `chunk_ops.cpp`, and `checkpoint.cpp` were
  split into focused translation units (`chunk_format`, `block_ops`,
  `chunk_cache`, `snapshot_generation`, `server_socket`, `server_io`,
  `server_tls`, `server_connection`, `version_clock`, `conditional_intent`,
  `durability_io`, `atomic_write`). Behavior-preserving: no protocol, on-disk
  format, durability, locking, or public header change, and the existing test
  suite passes unmodified

## v1.1.0 - 2026-07-18

### Correctness / durability hardening (audit remediation)

- ordinary writes (`SET`/`UNSET`/`CHUNKSET`, `MSET` items) now treat the
  successful WAL flush as the commit point: a rejected command fully rolls
  back memory, staged batch records, counters, and the WAL file (a torn or
  unsynced append is truncated back inside the same odd snapshot
  generation), while post-commit inline-checkpoint or generation
  republication failures are logged and retried instead of being returned as
  command errors. An error reply now always means "not applied"; previously
  a `SET` could return `-ERR` while its value became durable and reappeared
  after restart
- version tokens for ordinary writes are reserved before the WAL append (as
  conditional mutations already did), so a version-clock failure is a clean
  pre-write error
- WAL replay adds a structural guard that stops replay when a delta record
  crosses the payload/presence region boundary, a partial mitigation for the
  WAL record header (`byte_offset`/`data_size`) being outside the record CRC.
  This is format-compatible (no version bump; v2/v3 WALs unchanged and
  readable). It does not fully close the gap — an offset flip that stays
  within one region is still undetectable — so the header-CRC gap is recorded
  as a known limitation for the 1.x line and a full fix (extending the record
  CRC to cover the header) is deferred to the next format version
- conditional-intent artifacts moved to the dedicated shallow directory
  `.chunkdb.intents/`, making startup intent recovery proportional to
  pending intents instead of a full recursive data-directory walk;
  `chunkdb_verify` validates the new location and flags misplaced intents
- the snapshot-generation even (stable) record is published without a
  required directory sync: losing it to a crash re-exposes the durable odd
  record and readers fail closed — strictly more conservative — while
  halving a per-flush durable sync; the odd record keeps full durability
- the per-source auth-failure table is hard-bounded (4096 sources,
  least-recently-updated eviction that never evicts an actively banned entry)
  and IPv6 sources are bucketed per /64 prefix while IPv4-mapped/compatible
  IPv6 peers are tracked by their embedded IPv4 address, closing an
  unbounded-memory denial-of-service vector without collapsing all IPv4
  clients into one shared ban
- background maintenance no longer aborts the process on a transient eviction
  error, and a failed eviction WAL flush is a survivable per-operation error
  rather than a permanent store-wide fail-closed state
- `CHUNKSCAN` collection is bounded to the page size with duplicate-free,
  cursor-filtered accumulation: large dirty worlds (chunks with `.chk` +
  `.wal` + cached entries) stay fully enumerable and the previous
  1M-candidate hard failure is gone
- `MSET`'s per-item, non-atomic-across-items semantics are now documented
  (`docs/PROTOCOL.md`), including the applied-prefix behavior on mid-command
  failure; each individual item is all-or-nothing
- documented the rare world-read contention fallback that may cache a probed
  chunk, and corrected the sparse-write performance figures in
  `docs/KNOWN_LIMITATIONS.md` with platform-qualified measurements

### Protocol (additive)

- world-oriented reads: `CHUNKSCAN` (paginated enumeration of populated
  chunks with deterministic ordering and cursor continuation), `CHUNKRANGE`
  (bounded rectangular multi-chunk state read, max 256 chunks, full int64
  corner domain with a 64 MiB response-byte cap), and `CHUNKRADIUS` (bounded
  radius/disc multi-chunk read with the same limits)
- chunk concurrency primitives: `CHUNKVER` (opaque chunk version token),
  `CHUNKCAS` (conditional full-state replace), `CHUNKBATCH` (atomic
  single-chunk block batch); new error code `VERSION_MISMATCH`. Version tokens
  come from a persisted monotonic clock, so a stale version deterministically
  cannot match after eviction or restart. Rejected conditional mutations are
  fully rolled back (memory and WAL) and never reappear after a crash-style
  restart; geometries too large for single-record atomicity are rejected up
  front
- `WALFLUSH`: explicit global durability barrier that makes all previously
  acknowledged writes durable in every durability mode, including `relaxed`
- `METRICS`: Prometheus text-format runtime metrics with bounded per-class
  latency histograms, command/error/auth counters, store gauges, active and
  pending connection gauges, and server-side failure counters for malformed
  requests and admission-control rejections
- `CHUNKBINC`: zrle-compressed binary chunk transfer (opt-in per request)
- `INFO` gains counters for eviction recency skips, empty-chunk GC, WAL
  barriers (and full-sync fallbacks), compressed checkpoint images, background
  maintenance, and the configured checkpoint compression

### Storage / durability

- empty-chunk garbage collection: checkpointing a chunk with no present
  blocks now reclaims its image, WAL, and (when empty) its parent directory
  instead of writing an empty image; observable absent-vs-explicit-zero
  semantics are unchanged
- `fsync-wal` mode now syncs checkpoint images (and directory entries) before
  removing the WAL they replace, closing a window where acknowledged durable
  WAL data could be replaced by an unsynced image
- `WALFLUSH`'s bounded-tracking overflow fallback fails closed on any
  traversal or sync error, syncs files before the directories that reference
  them, and preserves its bookkeeping for a retry after a failure
- a checked persisted `chunkdb.version` clock record backs deterministic chunk
  version tokens; stable-v1 stores migrate safely, an intermediate 8-byte
  ceiling upgrades without reset, and stores with a valid initialized marker
  fail closed if the clock is missing, unreadable, or invalid (see
  `docs/STORAGE_FORMAT.md`)
- conditional WAL rollback intents have explicit rollback and committed
  states, so intent-establishment failures leave live state untouched and
  unlink/directory-sync failures cannot later truncate acknowledged writes
- concurrent read-only chunk loads use a checked durable monotonic
  `chunkdb.snapshot` generation around image/WAL/intent collection. This
  replaces byte-only double collection and rejects cross-process ABA cycles,
  crashed-writer epochs, malformed metadata, and generation wraparound
- optional checkpoint image compression (`--checkpoint-compression zrle`,
  image format v3); v1/v2/v3 images are all readable regardless of the flag,
  and compression stays off by default (see `bench/artifacts/` for recorded
  codec results)
- recency-aware cache eviction: LRU-ordered candidates with a second chance
  for recently accessed chunks, with a guaranteed-progress fallback pass
- opt-in background maintenance (`--background-maintenance`): checkpoints and
  eviction run on a bounded-queue maintenance thread with inline-fallback
  backpressure, drain-on-shutdown, and inline retry of failed background
  checkpoints

### Tooling

- `chunkdb_verify`: read-only data-directory integrity checker with
  machine-usable output and exit codes. It is included in normal
  install/package output, flags misplaced `.wal` files (not just `.chk`), and
  emits paths/details as quoted, escaped tokens so spaces or control
  characters cannot split or forge a field.
- `chunkdb_compression_bench`: reproducible codec size/throughput/latency
  micro-benchmark

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
