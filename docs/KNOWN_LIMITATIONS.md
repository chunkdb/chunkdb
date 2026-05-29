# Known Limitations

This list is intentionally explicit for release-preview readiness.

## Durability / Recovery

- durability is mode-dependent (`relaxed`, `fsync-wal`, `fsync-checkpoint`)
- no cross-chunk atomic transaction guarantee
- no replication/distributed durability
- on Windows, strict durability modes require directory-sync capability; if it is unavailable,
  strict writes fail instead of silently degrading to a weaker guarantee

## Runtime / Process Model

- single-writer / multi-reader process model (default)
- shared multi-writer on one data directory is unsupported
- lock metadata stale-takeover logic is conservative but not a distributed lease protocol

## Protocol / API

- protocol is alpha and may evolve before a stable compatibility policy
- no protocol-level transaction primitives

## Platform Support Boundaries

- Linux native: supported
- macOS native: supported
- Windows native (core non-TLS path): supported
- Windows native TLS: **not yet guaranteed** as fully supported for release-preview claims

## Performance — sparse write workloads

The `fs_split_v1` backend stores one file per regular chunk (plus a `.wal` per
dirty chunk). Under **sparse** workloads — writes scattered across a very large
coordinate space so the working set exceeds `max_loaded_chunks` — this layout
has an inherent cost:

- Each distinct chunk touched needs its large-chunk directory created and, when
  evicted while dirty, its delta flushed to a per-chunk `.wal` (open + write +
  close, plus a `mkdir` for a not-yet-seen directory).
- Once the cache cap is exceeded the eviction path is on the hot loop, so the
  forced per-chunk WAL flush dominates write time.

Measured (direct API, default geometry, `max_loaded_chunks=16384`, 20k ops over
a ±200k coordinate space): ~14–15k sparse SET ops/s with ~5.1k evictions and
~5.0k eviction-driven WAL flushes per 20k ops, with ~10% run-to-run variance.

This is a property of the file-per-chunk layout, not a discrete bug: profiling
shows the time goes to per-chunk filesystem syscalls (`mkdir`/`open`/`write`/
`stat`), and the eviction WAL flush is the cheapest durable way to persist a
dirty chunk that is being evicted. Micro-optimizing the I/O helpers (e.g.
avoiding symlink-resolving path canonicalization) was measured to make no
material difference. The structural improvement is the `fs_region_v1` backend
(many chunks packed per region file → far fewer files and syscalls), which is
currently experimental.

Guidance until then: size `max_loaded_chunks` to keep the hot working set
resident (avoid steady-state eviction), prefer denser coordinate locality where
possible, and evaluate `fs_region_v1` for sparse/large-world use cases.

## Packaging / Supply Chain

- archive packaging + SHA256 checksums are provided
- SBOM automation is not part of the current release-preview scope
