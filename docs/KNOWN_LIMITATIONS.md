# Known Limitations

This list is intentionally explicit. It documents what remains out of scope or
unguaranteed after the stable `v1.0.0` cut; see [COMPATIBILITY.md](COMPATIBILITY.md)
for the stable surface itself.

## Durability / Recovery

- durability is mode-dependent (`relaxed`, `fsync-wal`, `fsync-checkpoint`)
- no cross-chunk atomic transaction guarantee
- no replication/distributed durability
- writable operation on Windows requires directory-sync capability for the
  durable snapshot-generation record in every durability mode; strict modes
  also require it for data artifacts. If unavailable, opening/writing fails
  instead of silently degrading the ABA-safety or durability guarantee
- **WAL delta record header integrity is partial.** A WAL delta record's CRC32
  covers only the record body, not its `byte_offset`/`data_size` header
  fields (`.wal` format v3; see `docs/STORAGE_FORMAT.md`). A media/bit-level
  corruption of `byte_offset` that keeps the record in range and does not
  cross the payload/presence region boundary can therefore apply a
  CRC-valid body at the wrong location without detection. Replay rejects
  region-straddling records as a partial mitigation, and `data_size`
  corruption is caught by the body CRC, but in-region offset corruption is
  not. Closing this fully requires extending the record CRC to cover the
  header, which changes the on-disk record checksum and so is deferred to the
  next `.wal` format version (a compatibility break tracked for a future
  major release). Mitigate operationally with `fsync`-based durability modes
  and healthy storage; `chunkdb_verify` replays WALs and reports records it
  rejects.

## Runtime / Process Model

- single-writer / multi-reader process model (default)
- shared multi-writer on one data directory is unsupported
- lock metadata stale-takeover logic is conservative but not a distributed lease protocol
- read-only chunk loads are coherent per chunk, not a store-wide snapshot:
  each first load independently requires one unchanged even
  `chunkdb.snapshot` generation around its image/WAL/intent collection and may
  return an older coherent chunk between writer transitions. Eight failed
  stability attempts return a bounded error for that chunk; other chunks in
  the same read-only process remain usable when the global generation is
  stable. A crashed writer leaves the global generation odd, so all uncached
  read-only chunk loads fail closed until writer recovery completes
- overlapping on-disk transitions share one global odd snapshot epoch, so an
  uncached read-only load may retry because an unrelated chunk is changing.
  The first overlapping transition and last finisher add durable odd/even
  metadata publications, including in `relaxed` mode; these metadata syncs do
  not make relaxed WAL contents durable

## Protocol / API

- the stable `1.x` protocol surface is documented in [PROTOCOL.md](PROTOCOL.md)
  and governed by [COMPATIBILITY.md](COMPATIBILITY.md)
- conditional mutations (`CHUNKCAS`) and atomic batches (`CHUNKBATCH`) are
  limited to a single chunk; there are no cross-chunk transactions
- chunk versions are opaque tokens invalidated by chunk reload (eviction or
  restart); they are not persistent revision counters
- `CHUNKSCAN` is not a global snapshot: each chunk's populated state is
  evaluated per chunk at scan time
- `CHUNKSCAN` pagination has no persistent index yet: every page re-walks the
  data directory (bounded memory, ascending order, no failure cap), so a page
  over an N-chunk world costs O(N) directory traversal; a populated-chunk
  manifest is future work
- `MSET` is not atomic across its items: items apply strictly in order as
  independent per-block writes, and a mid-command failure leaves the earlier
  items applied (each individual item is still all-or-nothing). Use
  `CHUNKBATCH` for an atomic multi-block update within one chunk
- `CHUNKBATCH`/`CHUNKCAS` are atomic (including across crash recovery) only
  when the full chunk state fits in one WAL record (65535 bytes). On larger
  geometries these commands are rejected with `INVALID_ARGUMENT` before any
  mutation rather than accepting non-atomic prefix replay; the default
  geometry is far below this bound
- chunk version tokens are backed by a persisted monotonic clock, so the
  no-stale-match guarantee is deterministic on a read-write store. Read-only
  stores (which reject conditional mutations) issue non-persistent random
  tokens instead

## Observability / Tooling

- runtime metrics are exposed in Prometheus text format through the
  authenticated `METRICS` protocol command; there is no native HTTP scrape
  endpoint, so scraping requires a small adapter that issues `METRICS`
- `chunkdb_verify` is a read-only integrity checker; there is no consistent
  online snapshot/backup facility yet (back up offline, or use `WALFLUSH`
  followed by a filesystem-level copy while writes are quiesced externally)

## Platform Support Boundaries

- Linux native: supported
- macOS native: supported
- Windows native (core non-TLS path): supported
- Windows native TLS: **not part of stable support claims** (tracked in #6)

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
- Every WAL flush is additionally bracketed by the durable snapshot-generation
  publication that coordinates read-only readers (see
  `docs/DURABILITY_CONTRACT.md`). The odd-generation record is synced before
  the flush in every durability mode, including `relaxed`; on real durable
  media this per-flush sync dominates sparse-write cost (on macOS it is an
  `F_FULLFSYNC`). Concurrent writers amortize overlapping brackets; a
  single-threaded writer pays one bracket per flush.

Measured on macOS/APFS (arm64, real device-backed storage; direct
single-threaded API, default geometry, `max_loaded_chunks=16384`, 20k ops over
a ±200k coordinate space, `relaxed` durability): ~355 sparse SET ops/s with
~5.1k evictions/eviction WAL flushes per 20k ops; single-chunk
`hot_chunk_writes` (no flush per op) run at ~50k ops/s on the same setup. On
storage where sync is cheap (e.g. `tmpfs`), sparse throughput is one to two
orders of magnitude higher, confirming the cost is sync-bound rather than
CPU-bound. Figures vary with the fsync latency of the underlying device and
filesystem; treat them as an order-of-magnitude guide, not a benchmark claim.

This is a property of the file-per-chunk layout plus the per-flush durable
reader-coordination bracket, not a discrete bug. The structural improvements
are the `fs_region_v1` backend (many chunks packed per region file → far fewer
files and syscalls), currently experimental, and a populated-chunk
index/manifest (future work).

Guidance until then: size `max_loaded_chunks` to keep the hot working set
resident (avoid steady-state eviction), prefer denser coordinate locality where
possible, use more writer concurrency to amortize brackets, and evaluate
`fs_region_v1` for sparse/large-world use cases.

## Packaging / Supply Chain

- archive packaging + SHA256 checksums are provided
- SBOM automation is not currently provided
