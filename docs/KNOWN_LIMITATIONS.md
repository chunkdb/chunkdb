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
- chunk versions are persisted revisions (format v2): they survive eviction
  and restart and change only on content mutations. A chunk whose artifacts
  are all 1.x keeps the 1.x behavior (a fresh token per load) until its first
  mutation or checkpoint under 2.x
- `CHUNKSCAN` is not a global snapshot: each chunk's populated state is
  evaluated per chunk at scan time
- `CHUNKSCAN` has no persistent index: each page lists the top-level
  `L_<lx>_<ly>` entries and then only the large-chunk columns it needs
  (bounded memory, ascending order, no failure cap). A page therefore costs
  O(large chunks) plus the files of the visited columns, not a walk of every
  chunk in the world; a column holding many chunks is still listed whole
- `CHUNKSCAN` additionally merges **every** resident cached chunk into each
  pass's candidate set (`ScanPopulatedChunks` in `src/world_read.cpp`), with no
  cursor or column restriction, so a page also costs O(resident chunks) on top
  of the disk walk above. With a large `max_loaded_chunks` a warm cache makes a
  page measurably slower than a cold one
- `MSET` is not atomic across its items: items apply strictly in order as
  independent per-block writes, and a mid-command failure leaves the earlier
  items applied (each individual item is still all-or-nothing). Use
  `CHUNKBATCH` for an atomic multi-block update within one chunk
- `CHUNKBATCH`/`CHUNKCAS` (and full-chunk `CHUNKSET`/`CHUNKSETBIN`) are
  atomic across crash recovery for every geometry: one mutation is one WAL
  frame, applied entirely or not at all
- chunk version tokens are backed by a persisted monotonic clock, so the
  no-stale-match guarantee is deterministic on a read-write store. Read-only
  stores (which reject conditional mutations) report persisted revisions for
  migrated chunks and non-persistent random tokens for legacy chunks

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
- Windows native core path: supported
- Windows native TLS: supported for MSYS2 MinGW64 with MSYS2 OpenSSL; MSVC
  and other OpenSSL distributions are untested

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
  [DURABILITY_CONTRACT.md](DURABILITY_CONTRACT.md)). One bracket writes the
  16-byte generation record twice and costs three durable syncs — the odd
  record's file sync plus its directory sync, then the even record's file sync
  — in every durability mode, including `relaxed`, where the chunk's own data
  is deliberately *not* synced. On macOS the two file syncs are `F_FULLFSYNC`.
  On real durable media this bracket, not the WAL write, dominates sparse-write
  cost. Concurrent writers share one bracket; a single-threaded writer pays a
  whole bracket per flush.

**The comparable number is the cost of one eviction, not a sparse ops/s
average.** A throughput average over a sparse workload depends on how many of
its writes actually miss the cache, i.e. on the ratio between
`max_loaded_chunks` and the number of distinct chunks touched — so the same
engine yields ~355 ops/s, ~626 ops/s or ~60–115 ops/s depending only on how the
scenario was shaped. Normalized per eviction, all of those collapse onto one
figure.

Measured 2026-09-05 with `chunkdb_large_world_bench` on macOS 26.6 / APFS on an
internal SSD (Apple M1 Pro, arm64), `F_FULLFSYNC` confirmed honored by that
filesystem, default geometry, `relaxed`, `max_loaded_chunks=16384`,
`wal_group_commit_updates=8`, `checkpoint_update_interval=256`,
`background_maintenance=off`, cache pre-filled so that **every** measured write
evicts, 5 repeats per row:

| writers | new-chunk ops/s | ms per eviction | evictions per write |
| ---: | ---: | ---: | ---: |
| 1 | 84 ± 8 | **11.8 ± 1.2** | 1.02 |
| 4 | 136 ± 7 | 6.01 ± 0.31 | 1.23 |
| 8 | 396 ± 13 | 1.70 ± 0.08 | 1.49 |
| 16 | 1273 ± 699 | 0.63 ± 0.37 | 1.69 |
| 32 | 3503 ± 865 | 0.149 ± 0.058 | 2.07 |
| 64 | 2789 ± 139 | 0.172 ± 0.009 | 2.09 |

A single-writer eviction costs ~11.8 ms against a measured median `F_FULLFSYNC`
of ~3.9 ms on the same filesystem — almost exactly the three durable syncs of
the bracket described above, with the WAL write itself in the noise. Adding
writers lets them share one bracket, which is where the ~68x drop in
per-eviction cost between 1 and 32 writers comes from; the knee is at 32.

Two caveats on that table. The ops/s column also improves because the eviction
pass overshoots more as writers are added (evictions per write climbs from 1.02
to 2.09, and at 32–64 writers the cache ends nearly empty), so it flatters
concurrency; `ms per eviction` is the honest comparison. And the 16-writer row
has a very wide spread — that configuration is bimodal on this host.

Cross-check on the same host and session: `chunkdb_bench --ops 20000` reports
`sparse_world_writes ops_s=358.69` with `evictions=5125` (`evictions_per_op=0.26`),
i.e. `55.76 s / 5125 = 10.9 ms` per eviction — the same physical cost, and the
historical "~355 ops/s" figure reproducing exactly. `hot_chunk_writes` (single
chunk, no flush per op) ran at 47341 ops/s in the same run.

Raw data, full profile and host metadata:
[bench/artifacts/manual-runs/large-world-sparse-set-20260905-macos-metadata.txt](../bench/artifacts/manual-runs/large-world-sparse-set-20260905-macos-metadata.txt)
and the `-summary.txt` / `-5x.csv` / `-threads-5x.csv` files next to it.
Reproduce with:

```bash
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release
cmake --build build-bench --target chunkdb_large_world_bench
./build-bench/chunkdb_large_world_bench --cache 16384 --chunks 5000 --threads 1 \
    --repeats 5 --durability relaxed --data-dir "$TMPDIR/chunkdb-lw"
```

Platform caveat: every figure above is macOS/APFS. **Linux/ext4 has not been
measured yet** — the durable sync there is `fdatasync`, not `F_FULLFSYNC`, so
both the absolute cost and the shape of the writer-scaling curve are expected to
differ. Windows has not been measured with this benchmark either. On storage
where sync is cheap (e.g. `tmpfs`) sparse throughput is one to two orders of
magnitude higher, confirming the cost is sync-bound rather than CPU-bound.

This is a property of the file-per-chunk layout plus the per-flush durable
reader-coordination bracket, not a discrete bug. The structural improvements
are the `fs_region_v1` backend (many chunks packed per region file → far fewer
files and syscalls), currently experimental, and a populated-chunk
index/manifest (future work).

Guidance until then: size `max_loaded_chunks` to keep the hot working set
resident (avoid steady-state eviction), prefer denser coordinate locality where
possible, use more writer concurrency to amortize brackets, and evaluate
`fs_region_v1` for sparse/large-world use cases.

When sizing `max_loaded_chunks`, budget the memory too: on the measured host a
resident chunk costs the process about **5.9 kB of RSS** for 544 B of chunk
state (default geometry: 512 B payload + 32 B presence), so
`max_loaded_chunks=16384` is roughly **92 MiB** of resident set on top of the
rest of the process. That figure is the current resident size with the cache
exactly full, not `getrusage(ru_maxrss)`; measuring current rather than peak RSS
did **not** lower it, so it is a real steady-state cost and not a
peak-measurement artifact. It does include heap that was freed but not returned
to the OS, so treat it as the process-level cost to plan capacity with, not as
the size of the per-chunk data structures.

## Packaging / Supply Chain

- archive packaging + SHA256 checksums are provided
- SBOM automation is not currently provided
