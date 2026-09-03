# Storage Format v2 Design (chunkdb 2.0)

Status: **proposal** (2026-09-03). Nothing in this document is implemented.
It fixes the design for the one coordinated on-disk format bump that the
`ISSUES.md` audit recommended (§11, dependency note) so that the three
format-changing items land as a single migration event.

## 1. Scope

Items from `ISSUES.md` that this bump resolves:

| Item | Problem today | Resolved by |
| --- | --- | --- |
| CDB-LIM-1 | chunk version tokens are re-rolled on every load, so `CHUNKCAS`/`CHUNKBATCH` spuriously fail after eviction or restart | persisted per-chunk revision (§3, §4) |
| CDB-DEF-1 | the WAL record CRC covers only the body, so a corrupted `byte_offset` applies a valid body at the wrong place | record CRC over header and body, plus a frame CRC (§4) |
| CDB-LIM-2 | `CHUNKSET` on geometries whose state spans several WAL records is not crash-atomic; `CHUNKCAS`/`CHUNKBATCH` reject such geometries up front | WAL frames: one mutation is one all-or-nothing unit on replay (§4) |

Item explicitly **removed** from the format bump:

- **CDB-CAP-3/4 (populated-chunk manifest).** The quadratic `CHUNKSCAN` cost
  comes from re-walking and re-sorting the whole data directory per page. A
  cursor-aware walk over the `L_<lx>_<ly>` directories (§7) removes the blow-up
  without any new artifact and can ship in `1.x`. A manifest file would be a
  cache of the directory listing that must be kept a superset of reality; it
  adds a durability ordering rule and a rebuild path for no asymptotic gain
  over listing one `L_*` directory. It is not part of v2.

Item explicitly out of scope: CDB-CAP-1 (per-block metadata). Its concept
study (`ISSUES.md` §9) has no settled design; it must not ride this bump.

## 2. Why this is a MAJOR release

`docs/COMPATIBILITY.md` promises that any `1.x` build reads data written by
any other `1.x` build. v2 writes `.chk` images and `.wal` logs that a `1.x`
build rejects (unknown version), so the first write by a 2.0 server ends the
possibility of downgrading that data directory. That is a documented,
intentional break of the on-disk readability surface, hence `chunkdb 2.0.0`.

The wire protocol, CLI, and durability contract do **not** change. Existing
clients (`@chunkdb/client`, `chunkdb-go`, `chunk-cli`) keep working unchanged;
only the documented meaning of `CHUNKVER` tokens becomes stronger (§6).

## 3. `.chk` image v4

Header grows from 48 to 60 bytes. Fields 1–10 are the v2/v3 header unchanged
(`version` = `4` uncompressed, `5` zrle-compressed); two fields are appended:

11. `revision` (`u64`): the chunk revision after the mutation this image
    captures (§6). Zero means "unknown", written only by the migration path
    for chunks that have never been mutated by a 2.x writer.
12. `header_crc32` (`u32`): CRC32 over bytes `[0, 56)` of the header.

Body and `payload_crc32` semantics are unchanged; `4` and `5` mirror `2` and
`3`. Readers accept `1`, `2`, `3`, `4`, `5`. A v1–v3 image loads with
`revision = 0`.

## 4. `.wal` log v4: frames

The 36-byte WAL file header is unchanged except `wal_version` = `4`. The body
is a sequence of **frames**. One frame is one mutation (a `SET`, `UNSET`,
`CHUNKSET`, `MSET` item, `CHUNKCAS`, or `CHUNKBATCH`); relaxed-mode group
commit writes several frames in one flush.

```text
frame_header (22 bytes):
  frame_magic[4]   = "FRM1"
  revision          u64   chunk revision after this mutation
  record_count      u16   number of records in the frame (>= 1)
  body_size         u32   total bytes of all records that follow
  header_crc32      u32   CRC32 over revision || record_count || body_size

record (10-byte header + body), repeated record_count times:
  byte_offset       u32   offset into the chunk state (payload || presence)
  data_size         u16   body length
  record_crc32      u32   CRC32 over byte_offset || data_size || body
  body              data_size bytes

frame_trailer (4 bytes):
  frame_crc32       u32   CRC32 over every record (headers and bodies)
```

Replay rules:

1. Validate the frame header magic and `header_crc32`. A header that fails
   either check is an invalid interior frame: stop replay (as today for an
   invalid record).
2. If fewer than `body_size + 4` bytes follow the header, the frame is torn:
   stop replay and ignore the tail (as today for a torn record). A torn frame
   is exactly a crash inside one mutation's append, which the current format
   can only handle for single-record mutations.
3. Validate `frame_crc32`, then each `record_crc32`; validate that every
   record lies inside the state and does not straddle the payload/presence
   boundary. Any failure stops replay.
4. Apply all records in order and set the chunk revision to `revision`.

Consequences:

- `byte_offset`/`data_size` corruption is detected by `record_crc32`
  (closes CDB-DEF-1). The verifier inherits this through the shared replay.
- A multi-record mutation is applied entirely or not at all (closes
  CDB-LIM-2). `kMaxAtomicChunkStateBytes` (65535, the single-record bound that
  makes `CHUNKCAS`/`CHUNKBATCH` reject large geometries) is retired; a frame
  spans as many records as the state needs, bounded by `body_size` (`u32`).
- Overhead per mutation is 26 bytes (frame header and trailer) on top of the
  records, which themselves shrink by 4 bytes each (no per-record magic).
  For the single-record `SET` that dominates sparse workloads this is a net
  +22 bytes per mutation; the write-path benchmark must record the delta (§9).

The conditional-intent files (`.chunkdb.intents/`) keep their current role.
Frames make a crash *inside* an append safe by themselves, but the intent is
what lets a fully appended frame be rolled back when a later pre-commit step
fails, and what preserves a committed frame across a crash before cleanup.
Nothing in that protocol changes.

## 5. Bookkeeping files

`chunkdb.version`, `chunkdb.snapshot`, `.chunkdb.initialized`, and the intent
records are unchanged. The version clock is still the source of every
revision (§6), so the existing exposure and migration rules keep holding.

## 6. Chunk revision semantics

Today `CHUNKVER` returns a token that `GetOrLoadRegularChunk` re-rolls on every
load, which is why eviction alone changes it. In v2 the token is the
**persisted chunk revision**:

- Every mutation reserves `NextChunkVersion()` exactly as today and stores it
  as the frame's `revision`; the next checkpoint copies the in-memory
  revision into the image header. The clock stays global and strictly
  monotonic, so revisions are unique across chunks and never reused, and the
  stale-token guarantee from `docs/PROTOCOL.md` is preserved.
- Loading a chunk reads the revision from the image and the last valid frame;
  it does **not** reserve a token. Eviction and restart no longer change the
  version, and cold read-triggered loads no longer touch the version clock or
  its fsync path (the read-path cost noted in CDB-LIM-1).
- A chunk whose artifacts are all pre-v2 (`revision = 0` after load) is a
  **legacy chunk**: the read-write loader reserves a fresh token for it once,
  exactly as today, and the first checkpoint or mutation persists it. Until
  then that chunk keeps today's reload behavior. `chunkdb_verify` reports how
  many legacy chunks remain (§8).
- Read-only stores return persisted revisions for migrated chunks and keep
  issuing non-persistent tokens for legacy chunks, as documented today.

Documentation changes: `docs/PROTOCOL.md` (`CHUNKVER`, `CHUNKCAS`, `CHUNKBATCH`
lose the "invalidated by reload" caveat), `docs/KNOWN_LIMITATIONS.md` (drop the
reload caveat, the WAL header-CRC limitation, and the 65535-byte atomicity
bound), `docs/STORAGE_FORMAT.md` (§3, §4, §6, §7), `docs/COMPATIBILITY.md`
(2.x surface: reads all `1.x` artifacts, writes v4/v5 images and v4 WALs).

## 7. `CHUNKSCAN` without a manifest (1.x, independent of v2)

Status: implemented in `1.x` (`main` after v1.3.0).

Populated candidates come from three sources: `.chk` files, `.wal` files, and
cached chunks. The fix keeps those sources and changes only how they are
visited:

1. List the top-level `L_<lx>_<ly>` entries once per page, parse the large
   coordinates, and sort them in scan order. This is O(number of large
   chunks), typically 3 orders of magnitude below the chunk count.
2. Skip every large chunk whose coordinate range lies entirely before the
   cursor. Visit the remaining ones in order; inside each, list its files,
   merge the cached chunks that map to it, filter by the cursor, and feed a
   bounded min-heap of size `limit + 1`.
3. Stop as soon as the heap is full and the next large chunk starts after the
   heap's maximum. Only the `limit` winners are read with
   `ReadPopulatedChunkStateNoCache`, as today.

Per-page cost becomes O(L log L + chunks in the visited large chunks) instead
of O(N log N) for the whole world, with the same ordering, cursor, and
per-chunk consistency rules. The duplicate-inflated candidate cap from
CDB-DEF-4 was already replaced by the bounded accumulator in v1.1.0. This can
ship as a `1.x` PATCH/MINOR change and is a prerequisite for measuring v2 on
large worlds, not a part of v2.

## 8. Migration and operations

- **Upgrade:** stop the 1.x server, start 2.0 on the same data directory. All
  `1.x` artifacts are read. Chunks migrate lazily: the first mutation writes a
  v4 WAL frame and the next checkpoint writes a v4/v5 image. No offline tool
  is required. Startup logs the number of legacy chunks it encountered during
  recovery scanning (bounded, as recovery already is).
- **Downgrade:** possible only while no 2.0 write has happened. After that the
  directory is 2.x-only. This is stated in `CHANGELOG.md`, `README.md`, and
  `docs/COMPATIBILITY.md` as the reason for the MAJOR bump.
- **No dual-format mode.** A `--write-format v1` switch would keep every
  defect this bump fixes and double the write-path test matrix. Operators who
  need a staged rollout should take a backup and use `chunkdb_verify` on a
  copy.
- **`chunkdb_verify`:** validates v4/v5 images (both CRCs) and v4 frames via
  the shared replay; counts `legacy_images`, `legacy_wals`, and `legacy_chunks`
  per data directory so an operator can see migration progress.
- **Region backend (`fs_region_v1`, experimental):** slot state has no room
  for a revision. It stays on legacy revision semantics (fresh token per load)
  and is documented as such; it is outside the stable surface, so this is not
  a compatibility event.

## 9. Test and benchmark matrix

Correctness (unit/integration):

- frame torn at every byte boundary of a multi-record frame: replay applies
  nothing from that frame and everything before it;
- `byte_offset` single-bit flip with an intact body is rejected (the CDB-DEF-1
  test that cannot be written today), `data_size` flip rejected, body flip
  rejected, `frame_crc32` flip rejected, `header_crc32` flip rejected;
- `CHUNKSET` on a geometry whose state spans several records survives a crash
  injected between record appends with the pre-mutation state (CDB-LIM-2);
- `CHUNKCAS`/`CHUNKBATCH` accepted on such a geometry and crash-atomic;
- the CDB-LIM-1 transcript as a test: `CHUNKVER`, evict via
  `--max-loaded-chunks 1`, `CHUNKVER` unchanged, `CHUNKBATCH` with the old
  token succeeds; restart variant; read-only store variant;
- legacy stores: v1/v2/v3 images with v2/v3 WALs load, migrate on first
  mutation, and are readable afterwards; a mixed directory (some chunks
  migrated) scans and verifies cleanly;
- `chunkdb_verify` on v4 artifacts and on each corruption above;
- existing crash, kill-recovery, read-only bracket, conditional-intent, and
  WAL group-commit suites pass unchanged (frames are below their abstraction).

Performance gates (recorded in `bench/artifacts/`, same hosts as v1.x runs):

- `chunkdb_bench` sparse and dense write throughput and WAL bytes per
  mutation, v1.3.0 vs 2.0: the +22 bytes per single-record `SET` must be
  visible in bytes and must not move throughput outside the recorded
  real-disk bands;
- cold-load latency of a chunk with a long WAL (frames add one CRC pass);
- `CHUNKSCAN` page latency on 10^5 and 10^6 chunks after §7, constant in page
  index.

## 10. Implementation order

1. `CHUNKSCAN` cursor-aware walk (§7) on `main`, released in `1.x`.
2. Branch `v2`: WAL frames (writer, replay, verifier) with `kWalFileVersion =
   4`; image v4/v5; loader revision semantics; retire the 65535-byte atomic
   bound; docs; tests from §9.
3. Benchmarks and the legacy-store migration matrix.
4. `chunkdb 2.0.0` release; clients need no code change, only a note that
   `CHUNKVER` tokens are stable across reload against 2.x servers.
