# Extensible Storage Format Design (chunkdb 2.0, issue #40)

Status: **implemented** (decisions in §12). The normative description is
`docs/STORAGE_FORMAT.md`; measurements against the budget (§10) are in
`docs/PERFORMANCE.md`. Once implemented, the normative description moves to
`docs/STORAGE_FORMAT.md`; this document keeps the reasoning.

## 1. Goal

Every on-disk structure is a fixed layout today: a `.chk` image is a fixed
header plus exactly `payload_bytes + presence_bytes`, and a WAL v4 frame can
only overwrite bytes inside that fixed state. Per-block extra data (#44) and
block history (#45) do not fit, and neither will later features.

2.0 replaces the image and WAL layouts once, before release, so that later
features add **sections, frame fields and record types behind feature flags**
instead of new incompatible versions. The data-directory manifest (#38) gains
the flags and becomes the table manifest that #41 will place per table.

Out of scope: reading pre-2.0 artifacts (offline conversion is #43), tables
(#41), protocol 2 (#42), the extra-data and history features themselves.

## 2. Compatibility cut

- The manifest version is the gate. This build writes manifest version 2 and
  opens only version 2. A version-1 manifest (written between #38 and this
  change) and a directory without a manifest are refused before any file is
  read, so the engine never meets an older image or WAL.
- The engine therefore keeps exactly one image layout and one WAL layout.
  Every pre-2.0 read path leaves the engine and `chunkdb_verify` in this change:
  `.chk` v1–v5, WAL v2–v4 (`DLT1` records, `FRM1` frames), the mid-stream
  header switch (`wal_needs_v4_header`, `wal_v4_header_offset`), and
  revision-zero legacy chunks. The readers move into a separate module that
  the engine does not call, with their tests, for `chunkdb_migrate` (#43; D1).
- New magics make a stray old file fail with a clear "not a 2.0 image/WAL"
  error instead of a field mismatch.

## 3. Feature flags

Three `u32` sets, as in ext4:

| Set | A reader that does not know a set bit |
|---|---|
| `incompat` | must not open the store at all |
| `ro_compat` | may open it read-only; must not write |
| `compat` | may ignore it |

- The **manifest** records the union of the features the store uses. Opening
  checks it before any chunk is touched: an unknown `incompat` bit refuses
  every open; an unknown `ro_compat` bit refuses a read-write open and allows a
  read-only one; unknown `compat` bits are ignored. The server always opens
  read-write, so it refuses an unknown `ro_compat` bit.
- **Images and WAL headers** carry the flags of the features that file uses.
  They must be a subset of the manifest's flags; anything else is corruption
  (a file copied from another store, or a writer bug).
- A feature that adds a section type, a manifest option, a frame field or a
  record type owns a flag bit, and its data never changes the meaning of the
  structures older readers know. Hence the rule for an **unknown type**: when
  the containing file (or, for options, the manifest) carries a feature bit
  this reader does not know, the entry is skipped after its size and CRC
  checks; otherwise it is corruption. (With an unknown `incompat` bit the
  store is refused anyway, with that reason.) A reader skipping data is
  read-only by the `ro_compat` rule, so it never drops that data by rewriting
  the file. A `compat` feature must therefore be one whose data may be lost
  when an older writer rewrites a file (hints, caches).
- 2.0.0 defines `extra-data` (#44, `ro_compat` bit 0; `docs/STORAGE_FORMAT.md` Section 1.3), and tests inject unknown bits. Planned: `history` (#45, `ro_compat`).

## 4. Manifest (version 2)

```text
magic[4]          "CKMF"
version     u16   2
reserved    u16   0
incompat    u32
ro_compat   u32
compat      u32
large_chunk_width, large_chunk_height,
chunk_width, chunk_height, block_bits      u32 each
store_id[16]      random, not all zero
options_size u32  bytes of the options area
options           TLV entries: type u16, length u16, value[length]
crc32       u32   over every preceding byte
```

- 2.0.0 defines no option types; `options_size` is 0. Tables (#41) add the
  per-table options (durability mode, checkpoint thresholds, compression,
  history settings) here without a new layout.
- Size bound: at most 64 KiB, checked before parsing.
- Written exactly as in #38 (synced temp file, no-replace publish, directory
  sync); never rewritten in 2.0.0. Rewriting it for `TABLESET` is #41's job.

## 5. Chunk image

```text
magic[8]          "CHKIMAGE"
version     u16   1
section_count u16
incompat, ro_compat, compat               u32 each
store_id[16]
chunk_x     i64
chunk_y     i64
revision    u64   chunk revision after the last captured mutation, >= 1
commit_time_ms u64  commit time of that mutation
directory   section_count entries of
              type        u16
              flags       u16   bit 0 = zrle
              stored_size u32   bytes in the file
              raw_size    u32   bytes after decompression
              crc32       u32   over the raw bytes
header_crc32 u32  over magic .. end of directory
bodies            section bytes, in directory order, contiguous
```

- Section types in 2.0.0: `1 PAYLOAD` (raw size = payload bytes),
  `2 PRESENCE` (raw size = presence bytes). Both are required.
- Directory rules: types strictly ascending (sorted, no duplicates), no
  unknown type, no unknown section flag, `stored_size == raw_size` when not
  compressed, the bodies fill the file exactly. Raw sizes of known types are
  fixed by the manifest geometry.
- Compression is per section (`zrle` in section flags) instead of an image
  version. The CRC is over raw bytes, so a corrupt compressed body is caught by
  the bounded decoder or by the checksum of its output, as today.
- Geometry leaves the per-file header: it belongs to the manifest. The store id
  ties the file to its store; a mismatch fails the load.
- The old `write_timestamp_ms` (checkpoint wall time) is replaced by
  `commit_time_ms` of the captured state.
- Fixed overhead: 108 bytes at two sections, against 64 today (+44 bytes per
  image; 544-byte state at the default geometry). One image still fits one
  4 KiB filesystem block, so allocated size does not change.

## 6. WAL

### 6.1 File header

```text
magic[8]          "CHKWALOG"
version     u16   1
reserved    u16   0
incompat, ro_compat, compat               u32 each
store_id[16]
chunk_x     i64
chunk_y     i64
header_crc32 u32  over the preceding 56 bytes
```

60 bytes, against 36 today. The header gains a CRC (today it has none). A WAL
whose header is missing, damaged, or names another store or chunk is not
replayed, and the load fails (it is never treated as empty).

Today's replay also accepts a stream with no header at all. Its comment
blames a writer re-creating the file during a replacement race; the only test
covers a headerless 1.x stream. Before that tolerance is dropped, stage 4
proves that every append path writes the header into a new file, so a
headerless WAL can only mean damage. Finding: the headerless streams came
from the loader itself, which marked a 0-byte WAL (a crash right after the
file was created) as having its header, so the next append wrote frames
without one. Stage 4 replaces such a file instead. Every other path sets
"header written" only after writing it or after replaying a valid header and
clears it whenever the file is removed or truncated below the header; the
eviction and concurrency stress suites, which race checkpoints against
appends, now fail loudly on any headerless WAL.

Stage 4 also fixes a defect in today's read-write loader: a WAL that is not
replayable (for example a damaged header) is only logged as "WAL skipped",
later appends go after it, every restart skips them again, and the next
checkpoint deletes the file, so acknowledged writes are lost. The loader must
fail the chunk load instead. The one benign case, a WAL shorter than its
header that is a prefix of the expected header (a crash while the file was
being created, before any frame), is truncated and rewritten, never appended
to.

### 6.2 Frame

```text
magic[4]          "FRM2"
revision    u64
commit_time_ms u64
frame_flags u16   must be 0 in 2.0.0
tlv_size    u16   bytes of the TLV area
record_count u32  >= 1
body_size   u32   bytes of the records
tlv               entries: type u16, length u16, value[length]
header_crc32 u32  over revision .. end of tlv
records           record_count entries of
                    type u8
                    size u32   bytes of the body
                    body[size]
frame_crc32 u32   over all record bytes
```

- **TLV types in 2.0.0:** `1 TAG`, opaque bytes (1..65535). It is defined from
  2.0.0 on, so it needs no flag. Writing tags is #42/#45's API; this change
  defines, writes (tests) and validates them.
- **Record types in 2.0.0:** `1 SPAN`, body = `byte_offset u32` + bytes to
  overwrite at `state[byte_offset, byte_offset + size - 4)`. `size` is `u32`,
  so a full 64 MiB state is one record; the 65535-byte split goes away. The
  existing shape rule stays: a span lies wholly in the payload, wholly in the
  presence bitmap, or covers the full state.
- **One CRC for the records.** The frame CRC covers every record byte (types,
  sizes, offsets, bodies), and a frame is applied entirely or not at all, so a
  per-record CRC adds nothing; v4 has both (D2).
- Validation before applying anything: header CRC, `frame_flags == 0`, known
  TLV types with in-bounds lengths and no duplicate `TAG`, the whole frame
  present, frame CRC, every record's type, size and shape, records exactly
  filling `body_size`. A frame failing any check is not applied; a torn final
  frame is ignored as a whole and an invalid interior frame stops replay,
  exactly as today.
- Per-mutation overhead for a one-block `SET` at the default geometry: frame
  36 + payload record 9 + presence record 9 + trailer 4 = 58 bytes plus the
  changed bytes, against 22 + 10 + 10 + 4 = 46 today (+12 bytes).

### 6.3 Commit time

`commit_time_ms` is the server's wall clock in Unix milliseconds when the
mutation commits, clamped so it never goes backwards within one store
instance or below the time already recorded for the chunk. Across a restart
whose wall clock went backwards it can decrease; #45 needs table-wide
monotonic time and will persist the last issued time next to the version
clock. One frame carries one time for all its records.

## 7. Engine changes

- `ParseChunkImage` returns the sections as a map by type plus header fields;
  the loader requires `PAYLOAD` and `PRESENCE`. `SerializeChunkImage` writes
  them and compresses each section when `zrle` is configured.
- `ReplayWal` takes a context (geometry, store id, known flag masks) and
  dispatches records by type; `WalFrameBuilder` gains `SetCommitTime`,
  `AddTag` and typed `AppendSpan`.
- Loaders check store id and file flags; legacy branches go (§2).
- Conditional intents keep recording a WAL byte boundary; the snapshot
  generation, version clock and read-only collection protocols do not change.
- `RegularChunk` keeps the last commit time next to the revision.

## 8. `chunkdb_verify`

Checks the manifest flags (unknown bits reported by set and bit number), each
image's header CRC, directory rules, section CRCs and store id, and each WAL's
header CRC, store id, frames, TLV and record types. The `legacy_*` counters
are removed from the summary line.

## 9. Tests

- Flags: unknown `incompat` refuses read-write and read-only opens; unknown
  `ro_compat` refuses read-write (and the server) but opens read-only and
  reads data; unknown `compat` is ignored; image or WAL flags outside the
  manifest's are rejected.
- Image: round trip with and without `zrle` gives the same state; rejected:
  header CRC, section CRC, unsorted or duplicate types, unknown type or flag,
  wrong raw size, stored/raw mismatch, bodies shorter or longer than the
  directory, missing `PRESENCE`, wrong store id, zero revision.
- WAL: header CRC and store id; frames with `TAG` and several records replay;
  a torn frame with TLV and several records is ignored as a whole at every
  cut point; flipped bytes in header, TLV, record type, size, offset and body
  are each rejected; unknown TLV or record type stops replay.
- Old artifacts: manifest v1 and old `.chk`/`.wal` magics are refused with
  their own messages.
- Existing crash, recovery, eviction, stress, read-only and world-ops suites
  run unchanged on the new format.

## 10. Performance budget

Measured with the existing benchmarks against `5fcd153`, Release builds, same
host, 5 repetitions, medians:

- `chunkdb_bench --ops 20000` (hot chunk and sparse writes) and
  `chunkdb_large_world_bench` sparse writes: throughput within 5%.
- Logical bytes: image +44 bytes, WAL +12 bytes per one-block mutation and
  +24 bytes per file header. Allocated bytes per chunk unchanged at the
  default geometry.

Results are recorded under `bench/artifacts/manual-runs/` and summarized in
`docs/PERFORMANCE.md`.

## 11. Implementation stages

Each stage builds with `-Werror` and passes the full suite before the next.

1. Manifest version 2 with flags and options area; flag checks at open;
   version 1 refused.
2. 1.x-only read paths out of the engine and verifier (images v1–v3, WAL
   v2/v3 records, the mid-stream header switch, revision-zero chunks, the
   intermediate clock record). The legacy module receives a frozen copy of
   today's readers, which read every pre-2.0 artifact (images v1–v5, WAL
   v2–v4); stages 3 and 4 then drop v4/v5 and WAL v4 from the engine.
3. New image layout.
4. New WAL layout, typed records, commit time, `TAG`.
5. Verifier, `STORAGE_FORMAT.md` rewrite of §1, §3, §4, benchmarks and budget.

## 12. Decisions

- **D1. Pre-2.0 readers** move into a module the engine does not call, with
  their tests, so `chunkdb_migrate` (#43) reuses tested code.
- **D2. No per-record CRC.** The frame CRC covers every record byte.
- **D3. Budget** as in §10.
