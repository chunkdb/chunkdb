# Storage Format Specification (fs_split_v1 backend)

## 1. Hierarchy

Runtime hierarchy:
1. large chunk
2. regular chunk
3. block bitfield

Filesystem mapping:
- `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.chk`
- `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.wal`

Where:
- `(cx, cy)` = regular chunk coordinates
- `(lx, ly)` = large chunk coordinates derived from configured large-chunk dimensions

Bookkeeping artifacts in `data_dir` (not chunk data):
- `.chunkdb.lock/` — single-writer lock and metadata.
- `.chunkdb.initialized` — exactly 16 bytes: magic `CKID`, little-endian
  `u64` value `1`, and little-endian CRC32 over the first 12 bytes. It is
  synced after the first valid version record. Its checked presence is the
  persisted invariant proving that this store has exposed deterministic
  version tokens.
- `chunkdb.version` — exactly 16 bytes: magic `CKVR` (4 bytes), the
  little-endian `u64` exclusive ceiling of the persisted chunk version clock
  (8 bytes), and little-endian CRC32 over the first 12 bytes (4 bytes). The
  ceiling is nonzero. The complete record and its directory entry are synced
  before any token in a newly reserved range is issued.
- `chunkdb.snapshot` — exactly 16 bytes: magic `CKSG` (4 bytes), a
  little-endian `u64` snapshot generation (8 bytes), and little-endian CRC32
  over the first 12 bytes (4 bytes). Even generations identify stable
  image/WAL/intent epochs; odd generations identify a writer transition.
  The odd (transition) record has its file data and directory entry synced
  before any bracketed artifact changes. The even (stable) record has its
  file data synced but not its directory entry: losing the even rename to a
  crash only re-exposes the durable odd record, which fail-closes readers
  until writer recovery — a strictly more conservative outcome. See
  `docs/DURABILITY_CONTRACT.md`.

A stable-v1 store may contain `.chk`, `.wal`, or region data without these
bookkeeping files, because they did not exist in v1.0.0. Read-write
startup migrates that store by syncing a checked clock first and the initialized
marker second; existing data artifacts alone are not evidence that version
tokens were issued. A valid intermediate 8-byte little-endian nonzero ceiling
is upgraded to the checked record without lowering or resetting it.
Missing snapshot-generation metadata is the implicit stable generation zero.
A current read-write startup durably publishes generation one before recovery
can change any artifact and generation two afterward. The generation file is
never removed or reset.

Once a valid initialized marker exists, a missing, unreadable, uninspectable,
truncated, oversized, or invalid clock is bookkeeping damage: the server
refuses to open instead of potentially reissuing an exposed token. Restore the
clock from a consistent backup, or intentionally reinitialize the whole store.
If both version-token bookkeeping files are lost, the remaining state is
indistinguishable from stable-v1 legacy data; startup migrates it as legacy and
cannot deterministically detect prior token exposure. Back up the two files
together with the store. Read-only opening does not issue deterministic
persisted versions. `chunkdb_verify` reports valid legacy and intermediate
stores as migratable without changing them, and reports marker/clock damage as
an error.

During a conditional mutation an exactly 16-byte recovery intent is written
under the dedicated shallow directory `data_dir/.chunkdb.intents/`. The file
name embeds the target WAL's path relative to the data directory with `__`
replacing the directory separator plus the `.rollback` suffix (for example
`L_0_0__C_0_0.wal.rollback`), which is unambiguous for the layout grammar and
lets recovery derive the WAL path from the intent name alone. Keeping every
intent in one shallow directory makes startup intent recovery proportional to
the number of pending intents instead of the total world size.

The record is: magic `CKRB` (rollback) or `CKRC` (committed), little-endian
`u64` pre-command WAL size, and little-endian CRC32 over the first 12 bytes.
`CKRB` is synced before the conditional WAL append. After the WAL is synced,
atomically replacing it with synced `CKRC` is the commit point. Startup
truncates/removes the WAL to the recorded boundary only for `CKRB`; for `CKRC`
it preserves the committed WAL. It then removes and directory-syncs the intent.
This makes an unlink or post-unlink directory-sync failure safe whether the
unlink survives a crash or not.

## 2. Packed Chunk State

Per regular chunk:
- block_count = `chunk_width_blocks * chunk_height_blocks`
- payload_bits = `block_count * block_bits`
- payload_bytes = `ceil(payload_bits / 8)`
- presence_bits = `block_count`
- presence_bytes = `ceil(block_count / 8)`

Payload is tightly bit-packed (no block padding).

Presence bitmap is stored separately:
- bit = `1` means the block is explicitly present
- bit = `0` means the block is unset
- chunk-level presence is derived from this bitmap:
  - any set presence bit => chunk exists
  - all presence bits clear => chunk absent

Combined chunk state bytes:
- `payload_bytes` of packed block payload
- followed by `presence_bytes` of block presence bitmap

Protocol/API mapping:
- `CHUNKBIN <cx> <cy>` returns only `payload_bytes`
- `CHUNKBIN <cx> <cy> STATE` returns the full combined chunk state bytes
- `CHUNK <cx> <cy> STATE` returns the same state as text:
  `<payload_bits>|<presence_bits>`

## 3. `.chk` Data Image Format

All integers are little-endian.

Header (`52` bytes in versions `1`–`3`, `64` bytes in versions `4`–`5`):
1. `magic[8]` = `CHKDATA1`
2. `version` (`u16`) = `4` uncompressed, `5` zrle-compressed (format v2, written by
   chunkdb 2.x); `1`, `2`, `3` are the 1.x layouts, still accepted on read
3. `block_bits` (`u16`)
4. `chunk_width_blocks` (`u32`)
5. `chunk_height_blocks` (`u32`)
6. `chunk_x` (`i64` raw 64-bit)
7. `chunk_y` (`i64` raw 64-bit)
8. `payload_size` (`u32`) = payload bytes only
9. `payload_crc32` (`u32`) = CRC32 of full chunk state bytes in versions `2` and `3` (always over the canonical uncompressed state)
10. `write_timestamp_ms` (`u64`)
11. `revision` (`u64`, versions `4`–`5` only) = the chunk revision after the
    mutation this image captures (Section 4.2); zero means unknown
12. `header_crc32` (`u32`, versions `4`–`5` only) = CRC32 over header bytes
    `[0, 60)`

Body:
- version `4` (and `2`): `payload_size + presence_bytes` bytes of chunk state
- version `5` (and `3`): one `zrle` blob (Section 3.1) whose decompressed content is the `payload_size + presence_bytes` chunk state; written only when the server runs with `--checkpoint-compression zrle`
- version `1` legacy: exactly `payload_size` bytes of packed payload; presence is treated as all-present on read

Readers accept versions `1` through `5` regardless of the configured
compression mode; the flag only selects what new images are written. A
1.x image (`1`–`3`) loads with revision zero, which marks the chunk as not yet
migrated (Section 4.2). Compression is off by default. Region (`.rgn`) files
are never compressed and carry no revision.

### 3.1 `zrle` Codec

`zrle` is a dependency-free zero-run-length codec, also used by the
`CHUNKBINC` wire command:

```text
[codec_id u8 = 0x01][uncompressed_size u32le][token...]
token := 0x00 <uleb128 n>            n zero bytes
       | 0x01 <uleb128 n> <n bytes>  n literal bytes
```

Decoders must know the exact expected output size (from geometry) and must
reject truncated, malformed, or oversized inputs and any input whose declared
or produced size differs from the expected size. Because the image CRC covers
the canonical uncompressed state, corruption in the compressed blob is caught
either by the bounded decoder or by the checksum of its output.

For compression-ratio, throughput, and latency figures on representative
sparse and dense states, run `chunkdb_compression_bench` (fixed seed); see
`bench/artifacts/` for recorded results. Compression stays opt-in because
dense random states do not shrink (ratio ~1.01x) while sparse states shrink
by ~9x.

## 4. `.wal` Delta Log Format

WAL header (`36` bytes):
1. `magic[8]` = `CHKWAL02`
2. `wal_version` (`u16`) = `4` (format v2, frames); `2` and `3` are the 1.x
   record streams, still accepted on read
3. `block_bits` (`u16`)
4. `chunk_width_blocks` (`u32`)
5. `chunk_height_blocks` (`u32`)
6. `chunk_x` (`i64`)
7. `chunk_y` (`i64`)

### 4.1 Version `4`: frames

The body is an append-only sequence of frames. One frame is one mutation
(`SET`, `UNSET`, `CHUNKSET`, `CHUNKSETBIN`, an `MSET` item, `CHUNKCAS`, or
`CHUNKBATCH`); relaxed-mode group commit appends several frames in one flush.

Frame header (`22` bytes):
1. `frame_magic[4]` = `FRM1`
2. `revision` (`u64`) = the chunk revision after this mutation (Section 4.2)
3. `record_count` (`u16`) >= 1; a span longer than 65535 bytes is split into
   several records, so the largest supported geometry (64 MiB payload,
   1048576 blocks) needs at most ~1030 records and the `u16` ceiling is
   unreachable
4. `body_size` (`u32`) = total bytes of the records that follow
5. `header_crc32` (`u32`) = CRC32 over fields 2–4

Then `record_count` records, each:
1. `byte_offset` (`u32`)
2. `data_size` (`u16`) >= 1
3. `body` = `data_size` bytes to overwrite at `state[byte_offset:byte_offset+data_size)`
4. `record_crc32` (`u32`) = CRC32 over `byte_offset || data_size || body`

Frame trailer (`4` bytes):
1. `frame_crc32` (`u32`) = CRC32 over all record bytes of the frame

A record never straddles the payload/presence boundary: a full-chunk replace
logs the payload and the presence bitmap as separate spans, each split into
records of at most 65535 bytes.

Replay validates the header CRC, requires the whole frame (`body_size + 4`
bytes after the header) to be present, validates the frame CRC and every
record's CRC, bounds, and shape, and only then applies the records and adopts
the frame's revision. A frame that fails any check is not applied at all: a
torn frame (crash inside one mutation's append) is ignored as a whole, which
makes every mutation atomic across crash recovery regardless of its size; an
invalid interior frame stops replay. Because the record CRC covers
`byte_offset` and `data_size`, a corrupted offset can no longer relocate a
CRC-valid body (the 1.x header-CRC gap).

A `.wal` that a 1.x writer left in the `2`/`3` layout is appended to by a 2.x
writer only after a fresh version-`4` header is written mid-stream; replay
switches to frames at that header. A headerless stream that starts with
`FRM1` replays as frames, one that starts with `DLT1` as 1.x records.

### 4.2 Chunk revision

The revision is the value `CHUNKVER` reports. Every mutation reserves it from
the store-wide monotonic version clock (`chunkdb.version`) and stores it in
the frame; the next checkpoint copies the in-memory revision into the image
header. Loading a chunk takes the revision from the image and the last valid
frame and reserves nothing, so eviction and restart leave `CHUNKVER`
unchanged. A chunk whose artifacts are all 1.x (revision zero after load) is a
legacy chunk: the read-write loader reserves a fresh token for it once, as 1.x
did on every load, and its first mutation or checkpoint persists a revision.
When a persisted revision is at or above the clock, the clock is raised past
it and a new ceiling is persisted before any further token is issued, so
revisions never repeat even if the clock bookkeeping was lost and restarted.

### 4.3 Versions `2` and `3`: 1.x record streams (read only)

Record header (`14` bytes):
1. `record_magic[4]` = `DLT1`
2. `byte_offset` (`u32`)
3. `data_size` (`u16`)
4. `record_crc32` (`u32`) = CRC32 over the record body only

Record body:
- version `3`: `data_size` bytes to overwrite at `state[byte_offset:byte_offset+data_size)`
- version `2`: `data_size` bytes to overwrite at `payload[byte_offset:byte_offset+data_size)`

Because this record CRC covers only the body, replay of these streams keeps
the 1.x structural guard (a record may not straddle the payload/presence
boundary) as the only protection of the header fields. 2.x never writes this
layout.

## 5. Write Path

For each `SET`:
1. update touched bytes in in-memory payload
2. mark the target block present in the presence bitmap
3. encode delta record(s) for changed payload bytes and/or changed presence bytes into the per-chunk WAL batch buffer
4. flush batch to `.wal` when either:
  - durability mode requires immediate durability (`fsync-wal` / `fsync-checkpoint`), or
  - `pending_updates >= wal_group_commit_updates` (relaxed mode group commit)
5. optionally `fsync` WAL (depends on durability mode); when WAL is first created in synced modes, sync parent directory metadata
6. checkpoint `.chk` when thresholds hit:
  - `checkpoint_update_interval`
  - `checkpoint_wal_bytes`

For each `UNSET`:
1. zero touched bytes in the in-memory payload
2. clear the target block presence bit
3. encode delta record(s) for changed payload bytes and/or changed presence bytes
4. follow the same flush and checkpoint policy as `SET`

For each `CHUNKSET`:
1. replace the full in-memory chunk payload
2. set the full presence bitmap to all-present
3. encode delta record(s) for changed payload bytes and/or changed presence bytes
4. follow the same flush and checkpoint policy as `SET`

For each `CHUNKSET ... STATE`:
1. replace the full in-memory chunk payload
2. replace the full in-memory presence bitmap
3. canonicalize absent blocks so their payload bits are zero
4. encode delta record(s) for changed payload bytes and/or changed presence bytes
5. follow the same flush and checkpoint policy as `SET`

For each `CHUNKCAS` / `CHUNKBATCH`:
1. validate all operations and (when given) the expected chunk version
2. reserve the next version token before any mutation can become visible
3. durably publish a new odd store snapshot generation
4. persist a checked `C_<cx>_<cy>.wal.rollback` intent containing the
   pre-command WAL byte boundary
5. apply the new state in memory and encode the full canonical chunk state as
   one WAL frame (a payload span and a presence span), which makes the
   mutation atomic across crash recovery for every geometry
6. atomically replace and directory-sync `CKRB` with `CKRC`; this is the commit
   point
7. remove and directory-sync `CKRC`, then follow the same checkpoint policy as
   `SET`
8. durably publish the next even snapshot generation once the disk state is
   coherent

Before the commit point, any error restores memory and truncates/removes the
WAL back to the recorded boundary. If that repair cannot complete, the store
stops accepting durability-changing operations; startup consumes the retained
intent before WAL replay and repeats the rollback. After the commit point,
intent-cleanup or inline-checkpoint errors are reported in logs but cannot turn
the committed mutation into a command error. A retained `CKRC` never truncates
later successful writes.

Checkpoint writes full `.chk` atomically and removes `.wal`.

Empty-chunk garbage collection: when a checkpoint runs for a chunk whose
presence bitmap has no set bits, the chunk's `.chk` image is removed instead
of rewritten, the `.wal` is removed, and the parent `L_<lx>_<ly>` directory is
removed opportunistically once empty. In synced modes the data-image removal
is directory-synced before the WAL is removed, then the WAL removal is
directory-synced. Thus every crash boundary retains either the empty-state WAL
or the durably absent image. The data image is removed before the
WAL so a crash between the steps replays the empty-state WAL over an absent
image. An absent chunk and an empty chunk are observably identical; a chunk
whose blocks are explicitly present with all-zero payload is *not* empty and
is never garbage collected. In the experimental region layout the slot is
cleared instead, and the region file is removed once no slots remain.

### 5.1 Checkpoint Atomic Replace Sequence

Checkpoint image replacement uses same-directory temp files and replace semantics:
1. write full checkpoint image to `<target>.tmp.<pid>.<tid>.<clock>.<seq>` in the same directory
2. durability-mode dependent file flush:
   - `fsync-wal` / `fsync-checkpoint`: flush temp file data before replace
     (the checkpoint replaces the WAL, so the image must be durable before
     the WAL is removed in every synced mode)
   - `relaxed`: no required temp-file `fsync` for a genuinely new store before
     its first successful `WALFLUSH`; after a barrier or after reopening an
     initialized store, later checkpoint replacements flush before removing
     WAL state so they cannot downgrade previously durable data
3. close temp file and fail if close reports an error
4. atomically replace target namespace entry with temp file
5. durability-mode dependent directory flush:
   - `fsync-wal` / `fsync-checkpoint`: sync parent directory metadata after replace
   - `relaxed`: for a genuinely new store before its first successful barrier,
     no required directory sync (a later `WALFLUSH` syncs tracked artifacts);
     afterward, and after reopening an initialized store, replacement
     directories are synced to preserve the durability floor

Crash behavior:
- crash before replace: old target remains valid; orphan temp artifacts may remain
- crash after replace but before directory sync: namespace update is atomic, but durability after power loss is not guaranteed unless the mode includes directory sync
- startup/load path removes stale orphan temp artifacts for the target chunk before loading

Additional runtime behavior:
- pending WAL batches are flushed on clean shutdown
- pending WAL batches are flushed before chunk eviction

## 6. Recovery Path

On read-write load:
1. load `.chk` if it exists (or zero chunk state if absent)
2. if `.wal` exists, validate header and replay records in order onto the in-memory chunk state
3. keep recovered state in memory; defer checkpoint compaction to the normal checkpoint/eviction path

On read-only load:
1. read and validate `chunkdb.snapshot`
2. collect the chunk image (or complete region image), WAL, and adjacent
   `.wal.rollback` intent
3. read and validate `chunkdb.snapshot` again; accept only when both
   generations are the same even value, with at most eight attempts
4. for `CKRB`, require the WAL when the recorded boundary is nonzero and replay
   exactly the WAL prefix ending at that boundary; ignore every byte after it
5. for `CKRC` or no intent, replay the complete observed WAL
6. fail the chunk load for malformed generation or intent metadata, a missing required
   WAL, a WAL shorter than the `CKRB` boundary, corruption in the replayed
   bytes, or retry exhaustion
7. do not write checkpoints, truncate/remove WAL or intent files, clean temp
   artifacts, sync directories, or acquire writer ownership

Every WAL flush/truncation, conditional intent sequence, checkpoint/GC
replacement, and startup recovery runs between an odd publication and a
non-repeating even publication. Nested steps of one conditional mutation share
one generation. A crash leaves an odd generation until the next writer
publishes a fresh odd recovery generation and completes recovery; read-only
loads fail closed meanwhile. Generation exhaustion is a startup/write error,
never wraparound. Therefore byte-identical ABA cycles cannot pass the bracket:
each completed rollback or commit changes the generation even when image, WAL,
and absent-intent bytes return to earlier values.

This per-chunk rule allows an older coherent state when its full observation
falls between writer transitions, but not a rejected, in-flight, torn, or
image/WAL-mixed conditional state. It applies to split images and experimental
region images.

A trailing partial frame (e.g. torn append) is ignored as a whole; an
invalid interior frame stops replay. 1.x record streams keep their
per-record rules.

## 7. Validation and Corruption Handling

`.chk` validation checks:
- magic
- version
- geometry fields
- chunk coordinates
- payload size
- header CRC32 (versions `4`–`5`)
- payload CRC32

`.wal` validation checks:
- magic
- version
- geometry fields
- chunk coordinates
- per-frame magic, header CRC32, completeness, and frame CRC32 (version `4`)
- per-record bounds, shape, and CRC32 over header and body (version `4`) or
  body only (versions `2`–`3`)

### 7.1 `chunkdb_verify`

`chunkdb_verify` is a read-only checker: it never modifies the data directory.
It must be told the geometry the store was written with, because a data
directory is not self-describing at that level:

```bash
chunkdb_verify --data-dir ./data \
  --chunk-width 16 --chunk-height 16 --block-bits 16 \
  --large-chunk-width 8 --large-chunk-height 8
```

Each flag defaults to the corresponding server default; `--region-span-chunks`
applies to `fs_region_v1` stores. `chunkdb_verify --help` prints the full list.

Findings are printed one per line as `VERIFY <level> <code> <path> [detail...]`,
where `<level>` is `error`, `warning` or `info` and `<code>` is a stable
machine-readable token. The run ends with a summary line:

```text
SUMMARY checked=<n> warnings=<n> errors=<n> legacy_images=<n> legacy_wals=<n> legacy_chunks=<n>
```

`legacy_images` and `legacy_wals` count artifacts still in a 1.x layout, and
`legacy_chunks` counts the chunks that have at least one such artifact. Exit
code `0` means no findings, `1` means warnings or errors were reported, and `2`
means the run itself failed (bad arguments, unreadable directory).

## 8. Durability Notes

Durability guarantees depend on configured mode (`relaxed`, `fsync-wal`, `fsync-checkpoint`) and on `wal_group_commit_updates` in relaxed mode.
See [docs/CONCURRENCY.md](CONCURRENCY.md) for crash semantics details.
