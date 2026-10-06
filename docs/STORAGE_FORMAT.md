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
- `(lx, ly)` = large chunk coordinates derived from the large-chunk dimensions
  recorded in the store manifest

### 1.1 Store manifest

`data_dir/chunkdb.manifest` records the store's feature flags (Section 1.3),
the geometry it was created with, a random store id and options.
Little-endian, 64 bytes plus the options area, at most 64 KiB:

1. `magic[4]` = `CKMF`
2. `version` (`u16`) = `2`
3. `reserved` (`u16`) = `0`
4. `incompat`, `ro_compat`, `compat` feature flags (`u32` each)
5. `large_chunk_width`, `large_chunk_height`, `chunk_width`, `chunk_height`,
   `block_bits` (`u32` each)
6. `store_id[16]`: random bytes, not all zero
7. `options_size` (`u32`)
8. `options`: entries of `type` (`u16`), `length` (`u16`) and `length` value
   bytes, filling exactly `options_size` bytes. 2.0.0 defines no option types.
9. `crc32` (`u32`) over every preceding byte

Version `1` (46 bytes, no flags or options) was written only by 2.0
development builds; it is refused with its own message.

A new store writes its manifest before any other artifact: the bytes are
synced under a temporary name, published only if `chunkdb.manifest` does not
exist yet, and the directory entry is synced. A crash before that leaves no
manifest and at most a temporary `chunkdb.manifest.tmp.*` file, which the next
read-write start removes before it initializes the directory again. The
manifest is never rewritten.

A store opens with the geometry its manifest records. A requested geometry
value that differs from it makes the open fail with both values named, before
anything in the directory changes. A store refuses to open a directory whose
manifest is unreadable, has the wrong size, magic, version, or checksum, a
non-zero reserved field, malformed options, an invalid geometry, or a zero
store id, and checks the feature flags as Section 1.3 describes.

A read-write store initializes a directory without a manifest only when it
holds no chunkdb state: no `L_<x>_<y>` chunk directory and no `chunkdb.*` or
`.chunkdb.*` bookkeeping, apart from the writer lock (`.chunkdb.lock*`) and
unpublished manifest temp files. Such state without a manifest was written by
1.x or an earlier 2.0 development build, and the open fails. Entries chunkdb
never creates (for example `lost+found` on a volume root) are left alone.
Read-only mode never initializes a directory.

On POSIX the manifest is published with an exclusive rename
(`renameat2(RENAME_NOREPLACE)` on Linux, `renamex_np(RENAME_EXCL)` on macOS)
or, where the filesystem lacks it, a hard link; a filesystem with neither
cannot create a store. On Windows it is a rename that does not replace.

### 1.2 Bookkeeping artifacts

Bookkeeping artifacts in `data_dir` (not chunk data):
- `chunkdb.manifest` — the store manifest (Section 1.1).
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

A read-write start that finds no version bookkeeping (a new store, or one whose
initialization stopped after the manifest) syncs a checked clock first and the
initialized marker second.
Missing snapshot-generation metadata is the implicit stable generation zero.
A current read-write startup durably publishes generation one before recovery
can change any artifact and generation two afterward. The generation file is
never removed or reset.

Once a valid initialized marker exists, a missing, unreadable, uninspectable,
truncated, oversized, or invalid clock is bookkeeping damage: the server
refuses to open instead of potentially reissuing an exposed token. Restore the
clock from a consistent backup, or intentionally reinitialize the whole store.
If both version-token bookkeeping files are lost, startup cannot tell the
store from one that never issued tokens and starts a new clock, so it cannot
deterministically detect prior token exposure. Back up the two files
together with the store. Read-only opening does not issue deterministic
persisted versions. `chunkdb_verify` reports marker/clock damage as an error.

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

### 1.3 Feature flags

The manifest carries three flag sets. A reader that does not know a set bit
of

- `incompat` must not open the store;
- `ro_compat` may open it read-only and must not write;
- `compat` may ignore it.

The check runs when a store is opened, before any chunk is read. The server
always opens read-write, so it refuses a store with an unknown `ro_compat`
bit. A feature that adds an option, a section, a frame field or a record type
owns a flag bit, and its data never changes the meaning of what older
readers know. An unknown type is therefore skipped after its bounds and
checksum checks when the containing structure has a flag bit this reader does
not know, and is corruption otherwise. A `compat` feature's data may be lost
when an older writer rewrites a file, so only data that can be dropped (hints,
caches) may be `compat`.

2.0.0 defines no feature bits.

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

All integers are little-endian. An image is a fixed header, a section
directory, a header CRC, and the section bodies:

1. `magic[8]` = `CHKIMAGE`
2. `version` (`u16`) = `1`
3. `section_count` (`u16`), at most `64`
4. `incompat`, `ro_compat`, `compat` (`u32` each): the features this image
   uses (Section 1.3); they must be a subset of the manifest's
5. `store_id[16]`: the store id from the manifest
6. `chunk_x`, `chunk_y` (`i64`)
7. `revision` (`u64`): the chunk revision after the last mutation the image
   captures (Section 4.2), never zero
8. `commit_time_ms` (`u64`): that mutation's commit time (Unix ms)
9. directory: `section_count` entries of
   - `type` (`u16`)
   - `flags` (`u16`): bit 0 = the body is `zrle`-compressed (Section 3.1)
   - `stored_size` (`u32`): bytes of the body in the file
   - `raw_size` (`u32`): bytes after decompression
   - `crc32` (`u32`) over the raw bytes
10. `header_crc32` (`u32`) over fields 1–9
11. the section bodies, in directory order, filling the rest of the file

Section types:

| Type | Name | Raw size |
| --- | --- | --- |
| `1` | `PAYLOAD` | `payload_bytes` |
| `2` | `PRESENCE` | `presence_bytes` |

Both are required. Types are strictly ascending (no duplicates); an unknown
type is handled as Section 1.3 describes; unknown section flags are
corruption; an uncompressed body has `stored_size == raw_size`; the bodies
fill the file exactly. The geometry is not repeated per file: the raw sizes
are checked against the manifest's geometry.

`--checkpoint-compression zrle` stores each section compressed; readers accept
compressed and uncompressed sections regardless of the setting. Compression
is off by default. With two sections the header is 108 bytes.

Images of 1.x and of 2.0 development builds (magic `CHKDATA1`) are refused.

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
2. `wal_version` (`u16`) = `4` (frames); the 1.x record streams (`2`, `3`)
   are refused
3. `block_bits` (`u16`); this field is why geometry limits `block_bits` to `65535`
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
`byte_offset` and `data_size`, a corrupted offset cannot relocate a CRC-valid
body. A WAL header after the first frame position is damage and stops replay.
A headerless stream that starts with `FRM1` replays as frames.

### 4.2 Chunk revision

The revision is the value `CHUNKVER` reports. Every mutation reserves it from
the store-wide monotonic version clock (`chunkdb.version`) and stores it in
the frame; the next checkpoint copies the in-memory revision into the image
header. Loading a chunk takes the revision from the image and the last valid
frame and reserves nothing, so eviction and restart leave `CHUNKVER`
unchanged. A chunk with no artifact takes a fresh token when it is loaded.
When a persisted revision is at or above the clock, the clock is raised past
it and a new ceiling is persisted before any further token is issued, so
revisions never repeat even if the clock bookkeeping was lost and restarted.

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
is never garbage collected.

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
2. collect the chunk image, WAL, and adjacent
   `.wal.rollback` intent
3. read and validate `chunkdb.snapshot` again; accept only when both
   generations are the same even value, retrying with eight sleep-free
   attempts and then exponential backoff within a bounded total sleep budget
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
one generation, and so do consecutive transitions that fall inside one epoch —
concurrent writers join an open epoch, and a single writer's even publication
lingers briefly so a following transition can re-enter it (see
`docs/DURABILITY_CONTRACT.md`). A crash leaves an odd generation until the next
writer publishes a fresh odd recovery generation and completes recovery;
read-only loads fail closed meanwhile. Generation exhaustion is a startup/write
error, never wraparound. Therefore byte-identical ABA cycles cannot pass the
bracket: any collection that spans a completed rollback or commit also spans
that transition's epoch, so it observes either an odd generation or two
different even generations, even when image, WAL, and absent-intent bytes
return to earlier values.

This per-chunk rule allows an older coherent state when its full observation
falls between writer transitions, but not a rejected, in-flight, torn, or
image/WAL-mixed conditional state.

A trailing partial frame (e.g. torn append) is ignored as a whole; an
invalid interior frame stops replay.

## 7. Validation and Corruption Handling

`.chk` validation checks:
- magic and version
- header CRC32 over the header and the section directory
- feature flags within the manifest's, store id, chunk coordinates, and a
  non-zero revision
- section order, flags, sizes (against the geometry for known types), and
  that the bodies fill the file
- each section's CRC32 over its raw bytes

`.wal` validation checks:
- magic
- version
- geometry fields
- chunk coordinates
- per-frame magic, header CRC32, completeness, and frame CRC32
- per-record bounds, shape, and CRC32 over header and body

### 7.1 `chunkdb_verify`

`chunkdb_verify` is a read-only checker: it never modifies the data directory.
It reads the geometry from the store manifest:

```bash
chunkdb_verify --data-dir ./data
```

A missing or damaged manifest is reported as `manifest_missing` or
`manifest_invalid` (both errors), and chunk artifacts are then not checked.
Unknown feature bits are reported as `manifest_unknown_features`: an error for
`incompat` (artifacts are not checked), a warning for `ro_compat` and
`compat`. Entries
chunkdb does not create, such as `lost+found`, are listed as `info foreign_entry`
and do not affect the exit code.

Findings are printed one per line as `VERIFY <level> <code> <path> [detail...]`,
where `<level>` is `error`, `warning` or `info` and `<code>` is a stable
machine-readable token. The run ends with a summary line:

```text
SUMMARY checked=<n> warnings=<n> errors=<n>
```

Exit code `0` means no findings, `1` means warnings or errors were reported, and `2`
means the run itself failed (bad arguments, unreadable directory).

## 8. Durability Notes

Durability guarantees depend on configured mode (`relaxed`, `fsync-wal`, `fsync-checkpoint`) and on `wal_group_commit_updates` in relaxed mode.
See [docs/CONCURRENCY.md](CONCURRENCY.md) for crash semantics details.
