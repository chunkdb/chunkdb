# Storage Format Specification (fs_split_v1 backend)

## 1. Hierarchy

A data directory holds named tables. Each table is a separate store with its
own geometry, manifest, revision clock, snapshot generation, intents, WAL and
checkpoints:

```text
data_dir/
  chunkdb.manifest        data-directory manifest (Section 1.1)
  .chunkdb.lock/          writer lock: one writer process per data directory
  .chunkdb.staging/       tables being created (Section 1.4)
  .chunkdb.dropped/       tables being dropped (Section 1.4)
  tables/<name>/          one table (a store)
    table.manifest        table manifest (Section 1.2)
    chunkdb.version, chunkdb.snapshot, .chunkdb.initialized, .chunkdb.intents/
                          bookkeeping (Section 1.5)
    L_<lx>_<ly>/C_<cx>_<cy>.chk
    L_<lx>_<ly>/C_<cx>_<cy>.wal
```

Runtime hierarchy within a table:
1. large chunk
2. regular chunk
3. block bitfield

Where:
- `(cx, cy)` = regular chunk coordinates
- `(lx, ly)` = large chunk coordinates derived from the large-chunk dimensions
  recorded in the table manifest

Paths in the rest of this document are relative to the table directory
unless they name `data_dir`.

### 1.1 Data-directory manifest

`data_dir/chunkdb.manifest` records that the directory is a chunkdb data
directory and carries feature flags (Section 1.3) for the directory itself.
Little-endian, 44 bytes plus the options area, at most 64 KiB:

1. `magic[4]` = `CKDM`
2. `version` (`u16`) = `1`
3. `reserved` (`u16`) = `0`
4. `incompat`, `ro_compat`, `compat` feature flags (`u32` each)
5. `data_dir_id[16]`: random bytes, not all zero
6. `options_size` (`u32`)
7. `options`: TLV entries as in the table manifest; 2.0.0 defines none
8. `crc32` (`u32`) over every preceding byte

A writer that finds no `chunkdb.manifest` creates one only when the
directory holds no chunkdb entry (`tables`, `L_<x>_<y>`, `table.manifest`,
`chunkdb.*`, `.chunkdb.*`), apart from the writer lock and unpublished
manifest temp files; otherwise the open fails. Entries chunkdb never creates
(for example `lost+found` on a volume root) are left alone. The manifest is
published like a table manifest (synced, no-replace, before anything else)
and never rewritten. Read-only mode never initializes a directory.

A `chunkdb.manifest` with the table-manifest magic `CKMF` is the single-store
layout of a 2.0 development build before tables; it is refused with its own
message.

### 1.2 Table manifest

`tables/<name>/table.manifest` records the table's feature flags
(Section 1.3), the geometry it was created with, a random store id and its
options. Little-endian, 64 bytes plus the options area, at most 64 KiB:

1. `magic[4]` = `CKMF`
2. `version` (`u16`) = `2`
3. `reserved` (`u16`) = `0`
4. `incompat`, `ro_compat`, `compat` feature flags (`u32` each)
5. `large_chunk_width`, `large_chunk_height`, `chunk_width`, `chunk_height`,
   `block_bits` (`u32` each)
6. `store_id[16]`: random bytes, not all zero
7. `options_size` (`u32`)
8. `options`: entries of `type` (`u16`), `length` (`u16`) and `length` value
   bytes, filling exactly `options_size` bytes
9. `crc32` (`u32`) over every preceding byte

Options (`TABLEINFO` names in parentheses):

| Type | Option | Value |
|---|---|---|
| 1 | durability mode (`durability_mode`) | `u8`: 0 relaxed, 1 fsync-wal, 2 fsync-checkpoint |
| 2 | checkpoint update interval (`checkpoint_updates`) | `u64`, > 0 |
| 3 | checkpoint WAL bytes (`checkpoint_wal_bytes`) | `u64`, > 0 |
| 4 | WAL group commit updates (`wal_group_commit_updates`) | `u64`, > 0 |
| 5 | checkpoint compression (`checkpoint_compression`) | `u8`: 0 none, 1 zrle |
| 6 | extra data per block (`extra_max_block_bits`) | `u64`, 1 to 134217664 |
| 7 | extra data per chunk (`extra_max_chunk_bytes`) | `u64`, 9 to 16777216; one value of `extra_max_block_bits` must fit (`8 + ceil(bits / 8)` bytes) |

Tables record types 1 to 5. Types 6 and 7 appear together, exactly when the table has the `extra-data` feature (Section 1.3); without them the table has no extra data. Each type appears at most once; an absent type takes its default (relaxed, 256, 1048576, 8, none). A known option with another length or value, or repeated, makes the manifest invalid.

Version `1` (46 bytes, no flags or options) was written only by 2.0
development builds; it is refused with its own message.

The manifest is the first artifact of a table: the bytes are synced under a
temporary name, published only if `table.manifest` does not exist yet, and the
directory entry is synced. A table directory that holds only its manifest is
a valid empty table; the first read-write open writes its bookkeeping. The
manifest is replaced only to change options (`TABLESET`), atomically and
synced: a crash leaves the old or the new options. Enabling extra data sets the `extra-data` bit in the same write; bits are never cleared.

A table opens with the geometry its manifest records. A requested geometry
value that differs from it (the server's geometry flags for `default`) makes
the open fail with both values named, before anything in the directory
changes. A table whose manifest is unreadable, has the wrong size, magic,
version, or checksum, a non-zero reserved field, malformed options, an invalid
geometry, or a zero store id is refused, and the feature flags are checked as
Section 1.3 describes.

On POSIX manifests are published with an exclusive rename
(`renameat2(RENAME_NOREPLACE)` on Linux, `renamex_np(RENAME_EXCL)` on macOS)
or, where the filesystem lacks it, a hard link; a filesystem with neither
cannot create a data directory. On Windows it is a rename that does not
replace.

### 1.3 Feature flags

Both manifests carry three flag sets. A reader that does not know a set bit
of

- `incompat` must not open the data directory or table;
- `ro_compat` may open it read-only and must not write;
- `compat` may ignore it.

The check runs when a data directory or table is opened, before any chunk is
read. The server always opens read-write, so it refuses an unknown
`ro_compat` bit. The data-directory flags cover the directory layout (the
tables, how they are listed); a table's flags cover what is inside it. A feature that adds an option, a section, a frame field or a record type
owns a flag bit, and its data never changes the meaning of what older
readers know. An unknown type is therefore skipped after its bounds and checksum checks when the containing file has a flag bit this reader does not know (for a WAL, also when its table manifest has one: a WAL created before a feature was enabled keeps its header flags but may hold the feature's records), and is corruption otherwise. A `compat` feature's data may be lost
when an older writer rewrites a file, so only data that can be dropped (hints,
caches) may be `compat`.

Bits defined in 2.0.0 (the data-directory manifest defines none):

| Set | Bit | Name | Meaning |
|---|---|---|---|
| `ro_compat` | 0 (`0x1`) | `extra-data` | blocks may carry extra data: EXTRA image sections (Section 3.2) and WAL records 2 to 4 (Section 4.1). A build without it reads payload and presence correctly but must not write, because its checkpoints would drop the values. |

### 1.4 Tables

Table names match `[a-z0-9][a-z0-9_-]{0,63}` and are not a Windows device
name (`con`, `prn`, `aux`, `nul`, `com0`-`com9`, `lpt0`-`lpt9`), so a data
directory moves between platforms and case-insensitive filesystems cannot
alias two tables. Entries of `tables/` that are not valid names (an OS
metadata file) are ignored; a directory with a valid name and no
`table.manifest` is damage and the open fails, because tables are only ever
published complete.

Creating a table is atomic. The writer builds the table directory under a
fresh name in `data_dir/.chunkdb.staging/` (directory, synced
`table.manifest`, directory sync), renames it to `tables/<name>` with a rename
that does not replace, and syncs both parent directories. A crash before the
rename leaves a staging directory and no table; after it, the complete table.

Dropping a table is atomic. The writer waits for running commands on the
table, closes its store, renames `tables/<name>` to a fresh name in
`data_dir/.chunkdb.dropped/` and syncs both parent directories; the rename is
the commit point. It then deletes the directory. A crash during deletion
leaves leftovers in `.chunkdb.dropped/`.

A writer start removes everything in `.chunkdb.staging/` and
`.chunkdb.dropped/`, then opens every table (reading its manifest only; chunks
load and recover lazily). A writer that finds no table creates `default`.

### 1.5 Bookkeeping artifacts

Bookkeeping artifacts in a table directory (not chunk data):
- `table.manifest` — the table manifest (Section 1.2).
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
under the dedicated shallow directory `.chunkdb.intents/` of the table. The
file name embeds the target WAL's path relative to the table directory with `__`
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

Block index: a block's local coordinates are its coordinates modulo the chunk size (floor modulo, so they are never negative), and its index is `local_y * chunk_width_blocks + local_x`. Block `i` holds payload bits `[i * block_bits, (i + 1) * block_bits)` and presence bit `i`; bit `n` of a bit string is bit `n % 8` of byte `n / 8`, least significant first.

Protocol/API mapping:
- `CHUNKGET <cx> <cy>` returns only `payload_bytes`
- `CHUNKGET <cx> <cy> STATE` returns the full combined chunk state bytes
- `CHUNKGET <cx> <cy> STATE EXTRA` returns the state followed by the chunk's EXTRA section (Section 3.2), empty when it has no extra data
- `CHUNKPUT` takes the same layouts

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
| `3` | `EXTRA` | 1 to 16777216 bytes (Section 3.2) |

Types 1 and 2 are required. Type 3 is present only when the chunk has extra data, and only with the image's `ro_compat` bit 0 (`extra-data`) set. Types are strictly ascending (no duplicates); an unknown
type is handled as Section 1.3 describes; unknown section flags are
corruption; an uncompressed body has `stored_size == raw_size`; the bodies
fill the file exactly. The geometry is not repeated per file: the raw sizes
are checked against the manifest's geometry.

`--checkpoint-compression zrle` stores each section compressed; readers accept
compressed and uncompressed sections regardless of the setting. Compression
is off by default. With two sections the header is 108 bytes, with three 124; a chunk without extra data is written exactly as before the feature existed.

Images of 1.x and of 2.0 development builds (magic `CHKDATA1`) are refused;
`chunkdb_migrate` converts them (`docs/MIGRATING.md`).

### 3.2 EXTRA section

The extra data of a chunk (`docs/EXTRA_DATA.md`): one entry per block that has a value, in strictly ascending block index (Section 2), each

1. `block_index` (`u32`), below `block_count`
2. `bit_length` (`u32`), at least 1
3. `ceil(bit_length / 8)` value bytes, bit order as in Section 2; the bits of the last byte past `bit_length` are zero

Every entry belongs to a block whose presence bit is set. An entry takes `8 + ceil(bit_length / 8)` bytes, the measure of `extra_max_chunk_bytes`; no chunk holds more than 16 MiB. The same layout is the wire format of `CHUNKGET`/`CHUNKPUT ... STATE EXTRA` and of the `EXTRA_REPLACE` record. An image that breaks any rule is refused like any damaged image.

### 3.1 `zrle` Codec

`zrle` is a dependency-free zero-run-length codec, also used by the `ZRLE`
option of the chunk wire commands (`CHUNKGET`, `CHUNKPUT`, `CHUNKRANGE`,
`CHUNKRADIUS`):

```text
[codec_id u8 = 0x01][uncompressed_size u32le][token...]
token := 0x00 <uleb128 n>            n zero bytes
       | 0x01 <uleb128 n> <n bytes>  n literal bytes
```

Decoders must know the exact expected output size (from geometry), or for state with an EXTRA section a bounded range of sizes, and must
reject truncated, malformed, or oversized inputs and any input whose declared
or produced size differs from the expected size. The encoder never expands
its input by more than 11 bytes (header, one token byte and a 5-byte length):
input that the run encoding would make larger is written as one literal
token. Any valid token sequence decodes, so this changes no reader. Because the image CRC covers
the canonical uncompressed state, corruption in the compressed blob is caught
either by the bounded decoder or by the checksum of its output.

For compression-ratio, throughput, and latency figures on representative
sparse and dense states, run `chunkdb_compression_bench` (fixed seed); see
`bench/artifacts/` for recorded results. Compression stays opt-in because
dense random states do not shrink (ratio ~1.01x) while sparse states shrink
by ~9x.

## 4. `.wal` Delta Log Format

All integers are little-endian.

WAL header (`60` bytes):
1. `magic[8]` = `CHKWALOG`
2. `version` (`u16`) = `1`
3. `reserved` (`u16`) = `0`
4. `incompat`, `ro_compat`, `compat` (`u32` each): the table's features when the file was created (Section 1.3); they must be a subset of the manifest's
5. `store_id[16]`: the store id from the manifest
6. `chunk_x`, `chunk_y` (`i64`)
7. `header_crc32` (`u32`) over fields 1–6

A writer creates the file with its header in one append.

### 4.1 Frames

The body is an append-only sequence of frames. One frame is one mutation
(`SET`, `UNSET`, `CHUNKPUT`, an `MSET` item, `XPUT`, `XDEL` or `CHUNKBATCH`); relaxed-mode group commit appends several frames in one flush.

Frame:
1. `frame_magic[4]` = `FRM2`
2. `revision` (`u64`): the chunk revision after this mutation (Section 4.2)
3. `commit_time_ms` (`u64`): the mutation's commit time (Unix ms). Within one
   store instance and within one chunk it never decreases.
4. `frame_flags` (`u16`) = `0`
5. `tlv_size` (`u16`): bytes of the TLV area
6. `record_count` (`u32`) >= 1
7. `body_size` (`u32`): bytes of the records
8. TLV area: entries of `type` (`u16`), `length` (`u16`) and `length` value
   bytes, filling exactly `tlv_size` bytes
9. `header_crc32` (`u32`) over fields 2–8
10. `record_count` records, filling exactly `body_size` bytes: `type` (`u8`),
    `size` (`u32`), then `size` body bytes
11. `frame_crc32` (`u32`) over all record bytes

TLV types:

| Type | Name | Value |
| --- | --- | --- |
| `1` | `TAG` | opaque bytes, `1`–`65535`, at most once per frame |

Record types:

| Type | Name | Body |
| --- | --- | --- |
| `1` | `SPAN` | `byte_offset` (`u32`), then the bytes to write at `state[byte_offset, …)`, at least one |
| `2` | `EXTRA_PUT` | `block_index` (`u32`), `bit_length` (`u32`), value bytes: an entry as in Section 3.2 |
| `3` | `EXTRA_DEL` | `block_index` (`u32`) |
| `4` | `EXTRA_REPLACE` | a whole EXTRA section (Section 3.2), possibly empty |

A span lies wholly in the payload, wholly in the presence bitmap, or covers
the whole chunk state; a full-chunk replace logs the payload and the presence
bitmap as two spans. A span is never split, whatever its size.

Records 2 to 4 need the table's `extra-data` feature. A frame holds `EXTRA_PUT`/`EXTRA_DEL` records in strictly ascending block index, or one `EXTRA_REPLACE` and neither of them. `UNSET` adds an `EXTRA_DEL` for a block that had a value; a full-chunk write adds an `EXTRA_DEL` for each value whose block it makes absent, or with `EXTRA` one `EXTRA_REPLACE` when the section changes; a batch adds the changed values as `EXTRA_PUT`/`EXTRA_DEL`. A malformed record stops replay at its frame (stop reasons: `record_extra_disabled` without the feature, `record_extra_order`, `record_extra_invalid`, `record_out_of_range`).

Records overwrite, as spans do: `EXTRA_PUT` sets the value, `EXTRA_DEL` removes it if there is one, `EXTRA_REPLACE` replaces all values, so a WAL replayed over no image after empty-chunk collection ends in the same state as the chunk. The extra-data invariants (every value on a present block, at most 16 MiB per chunk) are checked on the state replay ends in: every committed state keeps them, so a violation is damage and the chunk is not loaded.

Replay validates the header CRC, `frame_flags`, the TLV area (no unknown
type, as Section 1.3 describes, one non-empty `TAG` at most), requires the
whole frame to be present, validates the frame CRC and every record's type,
size, bounds and shape, and only then applies the records and adopts the
frame's revision and commit time. A frame that fails any check is not applied
at all: a torn frame (crash inside one mutation's append) is ignored as a
whole, which makes every mutation atomic across crash recovery regardless of
its size; an invalid interior frame stops replay.

Frame revisions strictly increase (`frame_revision_order` otherwise). Frames at or below the image's revision are checked and skipped: the image already holds them. A checkpoint writes its image from memory, which in `relaxed` mode includes frames still in the group-commit batch, so a WAL that outlives its checkpoint (a crash or failed removal between publishing the image and removing the WAL) may lack frames the image holds; applying its older frames would mix old values into the newer state.

WALs of 1.x and of 2.0 development builds (magic `CHKWAL02`) are refused;
`chunkdb_migrate` converts them.

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

For each `CHUNKPUT` without `STATE`:
1. replace the full in-memory chunk payload
2. set the full presence bitmap to all-present
3. encode delta record(s) for changed payload bytes and/or changed presence bytes
4. follow the same flush and checkpoint policy as `SET`

For each `CHUNKPUT ... STATE`:
1. replace the full in-memory chunk payload
2. replace the full in-memory presence bitmap
3. canonicalize absent blocks so their payload bits are zero
4. encode delta record(s) for changed payload bytes and/or changed presence bytes
5. follow the same flush and checkpoint policy as `SET`

For each `CHUNKPUT ... IF` / `CHUNKBATCH`:
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
removed opportunistically once empty. The batch is flushed into the WAL first, so the WAL holds every frame that emptied the chunk (synced in synced modes and after a barrier). When an image exists, one more frame goes into that flush: it sets the whole payload and presence bitmap to zero (and replaces extra data with nothing) at a new revision. A WAL that outlived an earlier checkpoint holds only frames since some older point, which are right only over that image; with the last frame it still replays to the empty state over no image. If collection stops after that frame, the reloaded empty chunk reports the frame's revision. In synced modes the data-image removal
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
2. if `.wal` exists, validate its header and replay frames in order onto the
   in-memory chunk state:
   - a file that holds a prefix of this chunk's header (feature flags aside,
     the header CRC possibly incomplete) followed only by zero bytes, or zero
     bytes only, or nothing, was cut while it was being created and holds no
     mutation; it is removed, and the next append writes a new header
   - any other invalid or missing header (damage, a file from another store or
     chunk, frames without a header) fails the load and changes nothing
   - when replay stops at a frame that is not whole with both CRCs valid and
     no frame header with a valid CRC starts anywhere
     after the stop (the failing frame reaches the end of the file, or only
     zero or stale bytes follow), the stop is what a crash leaves: the file is
     truncated to the end of the last applied frame before anything is
     appended, so later frames are never written where replay does not reach
   - when a CRC-valid frame header follows the stop, acknowledged frames may
     be there, and a whole, CRC-valid frame that fails its checks was written completely (a writer bug or a foreign file): either way the load fails and the file is left as it is
   The removal and the truncation run inside a snapshot-generation
   transition and follow the durability mode's sync rules.
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
   bytes, or retry exhaustion. A WAL cut while it was being created (see the
   read-write rules) is treated as holding nothing
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
- the EXTRA section (Section 3.2): the image's feature bit, entry order, sizes, padding, and values only on present blocks

`.wal` validation checks:
- magic, version, reserved field and header CRC32
- feature flags within the manifest's, store id and chunk coordinates
- per-frame magic, header CRC32 (covering the TLV area), frame flags, TLV
  entries, completeness, and frame CRC32
- per-record type, size, bounds, and shape
- the extra-data invariants of the state replay ends in (Section 4.1)

### 7.1 `chunkdb_verify`

`chunkdb_verify` is a read-only checker: it never modifies the data directory.
It checks the data-directory manifest, then every table with the geometry its
own manifest records:

```bash
chunkdb_verify --data-dir ./data
```

A missing or damaged data-directory manifest is reported as
`data_dir_manifest_missing` or `data_dir_manifest_invalid` (errors), and no
table is then checked. In a table, a missing or damaged manifest is reported
as `manifest_missing` or `manifest_invalid` (errors), and that table's chunk
artifacts are then not checked. Unknown feature bits are reported as
`data_dir_manifest_unknown_features` or `manifest_unknown_features`: an error
for `incompat` (what they cover is not checked), a warning for `ro_compat`
and `compat`. Leftovers of an interrupted create or drop are warnings
(`interrupted_table_create`, `interrupted_table_drop`), and table state
outside `tables/` is a warning (`unexpected_entry`). Entries chunkdb does not
create, such as `lost+found`, are listed as `info foreign_entry` and do not
affect the exit code.

Findings are printed one per line as `VERIFY <level> <code> <path> [detail...]`,
where `<level>` is `error`, `warning` or `info` and `<code>` is a stable
machine-readable token. The run ends with a summary line:

```text
SUMMARY checked=<n> warnings=<n> errors=<n>
```

Damaged extra data shows up as `chunk_image_invalid`, as `wal_damaged` or `wal_tail_truncated` with a `record_extra_*` reason, or as `wal_extra_inconsistent` (an error) when a WAL leaves a value on an absent block.

Exit code `0` means no findings, `1` means warnings or errors were reported, and `2`
means the run itself failed (bad arguments, unreadable directory).

## 8. Durability Notes

Durability guarantees depend on configured mode (`relaxed`, `fsync-wal`, `fsync-checkpoint`) and on `wal_group_commit_updates` in relaxed mode.
See [docs/CONCURRENCY.md](CONCURRENCY.md) for crash semantics details.
