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
7. `options`: TLV entries as in the table manifest; one is defined:
   - type `1` `version_floor` (`u64`): every version token a table of this directory issued is below it. `DROP TABLE` raises it to the dropped table's version clock ceiling (durably, before the drop), and a table's new version clock starts there, so a table dropped and created again under the same name never reuses a token
8. `crc32` (`u32`) over every preceding byte

A writer that finds no `chunkdb.manifest` creates one only when the directory holds no chunkdb entry (`tables`, `L_<x>_<y>`, `table.manifest`, `chunkdb.*`, `.chunkdb.*`), apart from the writer lock and unpublished manifest temp files; otherwise the open fails. Entries chunkdb never creates (for example `lost+found` on a volume root) are left alone. The manifest is published like a table manifest (synced, no-replace, before anything else); after that only `DROP TABLE` replaces it (atomically and synced) to raise `version_floor`. Read-only mode never initializes a directory.

A `chunkdb.manifest` with the table-manifest magic `CKMF` is the single-store
layout of a 2.0 development build before tables; it is refused with its own
message.

### 1.2 Table manifest

`tables/<name>/table.manifest` records the table's feature flags
(Section 1.3), the geometry it was created with, a random store id, its
options and its columns. Little-endian, 64 bytes plus the options and schema
areas, at most 1 MiB:

1. `magic[4]` = `CKMF`
2. `version` (`u16`) = `4`
3. `reserved` (`u16`) = `0`
4. `incompat`, `ro_compat`, `compat` feature flags (`u32` each)
5. `large_chunk_width`, `large_chunk_height`, `chunk_width`, `chunk_height`
   (`u32` each)
6. `store_id[16]`: random bytes, not all zero
7. `options_size` (`u32`)
8. `options`: entries of `type` (`u16`), `length` (`u16`) and `length` value
   bytes, filling exactly `options_size` bytes
9. `schema_size` (`u32`)
10. `schema`: the table's columns (below), filling exactly `schema_size`
    bytes up to the checksum
11. `crc32` (`u32`) over every preceding byte

The schema area ([COLUMNS_DESIGN.md](COLUMNS_DESIGN.md)): `version` (`u64`, at least 1), `next_column_id` (`u32`), `column_count` (`u32`, 1 to 1024), then per column `id` (`u32`, unique, 1 to `next_column_id - 1`), `kind` (`u8`: 1 `uN`, 2 `iN`, 3 `bool`, 4 `f32`, 5 `f64`, 6 `bits(N)`, 7 `text(max)`, 8 `bytes(max)`), `size` (`u32`: N bits of `uN` (1–64), `iN` (2–64) and `bits(N)` (1–65535), 1 for `bool`, 32 and 64 for floats, the most bytes of `text` and `bytes`, 1 to 16 MiB), `flags` (`u8`: bit 0 `NULL`, bit 1 `REQUIRED`, bit 2 has a default; not both of the first two), `name_length` (`u8`) and the name (`[a-z_][a-z0-9_]*`, at most 63 bytes, unique), then with a default `default_length` (`u32`) and the value (fixed-width: `ceil(bits / 8)` bytes, unused bits zero; `text`: UTF-8 within `max` bytes; `bytes`: within `max` bytes). The fixed-width columns of a block take at most 65535 bits together. A block's width, which earlier versions recorded as `block_bits`, is that total. A table created with a block width only is the column `bits` of type `bits(block_bits)`. A table needs at least one fixed-width column (Section 2); `text` and `bytes` values are stored per chunk in its VARS section (Section 3.2).

After the columns come `version - 1` history steps, oldest first, one per version above 1 (step `k` made version `k + 1`): `change_count` (`u32`, at least 1), then per change `kind` (`u8`: 1 added, 2 dropped, 3 renamed, 4 type changed), `position` (`u32`: where in the column list the column was added, dropped, or is) and the column as above (the added or dropped column, the renamed one after the rename, or the column after its type change), then for a rename `old_name_length` (`u8`) and the old name, and for a type change the column before it and `conversion` (`u8`: 0 exact, 1 clamp, 2 default, 3 truncate). Undoing the steps from the newest gives every earlier version, which images and WAL frames of that version are read by (Sections 3 and 4.1). A table that never changed its columns has no steps, so its schema area is as it always was. Undoing an added column sets `next_column_id` back to its id. A narrowing in progress (`TableCatalog::NarrowColumn`) follows the steps: `1` (`u8`), the column id (`u32`) and the narrower type (`kind` `u8`, `size` `u32`); every write to that column must then fit both types. A read-write open drops a narrowing it finds (a crash interrupted it) before the table opens.

Options (`DESCRIBE` names in parentheses):

| Type | Option | Value |
|---|---|---|
| 1 | durability mode (`durability_mode`) | `u8`: 0 relaxed, 1 fsync-wal, 2 fsync-checkpoint |
| 2 | checkpoint update interval (`checkpoint_updates`) | `u64`, > 0 |
| 3 | checkpoint WAL bytes (`checkpoint_wal_bytes`) | `u64`, > 0 |
| 4 | WAL group commit updates (`wal_group_commit_updates`) | `u64`, > 0 |
| 5 | checkpoint compression (`checkpoint_compression`) | `u8`: 0 none, 1 zrle |
| 6 | text and bytes values per chunk (`var_max_chunk_bytes`) | `u64`, 13 to 67108864, as the VARS section measures it (Section 3.2) |

Tables record types 1 to 6. Each type appears at most once; an absent type takes its default (relaxed, 256, 1048576, 8, none, 1048576). A known option with another length or value, or repeated, makes the manifest invalid.

Versions `1` to `3` were written only by 2.0 development builds; they are
refused with their own message.

The manifest is the first artifact of a table: the bytes are synced under a temporary name, published only if `table.manifest` does not exist yet, and the directory entry is synced. A table directory that holds only its manifest is a valid empty table; the first read-write open writes its bookkeeping. The manifest is replaced only by `ALTER TABLE`, atomically and synced: a crash leaves the old or the new manifest.

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

Table `incompat` bit 0 (`0x1`) enables durable feed slots, USER frame metadata
and the empty-collection frame flag. It is synced in the manifest before the
first slot is published and remains set after slots are dropped. Builds that
do not implement it refuse both read-only and read-write opening: older frame
parsers cannot safely read the collection flag. Other bits remain undefined.

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
- `chunkdb.slots` — durable feed slot records (Section 1.6).
- `.chunkdb.feed/` — archived WAL segments and linked base images (Section 1.6).
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

A transaction commit (Section 5.2) writes a transaction intent `txn-<T>.rollback` in the same directory, where `<T>` is the commit's version in decimal. The record, little-endian, is 20 + 24 × `chunk_count` bytes: magic `CKTB` (rollback) or `CKTC` (committed), the version `T` (`u64`), `chunk_count` (`u32`, 1 to 64), then per written chunk `chunk_x` and `chunk_y` (`i64`) and `wal_boundary` (`u64`: the WAL's size before the commit, zero when the chunk had no WAL), and a CRC32 over every preceding byte.

A read-write start resolves transaction intents after conditional ones and before it serves. Every intent is checked first, then for `CKTB` each listed WAL is truncated to its boundary, or removed when the boundary is zero, and synced; a WAL shorter than its nonzero boundary or missing is damage and the start fails. `CKTC` keeps the WALs, including one checkpointed away since. Then the intent is removed and the directory synced. A crash during this leaves the intent, and the next start repeats it.

### 1.6 Durable feed slots and archives

`chunkdb.slots` is a little-endian checked record, atomically replaced with
file and directory sync in every durability mode:

1. magic `CKSL` (4 bytes), version `1` (`u16`), reserved zero (`u16`)
2. table epoch (16 bytes, equal to the manifest store id)
3. durable watermark (`u64`), slot count (`u32`, at most 1024)
4. per slot: name length (`u8`), loss flag (`u8`, 0 or 1), reserved zero
   (`u16`), written revision (`u64`), name bytes
5. CRC32 (`u32`) over every preceding byte

Names are unique and match `[a-z_][a-z0-9_]*`, 1–63 bytes. Every written
revision is at or below the durable watermark. A loss flag retains the fact
that the slot exceeded its limit; listing excludes it and advancing it reports
slot loss. Explicit drop removes the record. A writer removes unpublished
`chunkdb.slots.tmp.*` artifacts at open; read-only opening and verify never do.

The first slot starts after a completed, synced clock frontier. Activation
quiesces table writers, maintenance and external eviction and checkpoints
pre-slot live/staged WALs before choosing that frontier. This establishes an
exact image baseline even when a pre-slot checkpoint left a WAL behind.

While slots exist, checkpoint flushes all staged frames, syncs and hard-links
the old image to `.chunkdb.feed/C_<cx>_<cy>.<first>.chk` (absent when the WAL
started without an image), publishes the current live image, then renames the
WAL to `.chunkdb.feed/C_<cx>_<cy>.<first>-<last>.wal`. The linked base and its
directory are durable before the old image is replaced; the archive and live
directories are synced after rename. Empty-chunk collection follows the same
order. A retry reuses an existing base. After an image-before-rename crash,
history uses that base rather than the newer live image, or starts empty when
there was no base. Ordinary chunk loading keeps its image-plus-live-WAL rules.

Writer recovery validates and removes a live name that aliases the archived
inode, preventing future append from modifying an immutable archive. Independent
live and archived files with overlapping ranges are damage. Archives and their
bases are released only below every persisted written position; a pending base
for a live WAL is preserved. Active readers pin archive retention across store
reopen and keep live-WAL rollover archival even after the last slot is dropped.

An archived WAL names a closed revision range: a partial header or frame is
damage, not an interrupted append. Concurrent slot readers capture completed
live-WAL byte boundaries in memory and read only the prefix at or below the
persisted durable frontier; truncation inside that prefix is also damage.
This index adds no on-disk format. Ordinary live-WAL recovery retains its
trailing-partial-frame policy.

## 2. Packed Chunk State

Per regular chunk:
- block_count = `chunk_width_blocks * chunk_height_blocks`
- payload_bytes = the sum, over the fixed-width columns in schema order, of `ceil(block_count * width / 8)` for the values plus, for a `NULL` column, `ceil(block_count / 8)` for its validity bits; at most 64 MiB
- presence_bits = `block_count`
- presence_bytes = `ceil(block_count / 8)`

Payload is column-major: for each fixed-width column in schema order, its values (value `i` at bits `[i * width, (i + 1) * width)` of the array, least significant bit first: `uN` and `bool` as unsigned, `iN` in two's complement, floats as their IEEE 754 bits, `bits(N)` as given) padded with zero bits to a byte, then for a `NULL` column its validity bits (bit `i` is 1 when block `i` has a value) padded to a byte. A column's values are one byte range of the payload. A table with one column `bits(block_bits)` therefore has exactly one bit string per block, block after block.

Canonical form: an absent block has every value bit and validity bit zero, a `NULL` value has its value bits zero, and padding bits are zero. Whole-chunk writes (`SET CHUNK`) are brought to this form before they are stored.

Presence bitmap is stored separately:
- bit = `1` means the block is explicitly present
- bit = `0` means the block is unset
- chunk-level presence is derived from this bitmap:
  - any set presence bit => chunk exists
  - all presence bits clear => chunk absent

Combined chunk state bytes:
- `payload_bytes` of packed block payload
- followed by `presence_bytes` of block presence bitmap

Block index: a block's local coordinates are its coordinates modulo the chunk size (floor modulo, so they are never negative), and its index is `local_y * chunk_width_blocks + local_x`. Block `i` holds value `i` of every column and presence bit `i`; bit `n` of a bit string is bit `n % 8` of byte `n / 8`, least significant first.

Protocol mapping: the chunk form of `GET CHUNK`, `GET AREA` and `SET CHUNK` holds the chunk version, then `presence_bytes`, then `payload_bytes`, then the `text` and `bytes` values ([CQL.md](CQL.md)).

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
| `3` | `VARS` | 1 to 67108864 bytes (Section 3.2) |
| `4` | `SCHEMA` | 8 bytes: the schema version (`u64`) the other sections are laid out by, never compressed |

Types 1 and 2 are required. Type 3 is present only when the chunk has text or bytes values. Type 4 is present only when the table's schema version is above 1; without it the image is of version 1. A reader sizes and checks the sections by that version's columns and translates the state to the current version (Section 4.1); a version above the table's is damage. Types are strictly ascending (no duplicates); an unknown
type is handled as Section 1.3 describes; unknown section flags are
corruption; an uncompressed body has `stored_size == raw_size`; the bodies
fill the file exactly. The geometry is not repeated per file: the raw sizes
are checked against the manifest's geometry.

`--checkpoint-compression zrle` stores each section compressed; readers accept
compressed and uncompressed sections regardless of the setting. Compression
is off by default. With two sections the header is 108 bytes, with three 124.

Images of 1.x and of 2.0 development builds (magic `CHKDATA1`) are refused.

### 3.2 VARS section

The values of a chunk's `text` and `bytes` columns: one entry per value, in strictly ascending (`column_id`, `block_index`), each

1. `column_id` (`u32`): a `text` or `bytes` column of the table
2. `block_index` (`u32`), below `block_count`
3. `byte_length` (`u32`), at most the column's `max`
4. the value bytes; UTF-8 in a `text` column

Every entry belongs to a block whose presence bit is set. A block without an entry for a column has no value there: `NULL` in a `NULL` column, the empty value in any other, so an empty value of a column that cannot be `NULL` is never stored. An entry takes `12 + byte_length` bytes, the measure of `var_max_chunk_bytes`; no chunk holds more than 64 MiB. The same layout is the body of the `VAR_REPLACE` record. An image that breaks any rule is refused like any damaged image.

### 3.1 `zrle` Codec

`zrle` is a dependency-free zero-run-length codec:

```text
[codec_id u8 = 0x01][uncompressed_size u32le][token...]
token := 0x00 <uleb128 n>            n zero bytes
       | 0x01 <uleb128 n> <n bytes>  n literal bytes
```

Decoders must know the exact expected output size (from geometry) and must
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

The body is an append-only sequence of frames. One frame is one mutation (`SET BLOCK`, `DELETE BLOCK` or `SET CHUNK`, or one chunk's part of a transaction commit); relaxed-mode group commit appends several frames in one flush.

Frame:
1. `frame_magic[4]` = `FRM2`
2. `revision` (`u64`): the chunk revision after this mutation (Section 4.2)
3. `commit_time_ms` (`u64`): the mutation's commit time (Unix ms). Within one
   store instance and within one chunk it never decreases.
4. `frame_flags` (`u16`): normally `0`; bit 0 marks an empty-collection
   maintenance frame, only with the feed-slots feature. Other bits are invalid.
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
| `2` | `SCHEMA` | the schema version (`u64`, 8 bytes) the frame's records are laid out by; only in frames of tables past version 1, at most once; a frame without it is of version 1 |
| `3` | `USER` | writer identity bytes, nonempty and at most once, only with the feed-slots feature; absent for anonymous writes |

TAG, SCHEMA and USER together, including their TLV headers, must fit the
`u16` TLV area. Writers check this before adding anything to the staged batch.
A collection frame has no TAG, two zero-filled spans covering the full payload
and presence bitmap, and an empty VAR_REPLACE when the layout has variable
columns. It follows the actual deletion and preserves an already empty state;
archive readers apply it without emitting an extra user change.

Record types:

| Type | Name | Body |
| --- | --- | --- |
| `1` | `SPAN` | `byte_offset` (`u32`), then the bytes to write at `state[byte_offset, …)`, at least one |
| `2` | `VAR_PUT` | `column_id` (`u32`), `block_index` (`u32`), `byte_length` (`u32`), value bytes: an entry as in Section 3.2 |
| `3` | `VAR_DEL` | `column_id` (`u32`), `block_index` (`u32`) |
| `4` | `VAR_REPLACE` | a whole VARS section (Section 3.2), possibly empty |

A span lies wholly in the payload, wholly in the presence bitmap, or covers
the whole chunk state; a full-chunk replace logs the payload and the presence
bitmap as two spans. A span is never split, whatever its size.

Records 2 to 4 need a table with `text` or `bytes` columns. A frame holds `VAR_PUT`/`VAR_DEL` records in strictly ascending (`column_id`, `block_index`), or one `VAR_REPLACE` and neither of them. A typed block write adds a record per value it changes, `DELETE BLOCK` a `VAR_DEL` per value of the block, and a full-chunk write a `VAR_DEL` for each value whose block it makes absent. A malformed record stops replay at its frame (stop reasons: `record_vars_without_columns`, `record_vars_order`, `record_vars_invalid`, `record_out_of_range`).

Records overwrite, as spans do: `VAR_PUT` sets the value, `VAR_DEL` removes it if there is one, `VAR_REPLACE` replaces all values, so a WAL replayed over no image after empty-chunk collection ends in the same state as the chunk. The rules of Section 3.2 and the 64 MiB bound are checked on the state replay ends in: every committed state keeps them, so a violation is damage and the chunk is not loaded.

Replay validates the header CRC, `frame_flags`, the TLV area (no unknown
type, as Section 1.3 describes, one non-empty `TAG` at most), requires the
whole frame to be present, validates the frame CRC and every record's type,
size, bounds and shape, and only then applies the records and adopts the
frame's revision and commit time. A frame that fails any check is not applied
at all: a torn frame (crash inside one mutation's append) is ignored as a
whole, which makes every mutation atomic across crash recovery regardless of
its size; an invalid interior frame stops replay.

Frame revisions strictly increase (`frame_revision_order` otherwise). Frames at or below the image's revision are checked and skipped: the image already holds them. A checkpoint writes its image from memory, which in `relaxed` mode includes frames still in the group-commit batch, so a WAL that outlives its checkpoint (a crash or failed removal between publishing the image and removing the WAL) may lack frames the image holds; applying its older frames would mix old values into the newer state.

Each frame applies in its own schema version. Replay starts from the image's version (or, without an image, from the empty state of the first frame's version); before a frame of a later version it translates the state to that version, and at the end to the table's current version. Translation goes one version at a time. It keeps the values of columns both versions have, converting those of a column whose type changed by the step's conversion (a value that fits converts exactly; otherwise clamp takes the nearest value in range, default takes the column's `DEFAULT`, else `NULL`, else zero or empty, and truncate keeps the first bytes or bits, text at a character boundary); it gives a column added since what a new block would get (its `DEFAULT`, else `NULL`, else zero) in every present block, and drops the values of dropped columns; presence is unchanged. Versions only grow, so an applied frame older than the state it follows stops replay (`frame_schema_version_order`), and a frame of a version the table does not have is damage (`frame_schema_version`). Files keep their version until the chunk's next checkpoint, which writes the current one.

WALs of 1.x and of 2.0 development builds (magic `CHKWAL02`) are refused.

### 4.2 Chunk revision

The revision is the chunk version that `GET CHUNK` and writes report. Every mutation reserves it from the store-wide monotonic version clock (`chunkdb.version`) and stores it in the frame; the next checkpoint copies the in-memory revision into the image header. Loading a chunk takes the revision from the image and the last valid frame and reserves nothing, so eviction and restart leave the version unchanged. A chunk with no artifact takes a fresh token when it is loaded. When a persisted revision is at or above the clock, the clock is raised past it and a new ceiling is persisted before any further token is issued, so revisions never repeat even if the clock bookkeeping was lost and restarted.

## 5. Write Path

For each `SET BLOCK`:
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

A typed block write (`SetBlock`) changes the value bytes of each column it sets (and the byte of its validity bit), and logs one span per changed range plus the presence byte in one frame.

For each `DELETE BLOCK`:
1. zero touched bytes in the in-memory payload (every column's value and validity bit)
2. clear the target block presence bit
3. encode delta record(s) for changed payload bytes and/or changed presence bytes
4. follow the same flush and checkpoint policy as `SET BLOCK`

For each `SET CHUNK` without `IF VERSION`:
1. replace the full in-memory chunk payload and its `text` and `bytes` values
2. replace the full in-memory presence bitmap
3. canonicalize absent blocks, `NULL` values and padding bits to zero (Section 2)
4. encode delta record(s) for changed payload bytes and/or changed presence bytes
5. follow the same flush and checkpoint policy as `SET BLOCK`

For each `SET CHUNK ... IF VERSION`:
1. validate the chunk form and the expected chunk version
2. flush the chunk's group-commit batch into the WAL with a sync and, the first time this process takes a boundary of the chunk or after its files changed without a sync, sync its image, its WAL, their directory and the table directory, so the boundary below covers every acknowledged write and survives a power loss together with the image it applies over
3. reserve the next version token before any mutation can become visible
4. durably publish a new odd store snapshot generation
5. persist a checked `C_<cx>_<cy>.wal.rollback` intent containing the
   pre-command WAL byte boundary
6. apply the new state in memory and encode the full canonical chunk state as
   one WAL frame (a payload span and a presence span), which makes the
   mutation atomic across crash recovery for every geometry
7. atomically replace and directory-sync `CKRB` with `CKRC`; this is the commit
   point
8. remove and directory-sync `CKRC`, then follow the same checkpoint policy as `SET BLOCK`
9. durably publish the next even snapshot generation once the disk state is
   coherent

Before the commit point, any error restores memory and truncates/removes the
WAL back to the recorded boundary. If that repair cannot complete, the store
stops accepting durability-changing operations; startup consumes the retained
intent before WAL replay and repeats the rollback. After the commit point,
intent-cleanup or inline-checkpoint errors are reported in logs but cannot turn
the committed mutation into a command error. The one exception is a commit record that is visible but cannot be made durable: the store fails closed and the error says the write may or may not be applied. A retained `CKRC` never truncates
later successful writes.

Checkpoint writes full `.chk` atomically and removes `.wal`.

Empty-chunk garbage collection: when a checkpoint runs for a chunk whose
presence bitmap has no set bits, the chunk's `.chk` image is removed instead
of rewritten, the `.wal` is removed, and the parent `L_<lx>_<ly>` directory is
removed opportunistically once empty. The batch is flushed into the WAL first, so the WAL holds every frame that emptied the chunk (synced in synced modes and after a barrier). When an image exists, one more frame goes into that flush: it sets the whole payload and presence bitmap to zero (and, with `text` or `bytes` columns, replaces the values with none) at a new revision. A WAL that outlived an earlier checkpoint holds only frames since some older point, which are right only over that image; with the last frame it still replays to the empty state over no image. If collection stops after that frame, the reloaded empty chunk reports the frame's revision. In synced modes the data-image removal
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
   - `relaxed`: no required temp-file `fsync` for a genuinely new store before its first successful `FLUSH WAL`; after a barrier or after reopening an initialized store, later checkpoint replacements flush before removing WAL state so they cannot downgrade previously durable data
3. close temp file and fail if close reports an error
4. atomically replace target namespace entry with temp file
5. durability-mode dependent directory flush:
   - `fsync-wal` / `fsync-checkpoint`: sync parent directory metadata after replace
   - `relaxed`: for a genuinely new store before its first successful barrier, no required directory sync (a later `FLUSH WAL` syncs tracked artifacts); afterward, and after reopening an initialized store, replacement directories are synced to preserve the durability floor

Crash behavior:
- crash before replace: old target remains valid; orphan temp artifacts may remain
- crash after replace but before directory sync: namespace update is atomic, but durability after power loss is not guaranteed unless the mode includes directory sync
- startup/load path removes stale orphan temp artifacts for the target chunk before loading
- a writer's open removes stale temp artifacts of the version clock and its marker, the snapshot-generation record, conditional intents and transaction intents (the same records are replaced this way)

Additional runtime behavior:
- pending WAL batches are flushed on clean shutdown
- pending WAL batches are flushed before chunk eviction

### 5.2 Transaction commit

A transaction commit (docs/TRANSACTIONS_DESIGN.md) applies new states of up to 64 chunks together:

1. lock every chunk the transaction read or wrote, in coordinate order, and check that none changed since its snapshot
2. for each written chunk whose new state differs: make its WAL's size a durable boundary as a conditional write does (step 2 above), writing the batch without a stream from the shared pool
3. reserve one version token `T` and one commit time for all of them
4. durably publish a new odd store snapshot generation and write `CKTB` with the boundaries (file and directory synced)
5. append to each WAL one frame with revision `T` holding the spans and value records that differ, then sync it (and a new WAL's directory entry)
6. atomically replace and directory-sync `CKTB` with `CKTC`; this is the commit point
7. apply the new states in memory, unlock, remove and directory-sync the intent, publish the next even snapshot generation, then follow the usual checkpoint policy

Before the commit point, any error truncates the WALs back to their boundaries and removes the intent, changing nothing. If that repair cannot complete, the chunks stay cached and take no more writes, the store stops accepting durability-changing operations, and startup repeats the repair from the intent.

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
2. collect the chunk image, WAL, adjacent
   `.wal.rollback` intent and the pending transaction intents
3. read and validate `chunkdb.snapshot` again; accept only when both
   generations are the same even value, retrying with eight sleep-free
   attempts and then exponential backoff within a bounded total sleep budget
4. for `CKRB`, require the WAL when the recorded boundary is nonzero and replay
   exactly the WAL prefix ending at that boundary; ignore every byte after it
5. a `CKTB` that lists the chunk limits the replay to its boundary in the same way (the smaller boundary when a `CKRB` does too); `CKTC` changes nothing
6. for `CKRC` or no intent, replay the complete observed WAL
7. fail the chunk load for malformed generation or intent metadata, a missing required
   WAL, a WAL shorter than the `CKRB` boundary, corruption in the replayed
   bytes, or retry exhaustion. A WAL cut while it was being created (see the
   read-write rules) is treated as holding nothing
8. do not write checkpoints, truncate/remove WAL or intent files, clean temp
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
- the VARS section (Section 3.2): entry order and sizes, and every rule for the table's columns

`.wal` validation checks:
- magic, version, reserved field and header CRC32
- feature flags within the manifest's, store id and chunk coordinates
- per-frame magic, header CRC32 (covering the TLV area), frame flags, TLV
  entries, completeness, and frame CRC32
- per-record type, size, bounds, and shape
- the value rules for the state replay ends in (Section 4.1)

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

A pending transaction intent is a warning (`txn_rollback_pending`, `txn_commit_cleanup_pending`) and a malformed one an error (`txn_intent_invalid`).

Damaged `text` or `bytes` values show up as `chunk_image_invalid`, as `wal_damaged` or `wal_tail_truncated` with a `record_vars_*` reason, or as `wal_vars_inconsistent` (an error) when a WAL leaves values that break the rules of Section 3.2.

Exit code `0` means no findings, `1` means warnings or errors were reported, and `2`
means the run itself failed (bad arguments, unreadable directory).

## 8. Durability Notes

Durability guarantees depend on configured mode (`relaxed`, `fsync-wal`, `fsync-checkpoint`) and on `wal_group_commit_updates` in relaxed mode.
See [docs/CONCURRENCY.md](CONCURRENCY.md) for crash semantics details.
