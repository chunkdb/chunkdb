# Storage format

This reference describes the files written by chunkdb; format version numbers below belong to individual records.
All integers are little-endian; `uN` and `iN` denote unsigned and signed N-bit integers, and byte lengths count bytes rather than characters.
CRC32 uses the reflected polynomial `0xEDB88320`, initial value `0xFFFFFFFF`, and final XOR `0xFFFFFFFF`; checksums are stored as `u32`.
See [compatibility](COMPATIBILITY.md), [durability](DURABILITY_CONTRACT.md), [CQL](CQL.md), and [backup](BACKUP.md) for their user-visible contracts.

## File permissions

On Linux and macOS, directories chunkdb creates have mode 0700 and files have
mode 0600. On Windows, creation supplies a protected ACL granting full access
only to the effective user who creates the object; inherited grants are excluded.
The filesystem must support persistent ACLs (for example, NTFS); creation is
refused on filesystems such as FAT that ignore security descriptors.
This applies to data, staging, writer locks, WALs, images, metadata and backups.
Failure to apply these protections is an error.

Existing directory and data-file permissions are not changed in place; newly
written replacements use the private creation permissions above. On server startup,
an existing `chunkdb.users` is restricted to mode 0600 or an owner-only Windows
ACL before the registry reads it, with an info log when permissions change.
This also applies when authentication opens that registry in read-only mode.
Use the same operating-system account for later starts and offline tools.

## Directory layout

```text
data_dir/
  chunkdb.manifest                    data-directory identity, features, version floor
  chunkdb.users                       user verifiers and grants, when authentication is configured
  chunkdb.migrations                  completed named migration records, when present
  chunkdb.migration.pending           decided migration awaiting completion, when present
  .chunkdb.lock/                      writer-process lock
  .chunkdb.staging/                    table creation staging
  .chunkdb.dropped/                    tables renamed out of the catalog
  .chunkdb.backups/                    temporary pinned backup files
  tables/<name>/
    table.manifest                    identity, geometry, options, schema history
    chunkdb.version                   reserved revision ceiling
    chunkdb.snapshot                  checked writer-session crash fence
    chunkdb.snapshot.live             volatile mapped artifact generation
    .chunkdb.lock/snapshot.writer     writer-lifetime coordination lock
    .chunkdb.initialized              checked revision-clock initialization marker
    .chunkdb.intents/                  conditional and transaction recovery intents
    chunkdb.slots                     persisted feed slots, when present
    .chunkdb.feed/                     archived WALs and base images
    L_<lx>_<ly>/C_<cx>_<cy>.chk         checkpoint image
    L_<lx>_<ly>/C_<cx>_<cy>.wal         live delta log
```

Unless prefixed with `data_dir/`, paths below are relative to a table directory.
Chunk coordinates `(cx, cy)` are signed; large-chunk coordinates are their floor division by the manifest's large-chunk dimensions.
The catalog's directory names match `[a-z0-9][a-z0-9_-]{0,63}`, excluding `con`, `prn`, `aux`, `nul`, `com0`–`com9` and `lpt0`–`lpt9`; CQL identifiers follow the grammar in [CQL](CQL.md).
Entries in `tables/` with invalid names are ignored; a table directory with a valid name and no manifest is damage.

Creation prepares a complete table under `.chunkdb.staging/`, publishes it by a rename that cannot replace an existing table, then syncs both parents.
DROP raises and syncs the root version floor before moving the closed table into `.chunkdb.dropped/`.
The rename commits the drop; deletion can finish at startup.
Writer startup recovers decided migrations before interrupted-table cleanup and table opening.
A fresh catalog has no tables.

Chunks load lazily; opening the catalog does not load every chunk.

## Feature flags and manifests

Manifests, images, and WAL headers carry three `u32` feature sets: `incompat`, `ro_compat`, and `compat`.
Unknown `incompat` bits refuse opening; unknown `ro_compat` bits permit only read-only opening; unknown `compat` bits may be ignored.
The server opens read-write and therefore refuses either of the first two unknown sets.
Table incompat bit 0 (`0x1`) enables durable feed slots, USER WAL metadata, and collection-frame flags; it remains set after slot removal.

Data-directory incompat bit 1 (`0x2`) enables named migrations.
New directories enable every registered baseline feature before serving requests.
Existing directories retain their masks until explicit `ENABLE FEATURE`; slot creation and migrations never enable missing bits.
An image's or WAL header's features must be a subset of its table manifest's features.
Unknown option, image-section, WAL-TLV, or WAL-record types may be skipped only when an enclosing feature set has an unknown bit.
WAL parsing also considers the manifest’s features.

Skipping still validates bounds and checksums; unknown section/frame flag bits are invalid.
A `compat` extension can be discarded by a writer that does not understand it.

### Data-directory manifest

`data_dir/chunkdb.manifest` is 44 bytes plus its options, at most 64 KiB:

| Order | Field |
|---|---|
| 1 | `CKDM` (4 bytes), version `1` (`u16`), reserved zero (`u16`) |
| 2 | `incompat`, `ro_compat`, `compat` (`u32` each) |
| 3 | nonzero random `data_dir_id` (16 bytes) |
| 4 | options length (`u32`), options bytes |
| 5 | CRC32 of all preceding bytes (`u32`) |

Options are consecutive `type:u16, length:u16, value:length` entries filling the declared area.
Type 1 is `version_floor:u64`, the lower bound for a newly initialized table clock.
DROP raises it to at least the dropped table’s reserved ceiling, preventing token reuse after recreation.
Missing floor is zero; new clocks start at least at 1, and duplicate floor entries or a wrong length are invalid.

A writer creates a missing root manifest only when no chunkdb-owned entries exist, except its lock and unpublished manifest temporary files.
Foreign entries such as `lost+found` are preserved.
Read-only opening never initializes the directory.

### Table manifest

`table.manifest` is 64 bytes plus its options and schema, at most 1 MiB:

| Order | Field |
|---|---|
| 1 | `CKMF` (4 bytes), version `4` (`u16`), reserved zero (`u16`) |
| 2 | three feature sets (`u32` each) |
| 3 | large-chunk width/height, regular-chunk width/height (`u32` each) |
| 4 | nonzero random table `store_id` (16 bytes), also the feed epoch |
| 5 | options length (`u32`), options bytes |
| 6 | schema length (`u32`), schema bytes |
| 7 | CRC32 of all preceding bytes (`u32`) |

Large-chunk dimensions count regular chunks; regular-chunk dimensions count blocks.
Geometry is fixed by this manifest; conflicting requested geometry refuses opening before artifacts change.
The manifest is published before bookkeeping or chunk data, so a manifest-only table is valid and empty.
Publication syncs a temporary file, uses no-replace rename or an exclusive hard link, and syncs the directory; replacements sync the new file and directory too.

Schema/options changes and feed-feature activation replace the manifest atomically.
Incorrect magic/version, reserved fields, sizes, checksum, geometry, identity, or schema refuse opening.

Options use the same TLV layout as the root manifest:

| Type | Value | Default |
|---|---|---|
| 1 | durability `u8`: 0 relaxed, 1 fsync-wal, 2 fsync-checkpoint | 0 |
| 2 | checkpoint update interval `u64`, positive | 256 |
| 3 | checkpoint WAL-byte threshold `u64`, positive | 1048576 |
| 4 | WAL group-commit updates `u64`, positive | 8 |
| 5 | checkpoint compression `u8`: 0 none, 1 zrle | 0 |
| 6 | VARS-byte limit per chunk `u64`, 13–67108864 | 1048576 |
| 8 | live feed/watch byte limit `u64`, positive | current catalog/server default (67108864 unless configured) |
| 9 | retained byte limit per durable slot `u64`, positive | current catalog/server default (1073741824 unless configured) |
| 10 | immediate restore parent epoch, 16 nonzero bytes | absent |
| 11 | completed fork revision `u64`, including zero | absent |

Each known option appears at most once; absent options take defaults, and incorrect lengths or values are invalid. Types 8 and 9 are written only for explicit table overrides; leaving them absent preserves inheritance across restart and unrelated option changes. Type 7 is retired and is not a known option.

Types 10 and 11 belong to incompatibility feature `table_lineage` (`0x4`) and must appear together.
The parent epoch differs from the table's current epoch; the fork revision preserves its exact unsigned value.
New tables omit both fields.
Schema and option replacements preserve lineage and unknown compatible option entries.
Restore writes its checked fork-successor clock before publishing the new epoch and immediate-parent fields.
Committed image and WAL revisions retain their numeric values; reserved tokens belonging to the parent epoch do not set the restored frontier.

### Feature activation decision

`chunkdb.feature.pending` belongs to directory incompatibility feature `feature_activation` (`0x10`).
Its checked version-1 representation is:

| Order | Bytes |
|---|---|
| 1 | `CKFA`, version `u16` = 1, reserved zero `u16` |
| 2 | directory identity, 16 bytes |
| 3 | one registered incompatibility bit `u32`, target count `u32` |
| 4 | per target: name length `u8`, name bytes, table identity, 16 bytes |
| 5 | CRC32 `u32` over every preceding byte |

Targets appear in strictly increasing name order and cover the complete table inventory.
`feature_activation` itself uses one atomic root-manifest replacement and cannot appear as the journal's activation bit.
Names follow table-name rules; identities are nonzero.
The maximum target count is 65536; the file cannot exceed 16 MiB.
Malformed bounds, reserved fields, ownership, identities, inventory, or checksums prevent publication.

Recovery validates every target before changing a manifest, then unions the recorded bit into its registered owners.
Directory-only migrations do not copy their bit into table manifests.
It preserves current schema, options, lineage, and other feature masks.
Every replacement and affected directory is synced before the journal is removed and its parent directory is synced.
An interrupted replay repeats these unions without restoring stale manifest images.

### Schema area and history

The schema begins with `schema_version:u64` (1–65536), `next_column_id:u32`, `column_count:u32` (1–1024), followed by that many columns.
Each column is encoded in this order:

| Field | Encoding |
|---|---|
| column ID | `u32`, unique, positive, below `next_column_id` |
| kind | `u8`: 1 unsigned, 2 signed, 3 bool, 4 f32, 5 f64, 6 bits, 7 text, 8 bytes |
| size | `u32`: unsigned width 1–64; signed width 2–64; bool 1; floats 32/64; bits width 1–65535; text/bytes maximum 1–16777216 bytes |
| flags | `u8`: bit 0 nullable, bit 1 REQUIRED, bit 2 default present; nullable and REQUIRED cannot both be set |
| name | length `u8`, then 1–63 bytes matching `[a-z_][a-z0-9_]*`, unique in the schema |
| default, only with bit 2 | length `u32`, then bytes: fixed value takes `ceil(width/8)` with unused bits zero; text is UTF-8; variable defaults fit the declared maximum |

At least one column is fixed-width; the sum of fixed widths per block is at most 65535 bits.
Columns keep their IDs through renames and type changes; dropped IDs are not reused.
After the columns are exactly `schema_version - 1` history steps, oldest first; step `k` produced schema version `k + 1`.
A step is `change_count:u32` (1–2048), then each change's `kind:u8` (1 add, 2 drop, 3 rename, 4 type change), `position:u32`, and an encoded column.

The column is the added/dropped column or the post-rename/post-conversion column; position identifies its slot in the applicable column list.
Rename appends `old_name_length:u8, old_name`; type change appends the previous encoded column and `conversion:u8` (0 exact, 1 clamp, 2 default, 3 truncate).
Undoing steps reconstructs and validates every earlier schema used by stored chunks.
An optional final 10-byte narrowing suffix is `1:u8, column_id:u32, target_kind:u8, target_size:u32`; without pending narrowing there is no suffix.

While this suffix exists, writes to that column must fit both types; writer startup removes interrupted narrowing before opening the table.
Named migrations validate narrowing under exclusive admission before deciding and do not depend on this suffix for redo.

## Packed chunk state

For `B = chunk_width * chunk_height` blocks, each fixed column contributes `ceil(B * width/8)` value bytes followed, when nullable, by `ceil(B/8)` validity bytes.
These column sections appear in schema order; their sum is `payload_bytes`, at most 64 MiB.
Value `i` occupies bits `[i*width, (i+1)*width)`, least-significant bit first; signed values use two's complement, floats their IEEE 754 bits, and bool/unsigned/bits their bit values.
Validity bit `i = 1` means a nullable value exists.

The separate presence bitmap takes `ceil(B/8)` bytes; bit `i = 1` means that block is explicitly present, including an explicitly written all-zero block.
The stored fixed state is payload followed by presence; variable values are separate.
Absent blocks have zero value and validity bits; NULL values have zero value bits; all padding bits are zero.
A local block index is `local_y * chunk_width + local_x`, with local coordinates obtained by floor modulo, including for negative coordinates.

Bit `n` is bit `n % 8` of byte `n / 8`.
The full CQL chunk wire form has `chunk_version:u64, schema_version:u64, presence, payload, VARS`.
The two version fields occupy 16 bytes; presence precedes payload on the wire.
Selected-column forms contain the selected fixed sections and variable entries; see [CQL](CQL.md) for read/write rules.

### Variable values (VARS)

VARS is a sequence of `column_id:u32, block_index:u32, byte_length:u32, value:byte_length` entries, strictly ordered by `(column_id, block_index)` with no duplicates.
IDs identify text/bytes columns in the applicable schema.
Block indexes are below `B`, presence must be set, lengths fit the column maximum, and text bytes must be UTF-8.
Missing entries mean NULL for nullable columns and empty for other variable columns; non-nullable empty values are omitted.

An entry consumes `12 + byte_length` bytes toward the table's VARS limit; the hard maximum is 64 MiB per chunk.
The same encoding is used in images, chunk wire forms, and VAR_REPLACE records.

## Checkpoint images (`.chk`)

An image has the following fields, a section directory, a header checksum, then section bodies:

| Order | Field |
|---|---|
| 1 | `CHKIMAGE` (8 bytes), version `1:u16`, section count `u16` (at most 64) |
| 2 | three feature sets (`u32` each), table StoreId (16 bytes) |
| 3 | chunk x/y (`i64` each), nonzero revision (`u64`), commit time in Unix ms (`u64`) |
| 4 | section directory, one 16-byte entry per section |
| 5 | header CRC32 over everything preceding it |
| 6 | section bodies, in directory order, filling the remainder exactly |

A directory entry is `type:u16, flags:u16, stored_size:u32, raw_size:u32, raw_crc32:u32`.
Only flag bit 0 is defined: the body uses zrle; an uncompressed body has equal stored/raw sizes.
Types are strictly ascending, with no duplicates:

| Type | Section | Raw body |
|---|---|---|
| 1 | PAYLOAD, required | fixed payload of the stored schema |
| 2 | PRESENCE, required | `ceil(B/8)` bytes |
| 3 | VARS, when entries exist | nonempty VARS encoding, at most 64 MiB |
| 4 | SCHEMA | schema version `u64`, never compressed |

Absent SCHEMA means version 1; writers include it when the schema version exceeds 1.
A schema beyond the table's current version is damage; section sizes are checked against the stored schema and table geometry before translation.
No section’s `raw_size` may exceed 64 MiB, the largest payload or VARS body.
This bound applies before decompression, including sections owned by unknown features.

The header is `76 + 16 * section_count` bytes: 108 bytes for two sections, 124 for three.
Readers accept compressed and uncompressed bodies regardless of the current compression option; writers leave SCHEMA uncompressed.

### zrle

```text
codec_id:u8 = 1, uncompressed_size:u32, token...
token = 0:u8, run_length:ULEB128                  zero bytes
      | 1:u8, run_length:ULEB128, literal bytes   literal bytes
```

The expected raw size comes from the section's validated size; declared and produced sizes must match it exactly.
Truncated/oversized varints, unknown token kinds, truncated literal runs, and output overruns are invalid.
The section CRC checks the decompressed bytes.

## Delta logs (`.wal`)

A WAL begins with a 60-byte header.
It contains `CHKWALOG` (8 bytes), version `1:u16`, reserved zero `u16`, and three feature sets (`u32` each).
Then come table StoreId (16 bytes), chunk x/y (`i64` each), and CRC32 over the first 56 bytes.
The writer creates the header in one append; the body consists of mutation frames.

One frame represents a block mutation, a whole-chunk mutation, or one written chunk of a transaction.

### Frames and records

| Order | Field |
|---|---|
| 1 | `FRM2` (4 bytes) |
| 2 | revision `u64`, commit time in Unix ms `u64` |
| 3 | flags `u16`, TLV-area size `u16`, record count `u32` (positive), record-body size `u32` |
| 4 | TLV area: repeated `type:u16, length:u16, value:length` |
| 5 | header CRC32 over fields 2–4, excluding magic |
| 6 | records: repeated `type:u8, size:u32, body:size`, filling the declared record-body size |
| 7 | CRC32 over all bytes of the records |

Flags are normally zero; bit 0 denotes an empty-collection maintenance frame and requires the feed-slots feature; other bits are invalid.
TLVs fill their declared area exactly; each known type appears at most once:

| Type | TLV body |
|---|---|
| 1 TAG | nonempty opaque bytes |
| 2 SCHEMA | schema version `u64`; absent means version 1 |
| 3 USER | nonempty writer-identity bytes, requiring the feed-slots feature; absent for anonymous writes |

All TLVs including their four-byte headers must fit the 65535-byte area; TAG's usable size therefore depends on other metadata in the frame.
Collection frames have no TAG and zero spans for the full payload/presence.
They have an empty VAR_REPLACE when the schema has variable columns.
Archives apply them without an extra user change.

| Type | Record body |
|---|---|
| 1 SPAN | `offset:u32` followed by at least one state byte |
| 2 VAR_PUT | one VARS entry |
| 3 VAR_DEL | `column_id:u32, block_index:u32` |
| 4 VAR_REPLACE | complete VARS encoding, possibly empty |

A SPAN is wholly within payload, wholly within presence, or covers the complete fixed state; full replacement uses separate payload and presence spans.
Variable records require variable columns; PUT/DEL keys are strictly increasing, or one REPLACE appears without PUT/DEL.
PUT overwrites a value, DEL removes it if present, and REPLACE replaces all variable values.
Every record, frame checksum, metadata field, and bound is validated before any part of that frame is applied.

Frame revisions strictly increase; times within a chunk/store instance do not decrease.
Frames at or below an image's revision are validated and skipped, since their effects are already represented by the image.
Frames use their declared schema.
Replay translates forward between schema versions, then to the table’s current version.

Backward schema order or an unknown future version is damage.
Translation preserves presence, adds defaults/NULL/zero for added columns in present blocks, drops removed columns, and uses the history's conversion for changed types.
Clamp selects the nearest representable value; default uses DEFAULT, otherwise NULL or zero/empty; truncate retains leading bits/bytes and stops text at a UTF-8 boundary.
The final variable state must obey the VARS presence, type, ordering, UTF-8, and size rules.

### Live-WAL recovery

A writer replays a valid image, or an empty state when none exists, followed by committed frames from its live WAL.
A failed header is recoverable only with the interrupted-creation shape: an expected header prefix followed by zero bytes.
Feature-byte differences are ignored.
Incomplete CRC bytes are allowed after the first 56 header bytes match.

Other invalid headers are damage; interrupted creation contributes no frames and can be removed.
A trailing incomplete/bad-checksum frame can be discarded only when no later checksum-valid frame header exists.
A checksum-valid malformed frame or evidence of later framing is damage and is not repaired.
Accepted frames are atomic even when their bodies are large; a discarded frame contributes no records.

Repair truncates to the accepted boundary and syncs it; when backup hard links pin the inode, destructive truncation uses a synced replacement inode.
Archived WALs are closed segments and require complete valid framing throughout; the live trailing-append policy does not apply to them.
Persisted revisions survive eviction and restart; a missing chunk artifact gets a fresh revision when loaded.
The store clock reserves ranges durably before issuing tokens and advances past any recovered revision at or above its ceiling.

## Revision and snapshot bookkeeping

| File | Exact 16-byte encoding |
|---|---|
| `.chunkdb.initialized` | `CKID`, value `1:u64`, CRC32 over the first 12 bytes |
| `chunkdb.version` | `CKVR`, nonzero exclusive reserved ceiling `u64`, CRC32 over the first 12 bytes |
| `chunkdb.snapshot` | `CKSG`, generation `u64`, CRC32 over the first 12 bytes |

Clock reservations sync their complete record and directory entry before tokens are issued; first initialization publishes the clock before the initialized marker.
A valid initialized marker with missing, unreadable, or malformed clock is damage and refuses writer opening.
Missing both records with chunk artifacts or a capped startup inventory refuses writer opening before publishing recovery records.
Manifest-only and empty initialization remain valid; read-only inspection can still read retained committed versions without creating these records.

The checked snapshot record is a writer-session crash fence; a missing record is implicit stable zero.
A writer durably advances it to a new odd session before recovery and leaves it odd while open.
After coherent shutdown it syncs a new even close record and its directory entry.
Malformed checked records refuse opening; recovery advances the session instead of resetting it.

For a live writer, `chunkdb.snapshot.live` contains 32 bytes: `CKSL`, format `1:u32`, session `u64`, CRC32 over the first 16 bytes, zero `u32`, and an aligned native little-endian `u64` counter at offset 24.
The header is immutable during a session; the counter is outside its CRC.
Writer startup replaces this volatile file once; artifact guards update only its shared mapped counter, without file sync or rename.
Bit 0 is odd while publishing, bit 1 records a failed epoch, bits 2–17 count active guards, and bits 18–63 count completed epochs.
The last successful guard publishes a new even counter synchronously; failed epochs remain odd until recovery.
Lock-free aligned 64-bit mapped atomics are required; unsupported platforms are refused.

The writer holds an exclusive OS lock on `.chunkdb.lock/snapshot.writer` through shutdown.
Read-only snapshots accept a live session only while this ownership exists and its checked live header matches.
They require equal session and even counter observations around the complete artifact collection; a restarted writer cannot reuse the previous session.
A dead writer's odd crash fence refuses reads even if cached mapped counter bytes are even.
Rollback intents bound readable WAL prefixes; exhausted concurrent-snapshot retries fail without modifying files.
Only volatile live metadata is rebuilt after a crash; actual data and WAL validation and durability are unchanged.
Older readers that do not understand the live counter conservatively reject an open writer's odd fence; closed tables retain the checked CKSG encoding.

## Conditional and transaction intents

Intents live under `.chunkdb.intents/`; their publication and removal are file/directory synced in every durability mode.
A conditional intent is named from its WAL's relative path with `/` replaced by `__`, plus `.rollback`, for example `L_0_0__C_0_0.wal.rollback`.
Its exact 16-byte record is `CKRB` (rollback) or `CKRC` (committed), pre-command WAL byte length `u64`, then CRC32 over the first 12 bytes.
A transaction intent is `txn-<decimal revision>.rollback` and has `20 + 24 * count` bytes:

1. `CKTB` (rollback) or `CKTC` (committed), transaction revision `u64`, written-chunk count `u32` (1–64).
2. Per chunk: x/y `i64`, pre-commit WAL byte length `u64`; zero means no preceding WAL.
3. CRC32 over all preceding bytes.

Before mutation, writers establish a durable baseline and publish the rollback intent.
After syncing the new WAL frames, atomic synced replacement with CKRC/CKTC commits the operation.
Every changed transaction chunk uses the same revision and commit time.
Recovery validates all transaction intents before changing WALs, resolves conditional intents first, then transaction intents.

Rollback truncates/removes each WAL to its recorded boundary; a missing or shorter nonzero prefix is damage.
Committed intents preserve WAL effects, including WALs subsequently checkpointed away; removal and directory sync finish recovery idempotently.
Failure repairing an uncommitted operation fences the affected store until recovery; post-commit cleanup does not turn a committed operation into a rejected one.

## Feed slots and archives

`chunkdb.slots` is at most 1 MiB, atomically replaced and file/directory synced in every durability mode:

1. `CKSL`, version `1:u16`, reserved zero `u16`, table epoch (16 bytes).
2. Durable watermark `u64`, slot count `u32` (at most 1024).
3. Per slot: name length `u8`, lost flag `u8` (0/1), reserved zero `u16`, written revision `u64`, name bytes.
4. CRC32 over all preceding bytes.

Names are unique, 1–63 bytes matching `[a-z_][a-z0-9_]*`; every written revision is at or below the watermark, and epoch matches the table StoreId.
Lost records remain visible with their loss flag; advancing/watching a lost slot refuses it, and explicit DROP removes it.
The first slot activation quiesces writers/maintenance/eviction and checkpoints preceding WALs before selecting a durable frontier.
Checkpoint archives use `.chunkdb.feed/C_<cx>_<cy>.<first>.chk` for the hard-linked old base image, when one existed, and `C_<cx>_<cy>.<first>-<last>.wal` for a closed WAL range.

The base and its directory are durable before publishing the new live image; archival WAL rename syncs both archive and live directories.
Recovery distinguishes an archived base from the newer live image and removes a live name aliasing an archived inode; independent overlapping live/archive ranges are damage.
Retention preserves segments needed by persisted active-slot positions and pinned readers, including pending bases for live WALs.
Active readers keep live rollover archival even if the last slot is dropped.

Live-history readers capture completed byte boundaries and read only the prefix at or below the persisted frontier; a shortened accepted prefix is damage.

## Users

`data_dir/chunkdb.users` is atomically replaced with file and directory sync:

1. `CKDU`, version `1:u32`, server secret (32 bytes), user count `u32`.
2. Each user stores these fields in order:

- Name length `u16` and name.
- Flags `u8`, with bit 0 representing MANAGES USERS.
- SCRAM iterations `u32`, salt length `u16`, and salt.
- Stored key (32 bytes), server key (32 bytes), and grant count `u32`.
3. Per grant: table-name length `u16` and name, right `u8` (1 READ, 2 WRITE, 3 ADMIN); `*` names all tables.
4. CRC32 over all preceding bytes.

Writer serialization orders users by name and grants by table; decoding refuses duplicate users/grants, unknown flags/rights/version, bad checksum, truncation, or trailing bytes.
Password verifiers contain no plaintext password; the secret derives unknown-user salts used by authentication.

## Named migrations

`data_dir/chunkdb.migrations` is a completed ledger bound to the data-directory identity, at most 16 MiB and 16384 records:

1. `CKML`, version `1:u16`, reserved zero `u16`, data-directory ID (16 bytes), record count `u32`.
2. Each record: name length `u32` and name, applied Unix milliseconds `u64`, user length `u32` and user, statement length `u32` and statement.
3. CRC32 over all preceding bytes.

Names are unique identifiers matching `[a-z_][a-z0-9_]*`, 1–63 bytes; users are empty for auth none or valid user names of at most 63 bytes.
Times are positive and fit a signed protocol integer; record order is append order regardless of clock changes.
Statements are nonempty UTF-8, at most 65536 bytes, without CR/LF/NUL; completed-ledger decoding validates bytes without reparsing them as current CQL.
MIGRATE preserves keyword case and interior whitespace after trimming its separator and trailing spaces/tabs, and requires one supported schema statement without parameter frames.

`data_dir/chunkdb.migration.pending` is the durable redo decision, at most 64 MiB:

1. `CKMJ`, version `1:u16`, reserved zero `u16`, data-directory ID (16 bytes), one record encoded as above.
2. Directory action `u8` (0 none, 1 create, 2 drop), table length `u32` and name, table StoreId (16 bytes), operation-name length `u32` and name.
3. Participant count `u32` (1–5); per participant: relative-path length `u32` and path, before-image-present `u8` (0/1), optional before-image length `u32` and bytes, after-image length `u32` and bytes.
4. CRC32 over all preceding bytes.

Each participant image is at most 16 MiB and must validate as its native format with the expected identities; descendant symlinks are refused.
The ledger participant is last and must equal the previous ledger plus exactly the journal's record.
Required participants depend on the operation:

- CREATE/ALTER: table manifest.
- DROP: root manifest, plus users when that file exists.
- GRANT/REVOKE: users.
- CREATE SLOT: table manifest and slots.
- DROP SLOT: slots.

Every operation includes the ledger and rejects unrelated participants.
CREATE prepares its exact manifest under `.chunkdb.staging/<table>.<16 lowercase hex digits>/`; DROP uses the corresponding `.chunkdb.dropped/` operation name.
Synced atomic journal publication decides the migration.
Completion publishes DROP’s version floor first, applies any directory move, replaces metadata, publishes the ledger, then removes/syncs the journal.

Users involved in DROP/GRANT/REVOKE remain serialized through publication; target table leases and slot publishers are drained before preparation.
After a decided error, the catalog refuses commands until restart.
Unfinished completion retains its journal; failure reopening after completion may leave only the durable ledger.
Writer recovery validates every participant before changing any.

Current bytes must equal before or after bytes, and prepared CREATE must match its image.
A directory move must have exactly one expected identity at its two names.
Recovery redoes exact images rather than executing statement text; read-only and multi-process opening refuse pending recovery.
Malformed, foreign, or inconsistent journal state refuses opening without partial completion.

## Online backup records

Completed copies use the same metadata/image/WAL formats and add `chunkdb.backup`:

1. `CKBP`, version `1:u16`, reserved zero `u16`, creation Unix milliseconds `u64`, table count `u32`.
2. Each table: name length `u32` and name, epoch (16 bytes), completed revision cut `u64`.
3. File count `u32`; each file: relative-path length `u32` and path, length `u64`, CRC32 `u32`.
4. CRC32 over all preceding bytes.

Inventory paths use canonical `/` separators; absolute roots, drive names, backslashes, NUL, empty components, `.` and `..` are invalid before path conversion/access.
The inventory includes root metadata, users/ledger when present, table definitions/history, and clocks above cuts.
It also includes initialized markers, stable snapshot generations, slots, images, and bounded WAL copies.
It excludes archives and recovery intents.

Named migration publication and backup metadata capture exclude each other, keeping ledger, schemas and grants coherent; the metadata hold ends before bulk file copying.
Backup waits for an active migration to release metadata admission, then captures its completed outcome.
A fenced catalog or leftover pending decision requires recovery and refuses backup.
For table enumeration, tables dropped before pinning are omitted from the record, and tables created after enumeration are outside the copy.

Resident pinning flushes staged frames under chunk locks; cold chunks are hard-linked without loading payloads, and file validation/copying occurs after the pin phase.
Accepted frames above a table's cut are excluded; a cold WAL with no such frames preserves its captured whole file, including a recoverable interrupted tail.
`.chunkdb.backup.incomplete` contains `CKBI`; `.chunkdb.restore.incomplete` contains `CKRI`; either guard takes precedence over a completed record.
Normal catalog/direct-table opening refuses these guards and the completed backup marker; verification checks inventory, checksums, identities, framing, and revision cuts.

Backup staging names are `<data_dir_id hex32>.<nonce hex32>` under `.chunkdb.backups/`; their source identity records ownership even before writing `.chunkdb.backup.owner`.
The owner record is exactly 56 bytes: `CKBS`, source ID (16 bytes), nonce (32 lowercase ASCII hex bytes), CRC32 over the first 52 bytes.
Each backup, before it stages files, and each single-process writer start remove source-owned staging directories.
They preserve foreign, malformed, nonce-only and symlink entries, and entries holding a backup marker or guard.
A writer start without the writer lock leaves backup staging in place.
A failed removal is logged as a warning and retried by the next backup or writer start.
Restore validates the complete inventory before publication and gives the root and every table fresh identities.

It rewrites image/WAL header identities/checksums and re-encodes the ledger for the new root without changing its records.
Retained slots start at their table’s cut in the fresh epoch, with a new baseline and no historical archive transfer.
Recoverable WAL tails are normalized before rewriting.
Restore removes the backup marker and publishes the destination with its incomplete guard held until final durable completion.

## Checkpoint and verification boundaries

A checkpoint publishes a complete image through atomic temporary-file replacement before removing or archiving its WAL.
Synced modes, retained history, or the established durability floor require file/directory syncs.
A fresh relaxed store before its first barrier can omit them. Replay validates/skips frames already represented by the image.
Empty-chunk collection records the empty state when needed, removes the image before the live WAL, and retains the durable revision floor.

Explicit present-zero blocks are not collected.
Temporary files use `<target>.tmp.<pid>.<thread>.<clock>.<sequence>` names; writer recovery cleans recognized unpublished artifacts, while read-only opening and verification do not modify them.
`chunkdb_verify` checks manifests, bookkeeping, intents, images/WALs, archives/slots, migrations, and backup records without repairing them.
Its exit status is 0 without warnings/errors, 1 with warnings/errors, and 2 for invalid arguments or an unreadable target.

Findings carry stable codes and relative paths.
See [durability](DURABILITY_CONTRACT.md) for acknowledgement, FLUSH, filesystem sync, and recovery guarantees.
