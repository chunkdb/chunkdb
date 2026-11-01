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
Read-only opening leaves the file and its permissions unchanged and logs a warning when it is not owner-only.
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
    .chunkdb.lock/snapshot.owner      lock that excludes a second writer
    .chunkdb.initialized              checked revision-clock initialization marker
    .chunkdb.intents/                  conditional and transaction recovery intents
    chunkdb.slots                     persisted feed slots, when present
    chunkdb.index                     checked shard inventory and deletion floor
    .chunkdb.feed/                     archived WALs and base images
    L_<lx>_<ly>/C_<cx>_<cy>.chk         checkpoint image
    L_<lx>_<ly>/C_<cx>_<cy>.wal         live delta log
    L_<lx>_<ly>/chunkdb.index          checked coordinate decisions, when listed
```

Unless prefixed with `data_dir/`, paths below are relative to a table directory.
Coordinates are signed and contain exactly the table's two or three active axes.
Three-dimensional names append `_<z>` to each large-directory and chunk stem.
Large-chunk coordinates are floor division by the manifest's large-chunk dimensions.
Noncanonical names, wrong axis counts, or artifacts placed in another large directory are invalid.
The catalog's directory names match `[a-z0-9][a-z0-9_-]{0,63}`, excluding `con`, `prn`, `aux`, `nul`, `com0`–`com9` and `lpt0`–`lpt9`; CQL identifiers follow the grammar in [CQL](CQL.md).
Entries in `tables/` with invalid names are ignored; a table directory with a valid name and no manifest is damage.

Creation prepares a complete table under `.chunkdb.staging/`, publishes it by a rename that cannot replace an existing table, then syncs both parents.
DROP raises and syncs the root version floor before moving the closed table into `.chunkdb.dropped/`.
The rename commits the drop; deletion can finish at startup.
Writer startup recovers decided migrations before interrupted-table cleanup and table opening.
RECOVER TABLE repeats the last two steps for one table: it removes that table's entries of `.chunkdb.staging/` and `.chunkdb.dropped/`, then opens `tables/<name>/` when it exists.
It never completes a decided migration; a pending decision fences the catalog until writer startup.
A fresh catalog has no tables.

Chunks load lazily; opening the catalog does not load every chunk.

## Feature flags and manifests

Manifests, images, and WAL headers carry three `u32` feature sets: `incompat`, `ro_compat`, and `compat`.
Unknown `incompat` bits refuse opening; unknown `ro_compat` bits permit only read-only opening; unknown `compat` bits may be ignored.
The server opens read-write and therefore refuses either of the first two unknown sets; with `--read-only` it refuses only unknown `incompat` bits.
Table incompat bit 0 (`0x1`) enables durable feed slots, USER WAL metadata, and collection-frame flags; it remains set after slot removal.
Directory and table incompat bit 5 (`0x20`), named `chunk_model`, owns dimensional geometry, both column domains, sectioned forms, and retained coordinate decisions.
Directory and table incompat bit 6 (`0x40`), named `commit_groups`, owns checked group decisions, grouped WAL metadata, durability receipts and retained result identities.
It also requires an immutable manifest owner scope and the checked canonical `chunkdb.groups.owner`; a missing claimed owner is damage.
Earlier development envelopes are refused before artifact mutation; they require recreating the data directory.

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
| 1 | `CKDM` (4 bytes), format tag `2` (`u16`), reserved zero (`u16`) |
| 2 | `incompat`, `ro_compat`, `compat` (`u32` each) |
| 3 | nonzero random `data_dir_id` (16 bytes) |
| 4 | options length (`u32`), options bytes |
| 5 | CRC32 of all preceding bytes (`u32`) |

Options are consecutive `type:u16, length:u16, value:length` entries filling the declared area.
Type 1 is `version_floor:u64`, the lower bound for a newly initialized table clock.
DROP raises it to at least the dropped table’s reserved ceiling, preventing token reuse after recreation.
Missing floor is zero; new clocks start at least at 1, and duplicate floor entries or a wrong length are invalid.
Type 2 is the 40-byte commit-owner scope and is present exactly when `commit_groups` is set.
It contains nonzero directory and owner identities (16 bytes each), role `u8` and seven zero bytes; catalog role is 2 and its directory identity equals `data_dir_id`.

A writer creates a missing root manifest only when no chunkdb-owned entries exist, except its lock and unpublished manifest temporary files.
Foreign entries such as `lost+found` are preserved.
Read-only opening never initializes the directory.

### Table manifest

`table.manifest` is 80 bytes plus its options and two schema areas, at most 1 MiB:

| Order | Field |
|---|---|
| 1 | `CKMF` (4 bytes), format tag `5` (`u16`), reserved zero (`u16`) |
| 2 | three feature sets (`u32` each) |
| 3 | dimensions `u8` (2 or 3), three zero bytes, large x/y/z and regular x/y/z sides (`u32` each) |
| 4 | nonzero random table `store_id` (16 bytes), also the feed epoch |
| 5 | options length (`u32`), options bytes |
| 6 | block-schema length (`u32`), block-schema bytes |
| 7 | chunk-schema length (`u32`), chunk-schema bytes |
| 8 | CRC32 of all preceding bytes (`u32`) |

Large-chunk dimensions count regular chunks; regular-chunk dimensions count blocks.
Geometry is fixed by this manifest; conflicting requested geometry refuses opening before artifacts change.
Inactive z side lengths are 1, and inactive coordinates are zero.
A complete empty `chunkdb.index` is durable before publishing the manifest or catalog name; a missing inventory is damage.
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
| 12 | immutable 40-byte commit-owner scope | required exactly with `commit_groups` |

Catalog tables repeat their directory's exact scope and require its admitted directory context when opened.
A standalone table uses role 1 with its own `store_id` as the scope's directory identity; opening an isolated catalog table does not infer standalone ownership.
Scope is separate from mutable options and survives schema, option and lineage replacements.

Each known option appears at most once; absent options take defaults, and incorrect lengths or values are invalid. Types 8 and 9 are written only for explicit table overrides; leaving them absent preserves inheritance across restart and unrelated option changes. Type 7 is retired and is not a known option.

Types 10 and 11 belong to incompatibility feature `table_lineage` (`0x4`) and must appear together.
The parent epoch differs from the table's current epoch; the fork revision preserves its exact unsigned value.
New tables omit both fields.
Schema and option replacements preserve lineage and unknown compatible option entries.
Restore writes its checked fork-successor clock before publishing the new epoch and immediate-parent fields.
Committed image and WAL revisions retain their numeric values; reserved tokens belonging to the parent epoch do not set the restored frontier.

### Feature activation decision

`chunkdb.feature.pending` belongs to directory incompatibility feature `feature_activation` (`0x10`).
New decisions use checked version 2:

| Order | Bytes |
|---|---|
| 1 | `CKFA`, version `u16` = 2, reserved zero `u16` |
| 2 | directory identity, 16 bytes |
| 3 | one registered incompatibility bit `u32`, target count `u32` |
| 4 | owner scope, 40 bytes: required for `commit_groups`, otherwise all zero |
| 5 | per target: name length `u8`, name bytes, table identity, 16 bytes |
| 6 | CRC32 `u32` over every preceding byte |

Targets appear in strictly increasing name order and cover the complete table inventory.
`feature_activation` itself uses one atomic root-manifest replacement and cannot appear as the journal's activation bit.
Names follow table-name rules; identities are nonzero.
The maximum target count is 65536; the file cannot exceed 16 MiB.
Historical tag-1 decisions omit the scope field and remain readable for known nongroup features; tag 1 cannot activate `commit_groups`.
Malformed bounds, reserved fields, ownership, identities, inventory, or checksums prevent publication.

Recovery validates every target before changing a manifest, then unions the recorded bit into its registered owners.
Directory-only migrations do not copy their bit into table manifests.
It preserves current schema, options, lineage, and other feature masks.
Every replacement and affected directory is synced before the journal is removed and its parent directory is synced.

Initial owner creation uses the bounded `chunkdb.bootstrap.pending` CKGB decision, whose complete target manifest fixes both identities before publication.
The checked empty inventory and generation-one INACTIVE owner are durable before the manifest claims them; only then is the decision removed and its directory synced.
The owner is a 512-byte CKGO root with no selected references or counter in this initial state.
Read-only startup refuses pending bootstrap, and recovery never recreates an owner lost after its manifest was committed.
The exact owner and creation envelopes are specified in [owner format](design/COMMIT_GROUP_OWNER_FORMAT.md) and [bootstrap format](design/COMMIT_OWNER_BOOTSTRAP.md).
An interrupted replay repeats these unions without restoring stale manifest images.

### Schema area and history

Each schema begins with `schema_version:u64` (1–65536), `next_column_id:u32`, and `column_count:u32`, followed by that many columns.
The block domain has 1–1024 columns; the chunk domain permits 0–1024 columns.
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
Kind 5 is an other-domain marker containing only its kind byte.
Both domains have the same revision; at every step exactly one domain changes columns and the other records that marker.

The column is the added/dropped column or the post-rename/post-conversion column; position identifies its slot in the applicable column list.
Rename appends `old_name_length:u8, old_name`; type change appends the previous encoded column and `conversion:u8` (0 exact, 1 clamp, 2 default, 3 truncate).
Undoing steps reconstructs and validates every earlier schema used by stored chunks.
An optional final 10-byte narrowing suffix is `1:u8, column_id:u32, target_kind:u8, target_size:u32`; without pending narrowing there is no suffix.

While this suffix exists, writes to that column must fit both types; writer startup and RECOVER TABLE remove interrupted narrowing before opening the table.
Named migrations validate narrowing under exclusive admission before deciding and do not depend on this suffix for redo.

## Packed chunk state

For `B = chunk_width * chunk_height * chunk_depth` blocks, each fixed column contributes `ceil(B * width/8)` value bytes followed, when nullable, by `ceil(B/8)` validity bytes.
These column sections appear in schema order; their sum is `payload_bytes`, at most 64 MiB.
Value `i` occupies bits `[i*width, (i+1)*width)`, least-significant bit first; signed values use two's complement, floats their IEEE 754 bits, and bool/unsigned/bits their bit values.
Validity bit `i = 1` means a nullable value exists.

The separate presence bitmap takes `ceil(B/8)` bytes; bit `i = 1` means that block is explicitly present, including an explicitly written all-zero block.
The stored fixed state is payload followed by presence; variable values are separate.
Absent blocks have zero value and validity bits; NULL values have zero value bits; all padding bits are zero.
A local block index is `local_x + chunk_width * (local_y + chunk_height * local_z)`, with local coordinates obtained by floor modulo, including for negative coordinates.

Bit `n` is bit `n % 8` of byte `n / 8`.
The CQL wire form has a checked `CKCF` header and section directory; see [Protocol](PROTOCOL.md#chunk-form).
Chunk metadata is a separate checked row using stable IDs, null flags, lengths, and scalar bytes.
Its nondefault values make an otherwise empty chunk populated.

### Variable values (VARS)

VARS is a sequence of `column_id:u32, block_index:u32, byte_length:u32, value:byte_length` entries, strictly ordered by `(column_id, block_index)` with no duplicates.
IDs identify text/bytes columns in the applicable schema.
Block indexes are below `B`, presence must be set, lengths fit the column maximum, and text bytes must be UTF-8.
Missing entries mean NULL for nullable columns and empty for other variable columns; non-nullable empty values are omitted.

An entry consumes `12 + byte_length` bytes toward the table's VARS limit; the hard maximum is 64 MiB per chunk.
Block variable values and chunk-metadata variable values share this limit; fixed metadata and directories do not count toward it.
The same encoding is used in images, chunk wire forms, and VAR_REPLACE records.

## Checkpoint images (`.chk`)

An image has the following fields, a section directory, a header checksum, then section bodies:

| Order | Field |
|---|---|
| 1 | `CHKIMAGE` (8 bytes), format tag `2:u16`, section count `u16` (at most 64) |
| 2 | dimensions `u8`, three zero bytes, three feature sets (`u32` each), table StoreId (16 bytes) |
| 3 | each active chunk coordinate (`i64`), nonzero revision (`u64`), commit time in Unix ms (`u64`) |
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
| 5 | CHUNK_COLUMNS | complete metadata row, required when the stored chunk schema is nonempty |

Absent SCHEMA means version 1; writers include it when the schema version exceeds 1.
A schema beyond the table's current version is damage; section sizes are checked against the stored schema and table geometry before translation.
Ordinary sections are bounded at 64 MiB; the metadata section additionally permits its checked scalar directory and fixed values.
Stored file bytes and total raw section bytes are each bounded at 256 MiB, before decompression or body allocation.
Unknown sections remain subject to explicit feature ownership and byte limits.

The header is `64 + 8 * dims + 16 * section_count` bytes, including its checksum.
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

A WAL begins with a `48 + 8 * dims` byte header.
It contains `CHKWALOG` (8 bytes), format tag `2:u16`, reserved zero `u16`, dimensions `u8`, three zero bytes, and three feature sets (`u32` each).
Then come table StoreId (16 bytes), each active chunk coordinate (`i64`), and CRC32 over all preceding header bytes.
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
| 4 GROUP | nonzero 16-byte group identity, `u64` member count, `u64` member ordinal; requires `commit_groups` |

All TLVs including their four-byte headers must fit the 65535-byte area; TAG's usable size therefore depends on other metadata in the frame.
GROUP is exactly 32 bytes, its count is positive and its ordinal is below that count.
A structurally valid reference does not authorize a frame without the checked directory group decision and exact member proof.
Collection frames have no TAG and zero spans for the full payload/presence.
They have an empty VAR_REPLACE when the schema has variable columns.
Archives apply them without an extra user change.

| Type | Record body |
|---|---|
| 1 SPAN | `offset:u32` followed by at least one state byte |
| 2 VAR_PUT | one VARS entry |
| 3 VAR_DEL | `column_id:u32, block_index:u32` |
| 4 VAR_REPLACE | complete VARS encoding, possibly empty |
| 5 CHUNK_COLUMNS | complete checked metadata row, at most once per frame |

A SPAN is wholly within payload, wholly within presence, or covers the complete fixed state; full replacement uses separate payload and presence spans.
Variable records require variable columns; PUT/DEL keys are strictly increasing, or one REPLACE appears without PUT/DEL.
PUT overwrites a value, DEL removes it if present, and REPLACE replaces all variable values.
Every record, frame checksum, metadata field, and bound is validated before any part of that frame is applied.
A complete frame is bounded at 256 MiB before allocation.
Block and metadata changes within one frame share its producing identity and commit time.
Tables without chunk columns emit no metadata record.

Frame revisions strictly increase; times within a chunk/store instance do not decrease.
An already known producing revision must retain its exact original time, including a retained tombstone overlapped by a newer image.
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

A writer first takes an exclusive lock on `.chunkdb.lock/snapshot.owner`, which readers never open, so a second writer is refused.
It then tests `.chunkdb.lock/snapshot.writer` with a shared lock, refusing an exclusive holder, and waits for its exclusive lock, which it holds through shutdown.
Readers probe that file with a shared lock they drop at once, so a probe delays a starting writer; a writer still waiting after 5 seconds fails with an error naming a reader that holds its probe.
Read-only snapshots accept a live session only while this ownership exists and its checked live header matches.
They require equal session and even counter observations around the complete artifact collection; a restarted writer cannot reuse the previous session.
A read-only store keeps a loaded chunk while a later stable observation equals the one it was read at, and loads it again otherwise.
A dead writer's odd crash fence refuses read-only opening and reads while no writer holds the lease, even if cached mapped counter bytes are even.
A restarting writer takes the lease before it replaces `chunkdb.snapshot.live`; until then a reader can accept the dead session when its mapped counter is even, and reads the files as that writer left them.
Rollback intents bound readable WAL prefixes; exhausted concurrent-snapshot retries fail without modifying files.
Only volatile live metadata is rebuilt after a crash; actual data and WAL validation and durability are unchanged.
Older readers that do not understand the live counter conservatively reject an open writer's odd fence; closed tables retain the checked CKSG encoding.

## Coordinate index and tombstones

The table's `chunkdb.index` is a checked `CKII` inventory listing only shards with retained index decisions.
Its 60-byte fixed header is magic, format tag `4:u16`, dimensions `u8`, pending-compaction flag `u8`, table identity (16 bytes), deletion floor `u64`, operation-through bound `u64`, shard count `u64`, monotonic nonzero generation `u64`, and header CRC32.
Each sorted unique shard entry contains its active large coordinates (`i64` each), committed log bytes `u64`, replacement bytes `u64`, and a 32-byte SHA-256 replacement digest.
A final CRC32 covers the header and all entries; entry count and total length are checked before streaming them.
Replacement fields are nonzero only for a pending compaction decision.

Each listed `L_<coordinates>/chunkdb.index` starts with `CKIX`, format tag `1:u16`, dimensions `u8`, zero `u8`, table identity, active large coordinates, and CRC32.
Records contain total length `u32`, kind `u8` (1 live, 2 deleted, 3 forget), three zero bytes, active chunk coordinates, producing revision `u64`, time `u64`, schema revision `u64`, and CRC32.
The inventory's committed boundary must cover a complete valid header and whole records; a shorter log or any invalid record inside that boundary is damage.
Only unpublished bytes beyond an admitted boundary may be removed during recovery.
Live hints do not fabricate data or suppress newer committed images or WAL frames.

Collection syncs a deleted record and publishes its committed inventory boundary before removing an empty chunk's image or WAL.
The tombstone retains its producing version, time, and schema without allocating a user revision.
A later committed write resurrects it; GET CHUNK otherwise synthesizes an empty current-schema form with that retained identity.
Recovery validates the complete inventory, logs, and pending intents before changing owned artifacts.

Compaction first durably checkpoints current artifact and cached states so their WALs no longer depend on an older empty baseline.
It syncs every replacement shard and its parent before publishing a pending inventory containing exact lengths and SHA-256 digests.
That decision raises the cumulative deletion floor before any retained deletion disappears.
Recovery accepts an installed shard only if its complete structure, length, and digest match; otherwise it installs the checked staged replacement.
The final synced inventory clears the decision before staging cleanup.
Pending compaction refuses read-only opening, and uncertain decided failures fence serving until recovery.

## Conditional and transaction intents

Intents live under `.chunkdb.intents/`; their publication and removal are file/directory synced in every durability mode.
A conditional intent is named from its WAL's relative path with `/` replaced by `__`, plus `.rollback`, for example `L_0_0__C_0_0.wal.rollback`.
Its exact 16-byte record is `CKRB` (rollback) or `CKRC` (committed), pre-command WAL byte length `u64`, then CRC32 over the first 12 bytes.
A transaction intent is `txn-<canonical nonzero decimal revision>.rollback` and has `40 + (8 * dims + 8) * count` bytes:

1. `CKTI`, format `u16` equal to 2, state `u8` (0 rollback, 1 committed), and table dimension count `u8`.
2. Table identity (16 bytes), transaction revision `u64`, and nonzero written-chunk count `u32`, admitted against the exact dimensional record extent during recovery.
3. Per chunk in strictly increasing coordinate order: each active coordinate `i64`, then pre-commit WAL byte length `u64`; zero means no preceding WAL.
4. CRC32 over all preceding bytes.

Before mutation, writers establish a durable baseline and publish the rollback intent.
After syncing the new WAL frames, atomic synced replacement with a committed intent commits the operation.
Every changed transaction chunk uses the same revision and commit time.
Recovery validates all transaction intents and each rollback member's complete retained prefix before changing any WAL.
Recovery counts are independent of current statement limits, and streamed shape, table context, coordinate order and CRC admission precedes complete-record allocation.
Overlapping rollback decisions for one coordinate must agree on the committed cut; conflicting cuts refuse recovery before mutation.
It resolves conditional intents first, then transaction intents.

Rollback truncates/removes each WAL to its recorded boundary; a missing or shorter nonzero prefix is damage.
Committed intents preserve WAL effects, including WALs subsequently checkpointed away; removal and directory sync finish recovery idempotently.
Failure repairing an uncommitted operation fences the affected store until recovery; post-commit cleanup does not turn a committed operation into a rejected one.

## Feed slots and archives

`chunkdb.slots` is at most 1 MiB, atomically replaced and file/directory synced in every durability mode:

1. `CKSL`, format tag `3:u16`, dimensions `u8`, flags `u8`, table epoch (16 bytes).
2. Durable watermark `u64`, slot count `u32` (at most 1024).
3. Per slot: name length `u8`, flags `u8`, projection count `u16`, written revision `u64`, name bytes, optional AREA coordinates, then projected stable IDs (`u32` each).
4. CRC32 over all preceding bytes.

Names are unique, 1–63 bytes matching `[a-z_][a-z0-9_]*`; every written revision is at or below the watermark, and epoch matches the table StoreId.
The header's only defined flag bit records filtered-format capability.
Per-slot bits 0–5 mean lost, AREA present, projection present, NO BEFORE, CHUNKS, and schema-filter lost.
AREA stores the first active signed coordinates followed by the last active coordinates; axis count must match the owning table and endpoints must be ordered.
Lost records remain visible with their loss flag; advancing/watching a lost slot refuses it, and explicit DROP removes it.
The first slot activation quiesces writers/maintenance/eviction and checkpoints preceding WALs before selecting a durable frontier.
Checkpoint archives use `.chunkdb.feed/C_<cx>_<cy>.<first>.chk` for the hard-linked old base image, when one existed, and `C_<cx>_<cy>.<first>-<last>.wal` for a closed WAL range.
Three-dimensional archive stems include the z coordinate.
Each WAL range retains a checked `C_<coordinates>.<first>.base` descriptor with magic `CKFB`, format tag `2:u16`, dimensions `u8`, base kind `u8`, table epoch, active chunk coordinates, first revision, base revision, base time, base schema, image length, image SHA-256, and final CRC32.
Its base kind is 0 never written, 1 image, or 2 retained empty; the base revision is strictly below the first segment revision.
Image bases require matching complete image identity, byte length, and digest; nonimage bases have zero image fields.
The descriptor and any pinned image are durable before live prerequisites are removed.

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
After a decided error, the catalog refuses commands, RECOVER TABLE included, until restart.
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
A fenced catalog, a fenced table or a leftover pending decision requires recovery and refuses backup.
RECOVER TABLE waits for backup metadata capture and pinning, so a backup copies a recovered table or refuses.
For table enumeration, tables dropped before pinning are omitted from the record, and tables created after enumeration are outside the copy.

Resident pinning flushes staged frames under chunk locks; cold chunks are hard-linked without loading payloads, and file validation/copying occurs after the pin phase.
Accepted frames above a table's cut are excluded; a cold WAL with no such frames preserves its captured whole file, including a recoverable interrupted tail.
`.chunkdb.backup.incomplete` contains `CKBI`; `.chunkdb.restore.incomplete` contains `CKRI`; either guard takes precedence over a completed record.
Writer opening refuses these guards and the completed backup marker; read-only opening refuses the guards and opens a completed backup only after its inventory checks out.
Verification checks inventory, checksums, identities, framing, and revision cuts.

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
