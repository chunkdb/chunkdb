# Chunk model

This design addresses [#133](https://github.com/chunkdb/chunkdb/issues/133).
It specifies geometry, chunk columns, sectioned forms, retained empty chunks and a populated-coordinate index.
The implementation must switch every persistent and transport surface together before advertising the `chunk_model` capability.
Development directories with superseded formats are refused before any data-file mutation.

## Geometry and schema

A table fixes `dims` to 2 or 3, with inactive z side lengths equal to 1 and inactive z coordinates equal to 0.
Local block order is x fastest, then y, then z: `x + width * (y + height * z)`.
Division and modulo use floor rounding for negative coordinates.
Each active chunk side is at most 4096, the product is at most 1048576 blocks, and the packed payload is at most 64 MiB.
Every selector checks count and byte multiplication before reading or allocating.
`AROUND` uses the sum of squared active-axis distances, with arithmetic that cannot overflow signed coordinates.
Coordinate lists, files, WAL headers, intents, feed entries, backups and verification use exactly the owning table's dimension count.
Canonical names retain `L_x_y/C_x_y` in two dimensions and add the third coordinate in three dimensions.
A shared parser rejects noncanonical spelling, wrong dimensions and a chunk placed in the wrong large directory.

Chunk columns use a separate namespace with the existing scalar types, stable IDs, defaults, nullability and schema history.
Both column domains share one table schema version and one chunk producing version.
The default row is DEFAULT, otherwise NULL for nullable columns, otherwise zero or empty.
A chunk is populated when it has a present block or a chunk-column value differing from that baseline.
Metadata-only chunks appear in area reads and scans and cannot be collected.
`SET CHUNK ... META ...` validates the complete next row before publication and writes one WAL frame.
Full forms replace both domains; block-column projections preserve chunk metadata.
Partial block-column writes take presence from the incoming form, retain unnamed values on continuing blocks, initialize unnamed values on newly present blocks from defaults, and remove every block value at absent positions.
Adding a chunk column translates old chunks from their baseline without allocating a producing revision.

## Format boundaries

The directory and table envelopes own incompatibility bit 32, named `chunk_model`.
New tables publish complete geometry, both schema histories and an empty checked index inventory before catalog visibility.
Images and WAL headers include dims, active coordinates, table identity, feature sets and a header checksum.
Image directories retain bounded section counts, checked stored and raw lengths, compression flags and raw checksums.
Header feature sets must fit the already admitted table envelope.
Unknown persistent sections require explicit feature ownership; an unknown transport section grants no persistent ownership.
A metadata-replace WAL record occurs at most once per frame and is validated with all other records before any state changes.
Tables without chunk columns allocate no metadata row and append no metadata record.
Transaction intents include dims, table identity, producing revision, a bounded complete member list and a final checksum.
Recovery validates the entire intent and every member before changing any WAL.
Rollback repair retains the intent and fences serving until every member and directory sync completes.

The wire form starts with `CKCF`, `form_version:u16`, `flags:u16`, `chunk_version:u64`, `schema_version:u64`, `commit_time_ms:u64`, `section_count:u16` and zero reserved `u16`.
Its at-most-64 sorted unique directory entries are `kind:u16`, zero flags `u16`, `offset:u32` and `length:u32`.
Bodies are contiguous, nonoverlapping and fill the bulk value exactly.
Sections carry presence, fixed-column descriptors and values, nullable validity, variable values, chunk columns and optional projected block spans.
Descriptors identify fixed columns by stable ID and width, so unknown types never hide a known neighbour.
Read decoders validate ranges and skip unknown sections; mutation decoders refuse them.
Projected forms carry a flag, and transaction-private forms carry zero producing version and time.
Incoming version and time never choose the server's producing identity.
A never-written chunk remains NULL instead of receiving a version on load.

## Index, tombstones and recovery

One ordered coordinate index per table serves bounded scan pages without walking directories on each page.
Ordinary writes update membership only on population transitions, with no index I/O for updates to an already populated chunk.
A scan captures a coordinate page before acquiring any chunk locks.
Publication takes the chunk lock before its short membership lock and releases membership before I/O.
Checkpoint, backup and compaction retain the existing maintenance admission order.

Each large directory has a checked streaming `CKIX` log of live hints, deleted tombstones and explicit forget records.
Records carry coordinates, producing revision, time and schema, with exact lengths, checksums and placement validation.
The checked `CKII` inventory lists sorted unique large coordinates and compaction decision fields.
Recovery validates complete inventory and logs before mutating artifacts, resolves rollback intents, then reconciles committed image and WAL state.
Live hints do not fabricate data or suppress newer committed WAL state.
Listed missing or corrupt logs are damage; missing inventory is damage.
An incomplete final append may be discarded only when no later valid framing exists; a checksum-valid malformed record or interior corruption is damage.
Collection cannot remove artifacts until its complete tombstone record has been synced.
Before listing a new shard, publish and sync its complete log and parent directory; only then publish and sync its inventory entry.
An unlisted log cannot authorize deletion and recovery validates and adopts it before collection; cleanup cannot remove a log still referenced by an inventory or recovery decision.
Initial table staging contains a synced empty inventory before its catalog entry becomes visible, so interrupted unpublished staging follows the existing table-publication recovery path.
Read-only readers rebuild on a changed equal-even snapshot generation and publish no partial index.

Collection durably publishes the emptying commit's tombstone and any new inventory entry before removing image or WAL files.
The tombstone preserves the producing revision, time and schema; collection allocates no user revision.
A failure before tombstone publication leaves the original artifacts intact.
Uncertain publication or removal fences serving until recovery completes the decision idempotently.
A newer committed artifact resurrects a chunk; equal or older artifacts are suppressed only by a validated committed-empty boundary.
`GET CHUNK` synthesizes an empty current-schema form with the retained producing version and time.
Ordinary area reads and scans omit tombstones; `SCAN SINCE` includes them and advertises a retained-deletion floor.
Requests below that floor fail explicitly instead of silently omitting deletions.

Compaction requires exclusive mutation admission and persists its exact inclusive operation floor separately from the cumulative deletion floor.
For `BEFORE n`, the operation floor is `min(n - 1, completed_frontier)`; zero is a no-op.
A durable pending inventory decision precedes every shard replacement.
That decision publishes the cumulative refusal floor before any deletion disappears, and pending compaction fences read-only opening until recovery completes it.
Recovery repeats the persisted inclusive filter and clears the decision only after every replacement and directory is durable.
A decided failure fences serving with an unknown outcome.

## Feed, backup and verification

An archived WAL retains its original pre-segment base, including schema and metadata, with base revision strictly below the first frame.
A checked `CKFB` descriptor binds epoch, dims, coordinates, first revision and the exact image or retained-empty base.
Image length and checksum bind a pinned original image; a missing base is damage.
The original base and descriptor become durable before collection or WAL rename can remove their live prerequisites.
A resurrection segment uses its own retained-empty base and cannot replace a previous segment's nonempty base.
Rejected or undecided transaction frames never enter sealed archives.

Backup pins the complete inventory, index logs and committed image/WAL prefixes at one table cut under maintenance admission.
Restore validates the complete backup before publication and rewrites identities and checksums while preserving producing versions and times.
The restored clock exceeds every recovered image, WAL and tombstone revision.
Verification independently checks dimensions, placement, identities, schemas, checksums, index transitions, tombstone overlaps and backup cuts without repairing data.

## Validation

Tests cover negative boundaries, coordinate extremes, dimension mismatches, x-fastest indexing, checked products and canonical path round trips.
Format tests cover every truncated boundary, corrupt identity or checksum, reserved field, directory overlap, unknown section and unknown fixed type between known neighbours.
Crash tests interrupt index publication, tombstone sync, image removal, archive-base publication, WAL rename, intent repair, compaction, backup and restore.
Concurrency tests pin collection against writes, scans, schema changes and backup with deterministic hooks.
Clients and CLI gain matching dimension-aware forms, metadata, version and unknown-section fixtures before the server switches the public format.
Separate server-side budget windows measure timestamp bookkeeping, coordinate width, membership transitions, sectioned encoding and metadata against ordinary requests.
