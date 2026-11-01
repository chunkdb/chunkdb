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
Both schema histories advance together, using an explicit other-domain marker in the unchanged domain rather than an unexplained empty history step.
At each schema revision after creation exactly one domain records its column changes and the other records that marker.
Chunk-schema validation permits an empty column domain, including the versions preceding its first column and following its last drop; block-schema validation retains its nonempty fixed-column requirement.
Every chunk column marked REQUIRED without DEFAULT must be supplied when a chunk first becomes written, including a metadata-only initialization whose supplied values equal the baseline.
A block write cannot first initialize such a chunk; a complete chunk form or META statement supplies its required metadata first.
Adding a REQUIRED chunk column without DEFAULT is refused while any written chunk identity, including a tombstone, exists.
Collection preserves that initialized identity until explicit tombstone compaction forgets it.

## Format boundaries

The directory and table envelopes own incompatibility bit 32, named `chunk_model`.
New tables publish complete geometry, both schema histories and an empty checked index inventory before catalog visibility.
Images and WAL headers include dims, active coordinates, table identity, feature sets and a header checksum.
The table manifest places `dims:u8` and three zero reserved bytes before its six `u32` geometry sides, then the table identity, bounded options, and independently length-delimited block and chunk schema histories.
Its geometry sides are large x/y/z followed by chunk x/y/z; the schema histories must carry the same current revision and matching other-domain markers before the table is admitted.
Image directories retain bounded section counts, checked stored and raw lengths, compression flags and raw checksums.
An image has at most 256 MiB of stored bytes and total raw bytes, with at most 64 MiB in an ordinary section.
The chunk-column section additionally allows its bounded scalar values and directory above the shared 64 MiB variable-byte bound.
Header feature sets must fit the already admitted table envelope.
Unknown persistent sections require explicit feature ownership; an unknown transport section grants no persistent ownership.
A metadata-replace WAL record occurs at most once per frame and is validated with all other records before any state changes.
A WAL frame is at most 256 MiB including its header and trailer; checked framing and length admission precede frame allocation.
Tables without chunk columns allocate no metadata row and append no metadata record.
The variable-byte budget is the checked sum of encoded block and one-row metadata VARS bytes, including each stored entry's 12-byte header; fixed metadata bytes and wire directories are outside that budget.
Transaction intents include dims, table identity, producing revision, a bounded complete member list and a final checksum.
Recovery validates the entire intent and every member before changing any WAL.
Rollback-member validation streams complete frames through the recorded cut, retaining one bounded frame and one chunk state rather than the complete WAL prefix.
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
A new coordinate reserves its in-memory map node before the write can commit, and ordinary writes publish no persistent live hint before their commit decision.
Each stable coordinate node carries atomic producing identity and population state; writes to an existing node publish those fields before completing their producer bound without taking the ordered-map mutex.
These atomics are candidate hints, not a coherent returned chunk image; forms, producing revision and time are revalidated together under the chunk lock or obtained from a checked retained tombstone.
`SCAN SINCE` uses only the node's acquire-loaded producing revision for its lower-bound prefilter and never excludes an entry because of its population flag or a revision above the captured completed frontier.
The completed-frontier capture acquires the publication ordering of every producer it covers, so a node revision at or below the promised position cannot remain unpublished to that scan.
Ordinary scans may use a false population hint to omit a coordinate at that concurrent committed transition; every included coordinate is revalidated before output.
The chunk cache retains only a borrowed node pointer owned by its table index, and failed first-write reservations remove their unpublished node while still holding that chunk's lock and clear that cached pointer.
A reservation cannot outlive its pinned chunk, and compaction reopens the table after exclusive admission before removing nodes whose cached pointers could otherwise remain.
A committed empty identity remains an in-memory scan deletion while its image or WAL still holds it; collection makes that identity a durable index tombstone.
A scan captures a coordinate page before acquiring any chunk locks.
Publication takes the chunk lock before its short membership lock and releases membership before I/O.
Checkpoint, backup and compaction retain the existing maintenance admission order.

Each large directory with retained index decisions has a checked streaming `CKIX` log of live hints, deleted tombstones and explicit forget records.
The inventory lists these logs rather than every directory containing a live image or WAL; recovery discovers live artifacts once and builds the ordered in-memory candidate index.
Records carry coordinates, producing revision, time and schema, with exact lengths, checksums and placement validation.
The checked `CKII` inventory lists sorted unique large coordinates, each log's committed byte boundary, and compaction decision fields.
Its independently checksummed fixed header includes a nonzero monotonic inventory generation, advanced by each inventory publication and never reused within a table identity.
Read-only point reads may reuse a completely admitted retained-boundary index only when that header's identity and generation still match inside the same stable snapshot bracket as their image and WAL.
An ordinary block write changes the table snapshot generation without changing the inventory generation, so refreshing a point does not rebuild every retained shard after unrelated writes.
Read-only scans rebuild their complete live-coordinate candidate index when the table snapshot generation changes and publish it only after one unchanged equal-even whole-index observation.
Every listed boundary covers a complete header and whole records; a listed log shorter than that boundary is damage.
Recovery validates complete inventory and logs before mutating artifacts, resolves rollback intents, then reconciles committed image and WAL state.
Conditional intent names encode exactly the table's dimensions and canonical shard placement.
Every conditional decision and every rollback member is admitted before any intent cleanup or WAL truncation, including replay validation of its complete committed prefix over the checked image or retained-empty baseline.
The same prefix admission serves transaction and conditional rollback recovery, and neither path may discard another member's bytes before all decisions pass validation.
Pending rollback decisions for one coordinate must agree on their committed byte cut; conflicting cuts are damage and refuse the entire recovery before mutation.
Committed cleanup decisions may overlap later writes or rollback decisions because they never authorize truncation.
The durable startup snapshot fence owns one live publication count until this reconciliation and index repair finish, so nested normal WAL-tail repair cannot close or reject the enclosing recovery bracket.
Live hints do not fabricate data or suppress newer committed WAL state.
Listed missing or corrupt logs are damage; missing inventory is damage.
Without an independently durable boundary, only a shorter final record prefix may be treated as an interrupted append; a complete invalid record is damage.
With an inventory boundary, every record within that boundary must validate, including the final tombstone; any suffix beyond it is unpublished and may be removed only after complete recovery admission.
Collection syncs a complete appended record and then durably advances the inventory boundary before deleting artifacts, so a complete torn append can be distinguished from damage to an acknowledged tombstone.
Repair of a partial suffix never authorizes removing artifacts and must preserve every independently established durable boundary.
Collection cannot remove artifacts until its complete tombstone record has been synced.
Before listing a new shard, publish and sync its complete log and parent directory; only then publish and sync its inventory entry with the committed boundary.
An unlisted log cannot authorize deletion and recovery validates and adopts it before collection; cleanup cannot remove a log still referenced by an inventory or recovery decision.
Initial table staging contains a synced empty inventory before its catalog entry becomes visible, so interrupted unpublished staging follows the existing table-publication recovery path.
Direct store initialization also publishes the empty inventory before its manifest.
Without a manifest, only a checked empty inventory with zero deletion floors and no other owned storage state may be adopted under exclusive admission, preserving its table identity and requiring matching dimensions.
Owned temporary files from that unpublished initialization may then be removed; an inventory retaining any coordinate or compaction decision is never treated as a new store.
Read-only readers rebuild on a changed equal-even snapshot generation and publish no partial index.

Collection durably publishes the emptying commit's tombstone and any new inventory entry before removing image or WAL files.
The tombstone preserves the producing revision and time and records the schema of the retained empty baseline; collection allocates no user revision.
Later schema translation or repeated collection of that same producing identity reuses its existing checked tombstone rather than appending a conflicting schema at the same revision.
A failure before tombstone publication leaves the original artifacts intact.
Uncertain publication or removal fences serving until recovery completes the decision idempotently.
A newer committed artifact resurrects a chunk; equal or older artifacts are suppressed only by a validated committed-empty boundary.
`GET CHUNK` synthesizes an empty current-schema form with the retained producing version and time.
Ordinary area reads and scans omit tombstones; `SCAN SINCE` includes them and advertises a retained-deletion floor.
Requests below that floor fail explicitly instead of silently omitting deletions.

Compaction requires exclusive mutation admission and persists its exact inclusive operation floor separately from the cumulative deletion floor.
For `BEFORE n`, the operation floor is `min(n - 1, completed_frontier)`; zero is a no-op.
A durable pending inventory decision precedes every shard replacement.
Before that decision, each shard's filtered replacement is written and synced under its owned staging name.
Every staged image's parent directory is synced before the pending inventory decision can name it.
Each pending inventory entry binds the old committed cut, exact replacement length and SHA-256 digest of every replacement file byte.
The existing streaming SHA-256 implementation supplies this binding without adding a dependency.
Whole-file CRCs cannot identify replacement content when complete headers and records already contain trailing CRCs, because valid equal-length files may share a fixed residue.
That decision publishes the cumulative refusal floor before any deletion disappears, and pending compaction fences read-only opening until recovery completes it.
Recovery accepts an installed replacement only when its exact length, SHA-256 digest and complete record validation match the pending entry; otherwise it redoes that shard from its checked staged image.
A decided shorter replacement is never judged against the old committed cut alone, and a missing required staged image is damage unless the installed replacement already matches.
After every replacement and directory is durable, the final inventory publishes all replacement cuts, clears its decision fields, and only then permits staged-image cleanup.
Before a decision exists, unreferenced compaction staging is unpublished and cannot authorize discarding any live log or tombstone.
A decided failure fences serving with an unknown outcome.

## Feed, backup and verification

An archived WAL retains its original pre-segment base, including schema and metadata, with base revision strictly below the first frame.
A checked `CKFB` descriptor binds epoch, dims, coordinates, first revision and the exact image or retained-empty base.
Image length and SHA-256 digest bind every byte of a pinned original image, including its checked header and opaque sections; a missing base is damage.
The original base and descriptor become durable before collection or WAL rename can remove their live prerequisites.
A resurrection segment uses its own retained-empty base and cannot replace a previous segment's nonempty base.
Rejected or undecided transaction frames never enter sealed archives.

Backup pins the complete inventory, index logs and committed image/WAL prefixes at one table cut under maintenance admission.
The maintenance pin excludes collection until immutable inventory bytes and every listed committed log prefix have been linked into private staging.
Unpublished log suffixes are excluded, pending compaction is refused, and every retained producing revision must be at or below the table cut.
Private validation replays WALs over their checked image or retained-empty baseline, including chunk-column schema history.
Restore validates the complete backup before publication and rewrites identities and checksums while preserving producing versions and times.
The restored inventory and every listed log receive the new table identity before the table manifest is published, preserving deletion floors and committed boundaries.
The restored clock exceeds every recovered image, WAL and tombstone revision.
Verification independently checks dimensions, placement, identities, schemas, checksums, index transitions, tombstone overlaps and backup cuts without repairing data.

## Validation

Tests cover negative boundaries, coordinate extremes, dimension mismatches, x-fastest indexing, checked products and canonical path round trips.
Format tests cover every truncated boundary, corrupt identity or checksum, reserved field, directory overlap, unknown section and unknown fixed type between known neighbours.
Crash tests interrupt index publication, tombstone sync, image removal, archive-base publication, WAL rename, intent repair, compaction, backup and restore.
Concurrency tests pin collection against writes, scans, schema changes and backup with deterministic hooks.
Clients and CLI gain matching dimension-aware forms, metadata, version and unknown-section fixtures before the server switches the public format.
Separate server-side budget windows measure timestamp bookkeeping, coordinate width, membership transitions, sectioned encoding and metadata against ordinary requests.
