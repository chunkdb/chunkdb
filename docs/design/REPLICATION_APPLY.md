# Replication apply

`APPLY`, `APPLY GROUP` and `APPLY CATALOG` write what `WATCH t REPLICATION` and `WATCH CATALOG` send into follower tables, so a follower holds the primary's epoch, revisions, chunk versions, commit times, users, values, tombstones and schema (DESIGN-2.0 F3, #146 box 4).
The push format is defined by the replication feed note (`REPLICATION_FEED.md`, box 3); this note lists what APPLY needs from it.
The follower program, which watches a primary and applies its pushes, comes after 2.0; a core test plays its part (box 5).

## Follower tables

A follower's store id is the primary's epoch, so DESCRIBE, GET CHUNK, SCAN CHUNKS and WATCH on the follower answer with the primary's epoch, revisions and versions.
One data directory holds at most one table of an epoch: a follower and its primary are on different servers, and a create or restore of an epoch already in use is refused with INVALID_ARGUMENT `reason=epoch_in_use`.
A follower is made in one of three ways:
- `CREATE TABLE f (...) ... WITH follower = 'epoch'`: an empty follower at revision 0 of that epoch, for a primary table with no commit and no schema change yet.
- `RESTORE TABLE t FROM 'backup' AS f FOLLOWER`: the backed-up table with its epoch, lineage, chunks, versions, tombstones and schema history, at the backup cut; this seeds a follower of a primary that has data. Slots of the backup are left out.
- `APPLY CATALOG` of a table `create` (below).

The table manifest records the follower with option TLV 14, `follower`, a u64 base revision: 0 after CREATE, the cut after RESTORE (options 12 and 13 belong to `max_disk_bytes` and `expire_column`).
A build that does not know the option refuses to open the table, so it never writes to a follower.
DESCRIBE adds `follower: true` to `options`; the option cannot be set or changed by ALTER TABLE SET.

A follower refuses with INVALID_ARGUMENT `reason=follower`: SET BLOCK, DELETE BLOCK, SET CHUNK, writes inside transactions, ALTER TABLE column and check changes, COMPACT TOMBSTONES, MIGRATE and CREATE SLOT.
It serves reads, read-only transactions, WATCH, SCAN CHUNKS, DESCRIBE and SHOW TABLE STATS, and takes DROP TABLE, RENAME TO, SWAP TABLES and ALTER TABLE SET of its local options (durability, checkpoints, feed and slot limits).
Its checkpoints do not collect empty chunks and expiry passes (E8) skip it: collections and expiry deletions arrive from the primary.
Promotion is not part of 2.0: BACKUP of the follower and RESTORE TABLE give a writable table with a new epoch whose lineage is the primary's epoch at the cut.

## APPLY

`APPLY $1` takes one push of `WATCH t REPLICATION` exactly as the server sent it (its RESP3 bytes, at most `--feed-buffer-bytes`).
It needs REPLICATION; it is refused inside a transaction and on a `--read-only` server.
The target is the follower table whose epoch is the push's epoch; without one the reply is NO_TABLE.
The reply is `applied`, or `skipped` when the follower already holds the push.

- `replication`: skipped when its revision is not above the follower's revision. Its `schema_version` must be the follower's (otherwise SCHEMA_MISMATCH), and every chunk's version must be the revision. A `form` must decode like SET CHUNK and carry the push's revision, commit time and schema version; it replaces the chunk, and null leaves the chunk written and empty. `rows` writes each block's full row, or deletes the block for null, and `chunk_row` replaces the chunk columns. All chunks commit at the push's revision and commit time, with its user and tag in their WAL frames, whole or not at all.
- `progress`: raises the follower's completed revision to its revision; skipped when not higher.
- `schema`: skipped when its version and column maps equal the follower's; otherwise its version must be the next one, its `step` runs through the ALTER TABLE code, and the resulting version and maps must equal the push, or the statement fails with SCHEMA_MISMATCH and changes nothing.
- `collect`: each listed chunk must be written and empty at the listed version; it becomes the tombstone the primary recorded. A listed tombstone already present is kept.
- `compact`: COMPACT TOMBSTONES BEFORE `before` on the follower.
- `resync` and other kinds: INVALID_ARGUMENT; after a resync the follower must be built again.

A `form` is written through the chunk path and a one-block `rows` entry through the block path, as on the primary, so the follower's feed captures the same before rows.
Every write keeps the primary's commit, so the follower's own WATCH sends the same changes, revisions, commit times, users and schema versions as the primary's.
A push that does not fit the follower (unknown epoch, version not equal to the revision, a collect of a populated chunk) changes nothing; when the stream itself was right, the follower is no longer a copy and must be built again.
A malformed push is INVALID_ARGUMENT; the decoder accepts only the push shapes above and bounds every length before it allocates.

## What APPLY needs from the push

- Exact values: `rows` entries and `chunk_row` carry each value as a bulk string in its [parameter encoding](../PROTOCOL.md#parameters), null for NULL, in schema order. A RESP3 double loses a NaN's sign and payload, and a follower must hold the primary's bytes; forms are exact already.
- The epoch in schema pushes: a REPLICATION watch's `schema` map carries `epoch`, so APPLY finds the table without a name.
- Every commit that took a revision, also one whose write changed no value, since its chunks took the revision as their version.
- `AFTER epoch 0` continues on a table that has no commit and no schema change, whatever its revision: a new table's clock starts at the directory's version floor, so its revision can be above 0 before its first commit.
- A seed from a backup resumes `AFTER epoch cut`, so a program starts the replication watch before BACKUP, which keeps the cut in the live history.
- For APPLY GROUP: `txn` names the cross-table transaction and the number of tables it wrote, so a program knows when it holds every member.

## Durability and position

APPLY answers after its changes are durable, in every durability mode: a commit of one chunk appends one WAL frame and syncs it; a commit of several chunks uses a transaction intent, so it applies whole or not at all across a crash.
The APPLYs of one table run one at a time, under a per-table apply lock, so the follower's durable state is always the primary's state at some revision.
The follower never takes versions from its own clock. APPLY at revision R registers its producer bound, then raises the clock to R + 1, so no reader sees R completed before its changes are published.
The version clock record keeps its rule: raised on disk before a revision is used, never lowered.
At a start the follower's revision is the highest of its base revision and every revision its coordinate index recovered (chunk identities, tombstones and the deletion floor): its last applied commit, collection or compaction. The in-memory clock starts just above it; the directory's version floor does not apply, since a follower's versions are the primary's.
A position reached only by `progress` is not stored; after a restart the program resumes from DESCRIBE's revision and progress arrives again.
For the same reason slots on followers are refused in 2.0: a slot's durable frontier could pass a progress position that a restart loses.
A failure whose outcome is unknown answers OUTCOME_UNKNOWN, as for transactions; after RECOVER TABLE or a restart the program sends the push again, and APPLY skips it when it was applied.

## APPLY GROUP

`APPLY GROUP $1 [$2 ...]` applies the pushes of one cross-table transaction, one per follower table, all with the same `txn`, as one commit group on the follower in the strongest mode of its tables, each table at its push's revision.
It waits for commit groups (#136). A program holds each table's stream at a group push until every member has arrived; groups take versions under their locks in one order, so two groups never wait for each other.

## APPLY CATALOG

`APPLY CATALOG $1` applies one WATCH CATALOG push; it needs REPLICATION and waits for the durable catalog revision (#304).
Table entries only:
- `create`: a follower of the entry's epoch with its columns, chunk columns, geometry, checks and options; skipped when the name already holds a follower of that epoch, so a table seeded by RESTORE ... FOLLOWER stays.
- `drop`: drops the follower of that name (its `data` is null); skipped when the name holds no follower.
- A `drop` of a name and a `create` in the same event whose epoch is that follower's (RENAME TO) rename the follower; `alter` of two names that exchange epochs (SWAP TABLES) swaps them.
- Other `alter` entries change nothing: schema changes arrive in the table's own `schema` pushes, and options stay local.

Users, grants and slots are not applied: each server keeps its own.
A follower made from a whole-catalog `create` is empty; when the primary table has data, its replication watch from revision 0 answers resync and the program seeds the table from a backup.
The applied catalog position, epoch and revision, is stored in `data_dir/chunkdb.follow` (atomic replace with file and directory sync) after the entries are applied; entries are idempotent, so a crash in between applies them again.
APPLY CATALOG answers `{epoch, revision}`, skips a push at or below the stored revision of the same epoch, and SHOW STATUS reports the position as `applied_catalog`.
To be agreed with WATCH CATALOG: the whole-catalog event carries a marker, so APPLY CATALOG can drop followers whose epoch it no longer lists.

## Rights

REPLICATION, granted ON *, is needed for APPLY, APPLY GROUP and APPLY CATALOG, which write into every follower table without table rights.
CREATE TABLE ... WITH follower and RESTORE ... FOLLOWER need REPLICATION as well as the rights CREATE TABLE and RESTORE TABLE need.
HELLO adds feature `replication_apply`.

## Costs

Ordinary tables: statement admission checks one flag of the table it already holds; a measurement window covers this.
A single-chunk APPLY costs one WAL sync, a multi-chunk one the transaction intent's syncs.

## Tests

Engine: CREATE and RESTORE of followers with rights and `epoch_in_use`, every refusal, each push kind applied and skipped, the error cases, the position across a restart and after a crash between the frames of a multi-chunk APPLY.
Mirror (box 5): a primary engine and a follower engine; the primary's REPLICATION pushes go through APPLY, then GET CHUNK forms, SCAN SINCE 0 identities, DESCRIBE and both WATCH streams are compared, first for one table with DDL, then for two tables with cross-table transactions after #136.
