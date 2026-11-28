# Replication apply

`APPLY $1` accepts single-table commit and progress pushes for follower tables (DESIGN-2.0 F3, #146 box 4).
The [feed reference](../reference/FEED.md#apply) describes the implemented push format.
`REPLICATION_FEED.md` describes the planned `WATCH t REPLICATION` sender (box 3).
The mirror test is planned for box 5 and the follower program comes after 2.0.

## Follower tables

A follower's store id is the primary's epoch, so DESCRIBE, GET CHUNK, SCAN CHUNKS and WATCH on the follower answer with the primary's epoch, revisions and versions.
One data directory holds at most one table of an epoch, including tables that are fenced.
`CREATE TABLE f (...) WITH follower = 'epoch'` creates an empty follower at revision 0 for a primary with no commits or schema changes.
CREATE refuses an epoch already in use with INVALID_ARGUMENT `reason=epoch_in_use`.
A follower requires the `feed_slots` table feature because its WAL frames use the existing USER metadata field.
When the feature is disabled, CREATE refuses the follower with INVALID_ARGUMENT `reason=feature_disabled feature=feed_slots` and points to `ENABLE FEATURE 'feed_slots'`.

Table manifest option TLV 14, `follower`, holds the u64 base revision, which is 0 after CREATE.
A build that does not know the option refuses to open the table, so it never writes to a follower.
DESCRIBE adds `follower: true` to `options`; the option cannot be set or changed by ALTER TABLE SET.

A follower refuses writes, ALTER TABLE column and check changes, COMPACT TOMBSTONES and CREATE SLOT with INVALID_ARGUMENT `reason=follower`.
MIGRATE applies the same refusal to ALTER TABLE and CREATE SLOT steps on a follower.
It serves reads, read-only transactions, WATCH, SCAN CHUNKS, DESCRIBE and SHOW TABLE STATS, and takes DROP TABLE, RENAME TO, SWAP TABLES and ALTER TABLE SET of its local options (durability, checkpoints, feed and slot limits).
Follower checkpoints keep empty chunks.
BACKUP of the follower and RESTORE TABLE give a writable table with a new epoch and lineage pointing to the primary's epoch at the cut.

## APPLY

`APPLY $1` takes one RESP3 push of at most `--feed-buffer-bytes` and 1048576 values.
It needs REPLICATION; it is refused inside a transaction and on a `--read-only` server.
The target is the follower table whose epoch is the push's epoch; without one the reply is NO_TABLE.
The reply is `applied`, or `skipped` when the follower already holds the push.

- A `replication` push commits `form` and `rows` chunks together at the supplied revision, commit time and schema version.
- The optional tenth field `txn_tables` is null or a list containing only the name of the follower selected by epoch.
- A `progress` push raises the follower's completed revision.
- Other push kinds and non-null tag or transaction fields are refused with INVALID_ARGUMENT.

The WAL frames and live WATCH pushes carry the supplied user.
The decoder bounds every length before allocating and refuses malformed pushes with INVALID_ARGUMENT.

## Planned sender requirements

- Exact values: `rows` entries and `chunk_row` carry each value as a bulk string in its [parameter encoding](../PROTOCOL.md#parameters), null for NULL, in schema order. A RESP3 double loses a NaN's sign and payload, and a follower must hold the primary's bytes; forms are exact already.
- The epoch in schema pushes: a REPLICATION watch's `schema` map carries `epoch`, so APPLY finds the table without a name.
- Every commit that took a revision, also one whose write changed no value, since its chunks took the revision as their version.
- `AFTER epoch 0` continues on a table that has no commit and no schema change, whatever its revision: a new table's clock starts at the directory's version floor, so its revision can be above 0 before its first commit.
- A seed from a backup resumes `AFTER epoch cut`, so a program starts the replication watch before BACKUP, which keeps the cut in the live history.
- For APPLY GROUP, `txn` identifies the cross-table transaction and `txn_tables` lists its members.

## Durability and position

Every successful APPLY of a commit is durable in every mode and uses a transaction intent, including for one chunk.
APPLY takes a commit whatever the follower's transaction chunk limit, so a primary with a higher limit does not stop it.
The APPLYs of one table run one at a time, under a per-table apply lock, so the follower's durable state is always the primary's state at some revision.
APPLY reserves the incoming revision on disk before writing the intent and advances the live clock at the commit point.
The producer bound keeps the completed position below that revision until the changes are published.
A confirmed rollback leaves the completed position unchanged, so the same push can be retried.
On startup the follower's position is the highest revision recovered from the coordinate index or the base revision, whichever is higher.
The in-memory clock starts just above that position and uses the primary's revisions independently of the directory's version floor.
A position reached only by `progress` is not stored; after a restart the program resumes from DESCRIBE's revision and progress arrives again.
For the same reason slots on followers are refused in 2.0: a slot's durable frontier could pass a progress position that a restart loses.
A failure whose outcome is unknown answers OUTCOME_UNKNOWN, as for transactions; after RECOVER TABLE or a restart the program sends the push again, and APPLY skips it when it was applied.

## Planned follower operations

Planned single-table pushes will carry schema changes, checkpoint collection and tombstone compaction.
Planned `RESTORE TABLE t FROM 'backup' AS f FOLLOWER` would seed a follower at the backup cut with the primary's epoch, lineage, chunks, versions, tombstones and schema history, with slots left out.
That restore mode will also require the `feed_slots` table feature.

### APPLY GROUP (planned)

`APPLY GROUP $1 [$2 ...]` applies the pushes of one cross-table transaction, one per follower table, all with the same `txn`, as one commit group on the follower in the strongest mode of its tables, each table at its push's revision.
It waits for commit groups (#136). A program holds each table's stream at a group push until every member has arrived; groups take versions under their locks in one order, so two groups never wait for each other.

### APPLY CATALOG (planned)

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

REPLICATION, granted ON *, permits APPLY into every follower table without table rights.
CREATE TABLE ... WITH follower needs both REPLICATION and the regular CREATE TABLE rights.
HELLO adds feature `replication_apply`.

## Costs

Ordinary tables: statement admission checks one flag of the table it already holds; a measurement window covers this.
Each APPLY of a commit syncs the transaction intent and each written WAL, including for one chunk.

## Tests

The engine tests use synthetic commit and progress pushes and check commit identity, retry after rollback, epoch uniqueness and restart position.
The crash cases cover each durability mode with a commit larger than the follower's transaction chunk limit.
The planned mirror test (box 5) will replay real REPLICATION pushes and compare GET CHUNK forms, SCAN SINCE identities, DESCRIBE and both WATCH streams.
