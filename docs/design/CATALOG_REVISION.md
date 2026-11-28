# Catalog revision

WATCH CATALOG names each catalog state with an epoch and a revision (#146).
This note makes them survive restarts, so a consumer such as APPLY CATALOG can resume from its last revision instead of reading the whole catalog again.

## Rule

One revision of an epoch names one catalog state, as far as the server can tell.
A consumer at revision R of the current epoch has the catalog the server has at R: continuing from R loses nothing.

## Record

`data_dir/chunkdb.catalog` holds the catalog epoch, the last revision and an in-progress flag (layout in STORAGE_FORMAT.md).
It has one writer, the catalog clock of the server process, and every write is an atomic replace with file and directory sync.
`chunkdb.manifest` is not used: four paths rewrite it (DROP's version floor, migration journal images, feature activation, offline restore), and any of them could revert or lose a revision.
The directory feature `catalog_revision` (incompat bit 7) owns the file, so a build that does not keep it refuses to write the directory.

## Clock

A catalog statement is one CatalogScopeOf names: table, column, check, slot, user and grant statements, MIGRATE, RESTORE TABLE, SWAP TABLES, RECOVER TABLE and WATCH SLOT; a WATCH SLOT reaches a decision only when it records the slot's first owner or takes the slot over, so a plain attach does not touch the clock.
Begin writes the in-progress record when no durable mark covers the statement, before admitting its decision and before any catalog change.
If that write fails, the statement fails before its change.
End takes a revision for a statement that entered its decision and records it before publishing changes at that revision.
A stop accepted before the decision takes no statement revision, and on an exclusive writer End can clear its provisional mark at the current revision once other statements and unread tables no longer need it.
End runs under the feed's refresh mutex, so revisions are published in the order they are taken.
A whole-catalog read takes its revision after the read and uses the current revision when the clock is settled and no statement ran during the read.
When a statement overlaps the read, the read takes a new revision and keeps the record marked.
Watches advance across revisions with no visible changes through progress or a later visible change.

Every revision is recorded before it is handed out. A failed write leaves the record unknown: the next Begin writes the mark again, and the next read or End writes its revision first.
While a read could not read some tables, the record stays marked in progress, so a start does not let a consumer resume at a revision that misses them; a whole-catalog read with the record marked takes a new revision, and a refresh that reads every table again clears the mark at its own revision.
`chunkdb_admin reset-password` changes a user without the server: it marks the record in progress first.

## Start

A complete record with the same startup digest keeps its revision.
An in-progress record or a changed digest moves the clock past the stored revision.
The digest covers the user records and the server defaults for feed and slot byte limits, which can change across starts without a catalog statement.
A mark keeps the last recorded digest, and complete writes read the users before taking the clock lock because a user statement can hold the registry lock while it marks.
Startup leaves the record alone, so the first catalog statement must persist a mark if the record on disk is complete, even when the clock has advanced past it.
No record, or one that cannot be read: a new random epoch at revision 0. Consumers of the old epoch read the whole catalog again.

## Consumers

`WATCH CATALOG AFTER epoch revision` continues from that revision when the epoch matches and the revision is the current one or one the history still holds.
Otherwise the watch sends `resync` and the whole catalog, as without AFTER after a history overflow.
After a restart the history is empty: only a consumer at the current revision continues.

## Modes

Without `catalog_revision` the clock is in memory with a random epoch per process, as in #286.
WATCH CATALOG is refused on read-only and shared servers.
ENABLE FEATURE 'catalog_revision' takes effect at the next start, and the first mark or whole-catalog read then records the new epoch.
With `catalog_revision` enabled, a shared writer marks the record before its first catalog decision if it found the record unmarked and leaves it marked for the next exclusive writer.

A filesystem rollback or a cold copy of the directory keeps the epoch: consumers that were away must watch without AFTER.

## Backup and restore

Backups leave `chunkdb.catalog` out: a restored directory starts a new epoch, and an offline restore also gives it a new `data_dir_id`.
RESTORE TABLE and SWAP TABLES are catalog statements in the same directory and take revisions like the others.

## Failures

A failed Begin write refuses the statement before it changes anything.
A failed End write publishes nothing under the new revision and ends the catalog watches with INTERNAL; the record may hold the mark or a lower revision, and the next write, before any revision is handed out, records the current one.
A start writes nothing, and with several processes it leaves temporary files of the record alone, since another process may be replacing it.
A crash at any point leaves a complete record whose revision names the state on disk, or an in-progress record that the start moves past.

## Cost

Catalog statements write the record once or twice (two syncs); plain reads and writes do not touch the clock.
`chunkdb_verify` checks the record and reports a damaged one as a warning, since the next writer start replaces it.

## Tests

`catalog_feed` covers stops around decisions, shared mode, damaged and missing records, AFTER against history and epochs, progress, and restarts with changed defaults.
`catalog_revision_crash` covers crashes before and after the revision is written, including the first catalog statement after an authentication mode change.
`feed_watch`: a consumer resumes over the server after a restart.
