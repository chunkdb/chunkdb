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
Begin, just before the statement enters its decision (EnterStatementDecision, the point after which CANCEL no longer stops it, which every durable change of a statement comes after): when no statement is marked, the record is written in progress. If that write fails, the statement fails before its change. A statement refused before its decision writes nothing and takes no revision.
End, after the statement finished or failed: the revision moves up by one and is written, still in progress while another statement runs, complete otherwise. Only then are its changes published under it.
End runs under the feed's refresh mutex, so revisions are published in the order they are taken.
A whole-catalog read takes its revision after the read: the current one when no statement runs, since a statement ends only under the same mutex, and a new one, written in progress, when one does. Two reads at one revision cannot see different states.
Every statement that reached its decision takes a revision, also one that then changed nothing or failed; watches then receive `[progress, epoch, revision]`, so their position reaches the current revision.

Every revision is recorded before it is handed out. A failed write leaves the record unknown: the next Begin writes the mark again, and the next read or End writes its revision first.
While a read could not read some tables, the record stays marked in progress, so a start does not let a consumer resume at a revision that misses them; a whole-catalog read with the record marked takes a new revision, and a refresh that reads every table again clears the mark at its own revision.
`chunkdb_admin reset-password` changes a user without the server: it marks the record in progress first.

## Start

A complete record: the clock continues at its revision. Nothing changed after it: every change is preceded by a mark.
An in-progress record: the clock continues at its revision plus one, which no state was published under.
Users can change at a start without a statement: a server started with authentication after one without it, or the reverse, the first administrator created, or an offline change. The record keeps a digest of the user records at its revision, and a start whose users differ counts as in progress. A mark keeps the last digest, since a start moves past it anyway; complete writes read the users before taking the clock's lock, because a statement that changes users holds the registry lock while it marks.
The start writes nothing; the first mark or whole-catalog read records the revision before it is handed out.
No record, or one that cannot be read: a new random epoch at revision 0. Consumers of the old epoch read the whole catalog again.

## Consumers

`WATCH CATALOG AFTER epoch revision` continues from that revision when the epoch matches and the revision is the current one or one the history still holds.
Otherwise the watch sends `resync` and the whole catalog, as without AFTER after a history overflow.
After a restart the history is empty: only a consumer at the current revision continues.

## Modes

Without `catalog_revision` (an existing directory before ENABLE FEATURE) or on a read-only server, the clock is in memory with a random epoch per process, as in #286.
ENABLE FEATURE 'catalog_revision' takes effect at the next start, which creates the record with a new epoch.
With several processes on the directory the record stays in progress and the clock does not write it; WATCH CATALOG is refused there, and the next single-process start moves to a new revision.

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

`catalog_feed`: the clock across stops with and without a running statement, shared mode, damaged and missing records, AFTER against history and epochs, resume after a restart.
`catalog_revision_crash`: crashes after the mark, after the table is created and after the revision is written; a consumer rebuilds or continues as the record says.
`feed_watch`: a consumer resumes over the server after a restart.
