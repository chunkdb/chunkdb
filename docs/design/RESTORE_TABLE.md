# Online table restore: design

`RESTORE TABLE t FROM 'snapshot' [AS t_restored]` copies one table of a completed backup into a running writer as a new table (#158).
`SWAP TABLES t, t_restored` then exchanges the two names atomically; the restore itself never replaces a live table.
The [backup design](BACKUP_DESIGN.md) defines backups, cuts and lineage; the [table catalog](TABLES_DESIGN.md) defines table publication.

## Statement and rights

The backup path is resolved like `BACKUP TO`: relative to `--backup-dir`, with no `..`, rooted path, backslash or link below the resolved directory.
`t` names a table cut in the backup; without `AS` the restored table keeps that name.
The new name must not exist, be fenced, or be published by another statement before the restore publishes.
RESTORE TABLE needs ADMIN on `*`, the right CREATE TABLE needs, or MANAGES USERS, the right BACKUP needs.
SWAP TABLES needs ADMIN on both names; grants stay with names, so each grant applies to the table now under its name.
Both statements are refused inside a transaction and on a `--read-only` server.

## Identity

The restored table gets a new epoch; its lineage records the backed-up epoch and the backup cut as parent epoch and fork revision.
Its clock restarts at the cut's successor, and every chunk keeps its committed revision, commit time and deletion floor.
Retained slots get the new epoch at the cut, with no archived history, exactly as the offline restore does.
The table must have `table_lineage`, and its table-owned feature bits must equal the directory's; otherwise the copy is removed and nothing is published.
SWAP keeps each table's epoch: identity travels with the data, and only the name that reaches it changes.
The 2.0 plan asked for new epochs on both tables; that would rewrite every file of both, and would end the exact-fork resume of a restored table, while a name reaching another epoch already makes its consumers resume by lineage or resync.

## Building and publication

The restore copies only the table's inventory files into a private directory under `.chunkdb.staging`, the place CREATE TABLE builds tables.
Each copied file's length and CRC must match the backup inventory, and the table directory in the backup must hold exactly those files, all regular.
The copy is then checked against its cut like a backup inventory check, rebound to the new identity with the offline restore's code, verified like `chunkdb_verify`, and synced.
Until publication the table is in no catalog listing and no statement can name it.
Publication takes the catalog's table operations, checks the name again and renames the directory into `tables/` without replacing anything, then syncs both parents.
The rename is the commit point; the store is opened and served afterwards, as CREATE TABLE does.
A failure before the rename removes the private directory; a crash before it leaves the directory for the next writer start to remove.
A failure after the rename that leaves the table unopened moves it out again, or fences its name until RECOVER TABLE.
The backup is only read; a backup that changes while it is copied fails the checksum check.
The feature check runs after the copy and again at publication, under the feature metadata gate held shared until the table serves, so a feature cannot be enabled in between.
The copy itself holds no catalog gate, so other DDL, backups and feature activation proceed while it runs.

## Cancellation and progress

Server shutdown cancels a running restore at the next block, file or step boundary before the rename; nothing is published.
A client disconnect does not cancel it, as for BACKUP; per-statement cancellation is a separate item of the 2.0 plan.
The server logs the start, each tenth of the copied bytes, and the end, with the table, files and bytes.
The reply names the table, epoch, parent epoch, fork revision, files and bytes.

## Swap

SWAP takes the feature and backup metadata gates shared, as RECOVER TABLE does, so no backup pins a table while a swap waits for it.
It then takes the tables' DDL locks and exclusive access in name order, so two swaps of overlapping tables cannot deadlock, then the catalog's table operations.
It waits for running statements on both tables, makes their acknowledged writes durable with a WAL barrier and closes both stores; files a closing store leaves unsynced are carried to the reopened store under the new path.
One atomic directory exchange then swaps `tables/t` and `tables/t_restored` (`renameat2(RENAME_EXCHANGE)` on Linux, `renamex_np(RENAME_SWAP)` on macOS).
A platform or file system without an atomic exchange refuses the swap and reopens both tables unchanged; there is no non-atomic fallback.
After the exchange the parent directory is synced and each store is reopened under its new name as a new table object.
A crash at any point leaves both directories under their old names or both exchanged; each directory is a complete table.
A failure after the exchange that leaves a name unserved fences that name until RECOVER TABLE or a restart.
Open transactions on either table end with CONFLICT at their next statement, because their snapshot belongs to the closed store.

## Feed consumers

Watches of either table end with INTERNAL `watched table was swapped; watch it again`.
A consumer resumes by name with its saved epoch and revision; the name now reaches another epoch, so the feed answers by lineage.
A position in the parent epoch exactly at the fork revision continues on the restored table while the change feed rules prove no change is missing; any other position receives resync.
A durable slot belongs to its table's data, so it moves with the data; a restored table's slots start at the cut.

## Checks

Parser tests cover both statements and their errors; engine tests cover rights, read-only refusal, transactions and replies.
Catalog tests restore into a new name, compare chunks, versions, tombstones and slots with the backup cut, and check the new epoch and lineage.
Failure tests inject copy, verification and publication failures and show that no table or staging directory remains.
Subprocess crash tests stop at every restore step and every swap step; the restarted catalog has the table completely or not at all, and both names or neither exchanged.
Feed tests watch a swapped name from the old epoch and receive resync, or continue from the exact fork revision.
`chunkdb_verify` needs no new checks: restore and swap create no new file kinds.
