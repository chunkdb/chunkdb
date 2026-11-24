# Online table restore: design

`RESTORE TABLE t FROM 'snapshot' [AS t_restored]` copies one table of a completed backup into a running writer as a new table (#158).
`SWAP TABLES t, t_restored` then exchanges the two names atomically; the restore itself never replaces a live table.
`ALTER TABLE t RENAME TO u` moves one table to a free name with the same machinery.
The [backup design](BACKUP_DESIGN.md) defines backups, cuts and lineage; the [table catalog](TABLES_DESIGN.md) defines table publication.

## Statement and rights

The backup path is resolved like `BACKUP TO`: relative to `--backup-dir`, with no `..`, rooted path, backslash or link below the resolved directory.
`t` names a table cut in the backup; without `AS` the restored table keeps that name.
The new name must not exist, be fenced, or be published by another statement before the restore publishes.
RESTORE TABLE needs MANAGES USERS, ADMIN on `*`, or BACKUP together with ADMIN on the new name, the right CREATE TABLE needs; the backup may hold any table, and the restored table falls under the grants that match its new name.
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
Rebinding drops a column narrowing, DROP NULL check or ADD CHECK that the backup caught running: it belonged to the backed-up table, and the restored schema is the one before it.
Until publication the table is in no catalog listing and no statement can name it.
Publication takes the catalog's table operations, checks the name again and renames the directory into `tables/` without replacing anything, then syncs both parents.
The rename is the commit point; the store is opened and served afterwards, as CREATE TABLE does.
A failure before the rename removes the private directory; a crash before it leaves the directory for the next writer start to remove.
A failure after the rename that leaves the table unopened moves it out again, or fences its name until RECOVER TABLE.
The backup is only read; a backup that changes while it is copied fails the checksum check.
The feature check runs after the copy and again at publication, under the feature metadata gate held shared until the table serves, so a feature cannot be enabled in between.
The copy itself holds no catalog gate, so other DDL, backups and feature activation proceed while it runs.

## Cancellation and progress

Server shutdown, CANCEL, KILL CONNECTION or a statement timeout cancels a running restore at the next block, file or step boundary before the rename; nothing is published.
After the rename the statement reports its real outcome.
A client disconnect does not cancel it, as for BACKUP.
The server logs the start, each tenth of the copied bytes, and the end, with the table, files and bytes.
The statement also reports its files and bytes copied of the totals before the first file and after each one; SHOW CONNECTIONS lists them as `progress` until the statement completes.
The report is kept on the statement's control under its own mutex, which only the report and a listing take, so no hot path takes a new lock.
The reply names the table, epoch, parent epoch, fork revision, files and bytes.

## Swap

SWAP takes the feature and backup metadata gates shared, as RECOVER TABLE does, so no backup pins a table while a swap waits for it.
It then takes the tables' DDL locks and exclusive access in name order, so two swaps of overlapping tables cannot deadlock, then the catalog's table operations.
It waits for running statements on both tables, makes their acknowledged writes durable with a WAL barrier and closes both stores; files a closing store leaves unsynced are carried to the reopened store under the new path.
CANCEL, KILL CONNECTION or a statement timeout stops the swap only before the exchange, and both tables then keep serving under their names.
One atomic directory exchange then swaps `tables/t` and `tables/t_restored` (`renameat2(RENAME_EXCHANGE)` on Linux, `renamex_np(RENAME_SWAP)` on macOS).
A file system without an atomic exchange refuses the swap and reopens both tables unchanged; Windows refuses it before closing either table; there is no non-atomic fallback.
The [swap guard design](SWAP_GUARD.md) replaces this refusal with three renames under one guard record.
After the exchange the parent directory is synced and each store is reopened under its new name as a new table object.
A crash at any point leaves both directories under their old names or both exchanged; each directory is a complete table.
A failure after the exchange that leaves a name unserved fences that name until RECOVER TABLE or a restart.
Open transactions on either table end with CONFLICT at their next statement, because their snapshot belongs to the closed store.

## Rename

`ALTER TABLE t RENAME TO u` admits like SWAP: the feature and backup metadata gates shared, then the table's DDL lock, exclusive access and the catalog's table operations.
It needs ADMIN on both names, because the grants move with the table: each user's grant on `t` goes to `u`, the higher right kept where a user has both, and `t` keeps none.
The name `u` must be free: no table, no fence, and no directory under `tables/`.
It makes acknowledged writes durable with a WAL barrier and closes the store; CANCEL, KILL CONNECTION or a statement timeout stops it only before that.
The grants are copied to `u` first, then one rename that cannot replace an entry moves `tables/t` to `tables/u`, and the parent directory is synced; the grants on `t` are removed last.
GRANT and REVOKE take the catalog's table operations too, as DROP TABLE and MIGRATE change grants under them, so each runs before or after the whole rename.
A crash leaves the directory under one of the names, complete; the grants never name neither, and at worst name both, as a crash during DROP TABLE can leave grants of a dropped name.
A refused rename puts the copied grants back and reopens the table under `t`; a grant copy whose outcome is unknown is put back the same way.
A failure to sync the rename fences both names, as a failed swap does, because a crash could still undo it; the grants then stay on both names.
A failure to open the renamed table fences `u`; the rename is durable, so the grants on `t` are removed.
Each fence lasts until RECOVER TABLE of its name or a restart; RECOVER TABLE t then finds no directory and frees the name.
The epoch, lineage, slots and their positions stay: a consumer resumes by the new name with its saved position, without resync, and the old name reaches nothing.
Watches of the table end with INTERNAL `watched table was renamed; watch it again`, and open transactions end with CONFLICT.
MIGRATE does not take RENAME TO: its redo journal has no directory rename.

## Feed consumers

Watches of either table end with INTERNAL `watched table was swapped; watch it again`.
A consumer resumes by name with its saved epoch and revision; the name now reaches another epoch, so the feed answers by lineage.
A position in the parent epoch exactly at the fork revision continues on the restored table while the change feed rules prove no change is missing; any other position receives resync.
A durable slot belongs to its table's data, so it moves with the data; a restored table's slots start at the cut.
A slot's saved position then no longer matches its consumer, so the restore marks each slot to resync, and the swap marks both tables' slots durably before the exchange.
A watch without AFTER on a marked slot starts with resync; one with AFTER follows the epoch and lineage rules; an acknowledgement past the position clears the mark.
A crash before the exchange leaves the marks, costing one extra resync; a refused exchange puts the slot records back.

## Checks

Parser tests cover both statements and their errors; engine tests cover rights, read-only refusal, transactions and replies.
A protocol test holds a restore after each progress report and checks SHOW CONNECTIONS against it, and that the table is not visible until the reply.
Catalog tests restore into a new name, compare chunks, versions, tombstones and slots with the backup cut, and check the new epoch and lineage.
Failure tests inject copy, verification and publication failures and show that no table or staging directory remains.
Subprocess crash tests stop at every restore step and every swap step; the restarted catalog has the table completely or not at all, and both names or neither exchanged.
Rename tests check epoch, data, tombstones and slots under the new name, the ended watch, the refusals and each injected failure, the grants after a failed copy or open, and a REVOKE that waits for the rename; crash tests stop before the rename, after it and after its sync.
Feed tests watch a swapped name from the old epoch and receive resync, or continue from the exact fork revision.
Slot tests watch restored and swapped slots without AFTER and receive resync until an acknowledgement, and check the marks after each swap crash point and a refused swap.
`chunkdb_verify` needs no new checks: restore and swap create no new file kinds.
