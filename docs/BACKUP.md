# Backup and restore in 2.0

Take an online backup with a user who has `MANAGES USERS`:

```text
BACKUP TO 'snapshot'
```

Start the server with `--backup-dir /backups`.
The quoted destination is a name or relative path under that directory, using `/` between components on every platform; rooted paths, backslashes, `..` components and symlinks below the resolved backup directory are refused.
The backup directory itself and the live data directory may use symlinked paths, including macOS `/tmp`.
The destination must be absent or empty, outside the live data directory; missing parents are created.
Without `--backup-dir`, BACKUP returns an error explaining how to enable it.
Backup requires the server's data-directory writer lock.
`--auth none` allows backup within the same configured directory.

The reply contains `tables`, `files`, `bytes` and `cuts`, an array of `{table, epoch, revision}`.
File/byte counts cover the inventoried data files; the marker and temporary guards are excluded.
Each table has its own cut S: all committed changes through S are present, and changes above S are excluded.
Revisions can have gaps.
A transaction belongs to one table and is included whole.
Table cuts need not represent one shared wall-clock instant.
The table list is captured before pinning; tables created afterwards are omitted.
A table dropped before its pin is acquired is also omitted; the marker and returned cuts list only the copied tables.

Writes continue while the backup runs.
Ordinary DDL waits only for the table currently being pinned, and checkpoint/collection of its chunk files waits during pinning; ordinary chunk locks protect resident WAL flushing.
These holds are released before copying to the destination.
Another BACKUP receives `BUSY`.
Named migrations wait until metadata capture finishes; the backup includes their schema changes, user grants and completed ledger together.
Migrations can proceed during file copying.
Backup waits for an active migration to complete and then rechecks health; unfinished recovery or a fenced catalog refuses backup.
Stopping the server aborts an unfinished backup; client disconnect and half-close leave it running to completion.
An ordinary failure or aborted copy retains an incomplete guard and cannot be restored; remove that destination before retrying.

The backup includes table definitions and schema history, checkpoint images and WAL prefixes, revision bookkeeping, users and their password verifiers/rights, slot names and completed named migration records.
It excludes feed archives, writer locks, temporary table operations and foreign files.
Pending rollback effects are excluded from the completed cut.
Treat the backup as sensitive data, including its users file.

## Verify and restore

```bash
chunkdb_verify --data-dir /backups/snapshot
chunkdb_restore /backups/snapshot /var/lib/chunkdb/restored
chunkdb_server --data-dir /var/lib/chunkdb/restored
```

Verification is read-only.
It checks the backup marker and inventory, file checksums, table cuts and ordinary storage integrity.
A backup directory cannot be opened directly by the server; restore first.
Restore requires an absent or empty destination outside the backup directory.
Symlinked parent paths are allowed.
Missing parents are created; the tool needs permission to write them.
Stop any server intended to use the destination before restoring.
Platforms without an atomic exclusive directory rename refuse publication.

The restored data directory gets a new `data_dir_id`, and every table gets a new epoch.
Slot names are retained at S in that new epoch, with a fresh baseline and no archived history; prior lost slots start fresh too.
A consumer using its old epoch receives `resync` and must rebuild its state.
Two restores of the same backup have different epochs.
Users keep their passwords and rights.
Migration names, statements, users, timestamps and order are retained; retrying a completed step with the same text and required current rights returns `skipped`.
The ledger is rebound to the new data-directory identity.
Server settings and TLS keys are not part of the backup.

Restore builds and syncs a sibling temporary directory before publication.
Failed copies remove their temporary directory; one left by a crash is identified in the next restore error.
Interrupted published copies remain guarded and the server refuses them.
If publication sync and guard reinstatement both fail, the tool reports an unknown publication outcome: verify the destination before deciding whether to remove or use it.
A completed backup remains unchanged by restore.

## Space and Docker

Allow space for the copied images and WAL prefixes at the destination.
Staging uses hard links on the live data filesystem; while copying, those links retain old images/WALs even if checkpoint or collection replaces their live names.
Restore also needs space for its sibling temporary copy.
If backup staging cleanup fails, the completed copy remains usable and the server logs a warning.
The next writer start removes recognized staging copies belonging to that data directory, including a crash before the staging owner guard is written; verification reports remaining entries.

Mount a writable backup volume when running the server in Docker, for example `-v /srv/chunkdb-backups:/var/lib/chunkdb/backups`, with ownership permitting the container's `chunkdb` user to write there.
Send `BACKUP TO 'snapshot'` through your client; the image sets `--backup-dir /var/lib/chunkdb/backups` by default.
Verify or restore with the runtime image's utilities:

```bash
docker run --rm --entrypoint chunkdb_verify \
  -v /srv/chunkdb-backups:/backups:ro chunkdb:local --data-dir /backups/snapshot
docker run --rm --entrypoint chunkdb_restore \
  -v /srv/chunkdb-backups:/backups:ro -v /srv/chunkdb-restored:/restored \
  chunkdb:local /backups/snapshot /restored/data
```
