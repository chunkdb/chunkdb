# Backup and restore

Take an online backup with a user who has `MANAGES USERS`:

```text
BACKUP TO '/backups/snapshot'
```

The path is on the server's filesystem. It must be absent or an empty directory,
outside the live data directory, with no symlink components. Its parent must
exist and be writable by the server. A single-process read-write server supports
backup; read-only and `--allow-multi-process` configurations refuse it. Under
`--auth none`, connections retain their unrestricted development access.

The reply contains `tables`, `files`, `bytes` and `cuts`, an array of
`{table, epoch, revision}`. File/byte counts cover the inventoried data files;
the marker and temporary guards are excluded. Each table has its own cut S:
all committed changes through S are present, and changes above S are excluded.
Revisions can have gaps. A transaction belongs to one table and is included
whole. Table cuts need not represent one shared wall-clock instant.

Writes continue while the backup runs. DDL and replacement/removal of chunk
files wait during pinning; ordinary chunk locks protect WAL flushing and prefix
selection. These holds are released before copying to the destination. Another
BACKUP receives `BUSY`. Disconnecting the requesting client or stopping the
server aborts an unfinished backup. A failed or aborted copy retains an
incomplete guard and cannot be restored; remove that destination before retrying.

The backup includes table definitions and schema history, checkpoint images and
WAL prefixes, revision bookkeeping, users and their password verifiers/rights,
and slot names. It excludes feed archives, writer locks, temporary table
operations and foreign files. Pending rollback effects are excluded from the
completed cut. Treat the backup as sensitive data, including its users file.

## Verify and restore

```bash
chunkdb_verify --data-dir /backups/snapshot
chunkdb_restore /backups/snapshot /var/lib/chunkdb/restored
chunkdb_server --data-dir /var/lib/chunkdb/restored
```

Verification is read-only. It checks the backup marker and inventory, file
checksums, table cuts and ordinary storage integrity. A backup directory cannot
be opened directly by the server; restore first. Restore requires an absent or
empty destination with an existing parent, outside the backup directory and with
no symlink components. Stop any server intended to use the destination before
restoring. Platforms without an atomic exclusive directory rename refuse
publication.

Every restored table gets a new epoch. Slot names are retained at S in that new
epoch, with a fresh baseline and no archived history; prior lost slots start
fresh too. A consumer using its old epoch receives `resync` and must rebuild its
state. Two restores of the same backup have different epochs. Users keep their
passwords and rights; server settings and TLS keys are not part of the backup.

Restore builds and syncs a sibling temporary directory before publication.
Interrupted copies remain guarded and the server refuses them. If publication
sync and guard reinstatement both fail, the tool reports an unknown publication
outcome: verify the destination before deciding whether to remove or use it.
A completed backup remains unchanged by restore.

## Space and Docker

Allow space for the copied images and WAL prefixes at the destination. Staging
uses hard links on the live data filesystem; while copying, those links retain
old images/WALs even if checkpoint or collection replaces their live names.
Restore also needs space for its sibling temporary copy.

Mount a writable backup volume when running the server in Docker, for example
`-v /srv/chunkdb-backups:/backups`, with ownership permitting the container's
`chunkdb` user to write there. Send `BACKUP TO '/backups/snapshot'` through your
client. Verify or restore with the runtime image's utilities:

```bash
docker run --rm --entrypoint chunkdb_verify \
  -v /srv/chunkdb-backups:/backups:ro chunkdb:local --data-dir /backups/snapshot
docker run --rm --entrypoint chunkdb_restore \
  -v /srv/chunkdb-backups:/backups:ro -v /srv/chunkdb-restored:/restored \
  chunkdb:local /backups/snapshot /restored/data
```
