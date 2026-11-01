# Back up and restore data

You copy a running server and restore its data into a new directory.
Use a connection with MANAGES USERS and a server configured with `--backup-dir`.

## 1. Create the backup

```cql docs-check
CREATE TABLE world (kind u8) CHUNK 2 x 2
SET BLOCK 0 0 IN world kind = 1
BACKUP TO 'snapshot'
```

```output
OK
1
tables = 1
files = 8
bytes = 615
cuts:
  1) table = world, epoch = <epoch>, revision = 1
```

The destination is relative to `--backup-dir` and must be absent or empty, outside the live data directory.
Use `/` separators; rooted paths, backslashes, `..` components, and symbolic links or Windows reparse points below the resolved backup directory are refused.
Missing parents are created.
The configured backup and data directories themselves may use symbolic links or Windows directory junctions.

Writes continue during copying.
Each table has its own cut, a revision through which all its committed changes are included.
Transactions are included whole, but table cuts need not share one instant.
Tables created after capture or dropped before capture completes can be omitted.

## 2. Verify and restore

The check below uses the backup and data directories you configured:

```sh
chunkdb_verify --data-dir /backups/snapshot
chunkdb_restore /backups/snapshot /var/lib/chunkdb/restored
chunkdb_server --data-dir /var/lib/chunkdb/restored
```

Verification checks inventory, checksums, table cuts, and storage integrity without modifying the backup.
The backup includes both column domains, coordinate-index boundaries, retained tombstones, and their deletion floors.
Restore needs an absent or empty destination outside the backup directory.
Stop any server intended to use that destination first.
A server started without `--read-only` refuses a backup directory.

Restored tables receive new epochs, meaning new identities for feed consumers.
Saved slot names start at their restored table cuts, without previous archived history.
Each table records its immediate parent epoch and backup cut as its **lineage**.

Consumers exactly at that parent cut can continue when the completed cut or retained history proves no changes are missing.
Earlier positions need their retained replay prefix; an unrelated epoch or unavailable prefix requires rebuilding after a lineage-bearing resync.
Users retain passwords and rights; completed migrations retain names, text, and order.

Restore publishes a checked successor clock before exposing the new epoch; it preserves committed chunk revisions.
It also preserves chunk commit times and deletion floors while rebinding every index and log to the new table identity.
After later crashes or restarts, missing chunk files cannot prove that no history was lost.
Without durable proof, an exact-cut request also resyncs.
Existing backed-up tables need `table_lineage` enabled on the source before creating their backup.

## 3. Inspect a backup

Serve a backup without restoring it:

```sh
chunkdb_server --read-only --data-dir /backups/snapshot
```

At startup the server checks every file against the backup inventory, so it reads the whole backup once.
A missing, changed or extra file, or an incomplete backup, refuses the start.
Reads return each table at its cut, and logins use the users and rights in the backup.
Nothing in the backup is written, and the users file keeps its permissions, so the backup can still be verified and restored.
The inventory check runs only at startup: a file changed or removed while the server runs is not noticed, and a chunk whose files were removed then reads as absent.
Statements that change anything are refused; see [Read-only](SERVER_FLAGS.md#read-only).

## Contents and space

Backups include definitions, schema history, images, WAL prefixes, users, rights, slots, and completed migration records.
They exclude feed archives, server settings, and TLS keys.
Protect backups as sensitive data.
Allow space for the copied images and WAL prefixes, plus a temporary restore copy.

During copying, retained source files can continue consuming disk space even after checkpoints replace their live names.
A failed or interrupted backup can leave them in `.chunkdb.backups` inside the data directory.
The next backup or server start removes them.
The server logs a warning for anything it cannot remove.
[Docker](DOCKER.md) uses `/var/lib/chunkdb/backups` by default; mount writable storage there to keep backups outside the live volume.

## If it fails

- `BUSY`: wait for the existing backup to finish.
- Missing configuration: set `--backup-dir` before starting the server.
- Unsafe or nonempty destination: choose an empty path under the backup directory.
- Incomplete backup: remove that failed destination before retrying; it cannot be restored.
- Unfinished migration recovery: restart the writer before taking another backup.
- A fenced table: run RECOVER TABLE before taking another backup.
- Unknown restore publication outcome: verify the destination before deciding whether to remove or use it.

Stopping the server aborts unfinished copying; client disconnect leaves the backup running.
See [Server statements](reference/SERVER.md), [Storage format](STORAGE_FORMAT.md), and [Durability](DURABILITY_CONTRACT.md).
