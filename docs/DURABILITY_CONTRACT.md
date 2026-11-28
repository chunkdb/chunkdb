# Durability

Durability means an acknowledged change survives a crash or power loss.
Each table records its durability mode.
Your filesystem and device must honor the required sync and atomic publication operations.

Restore publishes its checked successor clock and immediate-parent lineage inside a guarded private copy before exposing the new epoch.
Committed revisions keep their values; missing chunks after a later crash do not prove that replay history remains available.

## Guarantees

| Operation | Success guarantees |
| --- | --- |
| Ordinary write, relaxed | Applied in memory; power loss can lose acknowledged writes, and a process crash can lose those not yet written to the WAL, up to four checkpoint intervals per chunk while a background checkpoint syncs it. |
| Ordinary write, fsync-wal | The WAL and required directory entries are synced before success; a checkpoint syncs its image and directory before it removes the WAL. |
| Ordinary write, fsync-checkpoint | The same as fsync-wal. |
| FLUSH WAL, every mode | All writes acknowledged before its arrival are durable in every table. |
| Transaction COMMIT, every mode | All changed chunks commit together durably, or none commit. |
| RESTORE TABLE | The restored table is durable and complete under its name, or nothing is published. |
| SWAP TABLES | Writes acknowledged before it are durable, and both names are exchanged durably. |
| ALTER TABLE ... RENAME TO | Writes acknowledged before it are durable, and the table is renamed durably; after a crash it has one of the two names. |

The **WAL**, or write-ahead log, records changes used during recovery.
A **checkpoint** saves a chunk image so recovery can discard earlier log records safely.
Ordinary writes to several chunks are not atomic together; use [Transactions](TRANSACTIONS.md).

## Make earlier writes durable

```cql docs-check
CREATE TABLE world (kind u8) CHUNK 2 x 2
SET BLOCK 0 0 IN world kind = 1
FLUSH WAL
```

```output
OK
1
OK
```

FLUSH WAL is a barrier: writes acknowledged after it starts may need another barrier.
Changing a table's durability mode first flushes pending writes.
A later checkpoint cannot discard state protected by a successful barrier.

Clean shutdown attempts pending barrier work for tables that are still writable and logs failure.
It does not replace receiving a successful explicit FLUSH WAL.
After a crash, a new barrier cannot recover writes already lost.

## Barrier cost

FLUSH WAL and clean shutdown sync each file and directory written without a sync since the previous barrier.
In relaxed mode that is about one sync per chunk written since then, so their time grows with the number of changed chunks.
The syncs run on up to 16 threads per table.
FLUSH WAL schedules table barriers on the shared maintenance pool; `--maintenance-workers` bounds the number of tables flushing concurrently.
If a table fails, the other submitted tables still finish before the error is returned.
Outside Windows, writes and checkpoints continue while they run.
On Windows, a write that checkpoints its chunk or undoes a failed WAL append waits until the syncs end.
Above 65,536 tracked files and directories, a table tracks its changed large-chunk directories (`L_<lx>_<ly>`) instead, and the barrier syncs every file in each of them.
Above 65,536 changed large-chunk directories, the barrier syncs every file and directory of the table.

## Failures

An ordinary rejected mutation changes nothing, except when its error explicitly reports an unknown write outcome.
An unknown outcome means the durable decision could not be confirmed.
Run RECOVER TABLE on that table, or restart the writer, for recovery before retrying application work.
A failed rollback also requires that recovery before durability-changing operations resume.

A required sync can fail after the kernel has cleared dirty pages. A subsequent sync may succeed even though the failed writeback never reached storage.
A failed sync of a live WAL, image, metadata record or directory fences its table for writes, barriers and feed-slot advancement until RECOVER TABLE or a writer restart. Reads can continue where the table still has a coherent state.
An unpublished temporary file can be discarded and written again to a fresh file. If a users-file replacement was visible when publication failed, the next users change first writes a fresh copy of the in-memory record.
If a conditional or transaction commit marker becomes visible before its directory sync fails, the operation reports an unknown outcome and leaves the marker for recovery.
If the decision was already durable when an intent cleanup or checkpoint sync failed, the operation still reports the committed result. The failure fences subsequent writes.

A failed sync during FLUSH WAL reports `OUTCOME_UNKNOWN stage=barrier_sync` with the affected table.
The server finishes the other tables' barriers before returning. A failure before the sync can be retried while the table is healthy.

### Syncs outside an open table

| Context | Failure and recovery |
| --- | --- |
| Users file | An unpublished replacement can be retried with a fresh file. A visible replacement with an unconfirmed directory sync reports `OUTCOME_UNKNOWN stage=users_record`. The registry keeps the visible users state and writes it to a fresh file before another users change can succeed. A sync failure opening the registry refuses startup. |
| Named migration and feature activation records | A failed private preparation can be discarded. After a decision becomes visible, a sync failure fences the catalog until writer restart, which checks and completes any retained decision. Named migrations can then be retried by the same name and text. |
| Data directory manifest | A sync failure during first publication refuses startup. A failed version-floor update stops DROP before it moves the table. A later attempt writes a fresh manifest and only a successful publication advances the in-memory floor. Feature activation follows the catalog fence above. |
| Backup and restore staging | A failed sync aborts the copy or its publication. A fresh attempt writes fresh output files, and a copy that still has its incomplete guard is treated as interrupted. A failed destination sync leaves the source tables' admission unchanged. |
| Table directory publication | CREATE and RESTORE TABLE attempt to retire a failed publication and fence the name if they cannot. A failed sync after DROP moves a table reserves the name until recovery. A failed sync after a swap or rename fences the affected names. |
| Slot records and version clock | Startup initialization and recovery refuse table opening on a sync failure. A failure in a private restored copy aborts that copy. SWAP uses a fence for each closed participant while writing its slot marks. A live slot record or clock publication uses its open table's fence. A failed offline clock rebuild returns an error to the operator. |
| Recovery and rollback | Failed startup repairs refuse writer opening. An already fenced table can still repair a rejected WAL tail or publish a fresh snapshot close record where read-only replay is coherent. Those operations leave the durability fence in force. |

Malformed WAL headers and interior frames are refused; only crash-shaped incomplete tails are repaired during writer recovery.

Keep `chunkdb.version` and `.chunkdb.initialized` with every table's artifacts.
With both records missing, a writer refuses chunk artifacts or a startup inventory that is capped or unavailable.
It leaves revision and recovery records unchanged; restore a consistent backup, [rebuild the clock](REBUILD_VERSION_CLOCK.md) offline with `chunkdb_admin rebuild-version-clock`, or reinitialize the whole store.
A missing or malformed clock beside a valid initialized marker, or a malformed marker, is also refused and has the same remedies.
Read-only inspection remains available and reports an unknown completed revision as null.

## Platforms and scope

- Linux uses fdatasync where applicable, with fsync fallback and directory fsync.
- macOS requests F_FULLFSYNC and falls back to fsync for unsupported operations.
- Windows requires FlushFileBuffers, native file IDs for scoped live-artifact syncs, and the directory-sync capability used by publication and snapshot bookkeeping.
- Required atomic publication or sync failures return errors, including required Windows bookkeeping in relaxed mode.
- SWAP TABLES needs an atomic directory exchange (Linux `renameat2`, macOS `renamex_np`); Windows and file systems without one refuse it.
- Replication and durability across machines are unavailable.

`chunkdb_verify` inspects files without modifying them; the server always opens its data directory for writing.
See [Storage format](STORAGE_FORMAT.md), [Backup and restore](BACKUP.md), and [Limitations](KNOWN_LIMITATIONS.md).
Implementation details are in [Persistence design](design/PERSISTENCE.md).
