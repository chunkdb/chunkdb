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

Clean shutdown attempts pending barrier work and logs failure.
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

## WAL archive

With `--archive-dir` or `--archive-command`, every checkpoint keeps its WAL as a sealed segment, and checkpoints publish their images synced in every mode.
Every `--archive-interval-ms`, at start and at clean shutdown, the server attempts to seal each table: it samples a revision S as backup does, syncs the WALs and checkpoints every chunk whose live WAL holds a change at or below S.
If it cannot capture the manifest of every catalog table, the pass leaves the previous seal record in place and reports the error in SHOW STATUS.
It ships the sealed segments, then publishes a seal record; a record's revision S for a table means that every change at or below S is in a segment the record or an earlier one of its chain lists, durable in the archive before the record.
The record also holds a time bound taken before S: a change above S has a commit time at or above it, unless it is a conditional or expression write that took its commit time before its revision and was in flight while S was sampled.
Changes after the newest record are not in the archive yet.
A failed copy or command is retried every interval; until then the files stay in the data directory.
A record is written to the data directory before it ships and becomes the chain's last only after it shipped, so a crash at any step leaves a chain without a fork, and the next start ships what is left.
A dropped table's changes after the last pass before DROP are not archived.
A writer start without archiving ends the chain.

## Failures

An ordinary rejected mutation changes nothing, except when its error explicitly reports an unknown write outcome.
An unknown outcome means the durable decision could not be confirmed.
Run RECOVER TABLE on that table, or restart the writer, for recovery before retrying application work.
A failed rollback also requires that recovery before durability-changing operations resume.

Named migrations use a durable decision containing their metadata change and completion record.
After an unknown migration outcome, restart and retry the same name and text.
Checkpoint or collection failures do not silently weaken synced durability.
Malformed WAL headers and interior frames are refused; only crash-shaped incomplete tails are repaired during writer recovery.

Keep `chunkdb.version` and `.chunkdb.initialized` with every table's artifacts.
With both records missing, a writer refuses chunk artifacts or a startup inventory that is capped or unavailable.
It leaves revision and recovery records unchanged; restore a consistent backup, [rebuild the clock](REBUILD_VERSION_CLOCK.md) offline with `chunkdb_admin rebuild-version-clock`, or reinitialize the whole store.
A missing or malformed clock beside a valid initialized marker, or a malformed marker, is also refused and has the same remedies.
Read-only inspection remains available and reports an unknown completed revision as null.

## Platforms and scope

- Linux uses fdatasync where applicable, with fsync fallback and directory fsync.
- macOS requests F_FULLFSYNC and falls back to fsync for unsupported operations.
- Windows requires FlushFileBuffers and the directory-sync capability used by publication and snapshot bookkeeping.
- Required atomic publication or sync failures return errors, including required Windows bookkeeping in relaxed mode.
- SWAP TABLES needs an atomic directory exchange (Linux `renameat2`, macOS `renamex_np`); Windows and file systems without one refuse it.
- Replication and durability across machines are unavailable.

`chunkdb_verify` inspects files without modifying them; the server always opens its data directory for writing.
See [Storage format](STORAGE_FORMAT.md), [Backup and restore](BACKUP.md), and [Limitations](KNOWN_LIMITATIONS.md).
Implementation details are in [Persistence design](design/PERSISTENCE.md).
