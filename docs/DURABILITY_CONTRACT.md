# Durability

Durability means an acknowledged change survives a crash or power loss.
Each table records its durability mode.
Your filesystem and device must honor the required sync and atomic publication operations.

Restore publishes its checked successor clock and immediate-parent lineage inside a guarded private copy before exposing the new epoch.
Committed revisions keep their values; missing chunks after a later crash do not prove that replay history remains available.

## Guarantees

| Operation | Success guarantees |
| --- | --- |
| Ordinary write, relaxed | Applied in memory; power loss can lose acknowledged writes. |
| Ordinary write, fsync-wal | The WAL and required directory entries are synced before success. |
| Ordinary write, fsync-checkpoint | Synced WAL acknowledgement and synced checkpoint publication. |
| FLUSH WAL, every mode | All writes acknowledged before its arrival are durable in every table. |
| Transaction COMMIT, every mode | All changed chunks commit together durably, or none commit. |

The **WAL**, or write-ahead log, records changes used during recovery.
A **checkpoint** saves a chunk image so recovery can discard earlier log records safely.
Separate write statements are not atomic together; use [Transactions](TRANSACTIONS.md).

## Commit group contract

A statement changing several chunks, a conditional chunk mutation, or a write carrying an idempotency ID or tag is one commit group.
A transaction commits one group across all tables it changes and remains durable in every table mode.
An ordinary group follows its table mode, and a group spanning tables uses their strongest mode.
Unconditional single-chunk writes without IDs or tags retain their direct path.

Recovery publishes a complete group or rolls back its undecided members together.
A checked durable decision with a missing or damaged required member is damage and refuses recovery.
A flush, checkpoint or later synced write cannot make an earlier relaxed group durable in only one table.
Named table operations wait for groups using that table, and a multi-table backup takes one group boundary.
Restoring a subset that splits a recorded cross-table group requires explicit permission and reports the affected transaction IDs.

A transaction captures a vector of completed table revisions at its first statement.
Block statements compare the blocks they read or changed and rebase their deltas over unrelated block changes at commit.
Equal canonical block values permit ABA; chunk and area statements retain whole-chunk version conflicts.
An altered, renamed, swapped, restored or dropped touched table causes `CONFLICT reason=table_changed`.

Idempotency records share their write's durability and are bounded by the configured retention window and byte limit.
A missing retained ID does not prove that an older write was never applied.
Tags describe a group and its change-feed entries rather than individual blocks.
The [commit group design](design/COMMIT_GROUPS.md) specifies publication, recovery and retention invariants.

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
Outside Windows, writes and checkpoints continue while they run.
On Windows, a write that checkpoints its chunk or undoes a failed WAL append waits until the syncs end.
Above 65,536 tracked files and directories, a table tracks its changed large-chunk directories (`L_<lx>_<ly>`) instead, and the barrier syncs every file in each of them.
Above 65,536 changed large-chunk directories, the barrier syncs every file and directory of the table.

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
It leaves revision and recovery records unchanged; restore a consistent backup or reinitialize the whole store.
Read-only inspection remains available and reports an unknown completed revision as null.

## Platforms and scope

- Linux uses fdatasync where applicable, with fsync fallback and directory fsync.
- macOS requests F_FULLFSYNC and falls back to fsync for unsupported operations.
- Windows requires FlushFileBuffers and the directory-sync capability used by publication and snapshot bookkeeping.
- Required atomic publication or sync failures return errors, including required Windows bookkeeping in relaxed mode.
- Replication and durability across machines are unavailable.

`chunkdb_verify` inspects files without modifying them; the server always opens its data directory for writing.
See [Storage format](STORAGE_FORMAT.md), [Backup and restore](BACKUP.md), and [Limitations](KNOWN_LIMITATIONS.md).
Implementation details are in [Persistence design](design/PERSISTENCE.md).
