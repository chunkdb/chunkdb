# Durability contract in 2.0

Each table records its own durability mode in its manifest.
The guarantees require a filesystem and device that honor the requested sync and atomic publication operations.
Replication and durability across machines are not provided.

## Acknowledgements

| Operation / mode | Success guarantees |
|---|---|
| Ordinary writes, `relaxed` | Applied in memory; WAL frames may remain batched or unsynced, so power loss can lose acknowledged writes. |
| Ordinary writes, `fsync-wal` | The WAL frame and required new directory entries are synced before acknowledgement. |
| Ordinary writes, `fsync-checkpoint` | Synced WAL acknowledgement and synced checkpoint publication. |
| `FLUSH WAL`, every mode | All writes acknowledged before the statement arrived are durable in every table. |
| Transaction COMMIT, every mode | Every changed chunk of the table is committed together, durably, or none is committed. |

Every mutation is one checked WAL frame, including full-size SET CHUNK; recovery applies a complete frame or none of it.
Ordinary operations on several chunks have no common atomic boundary outside a transaction.
Changing a table's durability mode first flushes its pending batches; the new mode applies after ALTER replies.

## Checkpoints, deletion and barriers

A checkpoint writes a same-directory temporary image, closes it with error checking, and atomically replaces the image.
In synced modes the image and directory entry are synced before deleting the WAL it replaces.
Relaxed mode preserves this durability floor after a successful FLUSH WAL and conservatively after reopening an initialized store.
Consequently a later checkpoint cannot discard an earlier barrier's durable state.

Empty-chunk collection persists an empty-state frame where needed, removes the image before the WAL, and syncs these steps when durability requires it.
Recovery therefore cannot resurrect deleted contents from an older image.
Present blocks whose values are zero are not empty blocks.

FLUSH WAL serializes concurrent barriers, flushes pending batches, and syncs tracked WALs, images and directories table by table.
Writes acknowledged after it started may require the next barrier.
Sync failure is returned and retained work remains eligible for a later barrier.
A clean shutdown also attempts the barrier work and logs failure; it is not a substitute for receiving a successful explicit barrier.
After a crash, a new process's barrier cannot recover acknowledged writes already lost by that crash.

## Commit decisions and failures

Conditional SET CHUNK first durably records its previous WAL boundary as CKRB, then writes the new state and commits by atomically publishing synced CKRC.
Before commit, failure restores memory and truncates/removes the WAL to the previous boundary.
Transaction COMMIT records all changed chunks' durable boundaries as CKTB, appends and syncs one frame per changed chunk, then publishes synced CKTC before exposing the new states.
Startup rolls back CKRB/CKTB boundaries and preserves CKRC/CKTC frames.
Cleanup failure after a durable commit is logged rather than reported as a rejected mutation.

An ordinary error means a mutation was not applied, except an explicit `INTERNAL write outcome unknown: ...` outcome.
A decision that became visible but could not be made durable has an unknown outcome and fences the store until restart.
A failed local rollback also fences durability-changing operations until startup completes repair; its rejected mutation remains rejected.
Retry application work only after distinguishing these outcomes and completing required recovery.

Named migrations prepare all participating metadata before durably publishing a redo journal.
Journal publication commits the step; recovery completes its schema/users/slot changes and ledger record together.
Post-decision failures fence the catalog until writer restart, including a completion whose ledger exists but whose table failed to reopen.
Retry the same migration name and text after restart to obtain applied/skipped behavior.

## Read-only processes

Checked snapshot generations bracket image/WAL/intent changes: odd before transition, a new even value after coherent completion.
A read-only chunk load accepts only one unchanged even generation around its complete collection and applies pending rollback boundaries without modifying files.
Overlapping or nearby transitions may share one odd epoch; publication can linger up to 50 ms.
Read-only loads retry within a bounded budget and fail closed on malformed state, a persistent odd generation or exhaustion.
A crash can leave odd state even after artifact changes completed; a writer must recover and publish a fresh generation.
Successful FLUSH WAL publishes the deferred even generation before returning.
This provides coherence per chunk, rather than a store-wide snapshot.

## Catalog, slots and backup

Manifests are synced and published without replacement before initializing directory/table contents.
CREATE and DROP use atomic directory publication/removal; success includes the required parent-directory syncs.
ALTER atomically publishes a checked manifest and schema history.

Slot activation quiesces writers and maintenance and durably establishes a checkpoint baseline before its initial position.
Slot passes sync completed producer state and persist a durable frontier; WATCH SLOT emits only through that frontier, including relaxed writes.
ACK positions are monotonic and are batched per table at most every 100 ms; UNWATCH persists its own eligible ACK before replying.
Archives are released only using persisted positions; a sync failure fences the store.
Activation alone does not establish the permanent FLUSH WAL durability floor.

Online backup pins completed per-table cuts and copies their checked inventory before publishing the synced completion record.
It captures schema, users and the completed migrations ledger under one metadata gate, waits for active migrations and refuses unfinished recovery or a fenced catalog.
Restore publishes a guarded new directory with fresh identities; an incomplete guard takes precedence over any completion record.
See [backup](BACKUP.md) and [storage format](STORAGE_FORMAT.md).

## Platform operations

Linux uses fdatasync where applicable with fsync fallback, and fsync for directories.
macOS requests F_FULLFSYNC for file and directory durability; unsupported-operation errors fall back to fsync.
Windows uses FlushFileBuffers/_commit and atomic handle-based rename; required directory-sync capability must be available or the operation fails.
These requests cannot compensate for storage that lies about persistence, damaged media or loss of required artifacts.
