# Concurrency invariants in 2.0

The public contracts are [transactions](../TRANSACTIONS.md), [durability](../DURABILITY_CONTRACT.md), [change feed](../CHANGE_FEED.md) and [backup](../BACKUP.md).

## Locking and ownership

One writer process owns a data directory; the opt-out flag is not shared-writer support.
Catalog leases keep table stores alive through statements; DDL drains leases before replacement.
Plain mutations hold one chunk payload lock; transaction commits take all required locks in coordinate order.
The history mutex is innermost and never causes chunk loading or locking while held.
WAL stream-pool closure skips chunks already locked by the calling commit.
Shared cache eviction does not hold a victim chunk's lock while waiting for metadata maintenance admission.

## Metadata and durability

DDL and migrations share table-before-catalog admission and recheck catalog health after waiting; named migrations also enter the backup metadata gate.
Backup maintenance admission excludes checkpoint replacement, collection and archival while pinning files.
Checkpoint attempts defer rather than wait for that gate while holding chunk locks.
Catalog/user/table ordering avoids holding user metadata locks across table-drain waits.
Failed post-decision recovery fences new work until restart.

Checked snapshot generations bracket persisted transitions; read-only processes never repair state and accept only unchanged even observations.
Required sync errors propagate or fence the store; Windows capability failures do not silently weaken the durability contract.
See [storage](../STORAGE_FORMAT.md) for rollback and commit records.

## Producer order and feed

Writers publish a per-thread lower bound before revision reservation and retain it through completion, including transaction postcommit work.
The feed watermark excludes all incomplete lower revisions; sender queues merge completed changes in revision order.
Transactions emit one whole change, optionally clipped by chunk-coordinate areas.
Live buffers and network output are bounded; ordinary watches resynchronize and slots catch up through durable archives.
Slot frontiers and persisted acknowledgements never exceed completed durable work.
Socket/TLS ownership moves to feed I/O only after WATCH admission, and teardown releases slot ownership before a subsequent watch can use it.
