# Shared maintenance pool

Relates to #160.

A data directory shares one lazily started pool through its existing `StoreResources`.
`--maintenance-workers` selects its positive worker count, with a default of two.
Standalone stores sharing the same resources use the same pool.
Read-only opening starts no maintenance work.

## Scheduling and ownership

Each writable store registers one reusable job when background maintenance is enabled.
A ready job appears once in a FIFO queue and runs on at most one worker.
One turn processes one checkpoint or one eviction pass, then joins the queue tail if work remains.
A busy table therefore cannot repeatedly run ahead of another ready table.
Checkpoint admission keeps the existing per-table bound and inline backpressure.
Ready-queue publication allocates no memory after job registration.

A store publishes its queue change and scheduling notification under its maintenance mutex.
Stopping maintenance closes admission under that mutex and waits for all accepted work to finish.
The registration is released before its store can close or reopen.
Feature activation, migrations, backup and table recovery retain their existing stop and restart boundaries.
Scheduler locks are never held while a callback takes store, chunk or catalog locks.
Pool destruction drains accepted jobs and joins its workers.

## Checkpoint file work

In relaxed mode a background checkpoint writes and syncs the chunk image and removes the WAL without holding the chunk lock.
It takes the image, closes the WAL stream and marks the chunk under the lock, and clears the mark when it has retaken the lock.
While the mark is set, an ordinary block write changes only memory and the batch; SET CHUNK waits for the mark.
Its group-commit flush and checkpoint check wait for the checkpoint, which runs both when it ends.
A write waits for the checkpoint once the writes since the image reach the hard bound of four checkpoint intervals or four WAL thresholds.
Transactions, conditional writes, barriers, eviction, backup and feed-slot preparation wait for the mark before they touch the chunk's files.
The checkpoint holds the publish lock from before the image write until the WAL is removed, so barrier drains and the durability floor order with it as before.
Synced durability modes and tables with a feed archive keep the whole checkpoint under the chunk lock, because their writes need the files.

Holding the chunk lock through the syncs made a writer that revisited the chunk wait for them.
With fewer workers than tables, those waits came one after another, and writes ran at the checkpoint rate.

## WAL barriers

`FLUSH WAL` snapshots the catalog and acquires each table lease before submitting its barrier.
A worker never waits to acquire a table lease, which avoids occupying the pool behind a schema operation that is itself draining maintenance.
Each submitted barrier owns only its table lease and releases it within its turn.
The caller keeps its catalog snapshot until every submitted barrier ends, so a table dropped meanwhile is destroyed by the caller or the drop, never on a worker.
A worker releases its reference to a job before waiters can see the end of the turn.
All submitted tables are awaited, including when one barrier fails.
The first failure in catalog order is returned after the other submitted barriers finish.
The per-store barrier and directory-sync rules remain unchanged.

## Validation

Deterministic scheduler gates cover FIFO fairness, worker concurrency, coalesced notification, drain and shutdown.
Catalog tests cover simultaneous table barriers, failure propagation, and dropping or reopening a table while a barrier holds its lease.
A catalog test with one worker drops a table during its barrier and closes the catalog while that worker is still between turns.
Existing checkpoint, eviction, backup, migration, feature-activation and crash suites cover the store lifecycle boundaries.
Measurements use 1, 100 and 1,000 tables and record process threads, memory, barrier latency and write throughput on the benchmark server.
A store regression test pauses a background checkpoint before its WAL removal and requires writes to that chunk to return, up to the hard bound, and to survive a reopen.
The hot-path budget window is required before this change is ready for review.

## Measurements

`chunkdb_maintenance_bench` with 200,000 writes, `checkpoint_update_interval` 32 and the initial barrier, so every checkpoint syncs, on an Apple M1 Pro with APFS, three runs, other load present.
Writes per second at 100 tables: main 6,894, 7,342 and 7,370; the pool with checkpoints under the chunk lock 2,467, 4,147 and 4,038; with the file work outside the lock and two workers 6,898, 9,077 and 8,483.
At 1,000 tables: main 7,042 and 7,515, a third run lost to a full disk; under the chunk lock 3,166 and 2,786; outside the lock 6,897, 6,848 and 8,441.
At one table: main 7,702, under the chunk lock 6,855, outside the lock 8,708, one run each.
Dirty FLUSH WAL p50 at 100 tables: main 863, 760 and 493 ms; under the chunk lock 589, 455 and 514; outside the lock 574, 537 and 634.
At 1,000 tables: main 10,771 and 5,796 ms; under the chunk lock 6,673 and 5,478; outside the lock 6,948, 7,043 and 5,641.
Four and eight workers ran 3,888 to 7,860 writes per second: more workers start checkpoints sooner, each absorbs fewer writes, and on this machine the extra syncs cost more than their overlap gained.
The default stays at two workers.

Sustained synced checkpoints are bounded by worker count divided by sync latency.
Writes beyond what that absorbs within the hard bound wait for or run their chunk's checkpoint.
The Linux benchmark-server rates imply about 3.7 ms per synced checkpoint, so two workers absorb about 69,000 writes per second at interval 32 and about 550,000 at the default 256.
