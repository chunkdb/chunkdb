# Shared maintenance pool

Relates to #160.

A data directory shares one lazily started pool through its existing `StoreResources`.
`--maintenance-workers` selects its positive worker count, with a default of eight.
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

## Measurements

`chunkdb_maintenance_bench` with 200,000 writes, `checkpoint_update_interval` 32 and the initial barrier, so every checkpoint syncs; three runs of each case on a Ryzen 5 4600H with ext4 on a SATA SSD ([rows](../../bench/artifacts/manual-runs/maintenance-pool-20261112-linux-rows.txt), [conditions](../../bench/artifacts/manual-runs/maintenance-pool-20261112-linux-metadata.txt)).
Writes per second at 100 tables: one thread per table 130,843, 131,207 and 135,005; the pool with two workers 44,922, 46,368 and 47,552; with eight workers 111,046, 116,710 and 116,894.
At 1,000 tables: one thread per table 143,513, 148,152 and 82,565; two workers 48,518, 50,652 and 50,889; eight workers 111,105, 112,123 and 116,515.
At one table: one thread per table 19,134, 4,795 and 5,500; two workers 32,010, 32,450 and 5,864; eight workers 28,938, 29,367 and 6,134.
Process threads at 1,000 tables: 1,002 with one thread per table, 4 with two workers and 10 with eight; peak resident memory 51 MiB against 33 and 34 MiB.
Dirty FLUSH WAL p50 at 1,000 tables: 1,257, 1,234 and 1,271 ms with one thread per table; 712, 707 and 705 ms with two workers; 406, 401 and 411 ms with eight.
The paired hot-path budget of the same window met no regression criterion: relaxed medians were -1.28% to -0.24% and fsync-wal -0.10% to +1.03%.

The default is eight workers: in this case, where every checkpoint syncs, two workers write about a third of what one thread per table writes, and eight write 78% to 89% of it with ten threads.
Sustained synced checkpoints are bounded by worker count divided by sync latency; writes beyond what that absorbs within the hard bound wait for or run their chunk's checkpoint.
