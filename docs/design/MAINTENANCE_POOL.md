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

## WAL barriers

`FLUSH WAL` snapshots the catalog and acquires each table lease before submitting its barrier.
A worker never waits to acquire a table lease, which avoids occupying the pool behind a schema operation that is itself draining maintenance.
Each submitted barrier owns its lease and table until its WAL barrier finishes.
All submitted tables are awaited, including when one barrier fails.
The first failure in catalog order is returned after the other submitted barriers finish.
The per-store barrier and directory-sync rules remain unchanged.

## Validation

Deterministic scheduler gates cover FIFO fairness, worker concurrency, coalesced notification, drain and shutdown.
Catalog tests cover simultaneous table barriers, failure propagation and dropping or reopening a table while a barrier holds its lease.
Existing checkpoint, eviction, backup, migration, feature-activation and crash suites cover the store lifecycle boundaries.
Measurements use 1, 100 and 1,000 tables and record process threads, memory, barrier latency and write throughput on the benchmark server.
The hot-path budget window is required before this change is ready for review.
