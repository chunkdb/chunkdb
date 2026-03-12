# Concurrency, Runtime, and Integrity Guarantees

## 1. In-Memory Locking Model

- Global large-chunk registry: `std::mutex`
- Per-large-chunk regular-chunk map: `std::mutex`
- Per-regular-chunk payload: `std::shared_mutex`
  - shared for `GET`/`CHUNK`/`CHUNKBIN`
  - unique for `SET`

Effects:
- concurrent reads on same chunk: allowed
- write vs read on same chunk: serialized
- operations on different chunks: can run concurrently

## 2. Lock Order

To avoid deadlocks:
1. global large-chunk mutex
2. large-chunk mutex
3. regular-chunk payload mutex

The engine never acquires two regular-chunk payload locks in one operation.

## 3. Server Runtime Concurrency

- Accept loop enqueues accepted sockets.
- Fixed worker pool processes connections.
- Connection parsing is buffered (not byte-by-byte recv loops).

This replaces detached thread-per-connection behavior and provides bounded thread growth.

## 4. Inter-Process Safety

Default behavior: one process per `data_dir`.

- `ChunkStore` acquires an exclusive lock file (`.chunkdb.lock`) in `data_dir`.
- If lock is already held, startup fails.

Override exists (`allow_multiple_processes` / `--allow-multi-process`) but is unsafe unless external coordination is present.

## 5. Cache / Memory Control

- `max_loaded_chunks` limits in-memory chunk cache size.
- LRU-style eviction removes least-recently-used chunks that are not currently referenced.

This prevents unbounded growth in long-running sparse-world workloads.

## 6. Durability Modes

### `relaxed`
- WAL append without fsync.
- Lowest latency, weakest power-loss guarantee.

### `fsync-wal`
- fsync WAL after each appended delta record.
- Acknowledged writes are durable in WAL after successful fsync.

### `fsync-checkpoint`
- `fsync-wal` semantics plus fsync for checkpointed `.chk` + directory updates.
- Strongest current mode.

## 7. Crash/Power-Loss Semantics

- Normal crash during runtime:
  - WAL replay restores latest committed deltas.
- Power loss semantics depend on durability mode and filesystem behavior.
- Engine does not provide full ACID transactional semantics across multiple chunks.

## 8. What Is Not Guaranteed Yet

- No cross-chunk atomic transactions.
- No replication.
- No consensus or distributed durability.
- No claim of full ACID database guarantees.
