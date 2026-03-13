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

## 4. Inter-Process Safety (SWMR)

Default model: **Single-Writer / Multi-Reader** per `data_dir`.

- Writer ownership is coordinated under `data_dir/.chunkdb.lock/`:
  - `writer.lock`: OS file lock for active writer exclusivity.
  - `writer.meta`: metadata heartbeat (`session_id`, `pid`, `heartbeat_ms`, mode).
- A second writer fails fast while `writer.lock` is held.
- Read-only stores (`access_mode=kReadOnly`) do not take writer ownership and can run concurrently with the writer.
- On writer restart/takeover, stale metadata is detected and moved to `writer.meta.stale.<timestamp>` before a new session is published.
- Writer metadata heartbeat is periodically refreshed while the writer process is alive.

Crash behavior:
- `kill -9`/crash releases the OS lock when the process exits.
- Next writer instance can take ownership and publish a new session id.
- Clean shutdown removes active `writer.meta`.

Override (`allow_multiple_processes`) bypasses this safety model and is unsafe unless external coordination is guaranteed.

## 5. Cache / Memory Control

- `max_loaded_chunks` limits in-memory chunk cache size.
- LRU-style eviction removes least-recently-used chunks that are not currently referenced.
- Before evicting a chunk, pending WAL batch bytes are flushed to disk.

This prevents unbounded growth in long-running sparse-world workloads while preserving chunk correctness across load/unload cycles.

## 6. Durability Modes

### `relaxed`
- WAL writes do not use `fsync`.
- WAL flush can be batched by `wal_group_commit_updates`.
- Lowest latency, weakest crash/power-loss guarantees.

### `fsync-wal`
- WAL is appended and `fsync`ed per acknowledged write.
- Acknowledged writes are durable in WAL after successful `fsync`.

### `fsync-checkpoint`
- `fsync-wal` semantics plus `fsync` for checkpointed `.chk` + directory updates.
- Strongest current mode.

## 7. Crash/Power-Loss Semantics

- Normal restart recovery:
  - WAL replay restores committed on-disk deltas.
- `relaxed` mode may lose more recent acknowledged writes due to absent `fsync` and optional group commit batching.
- Clean shutdown flushes pending WAL batches before process exit.
- Power-loss semantics still depend on mode and filesystem/device behavior.
- Engine does not provide full ACID transactional semantics across multiple chunks.

## 8. What Is Not Guaranteed Yet

- No cross-chunk atomic transactions.
- No replication.
- No consensus or distributed durability.
- No claim of full ACID database guarantees.
