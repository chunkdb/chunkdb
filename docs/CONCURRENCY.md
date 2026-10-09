# Concurrency, Runtime, and Integrity Guarantees

## 1. In-Memory Locking Model

- Global large-chunk registry: `std::mutex`
- Per-large-chunk regular-chunk map: `std::mutex`
- Per-regular-chunk payload: `std::shared_mutex`
  - shared for `GET BLOCK`/`GET CHUNK`
  - unique for `SET BLOCK`/`DELETE BLOCK`/`SET CHUNK`
  - a chunk's `text` and `bytes` values are guarded by the same lock as its payload

Effects:
- concurrent reads on same chunk: allowed
- write vs read on same chunk: serialized
- operations on different chunks: can run concurrently

## 2. Lock Order

To avoid deadlocks:
1. global large-chunk mutex
2. large-chunk mutex
3. regular-chunk payload mutex
4. checkpoint-publication mutex
5. transaction history mutex (innermost)

A plain operation holds one regular-chunk payload lock at a time. A transaction commit (docs/TRANSACTIONS_DESIGN.md) loads and pins every chunk it read or wrote first, then holds their payload locks together, taken in coordinate order (x, then y): written chunks exclusively, chunks only read shared. Since every other holder of several payload locks is a commit taking them in the same order, it cannot deadlock with plain operations or with other commits. While a thread holds a commit's locks, the WAL stream pool skips those chunks when it closes idle streams, as it skips the chunk it opens a stream for.

A table's transaction history (open snapshots and the chunk states they still need) has its own mutex. Writes take it under their payload lock; nothing loads a chunk or takes another lock while holding it. Snapshot-generation accounting takes a separate mutex only while publishing an odd/even edge or updating the active-transition count; it is not held during artifact I/O. Nested and overlapping transitions share the odd epoch, and only the last finisher publishes even. If a top-level transition fails, the epoch stays odd unless its owning outer transaction repairs the state. `FLUSH WAL` drains chunks before taking the checkpoint-publication mutex and never reverses this order.

The lazy scan catalog shares the global registry mutex. Its
initial directory listing and resident-registry merge publish one complete
catalog before releasing that lock. Subsequent visits copy only the selected
path and resident handle under the lock, then release it before merging that
large chunk or reading its directory. Loading registers new containers before
returning them; eviction prunes a catalog entry only after retiring its empty
container and confirming that its directory is absent.

## 3. Server Runtime Concurrency

- Accept loop enqueues accepted sockets.
- Fixed worker pool processes connections.
- Connection parsing is buffered (not byte-by-byte recv loops).

This replaces detached thread-per-connection behavior and provides bounded thread growth.

## 4. Inter-Process Safety (SWMR)

Default model: **Single-Writer / Multi-Reader** per `data_dir`. The writer
owns the data directory and every table in it; each table keeps its own
snapshot generation, which read-only processes follow per table.

- Writer ownership is coordinated under `data_dir/.chunkdb.lock/`:
  - `writer.lock`: OS file lock for active writer exclusivity.
  - `writer.meta`: metadata heartbeat (`session_id`, `pid`, `heartbeat_ms`, mode).
- A second writer fails fast while `writer.lock` is held.
- Read-only stores (`access_mode=kReadOnly`) do not take writer ownership and can run concurrently with the writer.
- Inside the writer, every statement on a table runs under a lease of that table. `DROP TABLE` and `ALTER TABLE` block new leases on the table, wait for running ones, then drop or reopen it; statements on other tables continue. A statement on a table that was dropped gets `NO_TABLE`.
- Tables share one chunk cache and one WAL-stream pool. A load in one table
  can evict a cold chunk of another; a failure to flush that chunk is logged
  and the other table is not chosen as a victim for a second, so one
  fail-closed table does not fail loads in the others.
- A new store creates its manifest under writer ownership, and publishes it
  only if no manifest exists yet; without the writer lock
  (`allow_multiple_processes`) the first process to publish wins and the others
  open its manifest or refuse a different geometry.
- Every writer transition affecting an image, WAL, conditional intent,
  checkpoint, or empty-GC state is bracketed by a durable monotonic generation
  in `chunkdb.snapshot`: odd while changing, a new even value when coherent.
  Startup recovery uses a fresh odd value, including after a crash left an odd
  value; generations never roll back or repeat.
- One odd epoch may bracket several consecutive transitions. Concurrent writers join an already-open epoch, and a single writer's even publication lingers briefly so a following transition can re-enter the same epoch (bounded by a 50 ms window and 512 transitions). `FLUSH WAL` and store close publish the deferred even record. See `docs/DURABILITY_CONTRACT.md`.
- On each first chunk load, and for each uncached chunk an area read (`GET AREA`, `SCAN CHUNKS`) visits, a read-only store brackets its image, WAL, and adjacent conditional-intent collection with generation reads. It accepts only the same validated even generation. `CKRB` limits replay to its recorded prior-WAL boundary; `CKRC` preserves the committed WAL. Byte equality is not a consistency invariant.
- Collection is bounded: eight sleep-free attempts, then exponential backoff
  up to a 250 ms total sleep budget (which exceeds the writer's linger window,
  so a coalesced epoch delays a reader instead of failing it). Once the budget
  is spent, an active or crashed writer that leaves the generation odd,
  generation movement, malformed generation or intent metadata, a missing/short
  required WAL, or replay-prefix corruption returns an error for that chunk.
  An artifact that exists but cannot be opened at that instant counts as
  instability, not damage, and is retried within the same budget: on Windows
  the target of an atomic replace is briefly unopenable even though nothing is
  wrong with it. A file that stays unreadable for the whole budget still fails
  closed, and the error names the last read failure. Read-only collection never truncates,
  removes, cleans, checkpoints, syncs, or writes generation metadata.
- On writer restart/takeover, stale metadata is detected and moved to `writer.meta.stale.<timestamp>` before a new session is published.
- Writer metadata heartbeat is periodically refreshed while the writer process is alive.

Crash behavior:
- `kill -9`/crash releases the OS lock when the process exits.
- A crash during a storage transition leaves an odd snapshot generation.
  Readers fail closed until the next writer takes ownership, publishes a fresh
  odd recovery generation, repairs conditional state, and publishes even.
- Clean shutdown removes active `writer.meta`.

Override (`allow_multiple_processes`) bypasses this safety model and is unsafe unless external coordination is guaranteed.

## 5. Cache / Memory Control

- `max_loaded_chunks` limits in-memory chunk cache size.
- Eviction is recency-aware with a second chance: candidates are ordered by
  their recorded last-access tick (least recently used first), and a
  candidate that was accessed after its tick was recorded is skipped in the
  first pass instead of evicted. If a full recency-respecting pass makes no
  progress while evictable chunks exist, a second pass evicts in recorded
  recency order regardless of later touches so the cache bound and its
  hysteresis are still enforced. Only chunks that are not currently
  referenced are ever evicted.
- Before evicting a chunk, pending WAL batch bytes are flushed to disk (with
  a sync in synced durability modes), and a due checkpoint is compacted.
- With `--background-maintenance`, eviction passes run on the maintenance
  thread; if the cache exceeds the bound by more than one hysteresis band
  while that thread catches up, the loading thread evicts inline.

This prevents unbounded growth in long-running sparse-world workloads while preserving chunk correctness across load/unload cycles.

## 6. Durability Modes

### `relaxed`
- WAL writes do not use `fsync`.
- WAL flush can be batched by `wal_group_commit_updates`.
- Snapshot-generation odd/even metadata is still synced for cross-process
  coherence; this does not make the WAL payload durable.
- Lowest latency, weakest crash/power-loss guarantees.
- Checkpoint image replace is atomic in namespace, but no required temp-file/data or directory sync.
- The `FLUSH WAL` statement provides an explicit durability barrier in this mode (see DURABILITY_CONTRACT.md).

### `fsync-wal`
- WAL is appended and `fsync`ed per acknowledged write.
- On first WAL file creation in this mode, parent directory metadata is also synced.
- Acknowledged writes are durable in WAL after successful `fsync`.
- Checkpoint image replace uses the strict path here too: a checkpoint removes
  the WAL it replaces, so the replacement image and its directory entry are
  synced before that WAL is deleted. Otherwise removing a durable WAL in favor
  of an unsynced image would silently downgrade this mode's acknowledgement
  (see `DURABILITY_CONTRACT.md`).

### `fsync-checkpoint`
- `fsync-wal` semantics plus `fsync` for checkpointed `.chk` + directory updates.
- Strongest current mode.
- Checkpoint sequence:
  1. write temp image in same directory
  2. flush temp file data (`fdatasync`/`fsync`, and `F_FULLFSYNC` attempt on macOS)
  3. close temp file with error check
  4. atomic replace
  5. sync parent directory metadata (best-effort fallback on Windows if directory-handle flush is unsupported by the runtime/filesystem)

## 7. Crash/Power-Loss Semantics

- Normal restart recovery:
  - WAL replay restores committed on-disk deltas.
- Atomic replace is about namespace visibility (old-or-new target path state), not equivalent to guaranteed post-power-loss durability.
- `relaxed` mode may lose more recent acknowledged writes due to absent `fsync` and optional group commit batching.
- Clean shutdown flushes pending WAL batches and syncs what was written without a sync, as `FLUSH WAL` does, before process exit.
- Power-loss semantics still depend on mode and filesystem/device behavior.
- Plain operations are atomic per chunk; a transaction commit is atomic across its chunks, also across a crash (docs/DURABILITY_CONTRACT.md).

Covered crash points in current validation:
- crash/fault after temp-file flush and before replace: old target remains readable; stale temp artifact is cleaned on later load.
- torn/truncated WAL tails: replay stops safely at the invalid tail and preserves earlier valid deltas.
- interrupted writer process (`kill -9`) in durability kill-recovery tests: restart remains writable and recovers valid state.

Not yet fully proven:
- arbitrary kernel/storage reorder faults beyond the tested crash points
- silent hardware corruption outside CRC-covered payload/record checks
- exhaustive fault matrices across all filesystems/devices and mount options

## 8. What Is Not Guaranteed Yet

- No replication.
- No consensus or distributed durability.
- No claim of full ACID database guarantees.
