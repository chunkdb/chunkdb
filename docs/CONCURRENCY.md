# Concurrency, Runtime, and Integrity Guarantees

## 1. In-Memory Locking Model

- Global large-chunk registry: `std::mutex`
- Per-large-chunk regular-chunk map: `std::mutex`
- Per-regular-chunk payload: `std::shared_mutex`
  - shared for `GET`/`EXISTS`/`CHUNKEXISTS`/`CHUNK`/`CHUNKBIN`
  - unique for `SET`/`UNSET`/`CHUNKSET`

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
5. experimental region I/O mutex

The engine never acquires two regular-chunk payload locks in one operation.
Snapshot-generation accounting takes a separate mutex only while publishing
an odd/even edge or updating the active-transition count; it is not held
during artifact I/O. Nested and overlapping transitions share the odd epoch,
and only the last finisher publishes even. If a top-level transition fails,
the epoch stays odd unless its owning outer transaction repairs the state.
`WALFLUSH` drains chunks before
taking the checkpoint-publication mutex and never reverses this order.

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
- Every writer transition affecting an image/region, WAL, conditional intent,
  checkpoint, or empty-GC state is bracketed by a durable monotonic generation
  in `chunkdb.snapshot`: odd while changing, a new even value when coherent.
  Startup recovery uses a fresh odd value, including after a crash left an odd
  value; generations never roll back or repeat.
- On each first chunk load, a read-only store brackets its image (or region
  image), WAL, and adjacent conditional-intent collection with generation
  reads. It accepts only the same validated even generation. `CKRB` limits
  replay to its recorded prior-WAL boundary; `CKRC` preserves the committed
  WAL. Byte equality is not a consistency invariant.
- Collection is bounded to eight attempts. An active or crashed writer that
  leaves the generation odd, generation movement, malformed generation or
  intent metadata, a missing/short required WAL, or replay-prefix corruption
  returns an error for that chunk. Read-only collection never truncates,
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
- The `WALFLUSH` protocol command provides an explicit durability barrier in this mode (see DURABILITY_CONTRACT.md).

### `fsync-wal`
- WAL is appended and `fsync`ed per acknowledged write.
- On first WAL file creation in this mode, parent directory metadata is also synced.
- Acknowledged writes are durable in WAL after successful `fsync`.
- Checkpoint image replace remains atomic in namespace, but checkpoint file/directory sync is not required by this mode.

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
- Clean shutdown flushes pending WAL batches before process exit.
- Power-loss semantics still depend on mode and filesystem/device behavior.
- Engine does not provide full ACID transactional semantics across multiple chunks.

Covered crash points in current validation:
- crash/fault after temp-file flush and before replace: old target remains readable; stale temp artifact is cleaned on later load.
- torn/truncated WAL tails: replay stops safely at the invalid tail and preserves earlier valid deltas.
- interrupted writer process (`kill -9`) in durability kill-recovery tests: restart remains writable and recovers valid state.

Not yet fully proven:
- arbitrary kernel/storage reorder faults beyond the tested crash points
- silent hardware corruption outside CRC-covered payload/record checks
- exhaustive fault matrices across all filesystems/devices and mount options

## 8. What Is Not Guaranteed Yet

- No cross-chunk atomic transactions.
- No replication.
- No consensus or distributed durability.
- No claim of full ACID database guarantees.
