# Runtime Flow

This document describes how `chunkdb` behaves at runtime for core commands.

## `GET x y`

1. Resolve block coordinate -> regular chunk coordinate -> large chunk coordinate.
2. Load regular chunk into memory if missing:
   - read chunk image (`.chk`) if present, otherwise start from zero state;
   - replay WAL (`.wal`) into in-memory chunk state if present;
   - do not checkpoint or remove WAL just because replay happened during load.
3. If the target block is unset, return zero bits.
4. Otherwise read block bits from the in-memory packed payload and return them as bulk text.

`GET` does not append WAL and does not trigger checkpoint by itself.

## `EXISTS x y`

1. Resolve block coordinate and ensure the target regular chunk is loaded.
2. Return `1` if the block has explicit presence, otherwise `0`.

## `SET x y bits`

1. Resolve block/chunk coordinates.
2. Ensure target regular chunk is loaded in memory.
3. Update only touched bytes in packed payload and mark the block as present.
4. Append WAL delta record(s) for changed payload bytes and/or the presence bitmap byte.
5. WAL flush behavior depends on durability mode:
   - `relaxed`: flush may be batched by `wal_group_commit_updates`;
   - `fsync-wal`: WAL append + file sync before ack;
   - `fsync-checkpoint`: same WAL path as `fsync-wal`, plus stricter checkpoint sync behavior.
6. Checkpoint is triggered when either threshold is reached:
   - `checkpoint_update_interval`
   - `checkpoint_wal_bytes`

## `UNSET x y`

1. Resolve block/chunk coordinates.
2. Ensure target regular chunk is loaded in memory.
3. Zero the block payload bits and clear the block presence bit.
4. Append WAL delta record(s) for changed payload bytes and/or the presence bitmap byte.
5. Follow the same WAL flush and checkpoint policy as `SET`.

## `CHUNKEXISTS cx cy`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return `1` if any block presence bit is set, otherwise `0`.

## `CHUNKSET cx cy bits`

1. Resolve chunk coordinate.
2. Ensure target regular chunk is loaded in memory.
3. Replace the full in-memory chunk payload and mark the whole chunk explicitly present.
4. Append WAL delta record(s) for changed payload bytes and/or the full presence bitmap.
5. Follow the same WAL flush and checkpoint policy as `SET`.

## `CHUNKSET cx cy STATE payload|presence`

1. Resolve chunk coordinate.
2. Ensure target regular chunk is loaded in memory.
3. Replace the full in-memory chunk payload and per-block presence bitmap.
4. Canonicalize absent blocks so their payload bits are zero in memory and on disk.
5. Append WAL delta record(s) for changed payload bytes and/or changed presence bytes.
6. Follow the same WAL flush and checkpoint policy as `SET`.

## `CHUNK cx cy`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return the full in-memory chunk payload as bit text.
3. If the chunk is absent, the returned payload is still all-zero bits; use `CHUNKEXISTS` to distinguish absence from an explicit all-zero chunk.

## `CHUNK cx cy STATE`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return exact chunk state as:
   - packed payload bits as text
   - `|`
   - per-block presence bits as text
3. Unset blocks remain zero-filled in the payload text, but their absence is visible in the trailing presence bitmap.

## `CHUNKBIN cx cy STATE`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return exact chunk state as:
   - legacy payload bytes
   - followed by presence bitmap bytes
3. This is the preferred machine-facing format for exact chunk-state transfer.

## Memory vs Disk

- In-memory state:
  - loaded regular chunk payloads
  - loaded regular chunk presence bitmaps
  - pending WAL batch data per chunk
- On-disk state:
  - chunk image (`.chk`)
  - WAL delta log (`.wal`)
  - checked version clock (`chunkdb.version`) and initialized-store marker
  - process lock metadata (`.chunkdb.lock`)

## Checkpoint Path

When checkpointing a regular chunk:

1. Serialize full chunk image from in-memory payload plus presence bitmap.
2. Write temp file in the same directory.
3. In `fsync-wal` and `fsync-checkpoint`, flush temp file data and close with
   error checks. A relaxed store also does this after a successful `WALFLUSH`
   or after reopening, so replacement cannot downgrade already durable state.
4. Atomic replace of `.chk`.
5. Remove `.wal`.
6. In the same synced/floor-preserving cases, sync parent directory metadata.

## Eviction and Reload

- If loaded chunks exceed `max_loaded_chunks`, eviction selects least-recently-used candidates that are not actively referenced.
- Eviction uses hysteresis: once over limit, it evicts down to a lower watermark (`max_loaded_chunks - max(256, max_loaded_chunks/16)`, clamped to at least `1`).
- Before eviction, pending WAL batch for the candidate chunk is flushed.
- If a loaded chunk still has replayed-on-load WAL state, eviction only compacts it when the normal checkpoint policy says compaction is due.
- On later access, chunk is loaded again from `.chk` plus WAL replay (if WAL exists).

## Runtime Counters (`INFO`)

`INFO` includes runtime counters:

- `loaded_chunks`
- `evictions`
- `checkpoints`
- `wal_batch_flushes`
- `unique_loaded_chunks`
- `open_wal_streams`
- `eviction_snapshot_builds`
- `eviction_probes`
- `eviction_no_progress_cycles`
- `eviction_forced_wal_flushes`
- `eviction_forced_wal_flushes_with_data`
- `eviction_forced_wal_flushes_empty_batch`

These counters are monotonic for the process lifetime (except `loaded_chunks` and `open_wal_streams`, which are current in-memory counts).

## How This Differs From Redis-Like Expectations

- `chunkdb` is chunk/grid storage first, not a generic in-memory key-value cache.
- Reads/writes are chunk-coordinate aware and bit-packed.
- Data is persisted as chunk images + WAL files in a filesystem layout.
- Recovery behavior is tied to WAL/checkpoint mode, not to an append-only command log.
- Runtime memory is bounded by chunk cache limits and eviction policy, not by "all data in RAM" assumptions.
