# Runtime Flow (Alpha)

This document describes how `chunkdb` behaves at runtime for core commands.

## `GET x y`

1. Resolve block coordinate -> regular chunk coordinate -> large chunk coordinate.
2. Load regular chunk into memory if missing:
   - remove stale orphan temp artifacts for that chunk target;
   - read chunk image (`.chk`) if present;
   - replay WAL (`.wal`) if present;
   - write checkpoint image and remove WAL after successful replay.
3. Read block bits from in-memory packed payload and return as bulk text.

`GET` does not append WAL and does not trigger checkpoint by itself.

## `SET x y bits`

1. Resolve block/chunk coordinates.
2. Ensure target regular chunk is loaded in memory.
3. Update only touched bytes in packed payload.
4. Append WAL delta record for changed byte range.
5. WAL flush behavior depends on durability mode:
   - `relaxed`: flush may be batched by `wal_group_commit_updates`;
   - `fsync-wal`: WAL append + file sync before ack;
   - `fsync-checkpoint`: same WAL path as `fsync-wal`, plus stricter checkpoint sync behavior.
6. Checkpoint is triggered when either threshold is reached:
   - `checkpoint_update_interval`
   - `checkpoint_wal_bytes`

## Memory vs Disk

- In-memory state:
  - loaded regular chunk payloads
  - pending WAL batch data per chunk
- On-disk state:
  - chunk image (`.chk`)
  - WAL delta log (`.wal`)
  - process lock metadata (`.chunkdb.lock`)

## Checkpoint Path

When checkpointing a regular chunk:

1. Serialize full chunk image from in-memory payload.
2. Write temp file in the same directory.
3. In `fsync-checkpoint`, flush temp file data and close with error checks.
4. Atomic replace of `.chk`.
5. Remove `.wal`.
6. In `fsync-checkpoint`, sync parent directory metadata.

## Eviction and Reload

- If loaded chunks exceed `max_loaded_chunks`, eviction selects least-recently-used candidates that are not actively referenced.
- Before eviction, pending WAL batch for the candidate chunk is flushed.
- On later access, chunk is loaded again from `.chk` plus WAL replay (if WAL exists).

## Runtime Counters (`INFO`)

`INFO` includes runtime counters:

- `loaded_chunks`
- `evictions`
- `checkpoints`
- `wal_batch_flushes`
- `unique_loaded_chunks`

These counters are monotonic for the process lifetime (except `loaded_chunks`, which is current in-memory count).

## How This Differs From Redis-Like Expectations

- `chunkdb` is chunk/grid storage first, not a generic in-memory key-value cache.
- Reads/writes are chunk-coordinate aware and bit-packed.
- Data is persisted as chunk images + WAL files in a filesystem layout.
- Recovery behavior is tied to WAL/checkpoint mode, not to an append-only command log.
- Runtime memory is bounded by chunk cache limits and eviction policy, not by "all data in RAM" assumptions.
