# Runtime Flow

This document describes how `chunkdb` behaves at runtime for the statements whose runtime path differs. Statements not listed here follow one of the paths below; `docs/CQL.md` is the complete statement reference.

Block and chunk statements run on the table they name, under a lease that keeps the table from being dropped or reopened while the statement runs. Each table is its own store; the paths below are per table.

## `GET BLOCK x y FROM t`

1. Resolve block coordinate -> regular chunk coordinate -> large chunk coordinate.
2. Load regular chunk into memory if missing:
   - read chunk image (`.chk`) if present, otherwise start from zero state;
   - replay WAL (`.wal`) into in-memory chunk state if present;
   - do not checkpoint or remove WAL just because replay happened during load.
3. If the target block is absent, return null (`_`).
4. Otherwise read the block's values from the in-memory chunk and return them as an array, in schema order or in `COLUMNS` order.

`GET BLOCK` does not append WAL and does not trigger checkpoint by itself.

## `SET BLOCK x y IN t ... [IF VERSION n]`

1. Resolve block/chunk coordinates.
2. Ensure target regular chunk is loaded in memory.
3. With `IF VERSION`, compare `n` with the chunk's version under the chunk lock; a mismatch returns `VERSION_MISMATCH` with no mutation. Otherwise update only the touched bytes of the given columns in the packed payload (a new block takes `DEFAULT`, `NULL` or zero for the others, as `docs/CQL.md` describes) and mark the block as present.
4. Append the changed payload bytes and/or the presence bitmap byte (and a record per changed `text` or `bytes` value) as one WAL frame. One mutation is one frame, applied entirely or not at all on replay (see `DURABILITY_CONTRACT.md`).
5. WAL flush behavior depends on durability mode:
   - `relaxed`: flush may be batched by `wal_group_commit_updates`;
   - `fsync-wal`: WAL append + file sync before ack;
   - `fsync-checkpoint`: same WAL path as `fsync-wal`, plus stricter checkpoint sync behavior.
6. Checkpoint is triggered when either threshold is reached:
   - `checkpoint_update_interval`
   - `checkpoint_wal_bytes`
7. Reply with the chunk's version after the write.

## `DELETE BLOCK x y FROM t [IF VERSION n]`

1. Resolve block/chunk coordinates.
2. Ensure target regular chunk is loaded in memory.
3. With `IF VERSION`, compare as `SET BLOCK` does. Zero the block payload bits and clear the block presence bit; in a table with `text` or `bytes` columns, delete the block's values.
4. Append the changed payload bytes and/or the presence bitmap byte (and a `VAR_DEL` record per deleted value) as one WAL frame.
5. Follow the same WAL flush, checkpoint and reply as `SET BLOCK`.

## `SET CHUNK cx cy IN t $1 [IF VERSION n]`

1. Parse the request line, then read the chunk form as one parameter frame. The frame is not subject to `max_line_bytes`; a declared length above the table's largest chunk form is refused with `BAD_REQUEST` before anything is buffered, and the connection closes.
2. Decode the chunk form into payload, presence bitmap and `text` and `bytes` values; the version in the form is not read. A size that does not match the table, or a value that does not fit, is refused and nothing changes.
3. Resolve chunk coordinate and ensure the target regular chunk is loaded in memory.
4. Without `IF VERSION`: replace the full in-memory chunk payload, presence bitmap and `text` and `bytes` values. Absent blocks are canonicalized so their payload bits are zero in memory and on disk. Append the changed payload span and/or presence span as one WAL frame, so a replace that spans several records is still all-or-nothing on replay. Follow the same WAL flush and checkpoint policy as `SET BLOCK`.
5. With `IF VERSION`: follow the conditional path below (`SET CHUNK ... IF VERSION`).
6. Reply with the chunk's version after the write.

## `GET CHUNK cx cy FROM t [COLUMNS ...]`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return the chunk form: the chunk's current revision, the presence bitmap, the packed payload and the `text` and `bytes` values; with `COLUMNS`, only the named columns' parts. Loading a chunk does not reserve a new token: the revision comes from the `.chk` header and the last valid WAL frame (see "Eviction and Reload").
3. A chunk without blocks returns its empty form, with its version; the presence bitmap tells absent blocks from present ones.

## `SET CHUNK ... IF VERSION`

1. Validate the chunk form and compare the given version with the chunk's current revision; a mismatch returns `VERSION_MISMATCH` with no mutation.
2. Reserve the next version token, publish a new odd snapshot generation, and persist a rollback intent holding the pre-write WAL byte boundary.
3. Apply the new state in memory and append the full canonical chunk state as one WAL frame (a payload span plus a presence span, and the values of blocks it makes absent), then flush it under the same WAL policy as `SET BLOCK` (synced in `fsync-wal`/`fsync-checkpoint`). Any pre-commit failure restores memory and truncates the WAL back to the recorded boundary.
4. Replace the rollback intent with a committed record, then clear it. The
   full sequence, including the crash cases, is in `DURABILITY_CONTRACT.md`.

## `SCAN CHUNKS` / `GET AREA`

1. Enumerate the candidate chunk coordinates: the first `SCAN CHUNKS` builds an ordered in-memory catalog of `L_<lx>_<ly>` directories and resident large chunks. Subsequent pages seek into that catalog and list only the relevant directories. Loads register new large chunks before returning; eviction removes entries that have neither a resident container nor a directory. A read-only store refreshes after a snapshot-generation change (and on every scan while the generation is odd or absent). The catalog uses memory proportional to large chunks, without a new disk artifact. Bounded reads derive candidates directly from their rectangle or disc.
2. Read each populated candidate directly from `.chk` plus WAL replay without inserting it into the chunk cache, so a world sweep does not evict the working set. A chunk that is already loaded is read from memory. `GET AREA` returns each chunk's form as `GET CHUNK` with the same `COLUMNS` does.
3. Only after several contended attempts on one chunk does the read fall back
   to the authoritative cache path, which does cache that chunk, to preserve
   read-your-writes consistency.

## `FLUSH WAL`

Runs the steps below for every table, one after another.

1. Serialize against other barriers, then flush every loaded chunk's pending
   WAL batch with a file sync.
2. Holding the checkpoint-publication mutex, drain the bookkeeping of
   artifacts written without a sync and sync those files and directories, so a
   concurrent checkpoint cannot replace a tracked WAL with an unsynced image.
3. Any sync failure aborts the barrier and is returned to the caller; the
   drained bookkeeping is retained so a retry still covers it.

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

1. Serialize full chunk image from in-memory payload, presence bitmap, and the
   chunk's current revision.
2. Write temp file in the same directory.
3. In `fsync-wal` and `fsync-checkpoint`, flush temp file data and close with error checks. A relaxed store also does this after a successful `FLUSH WAL` or after reopening, so replacement cannot downgrade already durable state.
4. Atomic replace of `.chk`.
5. Remove `.wal`.
6. In the same synced/floor-preserving cases, sync parent directory metadata.

## Eviction and Reload

- If loaded chunks of all tables together exceed `max_loaded_chunks`, eviction selects least-recently-used candidates that are not actively referenced, from any table: one access clock orders chunks across tables, and each step takes the coldest known candidate among them.
- Eviction uses hysteresis: once over limit, it evicts down to a lower watermark (`max_loaded_chunks - max(256, max_loaded_chunks/16)`, clamped to at least `1`).
- Before eviction, pending WAL batch for the candidate chunk is flushed. If that fails for a chunk of another table than the one loading, the chunk stays cached, the failure is logged, and that table is not chosen as a victim for a second.
- If a loaded chunk still has replayed-on-load WAL state, eviction only compacts it when the normal checkpoint policy says compaction is due.
- On later access, chunk is loaded again from `.chk` plus WAL replay (if WAL exists).
- The chunk's version (in its chunk form) survives this. The revision is persisted in the `.chk` header and in every WAL frame, so a reloaded chunk reports the same token it had before eviction or before a restart, and a cold load does not consume the version clock. At load the store raises the clock past any persisted revision it reads, so tokens are never reused even if the clock bookkeeping was lost. A chunk with no artifact gets a fresh token when it is loaded.

## Runtime Counters (`SHOW METRICS`)

`SHOW METRICS` includes these runtime counters, summed over all tables:

- `chunkdb_loaded_chunks`
- `chunkdb_evictions_total`
- `chunkdb_checkpoints_total`
- `chunkdb_wal_batch_flushes_total`
- `chunkdb_unique_loaded_chunks_total`
- `chunkdb_open_wal_streams`
- `chunkdb_eviction_snapshot_builds_total`, `chunkdb_eviction_probes_total`, `chunkdb_eviction_no_progress_cycles_total`, `chunkdb_eviction_forced_wal_flushes_with_data_total`, `chunkdb_eviction_forced_wal_flushes_empty_batch_total`

A table's counters are monotonic while it is open in the process (except `chunkdb_loaded_chunks` and `chunkdb_open_wal_streams`, which are current in-memory counts); `ALTER TABLE` reopens the table and starts its counters again.

## How This Differs From Redis-Like Expectations

- `chunkdb` is chunk/grid storage first, not a generic in-memory key-value cache.
- Reads/writes are chunk-coordinate aware and bit-packed.
- Data is persisted as chunk images + WAL files in a filesystem layout.
- Recovery behavior is tied to WAL/checkpoint mode, not to an append-only command log.
- Runtime memory is bounded by chunk cache limits and eviction policy, not by "all data in RAM" assumptions.
