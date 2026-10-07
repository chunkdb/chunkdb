# Runtime Flow

This document describes how `chunkdb` behaves at runtime for the commands
whose runtime path differs. Commands not listed here follow one of the paths
below; `docs/PROTOCOL.md` is the complete command reference.

Block and chunk commands run on the connection's selected table (`default`
until `USE`), under a lease that keeps the table from being dropped or
reopened while the command runs. Each table is its own store; the paths below
are per table.

## `GET x y`

1. Resolve block coordinate -> regular chunk coordinate -> large chunk coordinate.
2. Load regular chunk into memory if missing:
   - read chunk image (`.chk`) if present, otherwise start from zero state;
   - replay WAL (`.wal`) into in-memory chunk state if present;
   - do not checkpoint or remove WAL just because replay happened during load.
3. If the target block is unset, return null (`$-1`).
4. Otherwise read block bits from the in-memory packed payload and return them as bulk text.

`GET` does not append WAL and does not trigger checkpoint by itself.

## `SET x y bits`

1. Resolve block/chunk coordinates.
2. Ensure target regular chunk is loaded in memory.
3. Update only touched bytes in packed payload and mark the block as present.
4. Append the changed payload bytes and/or the presence bitmap byte as one
   WAL frame. One mutation is one frame, applied entirely or not at all on
   replay (see `DURABILITY_CONTRACT.md`).
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
4. Append the changed payload bytes and/or the presence bitmap byte as one WAL frame.
5. Follow the same WAL flush and checkpoint policy as `SET`.

## `MSET x1 y1 bits1 [...]` / `MGET x1 y1 [...]`

1. `MSET` validates every item first, then applies the items strictly in
   request order as independent `SET` operations: each item is its own WAL
   frame, so the command is not atomic across items.
2. `MGET` reads the requested blocks like repeated `GET`, without appending WAL.

## `CHUNKEXISTS cx cy`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return `1` if any block presence bit is set, otherwise `0`.

## `CHUNKPUT cx cy [STATE] [ZRLE] [IF version] length`

1. Parse the request line, then read exactly `<length>` raw bytes and the
   terminating empty line. The bytes are not subject to `max_line_bytes`; a
   declared length above the table's chunk state size (plus 16 bytes with
   `ZRLE`) is refused before anything is buffered.
2. With `ZRLE`, decode the bytes, bounded by the exact payload or state size.
3. Resolve chunk coordinate and ensure the target regular chunk is loaded in memory.
4. Without `IF`: replace the full in-memory chunk payload and, with `STATE`,
   the per-block presence bitmap (otherwise every block becomes present).
   Absent blocks are canonicalized so their payload bits are zero in memory
   and on disk. Append the changed payload span and/or presence span as one
   WAL frame, so a replace that spans several records is still
   all-or-nothing on replay. Follow the same WAL flush and checkpoint policy
   as `SET`.
5. With `IF`: follow the conditional path below (`CHUNKPUT ... IF` /
   `CHUNKBATCH`).
6. Reply with the chunk's version after the write.

## `CHUNKGET cx cy [STATE] [ZRLE]`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return the packed payload bytes, followed with `STATE` by the presence
   bitmap bytes; with `ZRLE`, zrle-encoded.
3. If the chunk is absent, the returned bytes are all zero; use `CHUNKEXISTS`
   to distinguish absence from an explicit all-zero chunk.

## `CHUNKVER cx cy`

1. Resolve chunk coordinate and ensure the target regular chunk is loaded.
2. Return the chunk's current revision. Loading a chunk does not reserve a new
   token: the revision comes from the `.chk` header and the last valid WAL
   frame (see "Eviction and Reload").

## `CHUNKPUT ... IF` / `CHUNKBATCH`

1. Validate every operation and, when a version was given, compare it with the
   chunk's current revision; a mismatch returns `VERSION_MISMATCH` with no
   mutation.
2. Reserve the next version token, publish a new odd snapshot generation, and
   persist a rollback intent holding the pre-command WAL byte boundary.
3. Apply the new state in memory and append the full canonical chunk state as
   one WAL frame (a payload span plus a presence span), then flush it under
   the same WAL policy as `SET` (synced in `fsync-wal`/`fsync-checkpoint`).
   Any pre-commit failure restores memory and truncates the WAL back to the
   recorded boundary.
4. Replace the rollback intent with a committed record, then clear it. The
   full sequence, including the crash cases, is in `DURABILITY_CONTRACT.md`.

## `CHUNKSCAN` / `CHUNKRANGE` / `CHUNKRADIUS`

1. Enumerate the candidate chunk coordinates: the first `CHUNKSCAN` builds
   an ordered in-memory catalog of `L_<lx>_<ly>` directories and resident
   large chunks. Subsequent pages seek into that catalog and list only the
   relevant directories. Loads register new large chunks before returning;
   eviction removes entries that have neither a resident container nor a
   directory. A read-only store refreshes after a snapshot-generation change
   (and on every scan while the generation is odd or absent). The catalog
   uses memory proportional to large chunks, without a new disk artifact.
   Bounded reads derive candidates directly from their rectangle or disc.
2. Read each populated candidate directly from `.chk` plus WAL replay without
   inserting it into the chunk cache, so a world sweep does not evict the
   working set. A chunk that is already loaded is read from memory.
   `CHUNKRANGE` and `CHUNKRADIUS` return each chunk's bytes as `CHUNKGET`
   with the same options does.
3. Only after several contended attempts on one chunk does the read fall back
   to the authoritative cache path, which does cache that chunk, to preserve
   read-your-writes consistency.

## `WALFLUSH`

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
3. In `fsync-wal` and `fsync-checkpoint`, flush temp file data and close with
   error checks. A relaxed store also does this after a successful `WALFLUSH`
   or after reopening, so replacement cannot downgrade already durable state.
4. Atomic replace of `.chk`.
5. Remove `.wal`.
6. In the same synced/floor-preserving cases, sync parent directory metadata.

## Eviction and Reload

- If loaded chunks of all tables together exceed `max_loaded_chunks`, eviction selects least-recently-used candidates that are not actively referenced, from any table: one access clock orders chunks across tables, and each step takes the coldest known candidate among them.
- Eviction uses hysteresis: once over limit, it evicts down to a lower watermark (`max_loaded_chunks - max(256, max_loaded_chunks/16)`, clamped to at least `1`).
- Before eviction, pending WAL batch for the candidate chunk is flushed. If that fails for a chunk of another table than the one loading, the chunk stays cached, the failure is logged, and that table is not chosen as a victim for a second.
- If a loaded chunk still has replayed-on-load WAL state, eviction only compacts it when the normal checkpoint policy says compaction is due.
- On later access, chunk is loaded again from `.chk` plus WAL replay (if WAL exists).
- The chunk's version (`CHUNKVER`) survives this. The revision is persisted in
  the `.chk` header and in every WAL frame, so a reloaded chunk reports the
  same token it had before eviction or before a restart, and a cold load no
  longer consumes the version clock. At load the store raises the clock past
  any persisted revision it reads, so tokens are never reused even if the
  clock bookkeeping was lost. A chunk with no artifact gets a fresh token when
  it is loaded.

## Runtime Counters (`INFO`)

`INFO` includes runtime counters of the selected table:

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

These counters are monotonic while the table is open in the process (except `loaded_chunks` and `open_wal_streams`, which are current in-memory counts); `TABLESET` reopens the table and starts them again. `METRICS` sums them over all tables.

## How This Differs From Redis-Like Expectations

- `chunkdb` is chunk/grid storage first, not a generic in-memory key-value cache.
- Reads/writes are chunk-coordinate aware and bit-packed.
- Data is persisted as chunk images + WAL files in a filesystem layout.
- Recovery behavior is tied to WAL/checkpoint mode, not to an append-only command log.
- Runtime memory is bounded by chunk cache limits and eviction policy, not by "all data in RAM" assumptions.
