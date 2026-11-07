# Typed columns and schema history
The [CQL reference](../CQL.md) defines supported types and conversions; the [storage reference](../STORAGE_FORMAT.md) specifies their encoding.

## State layout

Each column has a permanent id, name, type, null/required flags and optional default.
Fixed-width values are column-major packed arrays with optional validity bitmaps; presence is a separate bitmap.
Text and bytes values use ordered VARS entries keyed by column id and block index.
Absent blocks, NULL values and padding use canonical zero bits.
Tables retain at least one fixed-width column and bound total fixed payload and variable bytes.

## Positions instead of keys

A block has no key, pointer or map entry of its own: its place is computed from its coordinates, `local_y * chunk_width + local_x` within its chunk.
A present block costs its column bits, one presence bit and one validity bit per nullable column; an entry per block in a hash map (key, node and bucket) would take more memory than the values of typical small columns.
Reading or writing a block is arithmetic and a bit access, in constant time, without hashing, probing or allocation.
Neighbouring blocks are adjacent in each column's array, so area reads and scans walk contiguous memory, and reading one column touches only that column's bytes.
The cost is that a loaded chunk holds full arrays even when few of its blocks are present; a chunk with no present block is collected.
Maps exist only at chunk level: the chunks of a large chunk, the large chunks of a table and the cache of loaded chunks, each keyed by chunk coordinates.
On every operation, a point read or write of a loaded chunk does two hash lookups by chunk coordinates, each under its own mutex: its large chunk in the table's map, then the chunk in that large chunk's map; the third map is used only when a chunk is loaded or its object is freed.
A write that flushes its chunk's WAL batch, in relaxed mode once per `wal_group_commit_updates` writes to the chunk and in the synced modes on every write, does more lookups.
It finds the chunk's stream in the process's open WAL streams, keyed by the chunk object's address, and in relaxed mode it adds the WAL file's path, hashed as a string, to the store's set of unsynced files, first looking up the file's directory once that tracking has grown past its limit and tracks large-chunk directories instead of their files.
Opening a WAL stream, a checkpoint, an open transaction and a change feed's archive do further lookups, keyed by paths or chunk coordinates.

`chunkdb_chunk_map_bench` measures what the two per-operation lookups cost on a warm cache.
The published run uses a build with `-DCHUNKDB_BUILD_TESTS=OFF -DCHUNKDB_FAILPOINTS=OFF`, since compiled-in test failure injection reads the environment on every write; the output's `profile` line records `failpoints=off`.
Each run opens a fresh store in relaxed mode with 16,384 loaded chunks of 16x16 blocks of 15 bits, in 256 large chunks of 8x8 chunks, every block present and a cache of twice that.
At 15 bits the string a read returns fits the small-string buffer of libstdc++ and libc++, so no timed read allocates.
It times windows of 204,800 point reads (`GetBlockBits`) and point writes (`SetBlockBits`, each changing its block), once with each operation's chunk drawn uniformly from all loaded chunks and once with every operation in one chunk, both with the same sequence of block positions inside the chunk from a fixed seed.
It also times the two lookups alone, with their mutexes, on maps of the same types, hash, keys, sizes and insertion order outside the store, over the same chunk sequences.
Every window follows an untimed pass of the same sequence and is timed in segments of 1,024 operations; its figure is the median segment's ns per operation, which leaves out rare stalls such as the durable write of the version clock every 16,384 versions.
A timed write's WAL frame stays in its chunk's in-memory batch: group commit and checkpoint thresholds are above anything a run writes, and the untimed pass, drained by a WAL barrier, has grown each batch, so timed writes flush no batch, do no file I/O and no WAL allocation.
The benchmark fails a run in which a timed operation loaded, evicted, checkpointed or flushed a chunk, or a read returned another value than the last one written to its block.
Runs alternate which side goes first; the result is the median, minimum and maximum over 15 runs, and the same of the per-run difference between many chunks and one chunk.
In the store that difference also includes reaching the chunk's object and arrays outside the CPU caches, and for writes appending to 16,384 WAL batches instead of one, so the lookups timed alone give the share of the maps.
The numbers hold for `ChunkCoordHash` as of the measured commit.
Measured on TBD (machine, commit, 15 runs, artifact in `bench/artifacts/manual-runs/`): a read takes TBD ns across many chunks and TBD ns in one chunk, a write TBD ns and TBD ns, and the two lookups alone TBD ns and TBD ns.

## Schema evolution

The version-4 manifest stores current columns and the history needed to rebuild previous schemas.
Images and WAL frames identify their schema version; loading translates values by stable column ids and recorded conversions.
New columns receive their default, null, or zero/empty value as their flags require during translation; dropped columns disappear and renames preserve identity.
ADD of a required column needs a default for existing blocks.
Widening and explicit conversion policies publish one new schema version atomically.
Narrowing without conversion first publishes a temporary constraint, validates stored values and then commits or removes the constraint.
Writer restart removes an interrupted narrowing constraint without committing its proposed type change.

## Wire form

Chunk transfers carry revision, schema version, presence, fixed payload and variable entries.
SET CHUNK refuses a mismatched schema before changing data.
Typed SET BLOCK fills unspecified columns according to defaults/nullability and validates all supplied values before mutation.
Whole-chunk writes include their text/bytes values; they do not depend on an additional metadata API.
