# Typed Columns (#61): Design

Status: accepted (the owner delegated the open points on 2026-10-08). The product decisions are in the 2.0 plan (#69): typed fixed-width and variable-length columns, empty values, columnar chunks, a versioned schema with instant `ADD`/`DROP`/`RENAME COLUMN`. This document is how the core does it.

## Idea

A chunk keeps its byte-level shape: a `PAYLOAD` of packed bits and a `PRESENCE` bitmap, written by `SPAN` WAL records, published as image sections. Only the order of `PAYLOAD` changes: it holds one array per column instead of one bit string per block. WAL frames, conditional writes, images, recovery, read-only snapshots and `chunkdb_verify` keep working on bytes; what is new is the schema that says where each column lives, and the translation between schema versions.

## Schema

- A column has a permanent id (never reused), a name (`[a-z_][a-z0-9_]*`, at most 63 bytes), a type, and the flags `NULL` (may be null), `REQUIRED` (a new block must give it) and a `DEFAULT`.
- Fixed-width types: `uN` (1–64), `iN` (2–64, two's complement), `bool` (1 bit), `f32`, `f64` (IEEE 754), and `bits(N)` (1–65535), an opaque bit string. Variable-length types: `text(max)` (UTF-8, checked on write) and `bytes(max)`, `max` up to 16 MiB.
- `bits(N)` is new compared with the plan: it keeps raw-bit tables expressible (a table created with `block_bits` is a table with one column `bits bits(block_bits)`, so today's tables, tests and benchmarks keep their exact layout) and serves bit masks and hashes.
- A table's schema has a version (`u64`, starting at 1). Every committed change creates a new version; all versions are kept, with, per column, the conversion from the previous version when its type changed.
- The table manifest holds the schema in an area after its options (manifest version 3, at most 1 MiB), so the schema and the options change in one atomic, synced write and can never disagree after a crash. The same area holds the history that rebuilds earlier schema versions. The manifest stops recording `block_bits`: the geometry is the chunk and large-chunk sizes; a block's width comes from the schema. 2.0 development tables are refused, as before.

## Layout of one schema version

- `PAYLOAD`: for each fixed-width column in column order, its values (`block_count × width` bits, least significant bit first, as today) padded to a byte boundary, then, for a `NULL` column, its validity array (`block_count` bits, 1 = has a value) padded to a byte. A column's values are one contiguous byte range, so reading one column of a chunk is one slice.
- `PRESENCE`: unchanged, one bit per block (the block exists).
- Variable-length values: section `VARS` (replaces `EXTRA`): entries `column_id` (`u32`), `block_index` (`u32`), `byte_length` (`u32`), bytes, in ascending (column id, block index); only blocks with a value have an entry, and in a column that cannot be `NULL` the empty value has none; a write may not take a chunk's values past `var_max_chunk_bytes` (replaces `extra_max_chunk_bytes`), and a chunk already over a lowered limit may still shrink. A table keeps at least one fixed-width column. WAL records `VAR_PUT`, `VAR_DEL`, `VAR_REPLACE` replace `EXTRA_*` with a column id.
- An absent block is zero in every array and has no `VARS` entry (canonical, as today). A typed block write creates a block with its `DEFAULT`s written explicitly; a whole-chunk write sets raw fixed-width bytes, and the blocks it creates have no `text` or `bytes` values until #62 gives chunk writes a form that carries them. Defaults are applied lazily only when a column is added later.
- Limits: at most 1024 columns, at most 65535 fixed bits per block, the payload of a chunk at most 64 MiB (as today), at most 65535 schema versions per table.

## Versions in files

- An image records the schema version its sections use (section `SCHEMA`), and a WAL frame the version it was written with (TLV `SCHEMA`, 8 bytes); both only once the table is past version 1, so tables that never change keep their bytes.
- Loading translates to the current version in memory: the image, then each frame in its own version, translating the state when the version changes. The files keep their version until the chunk is next checkpointed, which happens on its next write anyway. Chunks of the current version are not translated, so the hot path does not change.
- Translation from version `a` to `b`: a column of `b` whose id exists in `a` takes its values, converted through every type change between them; a column new since `a` takes its `DEFAULT` (or null); a dropped column is skipped. Conversions are recorded per version, so a translation always gives the same result, whenever it runs.

## Changing the schema

- `ADD`, `DROP`, `RENAME COLUMN`, widening a type (`u8` → `u16`, `u8` → `i16`, `text(64)` → `text(256)`), and any type change with `USING CLAMP | DEFAULT | TRUNCATE` write one new schema version: instant, atomic, nothing else touched.
- Narrowing without `USING` must check every value first. Phase 1 records a pending constraint in the table manifest: from then on every write is checked against the narrower type. Phase 2 reads every chunk of the table. Phase 3 commits the new version, or removes the constraint and reports the first value that does not fit (block coordinates, value). A crash before phase 3 leaves the schema unchanged.
- Changes between type families (an integer to `f32`, `text` to `bytes`) are not `ALTER` in 2.0: add a column, copy, drop.
- `REQUIRED` without `DEFAULT` cannot be added to a table (its existing blocks would lack it). Each change is one table manifest write, synced before it is acknowledged.

## Interfaces

- `ChunkStore` gets typed block access (`SetBlock` and `GetBlock` with column values; `UnsetBlock` deletes a block with all its values) and the schema operations; `TableCatalog` creates tables with columns and routes `ALTER`. Column-sliced chunk reads and writes come with the chunk commands of #62, where their wire form is designed.
- The bit-string interface (`SetBlockBits`, `GetBlockBits`, chunk bit strings, `CHUNKBATCH`) works only on a table with one `bits(N)` column and refuses others.
- `text` and `bytes` columns replace per-block extra data (`XGET`, `XPUT`, `XDEL`, `EXTRA`), which never shipped.
- Schema changes: `TableCatalog::ChangeColumns` (`ADD`/`DROP`/`RENAME COLUMN`, `ChangeColumnType` with `kExact`, `kClamp`, `kDefault`, `kTruncate`) and `TableCatalog::NarrowColumn` (the check reads every populated chunk and holds the catalog's table operations while it runs); crash tests cover every phase.

## Measurements

A one-column table has the same bytes as before typed columns, so `scripts/bench/compare_budgets.py` holds it within 5% on `world`, `canvas` and `simulation`. `chunkdb_bench` has typed scenarios on a four-column table (mixed widths, one `NULL` column).
