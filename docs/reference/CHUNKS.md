# GET CHUNK, SET CHUNK, GET AREA, SCAN CHUNKS, COMPACT TOMBSTONES, SHOW TABLE STATS

You read or replace whole chunks and find populated chunks in an area.
A chunk groups the fixed rectangle or cuboid of blocks specified by its table.

## Examples

These examples create `chunk_demo`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE chunk_demo (kind u8) CHUNK 2 x 2
SET BLOCK 0 0 IN chunk_demo kind = 1
--blocks GET CHUNK 0 0 FROM chunk_demo
--blocks GET AREA 0 0 TO 0 0 FROM chunk_demo
GET AREA AROUND 0 0 RADIUS 0 FROM chunk_demo
SCAN CHUNKS FROM chunk_demo LIMIT 1
```

```output
OK
1
version = 1
schema_version = 1
commit_time_ms = <time-ms>
present = 1 of 4 blocks
block 0 0
  kind = 1
chunk 0 0
  version = 1
  schema_version = 1
  commit_time_ms = <time-ms>
  present = 1 of 4 blocks
  block 0 0
    kind = 1
chunk 0 0
  version = 1
  schema_version = 1
  commit_time_ms = <time-ms>
  present = 1 of 4 blocks
chunks:
  1) [0, 0, 1]
more = false
epoch = <epoch>
position = 1
retained_deletion_floor = 0
```

To replace a chunk through the CLI, save its complete binary form, then supply it as input:

```sh docs-check-shell
chunk-cli --uri "$CHUNKDB_URI" --out "$EXAMPLE_DIR/chunk.bin" "GET CHUNK 0 0 FROM chunk_demo"
chunk-cli --uri "$CHUNKDB_URI" --in "$EXAMPLE_DIR/chunk.bin" 'SET CHUNK 1 0 IN chunk_demo $1'
```

```output
wrote 105 bytes to <example-dir>/chunk.bin
2
```

`EXAMPLE_DIR` is a writable directory you choose.
Client APIs can construct a chunk form directly.

A three-dimensional table uses three coordinates for both blocks and chunks:

```cql docs-check session=main
CREATE TABLE volume_demo (kind u8) CHUNK 2 x 2 x 2 CHUNK COLUMNS (owner u8 DEFAULT 0)
SET CHUNK 0 0 0 IN volume_demo META owner = 7
SET BLOCK 1 1 1 IN volume_demo kind = 3
GET BLOCK 1 1 1 FROM volume_demo
```

```output
OK
1
2
kind = 3
```

The metadata write populates the chunk before any block exists.

## Syntax

```text
GET CHUNK cx cy [cz] FROM table [COLUMNS column, ...]
SET CHUNK cx cy [cz] IN table [COLUMNS column, ...] $1 [IF VERSION version | IF ABSENT]
SET CHUNK cx cy [cz] IN table META column = value [, ...] [IF VERSION version | IF ABSENT]
GET AREA cx0 cy0 [cz0] TO cx1 cy1 [cz1] FROM table [COLUMNS column, ...] [WITH SCHEMA]
GET AREA AROUND cx cy [cz] RADIUS radius FROM table [COLUMNS column, ...] [WITH SCHEMA]
SCAN CHUNKS FROM table [AFTER cx cy [cz]] [LIMIT count] [SINCE revision] [WITH DATA] [COLUMNS column, ...]
COMPACT TOMBSTONES table BEFORE revision
SHOW TABLE STATS table
```

## Arguments

- `cx`, `cy`, `cz`: signed chunk coordinates, from -9223372036854775808 to 9223372036854775807, rather than block coordinates.
- Supply exactly the table's number of axes, including when `cz` is zero.
- `TO`: inclusive opposite corner; endpoints must be ordered on every axis.
- `RADIUS`: nonnegative distance in chunks; the area contains coordinates whose sum of squared active-axis offsets is at most radius².
- `COLUMNS`: a block-column projection in requested order; chunk metadata remains complete.
- `WITH SCHEMA`: return `[schema_version, entries]`, including the schema version when no chunks match.
- `$1`: a complete chunk form supplied through your client; see [Protocol](../PROTOCOL.md#chunk-form).
- `IF VERSION`: an equality check against the destination chunk's current version.
- `IF ABSENT`: require an unpopulated destination, including a retained empty chunk; conditions are unavailable inside transactions.
- `META`: update named chunk columns atomically, preserving blocks and unnamed metadata.
- `AFTER`: exclude this position and all earlier positions in scan order.
- `LIMIT`: maximum positions per scan page; default and maximum are 1024.
- `SINCE`: include current identities with producing revisions greater than this value, including retained deletions.
- `WITH DATA`: append a complete form for populated chunks and null for deletions; required with scan COLUMNS.

## Returns

- GET CHUNK: version, schema version, and presence count; `--blocks` adds individual values.
- Unwritten chunk: `NULL`, with no version token.
- SET CHUNK: destination chunk version; the input's version and time do not select the committed identity.
- GET AREA: populated chunks, including metadata-only chunks, in lexicographic coordinate order.
- SCAN CHUNKS: `chunks`, `more`, `epoch`, `position`, and `retained_deletion_floor`.
- Each scan row contains the active chunk coordinates and producing version, followed by a form or null when WITH DATA is present.
- Continue AFTER the last returned coordinate while `more` is true; read-only `position` is null.
- COMPACT TOMBSTONES: the retained-deletion floor after completion.
- SHOW TABLE STATS: `epoch`, nullable completed `revision`, `dims`, retained `tombstones`, and `retained_deletion_floor`.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| NO_TABLE | The table is absent or inaccessible. | Create it or request access. |
| PERMISSION_DENIED | You lack READ or WRITE. | Request access. |
| SCHEMA_MISMATCH | Your chunk uses another schema version. | Read DESCRIBE and encode again. |
| VERSION_MISMATCH | The destination changed. | Read it again and recompute. |
| INVALID_ARGUMENT | The geometry, values, or arguments are invalid. | Check the table and syntax. |
| INVALID_ARGUMENT | You run SET CHUNK on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| OUT_OF_RANGE | The area or response exceeds a limit. | Read a smaller area. |
| RESYNC_REQUIRED reason=history_lost | SINCE is below the retained-deletion floor. | Rebuild the consumer's state and resume at an admitted position. |

## Notes

SET CHUNK without COLUMNS replaces both column domains, including text and bytes.
A projected write replaces presence and selected block columns while preserving metadata.
Continuing blocks retain unnamed values; newly present blocks initialize them from defaults, NULL, or zero.
Absent positions lose all block values.
The form's schema must match the table, and its projected flag and descriptors must match the requested columns.

Deleting every block leaves a written empty chunk with a version.
Collection preserves its producing version and time in a durable tombstone.
An active transaction keeps its earlier snapshot within the [Transactions](../TRANSACTIONS.md#limits) bounds.
Reads and no-op writes do not make an unwritten chunk written.

Plain area reads and scan pages do not share a snapshot.
Use [Transactions](../TRANSACTIONS.md) when you need a consistent view of one table.

## Tombstones

GET CHUNK returns an empty form for a retained tombstone; ordinary areas and scans omit it.
SCAN SINCE returns its coordinates and producing version so consumers can observe deletion.
A scan reports current identities rather than every intervening change; use WATCH for an ordered change stream.
COMPACT TOMBSTONES requires table ADMIN and is unavailable inside transactions or on read-only servers.
BEFORE n forgets eligible retained tombstones through `min(n - 1, completed_revision)`; zero changes nothing.
Forgetting a tombstone removes its version token and permanently raises the cumulative deletion floor.
Requests below that floor receive `RESYNC_REQUIRED` with `floor`, `epoch`, and a known current frontier.
Compaction does not discard a durable slot's independently retained history.

## Limits

GET AREA covers at most 256 chunks and returns at most 67108864 encoded bytes.
The chunk limit bounds the AROUND radius at 9 in two dimensions and 3 in three dimensions.
SCAN CHUNKS returns at most 1024 identities and at most 67108864 encoded bytes per page.
Chunk values follow [Tables](TABLES.md#limits) and [Server flags](../SERVER_FLAGS.md).
