# GET CHUNK, SET CHUNK, GET AREA, SCAN CHUNKS

You read or replace whole chunks and find populated chunks in an area.
A chunk groups the fixed rectangle of blocks specified by its table.

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
present = 1 of 4 blocks
block 0 0
  kind = 1
chunk 0 0
  version = 1
  schema_version = 1
  present = 1 of 4 blocks
  block 0 0
    kind = 1
chunk 0 0
  version = 1
  schema_version = 1
  present = 1 of 4 blocks
chunks:
  1) [0, 0]
more = false
```

To replace a chunk through the CLI, save its complete binary form, then supply it as input:

```sh docs-check-shell
chunk-cli --uri "$CHUNKDB_URI" --out "$EXAMPLE_DIR/chunk.bin" "GET CHUNK 0 0 FROM chunk_demo"
chunk-cli --uri "$CHUNKDB_URI" --in "$EXAMPLE_DIR/chunk.bin" 'SET CHUNK 1 0 IN chunk_demo $1'
```

```output
wrote 21 bytes to <example-dir>/chunk.bin
2
```

`EXAMPLE_DIR` is a writable directory you choose.
Client APIs can construct a chunk form directly.

## Syntax

```text
GET CHUNK cx cy FROM table [COLUMNS column, ...]
SET CHUNK cx cy IN table $1 [IF VERSION version]
GET AREA cx0 cy0 TO cx1 cy1 FROM table [COLUMNS column, ...] [WITH SCHEMA]
GET AREA AROUND cx cy RADIUS radius FROM table [COLUMNS column, ...] [WITH SCHEMA]
SCAN CHUNKS FROM table [AFTER cx cy] [LIMIT count]
```

## Arguments

- `cx`, `cy`: signed chunk coordinates, from -9223372036854775808 to 9223372036854775807, rather than block coordinates.
- `TO`: inclusive opposite corner; coordinates must form an ordered rectangle.
- `RADIUS`: nonnegative distance in chunks around the centre; the area is the disc of chunks with dx² + dy² ≤ radius², where dx and dy are offsets from the centre.
- `COLUMNS`: a projection, meaning only these columns are returned, in requested order.
- `WITH SCHEMA`: return `[schema_version, entries]`, including the schema version when no chunks match.
- `$1`: a complete chunk form supplied through your client; see [Protocol](../PROTOCOL.md#chunk-form).
- `IF VERSION`: an equality check against the destination chunk's current version.
- `AFTER`: exclude this position and all earlier positions in scan order.
- `LIMIT`: maximum positions per scan page; default and maximum are 1024.

## Returns

- GET CHUNK: version, schema version, and presence count; `--blocks` adds individual values.
- Unwritten chunk: `NULL`, with no version token.
- SET CHUNK: destination chunk version; the input's chunk version is ignored.
- GET AREA: chunks with at least one present block, ordered by `cx`, then `cy`.
- SCAN CHUNKS: populated positions in ascending `cx`, then ascending `cy`, and `more`; continue AFTER the last position when `more` is true.

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

## Notes

SET CHUNK replaces every column, including text and bytes.
A projection is not a complete replacement form.

Deleting every block leaves a written empty chunk with a version.
Collection, followed by cache eviction or restart, can remove that empty form.
An active transaction keeps its earlier snapshot within the [Transactions](../TRANSACTIONS.md#limits) bounds.
Reads and no-op writes do not make an unwritten chunk written.

Plain area reads and scan pages do not share a snapshot.
Use [Transactions](../TRANSACTIONS.md) when you need a consistent view of one table.

## Limits

GET AREA covers at most 256 chunks and returns at most 67108864 encoded bytes.
The chunk limit bounds the AROUND radius at 9, whose disc covers 253 chunks; radius 10 covers 317.
SCAN CHUNKS returns at most 1024 positions per page.
Chunk values follow [Tables](TABLES.md#limits) and [Server flags](../SERVER_FLAGS.md).
