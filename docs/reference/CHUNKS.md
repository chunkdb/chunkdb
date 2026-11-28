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
GET AREA cx0 cy0 [cz0] TO cx1 cy1 [cz1] FROM table [COLUMNS column, ...] [WITH SCHEMA] [LIMIT n] [AFTER cx cy [cz]]
    [SCALE k USING MAX | MIN | MODE | FIRST | ANY]
GET AREA AROUND cx cy [cz] RADIUS radius FROM table [COLUMNS column, ...] [WITH SCHEMA] [LIMIT n] [AFTER cx cy [cz]]
    [SCALE k USING MAX | MIN | MODE | FIRST | ANY]
SCAN CHUNKS FROM table [AFTER cx cy [cz]] [LIMIT count] [SINCE revision] [WHERE condition] [WITH DATA] [COLUMNS column, ...]
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
- `AFTER`: exclude this position and all earlier positions in coordinate order.
- `LIMIT`: requested cells per area page, or maximum positions per scan page; scans default to at most 1024.
- `SCALE k USING`: return each chunk downsampled by k with the named reduction; see [Downsampled areas](#downsampled-areas).
- `SINCE`: include current identities with producing revisions greater than this value, including retained deletions.
- `WHERE`: keep only blocks for which a [condition](../CQL.md#conditions) is true; see [Filter a scan](#filter-a-scan).
- `WITH DATA`: append a complete form for populated chunks and null for deletions; required with scan COLUMNS.

## Returns

- GET CHUNK: version, schema version, and presence count; `--blocks` adds individual values.
- Unwritten chunk: `NULL`, with no version token.
- SET CHUNK: destination chunk version; the input's version and time do not select the committed identity.
- GET AREA: populated chunks, including metadata-only chunks, in lexicographic coordinate order.
- SCAN CHUNKS: `chunks`, `more`, `epoch`, `position`, and `retained_deletion_floor`; with WHERE, `after` follows `more`.
- Each scan row contains the active chunk coordinates and the version used by IF VERSION, followed by a form or null when WITH DATA is present.
- For a written chunk, the returned version is its producing revision or the table's name floor, whichever is higher.
- Scan rows ascend by cx, then cy, then cz, within a page and across pages continued AFTER.
- Continue AFTER the last returned coordinate while `more` is true; with WHERE, continue AFTER the returned `after`, since a page may return no rows; on a server started with `--read-only`, `position` is null.
- Pass the first page's `epoch` and `position` to WATCH AFTER to follow later changes; see [Resume a consumer](../FEED_RESUME.md).
- COMPACT TOMBSTONES: the retained-deletion floor after completion.
- SHOW TABLE STATS: `epoch`, nullable completed `revision`, `dims`, `chunks`, retained `tombstones`, `retained_deletion_floor`, `loaded_chunks`, `files`, `disk_bytes`, `slots`, and `watches`.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| NO_TABLE | The table is absent or inaccessible. | Create it or request access. |
| PERMISSION_DENIED | You lack READ or WRITE. | Request access. |
| SCHEMA_MISMATCH | Your chunk uses another schema version. | Read DESCRIBE and encode again. |
| VERSION_MISMATCH | The destination changed. | Read it again and recompute. |
| INVALID_ARGUMENT | The geometry, values, or arguments are invalid. | Check the table and syntax. |
| INVALID_ARGUMENT | You run SET CHUNK on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| SYNTAX | SCAN CHUNKS has WHERE with SINCE but without WITH DATA, or a condition that does not parse. | Add WITH DATA and check the [condition rules](../CQL.md#conditions). |
| INVALID_ARGUMENT | WHERE names an unknown column, mixes types, has a parameter with no column to take its type from or of the wrong size, or divides an integer by zero. | Check DESCRIBE and the condition. |
| INVALID_ARGUMENT | SCALE does not divide every chunk side, or a column type does not accept the reduction. | Choose another factor, reduction or COLUMNS. |
| OUT_OF_RANGE | The area or response exceeds a limit. | Read a smaller area. |
| OUT_OF_RANGE reason=area_entry_too_large | One chunk cannot fit the response limit. | Read it with GET CHUNK or fewer COLUMNS; continue AFTER it. |
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

## Table stats

SHOW TABLE STATS needs READ on the table, or MONITOR.
`chunks` counts populated chunks, the ones SCAN CHUNKS lists; `loaded_chunks` counts the table's chunks held in the chunk cache.
`files` and `disk_bytes` count the regular files under the table's directory and their sizes, WAL, images, feed archive and index files included.
`slots` counts durable slots, lost ones included; `watches` counts open watches, slot watches included.
The counts come from memory, except `files` and `disk_bytes`, which list the table's directory: their cost grows with its file count.
The listing runs after the statement stops using the table, so no other statement waits for it, and a statement timeout stops it.
A file written or removed during the listing may or may not be counted; a table dropped before it returns NO_TABLE, and a SWAP TABLES, or a DROP TABLE and CREATE TABLE of the same name, during it may count the other table's files.

## Downsampled areas

`SCALE k USING reduction` returns each populated chunk as a downsampled form: every group of k blocks on each active axis becomes one sample.
k must be positive and divide every chunk side, so a group never crosses a chunk edge; a chunk of 16 x 16 blocks with `SCALE 4` has 4 x 4 samples.
Each chunk is reduced on its own, so the area can hold any number of chunks.
A sample is present when a block of its group is present; samples are numbered like blocks, with each side divided by k.
Each projected column, or every block column without COLUMNS, takes the value of one present block of the group:

| Reduction | Value | Column types |
| --- | --- | --- |
| MAX | The largest value. | Integers, bool, f32, f64 |
| MIN | The smallest value. | Integers, bool, f32, f64 |
| MODE | The most frequent value; of equally frequent values, the one that occurs first. | Integers, bool, bits |
| FIRST | The value of the first present block, NULL included. | All |
| ANY | The value of some present block that is not NULL; which block is unspecified. | All |

First means first in ascending local block index `x + width * (y + height * z)`, so x changes fastest.
MAX, MIN, MODE and ANY skip NULL values; their sample is NULL only when every present block of the group is NULL.
MAX and MIN skip NaN values and return NaN only when every value that is not NULL is NaN; -0 and 0 are equal, and of equal values the first wins.
A column of another type is refused with INVALID_ARGUMENT before any chunk is read.
The form keeps the chunk version, commit time, schema version and complete chunk columns, and adds section 7 with k and the reduction; see [Protocol](../PROTOCOL.md#chunk-form).
SET CHUNK refuses a downsampled form.
SCALE works with COLUMNS, WITH SCHEMA, LIMIT, AFTER and transactions, and the response limit counts the downsampled forms.
LIMIT, AFTER and `max_area_chunks` still count requested chunk positions, not samples.

## Tombstones

GET CHUNK returns an empty form for a retained tombstone; ordinary areas and scans omit it.
SCAN SINCE selects the tombstone by its stored producing revision and returns its coordinates and the version used by IF VERSION.
A scan reports current identities rather than every intervening change; use WATCH for an ordered change stream.
COMPACT TOMBSTONES requires table ADMIN and is unavailable inside transactions or on read-only servers.
BEFORE n forgets eligible retained tombstones through `min(n - 1, completed_revision)`; zero changes nothing.
Forgetting a tombstone removes its version token and permanently raises the cumulative deletion floor.
Requests below that floor receive `RESYNC_REQUIRED` with `floor`, `epoch`, and a known current frontier.
Compaction does not discard a durable slot's independently retained history.

## Filter a scan

WHERE keeps the present blocks for which the condition is true; a block where it is false or NULL is left out.
Without SINCE, a scan lists only chunks with a matching block, and WITH DATA returns a form of those blocks with complete chunk columns.
LIMIT counts every chunk the page visits, kept or not, so a page can hold fewer rows than LIMIT, or none.
Continue AFTER the returned `after` coordinate while `more` is true; it is null when `more` is false.

```cql docs-check session=main
SET BLOCK 3 1 IN chunk_demo kind = 4
SCAN CHUNKS FROM chunk_demo WHERE kind > 2 LIMIT 1
SCAN CHUNKS FROM chunk_demo AFTER 0 0 WHERE kind > 2
```

```output
3
chunks = []
more = true
after = [0, 0]
epoch = <epoch>
position = 3
retained_deletion_floor = 0
chunks:
  1) [1, 0, 3]
more = false
after = NULL
epoch = <epoch>
position = 3
retained_deletion_floor = 0
```

With SINCE, which then requires WITH DATA, a scan lists every identity changed after the revision: a form of the matching blocks, or null when no block matches or the chunk was deleted.
A consumer that replaces each listed chunk with that form, or drops the chunk on null, holds exactly the matching blocks.

To follow a filtered scan, start WATCH AFTER the first page's `epoch` and `position` as in [Resume a consumer](../FEED_RESUME.md) and apply the same condition to each change's blocks.
This converges when the condition reads only the block's own columns, EXISTS and positions.
A change to a chunk column carries no blocks, so a condition on `chunk.column` needs the chunk read again with GET CHUNK when that column changes; a condition with NOW() selects at the time of each statement.

## Limits

GET AREA defaults to at most 4096 requested chunk cells and 67108864 encoded response bytes.
Configure these positive bounds with `--max-area-chunks` and `--max-area-response-bytes`; HELLO reports their effective values.
Without LIMIT or AFTER, a larger selector or response is refused.
LIMIT or AFTER returns a map with `chunks`, `more` and `after` instead of the plain chunk array.
WITH SCHEMA wraps that map as `[schema_version, page]`.
The page visits at most LIMIT requested cells, including empty cells, and returns only populated chunks.
An omitted LIMIT uses `max_area_chunks`; an explicit limit must be positive and no greater than that value.
When `more` is true, resume the same selector strictly AFTER the returned coordinate; when it is false, `after` is null.
AFTER must have the table's exact coordinate arity and lie inside the requested range or Euclidean ball.
An empty page can advance across an empty region, and the response-byte bound can end a page before its requested cell count.
A populated chunk that cannot fit an otherwise empty page ends the page before it, so the next page starts at that chunk.
As a page's first cell, or in a plain area, it is refused with `OUT_OF_RANGE reason=area_entry_too_large chunk=x,y[,z] required=n limit=n`.
`required` is the size of a reply holding only that chunk; read it with GET CHUNK or fewer COLUMNS, then continue AFTER its coordinate.
A server started with `--max-area-response-bytes` of at least `required` returns that chunk in GET AREA.
Independent pages can observe intervening writes; pages inside a transaction share its snapshot and private writes.
SCAN CHUNKS runs inside a [BEGIN READ ONLY](TRANSACTION_STATEMENTS.md) transaction without SINCE: its pages list the chunks populated at the snapshot and report the snapshot's revision as `position`.

An empty page still provides a continuation when requested cells remain:

```cql docs-check
CREATE TABLE page_demo (kind u8) CHUNK 2 x 2
GET AREA 0 0 TO 2 0 FROM page_demo LIMIT 2 WITH SCHEMA
GET AREA 0 0 TO 2 0 FROM page_demo AFTER 1 0 LIMIT 2 WITH SCHEMA
```

```output
OK
schema_version = 1
more = true
after = [1, 0]
(empty)
schema_version = 1
more = false
after = NULL
(empty)
```

SCAN CHUNKS returns at most 1024 identities and at most 67108864 encoded bytes per page.
Chunk values follow [Tables](TABLES.md#limits) and [Server flags](../SERVER_FLAGS.md).
