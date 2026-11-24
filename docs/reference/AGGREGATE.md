# AGGREGATE

You count and summarize the present blocks of a chunk, an area or a ball on the server, optionally only those that match a condition.

## Examples

These examples create `aggregate_demo`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE aggregate_demo (height i16, depth f64 NULL) CHUNK 2 x 2
SET BLOCK 0 0 IN aggregate_demo height = 12, depth = 1.5
SET BLOCK 1 0 IN aggregate_demo height = 5
SET BLOCK 2 0 IN aggregate_demo height = -3, depth = 0.5
AGGREGATE AREA 0 0 TO 1 0 FROM aggregate_demo COUNT(*), SUM(height), MIN(height), MAX(depth), AVG(depth)
AGGREGATE AREA 0 0 TO 1 0 FROM aggregate_demo COUNT(*), SUM(height) BY CHUNK
AGGREGATE AREA 0 0 TO 1 0 FROM aggregate_demo COUNT(*), AVG(height) WHERE depth IS NOT NULL
```

```output
OK
1
2
3
count(*) = 3
sum(height) = 14
min(height) = -3
max(depth) = 1.5
avg(depth) = 1
1) [0, 0, 2, 17]
2) [1, 0, 1, -3]
count(*) = 2
avg(height) = 4.5
```

## Syntax

```text
AGGREGATE CHUNK cx cy [cz] FROM table function, ... [WHERE condition] [BY CHUNK]
AGGREGATE AREA cx0 cy0 [cz0] TO cx1 cy1 [cz1] FROM table function, ... [WHERE condition] [BY CHUNK]
AGGREGATE AREA AROUND cx cy [cz] RADIUS radius FROM table function, ... [WHERE condition] [BY CHUNK]
```

A function is `COUNT(*)`, `SUM(column)`, `MIN(column)`, `MAX(column)` or `AVG(column)`.

## Arguments

- `CHUNK`, `AREA`, `TO`, `AROUND`, `RADIUS`: the chunks, with chunk coordinates as in [GET CHUNK and GET AREA](CHUNKS.md#arguments).
- `COUNT(*)`: the present blocks.
- `SUM`, `MIN`, `MAX`, `AVG`: a block column of an integer (`uN`, `iN`) or float (`f32`, `f64`) type.
- Each function appears at most once.
- `WHERE`: a [condition](../CQL.md#conditions); only present blocks for which it is true count, and a block where it is false or NULL is skipped.
- `BY CHUNK`: one result row per chunk instead of one result for all chunks.

## Returns

- Without BY CHUNK: a map from each function's lowercase name, such as `count(*)` or `sum(height)`, to its result, in the order of the functions.
- BY CHUNK: rows `[cx, cy(, cz), results...]` in coordinate order, one for each chunk with a present block that matches WHERE, with results in the order of the functions.
- COUNT(*): the number of present blocks, zero when there is none.
- SUM, MIN, MAX and AVG skip NULL values and return NULL when no present block has a value.
- Integer SUM: the exact sum, an integer within the signed int64 range and a big number outside it.
- Integer MIN and MAX: the value, a big number for a u64 value above the signed int64 range.
- Integer AVG: a double, the exact sum and the count, each rounded to f64, divided in f64.
- Float SUM and AVG: a double, adding the values as f64 in coordinate order of the chunks and block index order within a chunk; a NaN, or infinities of both signs, give NaN.
- Float MIN and MAX: a value of the column's type; NaN orders above every other value, as in PostgreSQL.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| NO_TABLE | The table is absent or inaccessible. | Create it or request access. |
| PERMISSION_DENIED | You lack READ. | Request access. |
| SYNTAX | The statement repeats a function or WHERE, has `COUNT(column)`, or its condition does not parse or exceeds its limits. | Check the syntax and the [condition rules](../CQL.md#conditions). |
| INVALID_ARGUMENT | A column is unknown, or holds no integer or float values. | Check DESCRIBE. |
| INVALID_ARGUMENT | WHERE names an unknown column, mixes types, has a parameter with no column to take its type from or of the wrong size, or divides an integer by zero. | Check DESCRIBE and the condition. |
| INVALID_ARGUMENT | The coordinates differ from the table's dims, or the selector covers more chunks than the limit. | Aggregate smaller areas and combine their results. |
| INVALID_ARGUMENT | Inside a transaction, the selector would take the read set past `--txn-max-read-chunks`. | Aggregate a smaller area or commit first. |
| OUT_OF_RANGE | The reply exceeds the response limit, or an integer SUM or AVG leaves the 128-bit range: `sum(v) is outside the 128-bit integer range`, with `avg(v)` for AVG. | Aggregate a smaller area or fewer functions. |
| OUT_OF_RANGE | Integer arithmetic in WHERE leaves the 128-bit range. | Change the condition. |

## Limits

A selector covers at most `--max-area-chunks` requested chunk cells, 4096 by default, empty cells included; HELLO reports the value as `max_area_chunks`.
AGGREGATE has no pages: split a larger area and combine COUNT, SUM, MIN and MAX of the parts in your client.
The reply is at most `--max-area-response-bytes`, 67108864 bytes by default.
CANCEL, KILL CONNECTION and statement timeouts stop AGGREGATE between chunks and within large chunks.
WHERE decodes the columns it names for every present block, so it costs more than an aggregate without it.

## Notes

Outside a transaction, each chunk is read as it is when AGGREGATE reaches it, so writes during the statement can be counted in some chunks and not in others.
Inside a [transaction](../TRANSACTIONS.md), AGGREGATE reads the transaction's snapshot and its own writes, and every requested chunk cell joins its read set, as with GET AREA.
With WHERE, every requested cell still joins the read set, because a write to any block can change which blocks match.
A server started with `--read-only` runs AGGREGATE, with or without WHERE.
