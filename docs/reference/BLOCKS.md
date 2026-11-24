# GET BLOCK, SET BLOCK, DELETE BLOCK

You read, update, or remove one grid cell in a table.
Run examples in the [CLI shell](../QUICK_START.md#2-connect-the-shell).

## Examples

These examples create `block_demo`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE block_demo (kind u8, name text(16) NULL) CHUNK 2 x 2
SET BLOCK 0 0 IN block_demo kind = 1, name = 'grass'
GET BLOCK 0 0 FROM block_demo
GET BLOCK 0 0 FROM block_demo COLUMNS name
DELETE BLOCK 0 0 FROM block_demo
GET BLOCK 0 0 FROM block_demo
```

```output
OK
1
kind = 1
name = 'grass'
name = 'grass'
2
NULL
```

## Syntax

```text
GET BLOCK x y [z] FROM table [COLUMNS column, ...] [WITH VERSION] [WITH SCHEMA]
SET BLOCK x y [z] IN table column = expression [, ...] [IF VERSION version | IF condition] [RETURNING [OLD | NEW] (* | column, ...)]
    [PUBLISH 'channel' message]
DELETE BLOCK x y [z] FROM table [IF VERSION version | IF condition] [RETURNING [OLD] (* | column, ...)]
    [PUBLISH 'channel' message]
```

## Arguments

- `x`, `y`, `z`: signed block coordinates, from -9223372036854775808 to 9223372036854775807.
- Supply exactly two coordinates for a two-dimensional table and three for a three-dimensional table, including when `z` is zero.
- `table`: the table containing the block.
- `COLUMNS`: returned columns, in your requested order; omitting it uses schema order.
- `WITH VERSION`: include the containing chunk's committed version with the row.
- `WITH SCHEMA`: include the table's schema version with the row.
- `column = expression`: values to change, each a value, a `$n` parameter or an [expression](EXPRESSIONS.md) such as `coins + $1`; unspecified columns of an existing block retain their values.
- `IF VERSION`: update only when the containing chunk still has this version, an integer or a `$n` u64 parameter; another block's change can invalidate it.
- `IF condition`: write only when the [condition](EXPRESSIONS.md) holds for the block as it is ([conditions](../CQL.md#conditions)); it is false when it is unknown (NULL).
- IF VERSION and IF condition exclude each other; `IF version = 3` tests a column named `version`.
- `RETURNING`: return the block with the write; `NEW`, SET's default, is the block after the write, and `OLD`, DELETE's default and only form, the block before it.
- RETURNING `*` returns every column in schema order, a list the named columns in your order; OLD or NEW right after RETURNING is the keyword, so `RETURNING NEW old` returns a column named `old`.
- `PUBLISH`: publish a message in the write's commit, see [Publish](#publish); `channel` is 1 to 63 bytes of `[a-z_][a-z0-9_.]*`, `message` a text or bytes value or a `$n` parameter.

## Returns

- GET: one `column = value` line per column, or `NULL` when the block is absent.
- SET and DELETE: the containing chunk's version after the statement.
- SET and DELETE with RETURNING: `applied`, `version` and `rows`, described below.
- Transaction writes: `(applies at COMMIT)` in the CLI shell.

WITH VERSION returns `[chunk_version, row_or_null]`; WITH SCHEMA returns `[schema_version, row_or_null]`.
Combining them returns `[chunk_version, schema_version, row_or_null]` in either clause order.
The metadata describes the returned row, including transaction snapshot reads.
Transaction-private modified chunks have null versions until COMMIT.

An absent block retains a version when its containing chunk retains a committed version.
Collection preserves an empty chunk's producing version and time in a tombstone.
Never-written chunks and chunks forgotten by [tombstone compaction](CHUNKS.md#tombstones) have null versions.
The schema version is present even when the row is absent.

With RETURNING, `applied` is false when IF VERSION or the IF condition refuses the write, which then changes nothing and returns no rows.
`version` is the chunk's version after the write, or its current version when refused, null for a never-written chunk; inside a transaction it is that of the transaction's view, null for a chunk the transaction wrote.
Each row is `[x, y, values...]`, with `z` after `y` in a three-dimensional table.
An OLD row exists only for a block present before the write; a NEW row holds the values a GET BLOCK reads after the write, including the defaults of a new block.
Other refusals, such as an unknown column or a value out of range, are errors as without RETURNING.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| NO_TABLE | The table is absent or inaccessible. | Create it or request access. |
| PERMISSION_DENIED | You lack READ or WRITE. | Request the required right. |
| INVALID_ARGUMENT | A value, column, or required field is invalid, or a computed value is NULL for a column without NULL. | Check [Tables](TABLES.md) and [Expressions](EXPRESSIONS.md). |
| INVALID_ARGUMENT | You run SET BLOCK or DELETE BLOCK on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| VERSION_MISMATCH | Without RETURNING: another write changed the chunk. | Read it again and recompute your update. |
| CONDITION_FAILED | Without RETURNING: the IF condition is false or unknown; `current` is the chunk's version. | Read the block again and decide again. |
| OUT_OF_RANGE | A computed value does not fit its column, such as `coins - 3` below zero in a u16 column. | Change the expression or add a condition. |
| OUT_OF_RANGE reason=returning_limit | The RETURNING rows exceed a bound; nothing changed. | Return fewer columns. |
| INVALID_ARGUMENT reason=feature_disabled | PUBLISH on a table without the `messages` feature. | Run `ENABLE FEATURE 'messages'`, see [Tables](TABLES.md#feature-activation). |
| INVALID_ARGUMENT | A second PUBLISH in one transaction, or a NULL message. | Publish one message per transaction. |
| OUT_OF_RANGE | The message exceeds `max_message_bytes`. | Send a shorter message. |
| CONFLICT | Your transaction loses its snapshot. | Follow [Retry a transaction](../TRANSACTION_RETRY.md). |

## Notes

A new block uses each omitted column's default, then NULL if allowed, then zero or empty.
A REQUIRED column without a default must be supplied.
A REQUIRED chunk column without a default must be initialized by a complete chunk form or `SET CHUNK ... META` before the first block write.
Absent blocks differ from present blocks whose values are zero or NULL.
A rejected value changes nothing.

A committed chunk version is its producing commit revision, never a timestamp.
Compare revisions only within the same table epoch, obtained from DESCRIBE.
IF VERSION still compares for equality; another chunk's write does not change this chunk's version.
An absent chunk supplies no token, and zero does not mean create-if-absent.

A condition and computed values read the block with the write, so no other write comes between; see [Expressions in writes](EXPRESSIONS.md#in-writes).
RETURNING reads the block the same way, so it needs WRITE, which includes READ.

## Publish

PUBLISH adds an application message to the write's commit, and the [change feed](../CHANGE_FEED.md) delivers it in that commit's change.

```text
SET BLOCK 4 2 IN orders status = 2 PUBLISH 'orders.paid' $1
```

- The reply is the write's usual reply.
- A write that publishes always commits: when the block already holds these values, it still takes a new version, and its change has no blocks.
- A refused IF VERSION or IF condition publishes nothing.
- Inside a transaction the write stages the message and COMMIT publishes it with the transaction's one change; a transaction publishes at most one message.
- A watch with AREA receives the message only when it receives the change, that is when the change keeps a block or chunk change in its areas.

## Limits

Value bounds follow the table's column types and `var_max_chunk_bytes`.
RETURNING returns at most `max_returning_rows` (65536) rows in a reply of at most `max_response_bytes`, both reported by HELLO; over either bound the write is refused with `OUT_OF_RANGE reason=returning_limit` and changes nothing.
A message has at most `max_message_bytes` (16384) bytes, reported by HELLO.
See [Transactions](../TRANSACTIONS.md#limits) for transaction bounds and [Protocol](../PROTOCOL.md#parameters) for parameter bounds.
