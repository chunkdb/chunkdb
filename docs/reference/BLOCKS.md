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
2
kind = 1
name = 'grass'
name = 'grass'
3
NULL
```

## Syntax

```text
GET BLOCK x y FROM table [COLUMNS column, ...]
SET BLOCK x y IN table column = value [, ...] [IF VERSION version]
DELETE BLOCK x y FROM table [IF VERSION version]
```

## Arguments

- `x`, `y`: signed block coordinates, from -9223372036854775808 to 9223372036854775807.
- `table`: the table containing the block.
- `COLUMNS`: returned columns, in your requested order; omitting it uses schema order.
- `column = value`: values to change; unspecified columns of an existing block retain their values.
- `IF VERSION`: update only when the containing chunk still has this version; another block's change can invalidate it.

## Returns

- GET: one `column = value` line per column, or `NULL` when the block is absent.
- SET and DELETE: the containing chunk's version after the statement.
- Transaction writes: `(applies at COMMIT)` in the CLI shell.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| NO_TABLE | The table is absent or inaccessible. | Create it or request access. |
| PERMISSION_DENIED | You lack READ or WRITE. | Request the required right. |
| INVALID_ARGUMENT | A value, column, or required field is invalid. | Check [Tables](TABLES.md). |
| VERSION_MISMATCH | Another write changed the chunk. | Read it again and recompute your update. |
| CONFLICT | Your transaction loses its snapshot. | Follow [Retry a transaction](../TRANSACTION_RETRY.md). |

## Notes

A new block uses each omitted column's default, then NULL if allowed, then zero or empty.
A REQUIRED column without a default must be supplied.
Absent blocks differ from present blocks whose values are zero or NULL.
A rejected value changes nothing.

A version is an equality token, never a timestamp.
Compare versions only for equality; successful changes issue new tokens across restarts.
An absent chunk supplies no token, and zero does not mean create-if-absent.

## Limits

Value bounds follow the table's column types and `var_max_chunk_bytes`.
See [Transactions](../TRANSACTIONS.md#limits) for transaction bounds and [Protocol](../PROTOCOL.md#parameters) for parameter bounds.
