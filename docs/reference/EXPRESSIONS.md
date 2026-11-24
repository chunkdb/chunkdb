# Expressions

You compute a block's new values from its current ones, and write only when a condition holds, in [SET BLOCK and DELETE BLOCK](BLOCKS.md).

## Examples

These examples create `block_wallet`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE block_wallet (coins u16) CHUNK 2 x 2
SET BLOCK 0 0 IN block_wallet coins = COALESCE(coins, 0) + 5
SET BLOCK 0 0 IN block_wallet coins = coins - 3 IF coins >= 3
SET BLOCK 0 0 IN block_wallet coins = coins - 3 IF coins >= 3
SET BLOCK 0 0 IN block_wallet coins = coins - 3
DELETE BLOCK 0 0 FROM block_wallet IF coins = 2
```

```output
OK
1
2
error: CONDITION_FAILED current=2 reason=condition_failed the condition does not hold; nothing changed
error: OUT_OF_RANGE reason=out_of_range column coins does not hold -1: u16 holds 0..65535
3
```

## Syntax

```text
SET BLOCK x y [z] IN table column = value [, ...] [IF VERSION version | IF condition]
DELETE BLOCK x y [z] FROM table [IF VERSION version | IF condition]
```

A `value` and a `condition` follow [Conditions](../CQL.md#conditions): columns, `chunk.column`, positions, `NOW()`, typed `$n` parameters and the same operators, types and NULL rules.

## In writes

A statement whose values are all single values or parameters writes as without expressions; one computed value makes every value an expression.
The condition and every value read the block once, under the lock of the write, so no other write comes between the read and the write.
All values read the block as it was before the statement: `a = b, b = a` swaps two columns.
A refused statement changes nothing and produces no feed change.

- A false or unknown condition answers `CONDITION_FAILED current=<version>` with the chunk's version.
- A value outside its column's type is OUT_OF_RANGE: integers never wrap or clamp, a finite float beyond f32 is refused, and text or bytes must fit the column; floats round to nearest when assigned to f32.
- NULL for a column without NULL is INVALID_ARGUMENT; count from an absent block with `COALESCE(count, 0) + 1`.
- A float is never assigned to an integer column.
- `IF NOT EXISTS` writes only a new block, and `DELETE ... IF EXISTS AND ...` deletes only a present block.
- IF VERSION combines with computed values but not with a condition.
- A condition, like any write, needs WRITE on the table, which includes READ.

Inside a transaction, the condition and values read the transaction's view, including its own earlier writes.
A condition, or a value that reads a block or chunk column, counts as a read of its chunk: COMMIT conflicts when another write changed that chunk.
A refused statement leaves the transaction open and takes none of its write room; `NOW()` is refused there, since the commit time is known only at COMMIT.
