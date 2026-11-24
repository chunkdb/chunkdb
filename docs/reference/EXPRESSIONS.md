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
condition := and_expr { OR and_expr }
and_expr  := not_expr { AND not_expr }
not_expr  := NOT not_expr | predicate
predicate := ( condition ) | EXISTS | value cmp value | value IS [NOT] NULL
           | value [NOT] IN ( literal | $n, ... ) | value [NOT] BETWEEN value AND value | bool_value
value     := term { (+ | -) term }
term      := factor { (* | / | %) factor }
factor    := - factor | literal | $n | column | chunk.column | POS_X | POS_Y | POS_Z | NOW()
           | LEAST ( value, ... ) | GREATEST ( value, ... ) | COALESCE ( value, ... ) | ( value )
cmp       := = | <> | < | <= | > | >=
```

- `column` names a block column; write `"name"` for a column named like a keyword, such as `"exists"`.
- `chunk.column` names a [chunk column](CHUNKS.md); `POS_X`, `POS_Y` and `POS_Z` are the block's coordinates.
- `NOW()` is the write's commit time in milliseconds.
- Comparisons do not chain: write `a < b AND b < c` for `a < b < c`.

## Types

- Integers compute exactly; a result beyond 128 bits is OUT_OF_RANGE, and dividing an integer by zero is INVALID_ARGUMENT.
- `/` truncates toward zero, and `%` takes the sign of the dividend.
- An integer with a float computes in f64, but comparisons between them are exact; NaN equals nothing, and -0 equals 0.
- Text and bytes compare bytewise; bits compare with `=` and `<>`; bool compares with `=` and `<>` and is a condition by itself.
- A parameter or literal takes the type of the column operand it meets, and its frame is encoded as that type ([Protocol](../PROTOCOL.md#parameters)); `$1 < $2` has no column operand and is refused.
- LEAST and GREATEST take numbers and ignore NULL arguments; COALESCE returns its first argument that is not NULL.

## NULL

Arithmetic with NULL is NULL; a comparison with NULL is unknown, and an unknown condition is false.
`IS NULL` and `IS NOT NULL` are never unknown.
For an absent block, EXISTS is false and every block column is NULL; chunk columns keep their values.

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

## Limits

An expression has at most 256 nodes (values, columns, operators and list items) and 32 levels of nesting; HELLO reports `max_expression_nodes`.
