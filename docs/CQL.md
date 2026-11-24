# CQL reference

CQL is the statement language you use to read and change chunkdb tables.
Keywords are case-insensitive.
Reads and deletes name a table after `FROM`; writes use `IN`.

The examples use the [chunk-cli shell](QUICK_START.md#2-connect-the-shell).
Input and output appear in separate blocks; shell prompts are omitted.
Examples are recorded against the current source build and checked in CI.
Generated passwords, epochs, and timestamps appear as named placeholders.

## Statements

| Reference | Statements |
| --- | --- |
| [Blocks](reference/BLOCKS.md) | GET BLOCK, SET BLOCK, DELETE BLOCK |
| [Expressions](reference/EXPRESSIONS.md) | Computed values and IF conditions of SET BLOCK and DELETE BLOCK |
| [Chunks and areas](reference/CHUNKS.md) | GET CHUNK, SET CHUNK, GET AREA, SCAN CHUNKS, COMPACT TOMBSTONES, SHOW TABLE STATS |
| [Aggregates](reference/AGGREGATE.md) | AGGREGATE |
| [Tables](reference/TABLES.md) | CREATE TABLE, ALTER TABLE, DROP TABLE, RECOVER TABLE, CHECK TABLE, SWAP TABLES, SHOW TABLES, DESCRIBE, SHOW STATUS, ENABLE FEATURE |
| [Migrations](reference/MIGRATIONS.md) | MIGRATE, SHOW MIGRATIONS |
| [Transaction statements](reference/TRANSACTION_STATEMENTS.md) | BEGIN, COMMIT, ROLLBACK |
| [Feed statements](reference/FEED.md) | WATCH, ACK, UNWATCH, CREATE SLOT, DROP SLOT, SHOW SLOTS |
| [User statements](reference/USER_STATEMENTS.md) | CREATE USER, ALTER USER, DROP USER, GRANT, REVOKE, SHOW USERS |
| [Server statements](reference/SERVER.md) | PING, FLUSH WAL, SHOW METRICS, BACKUP TO, RESTORE TABLE, RELOAD TLS, SHOW CONNECTIONS, CANCEL, KILL CONNECTION |

## Names and values

See [Tables](reference/TABLES.md#arguments) for column types, identifiers, defaults, and limits.
Use integers such as `-12`, floats such as `1.5` or `2e-3`, booleans, or `NULL`.
Float values also accept `inf` and `nan`.
A float literal reads as the nearest f64, subnormal values such as `1e-310` included; one beyond the f64 range, such as `1e999`, is refused with SYNTAX.

Quote text with single quotes and double an embedded quote: `'it''s'`.
Bytes use hexadecimal literals such as `x'0a0bff'`.
Bits use literals such as `b'1010'`, with the first digit representing the lowest bit.

Trailing keyword options can appear in any order, each at most once.
A repeated option is refused with SYNTAX.
`IF` starts `IF [NOT] EXISTS` only when NOT or EXISTS follows it, so a table, column, user or check named `if` can be named where that clause may appear, as in `DROP TABLE if`.
Client APIs supply binary `$n` parameters in these places: SET BLOCK values, expressions and conditions, DELETE BLOCK conditions, values in the WHERE of AGGREGATE and SCAN CHUNKS, the SET CHUNK form or META values, `VERIFIER $1` in CREATE USER and ALTER USER (also `ADD VERIFIER $1`), HELLO WITH option values, and the SCRAM messages of `HELLO 3 USER <name> $1` and `AUTH $1`.
See [Protocol](PROTOCOL.md#parameters) for encoding.

Committed chunk versions are producing commit revisions, ordered only within the table epoch returned by DESCRIBE.
DESCRIBE's schema `version` differs from its completed commit `revision`; read-only mode returns null for that frontier.
Use IF VERSION for equality checks against the containing chunk, including changes to other blocks in that chunk.
Use IF with a [condition](#conditions) to test the block's own values in the same step as the write.
Add RETURNING to SET BLOCK or DELETE BLOCK to receive the block before or after the write in the same reply; see [Blocks](reference/BLOCKS.md#returns).

## Conditions

AGGREGATE and SCAN CHUNKS take `WHERE condition` and keep only the present blocks for which the condition is true.
SET BLOCK and DELETE BLOCK take `IF condition`, and SET BLOCK values may be computed from the block; see [Expressions](reference/EXPRESSIONS.md).
A table's `CHECK (condition)` refuses every write that leaves a block for which it is false; see [Tables](reference/TABLES.md#checks).

```text
condition := condition OR condition | condition AND condition | NOT condition | ( condition )
           | EXISTS | value op value | value IS [NOT] NULL
           | value [NOT] IN ( literal or $n, ... ) | value [NOT] BETWEEN value AND value | bool_column
value     := literal | $n | column | chunk.column | POS_X | POS_Y | POS_Z | NOW()
           | value + value | value - value | value * value | value / value | value % value | - value
           | LEAST(value, ...) | GREATEST(value, ...) | COALESCE(value, ...) | ( value )
op        := = | <> | < | <= | > | >=
```

- `column` is a block column and `chunk.column` a chunk column of the same chunk; quote a column named like a keyword: `"exists"`.
- `POS_X`, `POS_Y` and `POS_Z` are the block's coordinates; `NOW()`, in milliseconds since the Unix epoch, is the time the statement starts in reads and the commit time in writes.
- A literal or `$n` takes the type of the column it is compared or combined with; a parameter with no column beside it, as in `$1 < $2`, is refused.
- A float literal compared with an f32 value is rounded to the nearest f32, as an f32 `$n` would be: with an f32 column `f` holding 0.1, `f = 0.1` is true; arithmetic with a float literal computes in f64, and a finite literal beyond f32, such as `1e39`, keeps its value.
- Integers compute exactly; a result beyond 128 bits is `OUT_OF_RANGE`, and dividing an integer by zero is `INVALID_ARGUMENT`; `/` truncates toward zero and `%` takes the sign of the dividend.
- An integer with a float computes in f64, but comparisons between them are exact; NaN equals nothing, and -0 equals 0.
- Text and bytes compare bytewise; bits and bool support `=` and `<>`.
- Arithmetic with NULL is NULL; a comparison with NULL is unknown, AND, OR and NOT follow three-valued logic, and an unknown condition is false.
- LEAST and GREATEST ignore NULL arguments; COALESCE returns its first argument that is not NULL.
- For an absent block, EXISTS is false and every block column is NULL; chunk columns keep their values.
- Arithmetic chains from left to right, so `a - b - c` is `(a - b) - c`; comparisons do not chain: write `(a < b) AND (b < c)`, not `a < b < c`.
- A condition has at most 256 values, columns, operators and list items, reported as `max_expression_nodes` by HELLO, and at most 32 levels.

## Related data

chunkdb has no query planner and does not join tables.
Each statement reads or writes one table at the coordinates you name.
Lookup by value needs secondary indexes, which chunkdb does not have yet; see [Known limitations](KNOWN_LIMITATIONS.md) and keep a derived table keyed by the value instead.

### Keep data read together in one table

A transaction covers one table, so put the columns you read or change together in one table:

```cql docs-check session=main
CREATE TABLE terrain (kind u8, height i16) CHUNK 2 x 2
SET BLOCK 0 0 IN terrain kind = 1, height = 12
GET BLOCK 0 0 FROM terrain
```

```output
OK
1
kind = 1
height = 12
```

Values that belong to the whole chunk rather than to one block go in [chunk columns](reference/CHUNKS.md), which every GET CHUNK returns with the chunk's blocks:

```cql docs-check session=main
CREATE TABLE region (kind u8) CHUNK 2 x 2 CHUNK COLUMNS (biome u8 DEFAULT 0)
SET CHUNK 0 0 IN region META biome = 3
SET BLOCK 0 0 IN region kind = 1
--blocks GET CHUNK 0 0 FROM region
```

```output
OK
1
2
version = 2
schema_version = 1
commit_time_ms = <time-ms>
present = 1 of 4 blocks
chunk_columns:
  biome = 3
block 0 0
  kind = 1
```

### Read tables that share coordinates

Use a separate table when the data has its own writers, rights or feed consumers.
Give both tables the same chunk size, read the same block or area from each, and match the results by coordinates in your client:

```cql docs-check session=main
CREATE TABLE light (level u4) CHUNK 2 x 2
SET BLOCK 0 0 IN light level = 9
GET BLOCK 0 0 FROM terrain COLUMNS height
GET BLOCK 0 0 FROM light
```

```output
OK
1
height = 12
level = 9
```

The two reads do not share a snapshot, so a write between them can make the results disagree.

A transaction covers one table, so it cannot read both tables; its first table statement fixes the table and a statement on another table is refused:

```cql docs-check session=main
BEGIN
GET BLOCK 0 0 FROM terrain COLUMNS height
GET BLOCK 0 0 FROM light
ROLLBACK
```

```output
OK
height = 12
error: INVALID_ARGUMENT reason=invalid_argument a transaction covers one table: this one covers terrain
OK
```

To detect a write between the two reads, read the chunk of each table with GET CHUNK and note its `version`, then read both again.
If neither `version` changed, nothing wrote to either chunk between the passes and the first reads match one state of both tables.
Otherwise repeat the reads.
This holds only while each table keeps its epoch: SWAP TABLES, RESTORE TABLE, or DROP and CREATE start a new epoch, so also compare the `epoch` from DESCRIBE before the first pass and after the second.
A chunk that GET CHUNK returns as (null) has no version, so the check holds only for chunks present in both passes.
Rows read together from a transaction on one table are always consistent, so use one table when that matters.

### Keep a derived table up to date

A derived table holds values your client computes from another table, such as a summary or a lookup by value.
Here block `cx cy` of `terrain_top` holds the highest `height` in chunk `cx cy` of `terrain`, with the chunk version it was computed from.
Create the derived table and a [slot](CHANGE_FEED.md), fill it from SCAN CHUNKS and GET CHUNK, then apply each change the slot delivers:

```cql docs-check session=main
CREATE TABLE terrain_top (height i16, source_version u64) CHUNK 2 x 2
CREATE SLOT 'terrain_top' ON terrain
SET BLOCK 0 0 IN terrain_top height = 12, source_version = 1
```

```output
OK
OK
1
```

For each change, skip the chunk if its `source_version` is not lower than the change revision; otherwise read the chunk again, write its summary, and ACK the revision after the write succeeds.
The stored version lets a restarted consumer skip changes it already applied.
If the slot is lost, rebuild the derived table as in [Resume a consumer](FEED_RESUME.md).
