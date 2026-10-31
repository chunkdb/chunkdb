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
| [Chunks and areas](reference/CHUNKS.md) | GET CHUNK, SET CHUNK, GET AREA, SCAN CHUNKS |
| [Tables](reference/TABLES.md) | CREATE TABLE, ALTER TABLE, DROP TABLE, RECOVER TABLE, SHOW TABLES, DESCRIBE, SHOW STATUS, ENABLE FEATURE |
| [Migrations](reference/MIGRATIONS.md) | MIGRATE, SHOW MIGRATIONS |
| [Transaction statements](reference/TRANSACTION_STATEMENTS.md) | BEGIN, COMMIT, ROLLBACK |
| [Feed statements](reference/FEED.md) | WATCH, ACK, UNWATCH, CREATE SLOT, DROP SLOT, SHOW SLOTS |
| [User statements](reference/USER_STATEMENTS.md) | CREATE USER, ALTER USER, DROP USER, GRANT, REVOKE, SHOW USERS |
| [Server statements](reference/SERVER.md) | PING, FLUSH WAL, SHOW METRICS, BACKUP TO, RELOAD TLS, SHOW CONNECTIONS, CANCEL, KILL CONNECTION |

## Names and values

See [Tables](reference/TABLES.md#arguments) for column types, identifiers, defaults, and limits.
Use integers such as `-12`, floats such as `1.5` or `2e-3`, booleans, or `NULL`.
Float values also accept `inf` and `nan`.

Quote text with single quotes and double an embedded quote: `'it''s'`.
Bytes use hexadecimal literals such as `x'0a0bff'`.
Bits use literals such as `b'1010'`, with the first digit representing the lowest bit.

Trailing keyword options can appear in any order, each at most once.
Client APIs supply binary `$n` parameters in these places: SET BLOCK values, the SET CHUNK form, `VERIFIER $1` in CREATE USER and ALTER USER, HELLO WITH option values, and the SCRAM messages of `HELLO 3 USER <name> $1` and `AUTH $1`.
See [Protocol](PROTOCOL.md#parameters) for encoding.

Committed chunk versions are producing commit revisions, ordered only within the table epoch returned by DESCRIBE.
DESCRIBE's schema `version` differs from its completed commit `revision`; read-only mode returns null for that frontier.
Use IF VERSION for equality checks against the containing chunk, including changes to other blocks in that chunk.

## Related data

chunkdb has no query planner and does not join tables.
Each statement reads or writes one table at the coordinates you name.

### Keep data read together in one table

A transaction covers one table, so put the columns you read or change together in one table:

```cql docs-check session=main
CREATE TABLE terrain (kind u8, height i16) CHUNK 2 x 2
SET BLOCK 0 0 IN terrain kind = 1, height = 12
GET BLOCK 0 0 FROM terrain
```

```output
OK
2
kind = 1
height = 12
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
2
height = 12
level = 9
```

The two reads do not share a snapshot, so a write between them can make the results disagree.

### Keep a derived table up to date

A derived table holds values your client computes from another table, such as a summary or a lookup by value.
Here block `cx cy` of `terrain_top` holds the highest `height` in chunk `cx cy` of `terrain`, with the chunk version it was computed from.
Create the derived table and a [slot](CHANGE_FEED.md), fill it from SCAN CHUNKS and GET CHUNK, then apply each change the slot delivers:

```cql docs-check session=main
CREATE TABLE terrain_top (height i16, source_version u64) CHUNK 2 x 2
CREATE SLOT 'terrain_top' ON terrain
SET BLOCK 0 0 IN terrain_top height = 12, source_version = 2
```

```output
OK
OK
2
```

For each change, skip the chunk if its `source_version` is not lower than the change revision; otherwise read the chunk again, write its summary, and ACK the revision after the write succeeds.
The stored version lets a restarted consumer skip changes it already applied.
If the slot is lost, rebuild the derived table as in [Resume a consumer](FEED_RESUME.md).
