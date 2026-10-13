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
| [Tables](reference/TABLES.md) | CREATE TABLE, ALTER TABLE, DROP TABLE, SHOW TABLES, DESCRIBE, SHOW STATUS, ENABLE FEATURE |
| [Migrations](reference/MIGRATIONS.md) | MIGRATE, SHOW MIGRATIONS |
| [Transaction statements](reference/TRANSACTION_STATEMENTS.md) | BEGIN, COMMIT, ROLLBACK |
| [Feed statements](reference/FEED.md) | WATCH, ACK, UNWATCH, CREATE SLOT, DROP SLOT, SHOW SLOTS |
| [User statements](reference/USER_STATEMENTS.md) | CREATE USER, ALTER USER, DROP USER, GRANT, REVOKE, SHOW USERS |
| [Server statements](reference/SERVER.md) | PING, FLUSH WAL, SHOW METRICS, BACKUP TO |

## Names and values

See [Tables](reference/TABLES.md#arguments) for column types, identifiers, defaults, and limits.
Use integers such as `-12`, floats such as `1.5` or `2e-3`, booleans, or `NULL`.
Float values also accept `inf` and `nan`.

Quote text with single quotes and double an embedded quote: `'it''s'`.
Bytes use hexadecimal literals such as `x'0a0bff'`.
Bits use literals such as `b'1010'`, with the first digit representing the lowest bit.

Trailing keyword options can appear in any order, each at most once.
Client APIs supply binary `$n` parameters in these places: SET BLOCK values, the SET CHUNK form, `VERIFIER $1` in CREATE USER and ALTER USER, and the SCRAM messages of `HELLO 3 USER <name> $1` and `AUTH $1`.
See [Protocol](PROTOCOL.md#parameters) for encoding.

Committed chunk versions are producing commit revisions, ordered only within the table epoch returned by DESCRIBE.
DESCRIBE's schema `version` differs from its completed commit `revision`; read-only mode returns null for that frontier.
Use IF VERSION for equality checks against the containing chunk, including changes to other blocks in that chunk.
