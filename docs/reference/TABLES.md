# CREATE TABLE, ALTER TABLE, DROP TABLE, SHOW TABLES, DESCRIBE

You define a table's columns, inspect its schema, and change or remove it.
A schema defines the names, types, defaults, and nullability of its columns.

## Examples

These examples create `schema_demo`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE schema_demo (kind u8, name text(16) NULL) CHUNK 2 x 2
SHOW TABLES
DESCRIBE schema_demo
ALTER TABLE schema_demo ADD COLUMN light u4 DEFAULT 15
ALTER TABLE schema_demo RENAME COLUMN name TO label
ALTER TABLE schema_demo ALTER COLUMN kind TYPE u16
ALTER TABLE schema_demo SET var_max_chunk_bytes = 2097152
ALTER TABLE schema_demo DROP COLUMN light
DROP TABLE schema_demo
```

```output
OK
1) schema_demo
table = schema_demo
version = 1
chunk = 2 x 2
large = 8 x 8
columns:
  kind u8
  name text(16) NULL
options:
  durability_mode = relaxed
  checkpoint_updates = 256
  checkpoint_wal_bytes = 1048576
  wal_group_commit_updates = 8
  checkpoint_compression = none
  var_max_chunk_bytes = 1048576
  feed_buffer_bytes = 67108864
  slot_max_bytes = 1073741824
OK
OK
OK
OK
OK
OK
```

SHOW TABLES also lists any other tables you have created.

## Syntax

```text
CREATE TABLE [IF NOT EXISTS] table (column type [NULL] [REQUIRED] [DEFAULT value], ...)
  [CHUNK width x height] [LARGE width x height] [WITH option = value, ...]
ALTER TABLE table ADD COLUMN [IF NOT EXISTS] column type [NULL] [REQUIRED] [DEFAULT value]
ALTER TABLE table DROP COLUMN [IF EXISTS] column
ALTER TABLE table RENAME COLUMN column TO name
ALTER TABLE table ALTER COLUMN column TYPE type [USING CLAMP | DEFAULT | TRUNCATE]
ALTER TABLE table SET option = value
DROP TABLE [IF EXISTS] table
SHOW TABLES
DESCRIBE table
```

## Arguments

- `table`: `[a-z][a-z0-9_]*`, at most 64 bytes; reserved device names are excluded below.
- `column`: `[a-z_][a-z0-9_]*`, at most 63 bytes.
- `NULL`: the column accepts a missing value, distinct from an absent block.
- `REQUIRED`: new blocks must provide this column unless it has a default.
- `DEFAULT`: the value used for an omitted column in a new block.
- `CHUNK`: blocks per chunk, default `16 x 16`; fixed after creation.
- `LARGE`: chunks per disk file group, default `8 x 8`; fixed after creation.
- `IF NOT EXISTS` and `IF EXISTS`: skip an existing or absent object without comparing its definition; usual rights still apply.
- `USING CLAMP`: narrow numeric values to the nearest supported value.
- `USING DEFAULT`: replace values that do not fit with the column's default.
- `USING TRUNCATE`: shorten text, bytes, or bits to the new bound.

| Type | Values |
| --- | --- |
| `uN` | Unsigned integers, N from 1 through 64 |
| `iN` | Signed integers, N from 2 through 64 |
| `bool` | TRUE or FALSE |
| `f32`, `f64` | Floating-point values, including inf and nan |
| `bits(N)` | Exactly N binary digits, N from 1 through 65535 |
| `text(max)` | UTF-8 text with at most max bytes |
| `bytes(max)` | Uninterpreted bytes with at most max bytes |

Reserved table names are `con`, `prn`, `aux`, `nul`, `com0` through `com9`, and `lpt0` through `lpt9`.

## Options

| Option | Meaning |
| --- | --- |
| `durability_mode` | `'relaxed'`, `'fsync-wal'`, or `'fsync-checkpoint'`; see [Durability](../DURABILITY_CONTRACT.md). |
| `checkpoint_updates`, `checkpoint_wal_bytes` | Updates or WAL bytes that trigger checkpoints. |
| `wal_group_commit_updates` | Writes per WAL batch. |
| `checkpoint_compression` | `'none'` or `'zrle'` for new images. |
| `var_max_chunk_bytes` | Encoded text and bytes per chunk; default 1048576 bytes. |
| `feed_buffer_bytes` | Live feed history and output; default 67108864 bytes. |
| `slot_max_bytes` | Retained history per durable slot; default 1073741824 bytes. |

Feed and slot limits must be positive.
Unset limits inherit server defaults each time the table opens; explicit table values persist across restarts.
DESCRIBE reports effective limits; changing another option preserves whether a limit inherits.
See [Server flags](../SERVER_FLAGS.md#persisted-table-options) for option defaults and ranges.

## Returns

- CREATE, ALTER, and DROP: `OK`, including authorized conditional no-ops.
- SHOW TABLES: accessible table names.
- DESCRIBE: schema version, column definitions, chunk and large geometry, and effective options.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| TABLE_EXISTS | A table already exists. | Inspect it or use IF NOT EXISTS. |
| NO_TABLE | A table is absent or inaccessible. | Check its name and rights. |
| PERMISSION_DENIED | You lack ADMIN on the table, or on all tables for CREATE. | Request access. |
| INVALID_ARGUMENT | A name, type, default, conversion, or option is invalid. | Correct the reported argument. |
| INVALID_ARGUMENT | You run CREATE, ALTER or DROP TABLE on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| CONFLICT | An active transaction sees a schema change. | Retry from BEGIN. |

## Notes

Changing columns immediately publishes a new schema version; older chunks convert when loaded.
Types can change only within their family: integers, floats, text, bytes, or bits.
Narrowing without USING checks every stored value and refuses the first value that does not fit.
You can add REQUIRED without DEFAULT only when no block is present.

At least one fixed-width column must remain.
A text-only application can use `kind u1 DEFAULT 0` alongside its text column.
DROP TABLE deletes its data, durable slots, and table-specific grants.
Use [Migrations](MIGRATIONS.md) to record repeatable schema changes.

## Limits

A table has at most 1024 columns and 65535 fixed-width value bits per block.
Text and bytes maxima range from 1 through 16777216 bytes.
The variable-value budget includes 12 bytes of metadata per value.
The var_max_chunk_bytes option ranges from 13 through 67108864 bytes.
After reducing this limit, existing oversized chunks can still shrink.
There is no configured table-count limit; storage and memory still bound the count.
Chunk dimensions are 1 through 4096 blocks; their product is at most 1048576 blocks.

Large-chunk dimensions are 1 through 1000000 chunks.
Fixed chunk payload is at most 67108864 bytes, including nullable-column validity bits.
See [Server flags](../SERVER_FLAGS.md#table-geometry).
