# CREATE TABLE, ALTER TABLE, DROP TABLE, RECOVER TABLE, SWAP TABLES, SHOW TABLES, DESCRIBE

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
ALTER TABLE schema_demo ALTER COLUMN label SET DEFAULT 'none'
ALTER TABLE schema_demo ADD CHECK (kind < 1000)
ALTER TABLE schema_demo DROP CHECK kind_check
ALTER TABLE schema_demo SET var_max_chunk_bytes = 2097152
ALTER TABLE schema_demo DROP COLUMN light
ALTER TABLE schema_demo RENAME TO schema_renamed
DROP TABLE schema_renamed
```

```output
OK
1) schema_demo
table = schema_demo
version = 1
chunk = 2 x 2
large = 8 x 8
epoch = <epoch>
revision = 0
dims = 2
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
OK
OK
OK
OK
```

SHOW TABLES also lists any other tables you have created.

## Syntax

```text
CREATE TABLE [IF NOT EXISTS] table (column type [NULL] [REQUIRED] [DEFAULT value] [CHECK [name] (condition) ...], ...)
  [CHUNK width x height [x depth]] [LARGE width x height [x depth]]
  [CHUNK COLUMNS (column type [NULL] [REQUIRED] [DEFAULT value], ...)] [CHECK [name] (condition) ...] [WITH option = value, ...]
ALTER TABLE table ADD COLUMN [IF NOT EXISTS] column type [NULL] [REQUIRED] [DEFAULT value]
ALTER TABLE table DROP COLUMN [IF EXISTS] column
ALTER TABLE table RENAME COLUMN column TO name
ALTER TABLE table ADD CHUNK COLUMN [IF NOT EXISTS] column type [NULL] [REQUIRED] [DEFAULT value]
ALTER TABLE table DROP CHUNK COLUMN [IF EXISTS] column
ALTER TABLE table RENAME CHUNK COLUMN column TO name
ALTER TABLE table ALTER COLUMN column TYPE type [USING CLAMP | DEFAULT | TRUNCATE]
ALTER TABLE table ALTER COLUMN column SET DEFAULT value | DROP DEFAULT
ALTER TABLE table ALTER COLUMN column SET NULL | DROP NULL | SET REQUIRED | DROP REQUIRED
ALTER TABLE table ADD CHECK [name] (condition)
ALTER TABLE table DROP CHECK [IF EXISTS] name
ALTER TABLE table SET option = value
ALTER TABLE table RENAME TO name
DROP TABLE [IF EXISTS] table
RECOVER TABLE table
SWAP TABLES table, table
SHOW TABLES
DESCRIBE table
SHOW STATUS
ENABLE FEATURE 'name'
```

## Arguments

- `table`: `[a-z][a-z0-9_]*`, at most 64 bytes; reserved device names are excluded below.
- `column`: `[a-z_][a-z0-9_]*`, at most 63 bytes.
- `NULL`: the column accepts a missing value, distinct from an absent block.
- `REQUIRED`: new blocks must provide this column unless it has a default.
- `DEFAULT`: the value used for an omitted column in a new block.
- `CHUNK`: blocks per chunk, default `16 x 16`; a third side selects a three-dimensional table, and geometry is fixed after creation.
- `LARGE`: chunks per disk file group, default 8 on each active axis; its dimensions must match CHUNK.
- `CHUNK COLUMNS`: one metadata row per chunk, with an independent column namespace and stable IDs.
- `IF NOT EXISTS` and `IF EXISTS`: skip an existing or absent object without comparing its definition; usual rights still apply.
- `USING CLAMP`: narrow numeric values to the nearest supported value.
- `USING DEFAULT`: replace values that do not fit with the column's default.
- `USING TRUNCATE`: shorten text, bytes, or bits to the new bound.
- `SET DEFAULT` and `DROP DEFAULT`: change the value new blocks take for an omitted column; `SET DEFAULT NULL` drops the DEFAULT of a NULL column.
- `SET NULL` and `DROP NULL`: let the column hold NULL, or no longer; DROP NULL first checks every stored value.
- `SET REQUIRED` and `DROP REQUIRED`: whether new blocks must give the column.
- `RENAME TO`: the table's new name, under the same rules as `table`.
- `CHECK`: a [condition](../CQL.md#conditions) every present block must not make false; `name` follows the `column` rules.

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
CREATE TABLE, ALTER TABLE ... RENAME TO and RESTORE TABLE also refuse the name `catalog`, which [WATCH CATALOG](FEED.md#watch-catalog) reserves; a table of that name made before keeps working but cannot be watched.

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
- RECOVER TABLE: `OK` once the table serves again, or is gone because its directory is.
- SWAP TABLES: `OK` once each name serves the other table.
- RENAME TO: `OK` once the new name serves the table.
- SHOW TABLES: accessible table names, fenced tables included.
- DESCRIBE: schema version, column definitions, geometry, options, table epoch, completed revision, dimensions, and chunk columns; a table with checks adds `checks`, a list of maps with `name` and `expression`; a fenced table adds `fenced` with its reason.
- SHOW STATUS: `version`, the `server_version` HELLO reports, `uptime_ms` since the server started, `read_only`, `directory` features, `tables`, `fenced`, and `activation_pending`; each `tables` entry has `name`, `state`, `reason`, `enabled`, `compat`, `ro_compat`, and `incompat`, and each `fenced` entry has `name` and `reason`.

The `version` field describes the schema; `revision` describes the completed commit frontier within `epoch`.
The frontier can include terminal gaps from abandoned reservations and recovery.
Read-only servers return null for revision because they cannot establish the writer's completed frontier.
A fenced table also returns null for revision.
The `dims` field is 2 or 3 and determines coordinate and geometry tuple lengths.
The `chunk_columns` list uses the same column-description maps as `columns`.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| TABLE_EXISTS | A table already exists. | Use another name, IF NOT EXISTS, [MIGRATE](MIGRATIONS.md), or DROP TABLE. |
| NO_TABLE | A table is absent or inaccessible. | Check its name and rights. |
| PERMISSION_DENIED | You lack ADMIN on the table, on both tables for SWAP, or for CREATE and RENAME TO on the new name, a matching prefix, or all tables. | Request access. |
| INVALID_ARGUMENT | A name, type, default, conversion, or option is invalid, or RECOVER TABLE names a table that is not fenced. | Correct the reported argument. |
| INTERNAL | The table is fenced: `table 'name' is fenced: reason; run RECOVER TABLE name`. | Run RECOVER TABLE. |
| INVALID_ARGUMENT | RECOVER TABLE finds stored options that differ from the server's option flags. | Restart the server without those flags, or with the stored values. |
| INTERNAL | A migration with an unknown outcome fences every table. | Restart the server; see [Migrations](MIGRATIONS.md#errors). |
| INVALID_ARGUMENT | You run CREATE, ALTER, DROP, RECOVER TABLE, SWAP TABLES or ENABLE FEATURE on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| INVALID_ARGUMENT | SWAP TABLES names one table twice. | Name two tables. |
| INVALID_ARGUMENT | DROP NULL finds a NULL: `column c cannot drop NULL: block (x, y) holds NULL`. | Set a value in that block, then run DROP NULL again. |
| INVALID_ARGUMENT | An ALTER COLUMN change leaves the column as it is, or makes it both NULL and REQUIRED. | Drop the other attribute first. |
| CHECK_FAILED | A write leaves a block for which a check is false, or ADD CHECK finds a stored one: `block (x, y) fails CHECK name (condition)`, with `check=name`. | Change the values, or drop the check. |
| INVALID_ARGUMENT | A check names an unknown column, is not a condition, or uses parameters, chunk columns, NOW() or EXISTS; DROP COLUMN or ALTER COLUMN TYPE ... USING names a column a check reads. | Correct the condition, or drop the check first. |
| TABLE_EXISTS | RENAME TO names a table that exists, or a directory under `tables/` that no table of this server uses. | Use another name, or DROP TABLE that table first. |
| INTERNAL | SWAP TABLES on Windows or a file system without an atomic directory exchange; both tables keep their names. | Swap on Linux or macOS, or restore under the live name after DROP TABLE. |
| CONFLICT | An active transaction sees a schema change. | Retry from BEGIN. |

## Notes

Changing columns immediately publishes a new schema version; older chunks convert when loaded.
Types can change only within their family: integers, floats, text, bytes, or bits.
Narrowing without USING checks every stored value and refuses the first value that does not fit.
DROP NULL checks every stored value the same way and refuses the first NULL with its block.
While either check runs, writes that the change would refuse are refused already; a restart or RECOVER TABLE drops an interrupted check.
A table restored from a backup taken during a check does not have the check, and keeps the column as it was before it.
SET DEFAULT and DROP DEFAULT change only blocks created later; existing blocks keep their values.
SET REQUIRED applies to new blocks only: existing blocks of a column that is not NULL always hold a value.
The embedded bit-string calls (`ChunkStore::GetBlockBits` and the like) need a table whose one column is bits(N) and not NULL: SET NULL on that column, like ADD COLUMN, makes them refuse, and DROP NULL brings them back.
You can add REQUIRED without DEFAULT only when no block is present.
For chunk columns, adding REQUIRED without DEFAULT requires no written chunk identity, including retained tombstones.
Block and chunk columns share the table's schema version; changing either domain advances it once.
Chunk metadata uses DEFAULT, otherwise NULL for nullable columns, otherwise zero or empty as its baseline.
A chunk with metadata differing from that baseline is populated even when it has no blocks.

At least one fixed-width column must remain.
A text-only application can use `kind u1 DEFAULT 0` alongside its text column.
DROP TABLE deletes its data, durable slots, and table-specific grants.
Use [Migrations](MIGRATIONS.md) to record repeatable schema changes.

SWAP TABLES exchanges two tables' names in one atomic step; data, epochs, lineage and durable slots stay with their tables, and grants stay with the names.
It waits for running statements and backups on both tables and makes their acknowledged writes durable first.
CANCEL, KILL CONNECTION or a statement timeout stops it only before the exchange, and both tables then keep their names.
Open transactions on either table answer CONFLICT, and watches of either table end with INTERNAL `watched table was swapped; watch it again`.
A consumer that watches the name again sees another epoch, so it resumes by [lineage](../CHANGE_FEED.md) or rebuilds.
Each slot of either table is then reached by another name, so its next watch without AFTER starts with resync.
A failure after the exchange fences the names it leaves unserved until RECOVER TABLE.

RENAME TO gives a table a new name in one atomic step; data, epoch, lineage and durable slots stay with the table, and the grants on its name move to the new name; prefix and `*` grants stay as they are.
A user with grants on both names keeps the higher right; the old name keeps no grants and is free for a new table.
It waits for running statements and backups on the table and makes their acknowledged writes durable first.
CANCEL, KILL CONNECTION or a statement timeout stops it only before the rename.
Open transactions on the table answer CONFLICT, and watches end with INTERNAL `watched table was renamed; watch it again`.
A consumer watches the new name and resumes in the same epoch; slots keep their positions.
GRANT and REVOKE wait for a running RENAME TO, so each applies before or after it.
A crash during RENAME TO leaves the table under one of the two names; the grants then may name both, and REVOKE removes those no longer wanted.
A failure to make the rename durable fences both names until RECOVER TABLE of each, and the grants may then name both too.
A failure to open the renamed table fences the new name until RECOVER TABLE; the old name is free and keeps no grants.
RENAME TO is not taken inside [MIGRATE](MIGRATIONS.md).

A failure that leaves a table out of service fences it, while other tables keep serving.
Statements on that table return INTERNAL with the fenced message and its reason.
A store that is fail-closed after a durability failure or a failed snapshot generation fences its table too: it still serves reads, and changes return INTERNAL while the server log holds the reason.
A failed feed cleanup fences its table, whose statements return INTERNAL `feed cleanup failed`.
A column narrowing whose last manifest change fails before it is written fences its table; a refused narrowing still returns its refusal.
DESCRIBE of a fenced table shows the definition it served last; the change that fenced it may already be in its files.
RECOVER TABLE does for one fenced table what a server start does for every table.
It waits for the table's running statements and running backups, closes it, removes its interrupted creates and drops, drops an interrupted narrowing, DROP NULL or ADD CHECK, and opens it from its files.
Watches of a table that leaves service end with INTERNAL; watch it again after RECOVER TABLE to resume.
A table whose recovery fails stays fenced and out of service; a restart also recovers it.
A migration with an unknown outcome fences every table; RECOVER TABLE refuses it, and only a restart completes it.

## Checks

```text
CREATE TABLE units (hp i16 CHECK (hp >= 0), max_hp i16 NULL) CHECK hp_max (hp <= max_hp)
ALTER TABLE units ADD CHECK (hp < 10000)
ALTER TABLE units DROP CHECK hp_max
```

A check refuses a block only when its condition is false: NULL (unknown) passes, as in SQL, unlike WHERE; write `hp IS NOT NULL` or use DROP NULL to refuse NULL.
It reads the block's columns and its position (`pos_x`, `pos_y`, `pos_z`); parameters, chunk columns, NOW() and EXISTS are refused.
An unnamed check is named `column_check` after its column, or after the first column it reads (`check` when it reads none), with `_2`, `_3` and so on when that name is taken.

Every write that leaves a block present checks it as written: SET BLOCK with the columns it keeps and a new block's defaults, IF VERSION and IF ABSENT writes, SET CHUNK whole or with COLUMNS for every present block of the result, and MIGRATE.
In a transaction, a SET BLOCK or SET CHUNK that fails a check is refused at the statement, and COMMIT checks again every block it changes.
DELETE always passes; a check that cannot be evaluated, such as one dividing by zero, refuses the write with INVALID_ARGUMENT or OUT_OF_RANGE naming the check and the block.
A table without checks evaluates nothing when it is written.
A DEFAULT that fails a check is accepted by the schema change and refused only at the first write that gives a new block that DEFAULT.

ADD CHECK evaluates every stored block as DROP NULL checks values, and refuses with CHECK_FAILED for the first block that fails it or cannot evaluate it.
While it runs, writes must pass the new check too; a restart or RECOVER TABLE drops an interrupted ADD CHECK.
ADD CHECK and DROP CHECK each publish a schema version, and every version keeps the checks it had.
RENAME COLUMN renames the column in the checks too.
DROP COLUMN and ALTER COLUMN TYPE ... USING refuse a column a check reads; a type change without USING keeps the checks when they still type-check.
ADD COLUMN takes no CHECK: add the column, then ADD CHECK.

## Limits

A table has at most 1024 columns and 65535 fixed-width value bits per block.
A table has at most 64 checks; a check's condition has at most 16384 bytes and the limits of a WHERE condition.
Text and bytes maxima range from 1 through 16777216 bytes.
The variable-value budget includes 12 bytes of metadata per value.
The var_max_chunk_bytes option ranges from 13 through 67108864 bytes.
After reducing this limit, existing oversized chunks can still shrink.
There is no configured table-count limit; storage and memory still bound the count.
Each active chunk dimension is 1 through 4096 blocks; their product is at most 1048576 blocks.

Large-chunk dimensions are 1 through 1000000 chunks.
Fixed chunk payload is at most 67108864 bytes, including nullable-column validity bits.
See [Server flags](../SERVER_FLAGS.md#table-geometry).

## Feature activation

`SHOW STATUS` requires MONITOR or ADMIN on `*` and reports recorded directory and table features.
`tables` lists the tables whose features can be read; a table's `state` is `ok`, or `fenced` with the fence reason.
`fenced` lists, with the fence reason, the fenced tables whose features cannot be read: those whose store is not open or whose feed cleanup failed.
Its protected snapshot refuses unhealthy serving state instead of guessing whether activation completed.
`ENABLE FEATURE 'name'` requires ADMIN on `*` and returns OK for an already enabled feature.

New directories enable `feed_slots`, `migrations`, `table_lineage`, `slot_filters`, `feature_activation`, and `chunk_model`.
Existing directories retain their recorded bits; enable `feature_activation` before another missing feature.
Activation validates every table before recording its durable decision, then updates only that feature's registered owners.
A fenced table refuses activation with its fenced message until RECOVER TABLE.
An incomplete published activation fences serving until writer startup completes recovery.

| Error | Meaning |
|---|---|
| INVALID_ARGUMENT reason=unknown_feature | The feature name is not registered. |
| INVALID_ARGUMENT reason=feature_required | Enable the prerequisite named by `feature` first. |
| INVALID_ARGUMENT reason=feature_disabled | The command needs explicit activation of the named feature. |
| OUTCOME_UNKNOWN stage=feature_activation | Publication may have happened; restart to finish recovery. |
