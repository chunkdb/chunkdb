# CQL reference for 2.0

CQL addresses typed blocks and chunks within named tables. Reads and deletes name their table after FROM; writes use IN. Keywords are case-insensitive and every table statement carries its table name. [Protocol 3](PROTOCOL.md) defines line framing, binary parameters and replies. Grammar forms below use placeholders and brackets for optional clauses.

## Names, types and values

- Keywords are case-insensitive. Column identifiers use `[a-z_][a-z0-9_]*` and are at most 63 bytes. CQL table names must also pass catalog validation: `[a-z][a-z0-9_]*`, at most 64 bytes, excluding `con`, `prn`, `aux`, `nul`, `com0`–`com9` and `lpt0`–`lpt9`.
- Column types: `uN` (unsigned, N in 1..64), `iN` (signed, N in 2..64), `bool`, `f32`, `f64`, `bits(N)` (N in 1..65535), `text(max)` (UTF-8, at most `max` bytes) and `bytes(max)`. A column may be `NULL`, `REQUIRED` (a new block must give it) and have a `DEFAULT`.
- Literals: integers `-12`, floats `1.5`, `2e-3`, `inf`, `nan`, `TRUE`, `FALSE`, `NULL`, text `'it''s'` (a quote is doubled), bytes `x'0a0bff'`, bits `b'1010'` (the first digit is the lowest bit).
- Parameters `$1` … `$n` stand for values sent after the line (numbered without gaps, each used once). They are allowed as the values of `SET BLOCK` and as the chunk of `SET CHUNK`.

## Blocks

```text
GET BLOCK x y FROM t [COLUMNS a, b]               -> *n values, or _ when the block is absent
SET BLOCK x y IN t a = v, b = $1 [IF VERSION n]   -> :<chunk version>
DELETE BLOCK x y FROM t [IF VERSION n]            -> :<chunk version>
```

- `GET BLOCK` answers the columns in schema order, or in `COLUMNS` order.
- A new block takes, for each column not given, its `DEFAULT`, else `NULL` for a `NULL` column, else zero or empty; it is refused while a `REQUIRED` column is missing. A value outside its column's type is refused and nothing changes.

## Chunks and areas

```text
GET CHUNK cx cy FROM t [COLUMNS a, b]                  -> $<chunk form>
SET CHUNK cx cy IN t $1 [IF VERSION n]                 -> :<chunk version>
GET AREA cx0 cy0 TO cx1 cy1 FROM t [COLUMNS ...]       -> *n of [cx, cy, chunk form]
GET AREA AROUND cx cy RADIUS r FROM t [COLUMNS ...]
SCAN CHUNKS FROM t [AFTER cx cy] [LIMIT n]             -> {chunks: *k of [cx, cy], more: #t|#f}
```

- Chunk coordinates count chunks, not blocks. `GET AREA` covers at most `max_area_chunks` chunks and answers only chunks with a present block, in ascending `cx` then `cy`; `AROUND` takes the chunks within `r` chunks of the centre.
- The chunk form: the chunk version (`u64` little-endian), the schema version its columns follow (`u64`, as `DESCRIBE` reports it), the presence bitmap (one bit per block, row by row, lowest bit first), the payload (per fixed-width column its values, then for a `NULL` column one validity bit per block, each padded to a byte; [packed state](STORAGE_FORMAT.md#packed-chunk-state)), then the `text` and `bytes` values as entries of `column id` (`u32`), `block index` (`u32`), `length` (`u32`) and the bytes.
- With `COLUMNS` a read sends, per named column, its part of the payload and the entries of the named `text` and `bytes` columns.
- `GET CHUNK` of a chunk without blocks answers its empty form, with its version.
- `SET CHUNK` replaces every column of the chunk, its `text` and `bytes` values included; the chunk version in the form it sends is not read. A form encoded for another schema version than the table's is refused with `-ERR SCHEMA_MISMATCH current=<v>`: read `DESCRIBE` and encode it again.
- `SCAN CHUNKS` lists the chunks that have a present block, at most `LIMIT` (default and maximum `max_scan_limit`); a next page starts `AFTER` the last chunk of the previous one while `more` is true.

## Versions and IF VERSION

- Every chunk has a version, which every change of the chunk replaces with a new value drawn from one counter of the table. A version is never issued twice, also across restarts and dropped tables; compare versions only for equality.
- Writes answer the chunk version after them. `IF VERSION n` writes only when the chunk is still at version `n`, else answers `-ERR VERSION_MISMATCH current=<v>` and changes nothing: read, compute, write `IF VERSION`, and retry on a mismatch.
- The version belongs to the whole chunk, so `IF VERSION` on a block also fails when another block of the chunk changed.
- `SET CHUNK ... IF VERSION` is a conditional write with rollback intents ([DURABILITY_CONTRACT.md](DURABILITY_CONTRACT.md)); block writes with `IF VERSION` write like ordinary writes.

## Tables

```text
CREATE TABLE t (a u10 REQUIRED, b u4 DEFAULT 15, c text(256) NULL) CHUNK 16 x 16 [LARGE 8 x 8] [WITH option = v, ...]
ALTER TABLE t ADD COLUMN d i8 NULL
ALTER TABLE t DROP COLUMN d
ALTER TABLE t RENAME COLUMN c TO label
ALTER TABLE t ALTER COLUMN b TYPE u8 [USING CLAMP | DEFAULT | TRUNCATE]
ALTER TABLE t SET option = v
DROP TABLE t
SHOW TABLES                                     -> *n names
DESCRIBE t                                      -> {table, version, columns, chunk, large, options}
```

- `CHUNK w x h` sets the blocks of a chunk; `LARGE w x h` the chunks of a large chunk (one file group on disk). Both are fixed when the table is created.
- Column changes write a new schema version at once; chunks written before convert when they load ([COLUMNS_DESIGN.md](design/COLUMNS_DESIGN.md)). `ADD COLUMN` of a `REQUIRED` column needs a `DEFAULT`; the last fixed-width column cannot be dropped.
- `ALTER COLUMN ... TYPE` stays within a family (integers, floats, `text`, `bytes`, `bits`). A type that holds every value changes at once. A narrower type checks every stored value first and names the first that does not fit; `USING CLAMP` (numbers to the nearest value), `USING DEFAULT` (the column's default) or `USING TRUNCATE` (text, bytes, bits) converts instead.
- `DESCRIBE` answers the schema version, per column `id` (the column id that `text` and `bytes` values in a chunk form carry; never reused within a table), `name`, `type`, `null`, `required`, `default`, the `chunk` and `large` sizes as `[w, h]`, and the options.
- Options: `durability_mode` (`'relaxed'`, `'fsync-wal'`, `'fsync-checkpoint'`), `checkpoint_updates`, `checkpoint_wal_bytes`, `wal_group_commit_updates`, `checkpoint_compression`, `var_max_chunk_bytes` (the most bytes of `text` and `bytes` values in one chunk, default 1 MiB). Their meaning is in [SERVER_FLAGS.md](SERVER_FLAGS.md).

## Named migrations

```text
MIGRATE 'world_table' CREATE TABLE world (kind u8) CHUNK 16 x 16 -> +applied | +skipped
MIGRATE 'world_label' ALTER TABLE world ADD COLUMN label text(128) NULL
SHOW MIGRATIONS                                 -> *n of {name, applied_ms, user, statement}
```

Run the same list at every application start.
Each name records one `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `GRANT`, `REVOKE`, `CREATE SLOT` or `DROP SLOT` statement.
Names are quoted `[a-z_][a-z0-9_]*`, 1–63 bytes, as for slot names.
The inner statement's rights apply; migrations cannot run inside a transaction.
Migrations require a read-write server with single-process writer locking; `--allow-multi-process` is not supported.
A repeated name with the same statement text returns `skipped`; different text returns `-ERR CONFLICT` naming the migration.
This conflict reveals that a name was already used to anyone with the current rights to run the submitted inner statement, even without `MANAGES USERS`.
Separating and trailing spaces/tabs are removed; keyword case and whitespace inside the inner statement remain part of its identity.
Concurrent requests for a name wait for the first request and then compare their text.
`SHOW MIGRATIONS` requires `MANAGES USERS` and lists completed steps in applied order; `--auth none` permits it without users.
Records survive restart together with their schema changes; storage and crash recovery are described in [STORAGE_FORMAT.md](STORAGE_FORMAT.md).
The ledger holds at most 16384 records and 16 MiB. A new step that exceeds either limit returns `OUT_OF_RANGE` naming the limit; existing names still return `skipped` or `CONFLICT` after their rights checks.
An I/O failure after the durable migration decision has an unknown outcome and requires a writer restart before further commands; retry the same named step after restart.

Dropping a table removes grants on that specific table. A user with only per-table `ADMIN` therefore gets `NO_TABLE` when rerunning earlier steps for a table dropped later, even though those steps completed. Run a repeatable list that drops tables with a deployment user holding `ADMIN` on `*`, and give application users their per-table grants separately. For an existing deployment user, a user with `MANAGES USERS` can grant:

```text
GRANT ADMIN ON * TO deploy
```

## Transactions

```text
BEGIN                                           -> +OK
COMMIT                                          -> :<version>, or _ when nothing was written
ROLLBACK                                        -> +OK
```

Between `BEGIN` and `COMMIT` the block, chunk and area statements of one table read one snapshot, and their writes answer `_` and apply together at `COMMIT`, or not at all with `-ERR CONFLICT <reason>` ([TRANSACTIONS.md](TRANSACTIONS.md)).

## Users

`CREATE USER`, `ALTER USER`, `DROP USER`, `GRANT`, `REVOKE` and `SHOW USERS`, and the right each statement needs, are in [USERS.md](USERS.md).

## Server

```text
PING                                            -> +PONG
FLUSH WAL                                       -> +OK when every write acknowledged before is durable
SHOW METRICS                                    -> $<Prometheus text>
BACKUP TO 'snapshot'                               -> {tables, files, bytes, cuts}
```

`BACKUP TO` takes a relative destination under the server's `--backup-dir` and needs `MANAGES USERS`. It returns
per-table `{table, epoch, revision}` cuts; another backup receives `BUSY`. See
[BACKUP.md](BACKUP.md) for destination requirements, verification and restore.

## Change feed

```text
CREATE SLOT 'name' ON t                          -> +OK
DROP SLOT 'name' ON t                            -> +OK
SHOW SLOTS [ON t]                                -> *n of {table, name, epoch, acked, retained_bytes, lost}
WATCH t [SLOT 'name'] [AREA cx0 cy0 TO cx1 cy1] [AFTER epoch revision]
ACK revision                                    -> no reply on success, within a slot watch
UNWATCH                                         -> +OK, then ordinary statements resume
```

WATCH streams committed changes; a named slot retains durable history across
restarts. AREA uses chunk coordinates. Slot names are quoted
`[a-z_][a-z0-9_]*`, 1–63 bytes. CREATE/DROP SLOT require ADMIN on the table;
WATCH requires READ. SHOW SLOTS lists only tables the user has a right on;
`acked` is the position written to disk, and `lost` marks a retention limit loss.
See [CHANGE_FEED.md](CHANGE_FEED.md) for acknowledgement, resume, limits and
resynchronization.
