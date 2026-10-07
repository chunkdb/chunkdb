# CQL (#62): Design

Status: proposal for review. The language itself is decided in the 2.0 plan (#69): verb, object, coordinates, table after `FROM` or `IN`, options as keywords at the end, SQL forms where SQL has one, no `USE`, text in single quotes, bytes as `x'..'`, client values as parameters. This document is the wire form and the exact grammar.

## Protocol 3

- A connection starts with `HELLO 3 [AUTH <token>]`; the reply is a map of the server's version and limits. Users and passwords replace the token in #63.
- Protocol 2 commands go away in the same release; clients move to CQL (chunkdb/chunkdb-js#5, chunkdb/chunkdb-go#6, chunkdb/chunk-cli#6).
- One statement per request line, UTF-8, at most `max_line_bytes`. Keywords are case-insensitive; table and column names are `[a-z_][a-z0-9_]*`.

## Values

- Literals: integers (`-12`), floats (`1.5`, `2e-3`, `inf`, `nan`), `TRUE`, `FALSE`, `NULL`, text `'it''s'`, bytes `x'0a0bff'`, bits `b'1010'` (the first digit is the lowest bit).
- Parameters: `$1` … `$n` in the statement; the line is then followed by `n` frames `$<len>\r\n<bytes>\r\n` (or `$-1\r\n` for `NULL`), in order. The bytes are the value in its column's binary form: `uN` and `iN` 8 bytes little-endian, `bool` 1 byte, `f32` and `f64` IEEE 754 little-endian, `bits(N)` `ceil(N / 8)` bytes lowest bit first, `text` UTF-8, `bytes` as they are. The server knows each parameter's column before it reads the frame, so it bounds the length and refuses a frame that cannot be framed safely, as `CHUNKPUT` does today.
- A parameter is never parsed as part of the statement, so a client cannot turn user input into a command.

## Replies

- RESP2 framing as today (`+OK`, `-ERR <CODE> <message>`, `$` bulk, `*` arrays) plus RESP3 types for values: `:` integer, `,` double, `#t`/`#f`, `_` null, `%` map.
- `text`, `bytes` and `bits` values are bulk strings in their binary form; a client reads them by the column types from `DESCRIBE`.

## Statements in 2.0

```text
GET BLOCK x y FROM t [COLUMNS a, b]                 -> array of values (schema order or COLUMNS order), _ when absent
SET BLOCK x y IN t a = v, b = $1 [IF VERSION n]     -> chunk version
DELETE BLOCK x y FROM t [IF VERSION n]              -> chunk version
GET CHUNK cx cy FROM t [COLUMNS a, b]               -> bulk: chunk form (below), _ when absent
SET CHUNK cx cy IN t $1 [IF VERSION n]              -> chunk version
GET AREA x0 y0 TO x1 y1 FROM t [COLUMNS ...]        -> array of [cx, cy, chunk form]
GET AREA AROUND x y RADIUS r FROM t [COLUMNS ...]
CREATE TABLE t (a u10 REQUIRED, b u4 DEFAULT 15, c text(256) NULL) CHUNK 16 x 16 [LARGE 8 x 8] [WITH option = value, ...]
ALTER TABLE t ADD COLUMN ... | DROP COLUMN a | RENAME COLUMN a TO b | ALTER COLUMN a TYPE u8 [USING CLAMP | DEFAULT | TRUNCATE] | SET option = value
DROP TABLE t | SHOW TABLES | DESCRIBE t | FLUSH WAL | SHOW METRICS
```

- `ALTER COLUMN ... TYPE` without `USING` widens at once or, when the type is narrower, runs the check of every stored value (`TableCatalog::NarrowColumn`).
- `IF VERSION` is the chunk version of today's conditional writes, with the same meaning in every statement.
- `WATCH`/`ACK` (#65), `CREATE USER`/`GRANT`/`REVOKE` (#63) and `BEGIN`/`COMMIT`/`ROLLBACK` (#64) come with their steps.

## Chunk form

- `GET CHUNK` and `SET CHUNK` share one binary form: `version` (`u64`), the presence bitmap, then for each column (schema order, or `COLUMNS` order on reads) its values as in the chunk payload (values, then validity bits for a `NULL` column; docs/STORAGE_FORMAT.md Section 2), then the VARS entries of the `text` and `bytes` columns included.
- `SET CHUNK` takes every column; a block it creates takes the values it gives, so whole-chunk writes carry `text` and `bytes` values too (they cannot today).
- `ZRLE` stays an option of the chunk statements.

## Steps (one PR each)

1. Lexer and parser for the statements above, literals and parameter frames; unit tests, nothing wired.
2. Protocol 3: `HELLO 3`, the block statements and typed replies over the engine; the server bench drives them.
3. Chunk and area statements with the chunk form, and whole-chunk writes with values in the store.
4. DDL: `CREATE`/`ALTER`/`DROP TABLE`, `SHOW TABLES`, `DESCRIBE`, options.
5. Protocol 2 removed; docs; the clients.
