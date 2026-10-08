# CQL (#62): Design

The language follows the 2.0 plan (#69): verb, object, coordinates, table after `FROM` or `IN`, options as keywords at the end, SQL forms where SQL has one, no `USE`, text in single quotes, bytes as `x'..'`, client values as parameters. This document is the wire form and the exact grammar.

## Protocol 3

- A connection starts with `HELLO 3 [AUTH <token>]`; the reply is a map of `protocol`, `server_version`, `max_line_bytes`, `max_parameters`, `max_area_chunks` and `max_response_bytes`. Users and passwords replace the token in #63.
- Protocol 2 commands go away in the same release; clients move to CQL (chunkdb/chunkdb-js#5, chunkdb/chunkdb-go#6, chunkdb/chunk-cli#6).
- One statement per request line, UTF-8, at most `max_line_bytes`. Keywords are case-insensitive; table and column names are `[a-z_][a-z0-9_]*`.

## Values

- Literals: integers (`-12`), floats (`1.5`, `2e-3`, `inf`, `nan`), `TRUE`, `FALSE`, `NULL`, text `'it''s'`, bytes `x'0a0bff'`, bits `b'1010'` (the first digit is the lowest bit).
- Parameters: `$1` … `$n` in the statement; the line is then followed by `n` frames `$<len>\r\n<bytes>\r\n` (or `$-1\r\n` for `NULL`), in order. The bytes are the value in its column's binary form: `uN` and `iN` 8 bytes little-endian, `bool` 1 byte, `f32` and `f64` IEEE 754 little-endian, `bits(N)` `ceil(N / 8)` bytes lowest bit first, `text` UTF-8, `bytes` as they are. The server knows each parameter's column before it reads the frame, so it bounds the length and refuses a frame that cannot be framed safely, as `CHUNKPUT` does today.
- A parameter is never parsed as part of the statement, so a client cannot turn user input into a command.
- Parameters are numbered `$1` to `$n` without gaps (at most 65535), each used once, and are values of `SET BLOCK` and `SET CHUNK` only. A frame longer than its column holds, or a line with `$` that does not parse, is refused unread and closes the connection: the bytes that follow cannot be framed. A wrong-sized frame for a fixed-width column is an ordinary `INVALID_ARGUMENT`.

## Replies

- RESP2 framing as today (`+OK`, `-ERR <CODE> <message>`, `$` bulk, `*` arrays) plus RESP3 types for values: `:` integer, `,` double, `#t`/`#f`, `_` null, `%` map.
- `text`, `bytes` and `bits` values are bulk strings in their binary form; a client reads them by the column types from `DESCRIBE`. `uN` values above the `i64` range are written as they are.
- A statement that does not parse gets `-ERR SYNTAX column <n>: <what>`, naming the first token that does not fit.

## Statements in 2.0

```text
GET BLOCK x y FROM t [COLUMNS a, b]                 -> array of values (schema order or COLUMNS order), _ when absent
SET BLOCK x y IN t a = v, b = $1 [IF VERSION n]     -> chunk version
DELETE BLOCK x y FROM t [IF VERSION n]              -> chunk version
GET CHUNK cx cy FROM t [COLUMNS a, b]               -> bulk: chunk form (below), _ when absent
SET CHUNK cx cy IN t $1 [IF VERSION n]              -> chunk version
GET AREA cx0 cy0 TO cx1 cy1 FROM t [COLUMNS ...]    -> array of [cx, cy, chunk form]
GET AREA AROUND cx cy RADIUS r FROM t [COLUMNS ...]
CREATE TABLE t (a u10 REQUIRED, b u4 DEFAULT 15, c text(256) NULL) CHUNK 16 x 16 [LARGE 8 x 8] [WITH option = value, ...]
ALTER TABLE t ADD COLUMN ... | DROP COLUMN a | RENAME COLUMN a TO b | ALTER COLUMN a TYPE u8 [USING CLAMP | DEFAULT | TRUNCATE] | SET option = value
DROP TABLE t | SHOW TABLES | DESCRIBE t | FLUSH WAL | SHOW METRICS
```

- `ALTER COLUMN ... TYPE` without `USING` widens at once or, when the type is narrower, runs the check of every stored value (`TableCatalog::NarrowColumn`).
- `IF VERSION` is the chunk version of today's conditional writes, with the same meaning in every statement. Block statements check it under the chunk lock and write on the ordinary path; `SET CHUNK ... IF VERSION` writes on the conditional path with rollback intents, as `CHUNKPUT IF` does (docs/DURABILITY_CONTRACT.md).
- `GET AREA` takes chunk coordinates and a radius in chunks, as `CHUNKRANGE` and `CHUNKRADIUS` do, within `max_area_chunks` and `max_response_bytes`.
- `DESCRIBE` answers a map of `table`, `version` (schema version), `columns` (per column `name`, `type`, `null`, `required`, `default`), `chunk` and `large` as `[w, h]`, and `options` by the names `TABLEINFO` prints; `SHOW TABLES` an array of names; the other table statements `+OK`.
- `WATCH`/`ACK` (change feed, #65), `CREATE USER`/`GRANT`/`REVOKE` (users and rights, #63) and `BEGIN`/`COMMIT`/`ROLLBACK` (transactions, #64) are added with those features.

## Chunk form

- `GET CHUNK` and `SET CHUNK` share one binary form: `version` (`u64`), the presence bitmap, then for each column (schema order, or `COLUMNS` order on reads) its values as in the chunk payload (values, then validity bits for a `NULL` column; docs/STORAGE_FORMAT.md Section 2), then the VARS entries of the `text` and `bytes` columns included.
- With `COLUMNS` a read sends, per named column, its section of the payload and the VARS entries of the named `text` and `bytes` columns.
- `SET CHUNK` takes every column; a block it creates takes the values it gives, so whole-chunk writes carry `text` and `bytes` values too (they cannot today). The version in the form it sends is not read: `IF VERSION` is the condition.
- `ZRLE` is not part of the 2.0 chunk statements; it can come back as an additive option.
