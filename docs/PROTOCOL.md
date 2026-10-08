# chunkdb protocol (protocol 3)

A client sends one CQL statement per line and reads one reply per statement. The statements are in [CQL.md](CQL.md); this page is the connection, the framing and the replies.

## Transport

- TCP, optionally TLS (`chunks://`).
- A line ends with `\r\n` or `\n`; a line cut off by the end of the stream is never executed.
- A line, terminator included, is at most `max_line_bytes` (`--max-line-bytes`, default 65536); a longer one gets `-ERR BAD_REQUEST` and the connection closes. Parameter frames are not part of the line.
- Requests may be pipelined: replies come back in request order.

## Handshake

`HELLO` must be the first line of a connection. It logs in a user with SCRAM-SHA-256 (RFC 5802, RFC 7677; users and rights in [USERS.md](USERS.md)): the password never crosses the network, and the server proves it holds the user's verifier.

```text
> HELLO 3 USER <name> $1          $1 = client-first message   n,,n=<name>,r=<client nonce>
< +SCRAM <server-first message>                              r=<nonce>,s=<salt>,i=<iterations>
> AUTH $1                         $1 = client-final message   c=biws,r=<nonce>,p=<proof>
< %8 ...                          the map below; server_signature = v=<signature>
```

- The SCRAM messages are parameter frames (at most 1024 bytes). Channel binding is not used (`n,,`); TLS protects the connection.
- The client checks `server_signature` against the one it computes; a mismatch means the server does not hold the user's verifier.
- A wrong password and an unknown user both get `-ERR AUTH_FAILED invalid user or password` after `AUTH`. Failures count per connection (`--max-auth-failures`) and per source address, which is banned for a while after too many.
- `HELLO 3` alone logs in only on a server started with `--auth none`; elsewhere it gets `-ERR AUTH_REQUIRED`.
- Any other first line, or another protocol version, gets `-ERR PROTOCOL expected HELLO 3` and the connection closes. A chunkdb server of the earlier protocol answers `HELLO 3` with `-ERR PROTOCOL expected HELLO 2`.
- A second `HELLO` gets `-ERR PROTOCOL`.
- The reply is a map of the server's limits:

| Key | Value |
|---|---|
| `protocol` | `3` |
| `server_version` | the server's version string |
| `max_line_bytes` | the longest request line |
| `max_parameters` | the most `$n` parameters in one statement (65535) |
| `max_area_chunks` | the most chunks one `GET AREA` covers (256) |
| `max_response_bytes` | the largest `GET AREA` reply (64 MiB) |
| `max_scan_limit` | the largest `SCAN CHUNKS ... LIMIT` (1024) |
| `server_signature` | the SCRAM server-final message, or `_` without a user |

## Parameters

A statement may carry values as `$1` … `$n` instead of literals (see [CQL.md](CQL.md)). The line is then followed by `n` frames, in order:

```text
SET BLOCK 10 4 IN world sign = $1, chest = $2\r\n
$5\r\nhello\r\n
$-1\r\n
```

- A frame is `$<length>\r\n<bytes>\r\n`, or `$-1\r\n` for `NULL`.
- The bytes are the value in its column's binary form: `uN` and `iN` 8 bytes little-endian, `bool` 1 byte (0 or 1), `f32` and `f64` IEEE 754 little-endian, `bits(N)` `(N + 7) / 8` bytes with the lowest bit first, `text` UTF-8, `bytes` as they are, a chunk its chunk form.
- The server reads the line first and bounds each frame by its column. When it cannot, the frames are not read and the connection closes after the error, since the bytes that follow could not be told apart from the next statement: a frame longer than its column holds (`-ERR BAD_REQUEST`), a line with `$` that does not parse (`-ERR SYNTAX`), a table that does not exist (`-ERR NO_TABLE`) or a column it does not have (`-ERR INVALID_ARGUMENT`). A fixed-width value of the wrong size is an ordinary `-ERR INVALID_ARGUMENT`, and the connection stays.
- A parameter is never parsed as part of the statement, so user input passed as a parameter cannot become a command.

## Replies

Replies use RESP3 types:

| Type | Form | Used for |
|---|---|---|
| simple string | `+OK`, `+PONG` | statements without a value |
| error | `-ERR <CODE> <message>` | every failure |
| integer | `:<n>` | `uN`, `iN` values, chunk versions, coordinates |
| double | `,<n>` (also `,inf`, `,-inf`, `,nan`) | `f32`, `f64` values |
| boolean | `#t`, `#f` | `bool` values |
| null | `_` | `NULL`, an absent block |
| bulk string | `$<length>\r\n<bytes>\r\n` | `text`, `bytes`, `bits` values, chunk forms, metrics |
| array | `*<n>` then n replies | rows, areas, lists |
| map | `%<n>` then n key/value pairs | `HELLO`, `DESCRIBE`, `SCAN CHUNKS` |

A `uN` value above the `i64` range is written as it is; a client reads values by the column types `DESCRIBE` reports.

## Errors

- `PROTOCOL`: no `HELLO 3` yet, another protocol version, or a second `HELLO`.
- `AUTH_REQUIRED`, `AUTH_FAILED`.
- `PERMISSION_DENIED <right> on <table>`: the user lacks the right the statement needs ([USERS.md](USERS.md)).
- `SYNTAX`: the statement does not parse; the message names the column of the first token that does not fit.
- `INVALID_ARGUMENT`: a value, column, option or size the statement cannot take.
- `OUT_OF_RANGE`: a reply would exceed `max_response_bytes`.
- `VERSION_MISMATCH current=<v>`: `IF VERSION` did not match; nothing changed.
- `SCHEMA_MISMATCH current=<v>`: a chunk form was encoded for another schema version than the table's; nothing changed.
- `NO_TABLE`, `TABLE_EXISTS`.
- `BAD_REQUEST`: the request cannot be framed; the connection closes.
- `BUSY`: the server has no room for the connection.
- `INTERNAL`. After a write, `-ERR INTERNAL write outcome unknown: ...` means the write may or may not be applied and the table is fail-closed until the server restarts; any other error after a write means it was not applied.

## URI

- `chunk://user:password@host:4242/` and, with TLS, `chunks://user:password@host:4242/`; `%XX` escapes let a password hold `:`, `@` or `/`. Clients log in with them as above.
- A path (`chunk://host:4242/terrain`) is the client's default table; statements still name their table on the wire.

## Example

```text
> HELLO 3 USER bot $1   (+ client-first frame)
< +SCRAM r=...,s=...,i=4096
> AUTH $1               (+ client-final frame)
< %8 ... (protocol 3, server_version, limits, server_signature)
> SET BLOCK 10 4 IN world id = 23, light = 7
< :1043
> GET BLOCK 10 4 FROM world COLUMNS id, light
< *2 :23 :7
> GET BLOCK 11 4 FROM world
< _
> SET BLOCK 10 4 IN world light = 8 IF VERSION 1042
< -ERR VERSION_MISMATCH current=1043
```
