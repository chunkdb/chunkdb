# Protocol 3 in chunkdb 2.0

A connection sends one CQL statement per line and receives one reply per statement, in request order even when requests are pipelined.
WATCH changes that connection into a push stream.
[CQL](CQL.md) defines statements; [users](USERS.md) defines rights.

## Transport and greeting

TCP uses `chunk://`; TLS uses `chunks://` with certificate validation in the client.
Lines end with LF or CRLF; a line cut off at end of stream is never executed.
The terminator counts toward `--max-line-bytes` (65536 by default); overflow sends BAD_REQUEST and closes.
Binary parameter frames do not count toward that line limit.

The first line must be HELLO 3; a different version or statement receives PROTOCOL and closes without execution.
`HELLO 3` alone is accepted only under `--auth none`; otherwise it receives AUTH_REQUIRED.
An authenticated exchange uses these grammar forms:

```text
HELLO 3 USER <name> $1    # client-first SCRAM message in parameter 1
AUTH $1                 # client-final SCRAM message in parameter 1
```

The first reply is `+SCRAM <server-first>`; AUTH success is the HELLO map below.
SCRAM-SHA-256 uses `n,,` (no channel binding), nonce/salt/iteration fields and a client proof; messages are at most 1024 bytes.
The client verifies the server-final signature before accepting the session.
Wrong passwords and unknown names receive AUTH_FAILED with the same message; repeated failures can close a connection or temporarily ban its source address.
A second HELLO receives PROTOCOL.

| HELLO map key | Value |
|---|---|
| `protocol` | Integer 3. |
| `server_version` | Bulk version string. |
| `max_line_bytes` | Configured request-line limit. |
| `max_parameters` | 65535. |
| `max_area_chunks` | 256. |
| `max_response_bytes` | 67108864 for area responses. |
| `max_scan_limit` | 1024. |
| `server_signature` | Bulk SCRAM `v=<signature>`, or null without authentication. |

## Parameters

A line using `$1` through `$n` is followed by exactly n frames, without numbering gaps or reuse.
Each frame is `$<byte length>\r\n<bytes>\r\n`; `$-1\r\n` means NULL.
These are data bytes and are never parsed as CQL text.

| Column / value | Parameter bytes |
|---|---|
| uN, iN | 8-byte little-endian integer, range checked against the column. |
| bool | One byte, 0 or 1. |
| f32, f64 | IEEE 754 little-endian, 4 or 8 bytes. |
| bits(N) | ceil(N/8) bytes, lowest bit first. |
| text, bytes | UTF-8 text or uninterpreted bytes, within the column bound. |
| SET CHUNK | Chunk form defined by [CQL](CQL.md#chunks-and-areas). |

The server bounds a frame before reading its body where the statement's schema permits it.
An oversized frame, an unparseable parameter statement, missing table or unknown parameter column closes after the error because unread bytes cannot be framed as another statement.
A bounded fixed-width value of the wrong length returns INVALID_ARGUMENT and preserves the connection.

## Reply encoding

All RESP3 line prefixes end with CRLF; aggregates are followed by their encoded elements.

| Type | Encoding | Uses |
|---|---|---|
| Simple string | `+text` | OK, PONG, applied, skipped. |
| Error | `-ERR CODE message` | Failed statement. |
| Integer | `:decimal` | Integer values, coordinates, revisions, limits. |
| Double | `,decimal`, `,inf`, `,-inf`, `,nan` | Floating values. |
| Boolean | `#t`, `#f` | Boolean values. |
| Null | `_` | Absent block, NULL value, empty commit. |
| Bulk | `$length`, bytes, CRLF | Text, bytes, packed bits, chunk forms, metrics. |
| Array | `*count` | Rows, areas, lists. |
| Map | `%pair_count` | HELLO, DESCRIBE, scans, metadata records. |
| Push | `>count` | Change, schema and resync events. |

Unsigned values may exceed the signed 64-bit range; decode column values using DESCRIBE types and revisions without signed truncation.

## WATCH and durable slots

WATCH replies `+OK <epoch> <revision>` and starts after that position; epoch is the 32-hex-digit table identity.
The following list notation describes push elements, rather than literal wire bytes:

- `[change, epoch, revision, commit_time_ms, user, schema_version, blocks]`, with each block `[x, y, before, after]`.
- `[schema, epoch, revision, version, columns]`, using DESCRIBE column maps.
- `[resync, epoch, revision]`, requesting consumer state reconstruction.

Rows are in schema order and use the reply types above; null denotes absent rows or NULL values and an anonymous user.
Coordinates beyond int64 absolute range are represented as `[chunk_coordinate, local_block_offset]` on that axis.
One transaction forms one change; AREA clips it by inclusive chunk coordinates.
Revisions have gaps and define order; timestamps do not.

Only UNWATCH, plus ACK on a slot watch, is accepted while streaming; another statement receives PROTOCOL and closes.
UNWATCH replies OK after the last push, then ordinary statements resume.
ACK has no success reply and cannot exceed the last fully sent change, independently versioned live schema event or accepted start position.
A schema preface for a change does not make that change independently acknowledgeable.
An excessive ACK receives INVALID_ARGUMENT and leaves the watch open.
Slot pushes stop at the persisted durable frontier; ACK metadata is batched per table at most every 100 ms and UNWATCH persists its own eligible ACK before replying.
SHOW SLOTS reports written `acked`, retained bytes and loss state.
Slot ownership permits one watch; a competing watch receives BUSY and a lost slot receives SLOT_LOST.
DROP TABLE ends watches with NO_TABLE; read-only/multi-process tables refuse WATCH; watches have no idle timeout.
See [change feed](CHANGE_FEED.md) for catch-up, resync, retention and consumer recovery.

## Errors and URIs

PROTOCOL, AUTH_REQUIRED and AUTH_FAILED describe greeting/authentication failures.
SYNTAX, BAD_REQUEST, INVALID_ARGUMENT and OUT_OF_RANGE describe parsing, framing, validation and response/ledger limits.
PERMISSION_DENIED identifies the missing right; NO_TABLE also hides tables on which a user has no rights.
TABLE_EXISTS, VERSION_MISMATCH and SCHEMA_MISMATCH leave rejected operations unapplied.
CONFLICT ends a transaction or identifies a reused migration name with different text; BUSY and SLOT_LOST describe resource/retention state.
INTERNAL reports operational failure; `INTERNAL write outcome unknown: ...` identifies an ambiguous write decision, and a migration outcome-unknown error identifies a failed completion after its durable decision. Both require writer recovery before retrying ([durability](DURABILITY_CONTRACT.md)).

Client URIs use `chunk://user:password@host:4242/` or `chunks://user:password@host:4242/`; percent escapes encode reserved credential characters.
A URI path selects a client's default table; wire statements still name the table explicitly.
