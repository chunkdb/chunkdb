# Protocol in chunkdb

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
Wrong passwords and unknown names receive AUTH_FAILED with the same message.
A connection closes after 5 failed HELLO or AUTH attempts.
A source address is an IPv4 address or an IPv6 /64.
After 5 AUTH_FAILED replies to one source address, that address is banned for 1 second, and each further failure from it bans it again.
A banned address receives `RATE_LIMITED reason=auth_ban retry_after_ms=...` and its connection closes; the reply that starts a ban and replies during a ban are delayed by 50 ms.
A successful login from the address clears its failures.

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
| `connection_id` | Nonzero unsigned process-local connection identifier. |
| `features` | Array of supported protocol capability names. |
| `options` | Map of applied options and effective values. |
| `not_applied` | Map of requested option names to maps containing `reason`. |
| `idle_timeout_ms` | Configured idle timeout in milliseconds. |
| `limits` | Map of area, transaction, watch, returning and expression ceilings. |

Append `WITH client_name='name',statement_timeout_ms=0,default_tag=x'00'` to HELLO to request session options.
Keys are case-insensitive; duplicates and invalid known values receive INVALID_ARGUMENT without counting as authentication failures.
Unknown keys are accepted and reported with `reason=unknown_option`.

Only `client_name` currently applies; timeout and tag values are stored with `reason=implementation_pending` until their capabilities appear.
The implemented capabilities are `protocol_foundation`, `ordered_versions`, `read_metadata`, `feed_progress`, `slot_filters`, `slot_takeover`, and `lineage`.
Options become visible only after authentication succeeds.
Returning and expression ceilings describe reserved limits; their capabilities remain unavailable.

See [session negotiation](design/PROTOCOL_FOUNDATION.md#handshake-and-session-ownership-a2) for the complete option and limit contract.

## Parameters

A lexical scan counts `$n` tokens outside quoted literals and identifiers, independently of whether the statement parses.
That count determines the number of following frames.
Valid statements use unique indices from `$1` through `$n`; frames bind by numeric index even when references appear out of order.
Each frame is `$<byte length>\r\n<bytes>\r\n`; `$-1\r\n` means NULL.
These are data bytes and are never parsed as CQL text.

| Column / value | Parameter bytes |
|---|---|
| uN, iN | 8-byte little-endian integer, range checked against the column. |
| bool | One byte, 0 or 1. |
| f32, f64 | IEEE 754 little-endian, 4 or 8 bytes. |
| bits(N) | ceil(N/8) bytes, lowest bit first. |
| text, bytes | UTF-8 text or uninterpreted bytes, within the column bound. |
| SET CHUNK | Chunk form defined by [Chunk form](#chunk-form). |

The server bounds a frame before reading its body where the statement's schema permits it.
For SET BLOCK, it also bounds the sum of actual declared frame lengths at 67117056 bytes. Each header is checked before allocating or reading its payload; small values remain accepted when their columns allow larger maxima. NULL frames add no payload bytes. SET CHUNK retains its separate chunk-form bound.
An oversized typed frame or aggregate closes the connection after its error.
Invalid statements, missing tables and unknown parameter columns drain their frames under `--max-unparsed-parameter-bytes`, default 1048576 payload bytes.
Before HELLO, a non-handshake statement or wrong protocol version receives PROTOCOL and closes without waiting for advertised frames.
Recoverable HELLO syntax and option errors drain their frames before replying, so a later handshake remains correctly framed.
Repeated or gapped indices still determine the drain count by token occurrence; quoted `$n` text does not count.
After draining, SYNTAX, UNKNOWN_COMMAND or the semantic error leaves the connection usable.
Malformed framing or an excessive discard size receives PROTOCOL and closes; unread payload bytes never become statements.

A bounded fixed-width value of the wrong length returns INVALID_ARGUMENT and preserves the connection.

## Read metadata

DESCRIBE adds `epoch`, `revision`, `dims`, and `chunk_columns` to its existing map.
`version` remains the schema version; `revision` is the completed commit frontier within the 32-hex-digit table epoch.
Read-only mode encodes an unknown frontier as null; read-write mode returns an unsigned integer.
DESCRIBE of a fenced table adds `fenced` with its reason and encodes its frontier as null.
The frontier includes terminal reservation gaps but never crosses an unfinished producer.

| GET clauses | Encoded value |
| --- | --- |
| BLOCK without metadata | Row array, or null. |
| BLOCK WITH VERSION | `[chunk_version, row_or_null]`. |
| BLOCK WITH SCHEMA | `[schema_version, row_or_null]`. |
| BLOCK with both | `[chunk_version, schema_version, row_or_null]`, regardless of clause order. |
| AREA WITH SCHEMA | `[schema_version, entries]`. |

Versions and rows share the chunk read or transaction snapshot boundary; schemas remain pinned by the table lease.
Transaction-private modified chunks have null versions until committed.
A block's absent row can have a version when its containing chunk retains a committed version.
Never-written and collected empty chunks have null versions when no committed version remains retained.
Schema versions remain known for absent rows and empty areas; area consistency otherwise remains unchanged.

## Chunk form

A complete chunk form starts with its chunk version and schema version, each an unsigned 64-bit little-endian integer.
Next comes the presence bitmap, one bit per block in row order, lowest bit first.
Fixed-width column payloads follow, with one validity bit per block for nullable columns, each padded to a byte.
See [Packed chunk state](STORAGE_FORMAT.md#packed-chunk-state) for the payload layout.

Text and bytes follow as entries containing column id, block index, and length, each unsigned 32-bit little-endian, then value bytes.
A COLUMNS projection contains only those columns' payload and variable entries.
SET CHUNK requires the complete form and ignores its supplied chunk version.
Its schema version must match DESCRIBE; otherwise SCHEMA_MISMATCH reports the current schema version.

## Reply encoding

All RESP3 line prefixes end with CRLF; aggregates are followed by their encoded elements.

| Type | Encoding | Uses |
|---|---|---|
| Simple string | `+text` | OK, PONG, applied, skipped. |
| Error | `-ERR CODE key=value ... message` | Failed statement. |
| Integer | `:decimal` | Signed int64 values and unsigned values within that range. |
| Big number | `(decimal` | Exact integers outside the signed int64 range. |
| Double | `,decimal`, `,inf`, `,-inf`, `,nan` | Floating values. |
| Boolean | `#t`, `#f` | Boolean values. |
| Null | `_` | Absent block, unwritten chunk, NULL value, empty commit. |
| Bulk | `$length`, bytes, CRLF | Text, bytes, packed bits, chunk forms, metrics. |
| Array | `*count` | Rows, areas, lists. |
| Map | `%pair_count` | HELLO, DESCRIBE, scans, metadata records. |
| Push | `>count` | Change, schema and resync events. |

Unsigned values may exceed signed int64 and use big numbers; decode values and metadata without signed truncation or floating-point rounding.

## WATCH and durable slots

WATCH replies with a map containing `epoch` and `revision`, and starts after that position.
Epoch is the 32-hex-digit table identity.
The following list notation describes push elements, rather than literal wire bytes:

- `[change, epoch, revision, commit_time_ms, user, schema_version, blocks, tag, txn, chunk_changes]`, with each block `[x, y, before, after]`.
- `[schema, {epoch, revision, schema_version, dims, columns, chunk_columns}]`, using DESCRIBE column maps.
- `[resync, epoch, revision, parent_epoch, fork_revision]`, requesting consumer state reconstruction.
- `[progress, epoch, revision]`, marking a sender-drained cut during idle or filtered periods.

Current change frames reserve `tag=null`, `txn=null`, and `chunk_changes=[]`; schema maps reserve `chunk_columns=[]`.
Resync lineage fields are null when no parent lineage is recorded.

Rows are in schema order and use the reply types above; null denotes absent rows or NULL values and an anonymous user.
Coordinates beyond int64 absolute range are represented as `[chunk_coordinate, local_block_offset]` on that axis.
One transaction forms one change; AREA clips it by inclusive chunk coordinates.
Revisions have gaps and define order; timestamps do not.
AFTER can resume at a progress revision within the retained prefix, including a gap without a change frame.

Only UNWATCH, plus ACK on a slot watch, is accepted while streaming; another statement receives PROTOCOL and closes.
UNWATCH replies OK after the last push, then ordinary statements resume. An ordinary subscription is removed and its configured linger policy takes effect before OK.
ACK has no success reply and cannot exceed the last fully sent change, progress push or accepted start position.
Schema and resync pushes never advance its bound.
Slot progress cannot pass the durable frontier or changes not yet queued ahead of it.

An excessive ACK receives INVALID_ARGUMENT and leaves the watch open.
Slot pushes stop at the persisted durable frontier; see [durable slots](reference/FEED.md#notes) for ACK persistence and batching.
SHOW SLOTS reports written `acked`, retained bytes and loss state.
CHUNKS pushes use `[chunks,epoch,revision,commit_time_ms,user,schema_version,chunks,tag,txn]`.
Each chunk is `[cx,cy,version,form_or_null]`; a null form represents deletion.
Forms match GET CHUNK for that exact commit and projection; later writes cannot change queued bytes.
Tag and txn are null until their metadata is present.

Slot ownership permits one watch; a competing watch receives SLOT_BUSY and a lost slot receives SLOT_LOST.

DROP TABLE ends watches with NO_TABLE; a fenced or recovered table ends them with INTERNAL, and a new WATCH resumes; watches have no idle timeout.
See [change feed](CHANGE_FEED.md) for catch-up, resync, retention and consumer recovery.

## Errors and URIs

PROTOCOL, AUTH_REQUIRED and AUTH_FAILED describe greeting/authentication failures.
UNKNOWN_COMMAND identifies an unavailable statement.
SYNTAX, INVALID_ARGUMENT and OUT_OF_RANGE describe parsing, validation and response/ledger limits.
Malformed or unbounded parameter framing receives PROTOCOL and closes the connection.
INVALID_ARGUMENT also refuses a statement the server configuration rules out, such as a write on a server started with `--read-only`.
PERMISSION_DENIED identifies the missing right; NO_TABLE also hides tables on which a user has no rights.

TABLE_EXISTS, VERSION_MISMATCH and SCHEMA_MISMATCH leave rejected operations unapplied.
Errors contain a code, structured keys, and optional diagnostic text.
Keys precede text; percent escapes encode whitespace, `%`, `=`, and non-ASCII bytes in values.
Every error includes `reason`; additional keys identify versions, options, publication stages, or retry delays.
Clients classify errors by code, never diagnostic text, and preserve unknown keys.

| Retry class | Codes | Action |
| --- | --- | --- |
| reread | VERSION_MISMATCH, CONDITION_FAILED, SCHEMA_MISMATCH | Refresh the version or schema before retrying. |
| rerun | CONFLICT | Restart the whole transaction. |
| backoff | BUSY, DISK_FULL, RATE_LIMITED | Resolve load, free space, or wait for `retry_after_ms`. |
| unknown_outcome | OUTCOME_UNKNOWN | Inspect recovery state; never retry automatically. |
| fatal | All other and unknown codes | Correct the request or resolve the reported condition. |

MIGRATION_MISMATCH identifies different text under a recorded migration name.
SLOT_BUSY identifies an attached slot; SLOT_LOST identifies unavailable retained history or a dropped projected column.
`reason=schema_filter` identifies a lost stable-ID projection; `reason=retention_limit` identifies retained-history overflow.
SHOW SLOTS adds `lost_reason` (null while healthy) and a `filters` map.
Its keys are `area`, `columns`, `column_ids`, `no_before`, and `chunks`.

Unfiltered area and projection fields are null; projected names and IDs are aligned arrays in declared order.
A dropped column keeps its ID and has a null display name.
Area bounds are signed `[x0,y0,x1,y1]` with inclusive endpoints; both flags are booleans.
VERSION_MISMATCH and SCHEMA_MISMATCH include `current`; schema rejection also includes `actual` and `table`.
RATE_LIMITED authentication bans include remaining `retry_after_ms`.
OUTCOME_UNKNOWN includes `stage` and requires writer recovery before retrying ([durability](DURABILITY_CONTRACT.md)).
INTERNAL reports other operational failures without exposing internal diagnostics.

```text
-ERR SCHEMA_MISMATCH current=2 actual=1 table=world reason=schema_mismatch the chunk was encoded for schema version 1
```


Client URIs use `chunk://user:password@host:4242/` or `chunks://user:password@host:4242/`; percent escapes encode reserved credential characters.
A URI path selects a client's default table; wire statements still name the table explicitly.

Slot takeover fences old ACK validation, persists the accepted ACK, and closes the old socket before publishing its replacement claim.
The old owner receives best-effort `KILLED reason=slot_takeover`; partial or pinned TLS output closes without an injected error.
Concurrent handoffs receive `SLOT_BUSY reason=takeover_pending`.

An ambiguous durable-slot metadata publication reports `OUTCOME_UNKNOWN stage=slot_records` and fences table writes until recovery.
Takeover propagates this outcome to the replacement instead of installing a new owner after failed ACK publication.
