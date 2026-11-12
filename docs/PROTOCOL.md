# Protocol in chunkdb

A connection sends one CQL statement per line and receives one reply per statement, in request order even when requests are pipelined.
WATCH changes that connection into a push stream.
[CQL](CQL.md) defines statements; [users](USERS.md) defines rights.

## Transport and greeting

TCP uses `chunk://`; TLS uses `chunks://` with certificate validation in the client.
Lines end with LF or CRLF; a line cut off at end of stream is never executed.
The terminator counts toward `--max-line-bytes` (65536 by default); overflow sends BAD_REQUEST and closes.
Binary parameter frames do not count toward that line limit.
A PROXY protocol v2 header and the TLS handshake each have their own `--client-io-timeout-ms`, in that order, before the HELLO deadline starts.
Until authentication succeeds, HELLO, AUTH and all their parameter frames must arrive within `--client-io-timeout-ms` of that start; later ones receive PROTOCOL and close.
After authentication, each request line must arrive within `--client-io-timeout-ms` of its first byte, each frame header within it of the end of the line or of the previous frame, and each frame body, kept or drained, within it of the end of its header; a frame that misses its deadline receives `PROTOCOL incomplete parameter frame` and the connection closes.
Waiting for the server's input credit does not count: a request's frame deadline starts again when the credit is granted, and a wait longer than `--client-io-timeout-ms` receives `BUSY reason=input_budget` and the connection closes.

The first line must be HELLO 3; a different version or statement receives PROTOCOL and closes without execution.
`HELLO 3` alone is accepted only under `--auth none`; otherwise it receives AUTH_REQUIRED.
An authenticated exchange uses these grammar forms:

```text
HELLO 3 USER <name> $1    # client-first SCRAM message in parameter 1
AUTH $1                 # client-final SCRAM message in parameter 1
```

The first reply is `+SCRAM <server-first>`; AUTH success is the HELLO map below.
SCRAM-SHA-256 uses a GS2 header, nonce/salt/iteration fields and a client proof; messages are at most 1024 bytes.
The client-first message starts with `n,,` for a login that is not bound, or with `p=tls-exporter,,` or `p=tls-server-end-point,,` for a login bound to its TLS connection (SCRAM-SHA-256-PLUS).
The client-final message of a bound login carries `c=` with the base64 of that header followed by the channel binding data: the 32-byte TLS exporter of RFC 9266 for `tls-exporter`, the server certificate hash of RFC 5929 for `tls-server-end-point`.
`tls-exporter` needs TLS 1.3; `tls-server-end-point` works on TLS 1.2 and 1.3 unless the signature algorithm of the server certificate defines no single hash, as Ed25519 does not.
A binding the connection cannot give receives INVALID_ARGUMENT: any type on a connection whose TLS does not end at the server, `tls-exporter` below TLS 1.3, or an unknown type; `y,,` receives it on a TLS connection.
Binding data that differs from what the server sees receives `AUTH_FAILED reason=channel_binding`: something between the client and the server ends TLS.
With `--auth-channel-binding require` a login that is not bound receives `AUTH_REQUIRED reason=channel_binding`; an unbound login of a user that requires binding fails like a wrong password.
The client verifies the server-final signature before accepting the session.
Wrong passwords and unknown names receive AUTH_FAILED with the same message.
A connection closes after 5 failed HELLO or AUTH attempts.
A source address is an IPv4 address or an IPv6 /64.
After 5 AUTH_FAILED replies to one source address, that address is banned for 1 second.
When a ban ends, the address's failures are reset, so it again has 5 failures before the next ban.
A banned address receives `RATE_LIMITED reason=auth_ban retry_after_ms=...` and its connection closes; the reply that starts a ban and replies during a ban are delayed by 50 ms.
A successful login from the address clears only the failures of its own user name; failures for other names, existing or not, stay counted.
An address tracks failures separately for at most 8 names at a time; failures for further names are cleared by no login.

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

An idle authenticated connection keeps its session and open transaction until its next request or until it closes.

Append `WITH client_name='name',statement_timeout_ms=0,default_tag=x'00'` to HELLO to request session options.
Keys are case-insensitive; duplicates and invalid known values receive INVALID_ARGUMENT without counting as authentication failures.
`statement_timeout_ms` accepts an unsigned integer from zero through 86400000, including an eight-byte little-endian parameter.
Unknown keys are accepted and reported with `reason=unknown_option`.
Before authentication HELLO takes at most 12 parameters; a HELLO with more receives PROTOCOL and closes without its frames being read.

`client_name` and `statement_timeout_ms` apply; `default_tag` is stored with `reason=implementation_pending`.
The effective statement timeout appears in `options`, including the server default when HELLO omits it.
The implemented capabilities are `protocol_foundation`, `ordered_versions`, `read_metadata`, `feed_progress`, `slot_filters`, `slot_takeover`, `lineage`, `connection_controls`, `statement_timeout`, and `channel_binding`.
Options become visible only after authentication succeeds.
Returning and expression ceilings describe reserved limits; their capabilities remain unavailable.

The statement timeout starts after the complete request is framed and includes queue wait.
`CANCELLED` and statement `TIMEOUT` carry `applied=false` and mean that the statement did not apply.
An operation admitted to its irreversible decision completes with its real result.
Transport timeouts remain independent and close the connection.
Large chunk and area replies retain their schema, projection and immutable state while bounded output continuations drain.
Output progress renews the reply deadline; a stalled writer closes the connection.

See [session negotiation](design/PROTOCOL_FOUNDATION.md#handshake-and-session-ownership-a2) for the complete option and limit contract.

## Session controls

`SHOW CONNECTIONS` returns an array of maps containing `id`, `user`, `peer`, `state`, and `statement`.
An unauthenticated user or an idle statement is represented by NULL in its field.
The reply includes every authorized connection or fails with `OUT_OF_RANGE reason=response_limit`; it never truncates the listing.
Users control sessions belonging to their current user incarnation; ADMIN on `*` may control every session.

`CANCEL id` returns `{cancelled: bool}` and accepts cancellation only before the target statement enters its irreversible decision.
Missing, idle, decision-entered and self targets return false.
An accepted cancellation ends that statement with `CANCELLED applied=false` and leaves the connection available for its next request.
`KILL CONNECTION id` returns `{killed: bool}` and closes the target after any decision-entered statement completes.
Self-KILL sends its reply before closing.
KILL closes a pending read-only reply immediately and releases its retained state.
WATCH has completed its initiating statement, so CANCEL returns false and KILL releases its subscription and slot ownership.

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
Collection retains empty chunks' producing versions and times in checked tombstones.
Never-written chunks and identities forgotten by tombstone compaction have null versions.
Schema versions remain known for absent rows and empty areas; area consistency otherwise remains unchanged.
Coordinate tuples contain exactly `dims` signed values: x and y, then z only in a three-dimensional table.
SCAN CHUNKS returns a map with `chunks`, `more`, `epoch`, nullable completed `position`, and `retained_deletion_floor`.
Its rows are `[coordinates..., chunk_version]`, with a form-or-null appended under WITH DATA.
SINCE includes retained deletions and refuses a revision below the deletion floor with `RESYNC_REQUIRED reason=history_lost floor=... epoch=...`, adding `current` when known.

## Chunk form

All integers inside a form are little-endian.
The fixed 36-byte header is `CKCF`, `form_version:u16`, `flags:u16`, `chunk_version:u64`, `schema_version:u64`, `commit_time_ms:u64`, `section_count:u16`, and zero `reserved:u16`.
The form format tag is 1; flag 1 means projected block columns, and flag 2 means transaction-private state with zero producing version and time.
Each of at most 64 directory entries is `kind:u16`, zero `flags:u16`, `offset:u32`, and `length:u32`.
Kinds are strictly increasing, and section bodies are contiguous after the directory and fill the bulk value exactly.

| Kind | Body |
| --- | --- |
| 1 Presence | One bit per block, lowest bit first, padded to a byte. |
| 2 Fixed values | Consecutive packed fixed-column arrays described by section 6. |
| 3 Null validity | One bit per block for each nullable fixed column, using section 6 offsets. |
| 4 Variable values | Entries `column_id:u32, block_index:u32, length:u32, bytes:length`. |
| 5 Chunk columns | Complete checked one-row metadata encoding. |
| 6 Block spans | `count:u32`, then fixed-column descriptors. |

A block-span descriptor is six `u32` fields: stable column ID, width in bits, values offset, values length, validity offset, and validity length.
Offsets are relative to the corresponding fixed or validity body; a nonnullable descriptor has zero validity offset and length.
Descriptors follow the requested projection or schema order and cover each packed body exactly.
The local block index is `x + width * (y + height * z)`; inactive z is zero.
Padding bits are zero, and absent blocks contain no retained values.
Chunk-column bodies start with `count:u32` and a directory of `column_id:u32, null_flags:u16, reserved_zero:u16, offset:u32, length:u32` entries in stable-ID order.
Flag 1 means null; all other flag bits are invalid.
Offsets are relative to the metadata body, and values follow the complete directory contiguously.
Scalar value bytes use the parameter encodings above; null values have no bytes.

Read clients validate the complete directory and may retain unknown sections or fixed types as opaque data.
SET CHUNK refuses unknown sections and requires exactly the current schema and requested projection.
A full write replaces both domains; a projected write preserves chunk metadata and unnamed continuing block values.
The server ignores supplied producing version and time and assigns its own committed identity.
SCHEMA_MISMATCH reports a different supplied schema revision.
GET CHUNK is null only when no producing identity remains; a tombstone produces an empty form with its retained identity.

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

- `[change, epoch, revision, commit_time_ms, user, schema_version, blocks, tag, txn, chunk_changes]`, with each block `[x, y, before, after]` or `[x, y, z, before, after]`.
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
Each chunk is `[cx,cy,version,form_or_null]` or `[cx,cy,cz,version,form_or_null]`; a null form represents deletion.
Forms match GET CHUNK for that exact commit and projection; later writes cannot change queued bytes.
Every nonnull form retains that chunk's producing version and time, which can precede the enclosing feed entry.
Each `chunk_changes` row contains the active chunk coordinates followed by complete before and after metadata rows.
A null before row means no prior written identity, and NO BEFORE removes both block and metadata before rows.
Block projections preserve all metadata columns; empty metadata domains emit no metadata-change rows.
Tag and txn are null until their metadata is present.

A slot permits one watch at a time; a competing watch receives SLOT_BUSY and a lost slot receives SLOT_LOST.
A slot belongs to its owner; another user needs ADMIN on `*` to watch it, see [feed statements](reference/FEED.md#arguments).

DROP TABLE ends watches with NO_TABLE; a fenced or recovered table ends them with INTERNAL, and a new WATCH resumes; watches have no idle timeout.
Losing READ on the table ends a watch with a best-effort NO_TABLE, as losing ADMIN on `*` ends a watch of a slot its user does not own, and a dropped user with a best-effort `AUTH_REQUIRED reason=user_dropped`; the connection closes either way; see [users](USERS.md#rights).
See [change feed](CHANGE_FEED.md) for catch-up, resync, retention and consumer recovery.

## Errors and URIs

PROTOCOL, AUTH_REQUIRED and AUTH_FAILED describe greeting/authentication failures.
`AUTH_REQUIRED reason=user_dropped` answers a statement on a connection whose user was dropped, and the connection closes.
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
SHOW SLOTS adds `lost_reason` (null while healthy), a `filters` map, and `owner` (shown to that user and to MANAGES USERS; null otherwise or for a slot without a recorded owner).
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
The replaced watch receives best-effort `KILLED reason=slot_takeover`; partial or pinned TLS output closes without an injected error.
Concurrent handoffs receive `SLOT_BUSY reason=takeover_pending`.

An ambiguous durable-slot metadata publication reports `OUTCOME_UNKNOWN stage=slot_records` and fences table writes until recovery.
Takeover propagates this outcome to the replacement instead of installing a new watch after failed ACK publication.
