# Watching table changes

WATCH streams committed changes of one table, including the complete before and
after rows of changed blocks. Changes are ordered by revision; a transaction
produces one change. Commit timestamps may move backwards; use revisions to order.
The feed is in memory and follows the table's write visibility and durability mode.

```text
WATCH t [AREA cx0 cy0 TO cx1 cy1] [AFTER epoch revision]
UNWATCH
```

AREA is an inclusive rectangle of **chunk** coordinates. A transaction spanning
its boundary is clipped to the blocks inside it. WATCH requires READ on the table;
a table without that right is reported as nonexistent.

After logging in ([USERS.md](USERS.md)), use a dedicated connection:

```text
> WATCH world AREA 0 0 TO 3 3
< +OK 0123456789abcdef0123456789abcdef 1042
< > [change, epoch, 1043, time_ms, user, schema_version, [[x, y, before, after]]]
< > [schema, epoch, revision, schema_version, columns]
> UNWATCH
< +OK
> PING
< +PONG
```

The example abbreviates RESP3 frames as lists. Exact framing and types are in
[PROTOCOL.md](PROTOCOL.md). Rows use schema order; `_` denotes an absent block or
NULL value. Anonymous writes have `_` as their user. Schema frames contain the
same column descriptions as DESCRIBE. An absolute coordinate outside int64 is
sent as `[chunk_coordinate, local_block_offset]` for that axis.

`+OK epoch revision` is the position the stream starts **after**. Without AFTER,
WATCH starts after all currently completed writes. AFTER resumes from a position
still retained while a feed is active. Only UNWATCH is accepted while watching;
any other statement causes a PROTOCOL error and closes the connection. UNWATCH's
reply follows the last queued push. A dropped table ends WATCH with NO_TABLE.

## Resynchronizing

`> [resync, epoch, revision]` means the feed cannot continue from your position.
Every revision through this frontier has finished. Keep reading the stream while
using another connection to DESCRIBE and re-read state with GET AREA or SCAN
CHUNKS followed by GET CHUNK. Apply later changes to a chunk only when their
revision exceeds the version of the chunk you read. Replace local state for the
area you scanned, including blocks/chunks that disappeared. Retain the frontier
with the rebuilt state before applying subsequent changes.

Expect resync after a restart, when the last watch was closed, for an unknown
epoch/position, or when data was evicted or too large for the buffer. READ-only
and multi-process tables refuse WATCH. Watches have no idle timeout and do not
occupy statement workers.

## Limits

`--feed-buffer-bytes` defaults to 64 MiB per table, shared by raw queues, decoded
entries and encoded frames. The last watch releases the table's feed. Each watch
also has an equal share of that table budget for unsent bytes; a slow reader gets
resync. An already started frame is completed before resync so framing stays valid.
`--max-watches` defaults to 64 per server; additional watches receive BUSY.
