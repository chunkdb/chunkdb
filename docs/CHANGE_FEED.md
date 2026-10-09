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

Expect resync after a restart, when the last watch was closed and no durable slot
keeps the feed active, for an unknown
epoch/position, or when data was evicted or too large for the buffer. READ-only
and multi-process tables refuse WATCH. Watches have no idle timeout and do not
occupy statement workers.

## Limits

`--feed-buffer-bytes` defaults to 64 MiB per table, shared by raw queues, decoded
entries and encoded frames. The last watch releases the table's feed unless a
durable C++ slot keeps it active. Each watch
also has an equal share of that table budget for unsent bytes; a slow reader gets
resync. An already started frame is completed before resync so framing stays valid.
`--max-watches` defaults to 64 per server; additional watches receive BUSY.

## C++ durable history

The C++ table API can retain history across checkpoint and restart:

```cpp
auto table = catalog.Find("world");
auto slot = table->CreateFeedSlot("consumer");
// After subsequent writes become durable:
auto reader = table->ReadFeedArchive(slot.position);
while (auto change = reader.Next()) {
    apply(*change);
    slot.position = change->position;
}
table->AdvanceFeedSlot("consumer", slot.position);
```

Create, drop, advance and reader creation take exclusive table access: release
any Table lease before calling them. `ListFeedSlots()` returns written positions,
persisted durable watermarks and retained bytes; `DropFeedSlot()` removes a slot.
Slot names match `[a-z_][a-z0-9_]*`, 1–63 bytes. Slot mutation requires a
single-process read-write table. Read-only tables can list slots and read their
already durable history.

A reader returns typed changes after its position through the durable frontier
captured when it opens, then returns null. It merges transaction frames, decodes
historical schema versions, preserves before/after rows and writer identity,
honours rollback intents and ignores a partial final frame. It never repairs
files. A complete frame with a bad checksum is an error; after any read failure,
open a fresh reader from the last returned position. Positions before all retained
slot positions raise `FeedArchiveExpiredError`; wrong epochs and positions above
the frontier are refused. Lost slots raise `FeedSlotLostError` when advanced.

`StoreConfig::slot_sync_interval` defaults to 100 ms and `slot_max_bytes` to
1 GiB per slot, including base images. Active readers pin archive deletion until
they close. Archive history contains data changes; ALTER schema notifications
remain part of the in-memory stream. Slots and durable catch-up are currently C++
APIs; WATCH keeps its existing in-memory resume behavior.
