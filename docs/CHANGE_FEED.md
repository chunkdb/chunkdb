# Watching table changes

WATCH streams committed changes of one table, including the complete before and
after rows of changed blocks. Changes are ordered by revision; a transaction
produces one change. Commit timestamps may move backwards; use revisions to order.
Ordinary WATCH uses an in-memory feed and follows the table's write visibility
and durability mode. A durable slot retains history across restarts and sends
only changes through its persisted durable frontier.

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
still retained while a feed is active. Only UNWATCH is accepted in an ordinary watch;
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
durable slot keeps it active. Each watch
also has an equal share of that table budget for unsent bytes; a slow reader gets
resync. An already started frame is completed before resync so framing stays valid.
`--max-watches` defaults to 64 per server; additional watches receive BUSY.

## Durable slots

Create a slot before the writes you need to consume, then use a dedicated
connection to watch it:

```text
> CREATE SLOT 'consumer' ON world
< +OK
> SHOW SLOTS ON world
< [{table: world, name: consumer, epoch: 0123456789abcdef0123456789abcdef,
    acked: 1042, retained_bytes: 0, lost: false}]
> WATCH world SLOT 'consumer'
< +OK 0123456789abcdef0123456789abcdef 1042
< > [schema, epoch, 1043, schema_version, columns]
< > [change, epoch, 1043, time_ms, user, schema_version, blocks]
> ACK 1043
> UNWATCH
< +OK
> WATCH world SLOT 'consumer' AFTER 0123456789abcdef0123456789abcdef 1043
< +OK 0123456789abcdef0123456789abcdef 1043
```

The example abbreviates replies; ACK has no reply on success. CREATE and DROP
SLOT need ADMIN on the table, WATCH SLOT needs READ, and SHOW SLOTS lists only
tables the user has a right on. Names are quoted `[a-z_][a-z0-9_]*`, 1–63 bytes.
`DROP SLOT 'consumer' ON world` removes the slot and releases its retained
history once other slots/readers no longer need it. DROP TABLE removes its
slots and ends watches with NO_TABLE. A second watch of the same slot receives
BUSY. Dropping and recreating a name invalidates its old watch.

The start position is the slot's written acknowledgement, or the given AFTER
position when it is later in the same epoch. AFTER does not itself acknowledge
or release history. An old epoch or a position above the table's completed clock
produces resync at the current epoch. A completed position awaiting durability
is accepted; subsequent changes wait for the durable frontier. Archived
changes across checkpoints and empty-chunk collection are sent first, then live
changes, without a gap or a repeated handover revision. AREA clips both parts.
Schema descriptions precede changes using those columns, including archive
catch-up; they identify the layout of a change rather than the historical time
of an ALTER statement.

ACK may acknowledge only through the last fully sent change, an independently
versioned live schema event, or the starting position. A schema description
prefacing an archived change has that change's revision; apply the change before
acknowledging it. An excessive revision receives INVALID_ARGUMENT and leaves the watch
open. Eligible durable positions from every watch of a table are combined in one
batch, written at most every 100 ms. UNWATCH persists its own accepted ACK and
flushes other eligible positions before replying; if its ACK is not durable yet,
it synchronizes that position first. Durable-frontier passes and slot management
persist metadata separately. A position that has not become durable yet
waits for the durability pass before it can be written. SHOW SLOTS reports this **written** position
as `acked`; archives are released only through written positions. A crash may
therefore repeat changes acknowledged since the last write of slot metadata.

For exactly-once output, atomically store the last applied `(epoch, revision)`
with your output. Send ACK after that commit. On reconnect, watch AFTER your
stored position and keep ignoring already applied revisions. This prevents a
server crash between processing and acknowledgement from duplicating output;
ACK alone does not make writes to an external system atomic.

Slot delivery waits for the persisted durable watermark in every durability
mode. `--slot-sync-ms` defaults to 100 ms; in relaxed mode the pass flushes
staged WAL batches and syncs them before advancing that frontier. Failure to
sync freezes the frontier until restart. An idle slot watch still receives new
durable changes and persists ACKs without requiring another client command.

`--slot-max-bytes` defaults to 1 GiB per slot, including archived base images.
Exceeding it durably marks the slot lost; SHOW SLOTS returns `lost: true`, and
WATCH returns SLOT_LOST. Rebuild your consumer state, drop the lost slot and
create it again before consuming new changes. Slow slot watches use retained
archives to catch up when the in-memory buffer no longer holds their position.
An individual slot change that cannot fit its share of the output budget ends
the watch with OUT_OF_RANGE; raise the feed budget before resuming it.

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
honours rollback intents and ignores a partial final live-WAL frame. An immutable
archive or a captured completed prefix must be complete; truncation is an error.
It never repairs files. A complete frame with a bad checksum is an error; after any read failure,
open a fresh reader from the last returned position. Positions before all retained
slot positions raise `FeedArchiveExpiredError`; wrong epochs and positions above
the frontier are refused. Lost slots raise `FeedSlotLostError` when advanced.

`StoreConfig::slot_sync_interval` defaults to 100 ms and `slot_max_bytes` to
1 GiB per slot, including base images. Active readers pin archive deletion until
they close. Archive history contains data changes; ALTER schema notifications
remain part of the in-memory stream. Protocol slot watches send schema
descriptions before the data changes decoded with them.
