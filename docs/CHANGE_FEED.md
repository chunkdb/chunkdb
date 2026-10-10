# Watching table changes in 2.0

WATCH streams committed changes of one table in revision order, with before/after rows and writing user.
A transaction produces one change; timestamps may move backward and do not define order.
Use a dedicated authenticated connection and READ on the table ([users](USERS.md)).

```text
WATCH world AREA 0 0 TO 3 3
UNWATCH
```

AREA is an inclusive rectangle of chunk coordinates; a spanning transaction is clipped to its blocks inside that rectangle.
The initial OK names `(epoch, revision)` and starts after that position.
Epoch is the table's identity, represented by 32 hex digits.
Without AFTER, an ordinary watch starts after currently completed writes.
Only UNWATCH is accepted in an ordinary stream; its OK follows the last push and ordinary statements then resume.
A different statement closes the stream with PROTOCOL; DROP TABLE ends it with NO_TABLE.
Watches have no idle timeout, release statement workers and are unavailable in shared multi-process operation.

## Resynchronizing

[Protocol 3](PROTOCOL.md#watch-and-durable-slots) defines exact change/schema/resync framing and typed rows.
Changes contain whole transactions and revisions may have gaps.
Schema descriptions precede changes that use those columns; NULL rows denote absent blocks.

A resync means history after your previous position is unavailable.
Keep consuming the stream while a second connection reads DESCRIBE and GET AREA, or SCAN CHUNKS followed by GET CHUNK.
Replace state for the scanned area, including deleted blocks/chunks, and keep the resync frontier with that state.
Apply subsequent changes to a chunk only when their revision exceeds the version you read for that chunk.
An ordinary watch can resync after restart, an unknown epoch/position or buffer overflow, including an event too large to retain.

`--feed-buffer-bytes` defaults to 64 MiB per table and covers queues, retained changes and encoded output.
Watches share the output budget equally; a slow ordinary watch resynchronizes after overflow, completing an already started frame first.
`--max-watches` defaults to 64 per server; excess WATCH requests receive BUSY.
The live feed is released when no watch or slot keeps it active.

## Durable slots

Create a slot before the writes you need to retain; CREATE/DROP require ADMIN on the table.

```text
CREATE SLOT 'consumer' ON world
SHOW SLOTS ON world
WATCH world SLOT 'consumer'
UNWATCH
```

Names are quoted lowercase identifiers of 1–63 bytes.
SHOW SLOTS lists visible tables with `table`, `name`, `epoch`, written `acked`, `retained_bytes` and `lost`.
DROP SLOT releases history when no other slot/reader retains it; DROP TABLE removes its slots.
Only one watch can own a slot; a competing watch receives BUSY.

The stream starts after the written acknowledgement, or a later AFTER position in that epoch.
AFTER does not acknowledge history; an old epoch or a position above the completed clock produces resync.
Catch-up sends retained archives followed by live changes without repeating the handover revision, clipped by AREA.
It sends only through the persisted durable frontier, including in relaxed mode.
`--slot-sync-ms` defaults to 100 ms; failure to sync freezes the frontier until writer restart.

Within a slot watch send `ACK <revision>` after applying a fully delivered change.
An independently versioned live schema event and the start position can also be acknowledged; a schema preface for a change cannot acknowledge that change before it arrives.
ACK has no success reply; an excessive revision receives INVALID_ARGUMENT and the watch stays open.
Positions are monotonic and never release history until written to slot metadata.
Eligible ACKs share a table batch persisted at most every 100 ms; UNWATCH persists its own ACK before replying, syncing its position first when necessary.
A crash can repeat changes after the last written acknowledgement.
For exactly-once output, atomically store the applied position with your output, reconnect AFTER that position and ignore duplicates; ACK alone cannot make an external write atomic.

`--slot-max-bytes` defaults to 1 GiB per slot, including archive bases.
Exceeding it marks the slot lost; SHOW SLOTS reports loss and WATCH receives SLOT_LOST.
Rebuild consumer state, drop the lost slot and recreate it before consuming new changes.
A single slot change that exceeds its output share at admission ends the watch with OUT_OF_RANGE; raise the budget before resuming.
An admitted change remains deliverable if a later watch reduces its share.
See [Go](https://github.com/chunkdb/chunkdb-go), [TypeScript](https://github.com/chunkdb/chunkdb-js) and [CLI](https://github.com/chunkdb/chunk-cli) for runnable consumer examples.
