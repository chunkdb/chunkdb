# WATCH, ACK, UNWATCH, CREATE SLOT, DROP SLOT, SHOW SLOTS

You stream a table's committed changes and retain a consumer position with a durable slot.
A durable slot retains changes until your acknowledged position is saved.

## Examples

These examples create `feed_demo`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE feed_demo (kind u8) CHUNK 2 x 2
CREATE SLOT 'consumer' ON feed_demo
SHOW SLOTS ON feed_demo
```

```output
OK
OK
1) table = feed_demo, name = consumer, epoch = <epoch>, acked = 0, retained_bytes = 0, lost = false
```

To watch the table from a separate terminal:

```sh docs-check-watch
chunk-cli --uri "$CHUNKDB_URI" watch feed_demo
```

```output
start <epoch>:0
change revision 2 time_ms <time-ms> user admin
  block 0 0: (absent) -> {kind = 1}
```

After the start line, write in the shell:

```cql docs-check session=main
SET BLOCK 0 0 IN feed_demo kind = 1
```

```output
2
```

Press Ctrl-C to stop the CLI watcher; it sends UNWATCH.
For a slot, start the watcher at its saved position:

```sh docs-check-replay
chunk-cli --uri "$CHUNKDB_URI" watch feed_demo --slot consumer
```

```output
start <epoch>:0
schema <epoch>:2 version 1
change revision 2 time_ms <time-ms> user admin
  block 0 0: (absent) -> {kind = 1}
```

This command replays the preceding change because the slot has not acknowledged it yet.
The CLI acknowledges a change after printing it.
After stopping that watcher, remove the slot when you no longer need its retained history:

```cql docs-check session=main
DROP SLOT 'consumer' ON feed_demo
```

```output
OK
```

## Syntax

```text
CREATE SLOT [IF NOT EXISTS] 'name' ON table [AREA cx0 cy0 TO cx1 cy1] [COLUMNS a,b] [NO BEFORE] [CHUNKS]
DROP SLOT [IF EXISTS] 'name' ON table
SHOW SLOTS [ON table]
WATCH table [SLOT 'name'] [AREA cx0 cy0 TO cx1 cy1] [COLUMNS a,b] [NO BEFORE] [CHUNKS] [AFTER epoch revision] [ACK TIMEOUT ms] [TAKEOVER]
ACK revision
UNWATCH
```

## Arguments

- `name`: quoted `[a-z_][a-z0-9_]*`, 1 through 63 bytes.
- `AREA`: inclusive chunk-coordinate rectangle; transactions are clipped to blocks inside it.
- `COLUMNS`: project values in declared order; column IDs preserve the projection across renames.
- `NO BEFORE`: omit previous values from mutation events.
- `CHUNKS`: receive each relevant committed chunk's version and whole form instead of block changes.
- `epoch`: the table's identity; dropping and recreating a table changes it.
- `revision`: a position increasing with committed changes; gaps are allowed.
- `AFTER`: resume after this position; it does not acknowledge anything.
- `TAKEOVER`: replace the slot owner after publishing its accepted ACK and closing its connection.
  Same-user takeover needs READ; replacing another user additionally needs ADMIN on `*`.
- `ACK TIMEOUT`: positive milliseconds for a slot watch; closes the connection when its oldest fully sent, unacknowledged mutation expires.
- `SLOT`: retain history for this named consumer across restarts; only one watcher can own a slot.

## Returns

- CREATE and DROP SLOT: `OK`, including authorized conditional no-ops.
- SHOW SLOTS: visible table, name, epoch, saved `acked` revision, retained bytes, lost state, filters, and loss reason.
- WATCH: start position, followed by change or chunks, schema, progress, or resync events.
- ACK: no success reply; it is accepted only inside a slot watch.
- UNWATCH: `OK` after the last push; ordinary statements can then resume.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | WATCH lacks READ, slot creation/removal lacks ADMIN, or cross-user takeover lacks ADMIN on `*`. | Request access. |
| BUSY | A watcher or catch-up worker limit is reached. | Close another watcher or retry later. |
| SLOT_BUSY | The slot already has an owner. | Close its existing watcher. |
| SLOT_LOST | Retained history exceeds its limit, or a projected column was dropped. | Rebuild state and recreate the slot. |
| INVALID_ARGUMENT reason=slot_filters | WATCH or conditional creation uses different slot filters. | Inherit its filters or create another slot. |
| OUT_OF_RANGE | A single slot change cannot fit its output budget. | Increase feed_buffer_bytes before reconnecting. |
| INVALID_ARGUMENT | ACK exceeds the last fully delivered position. | Acknowledge only processed events. |
| PROTOCOL | You send another statement while streaming. | Use a separate connection. |
| NO_TABLE | The table is absent, inaccessible, or dropped. | Check the table and access. |

## Notes

CREATE SLOT accepts AREA, COLUMNS, NO BEFORE, and CHUNKS once each, in any order after ON.
COLUMNS also accepts parentheses around its names.

Conditional slot creation preserves an existing position and lost state when its filters match.
WATCH inherits omitted slot filters and rejects supplied filters that differ.
A dropped projected column makes the slot lost with `reason=schema_filter`.
SHOW SLOTS preserves its IDs and uses null for names that no longer exist.

Conditional removal still checks rights and requires an existing table.
Watches have no idle timeout.
After ordinary UNWATCH, the subscription is removed and its configured history linger begins before success returns.

Slot delivery stops at the durable frontier, including in relaxed mode.
ACK persistence is asynchronous; UNWATCH persists that watch's eligible ACK before returning.
After a crash, changes can repeat.
Store the applied position atomically with your own output to prevent duplicate effects.

## Limits

Defaults are 64 watches, 67108864 live feed bytes per table, and 1073741824 retained bytes per slot.
Ordinary history lingers for 30000 ms after the last watch; slots keep history active independently.
See [Change feed](../CHANGE_FEED.md) and [Resume a consumer](../FEED_RESUME.md) for overflow and recovery.

Takeover closes the old owner with best-effort `KILLED reason=slot_takeover`.
It preserves only received, validated ACKs; delivered but unacknowledged events replay for the replacement.
A concurrent handoff returns `SLOT_BUSY reason=takeover_pending`.
A partial frame or pinned TLS retry closes without injecting an error inside the frame.

A failure after slot metadata replacement returns `OUTCOME_UNKNOWN stage=slot_records`.
Recover the table before retrying; its ACK or slot change may already be published.

CREATE SLOT requires `feed_slots`; persisted filters also require `slot_filters`.
[Enable missing features](TABLES.md#feature-activation) before creating a slot; these commands never activate features implicitly.
