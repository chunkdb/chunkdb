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
CREATE SLOT [IF NOT EXISTS] 'name' ON table
DROP SLOT [IF EXISTS] 'name' ON table
SHOW SLOTS [ON table]
WATCH table [SLOT 'name'] [AREA cx0 cy0 TO cx1 cy1] [AFTER epoch revision]
ACK revision
UNWATCH
```

## Arguments

- `name`: quoted `[a-z_][a-z0-9_]*`, 1 through 63 bytes.
- `AREA`: inclusive chunk-coordinate rectangle; transactions are clipped to blocks inside it.
- `epoch`: the table's identity; dropping and recreating a table changes it.
- `revision`: a position increasing with committed changes; gaps are allowed.
- `AFTER`: resume after this position; it does not acknowledge anything.
- `SLOT`: retain history for this named consumer across restarts; only one watcher can own a slot.

## Returns

- CREATE and DROP SLOT: `OK`, including authorized conditional no-ops.
- SHOW SLOTS: visible table, name, epoch, saved `acked` revision, retained bytes, and `lost` state.
- WATCH: start position, followed by change, schema, or resync events.
- ACK: no success reply; it is accepted only inside a slot watch.
- UNWATCH: `OK` after the last push; ordinary statements can then resume.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | WATCH lacks READ, or slot creation/removal lacks ADMIN. | Request access. |
| BUSY | The watcher limit is reached or a slot already has an owner. | Close another watcher. |
| SLOT_LOST | Retained history exceeds the slot's limit. | Rebuild state and recreate the slot. |
| OUT_OF_RANGE | A single slot change cannot fit its output budget. | Increase feed_buffer_bytes before reconnecting. |
| INVALID_ARGUMENT | ACK exceeds the last fully delivered position. | Acknowledge only processed events. |
| INVALID_ARGUMENT | You run WATCH, CREATE SLOT or DROP SLOT on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| PROTOCOL | You send another statement while streaming. | Use a separate connection. |
| NO_TABLE | The table is absent, inaccessible, or dropped. | Check the table and access. |
| INTERNAL | The watched table was fenced or recovered. | Watch it again after [RECOVER TABLE](TABLES.md#notes), from your saved position. |

## Notes

Conditional slot creation preserves an existing position and lost state.
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
