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
1)
  table = feed_demo
  name = consumer
  epoch = <epoch>
  acked = 0
  retained_bytes = 0
  lost = false
  lost_reason = NULL
  filters:
    area = NULL
    column_ids = NULL
    columns = NULL
    no_before = false
    chunks = false
  owner = admin
```

To watch the table from a separate terminal:

```sh docs-check-watch
chunk-cli --uri "$CHUNKDB_URI" watch feed_demo
```

```output
start <epoch>:0
change revision 1 time_ms <time-ms> user admin
  block 0 0: (absent) -> {kind = 1}
```

After the start line, write in the shell:

```cql docs-check session=main
SET BLOCK 0 0 IN feed_demo kind = 1
```

```output
1
```

Press Ctrl-C to stop the CLI watcher; it sends UNWATCH.
For a slot, start the watcher at its saved position:

```sh docs-check-replay
chunk-cli --uri "$CHUNKDB_URI" watch feed_demo --slot consumer
```

```output
start <epoch>:0
schema <epoch>:1 version 1
change revision 1 time_ms <time-ms> user admin
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
CREATE SLOT [IF NOT EXISTS] 'name' ON table [AREA cx0 cy0 TO cx1 cy1] [COLUMNS a,b] [NO BEFORE] [CHUNKS] [OWNER user]
DROP SLOT [IF EXISTS] 'name' ON table
SHOW SLOTS [ON table]
WATCH table [SLOT 'name'] [AREA cx0 cy0 TO cx1 cy1] [COLUMNS a,b] [NO BEFORE] [CHUNKS] [AFTER epoch revision] [ACK TIMEOUT ms] [TAKEOVER]
ACK revision
UNWATCH
```

## Arguments

- `name`: quoted `[a-z_][a-z0-9_]*`, 1 through 63 bytes.
- `AREA`: inclusive chunk-coordinate rectangle or cuboid with exactly the table's active axes; transactions are clipped to it.
- `COLUMNS`: project block values in declared order; column IDs preserve the projection across renames, and chunk metadata remains complete.
- `NO BEFORE`: omit previous block and chunk-metadata rows from mutation events.
- `CHUNKS`: receive each relevant committed chunk's version and whole form instead of block changes.
- `OWNER`: the user the slot belongs to; it defaults to the user running CREATE SLOT, and the user need not exist yet.
- `epoch`: the table's identity; dropping and recreating a table changes it.
- `revision`: a position increasing with committed changes; gaps are allowed.
- `AFTER`: resume after this position; it does not acknowledge anything.
- `TAKEOVER`: replace the slot's watcher after publishing its accepted ACK and closing its connection.
  The owner replacing its own watch needs READ; replacing another user additionally needs ADMIN on `*`.
- `ACK TIMEOUT`: positive milliseconds for a slot watch; closes the connection when its oldest fully sent, unacknowledged mutation expires.
- `SLOT`: retain history for this named consumer across restarts; only one watcher can hold a slot at a time.
  Its owner needs READ; another user also needs ADMIN on `*`, with or without AFTER or TAKEOVER, for as long as the watch lasts.

## Returns

- CREATE and DROP SLOT: `OK`, including authorized conditional no-ops.
- SHOW SLOTS: visible table, name, epoch, saved `acked` revision, retained bytes, lost state, filters, loss reason, and owner; the owner is shown to that user and to MANAGES USERS, and is null otherwise; a table ADMIN can still test an owner name with CREATE SLOT IF NOT EXISTS ... OWNER, and SHOW MIGRATIONS shows statement text with its OWNER.
- WATCH: start position, followed by change or chunks, schema, progress, or resync events.
- ACK: no success reply; it is accepted only inside a slot watch.
- UNWATCH: `OK` after the last push; ordinary statements can then resume.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | WATCH lacks READ, slot creation/removal lacks ADMIN, or watching another user's slot or takeover of another user lacks ADMIN on `*`. | Request access. |
| BUSY | A watcher or catch-up worker limit is reached. | Close another watcher or retry later. |
| SLOT_BUSY | The slot already has a watcher. | Close its existing watcher. |
| SLOT_LOST | Retained history exceeds its limit, or a projected column was dropped. | Rebuild state and recreate the slot. |
| INVALID_ARGUMENT reason=slot_filters | WATCH or conditional creation uses different slot filters, or conditional creation gives another OWNER. | Inherit its filters or create another slot. |
| OUT_OF_RANGE | A single slot change or schema event, with its ACK TIMEOUT record when set, is larger than the table's `feed_buffer_bytes`. | Increase feed_buffer_bytes before reconnecting. |
| INVALID_ARGUMENT | ACK exceeds the last fully delivered position. | Acknowledge only processed events. |
| INVALID_ARGUMENT | You run WATCH, CREATE SLOT or DROP SLOT on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| PROTOCOL | You send another statement while streaming. | Use a separate connection. |
| NO_TABLE | The table is absent, inaccessible, or dropped, or while watching you lost READ on it, or ADMIN on `*` that your watch of a slot you do not own needs (sent best-effort before the connection closes). | Check the table and access. |
| AUTH_REQUIRED reason=user_dropped | Your user was dropped while watching; sent best-effort, and the connection closes. | Log in again as an existing user. |
| INTERNAL | The watched table was fenced or recovered. | Watch it again after [RECOVER TABLE](TABLES.md#notes), from your saved position. |

## Notes

CREATE SLOT accepts AREA, COLUMNS, NO BEFORE, CHUNKS, and OWNER once each, in any order after ON.
COLUMNS also accepts parentheses around its names.
Under `--auth none`, CREATE SLOT without OWNER records no owner.
A slot without a recorded owner takes the user of its first granted attach without ADMIN on `*`, saved before that watch starts; an attach with ADMIN on `*` records nobody.

Conditional slot creation preserves an existing position, owner and lost state when its filters match, and its OWNER when given.
WATCH inherits omitted slot filters and rejects supplied filters that differ.
A dropped projected column makes the slot lost with `reason=schema_filter`.
SHOW SLOTS preserves its IDs and uses null for names that no longer exist.

Conditional removal still checks rights and requires an existing table.
Change events contain complete before and after metadata rows when chunk columns change.
A null metadata before row means the chunk had never been written.
Metadata-only changes remain visible under block-column projections.
Watches have no idle timeout.
After ordinary UNWATCH, the subscription is removed and its configured history linger begins before success returns.

Slot delivery stops at the durable frontier, including in relaxed mode.
ACK persistence is asynchronous; UNWATCH persists that watch's eligible ACK before returning.
After a crash, changes can repeat.
Store the applied position atomically with your own output to prevent duplicate effects.

## Limits

Defaults are 64 watches, 67108864 live feed bytes per table, and 1073741824 retained bytes per slot.
Each watch queues output within its share, the table's `feed_buffer_bytes` divided among the table's watches.
A slot change larger than its share waits until the watch has sent its earlier output, then takes its turn, in arrival order, for a separate table pool of `feed_buffer_bytes`.
While it waits, the watch sends no progress events.
The server closes a connection sending any event larger than its share, including schema, progress and resync events, when no byte of it is written for `--client-io-timeout-ms`, which frees the pool; on TLS each completed record of up to 16 KiB counts as written.
It reads no ACK until less than the watch's share of that change is unsent, so set ACK TIMEOUT above the time your connection needs to receive `feed_buffer_bytes`.
Ordinary history lingers for 30000 ms after the last watch; slots keep history active independently.
See [Change feed](../CHANGE_FEED.md) and [Resume a consumer](../FEED_RESUME.md) for overflow and recovery.

Takeover closes the old watcher with best-effort `KILLED reason=slot_takeover`.
It preserves only received, validated ACKs; delivered but unacknowledged events replay for the replacement.
A concurrent handoff returns `SLOT_BUSY reason=takeover_pending`.
A partial frame or pinned TLS retry closes without injecting an error inside the frame.

A failure after slot metadata replacement returns `OUTCOME_UNKNOWN stage=slot_records`.
Recover the table before retrying; its ACK or slot change may already be published.

CREATE SLOT requires `feed_slots`; persisted filters also require `slot_filters`.
[Enable missing features](TABLES.md#feature-activation) before creating a slot; these commands never activate features implicitly.
