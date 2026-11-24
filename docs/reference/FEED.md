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
  revision = 0
  lag_revisions = 0
  retained_bytes = 0
  attached = false
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
SHOW SLOTS [ON table] [PREFIX 'name']
WATCH table [SLOT 'name'] [AREA cx0 cy0 TO cx1 cy1 [, AREA cx0 cy0 TO cx1 cy1 ...]] [COLUMNS a,b] [NO BEFORE] [CHUNKS] [AFTER epoch revision] [ACK TIMEOUT ms] [TAKEOVER]
WATCH CATALOG [AFTER epoch revision]
ACK revision
UNWATCH
```

## Arguments

- `name`: quoted `[a-z_][a-z0-9_]*`, 1 through 63 bytes.
- `AREA`: inclusive chunk-coordinate rectangle or cuboid with exactly the table's active axes; transactions are clipped to it.
- `AREA ..., AREA ...`: WATCH without SLOT takes up to 64 areas with the same axes and keeps a chunk inside any of them; a slot keeps one AREA, and WATCH SLOT accepts several only when each repeats it.
- `COLUMNS`: project block values in declared order; column IDs preserve the projection across renames, and chunk metadata remains complete.
- `NO BEFORE`: omit previous block and chunk-metadata rows from mutation events.
- `CHUNKS`: receive each relevant committed chunk's version and whole form instead of block changes.
- `OWNER`: the user the slot belongs to; it defaults to the user running CREATE SLOT, and the user need not exist yet.
- `PREFIX`: list only slots whose names start with this quoted text, which follows the slot-name rule.
- `epoch`: the table's identity; dropping and recreating a table changes it.
- `revision`: a position increasing with committed changes; gaps are allowed.
- `AFTER`: resume after this position; it does not acknowledge anything.
- `TAKEOVER`: replace the slot's watcher after publishing its accepted ACK and closing its connection.
  The owner replacing its own watch needs READ; replacing another user additionally needs ADMIN on `*`.
- `ACK TIMEOUT`: positive milliseconds for a slot watch; closes the connection when its oldest fully sent, unacknowledged mutation expires.
- `SLOT`: retain history for this named consumer across restarts; only one watcher can hold a slot at a time. After RESTORE TABLE, an offline restore or SWAP TABLES, its next watch without AFTER starts with resync until you acknowledge a later revision.
  Its owner needs READ; another user also needs ADMIN on `*`, with or without AFTER or TAKEOVER, for as long as the watch lasts.

## Returns

- CREATE and DROP SLOT: `OK`, including authorized conditional no-ops.
- SHOW SLOTS: visible table (every table with MONITOR), name, epoch, saved `acked` revision, `revision`, `lag_revisions`, retained bytes, `attached`, lost state, loss reason, filters, and owner; the owner is shown to that user and to MANAGES USERS, and is null otherwise; a table ADMIN can still test an owner name with CREATE SLOT IF NOT EXISTS ... OWNER, and SHOW MIGRATIONS shows statement text with its OWNER.
- WATCH: start position, followed by change or chunks, schema, progress, or resync events.
- WATCH CATALOG: the catalog epoch and the revision it starts at, followed by catalog, progress and resync events; see [WATCH CATALOG](#watch-catalog).
- ACK: no success reply; it is accepted only inside a slot watch.
- UNWATCH: `OK` after the last push; ordinary statements can then resume.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | WATCH lacks READ, slot creation/removal lacks ADMIN, or watching another user's slot or takeover of another user lacks ADMIN on `*`. | Request access. |
| PERMISSION_DENIED | WATCH CATALOG lacks MONITOR and ADMIN on `*`, or the watch lost them, or MANAGES USERS when it shows users (sent best-effort before the connection closes). | Request access. |
| BUSY | A watcher or catch-up worker limit is reached. | Close another watcher or retry later. |
| SLOT_BUSY | The slot already has a watcher. | Close its existing watcher. |
| SLOT_LOST | Retained history exceeds its limit, or a projected column was dropped. | Rebuild state and recreate the slot. |
| INVALID_ARGUMENT reason=slot_filters | WATCH or conditional creation uses different slot filters, or conditional creation gives another OWNER. | Inherit its filters or create another slot. |
| OUT_OF_RANGE | A single slot change or schema event, as the slot's filters send it and with its ACK TIMEOUT record when set, is larger than the table's `feed_buffer_bytes`. | Increase feed_buffer_bytes before reconnecting. |
| INVALID_ARGUMENT | ACK exceeds the last fully delivered position. | Acknowledge only processed events. |
| INVALID_ARGUMENT | You run WATCH, CREATE SLOT or DROP SLOT on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| INVALID_ARGUMENT | You run WATCH CATALOG on a server that shares its data directory with other processes. | Watch the catalog on a server that is the only process of its data directory. |
| INTERNAL | `the catalog feed failed; watch the catalog again`: the server could not read the catalog after a change. | Watch the catalog again; the first event holds the whole catalog. |
| PROTOCOL | You send another statement while streaming. | Use a separate connection. |
| NO_TABLE | The table is absent, inaccessible, or dropped, or while watching you lost READ on it, or ADMIN on `*` that your watch of a slot you do not own needs (sent best-effort before the connection closes). | Check the table and access. |
| AUTH_REQUIRED reason=user_dropped | Your user was dropped while watching; sent best-effort, and the connection closes. | Log in again as an existing user. |
| INTERNAL | The watched table was fenced or recovered. | Watch it again after [RECOVER TABLE](TABLES.md#notes), from your saved position. |
| INTERNAL | `watched table was swapped; watch it again` or `watched table was renamed; watch it again`. | Watch the table by its name after [SWAP TABLES or RENAME TO](TABLES.md#notes), from your saved position. |

## Notes

CREATE SLOT accepts AREA, COLUMNS, NO BEFORE, CHUNKS, and OWNER once each, in any order after ON.
COLUMNS also accepts parentheses around its names.
Under `--auth none`, CREATE SLOT without OWNER records no owner.
A slot without a recorded owner takes the user of its first granted attach without ADMIN on `*`, saved before that watch starts; an attach with ADMIN on `*` records nobody.

Conditional slot creation preserves an existing position, owner and lost state when its filters match, and its OWNER when given.
WATCH inherits omitted slot filters and rejects supplied filters that differ.
A dropped projected column makes the slot lost with `reason=schema_filter`.
SHOW SLOTS preserves its IDs and uses null for names that no longer exist.
SHOW SLOTS `revision` is the durable revision recorded for the table's slots, which slot delivery never passes; it advances with each durable frontier pass (`--slot-sync-ms`).
`lag_revisions` is how far `acked` is below `revision`, counted in revision numbers, gaps included.
`retained_bytes` is the history the slot keeps after `acked`: how many bytes its consumer is behind.
`attached` is true while a watch holds the slot; another WATCH ... SLOT then answers SLOT_BUSY unless it uses TAKEOVER.

Conditional removal still checks rights and requires an existing table.
Change events contain complete before and after metadata rows when chunk columns change.
A null metadata before row means the chunk had never been written.
Metadata-only changes remain visible under block-column projections.
Watches have no idle timeout.
Watches without SLOT that have the same AREA set in any order, COLUMNS, NO BEFORE and CHUNKS share each projected event and its encoding. Shared events that a member has not read yet take at most `feed_buffer_bytes` per table in addition to the live feed history, divided equally among the sets.
After ordinary UNWATCH, the subscription is removed and its configured history linger begins before success returns.

Slot delivery stops at the durable frontier, including in relaxed mode.
ACK persistence is asynchronous; UNWATCH persists that watch's eligible ACK before returning.
When a slot watch ends after losing access or exceeding an ACK or output deadline, its claim is released before the terminal reply or socket closure; reconnecting does not wait for background watch cleanup.
After a crash, changes can repeat.
Store the applied position atomically with your own output to prevent duplicate effects.

## WATCH CATALOG

WATCH CATALOG streams changes to tables, columns, users, grants and slots.
It needs MONITOR or ADMIN on `*`; users, grants and slot owners appear only with MANAGES USERS.
The reply is `{epoch, revision}`: the catalog epoch, and the catalog revision the watch starts at; one revision of an epoch names one catalog state.
The first event holds the whole catalog: one `create` for each table, user and slot, at the revision of that event.
Each catalog statement takes the next revision when it ends, also one that changed nothing or failed.
An event carries what a statement changed, under its revision; it can also carry changes of statements that ended at the same time.
A revision whose statement changed nothing the watch can see arrives as `[progress, epoch, revision]`: your catalog is the catalog of that revision.
An event is `[catalog, epoch, revision, commit_time_ms, user, events, txn]`, with `txn` null; `user` names the user whose statement sent the event, and is null in the whole-catalog event and for watches without MANAGES USERS.
Each entry of `events` is a map of `kind` (`table`, `user` or `slot`), `action` (`create`, `alter` or `drop`), `name`, `table` (the table of a slot, null otherwise) and `data`, the object after the change, null for `drop`.
A table's `data` is its DESCRIBE map without `revision`, so columns, CHECK constraints and options changes appear as `alter`.
A user's `data` is its SHOW USERS map, so GRANT and REVOKE appear as `alter` of the user; a table rename or drop also changes the users whose grants follow it.
A slot's `data` is a map of its SHOW SLOTS `filters` and `owner`; its position, lag and loss state are not catalog changes.
RENAME TO appears as `drop` of the old name and `create` of the new one, SWAP TABLES as `alter` of both names.
A table that cannot be read without waiting, such as one ALTER TABLE is reopening or one that is fenced, keeps its published records; its changes follow in a later event.
The server keeps at most `--feed-buffer-bytes` (default 67108864) of catalog events for all catalog watches together; a table's `feed_buffer_bytes` does not apply.
A watch further behind receives `[resync, epoch, revision, null, null]` and then the whole catalog again; start your catalog over from it.
Catalog watches count against `--max-watches`, and a connection that already watches cannot start another watch.
With the directory feature `catalog_revision`, which new directories have, the epoch and revision survive restarts; without it, or on a server that shares its directory with other processes, each start has a new epoch.
`AFTER epoch revision` continues from your last event or progress: with the current epoch and the current revision, or a revision the server still holds, it sends only later events.
With another epoch or revision it sends `resync` and then the whole catalog; after a restart only the current revision continues.
A start after a crash during a catalog statement moves to the next revision, and a restored backup starts a new epoch.
AFTER resumes the view the watch had: after MANAGES USERS of its user changed, watch without AFTER, since the server cannot tell.
A filesystem rollback or a cold copy of the data directory keeps the epoch: a consumer that was away from such a directory must watch without AFTER.
`CATALOG` after WATCH is a keyword: new tables cannot be named `catalog`, and a table of that name made before cannot be watched.
The watch ends when the user loses MONITOR and ADMIN on `*`, or MANAGES USERS when it shows users.
Only UNWATCH is accepted while streaming.

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
