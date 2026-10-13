# Change feed

A change feed streams committed table updates in revision order, with their previous values, new values, and writing user.

You can update a game view when another player changes a block.
Start a watcher in a separate terminal after creating the table:

```cql docs-check session=main
CREATE TABLE world (kind u8) CHUNK 2 x 2
```

```output
OK
```

```sh docs-check-watch
chunk-cli --uri "$CHUNKDB_URI" watch world
```

```output
start <epoch>:0
change revision 2 time_ms <time-ms> user admin
  block 0 0: (absent) -> {kind = 1}
```

After the start line, write from your shell:

```cql docs-check session=main
SET BLOCK 0 0 IN world kind = 1
```

```output
2
```

The **epoch** identifies the table; it changes when the table is recreated or restored.
The **revision** orders changes within that epoch and can contain gaps.

A restored table records its immediate parent epoch and the completed backup cut, called its **fork revision**.
Resync events include both fields, or nulls for tables without a recorded parent.
An exact parent-fork position continues when the completed cut or retained history proves that no changes are missing.
Earlier positions require their replay prefix; later or unrelated parent positions require rebuilding.

Backups contain state at the cut, without archived catch-up history.
After a later crash or restart, absent chunk files do not prove that no post-fork mutation was lost.
Without durable replay or loss proof, even an exact-fork watch resyncs.
Timestamps can move backward, so use revisions for ordering.
Idle or filtered watches receive progress at most once per `--feed-progress-ms`, default 1000.
It marks a sender-drained revision; slot progress also stays within the durable frontier.

## Guarantees

- One committed transaction produces one change containing all its affected blocks.
- Schema descriptions precede changes that need their columns.
- An ordinary watch starts after currently completed writes unless you supply AFTER.
- A durable slot retains a saved consumer position across server restarts.
- A slot delivers only changes that have reached the durable frontier.
- A resync event means you must rebuild consumer state because the requested history is unavailable.
- A terminal error closes the watch; acknowledgements arriving after termination do not replace that error.

A slot is a named consumer that retains changes until its saved acknowledgement advances.
Create it with fixed AREA, COLUMNS, NO BEFORE, or CHUNKS filters when a consumer needs a restricted stream.
WATCH inherits these filters; changing them requires a different slot.
Projection IDs survive renames, but dropping a projected column makes its slot lost with `reason=schema_filter`.
Acknowledgements alone cannot make writes to another system atomic.
Store your applied position together with your output, and ignore repeated positions after a crash.

## Limits

| Limit | Default |
| --- | --- |
| Watches per server | 64, `--max-watches` |
| Live history and encoded output per table | 67108864 bytes, `feed_buffer_bytes` |
| Retained history per slot | 1073741824 bytes, `slot_max_bytes` |
| Ordinary history after the last watch | 30000 ms, `--feed-linger-ms` |
| Slot durable-frontier sync interval | 100 ms, `--slot-sync-ms` |

Slow ordinary watches resync when the live history budget is exceeded.
A slot becomes lost when its retained history exceeds its limit.
Reducing a table's limits can also trigger resync or mark existing slots lost.

## Slots watched with AREA

ACK accepts at most the last change, chunks or progress revision delivered to your application.
A slot watch with AREA filters mutations, while progress can advance its ACK through completed writes outside that area.
Writes outside the area still add to the slot's retained history until acknowledged, which can exceed `slot_max_bytes` and mark the slot lost.
Consume and acknowledge progress as well as mutations, or raise `slot_max_bytes` to cover the writes between acknowledgements.
SHOW SLOTS reports each slot's retained bytes.

See [Feed statements](reference/FEED.md), [Resume a consumer](FEED_RESUME.md), and [Users and rights](USERS.md).

A `chunks` push carries mutations exactly like `change`, so a fully written `chunks` frame delivered to the application is ACK-eligible and starts (or keeps) the deadline of the oldest unacknowledged mutation; progress stays ACK-eligible but never starts or extends the timeout; schema, resync and unknown pushes move nothing.

The server enables TCP keepalive for plain and TLS connections with `--tcp-keepalive-ms`, default 60000.
See [server flags](SERVER_FLAGS.md) for platform rounding and probe counts.

`WATCH table SLOT 'consumer' ACK TIMEOUT 30000` sets a deadline for unacknowledged mutations.
The clock starts after a complete mutation frame is sent, and later mutations keep the oldest timestamp.
ACK removes covered timestamps; progress never starts or extends this clock.
Expiry closes the connection and keeps the accepted saved ACK.
At a clean frame boundary, the server attempts `TIMEOUT reason=ack_timeout` before closing.
A blocked socket or partial frame can close without this terminal error.
Omitting this option disables the mutation deadline.

CHUNKS delivers exact post-commit whole-chunk forms, including unchanged blocks; a null form denotes deletion.
COLUMNS projects each form in its declared column order, matching GET CHUNK with the same projection.
Queued forms retain their commit's values even when later writes change the chunk.
An ordinary resume over earlier row-only history requires resync; a slot can use its retained archive prefix.

`WATCH table SLOT 'consumer' TAKEOVER` replaces the owning connection.
The server persists its last accepted ACK and closes it before attaching the replacement.
Only the same user with READ or a user with ADMIN on `*` can take over an active owner.
Unacknowledged events remain replayable; queued or delivered events never become automatic ACKs.
The old connection receives best-effort `KILLED reason=slot_takeover`, while competing handoffs receive `SLOT_BUSY reason=takeover_pending`.
