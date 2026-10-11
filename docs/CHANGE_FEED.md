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

See [Feed statements](reference/FEED.md), [Resume a consumer](FEED_RESUME.md), and [Users and rights](USERS.md).

A `chunks` push carries mutations exactly like `change`, so a fully written `chunks` frame delivered to the application is ACK-eligible and starts (or keeps) the deadline of the oldest unacknowledged mutation; progress stays ACK-eligible but never starts or extends the timeout; schema, resync and unknown pushes move nothing.
