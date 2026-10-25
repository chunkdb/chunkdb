# Resume a consumer

You reconnect a consumer and rebuild its state when retained history is unavailable.
Use a dedicated connection with READ on the table.
See [Change feed](CHANGE_FEED.md) for epochs, revisions, and durable slots.

## Retain changes

Create a slot before the writes you need to retain:

```cql docs-check
CREATE TABLE world (kind u8) CHUNK 2 x 2
CREATE SLOT 'consumer' ON world
```

```output
OK
OK
```

Connect using the saved position, if you have one:

```sh
chunk-cli --uri "$CHUNKDB_URI" watch world --slot consumer
```

After applying each fully delivered change, acknowledge its revision through your client API.
The CLI acknowledges after printing, which is useful for inspection rather than atomic external processing.
Persist the applied epoch and revision with your output before acknowledging.
Reconnect AFTER that position and skip changes you already applied.

## Rebuild after resync

1. Keep consuming the stream while a second connection reads DESCRIBE and the required GET AREA.
2. For a larger table, use SCAN CHUNKS followed by GET CHUNK.
3. Replace the scanned state, including blocks or chunks that have disappeared.
4. Save the resync position with that state.

5. Apply later changes to a chunk only when their revision exceeds the chunk version you read.

## If it fails

- `resync`: rebuild; an old epoch, restart, expired ordinary history, or overflow can make resume unavailable.
- `SLOT_LOST`: rebuild state, drop the lost slot, and recreate it before consuming new changes.
- `BUSY`: close the competing slot owner or another watcher.
- `OUT_OF_RANGE`: increase the live output budget before reconnecting.
- `INTERNAL`: run RECOVER TABLE on the table or restart the affected writer, then watch again from your saved position; ordinary consumers rebuild their state after resync.

See [Feed statements](reference/FEED.md) for options and acknowledgement rules.
