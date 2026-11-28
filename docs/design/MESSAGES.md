# Application messages

A write or transaction can publish an application message that the change feed delivers in the same commit as its data ([#156](https://github.com/chunkdb/chunkdb/issues/156)).
The supported forms are in [Blocks](../reference/BLOCKS.md#publish), [Protocol](../PROTOCOL.md#messages) and [Storage format](../STORAGE_FORMAT.md#frames-and-records).

## Statement

A write ends with `PUBLISH 'channel' message`: SET BLOCK, DELETE BLOCK and every SET CHUNK form, after IF and RETURNING.
The channel is a quoted literal of 1 to 63 bytes matching `[a-z_][a-z0-9_.]*`, checked when the statement parses.
The message is a text or bytes literal or a `$n` parameter frame of 0 to 16384 bytes, `limits.max_message_bytes` in HELLO; NULL is refused.
The table needs the `messages` feature: new directories enable it, existing ones run `ENABLE FEATURE 'messages'`; without it the write is refused with `INVALID_ARGUMENT reason=feature_disabled` before it changes anything.

A commit publishes at most one message.
Inside a transaction any write may carry it and COMMIT publishes it; a second PUBLISH in the same transaction is refused and stages nothing.
A write whose IF condition or IF VERSION refuses it publishes nothing.
A write or transaction that publishes always commits: when its data are unchanged it still takes a revision, and its change has no blocks.
Without PUBLISH, an unchanged write commits nothing, as before.

## WAL representation

The message is WAL TLV type 5, MESSAGE: channel length `u8`, the channel, then the message bytes.
It sits in the frame header's TLV area, so the header CRC covers it, and replay validates it with the rest of the frame before applying anything.
A frame holds at most one MESSAGE; collection frames hold none.
The TLV requires table incompat feature `messages` (`0x200`), so a build without it refuses the table instead of reading the TLV as damage.
The largest MESSAGE takes 4 + 1 + 63 + 16384 = 16452 bytes of the 65535-byte TLV area, which leaves room for SCHEMA, USER and a TAG.

A single-chunk write carries the message on its one frame.
A transaction carries it on its first frame in coordinate order; it is still one revision, so the feed merges its frames into one change with one message.
An unchanged write that publishes still logs a record, its unchanged presence or state: a frame needs a record, and replaying it changes nothing.

## Durability and replay

The message has exactly the durability of its frame, so a crash keeps or loses it with the data.
When a write uses relaxed WAL batching, its message waits in the same batch as its data.
Transaction COMMIT makes its message and data durable before acknowledging success in every durability mode.
Chunk recovery validates the TLV and ignores it for state.
An ordinary watch decodes it from the frame the write captured.
A slot replays it from live and archived WALs, so a slot consumer receives every message of the history it retains, across restarts.
A checkpoint without slots drops the WAL and with it the message history, as it drops the history of row changes; backups contain no catch-up history.

## Feed

A change whose commit published a message has one more element after `chunk_changes`: `messages`, an array of `[channel, message]`.
A CHUNKS push likewise appends `messages` after `txn`.
A lagging CHUNKS watch never merges a push that carries a message with other pushes.
Other pushes keep their element count, so clients that read the elements they know keep working.
AREA drops a change that keeps no data in its areas, with its message: a message belongs to its commit's data.
COLUMNS, NO BEFORE and CHUNKS shape the data and never drop a change that carries a message, so a watch without AREA receives a published change even when it has no blocks.
HELLO lists the `messages` feature.

## Cost

A write without PUBLISH writes the same frame bytes as before and does no new work but reading one thread-local pointer.
The feed, the archive reader and replay only test whether a frame has the TLV.
