# Change-feed ordering and retention
The supported protocol is in [change feed](../CHANGE_FEED.md) and [protocol 3](../PROTOCOL.md); archive bytes are in [storage](../STORAGE_FORMAT.md).

## Producer ordering

A table keeps a stable per-thread producer registry even without subscribers, also used by backup.
A writer publishes its lower revision bound before taking a revision and retains it until completion, including postcommit transaction work.
The sender samples clock and bounds using the sequentially consistent pairing and emits only below the completed watermark.
Raw WAL frames and replaced state enter per-thread queues; the sender merges them in revision order, decodes once and publishes bounded retained entries.
A transaction's frames are merged into one typed change and area filtering preserves its single revision.
Feed recreation cannot invalidate producers still owned by the store.

## Transport and buffering

After WATCH admission, the socket/TLS session moves from its worker to feed I/O.
Wakeup channels signal new changes, durable frontiers and shutdown; output does not hold request workers.
Retained changes use the table budget; queued output uses per-watch shares of it plus one table pool of the same size for slot frames above a share.
Ordinary overflow requests resync; slot overflow catches up from archives, while an event larger than the table budget ends the watch.
A slot frame whose bytes and receipt exceed its watch share waits until that watch has no unsent output, then queues in arrival order for a separate table pool bounded by the table budget.
A watch waiting for anything but its turn, such as its own ACKs, leaves that queue and joins at the back when it needs the pool again.
Progress, resync and schema frames are a few bytes and never queue; above a share they use the pool whenever their watch has no unsent output and the pool has room.
A frame that fits the share alone waits for ACKs instead when receipts fill it.
Its connection closes, releasing its charges at once, when no byte of its queued output is written for the client I/O timeout; TLS writes one record per call, so every completed call is progress.
A watch waiting for the pool sends no progress, and reads no ACK while more than its share of a pool frame is unsent.
Only the catch-up worker charges either counter; while waiting, only frames within a share and the head of the pool queue keep their encoded form.
Within-share slot output and receipts use another table counter bounded by the budget, so a pending large frame never delays them.
Write captures evict the oldest retained changes before they are refused, so only readers below the evicted floor resync.
An admitted frame remains valid when output shares change, and teardown releases ownership before rewatch.

## Durable history

Activation drains table work and syncs an exact pre-slot baseline before persisting its initial frontier.
Checkpoint durably links the old base image, publishes the new live image and archives the complete WAL instead of deleting it.
Catch-up replays archive/live prefixes through the durable frontier and follows rollback decisions, merging transaction frames and historical schemas.
Immutable archive truncation and damage in a completed live prefix are errors; ordinary torn crash tails are excluded during validation.
Boundary publication tokens prevent delayed catch-up seeding from replacing newer chunk observations.
Readers pin retention through store reopen; files permit delete sharing on Windows.
Frontier passes capture completed producer state, sync files outside chunk locks and persist only that captured revision.
ACK positions are bounded by fully sent events and combined per table; only persisted positions release archives.
Retention loss is recorded durably before history is released.

## Internal C++ surface

Table subscriptions return typed change, schema, resync and end entries.
Archive readers capture a durable frontier, return changes after their input position and then finish; they never repair files.
Slot/reader creation uses exclusive table admission, so callers release an ordinary table lease first.
These C++ interfaces are internal and are outside the public compatibility promise.

## Acknowledgement persistence

Eligible ACKs share a table batch persisted at most every 100 ms.
UNWATCH persists its own eligible ACK before replying, syncing its position first when necessary.
An already admitted change remains deliverable after a later limit reduction.
New admission waits until charged output fits the current table budget.
