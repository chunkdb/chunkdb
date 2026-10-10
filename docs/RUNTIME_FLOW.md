# Runtime behavior in 2.0

The complete statement reference is [CQL](CQL.md).
Every table statement holds a lease so DROP or ALTER cannot replace the store while that statement uses it.

## Reads and writes

A point read loads its chunk image and replays valid WAL frames if the chunk is absent from cache; it does not checkpoint merely because it read data.
Absent blocks return null; present rows follow the typed schema.
Writes validate inputs and conditional versions before changing state, log one WAL frame and acknowledge according to the table's durability mode.
SET CHUNK carries revision, schema version, presence, packed payload and variable values; a mismatched schema is refused.
Chunk versions survive eviction/restart and compare only for equality.

Area and scan reads can read cold image/WAL state without filling the chunk cache; contended reads can use the authoritative cached path.
SCAN lists populated chunks in coordinate order and uses its last coordinate as the next cursor.
Neither operation is a global snapshot outside a transaction.

## Transactions and metadata

Transactions use private chunk copies and a registered table snapshot until durable COMMIT or ROLLBACK.
Conflicts discard their work; [transactions](TRANSACTIONS.md) describes the aborted connection state and retry pattern.
ALTER drains table work, publishes schema/options and reopens the store.
MIGRATE commits its prepared metadata and record through a durable redo journal; failed completion requires writer recovery.

## Persistence and streams

Checkpoints replace images atomically and remove/archive WALs only after preserving the required durability floor.
Eviction flushes pending batches and later loads recover the same chunk version.
FLUSH WAL is a barrier across all tables ([durability](DURABILITY_CONTRACT.md)).
WATCH emits completed changes in revision order; durable slots send only through persisted frontiers ([change feed](CHANGE_FEED.md)).
BACKUP pins completed per-table cuts, releases pin holds before copying and publishes its checked completion record last ([backup](BACKUP.md)).
SHOW METRICS returns Prometheus text and requires ADMIN on *; it is not an HTTP endpoint.
