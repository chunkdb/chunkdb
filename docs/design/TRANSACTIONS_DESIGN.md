# Transaction snapshots and commits in 2.0

The [transaction guide](../TRANSACTIONS.md) defines statements and limits; [storage](../STORAGE_FORMAT.md) defines CKTB and CKTC.

## Snapshot history

A connection's first table statement registers a table revision snapshot.
Reads combine that snapshot with private written-chunk copies; other sessions cannot see those copies.
Plain writers retain previous chunk states while registered snapshots need them.
History registration and checks are serialized, and the history lock is innermost: no chunk loading or locking occurs under it.
Expiry, history pressure, table generation changes and conflicting chunk revisions terminate a transaction with CONFLICT.
The aborted state remains on the connection until COMMIT or ROLLBACK so pipelined statements cannot run outside it.

## Durable commit

Commit loads and pins all read/written chunks and locks them in coordinate order, with read-only chunks shared.
It rechecks snapshot registration and conflicts, establishes durable previous WAL boundaries, and reserves one revision/time for changed chunks.
Synced CKTB lists those boundaries before any new frame is appended.
Every changed chunk receives one synced frame; publishing synced CKTC commits them all before memory swaps expose their new states.
Rollback truncates to CKTB boundaries; failed repair pins affected chunks and fences writes until restart.
Cleanup after durable commit cannot turn success into rejection; an ambiguous durability decision reports an unknown outcome.
Recovery is idempotent and read-only observers obey the decision without modifying files.
Feed publication groups the frames under one revision and completes postcommit bookkeeping before releasing its producer bound.

Limits bound duration, chunk counts, private bytes and retained history; [server flags](../SERVER_FLAGS.md) list defaults.
Plain reads remain per chunk; only transaction reads provide a common snapshot across those chunks.
