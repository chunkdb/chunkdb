# Transaction snapshots and commits
The [transaction guide](../TRANSACTIONS.md) defines statements and limits; [storage](../STORAGE_FORMAT.md) defines checked transaction intents.

## Snapshot history

A connection's first table statement registers a table revision snapshot.
Reads combine that snapshot with private written-chunk copies; other sessions cannot see those copies.
Plain writers retain previous chunk states while registered snapshots need them.
History registration and checks are serialized, and the history lock is innermost: no chunk loading or locking occurs under it.
Expiry, history pressure, table generation changes and conflicting chunk revisions terminate a transaction with CONFLICT.
The aborted state remains on the connection until COMMIT or ROLLBACK so pipelined statements cannot run outside it.
A scan at a READ ONLY snapshot first waits until every write the snapshot includes has completed, so the coordinate index lists every chunk those writes populated; writers wake it as they clear their producer bounds, only while a waiter is counted, and the snapshot deadline and statement stop end the wait. A COMMIT clears its bound once its outcome is settled, before it removes its intent and runs its inline checkpoints, so the scan does not wait for that work. Point and area reads need no wait.
Its SCAN CHUNKS pages take the coordinates populated now or changed after the snapshot, releasing the index lock between batches, and read each at the snapshot; a write publishes the revision before it removes a coordinate from the populated set.
It keeps no read set, and its COMMIT only unregisters the snapshot.
Writes count the states they copy for open snapshots, and their bytes, under the history lock; with no snapshot registered they copy nothing.

## Durable commit

Commit loads and pins all read/written chunks and locks them in coordinate order, with read-only chunks shared.
It rechecks snapshot registration and conflicts, establishes durable previous WAL boundaries, and reserves one revision/time for changed chunks.
A synced rollback intent lists those boundaries before any new frame is appended.
Every changed chunk receives one synced frame; publishing a synced committed intent commits them all before memory swaps expose their new states.
Rollback truncates to the recorded boundaries; failed repair pins affected chunks and fences writes until RECOVER TABLE or a restart.
Cleanup after durable commit cannot turn success into rejection; an ambiguous durability decision reports an unknown outcome.
Recovery is idempotent and read-only observers obey the decision without modifying files.
Feed publication groups the frames under one revision and completes postcommit bookkeeping before releasing its producer bound.

Limits bound duration, chunk counts, private bytes and retained history; [server flags](../SERVER_FLAGS.md) list defaults.
Plain reads remain per chunk; only transaction reads provide a common snapshot across those chunks.
