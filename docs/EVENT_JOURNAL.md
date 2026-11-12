# Event journal

chunkdb keeps the current state and a bounded change feed; a durable slot lets your application copy every committed change into a journal in its own store.
The [Go journal example](https://github.com/chunkdb/chunkdb-go/blob/main/examples/journal/main.go) is a runnable collector that follows this guide.

## Collect changes

Create the slot before the writes you need to keep:

```cql docs-check session=main
CREATE TABLE journal_demo (kind u8) CHUNK 2 x 2
SET BLOCK 0 0 IN journal_demo kind = 1
CREATE SLOT 'journal' ON journal_demo
SET BLOCK 1 0 IN journal_demo kind = 2
```

```output
OK
1
OK
2
```

The slot holds only the change committed after CREATE SLOT:

```sh docs-check-replay
chunk-cli --uri "$CHUNKDB_URI" watch journal_demo --slot journal
```

```output
start <epoch>:1
schema <epoch>:2 version 1
change revision 2 time_ms <time-ms> user admin
  block 1 0: (absent) -> {kind = 2}
```

The CLI acknowledges after printing, so a collector uses a client API instead.
For each change, it writes the epoch, revision, schema version, `time_ms`, user, blocks and chunk metadata rows to its store in one write, waits for the store's own sync, such as `fsync` of an append-only file, and only then sends ACK for that revision.
The revision is the ordering key; `time_ms` comes from the server clock and can move backward.
It also stores each schema event with its version, columns and chunk columns; schema events need no ACK.
A slot with AREA also needs progress acknowledged; store the progress position before you acknowledge it.

## Take a base snapshot

Changes before CREATE SLOT are only in the table state, so a journal starts with a base snapshot taken after CREATE SLOT.
Read the table with GET AREA, or with SCAN CHUNKS followed by GET CHUNK for a larger table:

```cql docs-check session=main
--blocks GET AREA 0 0 TO 0 0 FROM journal_demo
```

```output
chunk 0 0
  version = 2
  schema_version = 1
  commit_time_ms = <time-ms>
  present = 2 of 4 blocks
  block 0 0
    kind = 1
  block 1 0
    kind = 2
```

A chunk version is the revision of the last change to that chunk, so this chunk already contains revision 2.
Read `version` and `revision` from DESCRIBE before the first read and after the last one, and take the snapshot again when `version` differs.
Store each chunk with its version, and the snapshot with the epoch, the schema version, the first `revision` as its start, and the larger of the second `revision` and the highest chunk version as its position.
Plain reads do not share one snapshot; the chunk versions make reading while writes continue safe.

## Rebuild a point in time and render a timelapse

1. Load a snapshot of the same epoch whose position is at or before your target revision.
2. Replay the journal entries after the snapshot's start in revision order up to the target revision, applying an entry to a chunk only when its revision exceeds that chunk's snapshot version, and every entry to a chunk missing from the snapshot, which was empty when read.

Each entry holds the values of its blocks after the change, where an absent value deletes the block, and each changed chunk metadata row after the change.
A slot with CHUNKS delivers each changed chunk's whole form instead; applying it replaces the chunk, and a null form deletes it.
To rebuild a commit time, replay in revision order and stop before the first entry whose `time_ms` is later, without sorting entries by `time_ms`.
A schema change converts every stored block and chunk metadata row, but the journal holds only the rows written after it.
A replay stops at the first entry with a schema version above its snapshot's, so take a new base snapshot after each schema change; targets between the change and that snapshot's position cannot be rebuilt.
Keep the journal as segments, files that each start after a base snapshot or after the previous segment.
A timelapse is one replay: load a snapshot, replay the following segments in order, and render a frame each time the replay passes the next revision or time step; at a schema change it continues from the next snapshot.

## Retention

The slot retains unacknowledged changes up to the table's `slot_max_bytes`, 1073741824 bytes by default.
When the collector falls further behind, the slot becomes lost: WATCH answers SLOT_LOST and the journal has a gap.
Then drop and recreate the slot, take a new base snapshot, and start a new segment; the changes between the last stored entry and that snapshot are not in the history.
chunkdb discards acknowledged changes, so the application owns the history: take snapshots periodically; a snapshot needs every later segment, so delete old segments together with the snapshots before them.

## Resume after a crash

ACK persistence is asynchronous, so after a crash the slot can deliver changes the store already holds.
Make store writes idempotent by `epoch:revision`, and skip a change at or below the last stored revision of that epoch, and a schema event whose version is already stored, because a resumed watch describes the schema again.
On start, read the last stored change and resume after it with `WATCH journal_demo SLOT 'journal' AFTER epoch revision`, so a crash between the store's sync and ACK neither loses nor repeats an entry.
Remove an incomplete last entry; it was never acknowledged, so the slot delivers it again.

- `resync`: the history after your position is unavailable, for example after the table was recreated; start a new segment from a base snapshot.
- `SLOT_LOST`: the collector fell behind, or a projected column was dropped (`reason=schema_filter`); follow Retention.
- `SLOT_BUSY`: another collector, or the connection of the crashed one, holds the slot; stop it or watch with TAKEOVER.
- `NO_TABLE`: the table was dropped or your user lost READ on it.

See [Change feed](CHANGE_FEED.md), [Resume a consumer](FEED_RESUME.md), and [Feed statements](reference/FEED.md).
