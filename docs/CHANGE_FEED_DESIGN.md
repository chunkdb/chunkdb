# Change feed: design

The change feed streams every committed change of a table to subscribers: application servers that share a database, and processes that pass changes on to a journal or another system (#65). This document fixes the statements, the order of changes, the in-memory feed, named slots on top of the per-chunk WALs, recovery, and the costs.

## Statements

```text
WATCH t [AREA cx0 cy0 TO cx1 cy1] [AFTER epoch rev]           -> +OK epoch rev, then pushed changes
WATCH t SLOT 'name' [AREA ...] [AFTER epoch rev]              -> +OK epoch rev, then pushed changes
ACK rev                                                       -> (a slot's watch; no reply)
UNWATCH                                                       -> +OK after the last push; statements again
CREATE SLOT 'name' ON t | DROP SLOT 'name' ON t               -> +OK
SHOW SLOTS [ON t]                                             -> *n of {table, name, epoch, acked, retained_bytes, lost}
```

- A watch turns the connection into a stream: the server pushes RESP3 push frames (`>`) and reads only `ACK` and `UNWATCH`. `+OK epoch rev` names the position the stream starts after. Area coordinates are chunk coordinates; a transaction is cut to its part inside the area.
- A change: `> [change, epoch, revision, commit_time_ms, user, schema_version, blocks]`, `blocks` holding per changed block `[x, y, before, after]`, each the block's values in schema order or `_` for an absent block, typed as in `GET BLOCK`. Commit times come from the clock and may step back across a restart; the revision orders changes.
- `> [schema, epoch, revision, version, columns]` comes when the table's columns change, with the new columns as `DESCRIBE` gives them, before any change that follows them.
- `> [resync, epoch, revision]`: the feed cannot continue from the subscriber's position. Every revision up to the one given is finished; the subscriber reads the state again (`GET AREA`, `SCAN CHUNKS`) and applies a later change to a chunk only when its revision is above the version it read for that chunk.
- `WATCH` and `WATCH ... SLOT` need `READ` on the table; `CREATE SLOT` and `DROP SLOT` need `ADMIN`. Read-only and multi-process tables refuse watches. `--max-watches` (64) bounds the watches of a server.

Unscoped `SHOW SLOTS` skips tables dropped while it lists them; `SHOW SLOTS ON t` retains `NO_TABLE` for a dropped table.

## Order and positions

- A position is `(epoch, revision)`. The epoch is the table's store id; a restore from backup gives a new one (#66). Until #66, a position at or above the table's clock ceiling answers `resync`.
- One change per revision: a block write, a chunk write, or a transaction commit, whose frames share their revision. Revisions have gaps. An empty-chunk collection frame is flagged and is not a change.
- A change goes out only once every lower revision has finished (committed or failed). While a table has a feed, a writing thread publishes in its own slot of the table's feed a lower bound of the version it is taking (`clock.load()`), takes the version, and clears the slot when the write ends; the feed reads the clock first, then the slots, and its watermark is the lower of the two minus one. All of it is sequentially consistent. Writers never wait for the feed.
- A feed or slot is turned on or off under the table's exclusive lease, with no write in flight, so it starts exactly at the clock.

## In-memory feed

- The feed belongs to the table and survives `ALTER TABLE`; a schema change pushes `schema` at the reopened store's clock. `DROP TABLE` drops the table's slots and ends its watches with `-ERR NO_TABLE`.
- A table with a watch or a slot has a ring buffer (`--feed-buffer-bytes`, 64 MiB per table), freed when its last watch and slot go.
- Under its chunk's lock a write copies its raw WAL frame and the bytes it replaces, which it already keeps for rollback, into its thread's queue for the table; nothing is decoded there and no lock is shared between writers. Each queue is in revision order; the table's sender thread merges the queues up to the watermark, decodes each change once, and appends it to the ring buffer. The queues count toward the buffer bytes; past them writers drop their changes and every watch of the table gets `resync`.
- A feed thread serves every watch: after `+OK` the connection's socket and TLS session move from the worker to it, and it polls them with a wake-up channel, writing without blocking. A watch has no idle timeout. A watch that falls more than the buffer behind gets `resync`; one change larger than the buffer gives `resync` too.
- Without a slot, `AFTER` continues from the buffer while the position is in it, else `resync`. After a restart every watch without a slot gets `resync`.

## Slots

The storage API implements slot records, archival, retention and typed archive
reading. CQL slot management, WATCH SLOT and batched ACK expose this history over
the protocol. The implemented behavior is documented in
[CHANGE_FEED.md](CHANGE_FEED.md#durable-slots).

A slot delivers every change after its acknowledged position, across restarts of the server and of the subscriber.

- **Archives instead of removal.** While a table has slots, a checkpoint first writes the chunk's staged batch into its WAL (so no change reaches the image only), hard-links the image the WAL's frames apply over to `.chunkdb.feed/C_<cx>_<cy>.<first>.chk` (once per WAL, named by its first revision; none when the chunk had no image), writes the new image as today, and then renames the WAL to `.chunkdb.feed/C_<cx>_<cy>.<first>-<last>.wal` instead of removing it. An empty-chunk collection archives the same way. A crash between the steps leaves the live image and WAL as today; the next checkpoint finds the base already linked. The live WAL, chunk loads and checkpoint scheduling stay as today, and a write costs no extra bytes.
- **Before and after from replay.** A slot replays an archive's frames over its base image, as a load replays a WAL, and takes each change's `before` from the state the replay holds; frames below the slot's position only advance that state. The live WAL replays over the current image. Frames of a table with slots carry the writing user (`USER` TLV, tag 3, part of storage format 2).
- **Release.** A background pass deletes archives whose last revision is at or below every active slot's written position. A slot whose archives pass `--slot-max-bytes` (1 GiB) is marked lost with a log line; its next `WATCH` gets `-ERR SLOT_LOST`.
- **Catch-up.** A slot behind the in-memory feed merges its archives and the live WALs from its position: one cursor per file, ordered by the archive names' revision ranges, opened only when the merge reaches them, frames sharing a revision joined into one change. It then joins the in-memory feed. Readers open files with sharing that allows rename and removal and never trim them. Offline readers honour rollback intents and ignore a partial final live-WAL frame. Online readers capture completed live-WAL byte boundaries through the persisted durable frontier; truncation inside that prefix or an immutable archive is damage. On restart, the boundary index uses full WAL replay validation over the image state and excludes ordinary torn crash tails. Trimming a live WAL also truncates its index under the publication lock.
- **Only durable changes.** A slot sends a revision only once it is durable. In synced modes that is the watermark. In `relaxed` mode a table with slots writes its staged batches every `--slot-sync-ms` (100 ms) under each chunk's lock without syncing, syncs the files outside the locks, and moves the durable watermark to the watermark read before it started. The durable watermark is written to `chunkdb.slots`; after a restart, WALs holding frames above it are synced before those frames are sent. A fail-closed table freezes its durable watermark until restart. Persisting an advanced frontier explicitly wakes catch-up workers and feed I/O; their periodic waits remain housekeeping fallbacks.
- **Acknowledgement.** `ACK rev` above the last revision sent is refused. The server combines pending positions from every watch of a table in one ACK batch, persisted to `chunkdb.slots` (checksummed, replaced atomically, synced) at most every 100 ms and on `UNWATCH`. Durable-frontier passes and slot management persist metadata separately. Archives are released only below written positions. After a restart a slot resumes after its written position, so pending acknowledgements come again; a subscriber that needs exactly once keeps its position with its output and watches `AFTER` it.

A slot change and its schema batch must fit the watch's current output share on admission. Once admitted, it remains deliverable if another watch reduces the share; later admissions use the new share.

## Costs

- A table without watches and slots: one atomic load per write.
- With a watch: the version publish in the thread's slot and a copy of the frame and replaced bytes under the chunk's lock.
- With slots: the `USER` TLV in each frame, a hard link and a rename per checkpoint, archives kept until acknowledged, and in `relaxed` mode the background sync. A transaction or conditional write in flight holds the watermark through its syncs.
- The hot-path budget (5%, 15 alternating runs) applies to plain writes without a watch, and is measured with one watch and with one slot, in `relaxed` and `fsync-wal`.

## Measurements

Taken on the bench scenarios (200000 requests, 8 clients, keyspace 2048, relaxed, checkpoints off so every frame stays in its WAL; macOS, M-series):

| scenario | frames | WAL bytes per frame | bytes `BEFORE` records would add |
|---|---|---|---|
| world | 159985 | 126 | +60% |
| canvas | 203355 | 100 | +51% |
| simulation | 116275 | 575 | +91% |

Carrying the replaced bytes in each frame would grow the WAL by half to nearly double, so the design keeps base images instead and writes no extra bytes. Reading all frames of a table back (`chunkdb_verify`, warm cache) took 0.77 s for 20 MB in 16384 WAL files and 0.95 s for 67 MB in 16384 files, mostly opening files: catch-up over 1 GiB of archives takes seconds. The cost per write of the in-memory feed with one watch is measured with its implementation, against the budget below.

## Tests

- Order and values: concurrent writers on many chunks; each subscriber sees every revision once, in order, with the right before and after values; transactions whole, cut by areas.
- Slots across `SIGKILL` of the server and reconnects, in every durability mode: nothing lost, nothing repeated beyond the last written acknowledgement, and the changes replayed onto the starting state give the table's state; a relaxed checkpoint with a staged batch.
- Retention and release, a slot over its limit, `ALTER TABLE` and `DROP TABLE` while watching, a fail-closed table, `resync` for a slow watch, after a restart and on a stale epoch, `--max-watches`, TLS watches.
