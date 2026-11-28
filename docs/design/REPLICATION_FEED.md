# Replication feed

`WATCH t REPLICATION` streams everything a mirror needs to keep a copy of table `t` identical to the primary: same epoch, revisions, chunk versions, values, tombstones and schema (DESIGN-2.0 F3, #146 box 3).
`APPLY` (box 4) consumes it; the follower program comes after 2.0.

## Statement

`WATCH t REPLICATION [AFTER epoch revision]` needs the REPLICATION server right (granted ON *); READ on `t` is not required.
It takes no AREA, COLUMNS, NO BEFORE, CHUNKS or SLOT: a mirror needs the whole table.
HELLO adds feature `replication_watch`.
The reply is `{epoch, revision}` as for WATCH. AFTER resumes within a continuous interval of replication capture, and a position the live history no longer holds receives `resync` (the mirror then copies the table again).
A new capture on a table with existing history uses a fresh clock revision as its lower bound, since an unobserved collection can occur at an otherwise unchanged data revision.
`AFTER epoch 0` continues on a table with no commit and no schema change, whatever its revision: a new table's clock starts at the directory's version floor, so its revision can be above 0 before its first commit.
A follower seeded from a backup resumes `AFTER epoch cut`; the program starts the replication watch before BACKUP, so the cut stays in the live history.
Durable gap-free delivery through slots is not part of this box: a mirror that must survive primary restarts without a copy uses a slot when slots accept REPLICATION (later, if needed).

## Pushes

One push per commit, in commit order, with every chunk the commit changed:

`[replication, epoch, revision, commit_time_ms, user, schema_version, chunks, tag, txn, txn_tables]`

Every commit that took a revision is sent, also one whose writes changed no value: each chunk it wrote is listed, since the chunk took the revision as its version.
Each chunk is `[cx, cy(, cz), version, kind, data]`:
- `version` is the chunk's version after the commit, which equals `revision` (A6).
- `kind` `form`: `data` is the chunk's complete form after the commit, as GET CHUNK returns it, or null when the commit left the chunk empty. Used when the feed captures a whole-chunk preimage, including SET CHUNK, transactions, conditional chunk writes and writes captured for a simultaneous CHUNKS watch.
- `kind` `rows`: `data` is `[blocks, chunk_row]`: the blocks the commit wrote `[x, y(, z), after]` with full rows in schema order (null for a deleted block), and the chunk-column row after the commit (null when unchanged). Each value is a bulk string in its parameter encoding (PROTOCOL.md, Parameters), null for NULL. A RESP3 double would lose a NaN's sign and payload. Used for SET BLOCK and DELETE BLOCK captured with a one-row preimage.
Before rows are not sent: a mirror at the previous revision already has them.

Other pushes on the same stream:
- `[schema, map]` before the first change at a new schema version. The [feed reference](../reference/FEED.md#watch-replication) defines the column maps, `epoch`, `checks` and `step`.
- `[collect, epoch, revision, chunks]`: a checkpoint published an empty chunk boundary, with `[cx, cy(, cz), version, commit_time_ms]` as its tombstone records it. A follower never collects on its own.
- `[compact, epoch, revision, before]`: COMPACT TOMBSTONES dropped deletion identities older than the effective `before` bound.
- `[progress, epoch, revision]` and `[resync, epoch, revision, parent_epoch, fork_revision]` as for WATCH.

## WATCH CATALOG marker

The whole-catalog event of WATCH CATALOG gains an eighth element, `whole` (true; false in every other catalog event), so APPLY CATALOG can drop followers whose epoch the whole catalog no longer lists; arrays grow at the end, so older clients ignore it.

## Capture

The writer path is unchanged: SET BLOCK keeps its one-row preimage, chunk writes their whole-chunk preimage.
The sender uses the existing WAL replay to encode chunk forms and parameter rows. It reuses a changed block's decoded values for the ordinary `change` push.
The encoded frame is shared by every replication watch of the table and charged to the live ring. Replication encoding is active while watched, or while the last replication watch's history lingers with no other subscriptions.

For a replication watch the decoder lists every chunk a frame wrote, changed or not, and a commit whose writes changed no value is sent rather than dropped as an empty change.
Collection and COMPACT TOMBSTONES append a feed entry after the coordinate-index decision. They take separate clock revisions while replication capture is enabled, so a resume after the last data revision can still receive the maintenance event. Pending events merge below the same drained cut as writes, including during a store reopen.
Schema entries take a revision on the live ring. The replication stream has a push with `checks` and `step` for each version, including intermediate versions introduced in one reopen.

## What waits elsewhere

- `tag`, `txn` and `txn_tables` are null in the single-table stream. A future group push uses the nullable u64 `txn` and names its members in `txn_tables` so APPLY GROUP can recognize a complete group.
- Deletes by expiry (E8, `expire_column`, #279) use the chunk path with user `@expire` and are sent as `form` chunks when that producer is enabled.
- `APPLY`, follower tables and their refusals are box 4.

## Costs

The writer uses the ordinary WATCH capture path. An empty checkpoint checks for a replication feed under its existing publication lock.
With replication capture, the sender encodes and retains one shared replication frame per commit. A collection also reserves a clock position and enqueues a bounded frame under the existing checkpoint publication lock.

## Tests

Protocol: rights (REPLICATION needed, READ alone refused), refused filters, push shapes for SET BLOCK, DELETE BLOCK, SET CHUNK, a transaction over several chunks, chunk columns, schema steps, collection and COMPACT TOMBSTONES, AFTER and resync.
The wire round trip decodes actual sender output with the documented APPLY commit parser shape and checks GET CHUNK bytes and versions. The mirror replay through APPLY is box 5.
