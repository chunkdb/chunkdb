# Replication feed

`WATCH t REPLICATION` streams everything a mirror needs to keep a copy of table `t` identical to the primary: same epoch, revisions, chunk versions, values, tombstones and schema (DESIGN-2.0 F3, #146 box 3).
`APPLY` (box 4) consumes it; the follower program comes after 2.0.

## Statement

`WATCH t REPLICATION [AFTER epoch revision]` needs the REPLICATION server right (granted ON *); READ on `t` is not required.
It takes no AREA, COLUMNS, NO BEFORE, CHUNKS or SLOT: a mirror needs the whole table.
HELLO adds feature `replication_watch`.
The reply is `{epoch, revision}` as for WATCH; AFTER follows the ordinary resume rules, and a position the live history no longer holds receives `resync` (the mirror then copies the table again).
`AFTER epoch 0` continues on a table with no commit and no schema change, whatever its revision: a new table's clock starts at the directory's version floor, so its revision can be above 0 before its first commit.
A follower seeded from a backup resumes `AFTER epoch cut`; the program starts the replication watch before BACKUP, so the cut stays in the live history.
Durable gap-free delivery through slots is not part of this box: a mirror that must survive primary restarts without a copy uses a slot when slots accept REPLICATION (later, if needed).

## Pushes

One push per commit, in commit order, with every chunk the commit changed:

`[replication, epoch, revision, commit_time_ms, user, schema_version, chunks, tag, txn, txn_tables]`

The sender uses ten fields, with `tag` and `txn` null for the current single-table APPLY.
APPLY also accepts its original nine-field single-table form, which omits `txn_tables`.
When present, `txn_tables` must be null or an array containing only the name of the follower selected by `epoch`.
The sender uses null for single-table commits so they can apply to a follower with a different name.

Every commit that took a revision is sent, also one whose writes changed no value: each chunk it wrote is listed, since the chunk took the revision as its version.
Each chunk is `[cx, cy(, cz), version, kind, data]`:
- `version` is the chunk's version after the commit, which equals `revision` (A6).
- `kind` `form`: `data` is the chunk's complete form after the commit, as GET CHUNK returns it, or null when the commit removed the chunk. Used for chunk-level writes (SET CHUNK, transactions, conditional chunk writes), whose preimage the feed already captures whole.
- `kind` `rows`: `data` is `[blocks, chunk_row]`: the blocks the commit wrote `[x, y(, z), after]` with full rows in schema order (null for a deleted block), and the chunk-column row after the commit (null when unchanged). Each value is a bulk string in its parameter encoding (PROTOCOL.md, Parameters), null for NULL: a RESP3 double would lose a NaN's sign and payload, and a follower must hold the primary's bytes. Used for SET BLOCK and DELETE BLOCK, whose preimage is one row; no whole-chunk copy is forced on the writer.
Before rows are not sent: a mirror at the previous revision already has them.

Other pushes on the same stream:
- `[schema, map]` before the first change at a new schema version: the DESCRIBE column maps plus `epoch`, so APPLY finds the table without a name, `checks` and `step`, the schema change that produced the version (`add`, `drop`, `rename`, `retype`, `attributes`, `add_check`, `drop_check`, `chunk_*`) with the column id it touched, so a mirror replays the same history and keeps the same `schema_version`.
- `[collect, epoch, revision, chunks]`: empty chunks a checkpoint collected, each `[cx, cy(, cz), version, commit_time_ms]` as its tombstone records them; `revision` is the completed revision when it ran. A follower never collects on its own.
- `[compact, epoch, revision, before]`: COMPACT TOMBSTONES dropped deletion identities older than `before`.
- `[progress, epoch, revision]` and `[resync, epoch, revision, parent_epoch, fork_revision]` as for WATCH.

## WATCH CATALOG marker

The whole-catalog event of WATCH CATALOG gains an eighth element, `whole` (true; false in every other catalog event), so APPLY CATALOG can drop followers whose epoch the whole catalog no longer lists; arrays grow at the end, so older clients ignore it.

## Capture

The writer path is unchanged: SET BLOCK keeps its one-row preimage, chunk writes their whole-chunk preimage.
The sender decodes each frame once more for a replication watcher: chunk frames into a form (the CHUNKS encoder), block frames into after rows (already decoded for `change`).
The encoded frame is shared by every replication watch of the table, like an unfiltered change.
Without a replication watch nothing is decoded or encoded for it.

For a replication watch the decoder lists every chunk a frame wrote, changed or not, and a commit whose writes changed no value is sent rather than dropped as an empty change.
Collection and COMPACT TOMBSTONES append a feed entry while the feed is attached; today they append nothing (checkpoint.cpp:282, table_tombstones.cpp).
Schema entries already take a revision on the live ring; they gain `checks` and `step`.

## What waits elsewhere

- `txn` and `tag` stay null until #136 puts group ids and tags into WAL frames.
- A future group push will carry a nullable u64 `txn` and a `txn_tables` list of member table names so APPLY GROUP can collect every member.
- Deletes by expiry (E8, `expire_column`, #279) are ordinary commits of user `@expire` through the chunk path, so they arrive as `form` chunks with `user` `@expire`; a follower deletes nothing on its own and applies them.
- `APPLY`, follower tables and their refusals are box 4.

## Costs

No cost without a replication watch.
With one, the sender encodes a form per chunk-level write and the rows the change push already decodes; the writer pays what an unfiltered WATCH costs.

## Tests

Protocol: rights (REPLICATION needed, READ alone refused), refused filters, push shapes for SET BLOCK, DELETE BLOCK, SET CHUNK, a transaction over several chunks, chunk columns, schema steps, collection and COMPACT TOMBSTONES, AFTER and resync.
Mirror check: replaying the pushes into a second table through GET/SET primitives reproduces GET CHUNK bytes and versions (box 5 replaces this with APPLY).
