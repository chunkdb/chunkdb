# Transactions: design

Transactions group reads and writes of several chunks of one table: the reads see one consistent snapshot, and the writes apply all together or not at all, also across a crash (#64). This document fixes the statements, the snapshot, the commit protocol on top of the per-chunk WALs, recovery, and the limits.

## Statements

```text
BEGIN                                  -> +OK
GET BLOCK / GET CHUNK / GET AREA ...   -> as outside a transaction, from the snapshot
SET BLOCK / DELETE BLOCK / SET CHUNK   -> _ (the version is assigned at COMMIT)
COMMIT                                 -> :<version>, or _ when nothing was written
ROLLBACK                               -> +OK
```

- A transaction belongs to one connection and covers one table: the table of its first statement. A statement on another table gets `-ERR INVALID_ARGUMENT` and the transaction stays open.
- Inside a transaction only the statements above and `PING` are allowed; `SCAN CHUNKS`, table and user statements, `FLUSH WAL` and `SHOW` get `-ERR INVALID_ARGUMENT`. `IF VERSION` is refused too: `COMMIT` checks every chunk the transaction touched.
- A failed statement inside a transaction changes nothing and leaves the transaction open, except `CONFLICT`, which ends it.
- `COMMIT` and `ROLLBACK` end the transaction whatever they answer. `ROLLBACK` without a transaction answers `+OK`, so a retry loop may always send it. `BEGIN` inside a transaction gets `-ERR INVALID_ARGUMENT`.
- A closed connection rolls its transaction back.
- Rights are checked per statement as outside a transaction, and `COMMIT` checks `WRITE` again when the transaction wrote.

## Snapshot

- The first statement takes the snapshot: the table's version clock at that moment, `S`. As in PostgreSQL's `REPEATABLE READ`, every read in the transaction then sees the table as it was at `S`, plus the transaction's own writes.
- Chunk versions come from one clock per table, and a write takes its version under its chunk's lock, so for every chunk the writes with a version up to `S` are exactly the writes before the snapshot.
- While any transaction of the table is open, a write to a chunk first keeps the chunk's previous state (data, version) in the table's history, tagged with the new version. A write needs to keep one only when no kept state of that chunk is newer than the newest open snapshot, so a chunk keeps at most one state per snapshot.
- A read of chunk `c` at `S` takes the oldest kept state of `c` tagged above `S`; without one, the chunk's current state.
- Writers and `BEGIN` meet through the table's count of open transactions: a transaction counts itself before it reads the clock, and a writer reads the count after it takes its version, both sequentially consistent. A write either has a version at most `S` or sees the transaction and keeps the state.
- Kept states are dropped once no open snapshot is older than them. Without open transactions nothing is kept: a write pays one atomic load.
- Writes inside a transaction go to private copies of the chunks it writes, taken from the snapshot; reads in the transaction see those copies. Nobody else sees them before `COMMIT`.
- A table changed by `ALTER TABLE` or dropped after `S` ends the transaction at its next statement with `CONFLICT`.

## Commit

`COMMIT` of a transaction that wrote:

1. Load and pin the written chunks, then lock them in coordinate order. Plain writes take one chunk lock at a time, so the order cannot deadlock with them. The WAL stream pool skips chunks the committing thread holds when it closes idle streams.
2. Check, under the history lock: no chunk the transaction read or wrote has a kept state tagged above `S`. Otherwise answer `CONFLICT` and change nothing. Chunks only read need no lock: a later write to them orders after the commit.
3. Flush the written chunks' staged WAL batches and sync them, so each WAL's size is a known boundary.
4. Take one version `T` from the clock; every written chunk gets it.
5. Write the transaction intent `.chunkdb.intents/txn-<T>` (`CKTB`): `T` and, per written chunk, its coordinates and WAL boundary. Sync the file and the directory.
6. Append one frame per written chunk, revision `T`, holding the chunk's new state as differences from its current state; sync each WAL.
7. Replace the intent with its commit form (`CKTC`) and sync: the commit point.
8. Swap each chunk's state for its copy and set its version to `T`; the old state goes to the history when open snapshots need it. Unlock, remove the intent, and run the usual checkpoint checks.

- Steps 3–8 run inside one snapshot-generation write guard, so read-only processes do not take a chunk in the middle of a commit.
- `COMMIT` is durable when it answers, in every durability mode, like a conditional write. It costs one sync per written chunk plus three for the intent.
- A failure before step 7 truncates the WALs back to their boundaries and removes the intent; the answer is an error and nothing changed. When that repair fails, the table becomes fail-closed and the answer is `-ERR INTERNAL write outcome unknown`, as for other writes.
- The WAL format does not change: each frame is an ordinary frame, and revisions still rise in each WAL. Frames sharing revision `T` across chunks are one transaction, which the change feed can deliver whole.
- Plain reads outside a transaction stay per chunk: a `GET AREA` may see a commit on some chunks if it read the others before the commit. A transaction gives one consistent view.

## Recovery

- A writer's open resolves transaction intents before it serves, next to conditional intents. `CKTB` (crashed before the commit point): truncate each listed WAL to its boundary, removing a WAL whose boundary is empty. `CKTC`: keep the frames. Then remove the intent and sync the directory.
- WAL replay stays lazy per chunk; after the intents are resolved every WAL holds all of a transaction's frames or none.
- A read-only process treats a pending transaction intent like a pending conditional intent: it reads each listed chunk as of the intent's decision without changing files.

## Limits

- `--txn-max-duration-ms` (default 5000): a transaction older than this ends with `CONFLICT` at its next statement, and its snapshot no longer holds history.
- `--txn-max-bytes` (default 16 MiB): the private copies of one transaction; a write past it gets `-ERR INVALID_ARGUMENT` and the transaction stays open.
- `--txn-history-bytes` (default 64 MiB per table): the kept states. When a write would pass it, the oldest open transactions of the table end with `CONFLICT` until it fits; plain writes never fail for it.
- A transaction writes at most 256 chunks, the `GET AREA` limit.

## Errors

- `CONFLICT <reason>`: the transaction ended without writing anything; running it again may succeed. Reasons: a chunk changed after the snapshot, the duration limit, the history limit, the table was altered or dropped.

## Cost for plain operations

- Reads: none. Writes: one atomic load while no transaction is open; with open transactions, one copy of a chunk's state per snapshot that needs it.
- The hot-path budget (5%, 15 alternating runs) applies to plain block and chunk writes with no open transaction.

## Tests

- Crash at every step from 5 to 8 (failpoints in a child process): after reopen every written chunk shows all of the transaction or none of it.
- Kill test: clients move amounts between counters in random chunks inside transactions, the server is killed with `SIGKILL` and restarted; the total never changes and every acknowledged `COMMIT` is present.
- Isolation: a transaction reads chunk A, a plain write changes A and B, the transaction reads B and sees the old B; `COMMIT` after a write to a read chunk answers `CONFLICT`.
- Concurrency: increments in transactions from many connections under TSan; the final count equals the acknowledged commits.
- Limits: duration, private bytes, history bytes, 256 chunks, disconnect, `ALTER TABLE` during a transaction.
