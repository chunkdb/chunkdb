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
- Inside a transaction only the statements above, `DESCRIBE` and `PING` are allowed; `SCAN CHUNKS`, table and user statements, `FLUSH WAL` and `SHOW` get `-ERR INVALID_ARGUMENT`. `IF VERSION` is refused too: `COMMIT` checks every chunk the transaction touched.
- A failed statement inside a transaction changes nothing and leaves the transaction open, except `CONFLICT`, which ends it: its snapshot and writes are dropped, and until `COMMIT` or `ROLLBACK` every statement answers the same `CONFLICT`, as an aborted transaction does in PostgreSQL, so pipelined statements sent after it never run outside the transaction.
- `COMMIT` and `ROLLBACK` end the transaction whatever they answer. `ROLLBACK` without a transaction answers `+OK`, so a retry loop may always send it. `BEGIN` inside a transaction gets `-ERR INVALID_ARGUMENT`.
- A closed connection rolls its transaction back. A transaction holds no lock and no table lease between statements.
- Rights are checked per statement as outside a transaction, and `COMMIT` checks `WRITE` again when the transaction wrote.
- A table opened read-only or shared by several writer processes refuses transactions with `-ERR INVALID_ARGUMENT`: its history would not see every write.

## Snapshot

- Chunk versions come from one clock per table, and every write takes its version under its chunk's lock before it changes the chunk, so in each chunk version order is apply order.
- The first statement takes the snapshot, as in PostgreSQL's `REPEATABLE READ`: `S` is the last version the clock issued. Under the table's history lock it counts itself as open, reads `S` and registers it. Every read in the transaction then sees the table as of `S`, plus the transaction's own writes.
- After taking its version, a write loads the count of open transactions; the clock and the count are sequentially consistent. With none open it goes on as today: one atomic load is its whole cost.
- With some open, the write copies the chunk's state (data and version) before changing it and, under the history lock, keeps the copy tagged with its new version unless a kept state of that chunk is already tagged above the newest registered snapshot. A transaction registering at the same time either got an `S` below the write's version, so the write sees it under the lock, or above it, so it needs nothing.
- A read of chunk `c` at `S` reads the current state under the chunk's lock first, then, under the history lock, takes the oldest kept state of `c` tagged above `S` instead when there is one. `GET AREA` reads chunks that are not in memory from disk in the same order: the file first, then the history.
- Kept states are dropped once no registered snapshot is older than their tag. The history lock is the innermost lock: nothing takes a chunk lock or loads a chunk while holding it.
- A kept state is copied before the write's commit point and inserted without allocating, so a write never fails after its WAL commit because of the history. A write that fails after keeping its copy leaves a kept state behind, which can only cause a needless `CONFLICT`.
- Writes inside a transaction go to private copies of the chunks it writes, taken from the snapshot; reads in the transaction see those copies, and report the chunk's version as of the snapshot. Nobody else sees them before `COMMIT`.
- A table altered or dropped after `S` (its store generation changed) ends the transaction at its next statement with `CONFLICT`.

## Commit

`COMMIT` of a transaction that wrote:

1. Check that the table is not fail-closed. Load and pin the chunks the transaction read or wrote, then lock them in coordinate order: written chunks exclusively, chunks only read shared. Plain writes take one chunk lock at a time, so the order cannot deadlock with them or with other commits. A thread keeps the set of chunks it holds, and the WAL stream pool skips them when it closes idle streams.
2. Check, under the history lock, that the transaction is still registered and that no chunk it read or wrote has a kept state tagged above `S`. Otherwise answer `CONFLICT` and change nothing. With every touched chunk locked, the result is serializable: two transactions that read each other's written chunks cannot both commit.
3. For each written chunk in turn: flush its staged WAL batch, fsync the WAL file and its directories, and close its stream. Each WAL's size is then a durable boundary, in every durability mode.
4. Take one version `T` and one commit time from the clock; every written chunk gets them.
5. Write the transaction intent `.chunkdb.intents/txn-<T>.rollback` (`CKTB`): `T` and, per written chunk, its coordinates and WAL boundary. Sync the file and the directory.
6. For each written chunk in turn: open its WAL, append one frame with revision `T` holding the differences between its current state and its copy, fsync, close. A chunk whose copy equals its state is skipped.
7. Replace the intent with its commit form (`CKTC`) and sync: the commit point.
8. Swap each chunk's state for its copy and set its version to `T`, keeping the old state in the history when registered snapshots need it (no allocation: the old state moves into a node made before step 5). Unlock, remove the intent, and run the usual checkpoint checks. This is also where the change feed (#65) will receive the transaction's changes together.

- `COMMIT` is durable when it answers, in every durability mode. It costs, per written chunk, two syncs of the WAL, plus the syncs of the intent's two atomic replacements and their directories.
- Steps 5–7 run inside one snapshot-generation write guard, so read-only processes do not take a chunk in the middle of a commit.
- A failure before step 7 truncates the WALs back to their boundaries and removes the intent; the answer is an error and nothing changed. When that repair fails, every written chunk not confirmed truncated is marked so that it is not evicted, and the table becomes fail-closed until restart, whose recovery truncates them; the answer still means nothing changed.
- The WAL frames are ordinary frames, and revisions still rise in each WAL: `T` is taken after every written chunk is locked and flushed.
- Plain reads outside a transaction stay per chunk: a `GET AREA` may see a commit on some chunks if it read the others before the commit. A transaction gives one consistent view.

## Recovery

- A writer's open resolves transaction intents before it serves, next to conditional intents. `CKTB` (crashed before the commit point): truncate each listed WAL to its boundary, removing a WAL whose boundary is empty. `CKTC`: keep the frames; a WAL already checkpointed away is fine. Then remove the intent and sync the directory. Recovery is idempotent, so a crash during it is resolved at the next open.
- WAL replay stays lazy per chunk; after the intents are resolved every WAL holds all of a transaction's frames or none.
- A read-only process reads each chunk a pending intent lists as of the intent's decision, without changing files, as it does for conditional intents.
- Transaction intents are part of storage format 2: every 2.x binary resolves them, and `chunkdb_verify` and the temporary-file cleanup know them. STORAGE_FORMAT.md describes `CKTB` and `CKTC`.

## Limits

- `--txn-max-duration-ms` (default 5000): a transaction older than this is unregistered by the table's next history operation or its own next statement, whichever comes first, and that statement or `COMMIT` answers `CONFLICT`. History is pruned only below registered snapshots, and step 2 and every snapshot read check the registration under the history lock, so an unregistered transaction never reads or commits from pruned history.
- A transaction writes at most 64 chunks and reads at most 1024; a statement past either gets `-ERR INVALID_ARGUMENT` and the transaction stays open.
- `--txn-max-bytes` (default 16 MiB): the private copies of one transaction. `--txn-total-bytes` (default 256 MiB): the private copies of all transactions on the server. A write past either gets `-ERR INVALID_ARGUMENT` and the transaction stays open.
- `--txn-history-bytes` (default 64 MiB per table): the kept states. When a write would pass it, the oldest open transactions of the table are unregistered, ending with `CONFLICT`, until it fits; plain writes never fail for it.

## Errors

- `CONFLICT <reason>`: the transaction ended without writing anything; running it again may succeed. Reasons: a chunk changed after the snapshot, the duration limit, the history limit, the table was altered or dropped.

## Cost for plain operations

- Reads: none. Writes: one atomic load while no transaction is open; with open transactions, a copy of the chunk's state and the history lock, at most one kept state per chunk per snapshot.
- The hot-path budget (5%, 15 alternating runs) applies to plain block and chunk writes with no open transaction.

## Tests

- Crash at each step from 3 to 8 and during recovery (failpoints in a child process): after reopen every written chunk shows all of the transaction or none of it; a step-3 or step-6 failure answers an error and changes nothing; a failed repair keeps the chunks and the table fail-closed.
- Kill test: clients move amounts between counters in random chunks inside transactions, the server is killed with `SIGKILL` and restarted; the total never changes and every acknowledged `COMMIT` is present, in relaxed and synced modes.
- Snapshot: a transaction reads chunk A, a plain write changes A and B, the transaction reads B and sees the old B; a writer racing a registering transaction (pause hook) is kept.
- Isolation: `COMMIT` after a write to a read chunk answers `CONFLICT`; two transactions each reading what the other writes do not both commit.
- Concurrency: increments in transactions from many connections under TSan; the final count equals the acknowledged commits.
- Limits: duration, chunk counts, private and history bytes, disconnect, `ALTER TABLE` during a transaction, read-only and multi-process tables.
