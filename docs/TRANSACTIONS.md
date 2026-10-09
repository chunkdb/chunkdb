# Transactions

A transaction reads several chunks of one table as one consistent snapshot and writes them all together or not at all, also across a crash.

```text
BEGIN                                   -> +OK
GET BLOCK 10 4 FROM world               -> as of the snapshot
SET BLOCK 10 4 IN world gold = 90       -> _
SET BLOCK 300 7 IN world gold = 110     -> _
COMMIT                                  -> :<version>
```

## Statements

- Inside a transaction run `GET BLOCK`, `GET CHUNK`, `GET AREA`, `SET BLOCK`, `DELETE BLOCK`, `SET CHUNK`, `DESCRIBE` (clients need the columns to read values) and `PING`; any other statement gets `-ERR INVALID_ARGUMENT` and the transaction stays open.
- A transaction covers one table, the one its first statement names.
- Writes answer `_`: nobody else sees them before `COMMIT`, which answers the version every written chunk then has (`_` when nothing was written). Reads in the transaction see its own writes; `GET CHUNK` reports the chunk's version as of the snapshot.
- `IF VERSION` is refused inside a transaction: `COMMIT` checks every chunk the transaction read or wrote.
- `ROLLBACK` discards the transaction; without one it answers `+OK` too. `COMMIT` without a transaction gets `-ERR INVALID_ARGUMENT`. A closed connection rolls its transaction back.
- A failed statement inside a transaction changes nothing and leaves it open, except `CONFLICT`.
- After a `CONFLICT` from a statement, the transaction has ended and wrote nothing, but stays on the connection: every statement answers the same `CONFLICT` until `ROLLBACK` (or `COMMIT`) closes it, so statements sent after the conflict never run outside the transaction.
- Each statement needs its usual right ([USERS.md](USERS.md)); `COMMIT` checks `WRITE` again.

## Snapshot and conflicts

- The first statement takes the snapshot, as `REPEATABLE READ` does in PostgreSQL: every read sees the table as it was then.
- `COMMIT` answers `-ERR CONFLICT chunk_changed ...` when another write changed a chunk the transaction read or wrote after the snapshot. Nothing of the transaction is applied; run it again from `BEGIN`.
- Two transactions that each read what the other writes cannot both commit, so the result is the same as running them one after the other.
- Other `CONFLICT` reasons end the transaction the same way: `duration` (open longer than `--txn-max-duration-ms`), `history_limit` (the table kept too many earlier chunk states for open transactions) and `table_changed` (the table was altered or dropped).

```text
loop:
  BEGIN
  read, compute, write
  COMMIT        -> :<version> done; -ERR CONFLICT ... run the loop again
```

## Durability

`COMMIT` is durable when it answers, in every durability mode, and a crash leaves every written chunk with all of the transaction or none of it. It syncs each written chunk's WAL, so a commit costs more than a plain write in `relaxed` mode.

## Limits

- `--txn-max-duration-ms` (default 5000) from `BEGIN`.
- At most 64 written and 1024 read chunks per transaction.
- `--txn-max-bytes` (default 16 MiB): the written chunks of one transaction; `--txn-total-bytes` (default 256 MiB): those of all open transactions. A write past either gets `-ERR INVALID_ARGUMENT` and the transaction stays open.
- `--txn-history-bytes` (default 64 MiB per table): earlier chunk states kept while transactions are open; past it the oldest transactions end with `CONFLICT history_limit`.
- Tables opened read-only or shared by several server processes (`--allow-multi-process`) refuse transactions.

Plain statements outside transactions keep their cost: while no transaction is open, a write does nothing extra. A plain `GET AREA` reads chunk by chunk and may see a commit on some chunks only; read inside a transaction for one consistent view.
