# Transactions

A transaction reads one consistent table snapshot and commits its changes together, or applies none of them.

Suppose two players exchange gold stored in different chunks.
You need both balances to change together, even after a crash.
In the [CLI shell](QUICK_START.md#2-connect-the-shell), write both before committing:

```cql docs-check session=main
CREATE TABLE world (gold u16) CHUNK 2 x 2
BEGIN
SET BLOCK 0 0 IN world gold = 90
SET BLOCK 2 0 IN world gold = 110
COMMIT
```

```output
OK
OK
(applies at COMMIT)
(applies at COMMIT)
1
```

## Guarantees

- Your first table statement chooses the snapshot and the transaction's single table.
- Reads see that snapshot and your own pending writes.
- Other connections see none of your changes before COMMIT.
- COMMIT checks every chunk you read or wrote for intervening changes, except in a read-only transaction.
- A successful COMMIT survives a crash in every durability mode.
- A conflict applies none of your transaction's writes; retry the complete operation.

Plain GET AREA reads chunks separately and can see a partially observed multi-chunk commit.
Use a transaction when those chunks must share one consistent view.
A read-only transaction can commit while its snapshot remains valid.

## Read-only transactions

`BEGIN READ ONLY` reads one snapshot of a table with GET, AGGREGATE and SCAN CHUNKS, and refuses writes.
It keeps no read set, so `--txn-max-read-chunks` does not bound it and COMMIT, like ROLLBACK, only ends it.
`--txn-max-duration-ms` and `--txn-history-bytes` bound it as any transaction.

While a snapshot is open, a write copies the state of the chunk it changes and keeps the copy while an older open snapshot needs it.
Without an open snapshot writes copy nothing.
`chunkdb_txn_history_copies_total` and `chunkdb_txn_history_copied_bytes_total` in SHOW METRICS count these copies and their bytes; `chunkdb_txn_history_bytes` is what is kept now.
A long scan beside heavy writes therefore costs memory up to `--txn-history-bytes` per table, after which the oldest transaction ends with CONFLICT reason=history_limit.

## Limits

| Limit | Default or maximum |
| --- | --- |
| Duration from BEGIN | 5000 ms, `--txn-max-duration-ms` |
| Written chunks | 1024, `--txn-max-written-chunks` |
| Read chunks, except READ ONLY | 8192, `--txn-max-read-chunks` |
| Private written chunks per transaction | 16777216 bytes, `--txn-max-bytes` |
| Private written chunks across transactions | 268435456 bytes, `--txn-total-bytes` |
| Earlier chunk states per table | 67108864 bytes, `--txn-history-bytes` |

Exceeding a chunk-count, area-response or write-memory limit rejects the statement and leaves the transaction open without adding its reads or writes.
SET CHUNK is checked against the written-chunk and write-memory limits before the form is decoded, by the smallest size the chunk state it writes can take.
A form over a limit receives INVALID_ARGUMENT for that limit, even when it is also malformed.
History pressure ends the oldest transactions with CONFLICT.
A table change or expired duration also ends the transaction's work.
Past its duration, a transaction that has run a table statement ends with the next statement the connection runs, PING and DESCRIBE included, or earlier when another transaction needs its share of `--txn-total-bytes`.
An idle connection closes after `--idle-connection-timeout-ms` even with a transaction open, which rolls it back.

See [Transaction statements](reference/TRANSACTION_STATEMENTS.md), [Retry a transaction](TRANSACTION_RETRY.md), and [Durability](DURABILITY_CONTRACT.md).
