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
- COMMIT checks every chunk you read or wrote for intervening changes.
- A successful COMMIT survives a crash in every durability mode.
- A conflict applies none of your transaction's writes; retry the complete operation.

Plain GET AREA reads chunks separately and can see a partially observed multi-chunk commit.
Use a transaction when those chunks must share one consistent view.
A read-only transaction can commit while its snapshot remains valid.

## Limits

| Limit | Default or maximum |
| --- | --- |
| Duration from BEGIN | 5000 ms, `--txn-max-duration-ms` |
| Written chunks | 1024, `--txn-max-written-chunks` |
| Read chunks | 8192, `--txn-max-read-chunks` |
| Charged transaction storage per transaction | 16777216 bytes, `--txn-max-bytes` |
| Charged transaction storage across transactions | 268435456 bytes, `--txn-total-bytes` |
| Earlier chunk states per table | 67108864 bytes, `--txn-history-bytes` |

Exceeding a chunk-count, area-response or write-memory limit rejects the statement and leaves the transaction open without adding its reads or writes.
The byte limits include charged bookkeeping, owned transaction storage and the logical size of private written chunks.
BEGIN is refused without opening a transaction when its bookkeeping cannot fit.
Transient copies in the bulk-write path are still created before their logical size is checked, so these limits do not bound that path's peak allocation.
History pressure ends the oldest transactions with CONFLICT.
A table change or expired duration also ends the transaction's work.

See [Transaction statements](reference/TRANSACTION_STATEMENTS.md), [Retry a transaction](TRANSACTION_RETRY.md), and [Durability](DURABILITY_CONTRACT.md).
