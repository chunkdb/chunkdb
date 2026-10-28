# Retry a transaction

You rerun the complete operation when another writer invalidates your snapshot.
Open two [CLI shells](QUICK_START.md#2-connect-the-shell) using the same URI and password.
See [Transactions](TRANSACTIONS.md) for the guarantees.

## 1. Read in the first shell

```cql docs-check session=first
CREATE TABLE world (gold u16) CHUNK 2 x 2
SET BLOCK 0 0 IN world gold = 100
BEGIN
GET BLOCK 0 0 FROM world
```

```output
OK
2
OK
gold = 100
```

## 2. Write in the second shell

```cql docs-check session=second
SET BLOCK 0 0 IN world gold = 105
```

```output
3
```

## 3. Commit in the first shell

```cql docs-check session=first
SET BLOCK 0 0 IN world gold = 90
COMMIT
```

```output
(applies at COMMIT)
error: CONFLICT reason=chunk_changed a chunk the transaction read or wrote changed after its snapshot (the transaction ended and wrote nothing; run it again from BEGIN)
```

The conflicting commit writes nothing.
The CLI closes this transaction automatically.
With a direct client, send ROLLBACK after a statement conflict before retrying.

## 4. Reread and recompute

```cql docs-check session=first
BEGIN
GET BLOCK 0 0 FROM world
SET BLOCK 0 0 IN world gold = 95
COMMIT
```

```output
OK
gold = 105
(applies at COMMIT)
4
```

## If it fails

- `chunk_changed`: reread every value and recompute; do not retry only COMMIT.
- `duration`: perform less work between BEGIN and COMMIT.
- `history_limit`: close transactions promptly or increase the history budget.
- `table_changed`: refresh the schema before retrying.

Bound retries in your application and report persistent conflicts to its caller.
See [Transaction statements](reference/TRANSACTION_STATEMENTS.md#errors) for the complete error behavior.
