# BEGIN, COMMIT, ROLLBACK

You read one table consistently and commit changes to its chunks together.
Use the [CLI shell](../QUICK_START.md#2-connect-the-shell), because a transaction belongs to one connection.

## Examples

These examples create `transfer_demo`; use a server where that table does not exist.

```cql docs-check session=main
CREATE TABLE transfer_demo (gold u16) CHUNK 2 x 2
BEGIN
SET BLOCK 0 0 IN transfer_demo gold = 90
SET BLOCK 2 0 IN transfer_demo gold = 110
GET BLOCK 0 0 FROM transfer_demo
COMMIT
BEGIN
ROLLBACK
```

```output
OK
OK
(applies at COMMIT)
(applies at COMMIT)
gold = 90
3
OK
OK
```

## Syntax

```text
BEGIN
COMMIT
ROLLBACK
```

## Arguments

These statements take no arguments.
The first table statement after BEGIN chooses the table and snapshot.
A snapshot is the table's state at that moment.

## Returns

- BEGIN and ROLLBACK: `OK`.
- Writes before COMMIT: `(applies at COMMIT)` in the CLI shell.
- COMMIT with writes: one version shared by every changed chunk.
- COMMIT without writes: `(nothing written)` in the CLI shell.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| CONFLICT reason=chunk_changed | Another write changes a chunk you read or wrote. | ROLLBACK, then retry from BEGIN. |
| CONFLICT reason=table_changed | The table is altered or dropped. | ROLLBACK, refresh schema, and retry. |
| CONFLICT reason=duration | Duration exceeds the configured limit. | ROLLBACK and shorten the transaction. |
| CONFLICT reason=history_limit | Earlier chunk states exceed the history budget. | ROLLBACK and retry promptly. |
| INVALID_ARGUMENT | You use a prohibited statement, IF VERSION, another table, or exceed write/read limits. | Correct the statement or reduce the transaction. |
| INVALID_ARGUMENT | You run BEGIN on a server started with `--read-only`. | Run the transaction on a server started without `--read-only`. |
| PERMISSION_DENIED | You lose a required right; COMMIT checks WRITE again. | Request access or ROLLBACK. |

## Notes

Allowed statements are GET BLOCK, GET CHUNK, GET AREA, SET BLOCK, DELETE BLOCK, SET CHUNK, DESCRIBE, and PING.
Ordinary statement errors leave the transaction open without applying the failed statement.
A conflict ends its work and writes nothing.
The connection keeps returning that conflict until ROLLBACK or COMMIT closes it.

The CLI automatically rolls back a statement conflict; direct clients must clear the conflict themselves.
ROLLBACK without an open transaction succeeds; COMMIT without one fails.
Closing the connection rolls back its transaction.
COMMIT is durable in every [Durability](../DURABILITY_CONTRACT.md) mode.

## Limits

See [Transactions](../TRANSACTIONS.md#limits) for chunk, duration, memory, and history bounds.
Use [Retry a transaction](../TRANSACTION_RETRY.md) for a two-session conflict example.
