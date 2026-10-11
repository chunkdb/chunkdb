# MIGRATE, SHOW MIGRATIONS

You record a named schema or rights change and safely retry it at application startup.
A migration is a statement recorded under one permanent name.

## Examples

These examples create `migration_demo`; use a server where that table does not exist.

```cql docs-check session=main
MIGRATE 'migration_demo_table' CREATE TABLE migration_demo (kind u8) CHUNK 2 x 2
MIGRATE 'migration_demo_table' CREATE TABLE migration_demo (kind u8) CHUNK 2 x 2
MIGRATE 'migration_demo_label' ALTER TABLE migration_demo ADD COLUMN label text(16) NULL
SHOW MIGRATIONS
```

```output
applied
skipped
applied
1) name = migration_demo_table, applied_ms = <time-ms>, user = admin, statement = CREATE TABLE migration_demo (kind u8) CHUNK 2 x 2
2) name = migration_demo_label, applied_ms = <time-ms>, user = admin, statement = ALTER TABLE migration_demo ADD COLUMN label text(16) NULL
```

SHOW MIGRATIONS also lists any other migrations you have applied.

## Syntax

```text
MIGRATE 'name' statement
SHOW MIGRATIONS
```

## Arguments

- `name`: quoted `[a-z_][a-z0-9_]*`, from 1 through 63 bytes.
- `statement`: one CREATE TABLE, ALTER TABLE, DROP TABLE, GRANT, REVOKE, CREATE SLOT, or DROP SLOT statement.
- Conditional IF NOT EXISTS and IF EXISTS forms are supported inside a migration.

## Returns

- New name: `applied`, including when the inner statement does nothing.
- Same name and statement: `skipped`.
- SHOW MIGRATIONS: name, application timestamp, applying user, and statement, in applied order.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| MIGRATION_MISMATCH | A recorded name has different statement text. | Use a new name for the new change. |
| PERMISSION_DENIED | You lack the inner statement's rights, or MANAGES USERS for SHOW. | Request access. |
| INVALID_ARGUMENT | You run MIGRATE inside a transaction or submit an unsupported statement. | Run the allowed statement outside a transaction. |
| OUT_OF_RANGE | The ledger exceeds 16384 records or 16777216 bytes. | Stop adding new migration names. |
| INTERNAL | A durable decision has an unknown outcome. | Restart the writer, then retry the same name and text. |

## Notes

Rights are checked before an existing name returns skipped or conflict.
A conflict can reveal an existing name to a user allowed to run the submitted statement.
Keyword case and internal whitespace remain part of the statement identity.
Separating and trailing spaces or tabs are removed.

Concurrent requests for the same name wait, then compare text.
Completed records survive restart, backup, and restore together with their changes.
Replay lists that drop tables with ADMIN on all tables; table-specific grants disappear when those tables are dropped.
See [Users and rights](../USERS.md#rights) and [Backup and restore](../BACKUP.md).

## Limits

The migration ledger holds at most 16384 records and 16777216 bytes.
Existing names remain retryable when the ledger is full.
Migrations require the server's data-directory writer lock.

MIGRATE requires the recorded `migrations` feature; [enable missing features](TABLES.md#feature-activation) before submitting a named step.
