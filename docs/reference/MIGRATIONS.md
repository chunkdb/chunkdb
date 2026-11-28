# MIGRATE, SHOW MIGRATIONS

You record a named schema or rights change and safely retry it at application startup.
A migration is a statement, or several steps on one table or grant target, recorded under one permanent name.

## Examples

These examples create `migration_demo`; use a server where that table does not exist.

```cql docs-check session=main
MIGRATE 'migration_demo_table' CREATE TABLE migration_demo (kind u8) CHUNK 2 x 2
MIGRATE 'migration_demo_table' CREATE TABLE migration_demo (kind u8) CHUNK 2 x 2
MIGRATE 'migration_demo_label' ALTER TABLE migration_demo ADD COLUMN label text(16) NULL
MIGRATE 'migration_demo_levels' ALTER TABLE migration_demo ADD COLUMN level u8 NULL; ALTER TABLE migration_demo ADD COLUMN rank u8 NULL
SHOW MIGRATIONS
```

```output
applied
skipped
applied
applied
1) name = migration_demo_table, applied_ms = <time-ms>, user = admin, statement = CREATE TABLE migration_demo (kind u8) CHUNK 2 x 2
2) name = migration_demo_label, applied_ms = <time-ms>, user = admin, statement = ALTER TABLE migration_demo ADD COLUMN label text(16) NULL
3) name = migration_demo_levels, applied_ms = <time-ms>, user = admin, statement = ALTER TABLE migration_demo ADD COLUMN level u8 NULL; ALTER TABLE migration_demo ADD COLUMN rank u8 NULL
```

SHOW MIGRATIONS also lists the other recorded migrations within your rights.

## Syntax

```text
MIGRATE 'name' statement [; statement ...]
SHOW MIGRATIONS
```

## Arguments

- `name`: quoted `[a-z_][a-z0-9_]*`, from 1 through 63 bytes.
- `statement`: one CREATE TABLE, ALTER TABLE, DROP TABLE, GRANT, REVOKE, CREATE SLOT, or DROP SLOT statement; ALTER TABLE ... RENAME TO is refused.
- Several statements separated by `;`, at most 64, are the steps of one migration on one table or grant target; see [Steps](#steps).
- Conditional IF NOT EXISTS and IF EXISTS forms are supported inside a migration.

## Returns

- New name: `applied`, including when the inner statement does nothing.
- Same name and statement: `skipped`.
- SHOW MIGRATIONS: name, application timestamp, applying user, and statement, in applied order, for records within your rights; the applying user is null without MANAGES USERS.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| MIGRATION_MISMATCH | A recorded name has different statement text. | Use a new name for the new change. |
| PERMISSION_DENIED | You lack the inner statement's rights, or the name is recorded for a change outside your rights. | Request access, or use another name. |
| INVALID_ARGUMENT | You run MIGRATE inside a transaction or submit an unsupported statement. | Run the allowed statement outside a transaction. |
| INVALID_ARGUMENT | You run MIGRATE on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| INVALID_ARGUMENT | The steps name different tables or grant targets, include DROP TABLE, put CREATE TABLE, ADD CHECK, DROP NULL or a narrowing ALTER COLUMN ... TYPE without USING after the first step, or change slots of a table the same migration creates. | Give those steps migrations of their own. |
| OUT_OF_RANGE | The ledger exceeds 16384 records or 16777216 bytes. | Stop adding new migration names. |
| OUTCOME_UNKNOWN | Publishing or completing the migration decision failed, so the outcome is unknown. | Restart the writer to recover, then retry the same name and text. |
| INTERNAL | The inner statement names a fenced table. | Run [RECOVER TABLE](TABLES.md#notes), then retry the same name and text. |

## Notes

Migration names are shared by all tables and users; give each application's names a distinct prefix.
A record is within your rights when you have ADMIN on the table its statement names, or MANAGES USERS for GRANT and REVOKE.
A record whose statement this server cannot parse is within your rights only with ADMIN on `*`.
Records name tables by name: after a table is dropped and created again, its earlier records belong to whoever administers the new table.
Rights are checked before an existing name returns skipped or conflict.
A name recorded outside your rights returns PERMISSION_DENIED whatever statement you submit, without its statement.
With `--auth none`, every record is within your rights.
Keyword case and internal whitespace remain part of the statement identity.
Separating and trailing spaces or tabs are removed.

Concurrent requests for the same name wait, then compare text.
A narrowing ALTER COLUMN ... TYPE, ALTER COLUMN ... DROP NULL and ADD CHECK check stored values while statements on other tables keep running; BACKUP, ENABLE FEATURE and SHOW STATUS started during the check wait for it.
During the check, writes to that column must fit the new type, for DROP NULL must not leave it NULL, and for ADD CHECK must pass the new check.
A refused narrowing records nothing in the ledger and removes its narrowing again; if that removal fails, the table is fenced until RECOVER TABLE or a restart.
Completed records survive restart, backup, and restore together with their changes.
A migration with an unknown outcome fences every table, because its decision can include the ledger, users and the root manifest.
A table out of service after an ordinary failure fences only that table; a new migration naming it is refused until RECOVER TABLE recovers it, while an applied name still answers skipped.
Replay lists that drop tables with ADMIN on all tables; table-specific grants disappear when those tables are dropped.
See [Users and rights](../USERS.md#rights) and [Backup and restore](../BACKUP.md).

## Steps

The steps of a migration apply in order as one unit, with one record for the whole migration.
A failure before the decision is published leaves every step unapplied and records nothing.
If a published decision survives a failure or crash, restarting the writer finishes the unit and records it.
Each step sees the table as the steps before it left it, so a later step can name a column an earlier step added, and IF NOT EXISTS and IF EXISTS test that state.
SHOW MIGRATIONS lists the migration once, with every step in its statement; each step needs its own rights, and a record is within your rights only when every step is.
When a migration includes a table or slot change, every step names the same table; DROP TABLE runs as a migration of its own.
A migration of only GRANT and REVOKE steps uses the same grant target in every step, either a table name, a prefix followed by `*`, or `*`.
CREATE TABLE can be only the first step; with IF NOT EXISTS and an existing table, the other steps change that table.
ADD CHECK, DROP NULL and a narrowing ALTER COLUMN ... TYPE without USING check stored values, so they can be only the first step; CREATE SLOT and DROP SLOT on a table the same migration creates go in a later migration.
A migration with table or slot steps waits for running statements on its table, as ALTER TABLE does, also when every step's condition already holds.

## Limits

The migration ledger holds at most 16384 records and 16777216 bytes.
Existing names remain retryable when the ledger is full.
Migrations require the server's data-directory writer lock.

MIGRATE requires the recorded `migrations` feature, which every directory a 2.0 build creates has; see [Feature activation](TABLES.md#feature-activation).
