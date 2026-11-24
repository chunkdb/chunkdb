# PING, FLUSH WAL, SHOW METRICS, BACKUP TO, RESTORE TABLE, RELOAD TLS

You check connectivity, make earlier writes durable, inspect metrics, copy the running server, restore one table from a backup, or load a renewed TLS certificate.

## Examples

```cql docs-check session=main
PING
FLUSH WAL
```

```output
PONG
OK
```

To take a backup or restore a table, configure `--backup-dir` and follow [Backup and restore](../BACKUP.md).
To inspect metrics:

```sh docs-check-shell
chunk-cli --uri "$CHUNKDB_URI" "SHOW METRICS" | sed -n '1,2p'
```

```output
# HELP chunkdb_commands_total Commands processed, by command class and outcome.
# TYPE chunkdb_commands_total counter
```

## Syntax

```text
PING
FLUSH WAL
SHOW METRICS
BACKUP TO 'destination'
RESTORE TABLE table FROM 'backup' [AS name]
RELOAD TLS
SHOW CONNECTIONS
CANCEL id
KILL CONNECTION id
```

## Arguments

- `destination`: an absent or empty relative directory under the configured backup directory.
- Use `/` separators; rooted paths, backslashes, `..` components, and symlinks below the resolved backup directory are refused.
- `backup`: a completed backup under the configured backup directory, with the same path rules.
- `table`: a table the backup holds; `name`: the new table, `table` when AS is omitted.

## Returns

- PING: `PONG`.
- FLUSH WAL: `OK` after every write acknowledged before its arrival is durable.
- SHOW METRICS: Prometheus text, obtained through an authenticated connection.
- BACKUP: table count, inventoried files and bytes, and per-table epoch/revision cuts.
- RESTORE TABLE: the new `table`, its `epoch`, its `parent_epoch` and `fork_revision` (the backed-up epoch and cut), and the `files` and `bytes` copied.
- RELOAD TLS: `OK` once connections that start from then on use the certificate and key read again from the `--tls-cert` and `--tls-key` files.
- SHOW CONNECTIONS: an array of maps with `id`, `user`, `peer`, `state`, `statement`, and `progress`.
- CANCEL: `{cancelled: bool}`, indicating whether cancellation was accepted before the target statement's decision boundary.
- KILL CONNECTION: `{killed: bool}`, indicating whether closure was requested for the target connection.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | FLUSH lacks WRITE on a table, metrics lack MONITOR and wildcard ADMIN, RELOAD TLS lacks wildcard ADMIN, backup lacks BACKUP and MANAGES USERS, or RESTORE TABLE lacks MANAGES USERS and wildcard ADMIN, and BACKUP or ADMIN on the new name. | Request access. |
| TABLE_EXISTS | RESTORE TABLE names a table that exists. | Restore under another name with AS. |
| BUSY | Another backup is running. | Wait for it to finish. |
| INVALID_ARGUMENT | Backup configuration, destination or source is invalid, the backup has no such table, or its features or stored options differ from what this server serves. | Follow [Backup and restore](../BACKUP.md). |
| INVALID_ARGUMENT | You run FLUSH WAL, BACKUP or RESTORE TABLE on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| INVALID_ARGUMENT | RELOAD TLS on a server without a `chunks://` listener, or the files hold no readable certificate and matching key. | Fix the files; the previous certificate stays in use. |
| INTERNAL | Required persistence or copying fails, a restored backup is incomplete, damaged or changed, or the server stops during a restore. | Check the error and [Durability](../DURABILITY_CONTRACT.md). |

## Notes

There is no native HTTP metrics endpoint.
Metrics sum all tables, except the expiry metrics, which have a `table` label and appear for each table with an `expire_column` ([Expiry](TABLES.md#expiry)).
`chunkdb_expiry_lag_seconds` is the time since the start of the table's last completed expiry sweep: every block whose time was at or before that start has been deleted, unless a write set that time after the sweep passed its chunk.
Until a sweep completes, it counts from when expiry started on the table, at the server start or the `expire_column` change.
`chunkdb_expired_blocks_total` counts the blocks expiry deleted and `chunkdb_expiry_failed_passes_total` its passes that failed, which the log names; both start at zero when the server starts.
A backup has one cut per table, rather than a shared cross-table snapshot.
It includes users and rights, so protect it as sensitive data.
RESTORE TABLE never replaces a table; use SWAP TABLES from [Tables](TABLES.md) to put the restored table under the live name.
CANCEL, KILL CONNECTION or a statement timeout stops RESTORE TABLE only before it publishes the table, and nothing is restored.
While RESTORE TABLE copies the backup, SHOW CONNECTIONS shows its `progress`: `files_done` of `files_total` and `bytes_done` of `bytes_total` copied; after the copy it stays at the totals while the table is checked and published.
`progress` is null while the restore checks the backup before the copy and once the statement completes, also after a failure.

When both revision bookkeeping records are missing, a writer refuses directories with chunk artifacts, a capped startup inventory, or an unavailable inventory.
Restore a consistent backup or reinitialize the whole store; a refused writer does not publish replacement revision records.
Read-only inspection remains available with an unknown completed revision; see [Durability](../DURABILITY_CONTRACT.md#failures).

RELOAD TLS replaces the certificate for new connections only; open connections keep their session.
Replace the certificate and key files first, then run it; SIGHUP does the same on Linux and macOS.

Connection identifiers are exact unsigned integers scoped to the server process.
A user may list and control connections of the same live user incarnation; ADMIN on `*` permits access to all connections, and MONITOR lists all connections without controlling them.
Dropping and recreating a user does not grant the replacement user access to the previous user's connections.
An empty user or statement is null in SHOW CONNECTIONS, and `statement` contains only the statement verb.
`progress` is null unless the running statement reports progress; only RESTORE TABLE does.
A listing that exceeds the response limit returns OUT_OF_RANGE without a partial listing.
Missing, idle, self-cancelled, and decision-entered statements return `cancelled: false`.
Killing a connection preserves the real outcome of a statement that already entered its decision boundary.
Self-KILL returns its reply before closing the connection.
These statements also run inside an open or aborted transaction and on a read-only server.

## Limits

Backup requires the single-process writer lock and a configured backup directory.
You need space for copied images and WAL prefixes; see [Backup and restore](../BACKUP.md).
