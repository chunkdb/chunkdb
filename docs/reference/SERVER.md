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

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | FLUSH lacks WRITE on a table, metrics or RELOAD TLS lacks wildcard ADMIN, backup lacks MANAGES USERS, or RESTORE TABLE lacks both wildcard ADMIN and MANAGES USERS. | Request access. |
| TABLE_EXISTS | RESTORE TABLE names a table that exists. | Restore under another name with AS. |
| BUSY | Another backup is running. | Wait for it to finish. |
| INVALID_ARGUMENT | Backup configuration, destination or source is invalid, the backup has no such table, or its features or stored options differ from what this server serves. | Follow [Backup and restore](../BACKUP.md). |
| INVALID_ARGUMENT | You run FLUSH WAL, BACKUP or RESTORE TABLE on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| INVALID_ARGUMENT | RELOAD TLS on a server without a `chunks://` listener, or the files hold no readable certificate and matching key. | Fix the files; the previous certificate stays in use. |
| INTERNAL | Required persistence or copying fails, a restored backup is incomplete, damaged or changed, or the server stops during a restore. | Check the error and [Durability](../DURABILITY_CONTRACT.md). |

## Notes

There is no native HTTP metrics endpoint.
A backup has one cut per table, rather than a shared cross-table snapshot.
It includes users and rights, so protect it as sensitive data.
RESTORE TABLE never replaces a table; use SWAP TABLES from [Tables](TABLES.md) to put the restored table under the live name.

When both revision bookkeeping records are missing, a writer refuses directories with chunk artifacts, a capped startup inventory, or an unavailable inventory.
Restore a consistent backup or reinitialize the whole store; a refused writer does not publish replacement revision records.
Read-only inspection remains available with an unknown completed revision; see [Durability](../DURABILITY_CONTRACT.md#failures).

RELOAD TLS replaces the certificate for new connections only; open connections keep their session.
Replace the certificate and key files first, then run it; SIGHUP does the same on Linux and macOS.

## Limits

Backup requires the single-process writer lock and a configured backup directory.
You need space for copied images and WAL prefixes; see [Backup and restore](../BACKUP.md).
