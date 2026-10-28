# PING, FLUSH WAL, SHOW METRICS, BACKUP TO, RELOAD TLS

You check connectivity, make earlier writes durable, inspect metrics, copy the running server, or load a renewed TLS certificate.

## Examples

```cql docs-check session=main
PING
FLUSH WAL
```

```output
PONG
OK
```

To take a backup, configure `--backup-dir` and follow [Backup and restore](../BACKUP.md).
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
RELOAD TLS
```

## Arguments

- `destination`: an absent or empty relative directory under the configured backup directory.
- Use `/` separators; rooted paths, backslashes, `..` components, and symlinks below the resolved backup directory are refused.

## Returns

- PING: `PONG`.
- FLUSH WAL: `OK` after every write acknowledged before its arrival is durable.
- SHOW METRICS: Prometheus text, obtained through an authenticated connection.
- BACKUP: table count, inventoried files and bytes, and per-table epoch/revision cuts.
- RELOAD TLS: `OK` once connections that start from then on use the certificate and key read again from the `--tls-cert` and `--tls-key` files.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | FLUSH lacks WRITE on a table, metrics or RELOAD TLS lacks wildcard ADMIN, or backup lacks MANAGES USERS. | Request access. |
| BUSY | Another backup is running. | Wait for it to finish. |
| INVALID_ARGUMENT | Backup configuration or destination is invalid. | Follow [Backup and restore](../BACKUP.md). |
| INVALID_ARGUMENT | You run FLUSH WAL or BACKUP on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| INVALID_ARGUMENT | RELOAD TLS on a server without a `chunks://` listener, or the files hold no readable certificate and matching key. | Fix the files; the previous certificate stays in use. |
| INTERNAL | Required persistence or copying fails. | Check the error and [Durability](../DURABILITY_CONTRACT.md). |

## Notes

There is no native HTTP metrics endpoint.
A backup has one cut per table, rather than a shared cross-table snapshot.
It includes users and rights, so protect it as sensitive data.

When both revision bookkeeping records are missing, a writer refuses directories with chunk artifacts, a capped startup inventory, or an unavailable inventory.
Restore a consistent backup or reinitialize the whole store; a refused writer does not publish replacement revision records.
Read-only inspection remains available with an unknown completed revision; see [Durability](../DURABILITY_CONTRACT.md#failures).

RELOAD TLS replaces the certificate for new connections only; open connections keep their session.
Replace the certificate and key files first, then run it; SIGHUP does the same on Linux and macOS.

## Limits

Backup requires the single-process writer lock and a configured backup directory.
You need space for copied images and WAL prefixes; see [Backup and restore](../BACKUP.md).
