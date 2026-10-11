# PING, FLUSH WAL, SHOW METRICS, BACKUP TO

You check connectivity, make earlier writes durable, inspect metrics, or copy the running server.

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
```

## Arguments

- `destination`: an absent or empty relative directory under the configured backup directory.
- Use `/` separators; rooted paths, backslashes, `..` components, and symlinks below the resolved backup directory are refused.

## Returns

- PING: `PONG`.
- FLUSH WAL: `OK` after every write acknowledged before its arrival is durable.
- SHOW METRICS: Prometheus text, obtained through an authenticated connection.
- BACKUP: table count, inventoried files and bytes, and per-table epoch/revision cuts.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | FLUSH lacks WRITE on a table, metrics lacks wildcard ADMIN, or backup lacks MANAGES USERS. | Request access. |
| BUSY | Another backup is running. | Wait for it to finish. |
| INVALID_ARGUMENT | Backup configuration or destination is invalid. | Follow [Backup and restore](../BACKUP.md). |
| INTERNAL | Required persistence or copying fails. | Check the error and [Durability](../DURABILITY_CONTRACT.md). |

## Notes

There is no native HTTP metrics endpoint.
A backup has one cut per table, rather than a shared cross-table snapshot.
It includes users and rights, so protect it as sensitive data.

## Limits

Backup requires the single-process writer lock and a configured backup directory.
You need space for copied images and WAL prefixes; see [Backup and restore](../BACKUP.md).
