# Server flags

`chunkdb_server --help` (or `-h`) prints the accepted options.
Flags take the following argument as their value, except `--background-maintenance`.
Numeric values are decimal integers, with the bounds shown here.

## Connection and authentication

| Flag | Default | Meaning and bounds |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address. |
| `--port` | `4242` | TCP port, 1–65535. |
| `--listen-uri` | unset | `chunk://host:port/` or `chunks://host:port/`; sets address, port and TLS at this point in argument order; a username is refused. |
| `--workers` | CPU thread count, or 4 | Connections served at once; a connection holds a worker until it closes or reaches the idle timeout, and a WATCH releases it. |
| `--client-io-timeout-ms` | `5000` | 1–86400000 ms for handshake, HELLO, an active request or a complete reply write. |
| `--idle-connection-timeout-ms` | `60000` | 1–86400000 ms between requests; before HELLO the smaller I/O/idle timeout applies. |
| `--max-pending-clients` | `1024` | Accepted clients waiting for a worker; overflow is refused. |
| `--max-handshakes-per-ip` | `64` | Connections from an IPv4 address or IPv6 /64 occupying workers before HELLO; exceeding the limit is refused. `0` disables the limit. |
| `--max-line-bytes` | `65536` | Request line limit including terminator; parameter frames are bounded separately by column/chunk size. |
| `--log-level` | `info` | `info`, `warn`, `error`. |
| `--auth` | `scram` | `scram` or `none`; `none` grants all rights and is intended for local development. |
| `--admin-user` | unset | First administrator's name, used only when no users exist. |
| `--admin-password-file` | unset | First administrator's password from the first line of this file. |
| `--tls-cert` | unset | PEM certificate required by a `chunks://` listener. |
| `--tls-key` | unset | PEM private key required by a `chunks://` listener. |
| `--tls-min-version` | `1.2` | `1.2` or `1.3`: the lowest TLS version a `chunks://` listener accepts. |

The handshake limit bounds connections from one source before authentication finishes.
Raise --max-handshakes-per-ip when many clients share a network address and connect together.

CHUNKDB_ADMIN_USER and CHUNKDB_ADMIN_PASSWORD supply the first administrator credentials.
Explicit corresponding flags take precedence.
A directory with no users needs both credentials unless `--auth none` is selected.
Existing users are loaded on later starts; bootstrap settings do not change their passwords.
Use [Users and rights](USERS.md) for password changes and offline recovery.

## TLS

A `chunks://` listener accepts TLS from `--tls-min-version` up and refuses renegotiation.
The `--tls-cert` file holds the server certificate followed by any intermediate certificates, which are sent to clients; the `--tls-key` file holds an unencrypted private key.
To renew the certificate, replace both files, then run [RELOAD TLS](reference/SERVER.md) or, on Linux and macOS, send the server SIGHUP.
The server reads both files again and checks that the key matches the certificate.
Connections that start afterwards use the new certificate; open connections keep their session.
If the files are unreadable or do not match, the previous certificate stays in use, RELOAD TLS returns the error, and SIGHUP logs it.
A server without a `chunks://` listener keeps the default SIGHUP action and stops.

## Directory and shared resources

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir` | `data` | Directory containing manifests, users and `tables/`; a fresh directory starts with no tables. |
| `--backup-dir` | unset | Enables BACKUP; nonempty directory path, with safe relative destinations below its resolved root. |
| `--max-loaded-chunks` | `65536` | Shared cache limit counted in chunks across all tables, rather than bytes. |
| `--max-open-wal-streams` | `1024` | Shared append-stream limit; POSIX descriptor reserves may clamp it. |
| `--background-maintenance` | off | Boolean switch running checkpoint/eviction maintenance per table; acknowledgement durability is unchanged. |
| `--background-checkpoint-queue-limit` | `4096` | Per-table queue; overflow and excessive WAL growth force inline checkpointing. |

## Transactions and change feed

| Flag | Default | Meaning |
|---|---|---|
| `--txn-max-duration-ms` | `5000` | Transaction lifetime from BEGIN; expiry ends it with CONFLICT. |
| `--txn-max-bytes` | `16777216` | Private written-chunk bytes for one transaction. |
| `--txn-total-bytes` | `268435456` | Private written-chunk bytes for all open transactions. |
| `--txn-history-bytes` | `67108864` | Earlier chunk states retained per table; overflow cancels oldest transactions. |
| `--feed-buffer-bytes` | `67108864` | Default live feed budget for tables without an explicit `feed_buffer_bytes` option, shared among watches. |
| `--feed-linger-ms` | `30000` | Keep live history after the last watch closes, 0–2147483647 ms; 0 releases it immediately. Writes keep copying into the feed during this interval; slots retain history independently. |
| `--max-watches` | `64` | Concurrent server watches; excess receives BUSY. |
| `--slot-max-bytes` | `1073741824` | Default retained history per slot for tables without an explicit `slot_max_bytes` option; exceeding it marks that slot lost. |
| `--slot-sync-ms` | `100` | Durable frontier pass interval in ms, 1–2147483647; see [Feed statements](reference/FEED.md#notes). |

Watches release workers and have no idle timeout.
See [Transactions](TRANSACTIONS.md) and [Change feed](CHANGE_FEED.md) for outcomes at limits.
A table without an explicit feed/slot limit uses the server defaults each time it opens.
An explicit table limit persists; change it with ALTER TABLE SET.
`DESCRIBE` returns the effective limits.

## Persisted table options

| Flag | Default | CQL option / values |
|---|---|---|
| `--durability` | `relaxed` | `durability_mode`: relaxed, fsync-wal, fsync-checkpoint. |
| `--checkpoint-updates` | `256` | `checkpoint_updates`: update-count checkpoint threshold. |
| `--checkpoint-wal-bytes` | `1048576` | `checkpoint_wal_bytes`: WAL-byte checkpoint threshold. |
| `--wal-group-commit-updates` | `8` | `wal_group_commit_updates`: relaxed-mode batch flush threshold. |
| `--checkpoint-compression` | `none` | `checkpoint_compression`: none or zrle for new images. |

These defaults apply to newly created tables.
An explicitly supplied option flag must also match every existing table's stored option; a mismatch refuses startup without changing data.
Omit flags to use different stored options, or change a table with `ALTER TABLE ... SET`.
The var_max_chunk_bytes option has no flag; its default is 1048576 bytes.
Set it through CQL, from 13 through 67108864 bytes.
See the [Durability](DURABILITY_CONTRACT.md).

## Table geometry

Create tables with `CREATE TABLE`; see [Tables](reference/TABLES.md).
The default chunk is 16 × 16 blocks and the default large chunk is 8 × 8 chunks.
Geometry belongs to the table definition and remains fixed for the table's lifetime.
Column types are defined with `CREATE TABLE` and can be changed with `ALTER TABLE`.

## File access

Server-created data and backup directories permit owner-only access, mode 0700 on Linux/macOS.
New files use mode 0600 on those platforms; Windows uses protected owner-only access control lists.
Existing users-file permissions are tightened at startup.
Bind mounts must grant access to the process owner; see [Docker](DOCKER.md).
