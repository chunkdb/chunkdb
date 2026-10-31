# Server flags

`chunkdb_server --help` (or `-h`) prints the accepted options.
Flags take the following argument as their value, except `--background-maintenance` and `--read-only`.
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
| `--max-unparsed-parameter-bytes` | `1048576` | Payload bytes drained after a statement cannot parse or its parameter types cannot resolve; excess closes with PROTOCOL. |
| `--log-level` | `info` | `info`, `warn`, `error`. |
| `--auth` | `scram` | `scram` or `none`; `none` grants all rights and is intended for local development. |
| `--admin-user` | unset | First administrator's name, used only when no users exist. |
| `--admin-password-file` | unset | First administrator's password from the first line of this file. |
| `--auth-max-failures-per-connection` | `5` | 1–1000 failed HELLO or AUTH attempts that close a connection. |
| `--auth-ban-failures` | `5` | 0–1000000 failed logins from one source address that ban it; `0` disables bans. |
| `--auth-ban-ms` | `1000` | 1–86400000 ms a source address stays banned; each later failure from it bans it again. |
| `--proxy-protocol` | `off` | `off` or `v2`; with `v2` every connection starts with a PROXY protocol v2 header. |
| `--audit-log` | unset | File receiving the audit log; see [Logs](#logs). |
| `--slow-statement-ms` | `0` | 0–86400000 ms; statements taking at least this long are logged at WARN; `0` logs none. |
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

A source address is an IPv4 address or an IPv6 /64.
A successful login from a source address clears its failures.

## TLS

A `chunks://` listener accepts TLS from `--tls-min-version` up and refuses renegotiation.
The `--tls-cert` file holds the server certificate followed by any intermediate certificates, which are sent to clients; the `--tls-key` file holds an unencrypted private key.
To renew the certificate, replace both files, then run [RELOAD TLS](reference/SERVER.md) or, on Linux and macOS, send the server SIGHUP.
The server reads both files again and checks that the key matches the certificate.
Connections that start afterwards use the new certificate; open connections keep their session.
If the files are unreadable or do not match, the previous certificate stays in use, RELOAD TLS returns the error, and SIGHUP logs it.
A server with neither a `chunks://` listener nor `--audit-log` keeps the default SIGHUP action and stops.

## PROXY protocol

With `--proxy-protocol v2`, every connection must start with a PROXY protocol version 2 header, as a load balancer such as HAProxy sends it.
The header comes before the TLS handshake on a `chunks://` listener.
Its source address replaces the socket's peer for login bans, the handshake limit, the audit log and connection logs.
A LOCAL header, or one with an unspecified address family, keeps the socket's peer.
A connection without a valid header within `--client-io-timeout-ms` is closed without a reply; headers longer than 1040 bytes are refused.
While the header is read, the connection counts toward `--max-handshakes-per-ip` for the socket's peer, so a host that reaches the port directly holds at most that many workers this way; the proxy sends its header at once, so its connections hold that slot only briefly.
The first refused header is logged; later ones are counted as rejected connections.
Only TCP over IPv4 and IPv6 is accepted.
With `off`, the server never reads a header, so a client cannot claim another address; listen only where the proxy can reach the server when you use `v2`.

## Logs

The server log goes to standard output at `--log-level`.

`--slow-statement-ms` logs a WARN line for each statement that took at least that long.
The line names the statement verb, table, user, peer address, duration in milliseconds and, where the server knows it, the number of chunks read or written; it never contains values.
Slow-statement lines are WARN lines, so `--log-level error` leaves them out.

`--audit-log` appends one JSON object per line to the file, creating it readable and writable by its owner only.
Each line is written and flushed when its event ends; it is not synced to disk.
A line that cannot be written does not fail the statement; the server log reports the first failure.
The path must name a regular file or nothing yet.
These events are recorded:

| `event` | When |
|---|---|
| `login` | A login succeeds or fails. |
| `create_table`, `alter_table`, `drop_table`, `recover_table` | Schema statements and RECOVER TABLE. |
| `create_user`, `alter_user`, `drop_user`, `grant`, `revoke` | User and right statements. |
| `backup` | BACKUP. |
| `reload_tls` | RELOAD TLS. |
| `enable_feature` | ENABLE FEATURE. |
| `migrate` | MIGRATE, with the table its statement names. |
| `create_slot`, `drop_slot` | Durable feed slot statements. |

Every line has `time_ms` (UTC milliseconds since the Unix epoch), `connection` (an identifier unique within the server process), `event`, `user`, `peer` and `ok`.
A failure adds `error_code`; a failed login adds `reason`: `wrong_password`, `unknown_user`, `banned`, `no_user`, `invalid_user_name` or `invalid_login`.
A failed login whose name could not name a user (see [Users and rights](USERS.md)) is written with an empty `user`.
Lines add `table`, `target_user`, `slot` or `migration` when the event names them.
Passwords, verifiers, parameter values and block data are never written.

```text
{"time_ms":1760000000000,"connection":4,"event":"login","user":"admin","peer":"198.51.100.23","ok":true}
{"time_ms":1760000000210,"connection":4,"event":"drop_table","user":"admin","peer":"198.51.100.23","ok":false,"error_code":"NO_TABLE","table":"missing"}
```

On Linux and macOS, SIGHUP makes the server reopen the audit log at its path, so you can rotate it by renaming the file and sending SIGHUP.
If the path cannot be opened, the server logs an error and keeps writing to the previous file.
The same SIGHUP also reloads the TLS certificate; see [TLS](#tls).
On Windows, rename the file and restart the server; until then it keeps writing to the renamed file.

## Directory and shared resources

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir` | `data` | Directory containing manifests, users and `tables/`; a fresh directory starts with no tables. |
| `--backup-dir` | unset | Enables BACKUP; nonempty directory path, with safe relative destinations below its resolved root. |
| `--read-only` | off | Boolean switch serving the data directory, or a completed backup, without writing to it; see [Read-only](#read-only). |
| `--max-loaded-chunks` | `65536` | Shared cache limit counted in chunks across all tables, rather than bytes; chunks in use are not evicted, so it can be exceeded briefly, and `chunkdb_eviction_no_progress_cycles_total` counts eviction passes that freed nothing. |
| `--max-open-wal-streams` | `1024` | Shared append-stream limit; POSIX descriptor reserves may clamp it. |
| `--background-maintenance` | off | Boolean switch running checkpoint and eviction work on shared workers; acknowledgement durability is unchanged. |
| `--maintenance-workers` | `2` | Positive worker count shared by background maintenance and parallel per-table `FLUSH WAL` barriers. |
| `--background-checkpoint-queue-limit` | `4096` | Per-table queue; overflow and excessive WAL growth force inline checkpointing. |

## Read-only

`--read-only` serves a data directory, or a completed [backup](BACKUP.md#3-inspect-a-backup), without writing to its files.
It takes no writer lock, so it can run next to the server that writes the directory.
It runs no recovery, checkpoints or maintenance, and reserves no revisions.
It leaves the users file and its permissions as they are, and logs a warning when the file is not owner-only.
GET BLOCK, GET CHUNK, GET AREA, SCAN CHUNKS, DESCRIBE, the SHOW statements, PING and RELOAD TLS work.
COMMIT, ROLLBACK, UNWATCH and ACK answer as they do when nothing is open, because no transaction or watch can start.
Every other statement answers `INVALID_ARGUMENT reason=invalid_argument <statement> is not available: the server opened its data directory read-only`, including WATCH, BEGIN, FLUSH WAL and BACKUP.

Reads see what the writer has put in its files; a relaxed table can keep the latest changes in the writer's memory until FLUSH WAL.
A chunk read by GET BLOCK or GET CHUNK stays in memory, within `--max-loaded-chunks`, and is read from the files again only after the writer changed its table.
GET AREA and SCAN CHUNKS use chunks in memory and read others from the files without keeping them.
Each change the writer makes to a table makes the next read of every chunk of that table load it again.
That load waits for a moment when the writer is not updating the table's files, for at most about 250 ms, and returns an error if there is none; a writer that changes a table without pause can make its reads fail.
The tables, columns and users are the ones that exist at startup.
Restart the read-only server after the writer creates, alters or drops a table or changes users.
Until then, a table the writer dropped returns an error, and in a table it altered, chunks written after the change return an error while the others are returned with the columns the read-only server started with.

A directory whose last writer stopped without closing it needs recovery, which only a writer runs.
`--read-only` refuses such a directory with a message saying so; start the server once without `--read-only` to recover it.
The same refusal appears while a writer is recovering a table or finishing a migration; try again once it has.
With `--auth scram` the directory must already have users, because `--read-only` does not create the first administrator.

## Transactions and change feed

| Flag | Default | Meaning |
|---|---|---|
| `--txn-max-duration-ms` | `5000` | Transaction lifetime from BEGIN; expiry ends it with CONFLICT. |
| `--txn-max-bytes` | `16777216` | Private written-chunk bytes for one transaction. |
| `--txn-total-bytes` | `268435456` | Private written-chunk bytes for all open transactions. |
| `--txn-history-bytes` | `67108864` | Earlier chunk states retained per table; overflow cancels oldest transactions. |
| `--feed-buffer-bytes` | `67108864` | Default live feed budget for tables without an explicit `feed_buffer_bytes` option, shared among watches. |
| `--tcp-keepalive-ms` | `60000` | Set TCP idle and repeat probe intervals, 1–32767000 ms, for every accepted connection. |
| `--feed-progress-ms` | `1000` | Emit idle or filtered watch progress at most once per interval, 1–2147483647 ms. |
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
Existing users-file permissions are tightened at startup, except with `--read-only`, which logs a warning instead.
Bind mounts must grant access to the process owner; see [Docker](DOCKER.md).

TCP keepalive applies before TLS and HELLO.
Linux and macOS round each interval up to whole seconds and send three unanswered probes.
Windows configures millisecond intervals through [Winsock](https://learn.microsoft.com/en-us/windows/win32/winsock/sio-keepalive-vals) and uses its operating system probe count.
A socket configuration failure closes that connection.
Probe timing does not guarantee an exact peer-death detection time.
The shared upper bound matches Linux’s [32767-second limit](https://github.com/torvalds/linux/blob/master/include/net/tcp.h).
