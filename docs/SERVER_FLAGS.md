# Server flags in 2.0

`chunkdb_server --help` (or `-h`) prints the accepted options.
Flags take the following argument as their value, except the two boolean switches below.
Numeric values are positive decimal integers, with the additional bounds shown here.

## Connection and authentication

| Flag | Default | Meaning and bounds |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address. |
| `--port` | `4242` | TCP port, 1–65535. |
| `--listen-uri` | unset | `chunk://host:port/` or `chunks://host:port/`; sets address, port and TLS at this point in argument order; a username is refused. |
| `--workers` | CPU thread count, or 4 | Client statement workers. |
| `--client-io-timeout-ms` | `5000` | 1–86400000 ms for handshake, HELLO, an active request or a complete reply write. |
| `--idle-connection-timeout-ms` | `60000` | 1–86400000 ms between requests; before HELLO the smaller I/O/idle timeout applies. |
| `--max-pending-clients` | `1024` | Accepted clients waiting for a worker; overflow is refused. |
| `--max-handshakes-per-ip` | off | Connections from an IPv4 address or IPv6 /64 occupying workers before HELLO; exceeding the supplied positive limit is refused. |
| `--max-line-bytes` | `65536` | Request line limit including terminator; parameter frames are bounded separately by column/chunk size. |
| `--log-level` | `info` | `info`, `warn`, `error`. |
| `--auth` | `scram` | `scram` or `none`; `none` grants all rights and is intended for local development. |
| `--admin-user` | unset | First administrator's name, used only when no users exist. |
| `--admin-password-file` | unset | First administrator's password from the first line of this file. |
| `--tls-cert` | unset | PEM certificate required by a `chunks://` listener. |
| `--tls-key` | unset | PEM private key required by a `chunks://` listener. |

`CHUNKDB_ADMIN_USER` and `CHUNKDB_ADMIN_PASSWORD` supply bootstrap credentials; explicit corresponding flags take precedence.
A directory with no users needs both credentials unless `--auth none` is selected.
Existing users are loaded on later starts; bootstrap settings do not change their passwords.
Use [users and rights](USERS.md) for password changes and offline recovery.

## Directory and shared resources

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir` | `data` | Directory containing manifests, users and `tables/`; an initialized directory with no tables receives `default`. |
| `--backup-dir` | unset | Enables BACKUP; nonempty directory path, with safe relative destinations below its resolved root. |
| `--max-loaded-chunks` | `65536` | Shared cache limit counted in chunks across all tables, rather than bytes. |
| `--max-open-wal-streams` | `1024` | Shared append-stream limit; POSIX descriptor reserves may clamp it. |
| `--allow-multi-process` | off | Boolean switch disabling single-writer ownership; shared writers are unsupported and transactions, feed, migrations and backup are unavailable. |
| `--background-maintenance` | off | Boolean switch running checkpoint/eviction maintenance per table; acknowledgement durability is unchanged. |
| `--background-checkpoint-queue-limit` | `4096` | Per-table queue; overflow and excessive WAL growth force inline checkpointing. |

## Transactions and change feed

| Flag | Default | Meaning |
|---|---|---|
| `--txn-max-duration-ms` | `5000` | Transaction lifetime from BEGIN; expiry ends it with CONFLICT. |
| `--txn-max-bytes` | `16777216` | Private written-chunk bytes for one transaction. |
| `--txn-total-bytes` | `268435456` | Private written-chunk bytes for all open transactions. |
| `--txn-history-bytes` | `67108864` | Earlier chunk states retained per table; overflow cancels oldest transactions. |
| `--feed-buffer-bytes` | `67108864` | Live feed budget per table, shared among watches. |
| `--feed-linger-ms` | `30000` | Keep live history after the last watch closes, 0–2147483647 ms; 0 releases it immediately. Writes keep copying into the feed during this interval; slots retain history independently. |
| `--max-watches` | `64` | Concurrent server watches; excess receives BUSY. |
| `--slot-max-bytes` | `1073741824` | Retained history per slot; exceeding it marks that slot lost. |
| `--slot-sync-ms` | `100` | Durable frontier pass interval in ms, 1–2147483647; see [ACK batching](CHANGE_FEED.md#durable-slots). |

Watches release statement workers and have no idle timeout.
See [transactions](TRANSACTIONS.md) and [change feed](CHANGE_FEED.md) for outcomes at limits.

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
`var_max_chunk_bytes` has no flag: its default is 1048576 and CQL changes it.
See the [durability contract](DURABILITY_CONTRACT.md).

## Default-table geometry

| Flag | Default | Bounds |
|---|---|---|
| `--large-chunk-width` | `8` | 1–1000000 chunks. |
| `--large-chunk-height` | `8` | 1–1000000 chunks. |
| `--chunk-width` | `16` | 1–4096 blocks. |
| `--chunk-height` | `16` | 1–4096 blocks. |
| `--block-bits` | `16` | 1–65535 bits in the default table's `bits` column. |

These flags describe only `default`; other tables use CREATE TABLE geometry.
An omitted flag uses stored geometry; a supplied value must match an existing default table.
Chunk width × height must not exceed 1048576 and packed payload must not exceed 67108864 bytes.
Geometry is fixed for the lifetime of the table.
