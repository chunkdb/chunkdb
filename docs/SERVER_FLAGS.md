# Server Flags Reference

`chunkdb_server` supports the flags below.

Defaults reflect the stable `v1.0.0` server behavior unless a flag says otherwise.

## Network and Auth

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--host` | `127.0.0.1` | any bindable host/IP string | n/a | no | Server bind address. |
| `--port` | `4242` | `1..65535` | TCP port | no | Server listen port. |
| `--workers` | `hardware_concurrency` (fallback `4`) | integer `> 0` | threads | no | Worker pool size for client handling. |
| `--client-io-timeout-ms` | `5000` | `1..86400000` | milliseconds | no | Per-client active-phase timeout: the time for a TLS handshake, for a request line or payload from its first byte (over TLS, from the first byte of its record), for a full reply write, and for a new connection to complete `HELLO`. |
| `--idle-connection-timeout-ms` | `60000` | `1..86400000` | milliseconds | no | Idle keep-alive timeout between complete requests. Before `HELLO` the wait is the smaller of this and `--client-io-timeout-ms`. Long-idle connections are closed so they do not pin workers indefinitely. |
| `--max-pending-clients` | `1024` | integer `> 0` | connections | no | Upper bound for accepted clients waiting in the pending queue before worker pickup. Extra plain TCP connections receive `-ERR BUSY` and close under overload. |
| `--max-handshakes-per-ip` | off | integer `> 0` | connections | no | Most connections from one source (an IPv4 address, an IPv6 /64) that may hold a worker at once before `HELLO` succeeds, TLS handshake included. More get `-ERR BUSY too many connections before HELLO from this address` (plain TCP) and are closed. Off by default, since one host opening many connections at once (a client pool warming up) briefly has that many; set it below `--workers` when the port is reachable from untrusted networks. |
| `--max-line-bytes` | `65536` | integer `> 0` | bytes | no | Maximum length of one request line including its terminator. Longer lines get `-ERR BAD_REQUEST` and the connection is closed. Raise it for long `MSET` or `CHUNKBATCH` lines; `CHUNKPUT` payloads are not subject to this limit. Reported by `HELLO`. |
| `--log-level` | `info` | `info`, `warn`, `error` | level | no | Runtime log filter (`warn` keeps WARN/ERROR, `error` keeps ERROR only). |
| `--token-file` | unset | path to file containing token | path | conditional | Reads auth token from a file and enables auth. |
| `--token` | empty | non-empty string | n/a | conditional | Sets auth token and enables auth. Development-only because command-line tokens can be exposed through shell/process listings. |
| `--no-auth` | disabled | flag (no value) | n/a | no | Disables token auth for local/dev usage. Logs a WARN when used with a non-loopback bind address. |
| `--listen-uri` | unset | `chunk://chunk-token@host:port/` or `chunks://chunk-token@host:port/` | n/a | no | Parses host/port/token/TLS from URI and overrides individual fields. URI tokens are development-only because they can be exposed through logs, shell history, and process listings. |

Token source priority:

1. `--no-auth` disables authentication.
2. `--token-file <path>`
3. `CHUNKDB_TOKEN`
4. `--token <token>` (development-only)
5. token embedded in `--listen-uri` (development-only)

For deployments, prefer `--token-file` or `CHUNKDB_TOKEN` over command-line or URI tokens.

## Data Directory and Shared Budgets

A data directory holds named tables (`docs/PROTOCOL.md` commands 19-24). The
flags in this section apply to the server process and all its tables.

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--data-dir` | `data` | valid filesystem path | path | no | Data directory: the data-directory manifest, the writer lock and one directory per table under `tables/`. A new or empty directory gets a `default` table. |
| `--max-loaded-chunks` | `65536` | integer `> 0` | chunks | no | Upper bound for cached chunks of all tables together. Eviction picks the least recently used chunks across tables, so a busy table can use memory an idle table does not. The bound counts chunks, not bytes: a chunk of a table with wider blocks or larger chunks takes more memory, and `text` and `bytes` values add up to the table's `var_max_chunk_bytes` per chunk. |
| `--max-open-wal-streams` | `1024` (auto-clamped by OS file-descriptor limit reserve on POSIX) | integer `> 0` | streams | no | Upper bound for concurrently open WAL append streams of all tables together. |
| `--allow-multi-process` | disabled | flag (no value) | n/a | no | Disables single-writer guard. Use only for controlled experiments. |
| `--background-maintenance` | disabled | flag (no value) | n/a | no | Runs checkpoint compaction and cache eviction on a dedicated maintenance thread per table instead of request threads. Backpressure: when the checkpoint queue is full or a chunk's WAL exceeds 4x its checkpoint thresholds, the writer checkpoints inline; a failed background checkpoint is retried inline by the next eligible write so the error reaches a caller. The queue is drained on clean shutdown. |
| `--background-checkpoint-queue-limit` | `4096` | integer `> 0` | requests | no | Bound for each table's background checkpoint queue when `--background-maintenance` is enabled. |

## Table Options

Each table records these options when it is created and keeps them across
restarts. The flags are the options of tables this server creates: `default`
when the data directory has no table, and `TABLECREATE` without the option.
A flag that is given must also match the option every existing table stores;
otherwise the server refuses to start, names the flag, the table and the
stored value, and changes nothing on disk. Omit the flag to start with tables
whose options differ, and change a table with `TABLESET`.

| Flag | Default | Allowed values / range | Units | Table option | Effect |
| --- | --- | --- | --- | --- | --- |
| `--durability` | `relaxed` | `relaxed`, `fsync-wal`, `fsync-checkpoint` | mode | `durability_mode` | Selects write acknowledgment and sync policy. |
| `--checkpoint-updates` | `256` | integer `> 0` | updates | `checkpoint_updates` | Checkpoint trigger by pending update count per chunk. |
| `--checkpoint-wal-bytes` | `1048576` | integer `> 0` | bytes | `checkpoint_wal_bytes` | Checkpoint trigger by accumulated WAL bytes per chunk. |
| `--wal-group-commit-updates` | `8` | integer `> 0` | updates | `wal_group_commit_updates` | In `relaxed`, WAL flush batch threshold per chunk. |
| `--checkpoint-compression` | `none` | `none`, `zrle` | mode | `checkpoint_compression` | Compresses newly written checkpoint images with the internal `zrle` codec. Images written either way remain readable. |

`var_max_chunk_bytes` has no flag: tables start with 1048576, and `TABLECREATE` or `TABLESET` changes it.

## Geometry

The geometry flags describe the `default` table, which the server creates
when the data directory has no table. Geometry is fixed when a table is
created and recorded in its manifest. When `default` exists, these flags may
be omitted and its stored geometry is used; a flag that is given must match
the stored value, otherwise the server refuses to start, names the stored and
the requested values, and changes nothing on disk. Other tables get their
geometry from `TABLECREATE`.

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--large-chunk-width` | `8` (new table) | integer `1..1000000` | chunks | no | Large-chunk width in regular chunks. |
| `--large-chunk-height` | `8` (new table) | integer `1..1000000` | chunks | no | Large-chunk height in regular chunks. |
| `--chunk-width` | `16` (new table) | integer `1..4096` | blocks | no | Regular chunk width in blocks. |
| `--chunk-height` | `16` (new table) | integer `1..4096` | blocks | no | Regular chunk height in blocks. |
| `--block-bits` | `16` (new table) | integer `1..65535` | bits | no | Bit width of one block payload. |

Geometry must also satisfy:

- `chunk_width * chunk_height <= 1048576`
- chunk payload size `ceil(chunk_width * chunk_height * block_bits / 8) <= 67108864` bytes

## TLS

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--tls-cert` | empty | path to PEM cert | path | conditional | Required when TLS is enabled (`chunks://` URI). |
| `--tls-key` | empty | path to PEM private key | path | conditional | Required when TLS is enabled (`chunks://` URI). |

## Notes

- `--help` or `-h` prints usage and exits.
- `--listen-uri` can enable TLS implicitly (`chunks://...`), which then requires `--tls-cert` and `--tls-key`.
- `--max-line-bytes` bounds text request lines only. Binary chunk writes
  (`CHUNKPUT`) are bounded by the selected table's chunk state size instead.

## Lifecycle Log Format

`chunkdb_server` emits concise lifecycle/runtime lines in this format:

```text
<timestamp> <level> <component> pid=<pid> <message> <k=v ...>
```

Example startup line:

```text
2026-03-15T18:30:12.123Z INFO server pid=1234 ready to accept connections protocol=tcp host=127.0.0.1 port=4242 tls=off workers=4
```

Example warning line:

```text
2026-03-15T18:31:03.771Z WARN server pid=1234 bad request disconnect reason="request line exceeds max_line_bytes"
```

Log level usage:

These examples assume `./chunkdb.token` contains the auth token.

```bash
# default (INFO/WARN/ERROR)
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --log-level info

# warnings and errors only
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --log-level warn

# errors only
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --log-level error
```
