# Server Flags Reference

`chunkdb_server` supports the flags below.

Defaults reflect the current release-preview line (`v0.1.1-preview`) and engineering alpha maturity.

## Network and Auth

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--host` | `127.0.0.1` | any bindable host/IP string | n/a | no | Server bind address. |
| `--port` | `4242` | `1..65535` | TCP port | no | Server listen port. |
| `--workers` | `hardware_concurrency` (fallback `4`) | integer `> 0` | threads | no | Worker pool size for client handling. |
| `--client-io-timeout-ms` | `5000` | integer `> 0` | milliseconds | no | Per-client active-phase timeout used to bound TLS handshake completion, stalled partial-request reads, and full reply writes. Also used as the underlying socket read/write inactivity timeout while those phases are in progress. |
| `--idle-connection-timeout-ms` | `60000` | integer `> 0` | milliseconds | no | Idle keep-alive timeout applied only between complete requests. Long-idle connections are closed so they do not pin workers indefinitely. |
| `--max-pending-clients` | `1024` | integer `> 0` | connections | no | Upper bound for accepted clients waiting in the pending queue before worker pickup. Extra connections are closed early under overload. |
| `--log-level` | `info` | `info`, `warn`, `error` | level | no | Runtime log filter (`warn` keeps WARN/ERROR, `error` keeps ERROR only). |
| `--token` | empty | non-empty string | n/a | conditional | Sets auth token and enables auth. Required unless `--no-auth` is used. |
| `--no-auth` | disabled | flag (no value) | n/a | no | Disables token auth for local/dev usage. |
| `--listen-uri` | unset | `chunk://chunk-token@host:port/` or `chunks://chunk-token@host:port/` | n/a | no | Parses host/port/token/TLS from URI and overrides individual fields. |

## Storage and Durability

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--data-dir` | `data` | valid filesystem path | path | no | Base directory for chunk files, WAL, and lock metadata. |
| `--durability` | `relaxed` | `relaxed`, `fsync-wal`, `fsync-checkpoint` | mode | no | Selects write acknowledgment and sync policy. |
| `--checkpoint-updates` | `256` | integer `> 0` | updates | no | Checkpoint trigger by pending update count per chunk. |
| `--checkpoint-wal-bytes` | `1048576` | integer `> 0` | bytes | no | Checkpoint trigger by accumulated WAL bytes per chunk. |
| `--wal-group-commit-updates` | `8` | integer `> 0` | updates | no | In `relaxed`, WAL flush batch threshold per chunk. |
| `--max-loaded-chunks` | `65536` | integer `> 0` | chunks | no | In-memory chunk cache upper bound before eviction pressure. |
| `--max-open-wal-streams` | `1024` (auto-clamped by OS file-descriptor limit reserve on POSIX) | integer `> 0` | streams | no | Upper bound for concurrently open WAL append streams. |
| `--allow-multi-process` | disabled | flag (no value) | n/a | no | Disables single-writer guard. Use only for controlled experiments. |

## Geometry

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--large-chunk-width` | `8` | integer `> 0` (`u32`) | chunks | no | Large-chunk width in regular chunks. |
| `--large-chunk-height` | `8` | integer `> 0` (`u32`) | chunks | no | Large-chunk height in regular chunks. |
| `--chunk-width` | `16` | integer `> 0` (`u32`) | blocks | no | Regular chunk width in blocks. |
| `--chunk-height` | `16` | integer `> 0` (`u32`) | blocks | no | Regular chunk height in blocks. |
| `--block-bits` | `16` | integer `> 0` (`u32`) | bits | no | Bit width of one block payload. |

## TLS

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--tls-cert` | empty | path to PEM cert | path | conditional | Required when TLS is enabled (`chunks://` URI). |
| `--tls-key` | empty | path to PEM private key | path | conditional | Required when TLS is enabled (`chunks://` URI). |

## Notes

- `--help` or `-h` prints usage and exits.
- `--listen-uri` can enable TLS implicitly (`chunks://...`), which then requires `--tls-cert` and `--tls-key`.
- `max_line_bytes` is currently fixed in code (`65536`) and is not exposed as a CLI flag yet.

## Lifecycle Log Format

`chunkdb_server` emits concise lifecycle/runtime lines in this format:

```text
<timestamp> <level> <component> pid=<pid> <message> <k=v ...>
```

Example startup line:

```text
2026-03-15 18:30:12.123 INFO server pid=1234 ready to accept connections protocol=tcp host=127.0.0.1 port=4242 tls=off workers=4
```

Example warning line:

```text
2026-03-15 18:31:03.771 WARN server pid=1234 bad request disconnect reason="request line exceeds max_line_bytes"
```

Log level usage:

```bash
# default (INFO/WARN/ERROR)
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --log-level info

# warnings and errors only
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --log-level warn

# errors only
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --log-level error
```
