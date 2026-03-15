# Server Flags Reference

`chunkdb_server` supports the flags below.

Defaults reflect current alpha behavior (`v0.1.1-alpha` line).

## Network and Auth

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--host` | `127.0.0.1` | any bindable host/IP string | n/a | no | Server bind address. |
| `--port` | `4242` | `1..65535` | TCP port | no | Server listen port. |
| `--workers` | `hardware_concurrency` (fallback `4`) | integer `> 0` | threads | no | Worker pool size for client handling. |
| `--log-level` | `info` | `info`, `warn`, `error` | level | no | Runtime log filter (`warn` keeps WARN/ERROR, `error` keeps ERROR only). |
| `--token` | empty | non-empty string | n/a | conditional | Sets auth token and enables auth. Required unless `--no-auth` is used. |
| `--no-auth` | disabled | flag (no value) | n/a | no | Disables token auth for local/dev usage. |
| `--listen-uri` | unset | `chunk://token@host:port/` or `chunks://token@host:port/` | n/a | no | Parses host/port/token/TLS from URI and overrides individual fields. |

## Storage and Durability

| Flag | Default | Allowed values / range | Units | Required | Effect |
| --- | --- | --- | --- | --- | --- |
| `--data-dir` | `data` | valid filesystem path | path | no | Base directory for chunk files, WAL, and lock metadata. |
| `--durability` | `relaxed` | `relaxed`, `fsync-wal`, `fsync-checkpoint` | mode | no | Selects write acknowledgment and sync policy. |
| `--checkpoint-updates` | `256` | integer `> 0` | updates | no | Checkpoint trigger by pending update count per chunk. |
| `--checkpoint-wal-bytes` | `1048576` | integer `> 0` | bytes | no | Checkpoint trigger by accumulated WAL bytes per chunk. |
| `--wal-group-commit-updates` | `1` | integer `> 0` | updates | no | In `relaxed`, WAL flush batch threshold per chunk. |
| `--max-loaded-chunks` | `8192` | integer `> 0` | chunks | no | In-memory chunk cache upper bound before eviction pressure. |
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
./build/chunkdb_server --listen-uri chunk://token@127.0.0.1:4242/ --log-level info

# warnings and errors only
./build/chunkdb_server --listen-uri chunk://token@127.0.0.1:4242/ --log-level warn

# errors only
./build/chunkdb_server --listen-uri chunk://token@127.0.0.1:4242/ --log-level error
```
