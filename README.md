# chunk

`chunk` is a custom, high-performance C++ game database designed for universal chunk/block storage.

It stores data as:
1. Large chunks
2. Regular chunks inside large chunks
3. Blocks inside regular chunks

Block semantics are never hardcoded. The user configures block width in bits (`block_bits`) and writes/reads raw bit sequences for each block.

## Core Features

- Original architecture (not a PostgreSQL/Redis clone)
- Compact packed-bit storage for blocks
- Configurable geometry:
  - large chunk width/height (in regular chunks)
  - regular chunk width/height (in blocks)
  - block bit size (`block_bits`)
- Custom text protocol with strict wire format
- Token-based authentication (`AUTH <token>`)
- Connection URI format:
  - insecure: `chunk://token@host:port/`
  - TLS: `chunks://token@host:port/`
- Per-chunk write-ahead recovery (`.wal`) and checksum validation
- Concurrency-safe chunk-level locking
- TDD with separate tests for protocol, storage, concurrency, auth, URI parsing, and errors

## Build

Prerequisites:
- C++20 compiler
- CMake 3.20+
- (Optional) OpenSSL for TLS (`chunks://` transport)

```bash
cmake -S . -B build
cmake --build build -j
```

If OpenSSL is unavailable, server builds without TLS and prints a CMake warning.

## Run

### Insecure server

```bash
./build/chunkdb_server \
  --listen-uri chunk://mytoken@127.0.0.1:6752/ \
  --data-dir ./data \
  --large-chunk-width 8 \
  --large-chunk-height 8 \
  --chunk-width 16 \
  --chunk-height 16 \
  --block-bits 16
```

### TLS server (`chunks://`)

```bash
./build/chunkdb_server \
  --listen-uri chunks://mytoken@127.0.0.1:6752/ \
  --tls-cert ./certs/server.crt \
  --tls-key ./certs/server.key \
  --data-dir ./data
```

## Protocol Example

```text
AUTH mytoken
+OK

SET 0 0 0000000000001111
+OK

GET 0 0
$16
0000000000001111

CHUNK 0 0
$4096
<chunk bits>
```

Full protocol: [docs/PROTOCOL.md](docs/PROTOCOL.md)

## Storage and Concurrency Specs

- On-disk layout and binary format: [docs/STORAGE_FORMAT.md](docs/STORAGE_FORMAT.md)
- Locking rules and consistency guarantees: [docs/CONCURRENCY.md](docs/CONCURRENCY.md)

## Testing

```bash
ctest --test-dir build --output-on-failure
```

Current test executables:
- `chunkdb_geometry_bit_test`
- `chunkdb_storage_test`
- `chunkdb_protocol_test`
- `chunkdb_auth_test`
- `chunkdb_error_test`
- `chunkdb_concurrency_test`
- `chunkdb_uri_test`

## License

MIT. See [LICENSE](LICENSE).
