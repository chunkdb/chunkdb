# chunkdb

`chunkdb` is a specialized chunk/grid storage engine for games and grid-based
simulations: bit-packed block payloads, a chunk-native text protocol with binary
chunk transfer, and explicit WAL/checkpoint durability modes.

<!-- TODO: overview animation goes here -->

Current release: **[`v1.3.0`](https://github.com/chunkdb/chunkdb/releases/tag/v1.3.0)** (stable).
The stable channel commits to the surfaces listed in
[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) under Semantic Versioning.

## Install

Linux x86_64 and macOS arm64 archives are attached to each
[release](https://github.com/chunkdb/chunkdb/releases), each with a `.sha256`
sidecar ([docs/VERIFY_RELEASE.md](docs/VERIFY_RELEASE.md)). Build from source
with a C++20 compiler and CMake 3.20+ (OpenSSL optional, for TLS):

```bash
cmake -S . -B build
cmake --build build --parallel
```

Windows builds use the MSYS2 MinGW64 shell
([docs/WINDOWS_NATIVE.md](docs/WINDOWS_NATIVE.md)); for containers see
[docs/DOCKER.md](docs/DOCKER.md).

## Quick start

```bash
printf 'chunk-token\n' > ./chunkdb.token
./build/chunkdb_server \
  --listen-uri chunk://127.0.0.1:4242/ \
  --token-file ./chunkdb.token \
  --data-dir ./data \
  --durability relaxed
```

Connect over `chunk://` (or `chunks://` for TLS) and speak the text protocol:

```text
AUTH chunk-token
SET 0 0 1111000011110000
GET 0 0
CHUNKBIN 0 0 STATE
```

Clients: [chunk-cli](https://github.com/chunkdb/chunk-cli),
[@chunkdb/client](https://github.com/chunkdb/chunkdb-js) for Node.js, and
[chunkdb-go](https://github.com/chunkdb/chunkdb-go). `chunkdb_verify --data-dir
./data` checks a data directory offline without modifying it.

## Documentation

- [Protocol](docs/PROTOCOL.md), [server flags](docs/SERVER_FLAGS.md), [durability contract](docs/DURABILITY_CONTRACT.md), [known limitations](docs/KNOWN_LIMITATIONS.md)
- [Storage format](docs/STORAGE_FORMAT.md), [runtime flow](docs/RUNTIME_FLOW.md), [concurrency](docs/CONCURRENCY.md), [backends](docs/BACKENDS.md), [performance](docs/PERFORMANCE.md)
- [Compatibility policy](docs/COMPATIBILITY.md), [release policy](docs/RELEASE_POLICY.md), [changelog](CHANGELOG.md), [contributing](CONTRIBUTING.md), [issue policy](docs/ISSUE_POLICY.md)

## License

MIT. See [LICENSE](LICENSE).
