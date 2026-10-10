# chunkdb

chunkdb 2.0 stores grid data in tables of chunks of typed blocks.
Each table fixes its chunk geometry and defines named columns: integers, booleans, floats, bits, text and bytes.
CQL reads and writes blocks, transfers binary chunks and reads bounded areas over TCP or TLS.

Start with the [quick start](docs/QUICK_START.md): run the server, connect with the CLI, write two blocks and watch a change.
Use the [Go client](https://github.com/chunkdb/chunkdb-go), [Node.js/TypeScript client](https://github.com/chunkdb/chunkdb-js) or [CLI](https://github.com/chunkdb/chunk-cli) for applications and scripts.

## Model and operations

- [CQL](docs/CQL.md) defines tables, typed columns, block and chunk operations, area reads and named migrations.
- [Users and rights](docs/USERS.md) authenticate with SCRAM-SHA-256 and control access per table.
- [Transactions](docs/TRANSACTIONS.md) read a snapshot and commit changes to several chunks of one table together, durably in every mode.
- [Change feed and durable slots](docs/CHANGE_FEED.md) stream committed changes and retain consumer positions across restarts.
- [Online backup and offline restore](docs/BACKUP.md) copy a running server and restore to a new data directory.
- [Durability](docs/DURABILITY_CONTRACT.md) specifies WAL acknowledgements, checkpoints, barriers and failure outcomes.

Block coordinates address individual blocks; area and chunk operations use chunk coordinates.
Absent blocks differ from present blocks whose values are zero or NULL.
A chunk version is an equality token for conditional writes, rather than a timestamp.

## Install

Use [Docker](docs/DOCKER.md), a platform archive from [Releases](https://github.com/chunkdb/chunkdb/releases), or build from source with C++20 and CMake 3.20 or later.
OpenSSL enables TLS; [Windows native](docs/WINDOWS_NATIVE.md) describes the MSYS2 MinGW64 toolchain.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 3
```

The build supplies `chunkdb_server`, `chunkdb_verify`, `chunkdb_restore` and `chunkdb_admin`.
The quick start shows authenticated startup; `--auth none` is available for local development.

## Reference

[Protocol 3](docs/PROTOCOL.md), [server flags](docs/SERVER_FLAGS.md), [storage format](docs/STORAGE_FORMAT.md), [compatibility](docs/COMPATIBILITY.md) and [known limitations](docs/KNOWN_LIMITATIONS.md) describe the 2.0 surface.
Within 2.x, new protocol and storage features are additive; 2.0 clients and data remain usable with later 2.x servers.
Breaking changes require 3.0.
See the compatibility page for the direction of data readability and client versioning.

Report a reproducible problem through [issues](https://github.com/chunkdb/chunkdb/issues).
The software is licensed under [MIT](LICENSE).
