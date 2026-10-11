# chunkdb

chunkdb stores grid data in tables of chunks of typed blocks.

A **block** is one grid cell with named columns, such as `kind` and `name`.
A **chunk** groups a fixed rectangle of blocks.
A **table** defines their column types and chunk size.
An **area** selects a rectangle of chunks.

After [Quick start](docs/QUICK_START.md) connects your CLI shell, try:

```cql docs-check session=main
CREATE TABLE world (kind u8, name text(16) NULL) CHUNK 2 x 2
SET BLOCK 0 0 IN world kind = 1, name = 'grass'
GET BLOCK 0 0 FROM world
```

```output
OK
2
kind = 1
name = 'grass'
```

The write returns a chunk **version**, a token you compare for equality when making conditional writes.

## Install

Use [Docker](docs/DOCKER.md), [Install the server](docs/INSTALL.md), or [Windows native](docs/WINDOWS_NATIVE.md).
Application clients are available for [Go](https://github.com/chunkdb/chunkdb-go) and [Node.js/TypeScript](https://github.com/chunkdb/chunkdb-js).
Use [chunk-cli](https://github.com/chunkdb/chunk-cli) for a shell and scripts.

## Documentation

- [Quick start](docs/QUICK_START.md): start, connect, write, read, and watch.
- [CQL reference](docs/CQL.md): statements, arguments, results, and errors.
- [Transactions](docs/TRANSACTIONS.md): change several chunks together.
- [Change feed](docs/CHANGE_FEED.md): consume committed changes.
- [Users and rights](docs/USERS.md): control access to tables.
- [Backup and restore](docs/BACKUP.md): copy and recover data.
- [Durability](docs/DURABILITY_CONTRACT.md), [Compatibility](docs/COMPATIBILITY.md), and [Limitations](docs/KNOWN_LIMITATIONS.md): guarantees and boundaries.
- [Server flags](docs/SERVER_FLAGS.md), [Protocol](docs/PROTOCOL.md), and [Storage format](docs/STORAGE_FORMAT.md): detailed references.

Report reproducible problems through [Issues](https://github.com/chunkdb/chunkdb/issues).
chunkdb uses the [MIT license](LICENSE).
