# chunk

`chunk` is a specialized chunk-based, bit-packed storage engine for games and grid/world simulations.

Status: **Engineering Alpha**.

It is **not** positioned as a generic replacement for PostgreSQL or Redis.
Comparisons with PostgreSQL/Redis are only meaningful for narrow chunk-world workloads and should be backed by benchmark results.

## Alpha Scope

Current alpha scope is intentionally limited to one storage backend (`fs_split_v1`) and stabilization/quality work.
No backend expansion is part of this alpha milestone.

See [docs/ALPHA.md](docs/ALPHA.md) for milestone boundaries.

## Positioning

`chunk` targets workloads where data is naturally modeled as:
1. large chunks
2. regular chunks
3. fixed-width block bitfields

The engine stores arbitrary block bit patterns defined by user configuration (`block_bits`), not a hardcoded block schema.

## Current Architecture

- Coordinate mapping:
  - world block `(x, y)` -> regular chunk `(cx, cy)` -> local block index
  - regular chunk -> large chunk `(lx, ly)`
- Packed payload per regular chunk:
  - `chunk_width_blocks * chunk_height_blocks * block_bits`
  - stored as contiguous packed bytes
- Current disk backend (`fs_split_v1`):
  - large chunk = directory `L_<lx>_<ly>`
  - regular chunk data file `C_<cx>_<cy>.chk`
  - regular chunk WAL file `C_<cx>_<cy>.wal`

See:
- [docs/STORAGE_FORMAT.md](docs/STORAGE_FORMAT.md)
- [docs/BACKENDS.md](docs/BACKENDS.md)

## Write Path (current)

`SET` does not rewrite full chunk files for normal updates.

Current flow:
1. update only touched bytes in in-memory packed payload
2. append a delta record to per-chunk WAL (`offset + bytes + record CRC`)
3. periodically checkpoint full `.chk` image by threshold:
  - `checkpoint_update_interval`
  - `checkpoint_wal_bytes`

## Durability Modes

Durability is explicit and configurable:

- `relaxed`
  - no fsync
  - fastest, weakest crash/power-loss guarantees
- `fsync-wal`
  - fsync WAL on each `SET`
  - acknowledged deltas are durable in WAL under normal fsync assumptions
- `fsync-checkpoint`
  - `fsync-wal` behavior plus fsync on checkpointed chunk images/directory updates
  - strongest mode currently available

`chunk` does **not** claim full ACID guarantees.

## Concurrency and Runtime

- In-memory locking:
  - global large-chunk map mutex
  - per-large-chunk map mutex
  - per-regular-chunk `shared_mutex`
- Reads on same chunk are concurrent.
- Writes on same chunk are serialized.
- Worker-pool server runtime (detached thread-per-connection removed).
- Buffered socket parsing (not 1-byte read loops).
- Inter-process protection:
  - `data_dir` lock file blocks multiple server processes by default
  - override with `--allow-multi-process` only if external coordination exists

See [docs/CONCURRENCY.md](docs/CONCURRENCY.md).

## Protocol

Text protocol with strict framing and token auth support.

Key commands:
- `AUTH <token>`
- `GET <x> <y>`
- `SET <x> <y> <bits>`
- `CHUNK <cx> <cy>` (text bits)
- `CHUNKBIN <cx> <cy>` (raw packed bytes, preferred for high-volume transfer)
- `INFO`
- `QUIT`

See [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Build

Prerequisites:
- C++20 compiler
- CMake 3.20+
- Optional OpenSSL for TLS (`chunks://`)

```bash
cmake -S . -B build
cmake --build build -j
```

## Run

```bash
./build/chunkdb_server \
  --listen-uri chunk://mytoken@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability fsync-wal \
  --checkpoint-updates 512 \
  --checkpoint-wal-bytes 1048576 \
  --max-loaded-chunks 8192
```

TLS mode:

```bash
./build/chunkdb_server \
  --listen-uri chunks://mytoken@127.0.0.1:4242/ \
  --tls-cert ./certs/server.crt \
  --tls-key ./certs/server.key
```

## Benchmarks

Direct storage API benchmark:

```bash
./build/chunkdb_bench --ops 20000
```

End-to-end server-path benchmark:

```bash
./build/chunkdb_server_bench --ops 5000 --port 4242
```

See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for scenario coverage and comparison caveats.

## Test Suite

Current test coverage includes:
- protocol/auth/error handling
- storage/recovery
- durability mode parsing
- process lock behavior
- concurrency correctness
- cache eviction behavior
- combined concurrency + eviction stress
- kill-recovery durability scenario

## Release Notes (Alpha)

For this alpha release, implemented and stabilized:
- split-files backend (`fs_split_v1`)
- delta WAL + checkpoint write path
- explicit durability modes
- worker-pool runtime + buffered parsing
- binary chunk transfer command (`CHUNKBIN`)
- cache limit + eviction
- inter-process data-dir lock
- storage and server-path benchmark executables

Out of scope for this alpha:
- new storage backends
- broad cross-database performance claims

## License

MIT. See [LICENSE](LICENSE).
