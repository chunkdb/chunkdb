# chunk

[![CI](https://github.com/chunkdb/chunkdb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/chunkdb/chunkdb/actions/workflows/ci.yml)

`chunkdb` is a specialized chunk/grid storage engine for games and grid-based simulations with bit-packed block payloads.

Release target: **`v0.1.1-alpha`**.

## Project Identity

- **Specialized chunk/grid engine**: optimized for chunk-oriented worlds, not a general-purpose multi-model platform.
- **Chunk-native protocol**: text command protocol with optional binary chunk transfer for high-volume reads.
- **Bit-packed storage model**: configurable `block_bits` and chunk geometry for compact world-state representation.
- **Chunk-oriented access model**: efficient point block `GET`/`SET` and full-chunk reads (`CHUNK`, `CHUNKBIN`).
- **WAL/checkpoint durability modes**: explicit behavior trade-offs (`relaxed`, `fsync-wal`, `fsync-checkpoint`).

## Stability Status

- Stage: Engineering alpha.
- Focus: correctness, durability behavior, runtime scalability, and transparent benchmarks.
- Current status:
  - core storage/runtime path is implemented and tested
  - format/protocol are versioned but still alpha-level
  - production hardening is incomplete

See [docs/ALPHA.md](docs/ALPHA.md) for alpha boundaries.

## Included in `v0.1.1-alpha`

- specialized chunk hierarchy:
  - large chunk -> regular chunk -> block bitfield
- configurable geometry and block width (`block_bits`)
- backend: `fs_split_v1` (large chunk directory + regular chunk files)
- delta WAL + checkpoint write path (including configurable relaxed-mode WAL group commit)
- durability modes: `relaxed`, `fsync-wal`, `fsync-checkpoint`
- worker-pool TCP server with buffered parsing
- text protocol and binary chunk transfer (`CHUNKBIN`)
- chunk cache limit + eviction
- single-writer/multi-reader process coordination via `.chunkdb.lock` (writer PID/session/heartbeat metadata)
- direct API benchmark + server-path benchmark
- concurrency/eviction stress tests
- durability recovery tests:
  - kill-recovery path (`fsync-wal`, `fsync-checkpoint`)
  - WAL replay edge scenarios (truncated tails/headers)
  - long-run WAL growth + checkpoint cycle validation

## Out of Scope for `v0.1.1-alpha`

- additional storage backends
- distributed features (replication/sharding/consensus)
- cross-chunk transactions / full ACID semantics
- broad cross-system performance claims

## Architecture Summary

- Coordinate mapping:
  - block `(x, y)` -> regular chunk `(cx, cy)` -> local index
  - regular chunk `(cx, cy)` -> large chunk `(lx, ly)`
- Packed regular-chunk payload:
  - `chunk_width_blocks * chunk_height_blocks * block_bits` bits
  - contiguous packed bytes in memory and on disk
- Disk layout (`fs_split_v1`):
  - `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.chk`
  - `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.wal`

Reference docs:
- [docs/STORAGE_FORMAT.md](docs/STORAGE_FORMAT.md)
- [docs/BACKENDS.md](docs/BACKENDS.md)
- [docs/CONCURRENCY.md](docs/CONCURRENCY.md)

## Protocol, Startup, and Connection Examples

Default URI forms:
- insecure: `chunk://token@127.0.0.1:4242/`
- TLS: `chunks://token@127.0.0.1:4242/`

Server startup:

```bash
./build/chunkdb_server \
  --listen-uri chunk://mytoken@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability fsync-wal \
  --checkpoint-updates 512 \
  --checkpoint-wal-bytes 1048576 \
  --wal-group-commit-updates 8 \
  --max-loaded-chunks 8192
```

Quick protocol session example:

```text
AUTH mytoken
SET 0 0 1111000011110000
GET 0 0
CHUNKBIN 0 0
INFO
QUIT
```

Command reference:
- [docs/PROTOCOL.md](docs/PROTOCOL.md)

## Benchmark Scope

Benchmarks are scoped to chunk/grid workloads and reported as:
- direct storage API path (`chunkdb_bench`)
- end-to-end server path (`chunkdb_server_bench`)

They characterize this engine under its chunk-oriented workload model:
- operation latency/throughput for implemented point/chunk commands
- text vs binary chunk transfer behavior
- cold/warm path behavior and server runtime overhead

Benchmark results are intentionally workload-scoped and should not be treated as global rankings across unrelated database categories.

See [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

Latest measured snapshot (Apple M1 Pro, 32 GB RAM, `relaxed` mode) is published in [docs/PERFORMANCE.md#measured-snapshot-apple-m1-pro-32-gb-ram](docs/PERFORMANCE.md#measured-snapshot-apple-m1-pro-32-gb-ram).

## Durability Guarantees Matrix

| Mode | Write Acknowledgement Path | Crash/Restart Behavior | Power-Loss Risk | Not Guaranteed |
| --- | --- | --- | --- | --- |
| `relaxed` | `SET` returns after WAL append path without `fsync` (and may batch WAL flushes by `wal_group_commit_updates`) | Recovery replays WAL bytes that were flushed to disk | Highest risk of losing recent acknowledged writes on crash/power loss | No cross-chunk atomicity, no replication, no full ACID semantics |
| `fsync-wal` | `SET` returns after WAL append and WAL `fsync` | Recovery replays durable WAL deltas onto chunk image | Lower risk for acknowledged writes, but still depends on OS/filesystem/device honoring `fsync` | No cross-chunk atomicity, no replication, no full ACID semantics |
| `fsync-checkpoint` | `fsync-wal` path + checkpoint image/directory sync on checkpoint | Recovery uses fsynced WAL and fsynced checkpoints | Strongest mode in current engine, still not equivalent to full transactional DB guarantees | No cross-chunk atomicity, no replication, no full ACID semantics |

This matrix summarizes current behavior only for the implemented alpha architecture.

## Reproducible Benchmark Artifacts

Generate a reproducible benchmark bundle locally:

```bash
scripts/bench/run_reproducible_benchmarks.sh
```

Bundle format and required files:
- [bench/artifacts/README.md](bench/artifacts/README.md)

GitHub automation:
- `.github/workflows/benchmark-artifacts.yml`
- supports manual runs and release-triggered artifact generation

## Current Limitations

- only one storage backend is included (`fs_split_v1`)
- no multi-chunk atomic transaction model
- no replication/distributed durability
- multi-process mode is SWMR only (single writer, read-only readers); shared multi-writer is unsupported
- durability guarantees are mode-dependent and below full ACID DB guarantees
- benchmark suite is focused and not a full production workload matrix

## Roadmap (Post-Alpha Hardening)

- continue stabilization of current backend/runtime (no scope expansion in alpha line)
- improve long-run fault-injection and recovery coverage
- improve benchmark reproducibility/reporting artifacts
- define stronger compatibility policy for protocol/storage format before beta

## Build and Test

Prerequisites:
- C++20 compiler
- CMake 3.20+
- optional OpenSSL for TLS (`chunks://`)

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Benchmark runs:

```bash
./build/chunkdb_bench --ops 20000
./build/chunkdb_server_bench --ops 5000 --port 4242
```

Release history:
- [CHANGELOG.md](CHANGELOG.md)
- [docs/releases/v0.1.1-alpha.md](docs/releases/v0.1.1-alpha.md)

## License

MIT. See [LICENSE](LICENSE).
