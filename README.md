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

- Status: Engineering alpha.
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
- [docs/RUNTIME_FLOW.md](docs/RUNTIME_FLOW.md)

## Protocol, Startup, and Connection Examples

Default URI forms:
- insecure: `chunk://token@127.0.0.1:4242/`
- TLS: `chunks://token@127.0.0.1:4242/`

Server startup quick-start (same geometry/cache, different durability):

```bash
# dev / fastest acknowledgment path
./build/chunkdb_server \
  --listen-uri chunk://dev-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability relaxed \
  --log-level info \
  --workers 4

# safer WAL durability
./build/chunkdb_server \
  --listen-uri chunk://dev-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability fsync-wal \
  --log-level info \
  --workers 4

# strict-ish checkpoint sync behavior
./build/chunkdb_server \
  --listen-uri chunk://dev-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability fsync-checkpoint \
  --log-level info \
  --workers 4 \
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
- [docs/SERVER_FLAGS.md](docs/SERVER_FLAGS.md)
- [docs/RUNTIME_FLOW.md](docs/RUNTIME_FLOW.md)

## Lifecycle Logging

`chunkdb_server` uses concise machine-parseable lifecycle lines:

```text
<timestamp> <level> <component> pid=<pid> <message> <k=v ...>
```

- levels: `INFO`, `WARN`, `ERROR`
- components: `server`, `store`, `lock`, `recovery`
- timestamp: local server time with milliseconds

Startup sample:

```text
2026-03-15 18:30:12.123 INFO server pid=1234 ready to accept connections protocol=tcp host=127.0.0.1 port=4242 tls=off workers=4
```

Warning sample:

```text
2026-03-15 18:31:03.771 WARN server pid=1234 bad request disconnect reason="request line exceeds max_line_bytes"
```

Log filtering examples:

```bash
# default: INFO/WARN/ERROR
./build/chunkdb_server --listen-uri chunk://token@127.0.0.1:4242/ --log-level info

# WARN/ERROR only
./build/chunkdb_server --listen-uri chunk://token@127.0.0.1:4242/ --log-level warn

# ERROR only
./build/chunkdb_server --listen-uri chunk://token@127.0.0.1:4242/ --log-level error
```

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

Experimental layout A/B decision benchmarking is documented in:
- [docs/PERFORMANCE_LAYOUT_AB.md](docs/PERFORMANCE_LAYOUT_AB.md)
- entrypoint: `scripts/bench/layout_ab.sh`
- latest committed snapshot: `docs/benchmarks/layout_ab/2026-03-14-darwin/`

Latest measured snapshot (Apple M1 Pro, 32 GB RAM, `relaxed` mode) is published in [docs/PERFORMANCE.md#measured-snapshot-apple-m1-pro-32-gb-ram](docs/PERFORMANCE.md#measured-snapshot-apple-m1-pro-32-gb-ram).

## Durability Guarantees Matrix

| Mode | Write Acknowledgement Path | Checkpoint Replace Path | Power-Loss Risk | Not Guaranteed |
| --- | --- | --- | --- | --- |
| `relaxed` | `SET` returns after WAL append path without `fsync` (and may batch WAL flushes by `wal_group_commit_updates`) | Temp-write + atomic replace in namespace; no required temp-file/data sync and no required directory sync | Highest risk of losing recent acknowledged writes on crash/power loss | No cross-chunk atomicity, no replication, no full ACID semantics |
| `fsync-wal` | `SET` returns after WAL append and WAL `fsync` | Same checkpoint replace mechanics as `relaxed`; checkpoint image durability still does not require checkpoint file/directory sync | Lower risk for acknowledged writes, but still depends on OS/filesystem/device honoring `fsync` | No cross-chunk atomicity, no replication, no full ACID semantics |
| `fsync-checkpoint` | `fsync-wal` path + checkpoint image/directory sync on checkpoint | write temp -> flush temp file data -> close(check) -> atomic replace -> sync parent directory | Strongest mode in current engine, still not equivalent to full transactional DB guarantees | No cross-chunk atomicity, no replication, no full ACID semantics |

This matrix summarizes current behavior only for the implemented alpha architecture.
Atomic replace describes path-level old-or-new namespace behavior; it is not by itself a guarantee of durability after power loss without the mode-required flush/sync steps.
On some Windows runtime/filesystem combinations, directory-handle flush may be capability-limited and is treated as best-effort.

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

## How to Report Issues

Use the issue chooser with lightweight templates:
- bug report (`[BUG] ...`)
- feature request (`[FEATURE] ...`)

The templates are intentionally short so contributors can open a useful issue quickly.
For performance regressions, use the bug template and include benchmark command/output in the description.

Issue intake and triage policy:
- [docs/ISSUE_POLICY.md](docs/ISSUE_POLICY.md)

## Build and Test

Prerequisites:
- C++20 compiler
- CMake 3.20+
- optional OpenSSL for TLS (`chunks://`)

## Support Matrix

- Windows Native (MSYS2): Supported (core path)
- Windows Docker: Supported (recommended quick start)
- Windows Native TLS: Not yet guaranteed (follow-up validation)

## Windows (Native, no Docker)

Use **MSYS2 MinGW64** shell (not PowerShell/cmd) and follow:

- [docs/WINDOWS_NATIVE.md](docs/WINDOWS_NATIVE.md)

The guide includes linear copy-paste steps for package install, build, smoke tests, and server startup.

Fast local gate (smoke):

```bash
scripts/test/quick.sh
```

These default gates intentionally keep the experimental layout path OFF
(`-DCHUNKDB_BUILD_EXPERIMENTAL_LAYOUT=OFF`) and validate the production alpha path (`fs_split_v1`).

Full local gate (smoke + stress):

```bash
scripts/test/full.sh
```

Opt-in experimental layout checks:

```bash
cmake -S . -B build-exp \
  -DCHUNKDB_BUILD_TESTS=ON \
  -DCHUNKDB_BUILD_EXPERIMENTAL_LAYOUT=ON \
  -DCHUNKDB_WITH_TLS=OFF
cmake --build build-exp --parallel
ctest --test-dir build-exp -L experimental --output-on-failure
REPEATS=1 OPS_LIST='20000' SCENARIOS='sparse_world_writes' DURABILITIES='relaxed' \
  scripts/bench/layout_ab.sh
```

Targeted crash-hardening suite (separate from quick/full gates):

```bash
cmake --build build-full --target chunkdb_durability_crash_hardening_test
ctest --test-dir build-full -L crash --output-on-failure
```

Manual CMake/CTest flow remains available:

```bash
cmake -S . -B build -DCHUNKDB_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build -L smoke --output-on-failure
ctest --test-dir build -L stress --output-on-failure
```

Benchmark runs:

```bash
./build/chunkdb_bench --ops 20000
./build/chunkdb_server_bench --ops 5000 --port 4242
```

## Run with Docker

```bash
docker build -t chunkdb:local .
docker compose up -d
docker compose logs -f chunkdb
docker compose down -v
```

For full Docker and Docker Compose instructions (including test profile and buildx), see [docs/DOCKER.md](docs/DOCKER.md).
For host-vs-docker benchmark comparison on the same machine, run `scripts/bench/host_vs_docker.sh`.

Release history:
- [CHANGELOG.md](CHANGELOG.md)
- [docs/releases/v0.1.1-alpha.md](docs/releases/v0.1.1-alpha.md)

## License

MIT. See [LICENSE](LICENSE).
