# chunk

[![CI](https://github.com/chunkdb/chunkdb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/chunkdb/chunkdb/actions/workflows/ci.yml)

`chunkdb` is a specialized chunk/grid storage engine for games and grid-based simulations with bit-packed block payloads.

Current public release: **[`v0.1.1-preview`](https://github.com/chunkdb/chunkdb/releases/tag/v0.1.1-preview)**.

## Project Identity

- **Specialized chunk/grid engine**: optimized for chunk-oriented worlds, not a general-purpose multi-model platform.
- **Chunk-native protocol**: text command protocol with optional binary chunk transfer for high-volume reads.
- **Bit-packed storage model**: configurable `block_bits` and chunk geometry for compact world-state representation.
- **Chunk-oriented access model**: efficient point block `GET`/`SET`/`EXISTS`/`UNSET` plus chunk-level `CHUNKEXISTS`/`CHUNKSET`/`CHUNK`/`CHUNKBIN`, with opt-in exact `STATE` forms for per-block presence round-trips.
- **WAL/checkpoint durability modes**: explicit behavior trade-offs (`relaxed`, `fsync-wal`, `fsync-checkpoint`).

## Stability Status

- Status: Engineering alpha.
- Focus: correctness, durability behavior, runtime scalability, and transparent benchmarks.
- Current status:
  - core storage/runtime path is implemented and tested
  - format/protocol are versioned but still alpha-level
  - production hardening is incomplete

See [docs/ALPHA.md](docs/ALPHA.md) for alpha boundaries.

## Release Channels

- Current public channel: [Release Preview `v0.1.1-preview`](https://github.com/chunkdb/chunkdb/releases/tag/v0.1.1-preview)
- Stable release status: no stable release has been published yet
- `Pre-release` means the build is available for evaluation and integration testing, but should not be treated as a stable compatibility promise
- `.sha256` files are included so users can verify downloaded artifact integrity; see [docs/VERIFY_RELEASE.md](docs/VERIFY_RELEASE.md)
- Release channel policy and stable-release conditions are documented in [docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md)

Terminology mapping:
- **preview** = release channel label on GitHub Releases (`v0.1.1-preview`)
- **engineering alpha** = current maturity level of the implementation

Which release should I use?

- Want evaluation or integration testing now: use the current preview release
- Want the most conservative compatibility/support expectations: wait for the first stable release

Current platform support summary:

- Linux native: supported in the current preview
- macOS native: supported in the current preview
- Windows native core non-TLS path: supported in the current preview
- Windows Native TLS: not yet guaranteed as fully supported and not part of stable support claims yet

## Included in the current preview line

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

## Out of Scope for the current preview line

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
- Per-block presence bitmap:
  - distinguishes `unset` from an explicit all-zero bit payload
  - `GET` still returns zero bits for unset blocks; use `EXISTS` to distinguish
- Per-chunk presence semantics:
  - a chunk is considered explicitly present when any block presence bit is set
  - `CHUNK` still returns zero bits for an absent chunk; use `CHUNKEXISTS` to distinguish
  - `CHUNK ... STATE` / `CHUNKBIN ... STATE` expose the exact per-block presence bitmap
- Disk layout (`fs_split_v1`):
  - `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.chk`
  - `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.wal`

Reference docs:
- [docs/STORAGE_FORMAT.md](docs/STORAGE_FORMAT.md)
- [docs/BACKENDS.md](docs/BACKENDS.md)
- [docs/CONCURRENCY.md](docs/CONCURRENCY.md)
- [docs/RUNTIME_FLOW.md](docs/RUNTIME_FLOW.md)
- [docs/DURABILITY_CONTRACT.md](docs/DURABILITY_CONTRACT.md)
- [docs/KNOWN_LIMITATIONS.md](docs/KNOWN_LIMITATIONS.md)
- [docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md)
- [docs/VERIFY_RELEASE.md](docs/VERIFY_RELEASE.md)
- [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md)

## Protocol, Startup, and Connection Examples

Default URI forms:
- insecure: `chunk://chunk-token@127.0.0.1:4242/`
- TLS: `chunks://chunk-token@127.0.0.1:4242/`

Server startup quick-start (same geometry/cache, different durability):

```bash
# dev / fastest acknowledgment path
./build/chunkdb_server \
  --listen-uri chunk://chunk-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability relaxed \
  --log-level info \
  --workers 4

# safer WAL durability
./build/chunkdb_server \
  --listen-uri chunk://chunk-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability fsync-wal \
  --log-level info \
  --workers 4

# strict-ish checkpoint sync behavior
./build/chunkdb_server \
  --listen-uri chunk://chunk-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability fsync-checkpoint \
  --log-level info \
  --workers 4 \
  --checkpoint-updates 512 \
  --checkpoint-wal-bytes 1048576 \
  --wal-group-commit-updates 8 \
  --max-loaded-chunks 65536
```

Quick protocol session example:

```text
AUTH chunk-token
EXISTS 0 0
SET 0 0 1111000011110000
GET 0 0
UNSET 0 0
CHUNKEXISTS 0 0
CHUNKSET 0 0 <full_chunk_bits>
CHUNK 0 0
CHUNK 0 0 STATE
CHUNKSET 0 0 STATE <payload_bits>|<presence_bits>
CHUNKBIN 0 0
CHUNKBIN 0 0 STATE
INFO
QUIT
```

Protocol note:
- `GET x y` returns the configured zero-bit payload when a block is unset.
- `EXISTS x y` is the explicit way to distinguish `unset` from `SET x y 000...0`.
- `CHUNK x y` returns the configured zero-bit chunk payload when a chunk is absent.
- `CHUNKEXISTS cx cy` is the explicit way to distinguish an absent chunk from `CHUNKSET cx cy 000...0`.
- `CHUNK cx cy STATE` returns `<payload_bits>|<presence_bits>` for exact per-block presence.
- `CHUNKBIN cx cy STATE` returns `[payload_bytes][presence_bytes]`.
- `CHUNKSET cx cy STATE ...` writes mixed present/absent block state in one operation.

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
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --log-level info

# WARN/ERROR only
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --log-level warn

# ERROR only
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --log-level error
```

Operational note:
- use `--log-level warn` for throughput-focused deployments;
- keep `info` (default) for startup/bring-up visibility.

## Benchmark Scope

Benchmarks are scoped to chunk/grid workloads and reported as:
- protocol benchmark path (primary): `chunkdb_server_bench`
- direct storage benchmark path (internal): `chunkdb_bench`

They characterize this engine under its chunk-oriented workload model:
- operation latency/throughput for implemented point/chunk commands
- text vs binary chunk transfer behavior
- cold/warm path behavior and server runtime overhead

Benchmark results are intentionally workload-scoped and should not be treated as global rankings across unrelated database categories.

## Benchmark Quick Start

Binaries:
- `./build/chunkdb_server_bench` (protocol benchmark, primary)
- `./build/chunkdb_bench` (direct storage benchmark, internal)

Flag discovery:

```bash
./build/chunkdb_server_bench --help
./build/chunkdb_bench --help
```

First run (protocol path):

```bash
./build/chunkdb_server --listen-uri chunk://chunk-token@127.0.0.1:4242/ --data-dir ./data --durability relaxed --workers 4
./build/chunkdb_server_bench --uri chunk://chunk-token@127.0.0.1:4242/ --tests ping,set,get --requests 5000
```

Common benchmark commands:

```bash
# protocol benchmark against external server (primary path)
./build/chunkdb_server_bench \
  --uri chunk://chunk-token@127.0.0.1:4242/ \
  --tests ping,info,set,get,chunk,chunkbin,mixed \
  --requests 5000 --clients 50 --pipeline 1 --keyspace 512 --seed 1337

# sparse low-cache write pressure
./build/chunkdb_server_bench \
  --uri chunk://chunk-token@127.0.0.1:4242/ \
  --tests set \
  --requests 20000 --clients 50 --pipeline 1 --keyspace 200000

# JSON output for artifact capture
./build/chunkdb_server_bench \
  --uri chunk://chunk-token@127.0.0.1:4242/ \
  --tests set,get,mixed \
  --requests 5000 --output json > bench-server.json

# internal direct storage benchmark
./build/chunkdb_bench --ops 20000
```

See [docs/PERFORMANCE.md](docs/PERFORMANCE.md).
Historical benchmark snapshots that used legacy command syntax are grouped under
[Historical snapshots (legacy command syntax)](docs/PERFORMANCE.md#historical-snapshots-legacy-command-syntax).

Experimental layout A/B decision benchmarking is documented in:
- [docs/PERFORMANCE_LAYOUT_AB.md](docs/PERFORMANCE_LAYOUT_AB.md)
- entrypoint: `scripts/bench/layout_ab.sh`
- latest committed snapshot: `docs/benchmarks/layout_ab/2026-03-14-darwin/`

Latest committed sparse 5x snapshot (2026-03-19, `relaxed` mode) is published in [docs/PERFORMANCE.md#latest-committed-benchmark-snapshot-2026-03-19](docs/PERFORMANCE.md#latest-committed-benchmark-snapshot-2026-03-19).

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

Entry-point validation behavior:
- this script runs smoke tests only (`ctest -L smoke --output-on-failure`) before benchmark commands;
- full/stress/crash suites are intentionally left to dedicated test workflows/scripts.

Bundle format and required files:
- [bench/artifacts/README.md](bench/artifacts/README.md)

GitHub automation:
- `.github/workflows/benchmark-artifacts.yml`
- supports manual runs and release-triggered artifact generation

## Current Limitations

- only one storage backend is included (`fs_split_v1`)
- no multi-chunk atomic transaction model
- multi-process mode is SWMR only (single writer, read-only readers); shared multi-writer is unsupported
- durability guarantees are mode-dependent and below full ACID DB guarantees
- benchmark suite is focused and not a full production workload matrix
- Windows Native TLS is not yet guaranteed as fully supported

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

`fs_region_v1` remains experimental-only in this release and is not production-supported.

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

Benchmark command reference:
- quick entrypoint: [Benchmark Quick Start](#benchmark-quick-start)
- full benchmark docs: [docs/PERFORMANCE.md](docs/PERFORMANCE.md)

Release archive packaging + checksums:

```bash
cmake -S . -B build-release -DCHUNKDB_BUILD_TESTS=OFF -DCHUNKDB_WITH_TLS=OFF
cmake --build build-release --parallel
cpack --config build-release/CPackConfig.cmake -B build-release/packages
scripts/release/generate_checksums.sh build-release/packages
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
- [docs/releases/release-preview.md](docs/releases/release-preview.md)
- [CHANGELOG.md](CHANGELOG.md)
- [docs/releases/v0.1.1-alpha.md](docs/releases/v0.1.1-alpha.md)

## License

MIT. See [LICENSE](LICENSE).
