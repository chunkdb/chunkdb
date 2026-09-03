# chunk

[![CI](https://github.com/chunkdb/chunkdb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/chunkdb/chunkdb/actions/workflows/ci.yml)

`chunkdb` is a specialized chunk/grid storage engine for games and grid-based simulations with bit-packed block payloads.

Current public release: **[`v1.3.0`](https://github.com/chunkdb/chunkdb/releases/tag/v1.3.0)** (stable).

## Project Identity

- **Specialized chunk/grid engine**: optimized for chunk-oriented worlds, not a general-purpose multi-model platform.
- **Chunk-native protocol**: text command protocol with optional binary chunk transfer for high-volume reads.
- **Bit-packed storage model**: configurable `block_bits` and chunk geometry for compact world-state representation.
- **Chunk-oriented access model**: efficient point block `GET`/`SET`/`EXISTS`/`UNSET` plus chunk-level `CHUNKEXISTS`/`CHUNKSET`/`CHUNK`/`CHUNKBIN`, with opt-in exact `STATE` forms for per-block presence round-trips.
- **WAL/checkpoint durability modes**: explicit behavior trade-offs (`relaxed`, `fsync-wal`, `fsync-checkpoint`).

## Stability Status

- Status: **Stable** (`v1.3.0`).
- The stable channel commits to the surfaces in [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md):
  on-disk `fs_split_v1` format readability, the wire protocol, the durability
  contract, and the CLI — under [Semantic Versioning](https://semver.org/).
- Focus continues on correctness, durability behavior, runtime scalability, and
  transparent benchmarks.

## Release Channels

- Current public channel: **Stable** [`v1.3.0`](https://github.com/chunkdb/chunkdb/releases/tag/v1.3.0)
- Compatibility & versioning policy: [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)
- `.sha256` files are included so users can verify downloaded artifact integrity; see [docs/VERIFY_RELEASE.md](docs/VERIFY_RELEASE.md)
- Release channel policy is documented in [docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md)

Current platform support summary (stable claims):

- Linux native: supported
- macOS native: supported
- Windows native core non-TLS path: supported
- Windows native TLS (MSYS2 MinGW64 with MSYS2 OpenSSL): supported; MSVC and other OpenSSL builds untested
- `fs_region_v1` backend: experimental, not covered by stability guarantees

## Included in v1.0.0

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

## Out of Scope for v1.0.0

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

Token-in-URI and `--token` are development-only forms because tokens can appear
in shell history, process listings, and logs. For deployments, prefer
`--token-file <path>` or `CHUNKDB_TOKEN`. Server token source priority is
`--no-auth`, `--token-file`, `CHUNKDB_TOKEN`, `--token`, then URI token.

Create a local token file for the startup examples:

```bash
printf 'chunk-token\n' > ./chunkdb.token
```

Server startup quick-start (same geometry/cache, different durability):

```bash
# dev / fastest acknowledgment path
./build/chunkdb_server \
  --listen-uri chunk://127.0.0.1:4242/ \
  --token-file ./chunkdb.token \
  --data-dir ./data \
  --durability relaxed \
  --log-level info \
  --workers 4

# safer WAL durability
./build/chunkdb_server \
  --listen-uri chunk://127.0.0.1:4242/ \
  --token-file ./chunkdb.token \
  --data-dir ./data \
  --durability fsync-wal \
  --log-level info \
  --workers 4

# strict-ish checkpoint sync behavior
./build/chunkdb_server \
  --listen-uri chunk://127.0.0.1:4242/ \
  --token-file ./chunkdb.token \
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

World-oriented and operational commands:

```text
CHUNKSCAN 100                 # enumerate populated chunks (paginated, cursor-based)
CHUNKRANGE -2 -2 2 2          # bounded rectangular multi-chunk read (max 256 chunks)
CHUNKRADIUS 0 0 2             # bounded radius multi-chunk read (disc, max 256 chunks)
CHUNKVER 0 0                  # opaque chunk version token
CHUNKCAS 0 0 <version> STATE <payload_bits>|<presence_bits>   # conditional replace
CHUNKBATCH 0 0 - SET 1 1 <bits> UNSET 2 2                     # atomic single-chunk batch
CHUNKBINC 0 0 STATE           # zrle-compressed binary chunk transfer
CHUNKSETBIN 0 0 STATE <len>   # binary chunk write: <len> raw bytes + empty line follow
WALFLUSH                      # explicit global durability barrier
METRICS                       # Prometheus text-format runtime metrics
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
- timestamp: ISO 8601 UTC with milliseconds

Startup sample:

```text
2026-03-15T18:30:12.123Z INFO server pid=1234 ready to accept connections protocol=tcp host=127.0.0.1 port=4242 tls=off workers=4
```

Warning sample:

```text
2026-03-15T18:31:03.771Z WARN server pid=1234 bad request disconnect reason="request line exceeds max_line_bytes"
```

Log filtering examples:

```bash
# default: INFO/WARN/ERROR
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --log-level info

# WARN/ERROR only
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --log-level warn

# ERROR only
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --log-level error
```

Operational note:
- use `--log-level warn` for throughput-focused deployments;
- keep `info` (default) for startup/bring-up visibility.

## Benchmarking

Benchmarks are scoped to chunk/grid workloads and reported through:

- protocol benchmark path (primary): `chunkdb_server_bench`
- direct storage benchmark path (internal): `chunkdb_bench`

They characterize this engine under its chunk-oriented workload model:
- operation latency/throughput for implemented point/chunk commands
- text vs binary chunk transfer behavior
- cold/warm path behavior and server runtime overhead

Benchmark results are intentionally workload-scoped and should not be treated as global rankings across unrelated database categories.

First run (protocol path):

```bash
./build/chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --token-file ./chunkdb.token --data-dir ./data --durability relaxed --workers 4
./build/chunkdb_server_bench --uri chunk://chunk-token@127.0.0.1:4242/ --tests ping,set,get --requests 5000
```

The benchmark URI above is for local development benchmarking.

Full benchmark commands, reproducible artifact format, historical snapshots, and
experimental layout A/B notes are documented in [docs/PERFORMANCE.md](docs/PERFORMANCE.md)
and [docs/PERFORMANCE_LAYOUT_AB.md](docs/PERFORMANCE_LAYOUT_AB.md).

## Durability Guarantees Matrix

| Mode | Write Acknowledgement Path | Checkpoint Replace Path | Power-Loss Risk | Not Guaranteed |
| --- | --- | --- | --- | --- |
| `relaxed` | `SET` returns after WAL append path without `fsync` (and may batch WAL flushes by `wal_group_commit_updates`) | Temp-write + atomic replace in namespace; no required temp-file/data sync and no required directory sync | Highest risk of losing recent acknowledged writes on crash/power loss | No cross-chunk atomicity, no replication, no full ACID semantics |
| `fsync-wal` | `SET` returns after WAL append and WAL `fsync` | Checkpoint file and directory are synced before removing the durable WAL they replace | Lower risk for acknowledged writes, but still depends on OS/filesystem/device honoring sync operations | No cross-chunk atomicity, no replication, no full ACID semantics |
| `fsync-checkpoint` | `fsync-wal` path + checkpoint image/directory sync on checkpoint | write temp -> flush temp file data -> close(check) -> atomic replace -> sync parent directory | Strongest mode in current engine, still not equivalent to full transactional DB guarantees | No cross-chunk atomicity, no replication, no full ACID semantics |

This matrix summarizes current behavior for the stable architecture (fs_split_v1).
Atomic replace describes path-level old-or-new namespace behavior; it is not by itself a guarantee of durability after power loss without the mode-required flush/sync steps.
On Windows, a required directory-sync capability failure in a synced mode fails
the operation closed rather than weakening the selected durability contract.

## Reproducible Benchmark Artifacts

Generate a reproducible benchmark bundle locally with:

```bash
scripts/bench/run_reproducible_benchmarks.sh
```

Bundle format and required files are documented in [bench/artifacts/README.md](bench/artifacts/README.md).

## Integrity Verification

`chunkdb_verify` checks a data directory offline without modifying it:

```bash
./build/chunkdb_verify --data-dir ./data
```

It validates chunk/WAL/region file names, headers, geometry, checksums, version
and snapshot-generation bookkeeping, conditional intents, and WAL replay validity. Valid stable-v1 and
intermediate clock layouts are identified as migratable; initialized-marker
damage remains an error. The tool reports temporary artifacts, prints one machine-usable
`VERIFY <level> <code> <path>` line per finding plus a `SUMMARY` line, and
exits `0` (clean), `1` (findings), or `2` (fatal). Pass the same geometry
flags as the server when using a non-default geometry.

## Current Limitations

- only one storage backend is included (`fs_split_v1`)
- no multi-chunk atomic transaction model (single-chunk `CHUNKCAS`/`CHUNKBATCH` only)
- multi-process mode is SWMR only (single writer, read-only readers); shared multi-writer is unsupported
- durability guarantees are mode-dependent and below full ACID DB guarantees
- benchmark suite is focused and not a full production workload matrix
- Windows native TLS is validated only for the MSYS2 MinGW64 toolchain

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
- Windows Native TLS (MSYS2 MinGW64 + MSYS2 OpenSSL): Supported

## Windows (Native, no Docker)

Use **MSYS2 MinGW64** shell (not PowerShell/cmd) and follow:

- [docs/WINDOWS_NATIVE.md](docs/WINDOWS_NATIVE.md)

The guide includes linear copy-paste steps for package install, build, smoke tests, and server startup.

Build and run smoke tests:

```bash
cmake -S . -B build -DCHUNKDB_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build -L smoke --output-on-failure
```

Contributor test gates are documented in [CONTRIBUTING.md](CONTRIBUTING.md).

## Run with Docker

```bash
docker build -t chunkdb:local .
docker compose up -d
docker compose logs -f chunkdb
docker compose down -v
```

For full Docker and Docker Compose instructions (including test profile and buildx), see [docs/DOCKER.md](docs/DOCKER.md).

Release history is documented in [CHANGELOG.md](CHANGELOG.md).

## License

MIT. See [LICENSE](LICENSE).
