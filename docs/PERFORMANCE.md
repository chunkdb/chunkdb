# Performance and Benchmarking

## Benchmark Scope

`chunkdb` benchmarks are designed to characterize behavior under the engine's own workload model:
- chunk/grid-oriented data layout
- point block updates/lookups
- full-chunk reads (text and binary)
- fixed-width bit-packed block payloads
- explicit durability mode context

This suite is intended for transparency and reproducibility of `chunkdb` behavior, not for broad winner/loser rankings across unrelated database categories.

## Benchmark Executables

1. Direct storage API benchmark: `chunkdb_bench`
2. End-to-end server-path benchmark: `chunkdb_server_bench`

## Direct Storage Benchmark (`chunkdb_bench`)

Scenarios:
- point writes
- point reads
- hot chunk writes
- chunk reads (text)
- chunk reads (binary)
- mixed read/write (70/30)
- sparse world writes
- dense world writes
- cold start reads
- warm cache reads

Run:

```bash
./build/chunkdb_bench --ops 20000
```

## Server-Path Benchmark (`chunkdb_server_bench`)

Scenarios:
- protocol `PING`
- protocol `INFO`
- protocol `SET`
- protocol `GET`
- protocol `CHUNK` (text payload)
- protocol `CHUNKBIN` (binary payload)
- protocol mixed read/write (70/30)

Run:

```bash
./build/chunkdb_server_bench --ops 5000 --port 4242
```

Logging level A/B run (same workload, same machine):

```bash
./build/chunkdb_server_bench --ops 5000 --port 4242 --log-level info
./build/chunkdb_server_bench --ops 5000 --port 4242 --log-level warn
```

## Measured Snapshot (Apple M1 Pro, 32 GB RAM)

These are real measured runs executed locally on:
- CPU: Apple M1 Pro
- RAM: 32 GB
- Date: 2026-03-13

Commands used:

```bash
./build/chunkdb_bench --ops 20000
./build/chunkdb_server_bench --ops 5000 --port 4242
```

Durability mode for both benchmark binaries in this snapshot: `relaxed`.

Benchmark config note:
- this snapshot uses `wal_group_commit_updates=8` inside benchmark store configs (relaxed-mode benchmark tuning).

Raw outputs (committed):
- baseline before optimization pass:
  - [bench/artifacts/manual-runs/direct-20260313-112616-before.txt](../bench/artifacts/manual-runs/direct-20260313-112616-before.txt)
  - [bench/artifacts/manual-runs/server-20260313-112616-before.txt](../bench/artifacts/manual-runs/server-20260313-112616-before.txt)
- after optimization pass:
  - [bench/artifacts/manual-runs/direct-20260313-112616-after.txt](../bench/artifacts/manual-runs/direct-20260313-112616-after.txt)
  - [bench/artifacts/manual-runs/server-20260313-112616-after.txt](../bench/artifacts/manual-runs/server-20260313-112616-after.txt)

### Direct Storage Path Results (After Optimization Pass)

| Scenario | Ops | Ops/s | p50 (us) | p95 (us) | p99 (us) | Durability | Cache state |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `point_writes` | 20000 | 198921.76 | 0.25 | 25.29 | 101.33 | `relaxed` | warm |
| `point_reads` | 20000 | 7670917.63 | 0.08 | 0.12 | 0.21 | `relaxed` | warm |
| `mixed_rw_70_30` | 20000 | 626261.49 | 0.12 | 0.38 | 56.67 | `relaxed` | warm |
| `chunk_reads_text` | 5000 | 108571.35 | 8.96 | 9.62 | 12.25 | `relaxed` | warm |
| `chunk_reads_binary` | 5000 | 10320812.12 | 0.08 | 0.08 | 0.08 | `relaxed` | warm |
| `cold_start_reads` | 5000 | 18387.81 | 0.21 | 284.29 | 341.58 | `relaxed` | cold-start |
| `warm_cache_reads` | 5000 | 8504601.84 | 0.08 | 0.17 | 0.21 | `relaxed` | warm-cache |

### Server-Path Results (After Optimization Pass)

| Scenario | Ops | Ops/s | p50 (us) | p95 (us) | p99 (us) | Durability | Cache state |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `protocol_set` (point write) | 5000 | 46102.25 | 17.04 | 42.42 | 111.46 | `relaxed` | warm |
| `protocol_get` (point read) | 5000 | 59744.27 | 17.08 | 23.92 | 42.46 | `relaxed` | warm |
| `protocol_mixed_70_30` | 5000 | 61482.47 | 11.04 | 22.21 | 116.58 | `relaxed` | warm |
| `protocol_chunk` | 1250 | 32995.46 | 28.71 | 43.58 | 65.50 | `relaxed` | warm |
| `protocol_chunkbin` | 1250 | 52684.08 | 18.08 | 27.25 | 42.38 | `relaxed` | warm |
| `protocol_ping` | 5000 | 45233.38 | 17.17 | 61.29 | 83.88 | `relaxed` | warm |
| `protocol_info` | 5000 | 51682.32 | 17.96 | 29.92 | 50.00 | `relaxed` | warm |

## Windows Native Snapshot (2026-03-15, Local Machine)

These results were imported from a real Windows-native run on:
- OS: Windows 10 (10.0.26200.7922)
- CPU: AMD Ryzen 5 4600H
- RAM: 16 GB
- Durability mode: `relaxed`
- Lock mode: `serial-mutex` (MinGW default safety path at that time)

Raw logs:
- direct: [bench/artifacts/manual-runs/direct-20260315-windows-native.txt](../bench/artifacts/manual-runs/direct-20260315-windows-native.txt)
- server: [bench/artifacts/manual-runs/server-20260315-windows-native.txt](../bench/artifacts/manual-runs/server-20260315-windows-native.txt)
- smoke tests: [bench/artifacts/manual-runs/smoke-20260315-windows-native.txt](../bench/artifacts/manual-runs/smoke-20260315-windows-native.txt)
- metadata/commands: [bench/artifacts/manual-runs/windows-native-20260315-metadata.txt](../bench/artifacts/manual-runs/windows-native-20260315-metadata.txt)

Key direct-path throughput (`chunkdb_bench --ops 20000`):
- `point_writes`: 15709.57 ops/s
- `point_reads`: 551511.14 ops/s
- `mixed_rw_70_30`: 32314.51 ops/s
- `chunk_reads_binary`: 1038421.60 ops/s
- `sparse_world_writes`: 276.59 ops/s

Key server-path throughput (`chunkdb_server_bench --ops 5000 --port 4242`):
- `protocol_ping`: 22080.71 ops/s
- `protocol_info`: 16878.04 ops/s
- `protocol_set`: 7894.57 ops/s
- `protocol_get`: 18543.66 ops/s
- `protocol_chunkbin`: 17855.61 ops/s

Important caveat in this local run:
- the server benchmark produced valid metrics for all measured scenarios, then failed during cleanup because `writer.lock` in the benchmark temp directory was still in use.

Stability hardening note for later runs:
- benchmark teardown was updated after this snapshot to fully destroy server/store objects before cleanup and to use bounded Windows sharing-violation retries for temp-dir removal.
- treat the 2026-03-15 numbers above as workload behavior data, and the cleanup failure as a benchmark harness cleanup issue that is now tracked and addressed separately.

## Logging Volume A/B (info vs warn, Server Path)

Date: 2026-03-15  
Machine: Apple M1 Pro, 32 GB RAM  
Durability: `relaxed`  
Benchmark command: `chunkdb_server_bench --ops 5000 --port 4242`  
Runs: 3 per mode, averaged.

Raw logs:
- [bench/artifacts/manual-runs/server-loglevel-20260315-info-run1.txt](../bench/artifacts/manual-runs/server-loglevel-20260315-info-run1.txt)
- [bench/artifacts/manual-runs/server-loglevel-20260315-info-run2.txt](../bench/artifacts/manual-runs/server-loglevel-20260315-info-run2.txt)
- [bench/artifacts/manual-runs/server-loglevel-20260315-info-run3.txt](../bench/artifacts/manual-runs/server-loglevel-20260315-info-run3.txt)
- [bench/artifacts/manual-runs/server-loglevel-20260315-warn-run1.txt](../bench/artifacts/manual-runs/server-loglevel-20260315-warn-run1.txt)
- [bench/artifacts/manual-runs/server-loglevel-20260315-warn-run2.txt](../bench/artifacts/manual-runs/server-loglevel-20260315-warn-run2.txt)
- [bench/artifacts/manual-runs/server-loglevel-20260315-warn-run3.txt](../bench/artifacts/manual-runs/server-loglevel-20260315-warn-run3.txt)

| Scenario | `info` avg ops/s | `warn` avg ops/s | `warn` vs `info` |
| --- | ---: | ---: | ---: |
| `protocol_ping` | 49089.00 | 55371.70 | +12.80% |
| `protocol_info` | 44513.80 | 45897.00 | +3.11% |
| `protocol_set` | 36683.20 | 41104.10 | +12.05% |
| `protocol_get` | 45935.80 | 49036.70 | +6.75% |
| `protocol_chunk` | 23266.10 | 24626.30 | +5.85% |
| `protocol_chunkbin` | 32367.90 | 35576.20 | +9.91% |
| `protocol_mixed_70_30` | 46953.50 | 49689.30 | +5.83% |

Interpretation:
- Hot-path INFO noise was reduced (no per-chunk replay INFO and no per-checkpoint begin/end INFO).
- `warn` mode is now the recommended throughput-focused runtime mode.
- `info` remains the default for bring-up observability and still provides startup/shutdown lifecycle visibility.
- No unacceptable regression was observed in the recommended (`warn`) mode in this A/B run.

## Windows Native Snapshot (2026-03-15, Post-Fix CI Run)

These results are from the successful Windows CI run after benchmark teardown hardening:
- run: `https://github.com/chunkdb/chunkdb/actions/runs/23108584332`
- job: `https://github.com/chunkdb/chunkdb/actions/runs/23108584332/job/67122047583`
- commit: `70023dd11c112834cf55cbae6a9f7b5e3cb16e39`
- host: `windows-latest` (`MSYS2 MinGW64`)
- durability mode: `relaxed`
- lock mode: `serial-mutex`

Raw logs:
- direct: [bench/artifacts/manual-runs/direct-20260315-windows-ci-70023dd-serial-mutex.txt](../bench/artifacts/manual-runs/direct-20260315-windows-ci-70023dd-serial-mutex.txt)
- server: [bench/artifacts/manual-runs/server-20260315-windows-ci-70023dd-serial-mutex.txt](../bench/artifacts/manual-runs/server-20260315-windows-ci-70023dd-serial-mutex.txt)
- metadata: [bench/artifacts/manual-runs/windows-ci-20260315-70023dd-metadata.txt](../bench/artifacts/manual-runs/windows-ci-20260315-70023dd-metadata.txt)

Key direct-path throughput (`chunkdb_bench --ops 20000`):
- `point_writes`: 71121.81 ops/s
- `point_reads`: 560503.56 ops/s
- `mixed_rw_70_30`: 171466.74 ops/s
- `chunk_reads_binary`: 1101564.22 ops/s
- `sparse_world_writes`: 445.12 ops/s

Key server-path throughput (`chunkdb_server_bench --ops 5000 --port 4242`):
- `protocol_ping`: 20605.39 ops/s
- `protocol_info`: 14363.00 ops/s
- `protocol_set`: 10699.16 ops/s
- `protocol_get`: 16374.17 ops/s

Cleanup status in this post-fix run:
- benchmark step succeeded and no temp-dir cleanup failure line was emitted.

Lock-mode experiment status:
- Windows shared-lock mode (`CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS=OFF`) has not yet been validated in a stable CI benchmark run.

## Optimization Delta (Before -> After, Same Machine / Commands)

Selected scenarios from the paired run:

| Scenario | Before Ops/s | After Ops/s | Change |
| --- | ---: | ---: | ---: |
| direct `point_writes` | 37580.94 | 198921.76 | +429.3% |
| direct `hot_chunk_writes` | 2614720.88 | 6962779.76 | +166.3% |
| direct `mixed_rw_70_30` | 125310.34 | 626261.49 | +399.8% |
| server `protocol_set` | 11707.75 | 46102.25 | +293.8% |
| server `protocol_mixed_70_30` | 34059.47 | 61482.47 | +80.5% |
| server `protocol_chunkbin` | 46083.23 | 52684.08 | +14.3% |

Interpretation notes:
- largest gains are on write-heavy paths (`SET`, point writes, hot writes), consistent with WAL batching + write-path allocation reductions.
- `CHUNKBIN` remains materially more efficient than text chunk transfer.
- `PING` is within same order of magnitude but showed run-to-run variance in this pair.

## Sparse Write Investigation (Same Machine)

Command used for both runs:

```bash
./build/chunkdb_bench --ops 20000
```

Measured sparse scenario (`relaxed` durability):

- before (baseline): `sparse_world_writes ... ops_s=256.33 p50_us=2601.25 p95_us=10394.12 p99_us=11360.17`
- after the latest write-path update: `sparse_world_writes ... ops_s=625.79 p50_us=12.46 p95_us=7649.21 p99_us=9221.96`
- improvement on this machine: `~2.44x` (`625.79 / 256.33`)

Additional sparse-path counters emitted by `chunkdb_bench` after the latest write-path update:

- `sparse_metrics evictions=4639 checkpoints=0 wal_batch_flushes=2708 unique_loaded_chunks=19999`

Interpretation:
- dominant cost in sparse writes remains eviction + WAL flush pressure under high chunk churn.
- the latest update reduced this cost without changing protocol semantics or durability semantics.

## Key Behavior Observed in This Run

- Direct warm-cache reads remain extremely fast.
- `chunk_reads_binary`/`CHUNKBIN` path is much more efficient than text chunk reads.
- Server-path results include expected protocol/socket overhead versus direct in-process access.
- Writes are still slower than direct point reads, but write-path throughput improved significantly in this optimization pass.
- Cold-start tail latency (`p95`/`p99`) remains significantly higher than warm-cache latency.

## Reproducible Benchmark Artifacts

Use the reproducibility script to generate a benchmark bundle with:
- exact git revision
- build/test logs
- host metadata
- benchmark outputs

Run locally:

```bash
scripts/bench/run_reproducible_benchmarks.sh
```

Artifact layout is documented in:
- [bench/artifacts/README.md](../bench/artifacts/README.md)

GitHub workflow support:
- `.github/workflows/benchmark-artifacts.yml`
- triggers:
  - manual (`workflow_dispatch`)
  - automatically on release publish

The workflow uploads a downloadable artifact bundle for verification/review.

## What These Benchmarks Measure

- request/operation latency and throughput for implemented chunk workloads
- relative cost differences between text and binary chunk transfer
- cold-vs-warm behavior (direct API suite)
- runtime overhead for full TCP/protocol path (server-path suite)
- server command latency percentiles (p50/p95/p99) for each measured scenario

## What These Benchmarks Do Not Measure

- replication/distributed behavior
- full transactional semantics across chunks
- full power-loss validation across all filesystems/devices
- all multi-client saturation patterns
- every production fault model

## Optional Cross-System Study Policy

If a cross-system comparison is published (for example against PostgreSQL/Redis), it should:
1. define exact schema/workload mapping
2. match durability assumptions explicitly
3. include complete reproducible commands/scripts
4. keep conclusions scoped to tested scenarios only

Absent those controls, benchmark output should be treated as `chunkdb`-only behavior characterization.
