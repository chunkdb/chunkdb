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
- this snapshot uses `wal_group_commit_updates=8` inside benchmark store configs (optimization-stage tuning for relaxed mode).

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
