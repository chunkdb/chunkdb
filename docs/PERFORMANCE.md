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

Raw outputs (committed):
- [bench/artifacts/manual-runs/direct-20260313-080002.txt](../bench/artifacts/manual-runs/direct-20260313-080002.txt)
- [bench/artifacts/manual-runs/server-20260313-080002.txt](../bench/artifacts/manual-runs/server-20260313-080002.txt)

### Direct Storage Path Results

| Scenario | Ops | Ops/s | p50 (us) | p95 (us) | p99 (us) | Durability | Cache state |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `point_writes` | 20000 | 37692.89 | 30.21 | 103.88 | 150.96 | `relaxed` | warm |
| `point_reads` | 20000 | 8066412.39 | 0.08 | 0.17 | 0.25 | `relaxed` | warm |
| `mixed_rw_70_30` | 20000 | 117193.88 | 0.29 | 45.12 | 54.00 | `relaxed` | warm |
| `chunk_reads_text` | 5000 | 108308.33 | 9.00 | 9.62 | 11.96 | `relaxed` | warm |
| `chunk_reads_binary` | 5000 | 9857072.45 | 0.08 | 0.08 | 0.08 | `relaxed` | warm |
| `cold_start_reads` | 5000 | 18191.74 | 0.21 | 286.08 | 352.46 | `relaxed` | cold-start |
| `warm_cache_reads` | 5000 | 7533910.13 | 0.08 | 0.17 | 0.21 | `relaxed` | warm-cache |

### Server-Path Results

| Scenario | Ops | Ops/s | p50 (us) | p95 (us) | p99 (us) | Durability | Cache state |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `protocol_set` (point write) | 5000 | 11222.66 | 63.54 | 192.96 | 268.79 | `relaxed` | warm |
| `protocol_get` (point read) | 5000 | 55348.03 | 17.62 | 28.21 | 48.12 | `relaxed` | warm |
| `protocol_mixed_70_30` | 5000 | 36514.52 | 13.12 | 64.79 | 82.92 | `relaxed` | warm |
| `protocol_chunk` | 1250 | 31606.50 | 29.21 | 49.96 | 70.83 | `relaxed` | warm |
| `protocol_chunkbin` | 1250 | 50822.48 | 18.33 | 29.75 | 50.71 | `relaxed` | warm |

### Key Behavior Observed in This Run

- Direct warm-cache reads are extremely fast.
- `chunk_reads_binary`/`CHUNKBIN` path is much more efficient than text chunk reads.
- Server-path results include expected protocol/socket overhead versus direct in-process access.
- Writes are slower than reads in both direct and server-path runs.
- Cold-start tail latency (`p95`/`p99`) is significantly higher than warm-cache latency.

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
