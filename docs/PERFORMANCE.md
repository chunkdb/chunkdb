# Performance and Benchmarking

## Benchmark Scope

`chunk` is benchmarked as a specialized chunk/grid storage engine.

It should not claim universal superiority over PostgreSQL or Redis.
Direct comparison is only meaningful for narrow workloads where:
- data is chunk/grid-oriented
- access patterns are point/chunk get/set
- block payload is fixed-width bitfield data
- durability assumptions are matched explicitly

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

- PostgreSQL/Redis performance directly
- replication/distributed behavior
- long-run production fault models
- full power-loss validation
- all multi-client saturation patterns

## Comparison Policy

Comparisons against PostgreSQL/Redis should:
1. define exact workload and schema mapping
2. state durability level used for each system
3. include complete reproducible commands/scripts
4. avoid claims outside tested scenario boundaries

No broad "faster than PostgreSQL/Redis" claim should be made without that narrow, reproducible evidence.
