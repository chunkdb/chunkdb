# Performance and Benchmarking

## Scope

`chunk` is benchmarked as a specialized chunk/block storage engine.

It should not claim universal superiority over PostgreSQL or Redis.
Direct comparison is only meaningful for narrow workloads where:
- data is chunk/grid-oriented
- access patterns are point/chunk get/set
- block payload is fixed-width bitfield data

## Benchmark Suite

Executable: `chunkdb_bench`

Current scenarios:
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

## Reading Results Correctly

Interpret numbers with these caveats:
- default benchmark uses direct storage API (not full network path)
- hardware, filesystem, and OS cache heavily affect outcomes
- durability mode changes write latency significantly
- text `CHUNK` and binary `CHUNKBIN` are intentionally different transfer costs

## Comparison Policy

Comparisons against PostgreSQL/Redis should:
1. define exact workload and schema mapping
2. state durability level used for each system
3. include full command/scripts for reproducibility
4. avoid claims outside tested scenario boundaries

No broad "faster than PostgreSQL/Redis" claim should be made without that evidence.
