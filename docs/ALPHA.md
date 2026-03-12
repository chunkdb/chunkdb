# chunk Alpha Milestone

Release stage: **Engineering Alpha**

## Purpose

This alpha release is focused on architecture stabilization and quality validation for a specialized chunk/grid storage engine.

## Included in Alpha

- `fs_split_v1` backend (directory-per-large-chunk, file-per-regular-chunk)
- delta WAL + checkpoint write path
- explicit durability modes (`relaxed`, `fsync-wal`, `fsync-checkpoint`)
- worker-pool server runtime with buffered parsing
- binary chunk transfer command (`CHUNKBIN`)
- cache size limit + eviction
- inter-process `data_dir` lock
- direct API and server-path benchmarks

## Not Included in Alpha

- additional storage backends
- broad cross-database performance claims
- distributed features (replication, consensus, sharding)
- full ACID transactional semantics

## Alpha Quality Focus

- correctness under concurrent access
- crash recovery behavior validation
- honest benchmark reporting with explicit scope
- clear documentation of guarantees and limitations
