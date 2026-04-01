# chunk `v0.1.1-preview` (engineering alpha line)

Release status: **Engineering Alpha**

Release naming note:
- GitHub release channel tag: `v0.1.1-preview`
- Maturity label: engineering alpha

## Stability Status

- Alpha means the architecture is functional and test-covered, but not production-hardened.
- APIs/protocol/storage format are versioned but may still change between alpha releases.
- The project is positioned as a specialized chunk/grid storage engine.

## Included in `v0.1.1-preview` (engineering alpha line)

- `fs_split_v1` backend (directory-per-large-chunk, file-per-regular-chunk)
- configurable chunk geometry and `block_bits`
- delta WAL + checkpoint write path
- explicit durability modes (`relaxed`, `fsync-wal`, `fsync-checkpoint`)
- worker-pool server runtime with buffered parsing
- chunk-native protocol commands:
  - `GET` / `EXISTS` / `SET` / `UNSET` / `CHUNK` / `CHUNKBIN` / `INFO` / `AUTH`
- cache limit + eviction
- inter-process SWMR lock model (`.chunkdb.lock` writer lock + PID/session/heartbeat metadata)
- direct API benchmark + server-path benchmark
- stress/recovery validation:
  - hot-contention + eviction + load/unload stress
  - kill-recovery validation in `fsync-wal` and `fsync-checkpoint`
  - WAL replay edge-case recovery tests
  - long-run WAL growth + checkpoint cycle correctness tests

## Current Limitations

- only `fs_split_v1` backend is implemented in alpha
- no cross-chunk atomic transactions
- no replication/distributed durability model
- default operation assumes single process per `data_dir`
- durability and crash guarantees are mode-dependent and below full ACID systems
- benchmark suite is targeted, not a full production workload matrix

## Benchmark Scope

Benchmarking in this milestone is intentionally narrow:
- chunk/grid-oriented point and chunk operations
- direct storage path and end-to-end server path
- mode-dependent durability context

Benchmark output is intended to explain `chunkdb` behavior under this workload model, not to provide universal cross-category rankings.
See [docs/PERFORMANCE.md](PERFORMANCE.md).

## Roadmap (Next Updates)

- harden the existing alpha architecture (no scope expansion in the alpha line)
- expand long-run stress/fault-injection coverage
- improve benchmark reproducibility/reporting artifacts
- define stronger compatibility policy ahead of beta

## Not Included in Alpha

- additional storage backends
- distributed features (replication, consensus, sharding)
- broad cross-system performance claims
- full ACID transactional semantics
