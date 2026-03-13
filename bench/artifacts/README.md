# Benchmark Artifacts

This directory documents benchmark artifact formats for `chunkdb`.

## Reproducible Run Bundles

Generated reproducible bundles should be placed under:

- `bench/artifacts/runs/<timestamp>-<short_sha>/`

Each run bundle contains:

- `metadata.txt` (commit, flags, ops, port)
- `system_*.txt` (host/runtime metadata)
- `cmake_configure.log`, `cmake_build.log`, `ctest.log`
- `chunkdb_bench.txt`
- `chunkdb_server_bench.txt`

Generate locally:

```bash
scripts/bench/run_reproducible_benchmarks.sh
```

For CI/generated bundles, use the `Benchmark Artifacts` GitHub workflow and download the uploaded artifact from the workflow run.

## Committed Manual Measured Runs

Ad-hoc measured runs can be committed under:

- `bench/artifacts/manual-runs/`

Current committed measured snapshot:
- date: 2026-03-13
- hardware: Apple M1 Pro, 32 GB RAM
- mode: `relaxed`
- files:
  - `direct-20260313-080002.txt`
  - `server-20260313-080002.txt`

Summary interpretation for this snapshot is documented in:
- [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md)
