# Benchmark Artifacts

This directory documents the reproducible benchmark artifact format for `chunk`.

Generated run bundles should be placed under:

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
