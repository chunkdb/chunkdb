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

Committed snapshots:

1. 2026-03-13 (macOS)
   - hardware: Apple M1 Pro, 32 GB RAM
   - mode: `relaxed`
   - files:
     - `direct-20260313-080002.txt`
     - `server-20260313-080002.txt`

2. 2026-03-15 (Windows native)
   - host: Windows 10, AMD Ryzen 5 4600H, 16 GB RAM
   - mode: `relaxed`
   - files:
     - `direct-20260315-windows-native.txt`
     - `server-20260315-windows-native.txt`
     - `smoke-20260315-windows-native.txt`
     - `windows-native-20260315-metadata.txt`
   - note:
     - server benchmark printed valid metrics and then failed while removing the temp dir because `writer.lock` was still in use.

3. 2026-03-15 (Windows CI native after cleanup hardening)
   - host: GitHub Actions `windows-latest` (MSYS2 MinGW64)
   - mode: `relaxed`
   - lock mode: `serial-mutex`
   - files:
     - `direct-20260315-windows-ci-70023dd-serial-mutex.txt`
     - `server-20260315-windows-ci-70023dd-serial-mutex.txt`
     - `windows-ci-20260315-70023dd-metadata.txt`
   - note:
     - this snapshot is from CI run `23108584332` on commit `70023dd` and includes post-fix benchmark cleanup behavior.

Summary interpretation for this snapshot is documented in:
- [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md)
