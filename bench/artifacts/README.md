# Benchmark Artifacts

This directory documents benchmark artifact formats for `chunkdb`.

## Reproducible Run Bundles

Generated reproducible bundles default outside the source tree:

- `${CHUNKDB_BENCH_OUTPUT_DIR:-${TMPDIR:-/tmp}/chunkdb-bench-runs}/reproducible/<timestamp>-<short_sha>/`

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

Use `CHUNKDB_BENCH_OUTPUT_DIR` or `OUT_DIR` to choose a different destination. Only copy small curated summaries into the repository after review; raw generated bundles should not live in the checkout by default.

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

4. 2026-03-15 (macOS logging level A/B, server path)
   - host: Apple M1 Pro, 32 GB RAM
   - mode: `relaxed`
   - benchmark: `chunkdb_server_bench --ops 5000 --port 4242`
   - compared log levels: `info` vs `warn`
   - files:
     - `server-loglevel-20260315-info-run1.txt`
     - `server-loglevel-20260315-info-run2.txt`
     - `server-loglevel-20260315-info-run3.txt`
     - `server-loglevel-20260315-warn-run1.txt`
     - `server-loglevel-20260315-warn-run2.txt`
     - `server-loglevel-20260315-warn-run3.txt`

5. 2026-03-19 (sparse low-cache protocol SET, 5x)
   - profile:
     - `requests=20000`
     - `clients=50`
     - `pipeline=1`
     - `keyspace=200000`
     - `durability=relaxed`
     - `max_loaded_chunks=8192`
     - `wal_group_commit_updates=8`
   - macOS host: Apple M1 Pro, 32 GB RAM
   - Windows host: GitHub Actions `windows-latest` (MSYS2 MinGW64)
   - files:
     - `server-sparse-low-cache-set-20260319-macos-5x.csv`
     - `server-sparse-low-cache-set-20260319-macos-5x-summary.txt`
     - `server-sparse-low-cache-set-20260319-windows-ci-23285270445-5x.csv`
     - `server-sparse-low-cache-set-20260319-windows-ci-23285270445-5x-summary.txt`
     - `server-sparse-low-cache-set-20260319-windows-ci-23285270445-metadata.txt`

6. 2026-09-05 (large-world sparse SET, eviction-normalized, 5x)
   - benchmark: `chunkdb_large_world_bench` (`bench/large_world_bench.cpp`)
   - profile:
     - `geometry=8x8 chunks / 16x16 blocks / 16 bits`
     - `durability=relaxed`
     - `max_loaded_chunks=16384`
     - `wal_group_commit_updates=8`
     - `checkpoint_update_interval=256`
     - `checkpoint_wal_bytes=1 MiB`
     - `background_maintenance=off`
     - cache pre-filled with exactly `max_loaded_chunks` fresh chunks before the
       measured window, so every measured write evicts (warm-up excluded)
     - steady run: `--chunks 5000 --threads 1 --repeats 5`
     - writer scaling: `--chunks 15000 --threads 1|4|8|16|32|64 --repeats 5`
   - macOS host: Apple M1 Pro, 32 GB RAM, macOS 26.6.2, APFS on the internal
     SSD; `F_FULLFSYNC` confirmed honored (no fallback to plain `fsync`)
   - Linux/ext4 host: **not measured yet** (the durable sync there is
     `fdatasync`, so absolute numbers are expected to differ)
   - files:
     - `large-world-sparse-set-20260905-macos-5x.csv`
     - `large-world-sparse-set-20260905-macos-threads-5x.csv`
     - `large-world-sparse-set-20260905-macos-5x-summary.txt`
     - `large-world-sparse-set-20260905-macos-metadata.txt`
     - `large-world-sparse-set-20260905-macos-chunkdb-bench-crosscheck.txt`
   - note:
     - this snapshot replaces the three mutually inconsistent sparse-write
       throughputs that used to be quoted in `docs/`; the comparable figure is
       `ms_per_eviction`, not `ops_s`. See
       [docs/KNOWN_LIMITATIONS.md](../../docs/KNOWN_LIMITATIONS.md) and
       [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md).

Summary interpretation for these snapshots is documented in:
- [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md)
