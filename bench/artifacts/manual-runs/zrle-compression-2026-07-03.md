# zrle Codec Micro-Benchmark — 2026-07-03

Command (fixed seed, reproducible):

```bash
./build/chunkdb_compression_bench --iterations 20000
```

Environment:

- Apple M1 Pro, macOS 26.5.1
- Release build (`-DCMAKE_BUILD_TYPE=Release`), Apple clang
- default geometry state size: 544 bytes (512 payload + 32 presence)

Results:

```text
iterations=20000 state_bytes=544
workload=sparse_2pct raw_bytes=544 compressed_bytes=60 ratio=0.110294 compress_mib_s=762.734 compress_p50_us=0.625 compress_p99_us=0.667 decompress_mib_s=2810.74 decompress_p50_us=0.125 decompress_p99_us=0.167
workload=half_dense raw_bytes=544 compressed_bytes=500 ratio=0.919118 compress_mib_s=440.42 compress_p50_us=1.125 compress_p99_us=1.209 decompress_mib_s=858.051 decompress_p50_us=0.542 decompress_p99_us=0.625
workload=dense_random raw_bytes=544 compressed_bytes=552 ratio=1.01471 compress_mib_s=1674.16 compress_p50_us=0.291 compress_p99_us=0.334 decompress_mib_s=7553.26 decompress_p50_us=0.042 decompress_p99_us=0.042
```

Decision recorded:

- Sparse states compress ~9x with sub-microsecond per-image cost; dense
  random states slightly expand (ratio ~1.01). Compression therefore remains
  **opt-in** (`--checkpoint-compression zrle`, per-request `CHUNKBINC`) and is
  **not** enabled by default. No general performance improvement is claimed.
- Codec latency is orders of magnitude below WAL/checkpoint I/O costs on this
  machine; tail impact on the write path is dominated by I/O, not the codec.
- Results are single-machine; re-run on target hardware before changing
  defaults.
