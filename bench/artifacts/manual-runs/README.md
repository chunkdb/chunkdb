# Manual Benchmark Runs

This folder stores real measured benchmark outputs captured directly from local runs.

## Run: 2026-10-06 (Apple M1 Pro, macOS 27.0, APFS) — storage format 2.0

The 2.0 storage format (manifest v2, sectioned chunk image, WAL with commit
time, TLV fields and typed records; issue #40) against the format before it
(`5fcd153`): `chunkdb_bench` (15 alternating repetitions) with data-directory
sizes, `chunkdb_large_world_bench` sparse writes, and checkpoint image sizes.

- [Environment and commands](format-2.0-20261006-macos-metadata.txt)
- [Summary](format-2.0-20261006-macos-summary.txt)
- [chunkdb_bench raw lines](format-2.0-20261006-macos-chunkdb-bench.txt)
- [Large-world sparse CSV](format-2.0-20261006-macos-large-world-sparse.csv)

## Run: 2026-09-07 (Apple M1 Pro, macOS 26.6.2, APFS) — CHUNKSCAN warm vs cold

`CHUNKSCAN` candidate collection before and after the per-large-chunk cache
merge, over three world shapes (wide, small page, one large-chunk column
wide). Both binaries are the same tree with only `src/world_read.cpp`
differing, so what is compared is the candidate walk alone.

```bash
chunkdb_large_world_bench --scenario chunkscan --threads 8 --repeats 3 \
    --output csv --chunks 60000 --cache 65536 --grid-width 1024 --scan-limit 1024
```

Files:
- `chunkscan-warm-vs-cold-20260907-macos.csv` — per-repeat rows, all six sides
- `chunkscan-warm-vs-cold-20260907-macos-summary.txt` — means and how to read them
- `chunkscan-warm-vs-cold-20260907-macos-metadata.txt` — host, build, what is measured


## Run: 2026-03-13 (Apple M1 Pro, 32 GB RAM)

Commands:

```bash
./build/chunkdb_bench --ops 20000
./build/chunkdb_server_bench --ops 5000 --port 4242
```

Durability mode in both binaries for this snapshot: `relaxed`.

### Baseline (pre-optimization pass)

Files:
- `direct-20260313-112616-before.txt`
- `server-20260313-112616-before.txt`

### After optimization pass

Files:
- `direct-20260313-112616-after.txt`
- `server-20260313-112616-after.txt`

## Previous alpha snapshot

Files:
- `direct-20260313-080002.txt`
- `server-20260313-080002.txt`

Public summary and interpretation:
- [docs/PERFORMANCE.md](../../../docs/PERFORMANCE.md)
