# Manual Benchmark Runs

This folder stores real measured benchmark outputs captured directly from local runs.

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
