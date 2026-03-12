# Storage Backends (Alpha Scope)

## Implemented Backend: `fs_split_v1`

Layout:
- large chunk -> directory
- regular chunk -> `.chk` file
- per-regular-chunk WAL -> `.wal` file

### Strengths
- simple implementation
- natural per-chunk isolation
- straightforward per-chunk locking
- corruption blast radius is localized to one regular chunk
- incremental recovery via per-chunk WAL replay

### Weaknesses
- many files/inodes for very large worlds
- metadata overhead from filesystem operations
- directory scaling can become a factor for extremely large datasets

## Why This Is Frozen for Alpha

For the current alpha milestone, backend scope is intentionally frozen to `fs_split_v1`.
This keeps stabilization focused on correctness, durability behavior, runtime scalability, and benchmark transparency.

No additional backend is part of this alpha release scope.
