# Storage Backends

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

## Supported Backend Scope

`fs_split_v1` is the only storage backend. An experimental packed layout
(`fs_region_v1`, several chunks per file) was measured against it, failed its
gate, and was removed in 2.0; the measurement and the decision are kept in
[PERFORMANCE_LAYOUT_AB.md](PERFORMANCE_LAYOUT_AB.md).
