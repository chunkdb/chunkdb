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

## Experimental Layout Prototype (Benchmark-Only)

- `fs_region_v1` exists as an experimental benchmark prototype.
- It is not the server default.
- It is used only for A/B layout measurements and decision support.
- It is not part of the stable `1.x` support surface.
- It currently has no migration path and no compatibility guarantee.
- Latest measured A/B snapshot and gate decision are in [PERFORMANCE_LAYOUT_AB.md](PERFORMANCE_LAYOUT_AB.md);
  the recorded decision is `NO-GO` (whole-region read/modify/rewrite
  serialization and per-chunk WAL behavior defeat the layout's purpose), so
  the backend stays experimental until a redesign passes the repository's
  performance and correctness gates on supported platforms.

## Stable Backend Scope

For the stable `1.x` line, the supported backend scope is `fs_split_v1`.
This keeps stabilization focused on correctness, durability behavior, runtime scalability, and benchmark transparency.

No additional production-ready backend is part of the current stable support surface.
