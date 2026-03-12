# Storage Backends: Current and Planned

## Backend A (implemented): `fs_split_v1`

Layout:
- large chunk -> directory
- regular chunk -> `.chk` file
- per-regular-chunk WAL -> `.wal` file

### Strengths
- simple implementation
- natural per-chunk isolation
- straightforward per-chunk locking
- corruption blast radius is localized to one regular chunk
- easy incremental recovery via per-chunk WAL

### Weaknesses
- many files/inodes for very large worlds
- metadata overhead from filesystem operations
- directory scaling can become a factor for extremely large datasets

## Backend B (planned): `large_file_v1`

Planned direction:
- one file per large chunk
- internal offset/table-based mapping from regular chunk to slot/page
- optional free-space management for updates

Potential components:
- file header + format version + geometry
- chunk slot table / index page
- fixed-size or paged chunk payload areas
- per-large-file WAL/journal strategy

### Expected strengths
- fewer filesystem objects
- better sequential locality for scans
- lower inode pressure

### Expected weaknesses
- higher implementation complexity
- more complex locking and contention management
- allocator/fragmentation concerns
- larger corruption blast radius if index or core metadata is damaged

## Why `fs_split_v1` first

`fs_split_v1` was selected as the first productionable backend because it is easier to make correct under concurrency and crash recovery while iterating on protocol/runtime behavior.

`large_file_v1` remains an explicit roadmap item, not a hidden replacement.
