# Storage backend in 2.0

The only backend is `fs_split_v1`.
A table groups chunks into large-chunk directories and stores one checked image and optional WAL per populated chunk.
Geometry and typed schema are recorded in its manifest.
Chunk loading and WAL replay are lazy; cache and open-stream limits are shared across tables.
See [storage format](STORAGE_FORMAT.md), [server flags](SERVER_FLAGS.md) and [known limitations](KNOWN_LIMITATIONS.md).
