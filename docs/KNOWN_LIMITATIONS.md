# Known limitations in 2.0

## Data and concurrency

- One writer owns a data directory; shared multi-writer use is unsupported.
- Transactions cover one table, at most 64 written and 1024 read chunks, with bounded duration, private copies and history ([transactions](TRANSACTIONS.md)).
- Ordinary reads, `GET AREA` and `SCAN CHUNKS` do not provide a global snapshot; use a transaction for a consistent view within one table.
- Read-only processes obtain coherent state per chunk and never recover files; an odd snapshot generation after a writer crash requires writer recovery before uncached reads succeed.
- Read-only processes see the table catalog present at startup; new tables require reopening.
- A table's chunk and large-chunk geometry cannot be changed.
- Rights apply to a whole table, rather than a sub-area.
- ALTER reopens a table and waits for its running statements; a failure that leaves the table unavailable requires restart.

## Persistence

`relaxed` acknowledgements alone do not promise survival of power loss.
Use `FLUSH WAL`, a synced durability mode or a committed transaction according to the [durability contract](DURABILITY_CONTRACT.md).
Replication and distributed durability are not provided.
Filesystems must support the required sync and atomic publication operations; failures do not silently weaken strict durability.
Creating directories/tables on POSIX requires exclusive rename or hard-link support.
Windows writable operation requires the directory-sync capability used by snapshot bookkeeping, including in relaxed mode.

A damaged WAL header or interior frame is refused and is not automatically repaired.
Only a crash-shaped incomplete tail is repaired during writer recovery; use `chunkdb_verify` to inspect damage and a consistent backup to recover it.
Loss of both the revision clock and its initialized marker cannot be distinguished from interrupted first initialization; retain both with the data.
Unsupported layouts are refused without an in-place conversion ([storage format](STORAGE_FORMAT.md)).

## Streams and backup

The live change feed is memory bounded; ordinary watches receive `resync` after losing history.
Durable slots also have a retention limit; a lost slot must be recreated after rebuilding consumer state.
WATCH, slots, migrations and online backup require single-process writer operation.
Backup has a cut per table, rather than one cross-table revision or transaction snapshot.
It requires `--backup-dir`, rejects unsafe/nonempty destinations and pending migration decisions, and restores offline into a new directory.
See [change feed](CHANGE_FEED.md) and [backup](BACKUP.md).

## Resources and platforms

`--max-loaded-chunks` counts chunks rather than bytes; wider columns, larger geometry and variable-length values increase memory per chunk.
SCAN's first page builds an in-memory catalog of large chunks, and a visited large-chunk directory is listed in full.
The file-per-chunk layout consumes filesystem entries for populated chunks and WALs.
Background maintenance runs a thread per table; `FLUSH WAL` visits tables in turn.
Metrics are obtained with authenticated `SHOW METRICS`; there is no native HTTP scrape endpoint.
Native Windows TLS is supported with MSYS2 MinGW64/OpenSSL; other Windows TLS toolchains are untested.
Measured performance and host-specific results are kept separately in [PERFORMANCE.md](PERFORMANCE.md).
