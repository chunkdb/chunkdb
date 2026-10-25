# Limitations

- **One writer:** one process owns a data directory; shared multi-writer use is unsupported.
- **Local filesystem:** the writer lock relies on `flock`, so keep the data directory on a local filesystem; network and shared volumes may let a second process take it over.
- **One transaction table:** each transaction covers one table, at most 64 written chunks, and 1024 read chunks; see [Transactions](TRANSACTIONS.md).
- **Bounded transactions:** duration defaults to 5000 ms, with private-write and snapshot-history budgets; see [Transactions](TRANSACTIONS.md#limits).
- **No global read snapshot:** ordinary area reads and scan pages can observe concurrent changes; use a transaction within one table.
- **Power loss in relaxed mode:** ordinary acknowledgements can be lost; use FLUSH WAL, a synced mode, or COMMIT; see [Durability](DURABILITY_CONTRACT.md).
- **Required filesystem operations:** atomic publication and sync must succeed; unsupported operations return errors rather than weakening guarantees.
- **Damaged logs:** malformed WAL headers or interior frames are refused; only crash-shaped incomplete tails are repaired during writer recovery.
- **Keep bookkeeping files:** losing both the revision clock and its initialized marker makes interrupted initialization indistinguishable; retain a consistent [Backup](BACKUP.md).
- **No automatic layout conversion:** unsupported data layouts are refused; see [Storage format](STORAGE_FORMAT.md).
- **Bounded live history:** ordinary watches resync when history is unavailable; see [Change feed](CHANGE_FEED.md).
- **Bounded slot history:** slots default to 1073741824 retained bytes; lost slots require rebuilding consumer state and recreating the slot.
- **Per-table backup cuts:** backups do not share a cross-table snapshot; restore is offline into a new directory; see [Backup and restore](BACKUP.md).
- **Fixed geometry:** chunk and large-chunk dimensions cannot change after table creation.
- **Table-wide rights:** you cannot grant access to only part of an area; see [Users and rights](USERS.md).
- **Read-only view at startup:** a `--read-only` server reads the tables, columns and users that exist when it starts, and its reads can fail while the writer changes a table without pause; see [Server flags](SERVER_FLAGS.md#read-only).
- **Metadata waits:** ALTER waits for running statements; a failure leaving the table unavailable requires restart.
- **Chunk-count cache:** max-loaded-chunks counts chunks, not bytes, and is a soft limit: chunks in use are not evicted, so the cache can briefly exceed it; larger geometry and variable values consume more memory; see [Server flags](SERVER_FLAGS.md).
- **Filesystem entries:** populated chunks and WALs consume separate filesystem entries.
- **Scan catalog memory:** the first scan builds an in-memory large-chunk catalog; each visited large-chunk directory is listed in full.
- **Background threads:** maintenance uses a thread per table; FLUSH WAL visits tables in turn.
- **Authenticated metrics:** SHOW METRICS provides Prometheus text; there is no native HTTP scrape endpoint.
- **Audit log durability:** audit lines are flushed but not synced, so a power loss can drop the latest ones; a line that cannot be written does not fail its statement; on Windows a rotated audit log is reopened only by a restart; see [Server flags](SERVER_FLAGS.md#logs).
- **PROXY protocol version 2 only:** the text header of version 1 is not accepted.
- **Windows toolchain:** native builds use MSYS2 UCRT64/OpenSSL on Windows 10 or later; see [Windows native](WINDOWS_NATIVE.md) for untested alternatives.

## What chunkdb does not do

- **No secondary indexes or lookup by value:** statements address blocks and chunks by coordinates; to find blocks by a value, keep your own index, such as a second table keyed by that value, updated from a change feed slot.
- **No joins:** each read names one table; read each table and combine the results in your client, or keep values you read together in one table.
- **No server-side aggregates:** there is no COUNT, SUM or GROUP BY; compute them in your client from GET AREA or SCAN CHUNKS, or keep running totals updated from a change feed slot.
- **No replication:** durability across machines and distributed transactions are unavailable; keep copies with [Backup and restore](BACKUP.md), or copy changes to another system from a change feed slot.

Recorded measurements are in [Performance](PERFORMANCE.md).
