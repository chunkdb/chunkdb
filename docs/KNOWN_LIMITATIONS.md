# Limitations

- **One writer:** one process owns a data directory; shared multi-writer use is unsupported.
- **Local filesystem:** the writer lock relies on `flock`, so keep the data directory on a local filesystem; on network and shared volumes a process on another host does not start beside a writer whose heartbeat is under 5 seconds old by its own clock, but this cannot exclude two hosts that start together and needs distinct host names and agreeing clocks.
- **One transaction table:** each transaction covers one table and, by default, at most 1024 written chunks and 8192 read chunks; see [Transactions](TRANSACTIONS.md#limits).
- **Bounded transactions:** duration defaults to 5000 ms, with private-write and snapshot-history budgets; see [Transactions](TRANSACTIONS.md#limits).
- **No global read snapshot:** ordinary area reads and scan pages can observe concurrent changes; use a transaction within one table.
- **Power loss in relaxed mode:** ordinary acknowledgements can be lost; use FLUSH WAL, a synced mode, or COMMIT; see [Durability](DURABILITY_CONTRACT.md).
- **Required filesystem operations:** atomic publication and sync must succeed; unsupported operations return errors rather than weakening guarantees.
- **Damaged logs:** malformed WAL headers or interior frames are refused; only crash-shaped incomplete tails are repaired during writer recovery.
- **Keep bookkeeping files:** losing both revision records with chunk artifacts or a capped startup inventory refuses writer opening; restore a consistent [Backup](BACKUP.md).
- **No automatic layout conversion:** unsupported data layouts are refused; see [Storage format](STORAGE_FORMAT.md).
- **Bounded live history:** ordinary watches resync when history is unavailable; see [Change feed](CHANGE_FEED.md).
- **Bounded slot history:** slots default to 1073741824 retained bytes; lost slots require rebuilding consumer state and recreating the slot.
- **Per-table backup cuts:** backups do not share a cross-table snapshot; restore is offline into a new directory; see [Backup and restore](BACKUP.md).
- **Fixed geometry:** chunk and large-chunk dimensions cannot change after table creation.
- **Table-wide rights:** you cannot grant access to only part of an area; see [Users and rights](USERS.md).
- **Read-only view at startup:** a `--read-only` server reads the tables, columns and users that exist when it starts, and its reads can fail while the writer changes a table without pause; see [Server flags](SERVER_FLAGS.md#read-only).
- **Metadata waits:** ALTER waits for running statements; a failure leaving the table unavailable fences it until RECOVER TABLE or a restart.
- **Chunk-count cache:** max-loaded-chunks counts chunks, not bytes, and is a soft limit: chunks in use are not evicted, so the cache can briefly exceed it; larger geometry and variable values consume more memory; measure the memory per chunk as described in [Server flags](SERVER_FLAGS.md#directory-and-shared-resources).
- **No chunk-count limit:** chunkdb does not cap the chunks of a table; files, directories and the coordinate index grow with the world.
- **Filesystem entries:** a chunk with data has a checkpoint image, a WAL, or both, in the directory of its large chunk, so a default 8x8 large chunk holds up to 128 files; a checkpoint removes the WAL, but with feed slots each checkpoint adds the archived WAL and, when one existed, a hard-linked base image to the flat `.chunkdb.feed/` directory of the table until every slot passes them, up to `slot_max_bytes` (default 1 GiB) per slot, and a large chunk with retained index decisions also holds `chunkdb.index`; each small file takes at least one filesystem block; see [Storage format](STORAGE_FORMAT.md#directory-layout).
- **Open cost:** opening a table counts at most 100000 of its files and replays a chunk's WAL when that chunk is first loaded, apart from WALs named by [recovery intents](STORAGE_FORMAT.md#conditional-and-transaction-intents); with a feed slot, the first slot sync after opening syncs every image and WAL of the table; at the default log level each table logs `startup recovery summary` with `elapsed_ms`.
- **Coordinate index memory:** opening a table discovers live artifacts and retains an ordered in-memory coordinate index; scan pages do not repeat a filesystem walk.
- **Deletion history:** tombstone compaction forgets empty identities and advances the deletion floor; consumers below it must rebuild their state.
- **Background threads:** maintenance and FLUSH WAL share `--maintenance-workers` threads, but a table with watches or slots still runs its own feed sender and slot worker threads, and a timer thread while `--feed-linger-ms` keeps its feed after the last watch; see [Server flags](SERVER_FLAGS.md).
- **Background checkpoint syncs:** at most `--maintenance-workers` background checkpoints sync at once per data directory; when writes make checkpoints due faster than those workers finish them, a write waits for or runs its chunk's checkpoint once the chunk holds four checkpoint intervals of deferred updates.
- **Authenticated metrics:** SHOW METRICS provides Prometheus text; there is no native HTTP scrape endpoint.
- **Audit log durability:** audit lines are flushed but not synced, so a power loss can drop the latest ones; a line that cannot be written does not fail its statement; on Windows a rotated audit log is reopened only by a restart; see [Server flags](SERVER_FLAGS.md#logs).
- **Channel binding behind TLS termination:** a login can be bound only to TLS that ends at the server; behind a load balancer that ends TLS, clients turn channel binding off, and `--auth-channel-binding require` and REQUIRE CHANNEL BINDING cannot be used; `tls-server-end-point` is undefined for a certificate whose signature algorithm has no single hash, such as Ed25519, where clients need TLS 1.3 and `tls-exporter`.
- **PROXY protocol version 2 only:** the text header of version 1 is not accepted.
- **Windows toolchain:** native builds use MSYS2 UCRT64/OpenSSL on Windows 10 or later; see [Windows native](WINDOWS_NATIVE.md) for untested alternatives.

## What chunkdb does not do

- **No secondary indexes or lookup by value:** statements address blocks and chunks by coordinates; to find blocks by a value, keep your own index, such as a second table keyed by that value, updated from a change feed slot.
- **No query planner or joins:** each statement reads or writes one table at the coordinates you name; see [Related data](CQL.md#related-data) for keeping data together, reading tables that share coordinates, and derived tables.
- **No server-side aggregates:** there is no COUNT, SUM or GROUP BY; compute them in your client from GET AREA or SCAN CHUNKS, or keep running totals updated from a change feed slot.
- **No replication:** durability across machines and distributed transactions are unavailable; keep copies with [Backup and restore](BACKUP.md), or copy changes to another system from a change feed slot.

Recorded measurements are in [Performance](PERFORMANCE.md).
