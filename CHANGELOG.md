# Changelog

All notable changes to this project will be documented in this file.

Release naming note:
- Starting with `v1.0.0`, `chunkdb` follows [Semantic Versioning](https://semver.org/)
  against the surface defined in `docs/COMPATIBILITY.md`.
- `preview`/`engineering alpha` describe the earlier `v0.1.x` line.

## Unreleased

- Bound transaction pause storage by its enum, reject rooted or backslash backup names on Windows, and launch backup crash-test children with complete command quoting (#66).

- Preserve relaxed backup cuts across chunk eviction, sync linked WAL replacements before rename, give queued backups priority over deferrable checkpoints, and anchor destination writes against symlink replacement (#66).
- Create backup destination directories, guards and copied files without following path components; clean up interrupted staging even before its owner guard is written (#66).

- Confine BACKUP destinations to --backup-dir, allow disconnected clients to finish their copies, avoid cold-chunk loads during pinning and give restored data directories fresh identities (#66).

- Add online BACKUP TO with per-table revision cuts, checksummed inventories,
  backup verification and chunkdb_restore. Restore starts a new epoch per table
  and resets retained slots to the restored cut (#66).
- Seed recovered feed WAL boundaries on chunk load or catch-up instead of replaying every live WAL when opening a table with slots (#65).

- Add CREATE SLOT, DROP SLOT, SHOW SLOTS, durable WATCH SLOT catch-up and batched ACK with
  resume across server restarts. Slot watches share the feed I/O thread and
  read archives on a separate bounded worker. Add slot retention/sync flags (#65).

- Add C++ durable feed slots, checksummed positions and durable frontiers,
  checkpoint WAL archives with linked bases, retention limits, writer USER
  metadata, and a typed archive reader. Slot tables set storage incompat bit 0
  and are refused by builds without this feature (#65).

- Add in-memory WATCH/UNWATCH streams with typed before/after rows, AREA,
  buffered AFTER replay, schema notifications and resync. A shared I/O thread
  serves plain/TLS watches without holding workers. Add `--feed-buffer-bytes`,
  `--max-watches` and benchmark `--watch` support (#65).

### Breaking (storage format v2 — chunkdb 2.0)

- **A data directory records its geometry** (#38). A new store writes
  `chunkdb.manifest` (geometry and a random store id, checksummed) before any
  other file, and every later start uses the recorded geometry. Geometry
  settings apply only when a store is created: the server's geometry flags
  may be omitted for an existing store, and a given flag must match the
  stored value. Before this, restarting with different `--block-bits` or
  large-chunk flags read existing data as zeros and mixed new writes into the
  old files. A data directory without a manifest is initialized only when it
  holds no chunkdb data; one holding data written by 1.x or by an earlier 2.0
  development build is refused, so this build does not open 1.x data. `chunkdb_verify`
  reads the geometry from the manifest; its geometry flags are removed, and a
  missing or damaged manifest is reported as an error.
  `StoreConfig::geometry_fields` names the geometry values a library caller
  requires (all of them by default)
- **Extensible storage format** (#40). The store manifest (version 2) carries
  `incompat` / `ro_compat` / `compat` feature flags and an options area: a
  build refuses a store with a feature it does not know, or opens it
  read-only when the feature only forbids writing. Durable feed slots define
  table incompat bit 0. Manifests written by earlier 2.0 development builds (version 1) are
  refused. `chunkdb_verify` reports unknown features. The engine and
  `chunkdb_verify` no longer read 1.x artifacts (`.chk` v1–v3, `.wal`
  v2/v3, a v4 header written after 1.x records) or the intermediate 8-byte
  version-clock record. Chunk images are a new layout (magic `CHKIMAGE`): a header with
  the store id, feature flags, revision and commit time, then a directory of
  checksummed sections (`PAYLOAD`, `PRESENCE`), each optionally
  zrle-compressed. An image from another store, or one using a feature the
  store does not record, is rejected. WALs are a new layout too (magic
  `CHKWALOG`): a checksummed header with the store id and feature flags, and
  frames with a commit time, optional fields (`TAG`) and typed records; a
  span is no longer split into 64 KiB records, and one frame CRC replaces the
  per-record CRCs
- **On-disk format v2.** Checkpoint images are written as version `4`
  (raw) / `5` (zrle) with the chunk revision and a header CRC appended to the
  1.x header, and WAL logs as version `4`, a sequence of frames (one
  mutation per frame, record CRCs over header and body, a frame CRC). A 1.x
  binary cannot read v2 artifacts
- **Chunk versions are persisted revisions.** `CHUNKVER` no longer changes on
  eviction or restart, so conditional writes (`CHUNKPUT ... IF`,
  `CHUNKBATCH`) stop failing spuriously
  under memory pressure (audit CDB-LIM-1). Cold loads no longer consume the
  version clock
- **Every mutation is crash-atomic.** A WAL frame is applied entirely or not
  at all, so full-chunk writes (`CHUNKPUT`) are atomic for every geometry
  (CDB-LIM-2) and the 65535-byte single-record bound that made conditional
  writes and `CHUNKBATCH` reject large geometries is gone
- **WAL header corruption is detected.** The v4 record CRC covers
  `byte_offset` and `data_size` (CDB-DEF-1), closing the known limitation
- the version clock is raised past any persisted revision it meets at load
  time, so revisions cannot repeat even after the clock bookkeeping is lost
- `chunkdb_verify` validates v4/v5 images and v4 frames; its summary line is
  `SUMMARY checked=<n> warnings=<n> errors=<n>`
- version-clock bookkeeping writes no longer consume the generic
  `ATOMICWRITE` failpoints (they have their own hook)

- **Tables** (#41). A data directory holds named tables, each with its own
  geometry, fixed at creation, and its own durability and checkpoint options,
  which `ALTER TABLE ... SET` can change (statements in docs/CQL.md), and the error codes `NO_TABLE` and `TABLE_EXISTS`. The server creates a `default` table from its geometry flags when the data directory has no table. Layout:
  `chunkdb.manifest` now identifies the data directory (magic `CKDM`, feature
  flags), and each table lives in `tables/<name>/` with `table.manifest`
  (the former store manifest, now carrying the table options). Creating and
  dropping a table are crash-atomic (staging and drop directories, one
  rename). The option flags (`--durability`, `--checkpoint-updates`,
  `--checkpoint-wal-bytes`, `--wal-group-commit-updates`,
  `--checkpoint-compression`) set the options of tables the server creates;
  a given flag that differs from an existing table's stored option refuses
  the start. `--max-loaded-chunks` and
  `--max-open-wal-streams` are budgets for all tables together, with eviction
  and stream reuse across tables. `FLUSH WAL` covers every table; `SHOW METRICS` sums all tables.
  `chunkdb_verify` checks the data-directory manifest, leftovers of
  interrupted table operations and every table. A single-store data
  directory of an earlier 2.0 development build is refused
- **Users and rights** (#63, [docs/USERS.md](docs/USERS.md)). Users log in with a password through SCRAM-SHA-256 inside `HELLO` (`HELLO 3 USER <name> $1`, `+SCRAM ...`, `AUTH $1`; the HELLO map adds `server_signature`), so the password never crosses the network and the server proves it holds the user's verifier; a wrong password and an unknown user get the same `AUTH_FAILED`, and the per-connection and per-source failure limits stay. Users, verifiers and grants live in `chunkdb.users`, replaced atomically. `CREATE USER ... VERIFIER $1`, `ALTER USER`, `DROP USER`, `GRANT`/`REVOKE READ | WRITE | ADMIN ON <table> | *` and `SHOW USERS` manage them; verifiers are computed by clients. Every statement checks its right (`PERMISSION_DENIED <right> on <table>`), a table without any right reads as `NO_TABLE`, and `SHOW TABLES` lists only tables the user has a right on. The first start creates the first administrator from `--admin-user` and `--admin-password-file` (or `CHUNKDB_ADMIN_USER` and `CHUNKDB_ADMIN_PASSWORD`); `chunkdb_admin reset-password` sets a new password offline. `--auth none` runs without users for local development. The token is gone: `--token`, `--token-file`, `--no-auth`, `CHUNKDB_TOKEN`, `HELLO 3 AUTH <token>` and tokens in URIs; URIs carry `user:password`. A server listening beyond localhost without TLS logs a warning. `chunkdb_server_bench` logs in with `--user` and `--password-file` or the URI
- **Protocol 3: CQL** (#42, #62, [docs/PROTOCOL.md](docs/PROTOCOL.md), [docs/CQL.md](docs/CQL.md)). A connection starts with `HELLO 3`, which logs in and answers a map of the server's version and limits, and then sends one CQL statement per line: `GET`/`SET`/`DELETE BLOCK`, `GET`/`SET CHUNK`, `GET AREA ... TO` and `... AROUND ... RADIUS`, `SCAN CHUNKS`, `CREATE`/`ALTER`/`DROP TABLE`, `SHOW TABLES`, `DESCRIBE`, `FLUSH WAL`, `SHOW METRICS` and `PING`. Every statement names its table. Replies are typed (RESP3): `:` integers, `,` doubles, `#t`/`#f`, `_` for `NULL` or an absent block, `%` maps; `text`, `bytes` and `bits` values and chunks are bulk strings. Values are literals (`'it''s'`, `x'0aff'`, `b'1010'`, `NULL`) or `$n` parameters sent after the line as frames in the column's binary form, so user input passed as a parameter cannot become a command; the server bounds every frame by its column before reading it. A chunk travels as one binary form (version, schema version, presence bitmap, payload, `text` and `bytes` values), so whole-chunk writes carry every column; `SET CHUNK` refuses a form encoded for another schema version (`SCHEMA_MISMATCH`). `IF VERSION` makes any write conditional on the chunk version. Errors: `PROTOCOL`, `SYNTAX` (with the column of the first token that does not fit), `INVALID_ARGUMENT`, `OUT_OF_RANGE`, `VERSION_MISMATCH current=<v>`, `SCHEMA_MISMATCH current=<v>`, `PERMISSION_DENIED`, `NO_TABLE`, `TABLE_EXISTS`, `BAD_REQUEST`, `BUSY`, `INTERNAL`. Protocol 1 is not served: any other first line gets `-ERR PROTOCOL expected HELLO 3` and the connection closes, so a 1.x client fails at once instead of misreading replies. Gone without a statement: `EXISTS`, `MGET`/`MSET`, `CHUNKBATCH` (transactions come with #64), `INFO` (`DESCRIBE` and `SHOW METRICS`), `ZRLE` on the wire and text chunk transfer. The Docker health check sends `HELLO 3`; `chunkdb_server_bench` speaks protocol 3, and its `info` and `chunkget` scenarios are gone

- **Tables record their columns** (#61, [docs/COLUMNS_DESIGN.md](docs/COLUMNS_DESIGN.md)). The table manifest (version 3, at most 1 MiB) holds a schema area after its options: a version, and per column an id, a name, a type (`uN`, `iN`, `bool`, `f32`, `f64`, `bits(N)`, `text(max)`, `bytes(max)`), the `NULL` and `REQUIRED` flags and a default. It no longer records `block_bits`: a block's width is the schema's fixed bits. A table created with a block width is one column `bits` of type `bits(block_bits)` and stores exactly the bytes it did; this build refuses other schemas until typed tables arrive. Manifests of version 2, written by 2.0 development builds, are refused
- **Tables with typed columns** (#61). A chunk's payload is column-major: per fixed-width column its values, then for a `NULL` column one validity bit per block, each padded to a byte (docs/STORAGE_FORMAT.md Section 2); a one-column `bits(N)` table keeps its bytes. `StoreConfig::schema` and `TableCatalog::Create` create tables with `uN`, `iN`, `bool`, `f32`, `f64` and `bits(N)` columns; `ChunkStore::SetBlock` and `GetBlock` write and read typed values: a new block takes each column's `DEFAULT`, `NULL` or zero, and is refused while a `REQUIRED` column is missing; a value outside its column's type is refused before anything changes. `UnsetBlock` removes a block with all its values. The bit-string functions (`SetBlockBits`, `GetBlockBits`, chunk bit strings, `ApplyChunkBatch`) work only on a table with one `bits(N)` column. `Table::geometry()` returns the full `Geometry`, and `TableInfo` carries the schema
- **`text` and `bytes` columns** (#61). Their values are stored per chunk in a VARS section and logged as `VAR_PUT`/`VAR_DEL`/`VAR_REPLACE` WAL records (docs/STORAGE_FORMAT.md Sections 3.2 and 4.1); a block without a value takes no space. `SetBlock` and `GetBlock` write and read them as `std::string` (UTF-8, checked) and `BytesValue`, up to the column's `max` bytes; the values of one chunk are bounded by the new table option `var_max_chunk_bytes` (default 1 MiB, `CREATE TABLE ... WITH`, `ALTER TABLE ... SET`). In a column that cannot be `NULL` the empty value is stored as no value. Table manifests are version 4; version 3 tables of earlier 2.0 development builds are refused. They replace per-block extra data, which never shipped: `XGET`, `XPUT`, `XDEL`, the `EXTRA` option of `CHUNKGET`/`CHUNKPUT`, the `XPUT`/`XDEL` operations of `CHUNKBATCH`, the `extra_max_block_bits`/`extra_max_chunk_bytes` options, the `extra-data` capability and feature flag, and `max_extra_chunk_bytes` in `HELLO` are gone
- **Adding, dropping and renaming columns** (#61). `TableCatalog::ChangeColumns` with `AddColumn`, `DropColumn` or `RenameColumn` writes the next schema version into the table manifest atomically and reopens the table; nothing else is rewritten. Images and WAL frames written by an earlier version are translated when a chunk loads: kept columns keep their values, an added column takes its `DEFAULT` (else `NULL`, else zero) in every present block, a dropped column's values go. Images record their schema version in a new `SCHEMA` section and WAL frames in a `SCHEMA` TLV, both only once a table is past version 1; the schema area keeps the history that rebuilds every earlier version (docs/STORAGE_FORMAT.md Sections 1.2, 3 and 4.1). A `REQUIRED` column can be added only with a `DEFAULT`, and the last fixed-width column cannot be dropped. `Table::geometry()` returns a copy, since a table's columns can change
- **Changing a column's type** (#61). `ChangeColumnType` moves a column to another type of its family (integers `uN`/`iN`, floats, `text`, `bytes`, `bits`): exactly when the new type holds every old value, or with a conversion for values that do not fit — clamp (numbers), default (the column's `DEFAULT`, else `NULL`, else zero) or truncate (text at a character boundary, bytes, bits). The column's `DEFAULT` follows. Like other column changes it writes one schema version; values convert when a chunk written earlier loads. Changes between families are refused: add a column, copy, drop
- **Narrowing a column after checking its values** (#61). `TableCatalog::NarrowColumn` moves a column to a narrower type of its family without a conversion: it records the narrowing in the manifest, so every write to the column must fit the narrower type too, reads every populated chunk, and then writes the new schema version — or, when a stored value does not fit, drops the narrowing and names that block and value. A crash during the check leaves the schema as it was; the next open drops the narrowing
- `ChunkStore::SetBlock` and `UnsetBlock` take an expected chunk version and return the version after the write; `ChunkStore::ReadChunkState` and `WriteChunkState` read and replace a chunk with its `text` and `bytes` values; area reads can return each chunk's version and values (#62)
- **1.x data is neither read nor converted** (#60). The 2.0 engine reads only
  the 2.0 format; a data directory written by 1.x, or by the unreleased
  storage format of `main` between 1.3.0 and 2.0, is refused before the
  writer lock touches it and is not changed. There is no conversion tool:
  data starts anew in 2.0. A regular file where the `.chunkdb.lock`
  directory belongs (the lock of releases before 1.0) is no longer renamed
  and replaced: the start is refused

### Added

- **Transactions** (#64, [docs/TRANSACTIONS.md](docs/TRANSACTIONS.md), design in [docs/TRANSACTIONS_DESIGN.md](docs/TRANSACTIONS_DESIGN.md)). `BEGIN`, block, chunk and area statements of one table, then `COMMIT` or `ROLLBACK`. Reads see one snapshot taken by the first statement; writes go to private copies and answer `_`; `COMMIT` applies them to every chunk together with one version, durably in every durability mode and all-or-nothing across a crash (a transaction intent next to the WALs), or answers `-ERR CONFLICT <reason>` when a chunk read or written changed after the snapshot, so the result is serializable. While transactions are open, writes keep the earlier states their snapshots need; without open transactions a write pays one atomic load. Limits: `--txn-max-duration-ms`, `--txn-max-bytes`, `--txn-total-bytes`, `--txn-history-bytes`, 64 written and 1024 read chunks per transaction
- `--max-handshakes-per-ip <n>` (`ServerConfig::max_handshakes_per_ip`, off by default): one source address (IPv4, or IPv6 /64) may hold at most that many workers before `HELLO` succeeds; more connections get `-ERR BUSY` and are closed


### Removed

- the experimental `fs_region_v1` storage layout, which failed its A/B gate
  (`docs/PERFORMANCE_LAYOUT_AB.md`). It was never selectable from the server.
  Removed with it: `StoreConfig::storage_layout_mode` and
  `experimental_region_span_chunks`, the `CHUNKDB_BUILD_EXPERIMENTAL_LAYOUT`
  CMake option, `chunkdb_layout_ab_bench`, `scripts/bench/layout_ab.sh`, and
  `chunkdb_verify --region-span-chunks`. `.rgn` files are no longer read;
  `chunkdb_verify` reports one as `unexpected_file`

### Fixed

- in `relaxed` mode a chunk flushed its WAL batch after `wal_group_commit_updates` WAL records, not updates: a `SET` that creates a block writes two records, so the default of 8 flushed after 4 such writes, and a `CHUNKPUT` or batch counted twice. It now counts updates, as documented (#76). 1.3.0 has this bug too
- the server process could be killed by SIGPIPE: a client (no token needed) that sent requests and closed or reset the connection without reading the replies, or a TLS client that reset after the handshake, made a later reply write end the process, losing acknowledged `relaxed` writes still in the group-commit batch. The server ignores SIGPIPE and sends with `MSG_NOSIGNAL` / `SO_NOSIGPIPE`; a program that embeds the server and leaves SIGPIPE at its default action has it set to ignored when `ChunkServer::Run` starts. 1.3.0 has this bug too
- a request line cut off by the end of the stream (for example `TABLEDROP t` of `TABLEDROP t2`, or the first operations of a `CHUNKBATCH`) was executed when the connection closed; it is now discarded. 1.3.0 has this bug too
- a connection that never completed `HELLO` could hold a worker indefinitely by repeating it (`AUTH_REQUIRED` and invalid `HELLO`s were not counted, and each reset the idle timer). Every failed `HELLO` now counts toward `max_auth_failures`, and `HELLO` must succeed within `--client-io-timeout-ms` of the connection's start, including a `HELLO` line still arriving
- a TLS client could hold a worker indefinitely by sending one TLS record a byte at a time: the read waited inside the record with only the idle timeout, renewed by every byte. Once record bytes arrive, the request now has `--client-io-timeout-ms` to complete, as over plain TCP; a record without data (an alert, a TLS 1.3 key update) does not start a request. 1.3.0 has this bug too
- a request line or payload over plain TCP could take up to about twice `--client-io-timeout-ms`, because each wait for more bytes started the full timeout again; each wait now ends at the request's deadline. 1.3.0 has this bug too
- `MGET` had no reply bound: one request could make the server build a reply of about 1 GiB. A reply that could exceed 64 MiB now fails with `OUT_OF_RANGE` before anything is read. 1.3.0 has this bug too
- `--client-io-timeout-ms` and `--idle-connection-timeout-ms` above about 292 years overflowed the deadline clock and made every reply fail; both are now limited to 1 day. 1.3.0 has this bug too
- a crash between a checkpoint's image publish and its WAL removal could recover a state that never existed, at an old revision: in `relaxed` mode the image included frames still in the group-commit batch that the WAL file lacked, and replay applied the older WAL frames over the newer image. Replay now skips frames at or below the image's revision and requires frame revisions to increase. Empty-chunk collection could bring blocks back after the same kind of crash, because it removed the image before the batch reached the WAL; it now flushes the batch first. A store that is fail-closed after a failed conditional-write rollback no longer checkpoints (eviction keeps the WAL), which had removed the WAL its rollback intent needs and made the next start fail
- a WAL whose last frame was whole and checksum-valid but failed its checks (only a writer bug or a foreign file can produce one) was treated as a crash tail and truncated on a read-write load, dropping that frame silently; the load now fails and `chunkdb_verify` reports `wal_damaged`
- a table dropped and created again under the same name started its version clock at 1 again, so a version token from the earlier table could match a chunk of the new one and let a stale `CHUNKPUT ... IF` or `CHUNKBATCH IF` apply. `TABLEDROP` now records the dropped table's clock ceiling in `chunkdb.manifest` (option `version_floor`), and a new table's clock starts above it
- a clean shutdown wrote the group-commit batches to the WAL files but did not sync them, so a `WALFLUSH` in the restarted server, which only knows its own unsynced files, did not cover writes the previous process had acknowledged. A clean shutdown now syncs like `WALFLUSH`. Writes acknowledged by a server that crashed are durable only if a `WALFLUSH` covered them before the crash; the docs now say so
- on macOS, WAL acknowledgements in `fsync-wal` / `fsync-checkpoint`, `WALFLUSH`, conditional-write boundaries and directory syncs used plain `fsync`, which returns before the drive has stored the data, so an acknowledged write could be lost on power loss although DURABILITY_CONTRACT said strict durability uses `F_FULLFSYNC`. They now use `F_FULLFSYNC`, as checkpoint images already did; each synced write on macOS waits for the drive and is much slower as a result. 1.3.0 has this bug too
- empty-chunk collection could bring deleted blocks back when the chunk's WAL had outlived an earlier checkpoint (a crash between that checkpoint's image publish and its WAL removal, with frames still in the group-commit batch): such a WAL is right only over that image, and collection removes the image first. If collection then failed or crashed, the WAL was replayed over no image, in the same process (after eviction or through an uncached area read) or after a restart. When the chunk has an image, collection now ends the WAL with a frame that sets the whole chunk state to empty
- a store that had failed closed after a write's WAL repair failed (a failed conditional-write rollback, or an ordinary append whose truncation failed) could serve the write that failed: eviction dropped the chunk's rolled-back state from memory, and the reload replayed the failed frame from the WAL. Such a chunk now stays cached until a restart, and `TABLESET` refuses a fail-closed table instead of reopening it
- in `relaxed` mode a conditional write (`CHUNKPUT ... IF`, `CHUNKBATCH`) recorded its rollback boundary before flushing the group-commit batch and without syncing the WAL: when its rollback failed, the next start cut off writes acknowledged before it, and after a power loss the boundary could name WAL bytes that were gone, so the store refused to open. The batch is now flushed and the WAL synced before the boundary is recorded, which adds a WAL and directory sync to these commands in `relaxed` mode
- `TABLESET` could lose acknowledged writes: the old store's batched writes were flushed only by its destructor, which logs a failure, so `TABLESET` replied `+OK` and the writes were gone. And the record of files written without a sync belonged to the old store, so a `WALFLUSH` after `TABLESET` did not sync writes acknowledged before it. `TABLESET` now flushes the batches first and fails, changing nothing, if that fails; the new store takes over the record. `TABLEDROP` also flushes first, so a drop that fails and reopens the table keeps them, but it goes ahead when the flush fails
- a crash while the version clock, its marker, the snapshot-generation record, a conditional intent or the writer's heartbeat was being replaced left a temp file that nothing removed, and `chunkdb_verify` reported it on every run. A writer's open now removes such files of processes that are gone. 1.3.0 has this bug too
- a write that failed past the point where it may already be applied got an ordinary error reply, which reads as "not applied": a conditional write whose commit record was visible but could not be made durable, and a write whose failed WAL append could not be removed (both leave the store fail-closed, and a restart may keep the write). Both now reply `-ERR INTERNAL write outcome unknown: ...` (`WriteOutcomeUnknownError` for embedders). 1.3.0 has this bug too
- a store opened directly on a table directory under another name (`.` from inside it, a symlink, or `TABLES` on a case-insensitive file system) took a lock of its own instead of the data directory's, so it could write beside the server that owns the table. The path is now resolved and the parent compared with `tables` by file identity
- a read-only store read a table that was dropped and created again under the same name as an empty table: chunks absent from the new table loaded as empty, and `CHUNKSCAN` returned nothing. Loading an absent chunk and scanning now check the table manifest's store id and fail as for a dropped table
- a checkpoint that failed after removing the WAL (for example at the directory sync after it) left the chunk marked as having a WAL header, so the next write started a WAL without one: the chunk could not be loaded after a restart, and in `fsync-wal` mode the new file's directory entry was not synced. The next write now starts the WAL with its header. 1.3.0 has this bug too; there the next start skipped that WAL with a warning and lost its writes
- a read-only store failed every load of a chunk whose WAL a crash had torn at the end, until a writer loaded that chunk and trimmed the tail. It now reads up to the last whole frame, as a writer does; damage followed by a valid frame still fails the load. 1.3.0 has this bug too
- area reads on a read-only store (`CHUNKRANGE`, `CHUNKRADIUS`, `CHUNKSCAN`) read a chunk's image and WAL without the snapshot-generation check that chunk loads use, so a writer in another process could make them return a state that never existed (an old image with newer WAL frames), or the frame of a rejected conditional write that a rollback intent still covers. They now read each chunk the way a read-only chunk load does, which costs a bracketed read per uncached chunk and can fail closed under a busy writer like a chunk load. 1.3.0 has this bug too
- a read-only store whose directory is removed after it opened (a dropped
  table) now fails chunk loads and scans instead of reading the table as
  empty: the snapshot-generation record a writer never removes is required
  once seen
- a write acknowledged after a crash had torn the end of its chunk's WAL was
  lost at the next restart: the torn bytes were kept, later frames were
  appended after them, and replay stops at the torn bytes. A read-write load
  now truncates a crash-shaped tail (no CRC-valid frame header after the
  stop) to the last valid frame before appending, inside a
  snapshot-generation transition. A WAL with a damaged header, or damage
  followed by a valid frame, was
  skipped with a warning, which dropped its frames and every later append the
  same way; it now fails the chunk load and the file is left as it is
  (`chunkdb_verify` reports `wal_damaged`). A WAL cut while it was being
  created is replaced instead of being appended to without a header
- `block_bits` is limited to `65535`. Geometry accepted up to `1048576`, but
  the `.chk` and `.wal` headers store the value in 16 bits, so a store with
  wider blocks wrote truncated headers and its data could not be read back
  after a restart. Such a geometry is now rejected at startup
- a read-only chunk load no longer fails when a snapshot artifact exists but
  cannot be opened at that instant. Windows makes the target of an atomic
  replace briefly unopenable, which the reader treated as damage and reported
  as `read-only snapshot cannot read artifact`; it is namespace instability
  like a vanished path, so it is retried inside the existing bounded budget.
  An artifact that stays unreadable for the whole budget still fails closed,
  and the error now names the last read failure

### Performance

- `CHUNKSCAN` lazily indexes the top-level split-layout directories once and
  maintains the catalog as chunks are loaded and evicted. Later pages seek
  into the ordered catalog instead of listing the whole data directory and
  copying/sorting the resident registry. Read-only stores refresh the catalog
  when the writer snapshot generation changes

- snapshot-generation brackets coalesce across consecutive transitions. The
  even (stable) `chunkdb.snapshot` record is now published lazily instead of
  immediately when the last writer leaves an epoch, so a transition starting
  shortly afterwards re-enters the still-open odd epoch at no snapshot I/O
  cost — the same coalescing concurrent writers already had, extended across
  time. A cache-eviction pass previously paid three durable syncs of a 16-byte
  record per evicted chunk; it now pays roughly one bracket for the pass.
  `WALFLUSH`, store close, and ordinary group-commit flushes get the same
  saving. The epoch is bounded (a 50 ms window or 512 transitions) and both
  `WALFLUSH` and a clean store close publish the deferred record, so a barrier
  and a closed store still leave a stable even generation behind
- read-only chunk loads retry their bracketed collection with bounded backoff
  (eight sleep-free attempts, then exponential backoff within a 250 ms sleep
  budget) instead of eight sleep-free attempts and an immediate failure. A
  normal-length writer bracket now delays a read-only load rather than failing
  it; an unresolved epoch still fails closed once the budget is spent
- a resident chunk costs about **1.1 kB of RSS instead of ~5.9 kB** (544 B of
  chunk state, default geometry), so a full `max_loaded_chunks=16384` cache is
  roughly 18 MiB rather than 92 MiB. The per-chunk WAL append stream is held
  by pointer and created on first use: libc++ allocates the `basic_filebuf`
  buffer in the `std::ofstream` constructor, so an inline member cost every
  resident chunk about 4.7 kB of heap for a stream that sparse workloads
  usually never open. Eviction throughput is unchanged
- `CHUNKSCAN` candidate collection walks the `L_<lx>_<ly>` directories as
  columns in scan order: columns entirely before the cursor are skipped and
  the walk stops once the page window cannot change, so a page no longer
  lists every chunk file in the world. Ordering, cursor semantics, and the
  bounded per-page memory are unchanged
- `CHUNKSCAN` no longer merges the whole chunk cache into every candidate
  pass. The resident cache and the on-disk artifacts are now visited together,
  large chunk by large chunk in scan order, so the cursor and the page window
  prune both. A page costs O(large chunks) plus the contents of the large
  chunks it actually visits instead of O(resident chunks), and a warm cache no
  longer makes a page slower than a cold one
- `CHUNKSCAN` pruning is now per large chunk rather than per column, so a
  world no wider than `large_chunk_width_chunks` (a single column, where no
  column could ever be skipped) and the tail of any column are cut by `y` as
  well. The experimental `fs_region_v1` layout keeps its full `.rgn` walk;
  only its cache merge is scoped
- `chunkdb_large_world_bench` gained `--scenario chunkscan`, which enumerates
  a whole world through the cursor contract against a cold and then a warm
  cache and reports both times, their ratio, and the number of large-chunk
  directory listings and cache merges each walk performed. Measured on macOS
  arm64 / APFS with a 60 000-chunk world fully resident
  (`bench/artifacts/manual-runs/chunkscan-warm-vs-cold-20260907-macos*`): the
  warm walk merges 1443 large chunks instead of 60 416 at page size 1024 and
  7755 instead of 960 512 at page size 64, and takes 1.19x resp. 1.33x less
  time. On a world one large-chunk column wide, where the previous
  column-level test could prune nothing at all, both the directory listings
  and the cache merges drop from 25 000 to 8538 and the warm walk from 1.58 s
  to 0.60 s

### Internal

- hot-path budgets: `chunkdb_server_bench` gained the grid-world scenarios `world`, `canvas` and `simulation` and, for spawn mode, `--durability-mode` and `--server-workers`; `scripts/bench/compare_budgets.py` compares two builds on them (median of 15 alternating runs, a 5% budget in the `relaxed` profile), and `docs/PERFORMANCE.md` records the baseline
- `CHUNKSCAN` semantics are now pinned by regression tests that are
  independent of the pruning: an exhaustive cursor sweep compared against a
  brute-force reference (every cursor position, on and off a large-chunk edge,
  inside and outside the world), the same sweep replayed against a cold store
  and a fully resident one, enumeration of chunks living at the `int64`
  coordinate edges, a large chunk that holds flushed and cache-only chunks at
  once, and a `fs_region_v1` walk. `CHUNKRANGE`/`CHUNKRADIUS` are covered by a
  test asserting they never enter the scan walk at all
- added `docs/FORMAT_V2_DESIGN.md`, the proposal for the coordinated on-disk
  format bump (WAL frames with header-covering CRCs, persisted chunk
  revisions, image header v4) that becomes chunkdb 2.0, and the cursor-aware
  `CHUNKSCAN` walk that ships independently in 1.x

## v1.3.0 - 2026-09-03

### Protocol (additive)

- `CHUNKSETBIN <cx> <cy> [STATE] <payload_length>`: binary chunk write. The
  request line is followed by exactly `payload_length` raw bytes (the packed
  layout `CHUNKBIN` / `CHUNKBIN STATE` returns) and an empty line, so full
  chunk writes are no longer bounded by the request-line limit and large
  geometries can be written over the protocol. Length mismatches within the
  geometry bound are drained and rejected with `INVALID_ARGUMENT`; unframeable
  requests (malformed header, oversized length, bad terminator, or an
  unauthenticated session) are refused and the connection closed
- `--max-line-bytes` server flag exposes the previously fixed 65536-byte
  request-line limit

### Compatibility

- Windows native TLS is now a stable support claim for the MSYS2 MinGW64
  toolchain with MSYS2 OpenSSL. The `Build and Test TLS (windows-latest)` job
  runs the same `server_integration` TLS cases that back the Linux and macOS
  claims on every change. MSVC and other OpenSSL distributions remain untested
  and unclaimed. Closes #6

### Internal

- CI gains a Windows native TLS gate: an MSYS2 MinGW64 build with
  `CHUNKDB_WITH_TLS=ON` that runs the smoke tests, asserts OpenSSL was
  actually linked, and checks `AUTH` + `PING` over TLS with
  `openssl s_client`. `docs/WINDOWS_NATIVE.md` documents the same check.
  (see Compatibility below)
- the writer-lock heartbeat no longer consumes the generic `ATOMICWRITE`
  failpoints. Its metadata write runs on a background thread every 250 ms and
  could claim a failpoint armed for a concurrent conditional-intent write,
  which made that write succeed and `world_ops_regression` fail its
  `assert(threw)` intermittently under the ASan CI gate
- the Build and Test CI jobs (Linux, macOS, Windows, and both TLS jobs) now
  build with `CHUNKDB_WERROR=ON`; `scripts/test/quick.sh` accepts a
  `CHUNKDB_WERROR` environment variable (default `OFF`). Two GCC
  `-Wrange-loop-construct` diagnostics this exposed in
  `bench/layout_ab_bench.cpp` and `tests/durability_kill_recovery_test.cpp`
  are fixed
- `scripts/release/generate_checksums.sh` writes the bare artifact file name
  into each `.sha256` sidecar regardless of the directory argument, so
  `sha256sum -c` works next to the downloaded file

## v1.2.0 - 2026-09-03

### Security

- the runtime container image no longer ships a default `CHUNKDB_TOKEN`.
  Previously `Dockerfile` set `CHUNKDB_TOKEN=dev-token` in the runtime stage
  and repeated the same credential in the default `CMD` and `HEALTHCHECK`, so
  every image carried a publicly known auth token; because the environment
  variable outranks `--token` during resolution it also silently overrode an
  operator's explicit flag. The server already refuses to start when auth is
  enabled with an empty token, and that guardrail now applies to the image.
  **Breaking for container users:** `CHUNKDB_TOKEN` (or a mounted
  `--token-file`) must be supplied, and `docker compose up` fails fast without
  it. The compose service also publishes on `127.0.0.1` instead of all
  interfaces
- the `HEALTHCHECK` probe no longer authenticates: it sends `PING`, which is
  answered before the auth gate, so it needs no credential
- startup logs which token source was used (never the value) and warns when a
  lower-priority source is shadowed, so an ambient `CHUNKDB_TOKEN` silently
  overriding `--token` is visible
- fixed an out-of-bounds stack write in the server's socket readiness wait.
  `select()`'s `fd_set` is a fixed `FD_SETSIZE`-wide bitmap and `FD_SET`
  performs an unchecked store, so any descriptor at or above that bound wrote
  past the stack object. Descriptors come directly from `accept()` and an
  unauthenticated client could drive them past the bound under the
  file-descriptor limits this project ships, reaching the defect pre-auth via
  the busy-response path. The wait now uses `poll()`/`WSAPoll()`, matching the
  accept loop; this also fixes a latent bug where the `EINTR` retry reused
  `fd_set`s that `select()` had already cleared
- defense in depth: the region slot bound check in `WriteRegionSlotState` runs
  before the heap writes rather than after; conditional-intent filenames are
  validated for component grammar and containment before the reconstructed
  path reaches `resize_file()`/`remove()`; atomic-write temp files are created
  with `O_EXCL | O_NOFOLLOW` (POSIX) and `CREATE_NEW` plus
  `FILE_FLAG_OPEN_REPARSE_POINT` (Windows); token and key material is ignored
  by `.dockerignore` and `.gitignore`

### Fixed

- fixed a data race in the WAL append-stream cache: capacity checks scanned
  other chunks' `wal_stream_initialized` flag and `ofstream` state under the
  cache mutex only, while the owning thread reset them under the chunk mutex
  when a checkpoint or eviction closed the stream. The flag is now atomic and
  the cache no longer inspects another chunk's stream object; caught by the
  TSan gate in `world_ops` (background maintenance)
- fixed a `-Wignored-qualifiers` warning in the socket readiness wait that
  broke `CHUNKDB_WERROR=ON` builds with GCC

### Internal

- `chunk_store.cpp`, `server.cpp`, `chunk_ops.cpp`, and `checkpoint.cpp` were
  split into focused translation units (`chunk_format`, `block_ops`,
  `chunk_cache`, `snapshot_generation`, `server_socket`, `server_io`,
  `server_tls`, `server_connection`, `version_clock`, `conditional_intent`,
  `durability_io`, `atomic_write`). Behavior-preserving: no protocol, on-disk
  format, durability, locking, or public header change, and the existing test
  suite passes unmodified

## v1.1.0 - 2026-07-18

### Correctness / durability hardening (audit remediation)

- ordinary writes (`SET`/`UNSET`/`CHUNKSET`, `MSET` items) now treat the
  successful WAL flush as the commit point: a rejected command fully rolls
  back memory, staged batch records, counters, and the WAL file (a torn or
  unsynced append is truncated back inside the same odd snapshot
  generation), while post-commit inline-checkpoint or generation
  republication failures are logged and retried instead of being returned as
  command errors. An error reply now always means "not applied"; previously
  a `SET` could return `-ERR` while its value became durable and reappeared
  after restart
- version tokens for ordinary writes are reserved before the WAL append (as
  conditional mutations already did), so a version-clock failure is a clean
  pre-write error
- WAL replay adds a structural guard that stops replay when a delta record
  crosses the payload/presence region boundary, a partial mitigation for the
  WAL record header (`byte_offset`/`data_size`) being outside the record CRC.
  This is format-compatible (no version bump; v2/v3 WALs unchanged and
  readable). It does not fully close the gap — an offset flip that stays
  within one region is still undetectable — so the header-CRC gap is recorded
  as a known limitation for the 1.x line and a full fix (extending the record
  CRC to cover the header) is deferred to the next format version
- conditional-intent artifacts moved to the dedicated shallow directory
  `.chunkdb.intents/`, making startup intent recovery proportional to
  pending intents instead of a full recursive data-directory walk;
  `chunkdb_verify` validates the new location and flags misplaced intents
- the snapshot-generation even (stable) record is published without a
  required directory sync: losing it to a crash re-exposes the durable odd
  record and readers fail closed — strictly more conservative — while
  halving a per-flush durable sync; the odd record keeps full durability
- the per-source auth-failure table is hard-bounded (4096 sources,
  least-recently-updated eviction that never evicts an actively banned entry)
  and IPv6 sources are bucketed per /64 prefix while IPv4-mapped/compatible
  IPv6 peers are tracked by their embedded IPv4 address, closing an
  unbounded-memory denial-of-service vector without collapsing all IPv4
  clients into one shared ban
- background maintenance no longer aborts the process on a transient eviction
  error, and a failed eviction WAL flush is a survivable per-operation error
  rather than a permanent store-wide fail-closed state
- `CHUNKSCAN` collection is bounded to the page size with duplicate-free,
  cursor-filtered accumulation: large dirty worlds (chunks with `.chk` +
  `.wal` + cached entries) stay fully enumerable and the previous
  1M-candidate hard failure is gone
- `MSET`'s per-item, non-atomic-across-items semantics are now documented
  (`docs/PROTOCOL.md`), including the applied-prefix behavior on mid-command
  failure; each individual item is all-or-nothing
- documented the rare world-read contention fallback that may cache a probed
  chunk, and corrected the sparse-write performance figures in
  `docs/KNOWN_LIMITATIONS.md` with platform-qualified measurements

### Protocol (additive)

- world-oriented reads: `CHUNKSCAN` (paginated enumeration of populated
  chunks with deterministic ordering and cursor continuation), `CHUNKRANGE`
  (bounded rectangular multi-chunk state read, max 256 chunks, full int64
  corner domain with a 64 MiB response-byte cap), and `CHUNKRADIUS` (bounded
  radius/disc multi-chunk read with the same limits)
- chunk concurrency primitives: `CHUNKVER` (opaque chunk version token),
  `CHUNKCAS` (conditional full-state replace), `CHUNKBATCH` (atomic
  single-chunk block batch); new error code `VERSION_MISMATCH`. Version tokens
  come from a persisted monotonic clock, so a stale version deterministically
  cannot match after eviction or restart. Rejected conditional mutations are
  fully rolled back (memory and WAL) and never reappear after a crash-style
  restart; geometries too large for single-record atomicity are rejected up
  front
- `WALFLUSH`: explicit global durability barrier that makes all previously
  acknowledged writes durable in every durability mode, including `relaxed`
- `METRICS`: Prometheus text-format runtime metrics with bounded per-class
  latency histograms, command/error/auth counters, store gauges, active and
  pending connection gauges, and server-side failure counters for malformed
  requests and admission-control rejections
- `CHUNKBINC`: zrle-compressed binary chunk transfer (opt-in per request)
- `INFO` gains counters for eviction recency skips, empty-chunk GC, WAL
  barriers (and full-sync fallbacks), compressed checkpoint images, background
  maintenance, and the configured checkpoint compression

### Storage / durability

- empty-chunk garbage collection: checkpointing a chunk with no present
  blocks now reclaims its image, WAL, and (when empty) its parent directory
  instead of writing an empty image; observable absent-vs-explicit-zero
  semantics are unchanged
- `fsync-wal` mode now syncs checkpoint images (and directory entries) before
  removing the WAL they replace, closing a window where acknowledged durable
  WAL data could be replaced by an unsynced image
- `WALFLUSH`'s bounded-tracking overflow fallback fails closed on any
  traversal or sync error, syncs files before the directories that reference
  them, and preserves its bookkeeping for a retry after a failure
- a checked persisted `chunkdb.version` clock record backs deterministic chunk
  version tokens; stable-v1 stores migrate safely, an intermediate 8-byte
  ceiling upgrades without reset, and stores with a valid initialized marker
  fail closed if the clock is missing, unreadable, or invalid (see
  `docs/STORAGE_FORMAT.md`)
- conditional WAL rollback intents have explicit rollback and committed
  states, so intent-establishment failures leave live state untouched and
  unlink/directory-sync failures cannot later truncate acknowledged writes
- concurrent read-only chunk loads use a checked durable monotonic
  `chunkdb.snapshot` generation around image/WAL/intent collection. This
  replaces byte-only double collection and rejects cross-process ABA cycles,
  crashed-writer epochs, malformed metadata, and generation wraparound
- optional checkpoint image compression (`--checkpoint-compression zrle`,
  image format v3); v1/v2/v3 images are all readable regardless of the flag,
  and compression stays off by default (see `bench/artifacts/` for recorded
  codec results)
- recency-aware cache eviction: LRU-ordered candidates with a second chance
  for recently accessed chunks, with a guaranteed-progress fallback pass
- opt-in background maintenance (`--background-maintenance`): checkpoints and
  eviction run on a bounded-queue maintenance thread with inline-fallback
  backpressure, drain-on-shutdown, and inline retry of failed background
  checkpoints

### Tooling

- `chunkdb_verify`: read-only data-directory integrity checker with
  machine-usable output and exit codes. It is included in normal
  install/package output, flags misplaced `.wal` files (not just `.chk`), and
  emits paths/details as quoted, escaped tokens so spaces or control
  characters cannot split or forge a field.
- `chunkdb_compression_bench`: reproducible codec size/throughput/latency
  micro-benchmark

## v1.0.0 - 2026-05-29

First **stable** release. The stable channel commits to the compatibility and
support boundary documented in `docs/COMPATIBILITY.md`; preview caveats no
longer apply to the surfaces declared stable there.

### Stability / compatibility

- added `docs/COMPATIBILITY.md`: semver policy, stable surfaces (on-disk
  `fs_split_v1` format readability, wire protocol, durability contract, CLI),
  and explicit non-guarantees
- stable support boundary: Linux native, macOS native, Windows native non-TLS
- explicitly **out of stable claims**: experimental `fs_region_v1` backend and
  Windows native TLS (tracked in #6)

### Protocol

- added batch commands `MSET` (multi-block write) and `MGET` (multi-block read)
  returning a `*N` array reply, reducing round-trips on high-latency links

### Reliability / portability fixes

- file-descriptor budget is now fitted per platform at startup: Linux/macOS
  clamp `max_open_wal_streams` to `RLIMIT_NOFILE`; Windows raises and clamps to
  the CRT stdio limit (`_setmaxstdio`/`_getmaxstdio`), fixing `EMFILE` under
  open-heavy workloads; the server also fits `max_pending_clients` to the budget
- bounded startup recovery scan so large worlds do not stall on launch
- constant-time AUTH token comparison; internal errors no longer leak
  filesystem paths to clients
- thread-safe CRC32 table initialization
- fixed signed-shift UB in little-endian decode of high bytes
- async-signal-safe shutdown handling
- correct IPv6 bracketed-address parsing in the connection URI parser
- CLI/clients reject commands containing CR/LF (request-injection guard)

### CI / tests

- all build/test, crash, and stress gates green on Linux, macOS, and Windows
- made Linux-fragile networking tests deterministic (no reliance on OS
  socket-buffer or single-worker scheduling behavior); see
  `docs/CI_PORTABILITY_NOTES.md`

### Optimization (carried from the preview line)

- WAL group-commit controls for relaxed mode (`wal_group_commit_updates`,
  `--wal-group-commit-updates`)
- write hot path: removed per-`SET` payload copies, batched WAL append buffer,
  flush on clean shutdown and before eviction
- parse path: low-allocation `Protocol::ParseLineView`, case-insensitive
  view-based dispatch, `std::from_chars` integer parsing

### Known limitations (after the stable cut)

- single-backend stable (`fs_split_v1`); `fs_region_v1` remains experimental
- no cross-chunk transactions / full ACID; no replication
- sparse-write throughput is bounded by the file-per-chunk layout
  (`docs/KNOWN_LIMITATIONS.md`)

## v0.1.1-preview (engineering alpha) - 2026-03-13

Stabilization update with terminology/positioning polish and stronger validation coverage for the current engine.

### Storage Model and Scope

- reaffirmed project identity as a specialized chunk/grid storage engine
- standardized wording around:
  - chunk-native protocol
  - chunk-oriented access model
  - bit-packed block storage
  - WAL/checkpoint durability modes
- clarified that benchmarks are workload-scoped behavior characterization for `chunkdb`

### Protocol and Runtime Coverage

- server-path benchmark now covers:
  - `PING`
  - `INFO`
  - `SET`
  - `GET`
  - `CHUNK`
  - `CHUNKBIN`
  - mixed read/write (70/30)
- benchmark output includes per-scenario latency percentiles (`p50`, `p95`, `p99`)

### Durability and Recovery Validation

- dedicated stress test combining:
  - concurrent access
  - hot chunk contention
  - forced eviction pressure
  - repeated load/unload cycles
- kill-recovery durability test expanded to both:
  - `fsync-wal`
  - `fsync-checkpoint`
- added WAL recovery edge-case tests:
  - truncated trailing record handling
  - truncated header handling when checkpoint image exists
- added long-run WAL/checkpoint cycle tests for growth/trigger/correctness behavior

### Documentation and Public Presentation

- README/alpha/performance docs rewritten for a clearer self-contained identity
- benchmark framing rewritten to focus on workload-fit and reproducible behavior
- release-facing language polished for a clear alpha-level guarantees/limitations boundary

### Current Boundaries

- no new major feature areas were added in this release
- alpha remains single-backend (`fs_split_v1`) with explicit limitations

## v0.1.0-alpha - 2026-03-13

First public engineering alpha milestone for `chunk`, positioned as a specialized chunk/grid storage engine.

### Included

- core chunk hierarchy and configurable geometry (`block_bits`, chunk sizes, large-chunk sizes)
- `fs_split_v1` backend:
  - large chunk directory
  - regular chunk `.chk` image
  - per-chunk `.wal` delta log
- delta WAL + threshold checkpoint write path
- durability modes:
  - `relaxed`
  - `fsync-wal`
  - `fsync-checkpoint`
- TCP command server with:
  - worker pool
  - buffered request parsing
  - token auth
  - `CHUNKBIN` binary chunk response
- cache limit and eviction
- inter-process `data_dir` lock
- benchmark executables:
  - `chunkdb_bench` (direct storage path)
  - `chunkdb_server_bench` (end-to-end server path)
- test coverage for:
  - protocol/auth/error handling
  - storage and recovery
  - concurrency and eviction
  - process lock behavior
  - durability kill-recovery scenario

### Known Limitations

- only one backend is included in alpha (`fs_split_v1`)
- no distributed features
- no cross-chunk transactions / full ACID semantics
- benchmark scope is narrow and workload-specific

### Positioning Note

`chunk` does not make broad cross-system performance claims.
Any comparative analysis must be scenario-specific, durability-matched, and fully reproducible.
