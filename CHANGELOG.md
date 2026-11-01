# Changelog

## Unreleased

- Mix coordinate hashes with a process seed across every axis and bound shared large-chunk containers, exposing capacity metrics and a known BUSY refusal for new cache misses (#134).
- Add three-dimensional tables, typed chunk columns, checked sectioned chunk forms, atomic metadata and projected writes, and retained tombstone identities with incremental scans and explicit compaction (#133).
- Add matched-subscriber budget comparisons with `chunkdb_budget_compare --watch-both TABLE[:N][:slots]` and require a control with the same subscriber settings (#140).
- Add extensible reply metadata, exact unsigned revisions, HELLO options, persistent watch filters, ACK deadlines, takeover, lineage, and explicit feature activation (#69).
- Keep feed subscribers off the write path: a feed wakes the slot catch-up worker only for slot watches, once per scan and when a slot watch starts, notifies watches only when a scan publishes changes, stops a slot watch's steps when one changes nothing, counts every slot's retained bytes from one archive listing, and syncs only the files written since the last durable-frontier pass after a store's first, including those a rollback boundary is still syncing; `chunkdb_server_bench --watch TABLE[:N][:slots]` opens several ordinary or slot watches for the subscriber budget (#140).
- Flush and checkpoint an evicted chunk without holding its large-chunk lock, so loads of the other chunks in that large chunk no longer wait for the checkpoint's syncs, except while the table has feed slots, when a load still waits for the checkpoint's publish step; `chunkdb_large_world_bench --scenario evict-load` reports their cold-load latency percentiles during eviction checkpoints (#143).
- Add `--read-only` to serve a data directory, next to its writer, or a completed backup without writing to it: no writer lock, recovery, checkpoints, maintenance or revision reservations; statements that change anything answer INVALID_ARGUMENT, a directory that needs recovery is refused with a message saying so, a backup is checked against its inventory before it opens, the users file and its permissions stay as they are, and a chunk read once is kept and read again only after the writer changed its table; a reader probing a table no longer makes a starting writer fail as if another writer held it (#157).
- Bound FLUSH WAL and clean shutdown time on large worlds: the barrier syncs from up to 16 threads, no longer blocks checkpoints while it syncs except on Windows, and above 65,536 tracked files tracks changed large-chunk directories instead of syncing the whole table directory; a rollback that replaces a hard-linked WAL after a barrier syncs the new entry at once (#141).
- Add RECOVER TABLE to recover a fenced table without a restart while other tables keep serving: a table left out of service by a failed ALTER, DROP or CREATE, with a fail-closed store or a failed snapshot generation, or after a failed feed cleanup; SHOW TABLES lists fenced tables, DESCRIBE reports why one is fenced, backups refuse a fenced table, and watches of a fenced or recovered table end with INTERNAL instead of NO_TABLE (#159).
- Log how much of the reply the socket took when a connection misses its reply write deadline (`reply write deadline exceeded after <written> of <size> bytes`) (#130).
- Record the host name in the writer lock metadata, so a process on another host does not take over a writer whose heartbeat is under 5 seconds old because no local process has its pid; metadata without a host name keeps the pid check (#150).
- Remove staging left by failed or interrupted backups when the next backup starts, not only at server start; a start that cannot remove backup staging logs a warning instead of refusing to open, entries holding a backup marker or guard are kept, and catalogs opened without the writer lock leave backup staging in place (#150).
- Add `--audit-log` for logins and administrative statements as JSON Lines, reopened on SIGHUP; `--slow-statement-ms` for WARN lines on slow statements; `--auth-max-failures-per-connection`, `--auth-ban-failures` and `--auth-ban-ms` for the login limits; and `--proxy-protocol v2` to take client addresses from PROXY protocol v2 headers (#69).
- Format metric durations and bucket bounds without the C library's locale-dependent number formatting, so `SHOW METRICS` always uses a `.` decimal separator (#150).
- Builds without tests (`-DCHUNKDB_BUILD_TESTS=OFF`, as the release binaries and the Docker image are built) leave out failure injection, so the engine no longer reads `CHUNKDB_FAILPOINT_*` environment variables on its write and flush paths; `CHUNKDB_FAILPOINTS` sets it explicitly (#142).
- Publish stable read-only snapshot generations through a mapped counter when the last artifact transition finishes, without an idle timer or per-write metadata sync (#69).
- Sync the data directory before relying on a feed archive directory this store did not create, so a preparation that failed before syncing it cannot leave archived WALs in a directory whose entry may not survive a crash (#69).
- Add opt-in libFuzzer targets (`-DCHUNKDB_FUZZ=ON`) for request framing, CQL, WAL frames, chunk images, chunk forms, manifests, slot records, migration records and backup markers, run for 30 seconds on every pull request and nightly for longer, with crash inputs kept in `fuzz/regression` and replayed by `ctest` without libFuzzer; request parameter framing now has one implementation shared by the server and the fuzz harness, and the chunk form decoder is shared with the engine (#163).

- Add the expression engine for conditions and computed assignments: parsing, typing and exact evaluation with three-valued NULL logic, not yet used by statements (#69).

- Add RELOAD TLS, also run by SIGHUP on Linux and macOS when a `chunks://` listener is configured, to load a renewed TLS certificate and key for new connections without a restart, and `--tls-min-version 1.2|1.3`; the certificate file may hold intermediate certificates after the server certificate, and they are sent to clients (#69).
- Preserve queued terminal slot-watch errors when an acknowledgement arrives during shutdown (#69).

- Evict the oldest retained ordinary feed history before refusing a write capture, so watches that keep up no longer resync when the live feed budget is full (#69).

- Build and test native Windows with MSYS2 UCRT64; binaries use the Universal C Runtime included in Windows 10 and later (#69).
- Rewrite user guides and statement references with checked CLI examples and publish a major Docker image tag (#68).
- Limit connections before HELLO to 64 per source by default, create data and backup files/directories with owner-only access (including Windows ACLs), tighten existing users-file permissions at startup, and obtain salts/nonces directly from the operating system random generator (#63).

- Cancel pending Windows socket I/O when stopping active connections, and prevent dequeued connections from becoming active after shutdown (#69).

- Publish server shutdown under the pending-worker mutex so idle workers cannot miss the stop notification (#69).

- Prevent feed I/O shutdown from waiting indefinitely when Stop wakeups are drained before socket polling (#69).

- Remove an ordinary WATCH subscription and establish its linger policy before acknowledging UNWATCH (#69).

- Persist per-table live feed and durable-slot byte limits, expose effective limits in DESCRIBE and apply ALTER limits to existing feeds and slots (#65).

- Start fresh servers without tables, use explicit table definitions for geometry, remove the unsupported shared-writer server switch and recognize only current backup staging names (#60).

- Return null for unwritten chunks, default table chunks to 16 x 16, allow required columns without defaults on empty tables, include narrowing ranges, apply comma-separated rights atomically and accept trailing CQL options in any order (#69).

- Bound SET BLOCK parameter frames by their actual declared lengths, accepting small values in columns with large maxima while refusing an oversized total before reading its payload (#63).

- Bound the parameter frames of one statement together, write `chunkdb.users` readable by its owner alone, bound a verifier's iteration count, refuse TLS renegotiation and bound a chunk image section's declared size.

- Check the Docker quick-start page in PR CI and publish amd64/arm64 GHCR images for release tags (#67).

- Default relaxed budget comparisons to four matched clients and server workers; retain explicit eight- and sixteen-worker calibration options (#59).

- Use paired-bootstrap throughput intervals for hot-path budget gates; attach validated same-window noise references and support matched relaxed client/worker counts (#59).

- Report hot-path benchmark request mixes, actual block counts, latency ranges and build/machine provenance in JSON and pasteable Markdown budget comparisons (#59).
- Add benchmark `--io-timeout-ms` for socket send/receive waits and the spawned server; report the chosen budget (#59).

- Add conditional table, column, slot and user creation/removal with `IF NOT EXISTS` and `IF EXISTS`; authorized no-ops preserve existing definitions and state, and schema forms also work in named migrations (#62).

- Preserve TLS connections returned from WATCH when a worker has an unrelated OpenSSL error, report slot protocol test failures by group, and distinguish peer-closed socket timeout configuration failures (#65).

- Include the offline administration tool in packages and the Docker image, and correct documentation links, recovery guidance and release prerequisites.

- Rewrite the 2.0 user documentation and client entry points, document additive 2.x compatibility, separate current design notes from superseded proposals and release history, and repair documentation links (#68).
- Add typed tables, schema history and CQL over protocol 3 with SCRAM-SHA-256 users and per-table rights.
- Add single-table snapshot transactions with durable multi-chunk commits in every durability mode.
- Add live change feeds, durable slots, retained archive catch-up and batched acknowledgements.
- Add named schema migrations with durable redo decisions, ordered records and idempotent retries.
- Add online backup with per-table cuts, checked inventories, offline verification and restore with fresh identities and retained migration records.
- Add Docker bootstrap credentials, persistent data/backup storage and runnable quick-start examples.
- Use checked extensible images/WALs, immutable table geometry and persisted equality versions; refuse unsupported layouts rather than converting them in place.
- Preserve durable state across WAL/checkpoint replacement, barriers, rollback and recovery; report ambiguous decisions explicitly and fence affected stores/catalogs until restart.

Earlier released notes and superseded development entries are retained as [history](docs/releases/CHANGELOG_HISTORY.md).
The current [protocol](docs/PROTOCOL.md), [storage](docs/STORAGE_FORMAT.md) and [compatibility policy](docs/COMPATIBILITY.md) define the 2.0 surface.
