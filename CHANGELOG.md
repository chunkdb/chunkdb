# Changelog

## Unreleased

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
