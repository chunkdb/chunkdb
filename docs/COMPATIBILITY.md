# Compatibility

This policy covers chunkdb 2.0's [protocol 3](PROTOCOL.md), [CQL](CQL.md), [on-disk format](STORAGE_FORMAT.md) and [durability contract](DURABILITY_CONTRACT.md).
Within 2.x, protocol and storage changes are additive.
Clients that use the 2.0 surface and data written by 2.0 work with every later 2.x server.
Removing or incompatibly changing that surface requires 3.0.

## Protocol and clients

Existing statements, parameters, response types and meanings remain compatible.
Later 2.x servers may add commands, optional clauses and map keys; clients must ignore unfamiliar map keys.
A connection begins with `HELLO 3`; another version is refused without executing statements.
Committed chunk versions are producing commit revisions, ordered only within the same table epoch.
DESCRIBE exposes that epoch and a completed revision frontier; read-only mode returns null for an unknown frontier.
GET BLOCK WITH VERSION returns null when no committed version remains retained, including identities forgotten by tombstone compaction.
Collection retains an empty chunk's producing version and time across eviction and restart.
Versions retain their meaning across eviction and restart while their committed state remains retained.

The Go, JavaScript and CLI packages version independently.
Use a package that supports protocol 3 and the APIs your application needs; package version numbers need not equal the server's version.
This promise does not make new 2.x commands available on a 2.0 server.

## Stored data

Every later 2.x server reads valid data written by 2.0, preserving existing fields and their meanings.
Manifests record feature flags; sections, options and WAL records can grow behind those flags.

Unknown incompatible features prevent opening.
Unknown read-only-compatible features permit only read-only opening.
Compatible features may be ignored.

An older binary is not required to open data that uses a newer feature it does not implement.

Upgrade the binary before enabling such a feature.
Use `ENABLE FEATURE '<name>'` with ADMIN on `*`; activation never clears existing bits.
Existing directories require `feature_activation` before enabling another missing feature.
A durable activation decision finishes during writer startup; read-only opening refuses a pending decision.
An uncertain activation publication returns OUTCOME_UNKNOWN and fences serving until recovery.
`SHOW STATUS` reports recorded directory and table masks, known enabled names, and protected activation state.
Later 2.x servers still open valid 2.0 data.

| Feature | Scope and owned metadata | Rollback backup |
|---|---|---|
| `feed_slots` | Tables: slots, archive, writer identity and collection records. | Before bit `0x1` was enabled. |
| `migrations` | Directory: migration ledger and redo decisions. | Before bit `0x2` was enabled. |
| `table_lineage` | Tables: parent epoch and fork revision. | Before bit `0x4` was enabled. |
| `slot_filters` | Tables: stable projection IDs and persisted filters. | Before bit `0x8` was enabled. |
| `feature_activation` | Directory: checked activation decisions. | Before bit `0x10` was enabled. |
| `chunk_model` | Directory and tables: dimensional geometry, both column domains, sectioned forms, and retained coordinate decisions. | No conversion to an earlier development layout. |
| `commit_groups` | Directory and tables: checked group decisions, grouped WAL frames, durability receipts and retained result identities. | A build without this feature refuses both the directory and affected tables. |

A reader or writer must support every recorded incompatibility bit, even when its owned records are absent.
To use a binary lacking a feature, restore the whole consistent backup identified above.
Use a restore tool that supports that backup's recorded features; current restore requires `table_lineage` in its source tables.
Dropping records or manually clearing masks does not establish a supported rollback.
New directories already enable these initial features; an older binary must support their masks to open them.

Restore records the immediate parent epoch and completed backup cut, preserving committed revision values within a new epoch.
A parent position continues only when its required prefix survives or a proven exact-fork boundary needs no missing changes.
Backups contain state at their cuts, without archived catch-up history.
After later crashes or restarts, absent chunks cannot prove no history was lost; requests without durable proof receive lineage-bearing resync.


Table geometry is immutable and validated against its manifest.
Keep manifests, revision clocks, initialized markers and all other required artifacts together in a consistent backup.
2.0 accepts only its documented directory and file layouts.
It does not import superseded development layouts.
Development directories using earlier manifest, image, WAL, slot, or index envelopes must be recreated; opening refuses them before changing artifacts.

An unsupported layout is refused, rather than converted in place.

## Boundaries

The durability modes and `FLUSH WAL` preserve their documented guarantees throughout 2.x.

Supported native platforms are Linux, macOS, and Windows 10 or later.
Native Windows TLS uses MSYS2 UCRT64 and its OpenSSL package.
MSVC and other Windows TLS distributions are outside the validated toolchain boundary.
Filesystem and device sync semantics remain prerequisites for durability.

Internal C++ symbols, diagnostic log text, metric names and benchmark results are not protocol or storage compatibility guarantees.
Only one server writes a data directory.
Replication and shared multi-writer operation are unavailable.
See [known limitations](KNOWN_LIMITATIONS.md) before selecting runtime options.
