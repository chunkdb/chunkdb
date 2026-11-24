# Secondary indexes

A secondary index finds blocks by the value of one block column without reading every chunk.
This note fixes its statements, key encoding, structure, write maintenance, durability, recovery, failures and limits before any code.
Chunk state stays the only source of truth: an index is derived data, every index candidate is checked against the chunk, and a lost or damaged index is rebuilt from chunk state.

## Statements

`CREATE INDEX [IF NOT EXISTS] name ON t (column [PREFIX n])` and `DROP INDEX [IF EXISTS] name ON t` need ADMIN on the table.
An index covers one block column of type u8 to u64, i8 to i64, bool, f32, f64, bits(N), text(max) or bytes(max); chunk columns are not indexed.
A text or bytes column with max above 256 needs `PREFIX n` with 1 <= n <= 256, else CREATE INDEX is refused; PREFIX on any other column is refused.
A table holds at most 16 indexes, each name unique in the table, `[a-z_][a-z0-9_]*` and at most 63 bytes.
`GET BLOCKS FROM t WHERE condition [COLUMNS ...] [LIMIT n] [AFTER ...] [ALLOW SCAN]` returns rows `[x, y(, z), values...]` of present blocks for which the condition is true.
It has no selector: `GET AREA|BLOCKS <sel> FROM t` (#145) reads a region, `GET BLOCKS FROM t WHERE` looks blocks up by value.
DESCRIBE lists the indexes as `indexes: [{name, column, prefix, state}]`, and WATCH CATALOG reports CREATE INDEX and DROP INDEX as table changes carrying that map.
HELLO adds feature `secondary_indexes`.

## Index selection

The condition is split into its top-level AND conjuncts.
The first conjunct that compares an indexed column with constants or parameters selects the index: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN (...)` and `IS NOT NULL`; for bits(N) only `=` and `IN`.
Conjuncts are read in written order, so a statement chooses its index by putting that conjunct first.
The chosen conjunct becomes one or more key ranges; every candidate block is then read under its chunk's shared lock and the whole condition is evaluated on it, the chosen conjunct included.
Key ranges are built from the typed constants of the planned condition, so with an f32 index `WHERE f = 0.1` seeks the f32 value that 0.1 rounds to, as the condition compares it (#299).
Without a usable conjunct, GET BLOCKS is refused with `INVALID_ARGUMENT reason=no_index` naming the columns of the condition, unless the statement ends with ALLOW SCAN.
ALLOW SCAN without a usable index walks populated chunks in ascending coordinate order, as SCAN CHUNKS WHERE does, and LIMIT counts the blocks it visits.
A scanned page can therefore return no rows with `more` true; its `after` then names the last block it visited, and the next page continues after it.
NULL, NaN and absent blocks have no index entry, so `IS NULL`, `<> v`, `NOT` and conditions on NaN need ALLOW SCAN.

## Keys and order

An entry is `(key, chunk coordinates, block index)`; entries sort by key bytes, then coordinates ascending by x, y, z, then block index.
Pages through an index follow that order, and `AFTER` takes the last row's value and block coordinates: `AFTER value x y [z]`.
Unsigned integers are big-endian at their width; signed integers flip the sign bit, then big-endian.
Floats map -0 to +0, then flip the sign bit of a positive value or every bit of a negative one, so key order is numeric order; NaN has no entry.
bool is one byte; bits(N) is its packed bytes.
Text and bytes are their bytes with 0x00 written as 0x00 0xFF and the value ended by 0x00 0x00, so a shorter value sorts first and the coordinates that follow cannot change value order; text compares bytewise as in conditions.
With `PREFIX n` the key is the first n bytes of the value, encoded the same way; a match on the prefix is only a candidate, and the value itself is compared when the candidate is checked.
A value longer than its prefix still has one entry; values sharing a prefix sort by coordinates.

## Structure

An index has an in-memory delta and up to 8 immutable runs on disk, newest first.
A run holds its entries sorted, and a chunk table: every chunk the run covers with the revision whose state produced its entries.
A chunk's entries come from the newest layer that covers it, the delta included; older layers' entries for that chunk are ignored when read and dropped when runs merge.
The delta covers each chunk written since the newest run was published: the chunk's full entry set, ordered, with its revision.
An in-memory map from chunk coordinates to the newest covering layer serves both reads and maintenance; it costs about 32 bytes per covered chunk.
Runs merge when there are more than 8, or when the delta's entries reach a quarter of the base run's; a merge writes one run and drops the inputs.
An entry removed after an open snapshot is kept, marked with its removal revision, in the delta and in every run a flush or merge writes from it, while any open snapshot predates that revision; current reads skip it and snapshot reads before its removal see it.

## Write maintenance

Every path that publishes a chunk state does the maintenance under the chunk's exclusive lock, before the write completes its producer bound, next to the coordinate index publication: SET BLOCK, DELETE BLOCK, SET CHUNK, conditional writes, transaction COMMIT, expiry and the area writes of #145.
A table without indexes checks one empty list per published chunk and does nothing else.
A chunk entering the delta copies its entry set from the old state, at a cost of one key per present block of the chunk and the index.
After that a write changes only the entries of the blocks it changed: each path already knows them (a block, a projection, the transaction's block deltas, or the whole chunk for SET CHUNK).
Each changed block costs, per index, the old and new key encodings and two ordered-map updates under the index's mutex, sharded by key hash into 16 parts.
Read-your-writes holds because a write's new entries are in the delta before the write is acknowledged; a removed entry may linger as a candidate, which the check discards.
The delta is bounded by `index_delta_bytes`, 64 MiB per table by default; above it the maintenance pool flushes it to a run, and above twice that writers wait for the flush as they wait for checkpoints.

## Durability

A flush writes the delta as a run file with a temporary name in the index directory, syncs it, renames it and syncs the directory, then publishes a new index manifest the same way.
The manifest lists the runs and their checksums and records the table epoch; its atomic replacement is the only commit point of a flush or merge.
A failure after the manifest was replaced but before memory follows it fails the index closed until it is reopened, and no generation is used twice, so no run a manifest may list is rewritten.
Runs and manifests are written in every durability mode; they are a cache of chunk state and never carry a write that its chunk lost.
A run may cover a chunk state that the chunk's WAL later loses in relaxed mode; recovery then finds a different revision for that chunk and rebuilds its entries.
Within one epoch and clock a revision is never reissued, because the store clock reserves ranges durably before issuing them, so an equal revision means the same chunk state.
A new epoch (RESTORE TABLE or a fork) or a rebuilt clock breaks that, so each of them rebuilds the index: a manifest whose epoch differs from the table's is treated as missing, and `chunkdb_admin rebuild-version-clock` removes the index manifests of every table whose clock it rebuilds.
SWAP TABLES moves each table with its epoch and its index directory, so its indexes stay valid.
FLUSH WAL does not flush indexes; the next open repairs what the runs miss.

## Recovery

Opening a table loads the index manifest and the runs' chunk tables after chunk recovery and the coordinate index are complete.
Each chunk the coordinate index lists is compared with its covering layer: a missing chunk, a different revision or a covered chunk that is no longer populated has its entries recomputed from its recovered state into the delta.
This is the WAL replay of the index: chunk recovery already applied every durable frame after the run, and the comparison finds every chunk it changed.
The comparison walks the coordinate index once and reads only the changed chunks.
A missing, unreadable or mismatched manifest, a run whose digest, header, table identity or index identity does not match, or an index in state `building` is rebuilt from the chunk images; the schema version a run records does not decide this (see Build and drop).
A rebuild sets the index to `building`, and GET BLOCKS that would use it returns `INDEX_BUILDING name=...` until it is ready, or scans with ALLOW SCAN.
Every rebuild and every chunk recomputed at open is logged with the index name.

## Build and drop

CREATE INDEX publishes the definition in the table manifest in state `building` and replies once the index is ready; when the statement timeout or CANCEL stops the statement first, it answers `INDEX_BUILDING name=...` and the build continues.
From publication on, every write maintains the index; the builder then walks the coordinate index and, under each chunk's shared lock, adds the chunk unless the delta already covers its current revision.
When the walk ends the builder flushes the delta and publishes state `ready` in the manifest.
A crash during a build leaves state `building`, and the next open builds again from the beginning.
DROP INDEX removes the definition from the table manifest first, then deletes the index directory; an index directory without a definition is deleted at open.
ALTER TABLE that drops, renames or changes the type of an indexed column is refused while the index exists, naming it; any other schema change keeps the index and records the new schema version in the next run.

## Reads

An index page seeks the chosen key ranges after `AFTER`, merges the layers in key order, skips entries of chunks a newer layer covers, and checks each candidate.
A page costs its rows and the candidates it discards, each a seek in up to 9 layers and a block read; it does not grow with the table.
Inside a transaction, a page reads candidates from the index and checks them at the snapshot; entries removed after the snapshot are kept with their removal revision in the delta and in the runs written from it (see Structure), within `--txn-history-bytes`; a snapshot that outlives that bound ends with CONFLICT reason=history_limit as other snapshot reads do.
A read-only process never writes or removes an index file: it loads the runs, recomputes stale chunks into its own delta at open, rebuilds in memory an index it cannot trust, and recomputes the chunks the writer reports in `chunkdb.snapshot.changes` before a page, as it does for the coordinate index.

## Commit groups

Until #136 activates commit groups, each chunk's index entries change in the same per-chunk or per-table commit as the chunk.
With commit groups, one group publishes several chunks and tables together: the group stages each member's index changes while it holds the members' locks and applies them all before the group's producer bound completes.
Recovery needs no change: a group is atomic in chunk state, and the index is recomputed from chunk state.
No index change is ever a group member, so a group's record and its durability do not change either.

## Backup, restore and verify

BACKUP copies index definitions with the table manifest but not index runs; a restored table builds its indexes at open.
`chunkdb_verify` checks each run's checksums and order, and recomputes the entries of every chunk to compare them with the index.
DURABILITY_CONTRACT.md states that indexes never decide whether a write committed and are rebuilt from chunk state after any loss.
Crash tests stop the process inside a flush, a merge, a build and a drop, and check that GET BLOCKS returns exactly the rows a full scan returns.
Tests check that a rebuilt clock and RESTORE TABLE each make the next open rebuild the index, that SWAP TABLES keeps it, and that a flush and a merge during an open READ ONLY transaction keep the entries its snapshot reads.

## Limits

Indexes per table: 16.
Key: 256 bytes before encoding.
Delta: `index_delta_bytes`, default 67108864, per table.
Runs per index: 8 before a merge.
GET BLOCKS: LIMIT up to 1024 rows, and the reply within `--max-area-response-bytes`.
